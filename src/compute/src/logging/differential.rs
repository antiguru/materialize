// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logging dataflows for events generated by differential dataflow.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::Duration;

use differential_dataflow::consolidation::ConsolidatingContainerBuilder;
use differential_dataflow::logging::{
    BatchEvent, BatcherEvent, DifferentialEvent, DropEvent, MergeEvent, TraceShare,
};
use mz_ore::cast::CastFrom;
use mz_repr::{Datum, Diff, Timestamp};
use mz_timely_util::containers::{
    columnar_exchange, Col2ValBatcher, Column, ColumnBuilder, ProvidedBuilder,
};
use mz_timely_util::replay::MzReplay;
use timely::dataflow::channels::pact::{ExchangeCore, Pipeline};
use timely::dataflow::channels::pushers::buffer::Session;
use timely::dataflow::channels::pushers::{Counter, Tee};
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::Leave;
use timely::dataflow::{Scope, Stream, StreamCore};

use crate::extensions::arrange::MzArrangeCore;
use crate::logging::compute::{ArrangementHeapSizeOperatorDrop, ComputeEvent};
use crate::logging::{
    consolidate_and_pack, DifferentialLog, EventQueue, LogCollection, LogVariant,
    OutputSessionColumnar, SharedLoggingState,
};
use crate::row_spine::RowRowBuilder;
use crate::typedefs::{KeyBatcher, RowRowSpine};

pub(super) struct Return<S: Scope> {
    pub collections: BTreeMap<LogVariant, LogCollection>,
    pub compute_events: StreamCore<S, Column<(Duration, ComputeEvent)>>,
}

/// Constructs the logging dataflow for differential logs.
///
/// Params
/// * `scope`: The Timely scope hosting the log analysis dataflow.
/// * `config`: Logging configuration
/// * `event_queue`: The source to read log events from.
pub(super) fn construct<S: Scope<Timestamp = Timestamp>>(
    mut scope: S,
    config: &mz_compute_client::logging::LoggingConfig,
    event_queue: EventQueue<Vec<(Duration, DifferentialEvent)>>,
    shared_state: Rc<RefCell<SharedLoggingState>>,
) -> Return<S> {
    let logging_interval_ms = std::cmp::max(1, config.interval.as_millis());

    scope.scoped("Dataflow: differential logging", move |scope| {
        let enable_logging = config.enable_logging;
        let (logs, token) = event_queue.links
            .mz_replay::<_, ProvidedBuilder<_>, _>(
                scope,
                "differential logs",
                config.interval,
                event_queue.activator,
                move |mut session, mut data|{
                    // If logging is disabled, we still need to install the indexes, but we can leave them
                    // empty. We do so by immediately filtering all logs events.
                    if enable_logging {
                        session.give_container(data.to_mut())
                    }
                }
            );

        // Build a demux operator that splits the replayed event stream up into the separate
        // logging streams.
        let mut demux =
            OperatorBuilder::new("Differential Logging Demux".to_string(), scope.clone());
        let mut input = demux.new_input(&logs, Pipeline);
        let (mut batches_out, batches) = demux.new_output();
        let (mut records_out, records) = demux.new_output();
        let (mut sharing_out, sharing) = demux.new_output();
        let (mut batcher_records_out, batcher_records) = demux.new_output();
        let (mut batcher_size_out, batcher_size) = demux.new_output();
        let (mut batcher_capacity_out, batcher_capacity) = demux.new_output();
        let (mut batcher_allocations_out, batcher_allocations) = demux.new_output();
        let (mut compute_events_out, compute_events) = demux.new_output();

        let mut demux_state = Default::default();
        demux.build(move |_capability| {
            move |_frontiers| {
                let mut batches = batches_out.activate();
                let mut records = records_out.activate();
                let mut sharing = sharing_out.activate();
                let mut batcher_records = batcher_records_out.activate();
                let mut batcher_size = batcher_size_out.activate();
                let mut batcher_capacity = batcher_capacity_out.activate();
                let mut batcher_allocations = batcher_allocations_out.activate();
                let mut compute_events_out = compute_events_out.activate();

                input.for_each(|cap, data| {
                    let mut output_buffers = DemuxOutput {
                        batches: batches.session_with_builder(&cap),
                        records: records.session_with_builder(&cap),
                        sharing: sharing.session_with_builder(&cap),
                        batcher_records: batcher_records.session_with_builder(&cap),
                        batcher_size: batcher_size.session_with_builder(&cap),
                        batcher_capacity: batcher_capacity.session_with_builder(&cap),
                        batcher_allocations: batcher_allocations.session_with_builder(&cap),
                        compute_events: compute_events_out.session_with_builder(&cap),
                    };

                    for (time, event) in data.drain(..) {
                        DemuxHandler {
                            state: &mut demux_state,
                            output: &mut output_buffers,
                            logging_interval_ms,
                            time,
                            shared_state: &mut shared_state.borrow_mut(),
                        }
                        .handle(event);
                    }
                });
            }
        });

        // We're lucky and the differential logs all have the same stream format, so just implement
        // the call once.
        let stream_to_collection = |input: &Stream<_, ((usize, ()), Timestamp, Diff)>, log| {
            let worker_id = scope.index();
            consolidate_and_pack::<_, KeyBatcher<_, _, _>, ColumnBuilder<_>, _, _>(
                input,
                log,
                move |((op, ()), time, diff), packer, session| {
                    let data = packer.pack_slice(&[
                        Datum::UInt64(u64::cast_from(*op)),
                        Datum::UInt64(u64::cast_from(worker_id)),
                    ]);
                    session.give((data, *time, *diff))
                },
            )
        };

        // Encode the contents of each logging stream into its expected `Row` format.
        let arrangement_batches = stream_to_collection(&batches, ArrangementBatches);
        let arrangement_records = stream_to_collection(&records, ArrangementRecords);
        let sharing = stream_to_collection(&sharing, Sharing);
        let batcher_records = stream_to_collection(&batcher_records, BatcherRecords);
        let batcher_size = stream_to_collection(&batcher_size, BatcherSize);
        let batcher_capacity = stream_to_collection(&batcher_capacity, BatcherCapacity);
        let batcher_allocations = stream_to_collection(&batcher_allocations, BatcherAllocations);

        use DifferentialLog::*;
        let logs = [
            (ArrangementBatches, arrangement_batches),
            (ArrangementRecords, arrangement_records),
            (Sharing, sharing),
            (BatcherRecords, batcher_records),
            (BatcherSize, batcher_size),
            (BatcherCapacity, batcher_capacity),
            (BatcherAllocations, batcher_allocations),
        ];

        // Build the output arrangements.
        let mut collections = BTreeMap::new();
        for (variant, collection) in logs {
            let variant = LogVariant::Differential(variant);
            if config.index_logs.contains_key(&variant) {
                let trace = collection
                    .mz_arrange_core::<_, Col2ValBatcher<_, _, _, _>, RowRowBuilder<_, _>, RowRowSpine<_, _>>(
                        ExchangeCore::<ColumnBuilder<_>, _>::new_core(columnar_exchange::<mz_repr::Row, mz_repr::Row, Timestamp, mz_repr::Diff>),
                        &format!("Arrange {variant:?}"),
                    )
                    .trace;
                let collection = LogCollection {
                    trace,
                    token: Rc::clone(&token),
                };
                collections.insert(variant, collection);
            }
        }

        Return { collections, compute_events: compute_events.leave() }
    })
}

type Pusher<D> =
    Counter<Timestamp, Vec<(D, Timestamp, Diff)>, Tee<Timestamp, Vec<(D, Timestamp, Diff)>>>;
type OutputSession<'a, D> =
    Session<'a, Timestamp, ConsolidatingContainerBuilder<Vec<(D, Timestamp, Diff)>>, Pusher<D>>;

/// Bundled output buffers used by the demux operator.
struct DemuxOutput<'a> {
    batches: OutputSession<'a, (usize, ())>,
    records: OutputSession<'a, (usize, ())>,
    sharing: OutputSession<'a, (usize, ())>,
    batcher_records: OutputSession<'a, (usize, ())>,
    batcher_size: OutputSession<'a, (usize, ())>,
    batcher_capacity: OutputSession<'a, (usize, ())>,
    batcher_allocations: OutputSession<'a, (usize, ())>,
    compute_events: OutputSessionColumnar<'a, (Duration, ComputeEvent)>,
}

/// State maintained by the demux operator.
#[derive(Default)]
struct DemuxState {
    /// Arrangement trace sharing
    sharing: BTreeMap<usize, usize>,
}

/// Event handler of the demux operator.
struct DemuxHandler<'a, 'b> {
    /// State kept by the demux operator
    state: &'a mut DemuxState,
    /// Demux output buffers.
    output: &'a mut DemuxOutput<'b>,
    /// The logging interval specifying the time granularity for the updates.
    logging_interval_ms: u128,
    /// The current event time.
    time: Duration,
    /// State shared across log receivers.
    shared_state: &'a mut SharedLoggingState,
}

impl DemuxHandler<'_, '_> {
    /// Return the timestamp associated with the current event, based on the event time and the
    /// logging interval.
    fn ts(&self) -> Timestamp {
        let time_ms = self.time.as_millis();
        let interval = self.logging_interval_ms;
        let rounded = (time_ms / interval + 1) * interval;
        rounded.try_into().expect("must fit")
    }

    /// Handle the given differential event.
    fn handle(&mut self, event: DifferentialEvent) {
        use DifferentialEvent::*;

        match event {
            Batch(e) => self.handle_batch(e),
            Merge(e) => self.handle_merge(e),
            Drop(e) => self.handle_drop(e),
            TraceShare(e) => self.handle_trace_share(e),
            Batcher(e) => self.handle_batcher_event(e),
            _ => (),
        }
    }

    fn handle_batch(&mut self, event: BatchEvent) {
        let ts = self.ts();
        let operator_id = event.operator;
        self.output.batches.give(((operator_id, ()), ts, 1));

        let diff = Diff::try_from(event.length).expect("must fit");
        self.output.records.give(((operator_id, ()), ts, diff));
        self.notify_arrangement_size(operator_id);
    }

    fn handle_merge(&mut self, event: MergeEvent) {
        let Some(done) = event.complete else { return };

        let ts = self.ts();
        let operator_id = event.operator;
        self.output.batches.give(((operator_id, ()), ts, -1));

        let diff = Diff::try_from(done).expect("must fit")
            - Diff::try_from(event.length1 + event.length2).expect("must fit");
        if diff != 0 {
            self.output.records.give(((operator_id, ()), ts, diff));
        }
        self.notify_arrangement_size(operator_id);
    }

    fn handle_drop(&mut self, event: DropEvent) {
        let ts = self.ts();
        let operator_id = event.operator;
        self.output.batches.give(((operator_id, ()), ts, -1));

        let diff = -Diff::try_from(event.length).expect("must fit");
        if diff != 0 {
            self.output.records.give(((operator_id, ()), ts, diff));
        }
        self.notify_arrangement_size(operator_id);
    }

    fn handle_trace_share(&mut self, event: TraceShare) {
        let ts = self.ts();
        let operator_id = event.operator;
        let diff = Diff::cast_from(event.diff);
        debug_assert_ne!(diff, 0);
        self.output.sharing.give(((operator_id, ()), ts, diff));

        let sharing = self.state.sharing.entry(operator_id).or_default();
        *sharing = (i64::try_from(*sharing).expect("must fit") + diff)
            .try_into()
            .expect("under/overflow");
        if *sharing == 0 {
            self.state.sharing.remove(&operator_id);
            self.output.compute_events.give(&(
                self.time,
                ComputeEvent::ArrangementHeapSizeOperatorDrop(ArrangementHeapSizeOperatorDrop {
                    operator_id,
                }),
            ));
        }
    }

    fn handle_batcher_event(&mut self, event: BatcherEvent) {
        let ts = self.ts();
        let operator_id = event.operator;
        let records_diff = Diff::cast_from(event.records_diff);
        let size_diff = Diff::cast_from(event.size_diff);
        let capacity_diff = Diff::cast_from(event.capacity_diff);
        let allocations_diff = Diff::cast_from(event.allocations_diff);
        self.output
            .batcher_records
            .give(((operator_id, ()), ts, records_diff));
        self.output
            .batcher_size
            .give(((operator_id, ()), ts, size_diff));
        self.output
            .batcher_capacity
            .give(((operator_id, ()), ts, capacity_diff));
        self.output
            .batcher_allocations
            .give(((operator_id, ()), ts, allocations_diff));
    }

    fn notify_arrangement_size(&self, operator: usize) {
        // While every arrangement should have a corresponding arrangement size operator,
        // we have no guarantee that it already/still exists. Otherwise we could print a warning
        // here, but it's difficult to implement without maintaining state for a longer period than
        // while the arrangement actually exists.
        if let Some(activator) = self.shared_state.arrangement_size_activators.get(&operator) {
            activator.activate();
        }
    }
}
