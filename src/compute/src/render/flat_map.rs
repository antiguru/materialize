// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::VecDeque;

use differential_dataflow::consolidation::ConsolidatingContainerBuilder;
use mz_compute_types::dyncfgs::COMPUTE_FLAT_MAP_FUEL;
use mz_expr::MfpPlan;
use mz_expr::{MapFilterProject, MirScalarExpr, TableFunc};
use mz_repr::{DatumVec, RowArena, SharedRow};
use mz_repr::{Diff, Row, RowRef, Timestamp};
use mz_timely_util::operator::StreamExt;
use timely::dataflow::Scope;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::generic::Session;
use timely::progress::Antichain;

use crate::render::DataflowError;
use crate::render::context::{CollectionBundle, Context};

impl<G> Context<G>
where
    G: Scope,
    G::Timestamp: crate::render::RenderTimestamp,
{
    /// Applies a `TableFunc` to every row, followed by an `mfp`.
    pub fn render_flat_map(
        &self,
        input_key: Option<Vec<MirScalarExpr>>,
        input: CollectionBundle<G>,
        exprs: Vec<MirScalarExpr>,
        func: TableFunc,
        mfp: MapFilterProject,
    ) -> CollectionBundle<G> {
        let until = self.until.clone();
        let mfp_plan = mfp.into_plan().expect("MapFilterProject planning failed");

        // Budget to limit the number of rows processed in a single invocation.
        //
        // The current implementation can only yield between input batches, but not from within
        // a batch. A `generate_series` can still cause unavailability if it generates many rows.
        let budget = COMPUTE_FLAT_MAP_FUEL.get(&self.config_set);

        // When we have columnar input and no arrangement key, iterate the columnar
        // container directly — each item yields (&RowRef, T::Ref, Diff::Ref) without
        // allocating owned Rows.
        if input_key.is_none() {
            if let Some((col_oks, col_errs)) = &input.columnar_collection {
                let scope = input.scope();
                let (oks, errs) = col_oks.inner.clone().unary_fallible(
                    Pipeline,
                    "FlatMapStageColumnar",
                    move |_, info| {
                        let activator = scope.activator_for(info.address);
                        let mut queue = VecDeque::new();
                        Box::new(move |input, ok_output, err_output| {
                            use columnar::{Columnar, Index};
                            let mut datums = DatumVec::new();
                            let mut datums_mfp = DatumVec::new();
                            let mut table_func_output = Vec::new();
                            let mut budget = budget;

                            input.for_each(|cap, data| {
                                queue.push_back((
                                    cap.retain(0),
                                    cap.retain(1),
                                    std::mem::take(data),
                                ))
                            });

                            while let Some((ok_cap, err_cap, data)) = queue.pop_front() {
                                let mut ok_session = ok_output.session_with_builder(&ok_cap);
                                let mut err_session = err_output.session_with_builder(&err_cap);

                                for (row_ref, t_ref, r_ref) in data.borrow().into_index_iter() {
                                    let time: G::Timestamp = Columnar::into_owned(t_ref);
                                    let diff: Diff = Columnar::into_owned(r_ref);
                                    let temp_storage = RowArena::new();

                                    let datums_local = datums.borrow_with(row_ref);
                                    let args = exprs
                                        .iter()
                                        .map(|e| e.eval(&datums_local, &temp_storage))
                                        .collect::<Result<Vec<_>, _>>();
                                    let args = match args {
                                        Ok(args) => args,
                                        Err(e) => {
                                            err_session.give((e.into(), time, diff));
                                            continue;
                                        }
                                    };
                                    let mut extensions = match func.eval(&args, &temp_storage) {
                                        Ok(exts) => exts.fuse(),
                                        Err(e) => {
                                            err_session.give((e.into(), time, diff));
                                            continue;
                                        }
                                    };

                                    while let Some((extension, output_diff)) = extensions.next() {
                                        table_func_output.push((extension, output_diff));
                                        table_func_output.extend((&mut extensions).take(1023));
                                        drain_through_mfp(
                                            row_ref,
                                            &time,
                                            &diff,
                                            &mut datums_mfp,
                                            &table_func_output,
                                            &mfp_plan,
                                            &until,
                                            &mut ok_session,
                                            &mut err_session,
                                            &mut budget,
                                        );
                                        table_func_output.clear();
                                    }
                                }
                                if budget == 0 {
                                    activator.activate();
                                    break;
                                }
                            }
                        })
                    },
                );

                use differential_dataflow::AsCollection;
                let ok_collection = oks.as_collection();
                let new_err_collection = errs.as_collection();
                let err_collection = col_errs.clone().concat(new_err_collection);
                let col_oks = crate::render::columnar::vec_to_columnar(ok_collection);
                return CollectionBundle::from_columnar_collections(col_oks, err_collection);
            }
        }

        // Vec fallback: arrangement key or no columnar collection.
        let (ok_collection, err_collection) =
            input.as_specific_collection(input_key.as_deref(), &self.config_set);
        let stream = ok_collection.inner;
        let scope = input.scope();

        let (oks, errs) = stream.unary_fallible(Pipeline, "FlatMapStage", move |_, info| {
            let activator = scope.activator_for(info.address);
            let mut queue = VecDeque::new();
            Box::new(move |input, ok_output, err_output| {
                let mut datums = DatumVec::new();
                let mut datums_mfp = DatumVec::new();

                // Buffer for extensions to `input_row`.
                let mut table_func_output = Vec::new();

                let mut budget = budget;

                input.for_each(|cap, data| {
                    queue.push_back((cap.retain(0), cap.retain(1), std::mem::take(data)))
                });

                while let Some((ok_cap, err_cap, data)) = queue.pop_front() {
                    let mut ok_session = ok_output.session_with_builder(&ok_cap);
                    let mut err_session = err_output.session_with_builder(&err_cap);

                    'input: for (input_row, time, diff) in data {
                        let temp_storage = RowArena::new();

                        // Unpack datums for expression evaluation.
                        let datums_local = datums.borrow_with(&input_row);
                        let args = exprs
                            .iter()
                            .map(|e| e.eval(&datums_local, &temp_storage))
                            .collect::<Result<Vec<_>, _>>();
                        let args = match args {
                            Ok(args) => args,
                            Err(e) => {
                                err_session.give((e.into(), time, diff));
                                continue 'input;
                            }
                        };
                        let mut extensions = match func.eval(&args, &temp_storage) {
                            Ok(exts) => exts.fuse(),
                            Err(e) => {
                                err_session.give((e.into(), time, diff));
                                continue 'input;
                            }
                        };

                        // Draw additional columns out of the table func evaluation.
                        while let Some((extension, output_diff)) = extensions.next() {
                            table_func_output.push((extension, output_diff));
                            table_func_output.extend((&mut extensions).take(1023));
                            drain_through_mfp(
                                &input_row,
                                &time,
                                &diff,
                                &mut datums_mfp,
                                &table_func_output,
                                &mfp_plan,
                                &until,
                                &mut ok_session,
                                &mut err_session,
                                &mut budget,
                            );
                            table_func_output.clear();
                        }
                    }
                    if budget == 0 {
                        activator.activate();
                        break;
                    }
                }
            })
        });

        use differential_dataflow::AsCollection;
        let ok_collection = oks.as_collection();
        let new_err_collection = errs.as_collection();
        let err_collection = err_collection.concat(new_err_collection);
        CollectionBundle::from_collections(ok_collection, err_collection)
    }
}

/// Drains a list of extensions to `input_row` through a supplied `MfpPlan` and into output buffers.
///
/// The method decodes `input_row`, and should be amortized across non-trivial `extensions`.
fn drain_through_mfp<T>(
    input_row: &RowRef,
    input_time: &T,
    input_diff: &Diff,
    datum_vec: &mut DatumVec,
    extensions: &[(Row, Diff)],
    mfp_plan: &MfpPlan,
    until: &Antichain<Timestamp>,
    ok_output: &mut Session<
        '_,
        '_,
        T,
        ConsolidatingContainerBuilder<Vec<(Row, T, Diff)>>,
        Capability<T>,
    >,
    err_output: &mut Session<
        '_,
        '_,
        T,
        ConsolidatingContainerBuilder<Vec<(DataflowError, T, Diff)>>,
        Capability<T>,
    >,
    budget: &mut usize,
) where
    T: crate::render::RenderTimestamp,
{
    let temp_storage = RowArena::new();
    let mut row_builder = SharedRow::get();

    // This is not cheap, and is meant to be amortized across many `extensions`.
    let mut datums_local = datum_vec.borrow_with(input_row);
    let datums_len = datums_local.len();

    let event_time = input_time.event_time().clone();

    for (cols, diff) in extensions.iter() {
        // Arrange `datums_local` to reflect the intended output pre-mfp.
        datums_local.truncate(datums_len);
        datums_local.extend(cols.iter());

        let results = mfp_plan.evaluate(
            &mut datums_local,
            &temp_storage,
            event_time,
            *diff * *input_diff,
            |time| !until.less_equal(time),
            &mut row_builder,
        );

        for result in results {
            *budget = budget.saturating_sub(1);
            match result {
                Ok((row, event_time, diff)) => {
                    // Copy the whole time, and re-populate event time.
                    let mut time = input_time.clone();
                    *time.event_time_mut() = event_time;
                    ok_output.give((row, time, diff));
                }
                Err((err, event_time, diff)) => {
                    // Copy the whole time, and re-populate event time.
                    let mut time = input_time.clone();
                    *time.event_time_mut() = event_time;
                    err_output.give((err, time, diff));
                }
            };
        }
    }
}
