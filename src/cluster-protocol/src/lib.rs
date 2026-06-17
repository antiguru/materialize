// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The unified cluster protocol.
//!
//! Storage and compute run as two independent timely runtimes inside a single `clusterd` process,
//! historically each driven by its own cluster protocol over its own connection. This crate carries
//! both subsystems' messages on a single, totally-ordered command stream:
//!
//! * A union message type ([`ClusterCommand`] / [`ClusterResponse`]) and a delegating
//!   [`PartitionedState`] that routes each variant into the existing storage and compute partition
//!   state machines. The divergent routing and merge logic is reused verbatim through delegation,
//!   not rewritten, because the underlying machinery ([`Partitioned`], [`Partitionable`], CTP
//!   `transport::Client`) is already generic over the message type.
//! * A cluster-side demultiplexer ([`ClusterDemux`]) that splits the union stream back into the
//!   storage and compute server handlers, so the two runtimes stay separate.
//!
//! See `doc/developer/design/20260617_unify_storage_compute_protocol.md`.
//!
//! [`Partitioned`]: mz_service::client::Partitioned

use async_trait::async_trait;
use mz_compute_client::metrics::ReplicaMetrics as ComputeReplicaMetrics;
use mz_compute_client::protocol::command::ComputeCommand;
use mz_compute_client::protocol::response::ComputeResponse;
use mz_compute_client::service::PartitionedComputeState;
use mz_service::client::{GenericClient, Partitionable, PartitionedState};
use mz_service::transport;
use mz_storage_client::client::{PartitionedStorageState, StorageCommand, StorageResponse};
use mz_storage_client::metrics::ReplicaMetrics as StorageReplicaMetrics;
use serde::{Deserialize, Serialize};

/// A command on the unified cluster protocol, carrying either a storage or a compute command.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ClusterCommand {
    /// A storage command.
    Storage(StorageCommand),
    /// A compute command.
    Compute(ComputeCommand),
}

/// A response on the unified cluster protocol, carrying either a storage or a compute response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ClusterResponse {
    /// A storage response.
    Storage(StorageResponse),
    /// A compute response.
    Compute(ComputeResponse),
}

/// Partitioned state for the unified cluster protocol.
///
/// Holds the existing storage and compute partition state machines and dispatches each
/// [`ClusterCommand`] / [`ClusterResponse`] by variant. The per-subsystem routing (storage
/// broadcasts all commands; compute unicasts to worker 0 except `Hello`/`UpdateConfiguration`) and
/// merge logic (frontier union vs. meet, peek/subscribe row merging) are reused unchanged.
#[derive(Debug)]
pub struct PartitionedClusterState {
    /// The storage partition state machine.
    storage: PartitionedStorageState,
    /// The compute partition state machine.
    compute: PartitionedComputeState,
}

impl Partitionable<ClusterCommand, ClusterResponse> for (ClusterCommand, ClusterResponse) {
    type PartitionedState = PartitionedClusterState;

    fn new(parts: usize) -> PartitionedClusterState {
        PartitionedClusterState {
            storage: <(StorageCommand, StorageResponse)>::new(parts),
            compute: <(ComputeCommand, ComputeResponse)>::new(parts),
        }
    }
}

impl PartitionedState<ClusterCommand, ClusterResponse> for PartitionedClusterState {
    fn split_command(&mut self, command: ClusterCommand) -> Vec<Option<ClusterCommand>> {
        match command {
            ClusterCommand::Storage(command) => self
                .storage
                .split_command(command)
                .into_iter()
                .map(|part| part.map(ClusterCommand::Storage))
                .collect(),
            ClusterCommand::Compute(command) => self
                .compute
                .split_command(command)
                .into_iter()
                .map(|part| part.map(ClusterCommand::Compute))
                .collect(),
        }
    }

    fn absorb_response(
        &mut self,
        shard_id: usize,
        response: ClusterResponse,
    ) -> Option<Result<ClusterResponse, anyhow::Error>> {
        match response {
            ClusterResponse::Storage(response) => self
                .storage
                .absorb_response(shard_id, response)
                .map(|result| result.map(ClusterResponse::Storage)),
            ClusterResponse::Compute(response) => self
                .compute
                .absorb_response(shard_id, response)
                .map(|result| result.map(ClusterResponse::Compute)),
        }
    }
}

/// CTP connection metrics for the unified cluster protocol.
///
/// Delegates to the storage and compute per-replica metrics. The typed message callbacks are
/// routed by variant, preserving the existing per-command and per-response counters. The
/// connection-level byte callbacks cannot be attributed to a subsystem once the two protocols
/// share one connection, so they are forwarded to both sub-metrics; each subsystem's byte counter
/// then reflects the shared connection's total throughput.
#[derive(Clone, Debug)]
pub struct ClusterReplicaMetrics {
    /// The storage per-replica metrics.
    storage: StorageReplicaMetrics,
    /// The compute per-replica metrics.
    compute: ComputeReplicaMetrics,
}

impl ClusterReplicaMetrics {
    /// Create a new `ClusterReplicaMetrics` delegating to the given storage and compute metrics.
    pub fn new(storage: StorageReplicaMetrics, compute: ComputeReplicaMetrics) -> Self {
        Self { storage, compute }
    }
}

impl transport::Metrics<ClusterCommand, ClusterResponse> for ClusterReplicaMetrics {
    fn bytes_sent(&mut self, len: usize) {
        self.storage.bytes_sent(len);
        self.compute.bytes_sent(len);
    }

    fn bytes_received(&mut self, len: usize) {
        self.storage.bytes_received(len);
        self.compute.bytes_received(len);
    }

    fn message_sent(&mut self, msg: &ClusterCommand) {
        match msg {
            ClusterCommand::Storage(msg) => self.storage.message_sent(msg),
            ClusterCommand::Compute(msg) => self.compute.message_sent(msg),
        }
    }

    fn message_received(&mut self, msg: &ClusterResponse) {
        match msg {
            ClusterResponse::Storage(msg) => self.storage.message_received(msg),
            ClusterResponse::Compute(msg) => self.compute.message_received(msg),
        }
    }
}

/// A cluster-side demultiplexer for the unified cluster protocol.
///
/// Sits in front of the per-process storage and compute server handlers and splits the union
/// command stream back into the two subsystems, merging their responses back into the union stream.
/// The relative order between a storage and a compute command is discarded at this split, which is
/// correct because the two runtimes are still separate and do not consume cross-subsystem order.
#[derive(Debug)]
pub struct ClusterDemux<S, C> {
    /// The storage server handler.
    storage: S,
    /// The compute server handler.
    compute: C,
}

impl<S, C> ClusterDemux<S, C> {
    /// Create a new `ClusterDemux` wrapping the given storage and compute server handlers.
    pub fn new(storage: S, compute: C) -> Self {
        Self { storage, compute }
    }
}

#[async_trait]
impl<S, C> GenericClient<ClusterCommand, ClusterResponse> for ClusterDemux<S, C>
where
    S: GenericClient<StorageCommand, StorageResponse>,
    C: GenericClient<ComputeCommand, ComputeResponse>,
{
    async fn send(&mut self, command: ClusterCommand) -> Result<(), anyhow::Error> {
        match command {
            ClusterCommand::Storage(command) => self.storage.send(command).await,
            ClusterCommand::Compute(command) => self.compute.send(command).await,
        }
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe: it only awaits `GenericClient::recv` on the wrapped handlers,
    /// which are required to be cancel safe, in a [`tokio::select!`] that drops the unselected
    /// branch without losing messages.
    async fn recv(&mut self) -> Result<Option<ClusterResponse>, anyhow::Error> {
        tokio::select! {
            response = self.storage.recv() => {
                Ok(response?.map(ClusterResponse::Storage))
            }
            response = self.compute.recv() => {
                Ok(response?.map(ClusterResponse::Compute))
            }
        }
    }
}
