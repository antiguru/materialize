// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A supervisor for a cluster replica on the unified cluster protocol.
//!
//! A single [`ClusterReplica`] owns one CTP connection per cluster process and one task that
//! interleaves storage and compute commands onto it. The interleaving order chosen by that task is
//! the total order, by construction: it is the single point that owns the connection write. This is
//! the controller-side counterpart of the cluster-side [`mz_cluster_protocol::ClusterDemux`].
//!
//! The supervisor follows the same die-and-respawn model as the previous per-subsystem replica
//! tasks: the task never reconnects in place. On disconnect the message loop returns an error, the
//! task finishes, and [`ClusterReplica::failed`] flips. The owner drops the dead supervisor and
//! spawns a fresh one with a new epoch; reconciliation is the controllers replaying their full
//! command streams into the fresh task.

use std::sync::Arc;
use std::sync::atomic::{self, AtomicBool};
use std::time::{Duration, Instant};

use anyhow::bail;
use mz_build_info::BuildInfo;
use mz_cluster_client::ReplicaId;
use mz_cluster_client::client::ClusterReplicaLocation;
use mz_cluster_protocol::{ClusterCommand, ClusterReplicaMetrics, ClusterResponse};
use mz_compute_client::controller::sequential_hydration::SequentialHydration;
use mz_compute_client::logging::LoggingConfig;
use mz_compute_client::metrics::{IntCounter, ReplicaMetrics as ComputeReplicaMetrics};
use mz_compute_client::protocol::command::ComputeCommand;
use mz_compute_client::protocol::response::ComputeResponse;
use mz_compute_types::dyncfgs::ENABLE_COMPUTE_REPLICA_EXPIRATION;
use mz_dyncfg::ConfigSet;
use mz_ore::channel::InstrumentedUnboundedSender;
use mz_ore::retry::{Retry, RetryState};
use mz_ore::task::AbortOnDropHandle;
use mz_service::client::{GenericClient, Partitioned};
use mz_service::params::GrpcClientParameters;
use mz_service::transport;
use mz_storage_client::client::{StorageCommand, StorageResponse};
use mz_storage_client::metrics::ReplicaMetrics as StorageReplicaMetrics;
use tokio::select;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::{debug, info, trace, warn};
use uuid::Uuid;

/// The compute controller's per-replica response, tagged with replica id and epoch.
type ComputeResponseTx = InstrumentedUnboundedSender<(ReplicaId, u64, ComputeResponse), IntCounter>;
/// The storage controller's per-replica response, tagged with the originating replica id.
type StorageResponseTx = UnboundedSender<(Option<ReplicaId>, StorageResponse)>;

/// The CTP client for the unified cluster protocol.
type ClusterCtpClient = transport::Client<ClusterCommand, ClusterResponse>;
/// The partitioned cluster client, fanning out across the replica's processes.
type Client = Partitioned<ClusterCtpClient, ClusterCommand, ClusterResponse>;

/// Configuration for a [`ClusterReplica`].
#[derive(Clone, Debug)]
pub struct ClusterReplicaConfig {
    /// The build information for this process.
    pub build_info: &'static BuildInfo,
    /// The location (CTP addresses) of the replica processes.
    pub location: ClusterReplicaLocation,
    /// gRPC client parameters (connect and keepalive timeouts).
    pub grpc_client: GrpcClientParameters,
    /// Dynamic system configuration.
    pub dyncfg: Arc<ConfigSet>,
    /// The logging configuration to install on the compute instance.
    pub compute_logging: LoggingConfig,
    /// The offset to use for compute replica expiration, if any.
    pub compute_expiration_offset: Option<Duration>,
    /// Whether compute arrangements on this replica use dictionary compression.
    pub compute_arrangement_dictionary_compression: bool,
}

/// A supervisor for a single cluster replica.
#[derive(Debug)]
pub struct ClusterReplica {
    /// A handle to the task that aborts it when the supervisor is dropped.
    ///
    /// If the task is finished, the replica has failed and needs rehydration.
    task: AbortOnDropHandle<()>,
    /// Flag reporting whether the replica connection has been established.
    connected: Arc<AtomicBool>,
}

impl ClusterReplica {
    /// Spawns a supervisor that connects to the replica and interleaves storage and compute
    /// commands onto a single connection.
    ///
    /// Storage and compute commands are delivered on `storage_command_rx` and `compute_command_rx`,
    /// and responses are forwarded to `storage_response_tx` and `compute_response_tx`. The `epoch`
    /// identifies this incarnation of the replica and is attached to compute responses so the
    /// controller can discard stale responses.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        replica_id: ReplicaId,
        config: ClusterReplicaConfig,
        epoch: u64,
        storage_metrics: StorageReplicaMetrics,
        compute_metrics: ComputeReplicaMetrics,
        storage_command_rx: UnboundedReceiver<StorageCommand>,
        compute_command_rx: UnboundedReceiver<ComputeCommand>,
        storage_response_tx: StorageResponseTx,
        compute_response_tx: ComputeResponseTx,
    ) -> Self {
        let connected = Arc::new(AtomicBool::new(false));

        let task = mz_ore::task::spawn(
            || format!("cluster-replica-{replica_id}"),
            ReplicaTask {
                replica_id,
                config,
                epoch,
                storage_metrics,
                compute_metrics,
                connected: Arc::clone(&connected),
                storage_command_rx,
                compute_command_rx,
                storage_response_tx,
                compute_response_tx,
            }
            .run(),
        );

        Self {
            task: task.abort_on_drop(),
            connected,
        }
    }

    /// Determine if the replica task has failed.
    pub fn failed(&self) -> bool {
        self.task.is_finished()
    }

    /// Determine if the replica connection has been established.
    pub fn is_connected(&self) -> bool {
        self.connected.load(atomic::Ordering::Relaxed)
    }
}

/// The task backing a [`ClusterReplica`].
struct ReplicaTask {
    /// The ID of the replica.
    replica_id: ReplicaId,
    /// Replica configuration.
    config: ClusterReplicaConfig,
    /// A number identifying this incarnation of the replica.
    ///
    /// The semantics of this don't matter, except that it must strictly increase.
    epoch: u64,
    /// Storage replica metrics.
    storage_metrics: StorageReplicaMetrics,
    /// Compute replica metrics.
    compute_metrics: ComputeReplicaMetrics,
    /// Flag to report successful replica connection.
    connected: Arc<AtomicBool>,
    /// A channel upon which storage commands intended for the replica are delivered.
    storage_command_rx: UnboundedReceiver<StorageCommand>,
    /// A channel upon which compute commands intended for the replica are delivered.
    compute_command_rx: UnboundedReceiver<ComputeCommand>,
    /// A channel upon which storage responses from the replica are delivered.
    storage_response_tx: StorageResponseTx,
    /// A channel upon which compute responses from the replica are delivered.
    compute_response_tx: ComputeResponseTx,
}

impl ReplicaTask {
    /// Asynchronously forwards commands to and responses from a single replica.
    async fn run(self) {
        let replica_id = self.replica_id;
        info!(%replica_id, "starting cluster replica task");

        let client = self.connect().await;
        match self.run_message_loop(client).await {
            Ok(()) => info!(%replica_id, "stopped cluster replica task"),
            Err(error) => warn!(%replica_id, %error, "cluster replica task failed"),
        }
    }

    /// Connects to the replica.
    ///
    /// The connection is retried forever (with backoff) and this method returns only after a
    /// connection was successfully established.
    async fn connect(&self) -> Client {
        let metrics =
            ClusterReplicaMetrics::new(self.storage_metrics.clone(), self.compute_metrics.clone());

        let try_connect = async |retry: RetryState| {
            let version = self.config.build_info.semver_version();
            let client_params = &self.config.grpc_client;
            let connect_timeout = client_params.connect_timeout.unwrap_or(Duration::MAX);
            let keepalive_timeout = client_params
                .http2_keep_alive_timeout
                .unwrap_or(Duration::MAX);

            let connect_start = Instant::now();
            let connect_result = ClusterCtpClient::connect_partitioned(
                self.config.location.ctl_addrs.clone(),
                version,
                connect_timeout,
                keepalive_timeout,
                metrics.clone(),
            )
            .await;

            self.storage_metrics
                .observe_connect_time(connect_start.elapsed());
            self.compute_metrics
                .observe_connect_time(connect_start.elapsed());

            connect_result.inspect_err(|error| {
                let next_backoff = retry.next_backoff.unwrap();
                if retry.i >= mz_service::retry::INFO_MIN_RETRIES {
                    info!(
                        replica_id = %self.replica_id, ?next_backoff,
                        "error connecting to replica: {error:#}",
                    );
                } else {
                    debug!(
                        replica_id = %self.replica_id, ?next_backoff,
                        "error connecting to replica: {error:#}",
                    );
                }
            })
        };

        let client = Retry::default()
            .clamp_backoff(Duration::from_secs(1))
            .retry_async(try_connect)
            .await
            .expect("retry retries forever");

        self.storage_metrics.observe_connect();
        self.compute_metrics.observe_connect();
        self.connected.store(true, atomic::Ordering::Relaxed);

        client
    }

    /// Runs the message loop.
    ///
    /// Returns (with an `Err`) if it encounters an error condition (e.g. the replica disconnects).
    /// If no error condition is encountered, the task runs until both controllers disconnect from
    /// their command channels, or the task is dropped.
    async fn run_message_loop(mut self, mut client: Client) -> Result<(), anyhow::Error> {
        // The sequential hydration interceptor holds back compute `Schedule` commands and releases
        // them as hydration capacity frees up. It is recreated per incarnation, matching the
        // lifetime of the connection.
        let mut hydration = SequentialHydration::new(
            Arc::clone(&self.config.dyncfg),
            self.compute_metrics.clone(),
        );

        // Whether each command channel is still open. The two controllers release their senders
        // independently when a replica is dropped; we keep serving the surviving subsystem until
        // both are gone.
        let mut storage_open = true;
        let mut compute_open = true;

        while storage_open || compute_open {
            select! {
                // Storage command from controller to forward to the replica.
                // `mpsc::UnboundedReceiver::recv` is documented as cancel safe.
                command = self.storage_command_rx.recv(), if storage_open => {
                    match command {
                        Some(mut command) => {
                            specialize_storage_command(&mut command);
                            client.send(ClusterCommand::Storage(command)).await?;
                        }
                        None => storage_open = false,
                    }
                },
                // Compute command from controller to forward to the replica.
                command = self.compute_command_rx.recv(), if compute_open => {
                    match command {
                        Some(mut command) => {
                            self.specialize_compute_command(&mut command);
                            self.observe_compute_command(&command);
                            for command in hydration.absorb_command(command) {
                                client.send(ClusterCommand::Compute(command)).await?;
                            }
                        }
                        None => compute_open = false,
                    }
                },
                // Response from the replica to forward to the controllers.
                // `GenericClient::recv` implementations are required to be cancel safe.
                response = client.recv() => {
                    let Some(response) = response? else {
                        bail!("replica unexpectedly gracefully terminated connection");
                    };

                    match response {
                        ClusterResponse::Storage(response) => {
                            if self
                                .storage_response_tx
                                .send((Some(self.replica_id), response))
                                .is_err()
                            {
                                // Controller is no longer interested in this replica. Shut down.
                                break;
                            }
                        }
                        ClusterResponse::Compute(response) => {
                            self.observe_compute_response(&response);

                            for command in hydration.observe_response(&response) {
                                client.send(ClusterCommand::Compute(command)).await?;
                            }

                            if self
                                .compute_response_tx
                                .send((self.replica_id, self.epoch, response))
                                .is_err()
                            {
                                // Controller is no longer interested in this replica. Shut down.
                                break;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Specialize a compute command for this replica.
    ///
    /// Most `ComputeCommand`s are independent of the target replica, but some contain
    /// replica-specific fields that must be adjusted before sending.
    fn specialize_compute_command(&self, command: &mut ComputeCommand) {
        match command {
            ComputeCommand::Hello { nonce } => {
                *nonce = Uuid::new_v4();
            }
            ComputeCommand::CreateInstance(config) => {
                config.logging = self.config.compute_logging.clone();
                if ENABLE_COMPUTE_REPLICA_EXPIRATION.get(&self.config.dyncfg) {
                    config.expiration_offset = self.config.compute_expiration_offset;
                }
                config.arrangement_dictionary_compression =
                    self.config.compute_arrangement_dictionary_compression;
            }
            _ => {}
        }
    }

    /// Update task state according to an observed compute command.
    fn observe_compute_command(&self, command: &ComputeCommand) {
        if let ComputeCommand::Peek(peek) = command {
            peek.otel_ctx.attach_as_parent();
        }

        trace!(
            replica = ?self.replica_id,
            command = ?command,
            "sending compute command to replica",
        );

        self.compute_metrics.inner.command_queue_size.dec();
    }

    /// Update task state according to an observed compute response.
    fn observe_compute_response(&self, response: &ComputeResponse) {
        if let ComputeResponse::PeekResponse(_, _, otel_ctx) = response {
            otel_ctx.attach_as_parent();
        }

        trace!(
            replica = ?self.replica_id,
            response = ?response,
            "received compute response from replica",
        );
    }
}

/// Specialize a storage command for the target replica.
///
/// Most [`StorageCommand`]s are independent of the target replica, but some contain
/// replica-specific fields that must be adjusted before sending.
fn specialize_storage_command(command: &mut StorageCommand) {
    if let StorageCommand::Hello { nonce } = command {
        *nonce = Uuid::new_v4();
    }
}
