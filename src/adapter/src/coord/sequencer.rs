// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Prevents anyone from accidentally exporting a method from the `inner` module.
#![allow(clippy::pub_use)]

//! Logic for executing a planned SQL query.

use inner::return_if_err;
use mz_controller::clusters::ClusterId;
use mz_expr::{MirRelationExpr, OptimizedMirRelationExpr, RowSetFinishing};
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::explain::ExplainFormat;
use mz_repr::{Diff, GlobalId, Timestamp};
use mz_sql::catalog::{CatalogCluster, CatalogError};
use mz_sql::names::ResolvedIds;
use mz_sql::plan::{
    self, AbortTransactionPlan, CommitTransactionPlan, CreateRolePlan, CreateSourcePlans,
    FetchPlan, MutationKind, Params, Plan, PlanKind, RaisePlan, RotateKeysPlan,
};
use mz_sql_parser::ast::{Raw, Statement};
use tokio::sync::oneshot;
use tracing::{event, Level};

use crate::catalog::Catalog;
use crate::command::{Command, ExecuteResponse, Response};
use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::{introspection, Coordinator, Message};
use crate::error::AdapterError;
use crate::notice::AdapterNotice;
use crate::session::{EndTransactionAction, Session, TransactionOps, TransactionStatus, WriteOp};
use crate::util::ClientTransmitter;
use crate::{rbac, ExecuteContext, ExecuteResponseKind};

// DO NOT make this visible in any way, i.e. do not add any version of
// `pub` to this mod. The inner `sequence_X` methods are hidden in this
// private module to prevent anyone from calling them directly. All
// sequencing should be done through the `sequence_plan` method.
// This allows us to add catch-all logic that should be applied to all
// plans in `sequence_plan` and guarantee that no caller can circumvent
// that logic.
//
// The two exceptions are:
//
// - Creating a role during connection startup. In this scenario, the session has not been properly
// initialized and we need to skip directly to creating role. We have a specific method,
// `sequence_create_role_for_startup` for this purpose.
// - Methods that continue the execution of some plan that was being run asynchronously, such as
// `sequence_peek_stage` and `sequence_create_connection_stage_finish`.
mod alter_set_cluster;
mod cluster;
mod inner;
mod linked_cluster;

impl Coordinator {
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn sequence_plan(
        &mut self,
        mut ctx: ExecuteContext,
        plan: Plan,
        resolved_ids: ResolvedIds,
    ) {
        event!(Level::TRACE, plan = format!("{:?}", plan));
        let mut responses = ExecuteResponse::generated_from(PlanKind::from(&plan));
        responses.push(ExecuteResponseKind::Canceled);
        ctx.tx_mut().set_allowed(responses);

        let session_catalog = self.catalog.for_session(ctx.session());

        if let Err(e) = introspection::user_privilege_hack(
            &session_catalog,
            ctx.session(),
            &plan,
            &resolved_ids,
        ) {
            return ctx.retire(Err(e));
        }
        if let Err(e) = introspection::check_cluster_restrictions(&session_catalog, &plan) {
            return ctx.retire(Err(e));
        }

        // If our query only depends on system tables, a LaunchDarkly flag is enabled, and a
        // session var is set, then we automatically run the query on the mz_introspection cluster.
        let target_cluster =
            introspection::auto_run_on_introspection(&self.catalog, ctx.session(), &plan);
        let target_cluster_id = self
            .catalog()
            .resolve_target_cluster(target_cluster, ctx.session())
            .ok()
            .map(|cluster| cluster.id());

        if let Err(e) = rbac::check_plan(
            self,
            &session_catalog,
            ctx.session(),
            &plan,
            target_cluster_id,
            &resolved_ids,
        ) {
            return ctx.retire(Err(e));
        }

        match plan {
            Plan::CreateSource(plan) => {
                let source_id = return_if_err!(self.catalog_mut().allocate_user_id().await, ctx);
                let result = self
                    .sequence_create_source(
                        ctx.session_mut(),
                        vec![CreateSourcePlans {
                            source_id,
                            plan,
                            resolved_ids,
                        }],
                    )
                    .await;
                ctx.retire(result);
            }
            Plan::CreateSources(plans) => {
                assert!(
                    resolved_ids.0.is_empty(),
                    "each plan has separate resolved_ids"
                );
                let result = self.sequence_create_source(ctx.session_mut(), plans).await;
                ctx.retire(result);
            }
            Plan::CreateConnection(plan) => {
                self.sequence_create_connection(ctx, plan, resolved_ids)
                    .await;
            }
            Plan::CreateDatabase(plan) => {
                let result = self.sequence_create_database(ctx.session_mut(), plan).await;
                ctx.retire(result);
            }
            Plan::CreateSchema(plan) => {
                let result = self.sequence_create_schema(ctx.session_mut(), plan).await;
                ctx.retire(result);
            }
            Plan::CreateRole(plan) => {
                let result = self
                    .sequence_create_role(Some(ctx.session().conn_id()), plan)
                    .await;
                if result.is_ok() {
                    self.maybe_send_rbac_notice(ctx.session());
                }
                ctx.retire(result);
            }
            Plan::CreateCluster(plan) => {
                let result = self.sequence_create_cluster(ctx.session(), plan).await;
                ctx.retire(result);
            }
            Plan::CreateClusterProfile(plan) => {
                let result = self
                    .sequence_create_cluster_profile(ctx.session(), plan)
                    .await;
                ctx.retire(result);
            }
            Plan::CreateClusterReplica(plan) => {
                let result = self
                    .sequence_create_cluster_replica(ctx.session(), plan)
                    .await;
                ctx.retire(result);
            }
            Plan::CreateTable(plan) => {
                let result = self
                    .sequence_create_table(ctx.session_mut(), plan, resolved_ids)
                    .await;
                ctx.retire(result);
            }
            Plan::CreateSecret(plan) => {
                let result = self.sequence_create_secret(ctx.session_mut(), plan).await;
                ctx.retire(result);
            }
            Plan::CreateSink(plan) => {
                self.sequence_create_sink(ctx, plan, resolved_ids).await;
            }
            Plan::CreateView(plan) => {
                let result = self
                    .sequence_create_view(ctx.session_mut(), plan, resolved_ids)
                    .await;
                ctx.retire(result);
            }
            Plan::CreateMaterializedView(plan) => {
                let result = self
                    .sequence_create_materialized_view(ctx.session_mut(), plan, resolved_ids)
                    .await;
                ctx.retire(result);
            }
            Plan::CreateIndex(plan) => {
                let result = self
                    .sequence_create_index(ctx.session_mut(), plan, resolved_ids)
                    .await;
                ctx.retire(result);
            }
            Plan::CreateType(plan) => {
                let result = self
                    .sequence_create_type(ctx.session(), plan, resolved_ids)
                    .await;
                ctx.retire(result);
            }
            Plan::DropObjects(plan) => {
                let result = self.sequence_drop_objects(ctx.session_mut(), plan).await;
                ctx.retire(result);
            }
            Plan::DropOwned(plan) => {
                let result = self.sequence_drop_owned(ctx.session_mut(), plan).await;
                ctx.retire(result);
            }
            Plan::EmptyQuery => {
                ctx.retire(Ok(ExecuteResponse::EmptyQuery));
            }
            Plan::ShowAllVariables => {
                let result = self.sequence_show_all_variables(ctx.session());
                ctx.retire(result);
            }
            Plan::ShowVariable(plan) => {
                let result = self.sequence_show_variable(ctx.session(), plan);
                ctx.retire(result);
            }
            Plan::InspectShard(plan) => {
                // TODO: Ideally, this await would happen off the main thread.
                let result = self.sequence_inspect_shard(ctx.session(), plan).await;
                ctx.retire(result);
            }
            Plan::SetVariable(plan) => {
                let result = self.sequence_set_variable(ctx.session_mut(), plan);
                ctx.retire(result);
            }
            Plan::ResetVariable(plan) => {
                let result = self.sequence_reset_variable(ctx.session_mut(), plan);
                ctx.retire(result);
            }
            Plan::SetTransaction(plan) => {
                let result = self.sequence_set_transaction(ctx.session_mut(), plan);
                ctx.retire(result);
            }
            Plan::StartTransaction(plan) => {
                if matches!(
                    ctx.session().transaction(),
                    TransactionStatus::InTransaction(_)
                ) {
                    ctx.session()
                        .add_notice(AdapterNotice::ExistingTransactionInProgress);
                }
                let result = ctx.session_mut().start_transaction(
                    self.now_datetime(),
                    plan.access,
                    plan.isolation_level,
                );
                ctx.retire(result.map(|_| ExecuteResponse::StartedTransaction))
            }
            Plan::CommitTransaction(CommitTransactionPlan {
                ref transaction_type,
            })
            | Plan::AbortTransaction(AbortTransactionPlan {
                ref transaction_type,
            }) => {
                let action = match &plan {
                    Plan::CommitTransaction(_) => EndTransactionAction::Commit,
                    Plan::AbortTransaction(_) => EndTransactionAction::Rollback,
                    _ => unreachable!(),
                };
                if ctx.session().transaction().is_implicit() && !transaction_type.is_implicit() {
                    // In Postgres, if a user sends a COMMIT or ROLLBACK in an
                    // implicit transaction, a warning is sent warning them.
                    // (The transaction is still closed and a new implicit
                    // transaction started, though.)
                    ctx.session()
                        .add_notice(AdapterNotice::ExplicitTransactionControlInImplicitTransaction);
                }
                self.sequence_end_transaction(ctx, action);
            }
            Plan::Select(plan) => {
                self.sequence_peek(ctx, plan, target_cluster).await;
            }
            Plan::Subscribe(plan) => {
                let result = self
                    .sequence_subscribe(&mut ctx, plan, target_cluster)
                    .await;
                ctx.retire(result);
            }
            Plan::SideEffectingFunc(plan) => {
                ctx.retire(self.sequence_side_effecting_func(plan));
            }
            Plan::ShowCreate(plan) => {
                ctx.retire(Ok(Self::send_immediate_rows(vec![plan.row])));
            }
            Plan::ShowColumns(show_columns_plan) => {
                self.sequence_peek(ctx, show_columns_plan.select_plan, target_cluster)
                    .await;
            }
            Plan::CopyFrom(plan) => {
                let (tx, _, session, ctx_extra) = ctx.into_parts();
                tx.send(
                    Ok(ExecuteResponse::CopyFrom {
                        id: plan.id,
                        columns: plan.columns,
                        params: plan.params,
                        ctx_extra,
                    }),
                    session,
                );
            }
            Plan::ExplainPlan(plan) => {
                self.sequence_explain_plan(ctx, plan, target_cluster).await;
            }
            Plan::ExplainTimestamp(plan) => {
                self.sequence_explain_timestamp(ctx, plan, target_cluster);
            }
            Plan::Insert(plan) => {
                self.sequence_insert(ctx, plan).await;
            }
            Plan::ReadThenWrite(plan) => {
                self.sequence_read_then_write(ctx, plan).await;
            }
            Plan::AlterNoop(plan) => {
                ctx.retire(Ok(ExecuteResponse::AlteredObject(plan.object_type)));
            }
            Plan::AlterCluster(plan) => {
                let result = self.sequence_alter_cluster(ctx.session(), plan).await;
                ctx.retire(result);
            }
            Plan::AlterClusterProfile(plan) => {
                let result = self
                    .sequence_alter_cluster_profile(ctx.session(), plan)
                    .await;
                ctx.retire(result);
            }
            Plan::AlterClusterRename(plan) => {
                let result = self
                    .sequence_alter_cluster_rename(ctx.session(), plan)
                    .await;
                ctx.retire(result);
            }
            Plan::AlterClusterItemRename(plan) => {
                let result = self
                    .sequence_alter_cluster_replica_rename(ctx.session(), plan)
                    .await;
                ctx.retire(result);
            }
            Plan::AlterSetCluster(plan) => {
                let result = self.sequence_alter_set_cluster(ctx.session(), plan).await;
                ctx.retire(result);
            }
            Plan::AlterItemRename(plan) => {
                let result = self.sequence_alter_item_rename(ctx.session(), plan).await;
                ctx.retire(result);
            }
            Plan::AlterIndexSetOptions(plan) => {
                let result = self.sequence_alter_index_set_options(plan);
                ctx.retire(result);
            }
            Plan::AlterIndexResetOptions(plan) => {
                let result = self.sequence_alter_index_reset_options(plan);
                ctx.retire(result);
            }
            Plan::AlterRole(plan) => {
                let result = self.sequence_alter_role(ctx.session(), plan).await;
                if result.is_ok() {
                    self.maybe_send_rbac_notice(ctx.session());
                }
                ctx.retire(result);
            }
            Plan::AlterSecret(plan) => {
                let result = self.sequence_alter_secret(ctx.session(), plan).await;
                ctx.retire(result);
            }
            Plan::AlterSink(plan) => {
                let result = self.sequence_alter_sink(ctx.session(), plan).await;
                ctx.retire(result);
            }
            Plan::PurifiedAlterSource {
                alter_source,
                subsources,
            } => {
                let result = self
                    .sequence_alter_source(ctx.session_mut(), alter_source, subsources)
                    .await;
                ctx.retire(result);
            }
            Plan::AlterSource(_) => {
                unreachable!("ALTER SOURCE must be purified")
            }
            Plan::AlterSystemSet(plan) => {
                let result = self.sequence_alter_system_set(ctx.session(), plan).await;
                ctx.retire(result);
            }
            Plan::AlterSystemReset(plan) => {
                let result = self.sequence_alter_system_reset(ctx.session(), plan).await;
                ctx.retire(result);
            }
            Plan::AlterSystemResetAll(plan) => {
                let result = self
                    .sequence_alter_system_reset_all(ctx.session(), plan)
                    .await;
                ctx.retire(result);
            }
            Plan::DiscardTemp => {
                self.drop_temp_items(ctx.session().conn_id()).await;
                ctx.retire(Ok(ExecuteResponse::DiscardedTemp));
            }
            Plan::DiscardAll => {
                let ret = if let TransactionStatus::Started(_) = ctx.session().transaction() {
                    self.clear_transaction(ctx.session_mut());
                    self.drop_temp_items(ctx.session().conn_id()).await;
                    ctx.session_mut().reset();
                    Ok(ExecuteResponse::DiscardedAll)
                } else {
                    Err(AdapterError::OperationProhibitsTransaction(
                        "DISCARD ALL".into(),
                    ))
                };
                ctx.retire(ret);
            }
            Plan::Declare(plan) => {
                let param_types = vec![];
                self.declare(ctx, plan.name, plan.stmt, plan.sql, param_types);
            }
            Plan::Fetch(FetchPlan {
                name,
                count,
                timeout,
            }) => {
                let ctx_extra = std::mem::take(ctx.extra_mut());
                ctx.retire(Ok(ExecuteResponse::Fetch {
                    name,
                    count,
                    timeout,
                    ctx_extra,
                }));
            }
            Plan::Close(plan) => {
                if ctx.session_mut().remove_portal(&plan.name) {
                    ctx.retire(Ok(ExecuteResponse::ClosedCursor));
                } else {
                    ctx.retire(Err(AdapterError::UnknownCursor(plan.name)));
                }
            }
            Plan::Prepare(plan) => {
                if ctx
                    .session()
                    .get_prepared_statement_unverified(&plan.name)
                    .is_some()
                {
                    ctx.retire(Err(AdapterError::PreparedStatementExists(plan.name)));
                } else {
                    ctx.session_mut().set_prepared_statement(
                        plan.name,
                        Some(plan.stmt),
                        plan.sql,
                        plan.desc,
                        self.catalog().transient_revision(),
                        self.now(),
                    );
                    ctx.retire(Ok(ExecuteResponse::Prepare));
                }
            }
            Plan::Execute(plan) => {
                match self.sequence_execute(ctx.session_mut(), plan) {
                    Ok(portal_name) => {
                        let (tx, _, session, extra) = ctx.into_parts();
                        self.internal_cmd_tx
                            .send(Message::Command(Command::Execute {
                                portal_name,
                                session,
                                tx: tx.take(),
                                outer_ctx_extra: Some(extra),
                                span: tracing::Span::none(),
                            }))
                            .expect("sending to self.internal_cmd_tx cannot fail");
                    }
                    Err(err) => ctx.retire(Err(err)),
                };
            }
            Plan::Deallocate(plan) => match plan.name {
                Some(name) => {
                    if ctx.session_mut().remove_prepared_statement(&name) {
                        ctx.retire(Ok(ExecuteResponse::Deallocate { all: false }));
                    } else {
                        ctx.retire(Err(AdapterError::UnknownPreparedStatement(name)));
                    }
                }
                None => {
                    ctx.session_mut().remove_all_prepared_statements();
                    ctx.retire(Ok(ExecuteResponse::Deallocate { all: true }));
                }
            },
            Plan::Raise(RaisePlan { severity }) => {
                ctx.session()
                    .add_notice(AdapterNotice::UserRequested { severity });
                ctx.retire(Ok(ExecuteResponse::Raised));
            }
            Plan::RotateKeys(RotateKeysPlan { id }) => {
                let result = self.sequence_rotate_keys(ctx.session(), id).await;
                ctx.retire(result);
            }
            Plan::GrantPrivileges(plan) => {
                let result = self
                    .sequence_grant_privileges(ctx.session_mut(), plan)
                    .await;
                ctx.retire(result);
            }
            Plan::RevokePrivileges(plan) => {
                let result = self
                    .sequence_revoke_privileges(ctx.session_mut(), plan)
                    .await;
                ctx.retire(result);
            }
            Plan::AlterDefaultPrivileges(plan) => {
                let result = self
                    .sequence_alter_default_privileges(ctx.session_mut(), plan)
                    .await;
                ctx.retire(result);
            }
            Plan::GrantRole(plan) => {
                let result = self.sequence_grant_role(ctx.session_mut(), plan).await;
                ctx.retire(result);
            }
            Plan::RevokeRole(plan) => {
                let result = self.sequence_revoke_role(ctx.session_mut(), plan).await;
                ctx.retire(result);
            }
            Plan::AlterOwner(plan) => {
                let result = self.sequence_alter_owner(ctx.session_mut(), plan).await;
                ctx.retire(result);
            }
            Plan::ReassignOwned(plan) => {
                let result = self.sequence_reassign_owned(ctx.session_mut(), plan).await;
                ctx.retire(result);
            }
            Plan::ValidateConnection(plan) => {
                let connection_context = self.connection_context.clone();
                mz_ore::task::spawn(|| "coord::validate_connection", async move {
                    let res = match plan.connection.validate(plan.id, &connection_context).await {
                        Ok(()) => Ok(ExecuteResponse::ValidatedConnection),
                        Err(err) => Err(err.into()),
                    };
                    ctx.retire(res);
                });
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) async fn sequence_execute_single_statement_transaction(
        &mut self,
        ctx: ExecuteContext,
        stmt: Statement<Raw>,
        params: Params,
    ) {
        // Put the session into single statement implicit so anything can execute.
        let (tx, internal_cmd_tx, mut session, extra) = ctx.into_parts();
        assert!(matches!(session.transaction(), TransactionStatus::Default));
        session.start_transaction_single_stmt(self.now_datetime());
        let conn_id = session.conn_id().unhandled();

        // Execute the saved statement in a temp transmitter so we can run COMMIT.
        let (sub_tx, sub_rx) = oneshot::channel();
        let sub_ct = ClientTransmitter::new(sub_tx, self.internal_cmd_tx.clone());
        let sub_ctx = ExecuteContext::from_parts(sub_ct, internal_cmd_tx, session, extra);
        self.handle_execute_inner(stmt, params, sub_ctx).await;

        // The response can need off-thread processing. Wait for it elsewhere so the coordinator can
        // continue processing.
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        mz_ore::task::spawn(
            || format!("execute_single_statement:{conn_id}"),
            async move {
                let Ok(Response { result, session }) = sub_rx.await else {
                    // Coordinator went away.
                    return;
                };
                let (sub_tx, sub_rx) = oneshot::channel();
                let _ = internal_cmd_tx.send(Message::Command(Command::Commit {
                    action: EndTransactionAction::Commit,
                    session,
                    tx: sub_tx,
                    otel_ctx: OpenTelemetryContext::obtain(),
                }));
                let Ok(commit_response) = sub_rx.await else {
                    // Coordinator went away.
                    return;
                };
                assert!(matches!(
                    commit_response.session.transaction(),
                    TransactionStatus::Default
                ));
                // The fake, generated response was already sent to the user and we don't need to
                // ever send an `Ok(result)` to the user, because they are expecting a response from
                // a `COMMIT`. So, always send the `COMMIT`'s result if the original statement
                // succeeded. If it failed, we can send an error and don't need to wrap it or send a
                // later COMMIT or ROLLBACK.
                let result = match (result, commit_response.result) {
                    (Ok(_), commit) => commit,
                    (Err(result), _) => Err(result),
                };
                // We ignore the resp.result because it's not clear what to do if it failed since we
                // can only send a single ExecuteResponse to tx.
                tx.send(result, commit_response.session);
            },
        );
    }

    /// Creates a role during connection startup.
    ///
    /// This should not be called from anywhere except connection startup.
    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) async fn sequence_create_role_for_startup(
        &mut self,
        plan: CreateRolePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        // This does not set conn_id because it's not yet in active_conns. That is because we can't
        // make a ConnMeta until we have a role id which we don't have until after the catalog txn
        // is committed. Passing None here means the audit log won't have a user set in the event's
        // user field. This seems fine because it is indeed the system that is creating this role,
        // not a user request, and the user name is still recorded in the plan, so we aren't losing
        // information.
        self.sequence_create_role(None, plan).await
    }

    pub(crate) fn sequence_explain_timestamp_finish(
        &mut self,
        ctx: &mut ExecuteContext,
        format: ExplainFormat,
        cluster_id: ClusterId,
        optimized_plan: OptimizedMirRelationExpr,
        id_bundle: CollectionIdBundle,
        real_time_recency_ts: Option<Timestamp>,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.sequence_explain_timestamp_finish_inner(
            ctx.session_mut(),
            format,
            cluster_id,
            optimized_plan,
            id_bundle,
            real_time_recency_ts,
        )
    }

    pub(crate) fn allocate_transient_id(&mut self) -> Result<GlobalId, AdapterError> {
        let id = self.transient_id_counter;
        if id == u64::MAX {
            coord_bail!("id counter overflows i64");
        }
        self.transient_id_counter += 1;
        Ok(GlobalId::Transient(id))
    }

    fn maybe_send_rbac_notice(&self, session: &Session) {
        if !rbac::is_rbac_enabled_for_session(self.catalog.system_config(), session) {
            if !self.catalog.system_config().enable_ld_rbac_checks() {
                session.add_notice(AdapterNotice::RbacSystemDisabled);
            } else {
                session.add_notice(AdapterNotice::RbacUserDisabled);
            }
        }
    }

    pub(crate) fn insert_constant(
        catalog: &Catalog,
        session: &mut Session,
        id: GlobalId,
        constants: MirRelationExpr,
    ) -> Result<ExecuteResponse, AdapterError> {
        // Insert can be queued, so we need to re-verify the id exists.
        let desc = match catalog.try_get_entry(&id) {
            Some(table) => {
                table.desc(&catalog.resolve_full_name(table.name(), Some(session.conn_id())))?
            }
            None => {
                return Err(AdapterError::SqlCatalog(CatalogError::UnknownItem(
                    id.to_string(),
                )))
            }
        };

        match constants.as_const() {
            Some((rows, ..)) => {
                let rows = rows.clone()?;
                for (row, _) in &rows {
                    for (i, datum) in row.iter().enumerate() {
                        desc.constraints_met(i, &datum)?;
                    }
                }
                let diffs_plan = plan::SendDiffsPlan {
                    id,
                    updates: rows,
                    kind: MutationKind::Insert,
                    returning: Vec::new(),
                    max_result_size: catalog.system_config().max_result_size(),
                };
                Self::send_diffs(session, diffs_plan)
            }
            None => panic!(
                "tried using sequence_insert_constant on non-constant MirRelationExpr {:?}",
                constants
            ),
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn send_diffs(
        session: &mut Session,
        mut plan: plan::SendDiffsPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let affected_rows = {
            let mut affected_rows = Diff::from(0);
            let mut all_positive_diffs = true;
            // If all diffs are positive, the number of affected rows is just the
            // sum of all unconsolidated diffs.
            for (_, diff) in plan.updates.iter() {
                if *diff < 0 {
                    all_positive_diffs = false;
                    break;
                }

                affected_rows += diff;
            }

            if !all_positive_diffs {
                // Consolidate rows. This is useful e.g. for an UPDATE where the row
                // doesn't change, and we need to reflect that in the number of
                // affected rows.
                differential_dataflow::consolidation::consolidate(&mut plan.updates);

                affected_rows = 0;
                // With retractions, the number of affected rows is not the number
                // of rows we see, but the sum of the absolute value of their diffs,
                // e.g. if one row is retracted and another is added, the total
                // number of rows affected is 2.
                for (_, diff) in plan.updates.iter() {
                    affected_rows += diff.abs();
                }
            }

            usize::try_from(affected_rows).expect("positive isize must fit")
        };
        event!(
            Level::TRACE,
            affected_rows,
            id = format!("{:?}", plan.id),
            kind = format!("{:?}", plan.kind),
            updates = plan.updates.len(),
            returning = plan.returning.len(),
        );

        session.add_transaction_ops(TransactionOps::Writes(vec![WriteOp {
            id: plan.id,
            rows: plan.updates,
        }]))?;
        if !plan.returning.is_empty() {
            let finishing = RowSetFinishing {
                order_by: Vec::new(),
                limit: None,
                offset: 0,
                project: (0..plan.returning[0].0.iter().count()).collect(),
            };
            return match finishing.finish(plan.returning, plan.max_result_size) {
                Ok(rows) => Ok(Self::send_immediate_rows(rows)),
                Err(e) => Err(AdapterError::ResultSize(e)),
            };
        }
        Ok(match plan.kind {
            MutationKind::Delete => ExecuteResponse::Deleted(affected_rows),
            MutationKind::Insert => ExecuteResponse::Inserted(affected_rows),
            MutationKind::Update => ExecuteResponse::Updated(affected_rows / 2),
        })
    }
}
