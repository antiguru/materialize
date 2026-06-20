// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `Transform` wrappers over the eqsat optimizer, registered in the logical and
//! physical optimizers behind per-phase feature flags.

use mz_expr::MirRelationExpr;
use mz_expr::MirScalarExpr;
use mz_repr::GlobalId;
use std::collections::BTreeMap;

use crate::{Transform, TransformCtx, TransformError};

/// Maximum input plan size (node count) the pass will attempt. Equality
/// saturation explores a combinatorial space; on large plans the e-graph blows
/// up (tens of seconds per object observed on builtin indexes). Skipping large
/// plans keeps catalog bootstrap and optimization within their time budgets. A
/// skipped plan is a sound no-op. The cap is deliberately generous so the vast
/// majority of user and builtin plans still run through the pass.
const MAX_PLAN_SIZE: usize = 200;

/// Runs the equality-saturation pass as a `Transform`.
#[derive(Debug)]
pub struct EqSatTransform;

impl Transform for EqSatTransform {
    fn name(&self) -> &'static str {
        "EqSatTransform"
    }

    fn actually_perform_transform(
        &self,
        relation: &mut MirRelationExpr,
        _ctx: &mut TransformCtx,
    ) -> Result<(), TransformError> {
        // Skip plans above the size cap: saturation cost is superlinear in plan
        // size and a large plan can take tens of seconds. A no-op is sound.
        let plan_size = relation.size();
        if plan_size > MAX_PLAN_SIZE {
            return Ok(());
        }
        // Hard arity guard at the live boundary: the pass is equivalence
        // preserving and must never change arity. Optimize a clone, and adopt
        // the result only if its arity matches. On any mismatch, leave the
        // input untouched (a no-op is always sound) and log loudly;
        // `soft_panic_or_log!` panics in debug/test builds to surface the bug
        // and logs in release rather than emitting a malformed plan.
        let input_arity = relation.arity();
        // The pass runs in the logical optimizer, so its output must carry only
        // `Unimplemented` joins: the immediately following ProjectionPushdown
        // (run with `include_joins`) panics on a filled-in implementation, and
        // physical join planning expects to choose the implementation itself.
        // `optimize_logical` therefore emits worst-case-optimal joins as plain
        // `Unimplemented` joins rather than committing them to `DeltaQuery`. The
        // delta commitment (the experiment's offline payoff) is exercised by the
        // direct `optimize` callers in tests, not here.
        let optimized = crate::eqsat::optimize_logical(relation.clone());
        if optimized.arity() == input_arity {
            *relation = optimized;
        } else {
            mz_ore::soft_panic_or_log!(
                "eqsat optimize changed arity ({} -> {}); leaving the plan unchanged",
                input_arity,
                optimized.arity(),
            );
        }
        Ok(())
    }
}

/// Runs the equality-saturation pass in the physical optimizer, committing the
/// WcoJoin-to-DeltaQuery decision.
///
/// Placement contract: runs after `fixpoint_physical_01` and before
/// `LiteralConstraints`/`JoinImplementation`. At that point joins are still
/// `Unimplemented` (the ProjectionPushdown inside `fixpoint_physical_01` that
/// panics on filled-in implementations has already run). The committed
/// `DeltaQuery` survives `JoinImplementation` because that transform only
/// replans `Unimplemented` and `Differential` joins.
#[derive(Debug)]
pub struct PhysicalEqSatTransform;

impl Transform for PhysicalEqSatTransform {
    fn name(&self) -> &'static str {
        "PhysicalEqSatTransform"
    }

    fn actually_perform_transform(
        &self,
        relation: &mut MirRelationExpr,
        ctx: &mut TransformCtx,
    ) -> Result<(), TransformError> {
        // Same size cap as the logical pass: saturation cost is superlinear and
        // a large plan can take tens of seconds.
        let plan_size = relation.size();
        if plan_size > MAX_PLAN_SIZE {
            return Ok(());
        }
        // Hard arity guard: optimize a clone, adopt only if arity is preserved.
        // On any mismatch, leave the input untouched and log loudly.
        let input_arity = relation.arity();
        // Build index availability from ctx.indexes so the cost model does not
        // charge the arrangement-build memory term for join inputs that are
        // already arranged by an available index.  The physical pass has access
        // to the real index oracle; the logical pass uses empty availability.
        let available = build_availability(relation, ctx.indexes);
        // Unlike the logical pass, this calls optimize_with_availability
        // (commit_wcoj=true) so the e-graph's WcoJoin choice is lowered to a
        // live DeltaQuery with an index-aware cost model.
        let optimized = crate::eqsat::optimize_with_availability(relation.clone(), available);
        if optimized.arity() == input_arity {
            *relation = optimized;
        } else {
            mz_ore::soft_panic_or_log!(
                "eqsat physical optimize changed arity ({} -> {}); leaving the plan unchanged",
                input_arity,
                optimized.arity(),
            );
        }
        Ok(())
    }
}

/// Build an index-availability map from the oracle for all global `Get`s
/// reachable in `relation`.
///
/// Walks `relation` to collect every `GlobalId` referenced by a global `Get`,
/// then queries `oracle.indexes_on` for each to gather available index keys.
/// The result is passed to the cost model so indexed join inputs are not
/// charged the arrangement-build memory term.
fn build_availability(
    relation: &MirRelationExpr,
    oracle: &dyn crate::IndexOracle,
) -> BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>> {
    use mz_expr::Id;
    let mut gids: std::collections::BTreeSet<GlobalId> = std::collections::BTreeSet::new();
    relation.visit_pre(|e| {
        if let MirRelationExpr::Get {
            id: Id::Global(gid),
            ..
        } = e
        {
            gids.insert(*gid);
        }
    });
    let mut available: BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>> = BTreeMap::new();
    for gid in gids {
        let keys: Vec<Vec<MirScalarExpr>> = oracle
            .indexes_on(gid)
            .map(|(_idx_id, key)| key.to_vec())
            .collect();
        if !keys.is_empty() {
            available.insert(gid, keys);
        }
    }
    available
}
