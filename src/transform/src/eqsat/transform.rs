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
        // Hard equivalence guard at the live boundary: the pass is equivalence
        // and type preserving, so it must never change arity or any column's
        // scalar type. Optimize a clone, and adopt it only if both match (see
        // `adopt_if_type_preserving`). On any mismatch, leave the input
        // untouched (a no-op is always sound) and log loudly.
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
        adopt_if_type_preserving(relation, optimized, input_arity, "eqsat optimize");
        Ok(())
    }
}

/// Adopt `optimized` into `relation` only if it preserves the input's arity and
/// per-column scalar types. On any mismatch, leave `relation` untouched (a no-op
/// is always sound) and log loudly.
///
/// The pass is equivalence and type preserving, so its output must agree with
/// the input on arity and on each column's representation scalar type. It may
/// legitimately change nullability (a `Filter` strengthens columns to non-null,
/// a `Union` takes the least upper bound), so nullability is not compared. This
/// guard is the live boundary's last line of defense: a synthesized `Empty`
/// whose column types could not be derived at synthesis time falls back to a
/// placeholder type, and this check rejects such a plan rather than emitting a
/// wrong-typed one. `soft_panic_or_log!` panics in debug/test builds to surface
/// the bug and logs in release.
fn adopt_if_type_preserving(
    relation: &mut MirRelationExpr,
    optimized: MirRelationExpr,
    input_arity: usize,
    what: &str,
) {
    let output_arity = optimized.arity();
    if output_arity != input_arity {
        mz_ore::soft_panic_or_log!(
            "{what} changed arity ({} -> {}); leaving the plan unchanged",
            input_arity,
            output_arity,
        );
        return;
    }
    let input_types = relation.typ().column_types;
    let output_types = optimized.typ().column_types;
    let scalar_types_match = input_types
        .iter()
        .zip(output_types.iter())
        .all(|(a, b)| a.scalar_type == b.scalar_type);
    if scalar_types_match {
        *relation = optimized;
    } else {
        mz_ore::soft_panic_or_log!(
            "{what} changed column types ({:?} -> {:?}); leaving the plan unchanged",
            input_types,
            output_types,
        );
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
        // Hard equivalence guard: optimize a clone, adopt only if arity and
        // column scalar types are preserved (see `adopt_if_type_preserving`). On
        // any mismatch, leave the input untouched and log loudly.
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
        adopt_if_type_preserving(relation, optimized, input_arity, "eqsat physical optimize");
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
