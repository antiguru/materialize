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
        _ctx: &mut TransformCtx,
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
        // Unlike the logical pass, this calls `optimize` (commit_wcoj=true) so
        // the e-graph's WcoJoin choice is lowered to a live DeltaQuery.
        let optimized = crate::eqsat::optimize(relation.clone());
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
