// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A `Transform` wrapper over [`crate::eqsat::optimize`], registered in the logical
//! optimizer behind the `enable_eqsat_optimizer` feature flag.

use mz_expr::MirRelationExpr;

use crate::{Transform, TransformCtx, TransformError};

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
        // Hard arity guard at the live boundary: the pass is equivalence
        // preserving and must never change arity. Optimize a clone, and adopt
        // the result only if its arity matches. On any mismatch, leave the
        // input untouched (a no-op is always sound) and log loudly;
        // `soft_panic_or_log!` panics in debug/test builds to surface the bug
        // and logs in release rather than emitting a malformed plan.
        let input_arity = relation.arity();
        let optimized = crate::eqsat::optimize(relation.clone());
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
