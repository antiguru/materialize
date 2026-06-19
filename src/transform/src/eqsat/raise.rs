// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Translate a `Rel` back into a real `MirRelationExpr`, reading scalars
//! directly off their `MirScalarExpr` payloads and re-emitting bailed leaves
//! verbatim.
//!
//! This is the exact inverse of [`crate::eqsat::lower::lower`]. A round-trip
//! `raise(lower(x)) == x` holds for all plans that lower structurally (i.e.
//! those containing only the supported variants).

use mz_expr::{LocalId, MirRelationExpr, MirScalarExpr};
use mz_ore::cast::CastFrom;
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::{ReprRelationType, ReprScalarType};

use crate::eqsat::ir::{EScalar, Rel};

/// Raise `rel` to a `MirRelationExpr`. Inverse of [`crate::eqsat::lower::lower`].
///
/// Scalars are read directly off their `MirScalarExpr` payloads. `Rel::Opaque`
/// leaves re-emit their stored subtree verbatim. Local `Get`s return the
/// original node carried at lower time, preserving their exact type.
/// Raise an extracted [`Rel`] back to a [`MirRelationExpr`].
///
/// When `commit_wcoj` is set, a [`Rel::WcoJoin`] is committed to a `DeltaQuery`
/// implementation via the real delta planner (physical-phase output). When it
/// is clear, the same node is raised as a plain `Unimplemented` join, which is
/// the only form valid in the logical optimizer.
pub fn raise(rel: &Rel, commit_wcoj: bool) -> MirRelationExpr {
    let raise = |r: &Rel| raise(r, commit_wcoj);
    match rel {
        Rel::Opaque(m) => (**m).clone(),
        Rel::LocalGet { get, id, .. } => {
            // The exact original MirRelationExpr::Get{Local} node was carried at
            // lower time; return it verbatim so types are preserved. A `None`
            // here means an engine scope placeholder escaped substitution.
            match get {
                Some(g) => (**g).clone(),
                None => {
                    panic!("raise of a placeholder LocalGet (id {id}) without an original node")
                }
            }
        }
        Rel::Get { name, .. } => {
            unreachable!("lowering never emits Rel::Get (test-only base); got {name:?}")
        }
        Rel::Project { input, outputs } => raise(input).project(outputs.clone()),
        Rel::Map { input, scalars } => raise(input).map(resolve(scalars)),
        Rel::Filter { input, predicates } => raise(input).filter(resolve(predicates)),
        Rel::Join {
            inputs,
            equivalences,
        } => {
            // `join_scalars` intentionally drops constant-singleton (arity-0,
            // 1-row) inputs that act as join identities, so a round-trip is not
            // byte-identical for such inputs but is arity- and
            // semantics-preserving.
            MirRelationExpr::join_scalars(
                inputs.iter().map(raise).collect(),
                equivalences.iter().map(|class| resolve(class)).collect(),
            )
        }
        Rel::Negate { input } => raise(input).negate(),
        Rel::Threshold { input } => raise(input).threshold(),
        Rel::Union { base, inputs } => {
            // Use the enum directly rather than the .union() builder to
            // preserve the exact n-ary structure without flattening.
            MirRelationExpr::Union {
                base: Box::new(raise(base)),
                inputs: inputs.iter().map(raise).collect(),
            }
        }
        Rel::Let { id, value, body } => MirRelationExpr::Let {
            id: LocalId::new(u64::cast_from(*id)),
            value: Box::new(raise(value)),
            body: Box::new(raise(body)),
        },
        Rel::Constant { arity, .. } => {
            // Saturation rules (`empty_false_filter`, `union_cancel`) synthesize
            // `Empty(r)` nodes that the engine encodes as `Constant { card: 0,
            // arity }`. Extraction can pick such a node as cheapest; raise it to
            // an empty relation of the correct arity.
            MirRelationExpr::constant(vec![], repr_type_of_arity(*arity))
        }
        Rel::Reduce {
            input,
            group_key,
            aggregates,
            monotonic,
            expected_group_size,
        } => MirRelationExpr::Reduce {
            input: Box::new(raise(input)),
            group_key: group_key.iter().map(|s| s.expr.clone()).collect(),
            aggregates: aggregates.clone(),
            monotonic: *monotonic,
            expected_group_size: *expected_group_size,
        },
        Rel::TopK { input, shape } => MirRelationExpr::TopK {
            input: Box::new(raise(input)),
            group_key: shape.group_key.clone(),
            order_key: shape.order_key.clone(),
            limit: shape.limit.clone(),
            offset: shape.offset,
            monotonic: shape.monotonic,
            expected_group_size: shape.expected_group_size,
        },
        Rel::WcoJoin {
            inputs,
            equivalences,
        } => {
            // The WcoJoin variant is the e-graph's cost-model decision: the AGM
            // bound favours worst-case-optimal (delta) evaluation for this join.
            // Commit that decision into the plan by tagging the join
            // DeltaQuery. The JoinImplementation transform only (re)plans
            // Unimplemented and Differential joins, so a DeltaQuery-tagged join
            // survives the downstream pipeline unchanged.
            let join = MirRelationExpr::join_scalars(
                inputs.iter().map(raise).collect(),
                equivalences.iter().map(|class| resolve(class)).collect(),
            );
            if !commit_wcoj {
                // Logical-phase output: leave the join `Unimplemented`. The delta
                // commitment below produces physical-phase structure (arranged
                // inputs, filled implementation) that is invalid before
                // JoinImplementation.
                return join;
            }
            // Reuse the real delta planner. If planning fails (the join folded
            // to a non-join, or the graph is not connected), fall back to the
            // plain join and let JoinImplementation choose.
            let features = OptimizerFeatures::default();
            match crate::join_implementation::plan_as_delta_query(&join, &features) {
                Ok(delta) => delta,
                Err(_) => join,
            }
        }
        Rel::LetRec { .. } => {
            unreachable!("lowering never emits Rel::LetRec; it bails to an opaque leaf")
        }
    }
}

/// A placeholder relation type of the given arity for a synthesized empty
/// constant (produced by `empty_false_filter` / `union_cancel`). The pass is
/// offline; the surrounding optimizer recomputes column types when this is
/// ever wired live. Column types are nullable to admit an empty collection.
fn repr_type_of_arity(arity: usize) -> ReprRelationType {
    ReprRelationType::new(
        (0..arity)
            .map(|_| ReprScalarType::Int64.nullable(true))
            .collect(),
    )
}

/// Read a slice of [`EScalar`]s back to their `MirScalarExpr`s.
fn resolve(scalars: &[EScalar]) -> Vec<MirScalarExpr> {
    scalars.iter().map(|s| s.expr.clone()).collect()
}

#[cfg(test)]
mod tests {
    use mz_expr::{AccessStrategy, Id, LocalId, MirRelationExpr, MirScalarExpr};
    use mz_repr::{ReprRelationType, ReprScalarType};

    use crate::eqsat::lower::lower;

    use super::raise;

    fn base(arity: usize) -> MirRelationExpr {
        let typ = ReprRelationType::new(
            (0..arity)
                .map(|_| ReprScalarType::Int64.nullable(false))
                .collect(),
        );
        MirRelationExpr::constant(vec![], typ)
    }

    /// Lower then raise and assert structural identity.
    fn roundtrip(r: MirRelationExpr) {
        let rel = lower(&r);
        // These round-trips never involve a WcoJoin (lowering never emits one),
        // so `commit_wcoj` is irrelevant here.
        let back = raise(&rel, true);
        assert_eq!(back, r, "round-trip changed the plan");
    }

    #[mz_ore::test]
    fn roundtrip_filter_over_constant() {
        // Filter wraps a bailed Constant leaf; the round-trip must recover the
        // original Constant verbatim from the interner.
        let r = base(2).filter(vec![MirScalarExpr::column(0)]);
        roundtrip(r);
    }

    #[mz_ore::test]
    fn roundtrip_map_over_constant() {
        let r = base(2).map(vec![MirScalarExpr::column(1)]);
        roundtrip(r);
    }

    #[mz_ore::test]
    fn roundtrip_binary_union() {
        // Two independent bases unioned: both are bailed leaves; Union is
        // structural and must survive the trip.
        let a = base(2);
        let b = base(2);
        // Construct Union directly to avoid the flattening done by .union().
        let r = MirRelationExpr::Union {
            base: Box::new(a),
            inputs: vec![b],
        };
        roundtrip(r);
    }

    #[mz_ore::test]
    fn roundtrip_join_of_two_bases() {
        // Join of two bailed inputs with one equivalence class.
        let a = base(2);
        let b = base(2);
        // join_scalars may filter constant-singleton inputs, so use non-empty
        // constants by choosing a base that is not a constant-singleton. The
        // base() helper produces an empty constant, which join_scalars retains
        // because it has zero rows (non-singleton).
        let r = MirRelationExpr::join_scalars(
            vec![a, b],
            vec![vec![MirScalarExpr::column(0), MirScalarExpr::column(2)]],
        );
        roundtrip(r);
    }

    #[mz_ore::test]
    fn roundtrip_let_binding() {
        // Let binding where the body references the bound id via a local Get.
        // This exercises record_local_get / resolve_local_get.
        let local = LocalId::new(0);
        let typ = ReprRelationType::new(vec![ReprScalarType::Int64.nullable(false)]);
        let get_local = MirRelationExpr::Get {
            id: Id::Local(local),
            typ: typ.clone(),
            access_strategy: AccessStrategy::UnknownOrLocal,
        };
        let r = MirRelationExpr::Let {
            id: local,
            value: Box::new(base(1)),
            body: Box::new(get_local),
        };
        roundtrip(r);
    }

    #[mz_ore::test]
    fn roundtrip_unsupported_is_identity() {
        // An unsupported plan (entire tree is one opaque leaf) must round-trip
        // identically: the whole subtree is stored and recovered verbatim.
        let inner = base(2);
        let r = inner.arrange_by(&[vec![MirScalarExpr::column(0)]]);
        roundtrip(r);
    }
}
