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
//! This is the structural inverse of [`crate::eqsat::lower::lower`]. The
//! round-trip is semantics-preserving, scalar-canonicalizing, and
//! MFP-canonicalizing rather than byte-identical: lower reduces every scalar
//! payload, and the post-raise [`coalesce_mfp`] pass coalesces each maximal
//! Map/Filter/Project run into canonical Map-then-Filter-then-Project form
//! (reusing the production `CanonicalizeMfp` machinery). Together,
//! `raise(lower(x))` returns `x` with scalars in `MirScalarExpr::reduce`
//! canonical form and with MFP runs in `MapFilterProject` canonical form.

use mz_expr::visit::VisitChildren;
use mz_expr::{LocalId, MapFilterProject, MirRelationExpr, MirScalarExpr};
use mz_ore::cast::CastFrom;
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::{ReprRelationType, ReprScalarType};

use crate::canonicalize_mfp::CanonicalizeMfp;
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

/// Coalesce each maximal Map/Filter/Project run in `expr` into canonical
/// Map-then-Filter-then-Project form, bottom up.
///
/// Reuses `MapFilterProject::extract_non_errors_from_expr_mut`,
/// `MapFilterProject::optimize`, and `CanonicalizeMfp::rebuild_mfp` (which
/// includes `fusion::filter::Filter::action` for predicate canonicalization).
/// This produces output identical to what the production `CanonicalizeMfp`
/// transform emits, so eqsat fully subsumes that transform.
/// Runs only on Map/Filter/Project nodes, so it never touches Join
/// implementations or disturbs the logical-phase joins-Unimplemented contract.
pub(crate) fn coalesce_mfp(expr: &mut MirRelationExpr) {
    // Guard: the eqsat pass can produce Map-then-Project sequences where the
    // Project references a column beyond the base arity (e.g. a Map's own
    // output column folded back via `map_columns_to_projection`). Such chains
    // are invalid MIR, and `extract_non_errors_from_expr_mut` panics on them.
    // Skip coalescing for chains that fail this check; the downstream pipeline
    // (CanonicalizeMfp) will handle them after type information is available.
    if !mfp_chain_valid(expr) {
        // Still recurse into children of the base so inner valid chains are
        // coalesced. Walk past the M/F/P prefix to find the non-MFP base and
        // recurse into its children directly.
        coalesce_mfp_children_of_base(expr);
        return;
    }
    // Extract the maximal error-free MFP run at the root of `expr`, stripping
    // all Map/Filter/Project layers down to the non-MFP base. This matches the
    // approach used in `CanonicalizeMfp::action`: extract first, then recurse
    // into the base's children, then rebuild. Extracting before recursing avoids
    // visiting intermediate M/F/P nodes as if they were independent roots (which
    // would cause double-processing and mismatched column arities).
    let mut mfp = MapFilterProject::extract_non_errors_from_expr_mut(expr);
    mfp.optimize();
    // Recurse into the children of the base (non-MFP node now in `expr`).
    // `VisitChildren::visit_mut_children` visits direct children only, so each
    // child recursion independently coalesces its own MFP run.
    expr.visit_mut_children(coalesce_mfp);
    // Rebuild the optimized MFP on top of the now-coalesced base using the
    // production canonicalizer, which also runs Filter::action (predicate
    // canonicalization: sort, dedup, split conjuncts, reduce).
    CanonicalizeMfp::rebuild_mfp(mfp, expr);
}

/// Returns true iff the Map/Filter/Project chain rooted at `expr` has
/// consistent column arities (each Project's output indices are within bounds
/// for the arity that the chain produces up to that point). An invalid chain
/// indicates the eqsat pass produced a plan with out-of-scope column references,
/// which `extract_non_errors_from_expr_mut` cannot handle without panicking.
fn mfp_chain_valid(expr: &MirRelationExpr) -> bool {
    // Walk the chain recursively, returning the output arity or None on OOB.
    fn check(expr: &MirRelationExpr) -> Option<usize> {
        match expr {
            MirRelationExpr::Map { input, scalars }
                if scalars.iter().all(|s| !s.is_literal_err()) =>
            {
                let inner_arity = check(input)?;
                Some(inner_arity + scalars.len())
            }
            MirRelationExpr::Filter { input, predicates }
                if predicates.iter().all(|p| !p.is_literal_err()) =>
            {
                check(input)
            }
            MirRelationExpr::Project { input, outputs } => {
                let inner_arity = check(input)?;
                // All output indices must be within the arity produced by the
                // inner chain (which includes any mapped columns from Map nodes
                // below).
                if outputs.iter().all(|&o| o < inner_arity) {
                    Some(outputs.len())
                } else {
                    None
                }
            }
            x => Some(x.arity()),
        }
    }
    check(expr).is_some()
}

/// Walk past the Map/Filter/Project prefix of `expr` and recurse
/// `coalesce_mfp` into the children of the non-MFP base node.
/// Used when the chain is invalid and we skip extraction but still want to
/// coalesce inner sub-trees.
fn coalesce_mfp_children_of_base(expr: &mut MirRelationExpr) {
    match expr {
        MirRelationExpr::Map { input, scalars } if scalars.iter().all(|s| !s.is_literal_err()) => {
            coalesce_mfp_children_of_base(input);
        }
        MirRelationExpr::Filter { input, predicates }
            if predicates.iter().all(|p| !p.is_literal_err()) =>
        {
            coalesce_mfp_children_of_base(input);
        }
        MirRelationExpr::Project { input, .. } => {
            coalesce_mfp_children_of_base(input);
        }
        base => {
            base.visit_mut_children(coalesce_mfp);
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
    use mz_expr::{AccessStrategy, Id, LocalId, MirRelationExpr, MirScalarExpr, func};
    use mz_repr::{ReprRelationType, ReprScalarType};

    use crate::eqsat::lower::lower;

    use super::{coalesce_mfp, raise};

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
        // original Constant verbatim from the interner. The predicate must be
        // boolean-typed so that coalesce_mfp can call Filter::action without
        // triggering the boolean-type assertion in canonicalize_predicates. Use
        // a column-equality predicate (#0 = #1) which is boolean and stable
        // under canonicalize_predicates (not reducible to a constant).
        let pred = MirScalarExpr::column(0).call_binary(MirScalarExpr::column(1), func::Eq);
        let r = base(2).filter(vec![pred]);
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

    #[mz_ore::test]
    fn coalesce_fuses_nested_filter_map_filter() {
        // Build a Rel that represents filter(p1, map(s, filter(p2, base))).
        // This is a non-canonical MFP run: two Filters with a Map between them.
        // After raise + coalesce_mfp, the result must be canonical:
        //   at most one Map, one Filter, one Project per contiguous run,
        //   in Map-then-Filter-then-Project order.
        use crate::eqsat::ir::{EScalar, Rel};

        let base_rel = Rel::Constant { card: 0, arity: 2 };
        // filter(p2 = is_null(#0), base) -- boolean predicate, non-trivial
        let p2 = EScalar::plain(MirScalarExpr::column(0).call_is_null());
        let after_inner_filter = Rel::Filter {
            input: Box::new(base_rel),
            predicates: vec![p2],
        };
        // map(s = #1, filter(p2, base))  -- appends column #2 = #1
        let s = EScalar::plain(MirScalarExpr::column(1));
        let after_map = Rel::Map {
            input: Box::new(after_inner_filter),
            scalars: vec![s],
        };
        // filter(p1 = is_null(#1), map(s, filter(p2, base))) -- second boolean pred
        let p1 = EScalar::plain(MirScalarExpr::column(1).call_is_null());
        let rel = Rel::Filter {
            input: Box::new(after_map),
            predicates: vec![p1],
        };

        // Raise the non-canonical tree then coalesce it.
        let mut result = raise(&rel, false);
        coalesce_mfp(&mut result);

        // Walk the result tree and count contiguous M/F/P layers.
        // Canonical order is: Map? then Filter? then Project? over a non-MFP base.
        // We verify that the filter and map are not interleaved (i.e., no Filter
        // directly below a Map below another Filter).
        fn is_filter(e: &MirRelationExpr) -> bool {
            matches!(e, MirRelationExpr::Filter { .. })
        }
        fn is_map(e: &MirRelationExpr) -> bool {
            matches!(e, MirRelationExpr::Map { .. })
        }

        // The outermost node must NOT be a Filter sitting on top of a Map
        // sitting on top of a Filter — that is the non-canonical pattern.
        // After coalescing the two filters fuse, so at most one Filter survives.
        let mut filter_count = 0usize;
        let mut map_count = 0usize;
        let mut e = &result;
        loop {
            if is_filter(e) {
                filter_count += 1;
                match e {
                    MirRelationExpr::Filter { input, .. } => e = input,
                    _ => unreachable!(),
                }
            } else if is_map(e) {
                map_count += 1;
                match e {
                    MirRelationExpr::Map { input, .. } => e = input,
                    _ => unreachable!(),
                }
            } else {
                break;
            }
        }
        // The two filters must have been fused into one: at most one Filter and
        // one Map in the contiguous top-level MFP run.
        assert!(
            filter_count <= 1,
            "expected at most 1 Filter in MFP run after coalescing, got {filter_count}"
        );
        assert!(
            map_count <= 1,
            "expected at most 1 Map in MFP run after coalescing, got {map_count}"
        );
    }
}
