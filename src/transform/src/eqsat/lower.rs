// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Translate a real `MirRelationExpr` into the prototype `Rel`. Supported
//! variants map structurally; every unsupported variant is carried verbatim in
//! a [`Rel::Opaque`] leaf, so the supported envelope around it still saturates.

use mz_expr::{Id, MirRelationExpr, MirScalarExpr};
use mz_ore::cast::CastFrom;
use mz_repr::ReprColumnType;

use crate::eqsat::ir::{EScalar, Rel, TopKShape};

/// Lower `expr` to a `Rel`, carrying scalars as real `MirScalarExpr`s and
/// bailing unsupported subtrees verbatim into [`Rel::Opaque`].
///
/// Supported variants lower structurally: `Project`, `Map`, `Filter`, `Join`,
/// `Reduce`, `TopK`, `Negate`, `Threshold`, `Union`, `Let`, and
/// `Get { Id::Local }`. Everything else (`Constant`, `Get { Id::Global }`,
/// `FlatMap`, `ArrangeBy`, `LetRec`) is bailed: the whole subtree is stored
/// verbatim in a `Rel::Opaque`, so raise re-emits it exactly.
pub fn lower(expr: &MirRelationExpr) -> Rel {
    use MirRelationExpr::*;
    match expr {
        Get {
            id: Id::Local(local),
            typ,
            ..
        } => {
            // LocalId wraps a pub(crate) u64; only From<&LocalId> for u64 is
            // available, so we go through that and then widen to usize.
            let id_usize = usize::cast_from(u64::from(local));
            // Carry the exact original node (with its real type) so raise can
            // reconstruct it verbatim.
            Rel::LocalGet {
                id: id_usize,
                arity: typ.arity(),
                get: Some(Box::new(expr.clone())),
            }
        }
        Project { input, outputs } => Rel::Project {
            input: Box::new(lower(input)),
            outputs: outputs.clone(),
        },
        Map { input, scalars } => {
            // Scalars in a Map reference input columns AND the columns produced
            // by earlier scalars in the same Map: scalar `i` may reference
            // columns `0..input_arity + i`. Fold each against the context built
            // from the input type extended with the types of preceding scalars,
            // so a later scalar's column reference stays in bounds.
            let col_types = input.typ().column_types;
            Rel::Map {
                input: Box::new(lower(input)),
                scalars: escalars_in_map_context(scalars, col_types),
            }
        }
        Filter { input, predicates } => {
            // Predicates reference input columns; use the input type to fold.
            let col_types = input.typ().column_types;
            Rel::Filter {
                input: Box::new(lower(input)),
                predicates: escalars_in_context(predicates, &col_types),
            }
        }
        Join {
            inputs,
            equivalences,
            ..
        } => {
            // Equivalence scalars reference the concatenated input column space
            // (input i's columns offset by the sum of prior input arities), so
            // fold them against the concatenation of input column types.
            let mut col_types = Vec::new();
            for inp in inputs {
                col_types.extend(inp.typ().column_types);
            }
            Rel::Join {
                inputs: inputs.iter().map(lower).collect(),
                equivalences: equivalences
                    .iter()
                    .map(|class| {
                        class
                            .iter()
                            .map(|e| reduced_escalar(e, &col_types))
                            .collect()
                    })
                    .collect(),
            }
        }
        Negate { input } => Rel::Negate {
            input: Box::new(lower(input)),
        },
        Threshold { input } => Rel::Threshold {
            input: Box::new(lower(input)),
        },
        Union { base, inputs } => Rel::Union {
            base: Box::new(lower(base)),
            inputs: inputs.iter().map(lower).collect(),
        },
        Let { id, value, body } => Rel::Let {
            id: usize::cast_from(u64::from(id)),
            value: Box::new(lower(value)),
            body: Box::new(lower(body)),
        },
        Reduce {
            input,
            group_key,
            aggregates,
            monotonic,
            expected_group_size,
        } => {
            // Group keys and aggregate input expressions reference the input
            // columns; fold them against the input type.
            let col_types = input.typ().column_types;
            let aggregates = aggregates
                .iter()
                .map(|agg| {
                    let mut agg = agg.clone();
                    agg.expr.reduce(&col_types);
                    agg
                })
                .collect();
            Rel::Reduce {
                input: Box::new(lower(input)),
                group_key: group_key
                    .iter()
                    .map(|e| reduced_escalar(e, &col_types))
                    .collect(),
                aggregates,
                monotonic: *monotonic,
                expected_group_size: *expected_group_size,
            }
        }
        TopK {
            input,
            group_key,
            order_key,
            limit,
            offset,
            monotonic,
            expected_group_size,
        } => {
            // The limit is a scalar evaluated over the input; fold it.
            let col_types = input.typ().column_types;
            let limit = limit.as_ref().map(|l| {
                let mut l = l.clone();
                l.reduce(&col_types);
                l
            });
            Rel::TopK {
                input: Box::new(lower(input)),
                shape: TopKShape {
                    group_key: group_key.clone(),
                    order_key: order_key.clone(),
                    limit,
                    offset: *offset,
                    monotonic: *monotonic,
                    expected_group_size: *expected_group_size,
                },
            }
        }
        // Unsupported: bail the entire subtree to an opaque leaf, carried
        // verbatim. Their payloads are type/row-sensitive and no rule rewrites
        // them, so an opaque leaf makes raising trivially exact. Global Get and
        // Constant are also bailed: Global Get carries a GlobalId not a
        // structural subexpression, and Constant carries row data irrelevant to
        // relational rewrites.
        Constant { .. }
        | Get {
            id: Id::Global(_), ..
        }
        | FlatMap { .. }
        | ArrangeBy { .. }
        | LetRec { .. } => Rel::Opaque(Box::new(expr.clone())),
    }
}

/// Reduce `expr` against `col_types` (its evaluation context) and wrap the
/// reduced form in an [`EScalar`], recording the `lit` fact off the reduced
/// expression. This is `ReduceScalars` performed once at lower time, so every
/// scalar canonicalization MIR's simplifier provides (constant folding, CASE
/// coalescing, literal CASE rewriting) lands in the interned payload and rides
/// through saturation unchanged.
///
/// `reduce` may use column nullability to simplify, for example rewriting
/// `IsNull(x)` to `false` when `x` is provably non-null in `col_types`. The
/// reduced form is therefore tied to the nullability of the context it was
/// folded against. Most active rules keep a scalar in an equal-or-stricter
/// context (column permutation renames indices but preserves types, filter
/// pushdown past a project or through map/negate/threshold preserves each
/// referenced column's type), where a conservatively-reduced scalar stays
/// correct. The one rule that moves a scalar from a stricter to a looser
/// context is filter-pushdown into a join input: the predicate is folded
/// against the join output type, which strengthens columns to non-null across
/// equivalence classes, then pushed onto a single input whose plain type is
/// nullable. That is sound because the join equivalence enforces the
/// strengthened non-null fact on every surviving row, so the reduced predicate
/// computes the same result after the push. This mirrors what the production
/// `predicate_pushdown` transform already does (it reduces against the join
/// output type before pushing into inputs). The soundness condition is thus
/// per-rule semantic identity, not a blanket no-relaxation rule.
fn reduced_escalar(expr: &MirScalarExpr, col_types: &[ReprColumnType]) -> EScalar {
    let mut folded = expr.clone();
    folded.reduce(col_types);
    let lit = if folded.is_literal_true() {
        Some(true)
    } else if folded.is_literal_false() {
        Some(false)
    } else {
        None
    };
    EScalar::new(folded, lit)
}

/// Reduce each scalar against `col_types` and wrap it in an [`EScalar`].
fn escalars_in_context(exprs: &[MirScalarExpr], col_types: &[ReprColumnType]) -> Vec<EScalar> {
    exprs
        .iter()
        .map(|e| reduced_escalar(e, col_types))
        .collect()
}

/// Like [`escalars_in_context`], but for `Map` scalars: each scalar may
/// reference the columns produced by preceding scalars in the same `Map`, so
/// the fold context grows by each scalar's type as we go. Folding against a
/// context missing those columns would index out of bounds in `typ`/`reduce`.
fn escalars_in_map_context(
    exprs: &[MirScalarExpr],
    mut col_types: Vec<ReprColumnType>,
) -> Vec<EScalar> {
    let mut out = Vec::with_capacity(exprs.len());
    for e in exprs {
        let escalar = reduced_escalar(e, &col_types);
        // Extend the context with this scalar's type so the next scalar can
        // reference it. `reduce` preserves the type, so the reduced payload's
        // type matches the original's.
        let typ = escalar.expr.typ(&col_types);
        out.push(escalar);
        col_types.push(typ);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_expr::{AggregateExpr, AggregateFunc, BinaryFunc, MirRelationExpr, MirScalarExpr};
    use mz_repr::{Datum, ReprRelationType, ReprScalarType};

    use crate::eqsat::ir::Rel;

    fn base(arity: usize) -> MirRelationExpr {
        let typ = ReprRelationType::new(
            (0..arity)
                .map(|_| ReprScalarType::Int64.nullable(false))
                .collect(),
        );
        MirRelationExpr::constant(vec![], typ)
    }

    #[mz_ore::test]
    fn lower_filter_of_constant_gives_filter_over_opaque() {
        // A `Filter` whose input is `Constant` (bailed) should still produce a
        // `Rel::Filter` envelope with a `Rel::Opaque` input.
        let r = base(2).filter(vec![MirScalarExpr::column(0)]);
        let rel = lower(&r);
        match rel {
            Rel::Filter { predicates, input } => {
                assert_eq!(predicates.len(), 1);
                // The Constant input is bailed to an opaque leaf.
                assert!(
                    matches!(*input, Rel::Opaque(_)),
                    "expected Opaque under filter, got {input:?}"
                );
            }
            other => panic!("expected Filter, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn unsupported_arrange_by_becomes_opaque_leaf_with_arity() {
        // `ArrangeBy` is in the bail set; the resulting `Opaque` must carry the
        // original subtree (and so its arity).
        let inner = base(2);
        let arity = inner.arity();
        let r = inner.arrange_by(&[vec![MirScalarExpr::column(0)]]);
        let rel = lower(&r);
        match rel {
            Rel::Opaque(m) => assert_eq!(m.arity(), arity),
            other => panic!("expected opaque leaf, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn filter_over_arrange_by_keeps_filter_envelope() {
        // A supported `Filter` wrapping an unsupported `ArrangeBy` should lower
        // to `Rel::Filter` with the `ArrangeBy` as one opaque leaf inside.
        let arranged = base(2).arrange_by(&[vec![MirScalarExpr::column(0)]]);
        let r = arranged.filter(vec![MirScalarExpr::column(0)]);
        let rel = lower(&r);
        assert!(
            matches!(rel, Rel::Filter { .. }),
            "expected Filter envelope, got {rel:?}"
        );
    }

    #[mz_ore::test]
    fn lower_project() {
        let r = base(3).project(vec![2, 0]);
        let rel = lower(&r);
        match rel {
            Rel::Project { outputs, .. } => assert_eq!(outputs, vec![2, 0]),
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn lower_map() {
        let r = base(2).map(vec![MirScalarExpr::column(0), MirScalarExpr::column(1)]);
        let rel = lower(&r);
        match rel {
            Rel::Map { scalars, .. } => assert_eq!(scalars.len(), 2),
            other => panic!("expected Map, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn lower_negate_threshold() {
        let neg = lower(&base(1).negate());
        assert!(matches!(neg, Rel::Negate { .. }), "expected Negate");
        let thr = lower(&base(1).threshold());
        assert!(matches!(thr, Rel::Threshold { .. }), "expected Threshold");
    }

    #[mz_ore::test]
    fn constant_is_bailed_to_opaque_leaf() {
        // `Constant` is in the bail set; verify it becomes a `Rel::Opaque`.
        let rel = lower(&base(3));
        match rel {
            Rel::Opaque(m) => assert_eq!(m.arity(), 3),
            other => panic!("expected opaque leaf, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn filter_predicate_payload_is_reduced() {
        // `#0 AND true` over a boolean column reduces to `#0`. The stored
        // payload must be the reduced form, not the original conjunction.
        let typ = ReprRelationType::new(vec![ReprScalarType::Bool.nullable(false)]);
        let r = MirRelationExpr::constant(vec![], typ).filter(vec![
            MirScalarExpr::column(0).and(MirScalarExpr::literal_true()),
        ]);
        let rel = lower(&r);
        match rel {
            Rel::Filter { predicates, .. } => {
                assert_eq!(predicates.len(), 1);
                assert_eq!(
                    predicates[0].expr,
                    MirScalarExpr::column(0),
                    "filter predicate payload was not reduced"
                );
            }
            other => panic!("expected Filter, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn reduce_group_key_payload_is_reduced() {
        // A group key `#0 AND true` over a boolean column reduces to `#0`.
        let typ = ReprRelationType::new(vec![ReprScalarType::Bool.nullable(false)]);
        let input = MirRelationExpr::constant(vec![], typ);
        let r = MirRelationExpr::Reduce {
            input: Box::new(input),
            group_key: vec![MirScalarExpr::column(0).and(MirScalarExpr::literal_true())],
            aggregates: vec![],
            monotonic: false,
            expected_group_size: None,
        };
        let rel = lower(&r);
        match rel {
            Rel::Reduce { group_key, .. } => {
                assert_eq!(group_key.len(), 1);
                assert_eq!(
                    group_key[0].expr,
                    MirScalarExpr::column(0),
                    "reduce group key payload was not reduced"
                );
            }
            other => panic!("expected Reduce, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn map_scalar_payload_is_reduced() {
        // `1 + 1` is constant-folded to `2`. The stored Map payload must be the
        // folded literal.
        let one = MirScalarExpr::literal_ok(Datum::Int64(1), ReprScalarType::Int64);
        let sum = one
            .clone()
            .call_binary(one, BinaryFunc::AddInt64(mz_expr::func::AddInt64));
        let r = base(1).map(vec![sum]);
        let rel = lower(&r);
        match rel {
            Rel::Map { scalars, .. } => {
                assert_eq!(scalars.len(), 1);
                assert_eq!(
                    scalars[0].expr,
                    MirScalarExpr::literal_ok(Datum::Int64(2), ReprScalarType::Int64),
                    "map scalar payload was not constant-folded"
                );
            }
            other => panic!("expected Map, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn join_equivalence_payload_is_reduced() {
        // A two-input join over boolean columns with an equivalence containing
        // `#0 AND true`. Join equivalences reference the concatenated input
        // column space, so column 0 of the first input is `#0`. The payload
        // must reduce to `#0`.
        let typ = ReprRelationType::new(vec![ReprScalarType::Bool.nullable(false)]);
        let a = MirRelationExpr::constant(vec![], typ.clone());
        let b = MirRelationExpr::constant(vec![], typ);
        let r = MirRelationExpr::join_scalars(
            vec![a, b],
            vec![vec![
                MirScalarExpr::column(0).and(MirScalarExpr::literal_true()),
                MirScalarExpr::column(1),
            ]],
        );
        let rel = lower(&r);
        match rel {
            Rel::Join { equivalences, .. } => {
                assert_eq!(equivalences.len(), 1);
                assert_eq!(
                    equivalences[0][0].expr,
                    MirScalarExpr::column(0),
                    "join equivalence payload was not reduced"
                );
            }
            other => panic!("expected Join, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn reduce_aggregate_expr_payload_is_reduced() {
        // An aggregate input expression `1 + 1` is constant-folded to `2`. The
        // aggregate path reduces `agg.expr` directly (not via reduced_escalar),
        // so it needs its own coverage.
        let one = MirScalarExpr::literal_ok(Datum::Int64(1), ReprScalarType::Int64);
        let sum = one
            .clone()
            .call_binary(one, BinaryFunc::AddInt64(mz_expr::func::AddInt64));
        let r = MirRelationExpr::Reduce {
            input: Box::new(base(1)),
            group_key: vec![],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::MaxInt64,
                expr: sum,
                distinct: false,
            }],
            monotonic: false,
            expected_group_size: None,
        };
        let rel = lower(&r);
        match rel {
            Rel::Reduce { aggregates, .. } => {
                assert_eq!(aggregates.len(), 1);
                assert_eq!(
                    aggregates[0].expr,
                    MirScalarExpr::literal_ok(Datum::Int64(2), ReprScalarType::Int64),
                    "aggregate input expression was not constant-folded"
                );
            }
            other => panic!("expected Reduce, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn topk_limit_payload_is_reduced() {
        // A TopK limit `1 + 0` is constant-folded to `1`. The limit path reduces
        // the scalar directly, so it needs its own coverage.
        let one = MirScalarExpr::literal_ok(Datum::Int64(1), ReprScalarType::Int64);
        let zero = MirScalarExpr::literal_ok(Datum::Int64(0), ReprScalarType::Int64);
        let limit = one.call_binary(zero, BinaryFunc::AddInt64(mz_expr::func::AddInt64));
        let r = MirRelationExpr::TopK {
            input: Box::new(base(1)),
            group_key: vec![],
            order_key: vec![],
            limit: Some(limit),
            offset: 0,
            monotonic: false,
            expected_group_size: None,
        };
        let rel = lower(&r);
        match rel {
            Rel::TopK { shape, .. } => {
                assert_eq!(
                    shape.limit,
                    Some(MirScalarExpr::literal_ok(
                        Datum::Int64(1),
                        ReprScalarType::Int64
                    )),
                    "TopK limit was not constant-folded"
                );
            }
            other => panic!("expected TopK, got {other:?}"),
        }
    }
}
