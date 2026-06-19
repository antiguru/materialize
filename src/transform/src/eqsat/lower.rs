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
            // Scalars in a Map reference input columns; fold against the input
            // type so nullability-provable constants are reflected in `lit`.
            let col_types = input.typ().column_types;
            Rel::Map {
                input: Box::new(lower(input)),
                scalars: escalars_in_context(scalars, &col_types),
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
        } => Rel::Join {
            inputs: inputs.iter().map(lower).collect(),
            equivalences: equivalences
                .iter()
                .map(|class| class.iter().map(|e| EScalar::plain(e.clone())).collect())
                .collect(),
        },
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
        } => Rel::Reduce {
            input: Box::new(lower(input)),
            group_key: group_key
                .iter()
                .map(|e| EScalar::plain(e.clone()))
                .collect(),
            aggregates: aggregates.clone(),
            monotonic: *monotonic,
            expected_group_size: *expected_group_size,
        },
        TopK {
            input,
            group_key,
            order_key,
            limit,
            offset,
            monotonic,
            expected_group_size,
        } => Rel::TopK {
            input: Box::new(lower(input)),
            shape: TopKShape {
                group_key: group_key.clone(),
                order_key: order_key.clone(),
                limit: limit.clone(),
                offset: *offset,
                monotonic: *monotonic,
                expected_group_size: *expected_group_size,
            },
        },
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

/// Build a scalar payload with its `lit` fold against the column types of the
/// relation it is evaluated over. The original expression is carried unchanged;
/// only `lit` is sharpened.
fn escalars_in_context(exprs: &[MirScalarExpr], col_types: &[ReprColumnType]) -> Vec<EScalar> {
    exprs
        .iter()
        .map(|e| {
            let mut folded = e.clone();
            folded.reduce(col_types);
            let lit = if folded.is_literal_true() {
                Some(true)
            } else if folded.is_literal_false() {
                Some(false)
            } else {
                None
            };
            EScalar::new(e.clone(), lit)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_expr::{MirRelationExpr, MirScalarExpr};
    use mz_repr::{ReprRelationType, ReprScalarType};

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
}
