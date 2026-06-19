// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! A faithful *subset* of Materialize's MIR relational algebra.
//!
//! The goal of this crate is to reason about **relational** rewrites, so the
//! scalar payloads are the real [`mz_expr::MirScalarExpr`]s, wrapped in
//! [`EScalar`] alongside one precomputed fact (`lit`). The relational facts the
//! rewrite rules read (column support, whether the scalar is a bare column
//! reference) are computed live off the expression; column remapping is the
//! real `MirScalarExpr::permute`.
//!
//! The relational variants mirror [`mz_expr::MirRelationExpr`] one-to-one for
//! the operators we model, with one addition: [`Rel::WcoJoin`], a *physical*
//! marker for a worst-case-optimal (generic / leapfrog-triejoin) multiway join.
//! `Join` and `WcoJoin` are semantically identical; they differ only in cost.
//! Unsupported subtrees are carried verbatim in [`Rel::Opaque`].

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use mz_expr::{AggregateExpr, ColumnOrder, Columns, MirRelationExpr, MirScalarExpr};

/// A column index, 0-based, just like MIR's `#n`.
pub type Col = usize;

/// The non-input payload of a [`Rel::TopK`], carried verbatim.
///
/// `TopK` is a structural passthrough: no rule rewrites it, so its payload is
/// opaque and rides along unchanged through lower and raise. Modeling it as a
/// node (rather than an opaque leaf) lets the optimizer rewrite the subtree
/// below it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TopKShape {
    pub group_key: Vec<Col>,
    pub order_key: Vec<ColumnOrder>,
    pub limit: Option<MirScalarExpr>,
    pub offset: usize,
    pub monotonic: bool,
    pub expected_group_size: Option<u64>,
}

/// A scalar payload: the reduced [`MirScalarExpr`] plus one precomputed fact.
///
/// `expr` is the authoritative value (raise returns it verbatim). It is the
/// `MirScalarExpr::reduce` canonical form computed once at lower time against
/// the column types of the relation the scalar is evaluated over, so the
/// e-graph carries `ReduceScalars`-equivalent payloads. The column support and
/// bare-column-reference facts the rewrite rules read are computed live from
/// `expr` (see [`EScalar::cols`], [`EScalar::is_col`]).
///
/// `lit` records whether `expr` folds to a literal `true`/`false` against the
/// column types of the relation it is evaluated over. The e-graph tracks only
/// arities, not column types, so this nullability-sensitive fact cannot be
/// recomputed there; it is set once at lower time and carried through column
/// permutation unchanged (permuting columns cannot change literal-ness).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EScalar {
    pub expr: MirScalarExpr,
    pub lit: Option<bool>,
}

impl EScalar {
    /// A scalar with a precomputed `lit` fact.
    pub fn new(expr: MirScalarExpr, lit: Option<bool>) -> Self {
        EScalar { expr, lit }
    }

    /// A scalar with no precomputed `lit` fact (join equivalences, group keys,
    /// and the engine's own unit tests).
    pub fn plain(expr: MirScalarExpr) -> Self {
        EScalar { expr, lit: None }
    }

    /// The columns the scalar references.
    pub fn cols(&self) -> BTreeSet<Col> {
        self.expr.support()
    }

    /// The column index if the scalar is a bare column reference `#k`.
    pub fn is_col(&self) -> Option<Col> {
        self.expr.as_column()
    }

    /// The largest column referenced, plus one (0 if none) — a lower bound on
    /// the arity of any relation this scalar can be evaluated against.
    pub fn min_arity(&self) -> usize {
        self.cols().iter().copied().max().map_or(0, |c| c + 1)
    }

    /// Apply a column-index function to every referenced column, returning a new
    /// scalar with the remapped expression. The `lit` fact is preserved.
    /// Errors if any column maps to a negative index.
    pub fn permute_cols(&self, mut f: impl FnMut(Col) -> i64) -> Result<EScalar, String> {
        let mut map: BTreeMap<usize, usize> = BTreeMap::new();
        for c in self.cols() {
            let nc = f(c);
            if nc < 0 {
                return Err(format!(
                    "column shift produced negative index for `{}`",
                    self.expr
                ));
            }
            map.insert(
                c,
                usize::try_from(nc).expect("non-negative index fits usize"),
            );
        }
        let mut expr = self.expr.clone();
        expr.permute_map(&map);
        Ok(EScalar {
            expr,
            lit: self.lit,
        })
    }
}

impl fmt::Display for EScalar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expr)
    }
}

/// A relational expression: a subset of `MirRelationExpr`.
///
/// `Ord` is derived so extraction can break cost ties deterministically (the
/// e-graph is stored in hash maps with randomized iteration order).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Rel {
    /// A literal collection of `card` rows over `arity` columns.
    ///
    /// We do not track the actual row data (irrelevant to relational rewrites),
    /// only the cardinality and arity.
    Constant { card: u64, arity: usize },
    /// A reference to a named base collection. Cardinality comes from the
    /// [`crate::eqsat::cost::Stats`] oracle.
    Get { name: String, arity: usize },
    /// Retain only `outputs` columns, in that order.
    Project { input: Box<Rel>, outputs: Vec<Col> },
    /// Append `scalars` as new columns. Columns of `input` keep their indices.
    Map {
        input: Box<Rel>,
        scalars: Vec<EScalar>,
    },
    /// Keep rows where every predicate holds.
    Filter {
        input: Box<Rel>,
        predicates: Vec<EScalar>,
    },
    /// An equi-join over the cross product of `inputs`, constrained by
    /// `equivalences` (each class is a list of scalars that must be equal).
    /// `WcoJoin` is the same operator with a different physical strategy.
    Join {
        inputs: Vec<Rel>,
        equivalences: Vec<Vec<EScalar>>,
    },
    /// A worst-case-optimal (generic / leapfrog-triejoin) join. Semantically
    /// identical to [`Rel::Join`]; only the cost model treats it differently.
    WcoJoin {
        inputs: Vec<Rel>,
        equivalences: Vec<Vec<EScalar>>,
    },
    /// Group by `group_key`, producing the `aggregates` aggregate columns.
    /// `monotonic` and `expected_group_size` are physical hints carried
    /// verbatim for a faithful round-trip; no rule reads them.
    Reduce {
        input: Box<Rel>,
        group_key: Vec<EScalar>,
        aggregates: Vec<AggregateExpr>,
        monotonic: bool,
        expected_group_size: Option<u64>,
    },
    /// Group, order within each group, and limit. A structural passthrough: its
    /// `shape` is opaque and no rule rewrites it, but its `input` is optimized.
    TopK { input: Box<Rel>, shape: TopKShape },
    /// An unsupported subtree, carried verbatim. The supported envelope around
    /// it still saturates; raise re-emits the stored `MirRelationExpr`.
    /// Hash-consing dedups identical opaque leaves.
    Opaque(Box<MirRelationExpr>),
    /// Add the multiplicities of `base` and every relation in `inputs`.
    Union { base: Box<Rel>, inputs: Vec<Rel> },
    /// Negate every multiplicity.
    Negate { input: Box<Rel> },
    /// Drop rows whose accumulated multiplicity is not positive.
    Threshold { input: Box<Rel> },
    /// Bind `value` to a local id, available as [`Rel::LocalGet`] in `body`.
    /// Introduced by extraction-time CSE (see [`crate::eqsat::cse`]); the rewrite
    /// rules never produce it.
    Let {
        id: usize,
        value: Box<Rel>,
        body: Box<Rel>,
    },
    /// Mutually-recursive bindings, each `(id, value)`, in scope in every
    /// `value` and in `body`. Evaluated as the least fixpoint from the empty
    /// collection (differential-dataflow `iterate`). The recursive references
    /// are [`Rel::LocalGet`]s of the bound ids; this is the only operator that
    /// closes a genuine cycle in the plan.
    LetRec {
        bindings: Vec<(usize, Rel)>,
        body: Box<Rel>,
    },
    /// A reference to a [`Rel::Let`]-bound local. Carries its `arity` so the
    /// tree remains context-free to traverse, and the original
    /// `MirRelationExpr::Get { Id::Local(..) }` node (`get`) so raise can return
    /// it verbatim, preserving its type. `get` is `None` for the engine's scope
    /// placeholders (substituted out before raise) and cse-introduced locals.
    LocalGet {
        id: usize,
        arity: usize,
        get: Option<Box<MirRelationExpr>>,
    },
}

impl Rel {
    /// The number of output columns.
    pub fn arity(&self) -> usize {
        match self {
            Rel::Constant { arity, .. } | Rel::Get { arity, .. } => *arity,
            Rel::Project { outputs, .. } => outputs.len(),
            Rel::Map { input, scalars } => input.arity() + scalars.len(),
            Rel::Filter { input, .. }
            | Rel::Negate { input }
            | Rel::Threshold { input }
            | Rel::TopK { input, .. } => input.arity(),
            Rel::Join { inputs, .. } | Rel::WcoJoin { inputs, .. } => {
                inputs.iter().map(Rel::arity).sum()
            }
            Rel::Reduce {
                group_key,
                aggregates,
                ..
            } => group_key.len() + aggregates.len(),
            Rel::Union { base, .. } => base.arity(),
            Rel::Opaque(m) => m.arity(),
            Rel::Let { body, .. } | Rel::LetRec { body, .. } => body.arity(),
            Rel::LocalGet { arity, .. } => *arity,
        }
    }

    /// The children of this node, for generic traversal.
    pub fn children(&self) -> Vec<&Rel> {
        match self {
            Rel::Constant { .. } | Rel::Get { .. } | Rel::Opaque(_) => vec![],
            Rel::Project { input, .. }
            | Rel::Map { input, .. }
            | Rel::Filter { input, .. }
            | Rel::Reduce { input, .. }
            | Rel::TopK { input, .. }
            | Rel::Negate { input }
            | Rel::Threshold { input } => vec![input],
            Rel::Join { inputs, .. } | Rel::WcoJoin { inputs, .. } => inputs.iter().collect(),
            Rel::Union { base, inputs } => {
                let mut v = vec![&**base];
                v.extend(inputs.iter());
                v
            }
            Rel::Let { value, body, .. } => vec![value, body],
            Rel::LetRec { bindings, body } => {
                let mut v: Vec<&Rel> = bindings.iter().map(|(_, r)| r).collect();
                v.push(body);
                v
            }
            Rel::LocalGet { .. } => vec![],
        }
    }

    /// Replace the children of this node with `new`, preserving order. The
    /// length of `new` must match [`Rel::children`].
    pub fn with_children(&self, mut new: Vec<Rel>) -> Rel {
        let mut take =
            |i: usize| std::mem::replace(&mut new[i], Rel::Constant { card: 0, arity: 0 });
        match self {
            Rel::Constant { .. } | Rel::Get { .. } | Rel::Opaque(_) => {
                assert!(new.is_empty());
                self.clone()
            }
            Rel::Project { outputs, .. } => Rel::Project {
                input: Box::new(take(0)),
                outputs: outputs.clone(),
            },
            Rel::Map { scalars, .. } => Rel::Map {
                input: Box::new(take(0)),
                scalars: scalars.clone(),
            },
            Rel::Filter { predicates, .. } => Rel::Filter {
                input: Box::new(take(0)),
                predicates: predicates.clone(),
            },
            Rel::Reduce {
                group_key,
                aggregates,
                monotonic,
                expected_group_size,
                ..
            } => Rel::Reduce {
                input: Box::new(take(0)),
                group_key: group_key.clone(),
                aggregates: aggregates.clone(),
                monotonic: *monotonic,
                expected_group_size: *expected_group_size,
            },
            Rel::TopK { shape, .. } => Rel::TopK {
                input: Box::new(take(0)),
                shape: shape.clone(),
            },
            Rel::Negate { .. } => Rel::Negate {
                input: Box::new(take(0)),
            },
            Rel::Threshold { .. } => Rel::Threshold {
                input: Box::new(take(0)),
            },
            Rel::Join { equivalences, .. } => Rel::Join {
                inputs: new,
                equivalences: equivalences.clone(),
            },
            Rel::WcoJoin { equivalences, .. } => Rel::WcoJoin {
                inputs: new,
                equivalences: equivalences.clone(),
            },
            Rel::Union { .. } => {
                let base = Box::new(take(0));
                let inputs = new.split_off(1);
                Rel::Union { base, inputs }
            }
            Rel::Let { id, .. } => Rel::Let {
                id: *id,
                value: Box::new(take(0)),
                body: Box::new(take(1)),
            },
            Rel::LetRec { bindings, .. } => {
                let ids: Vec<usize> = bindings.iter().map(|(id, _)| *id).collect();
                let body = Box::new(take(bindings.len()));
                let values = (0..bindings.len()).map(&mut take).collect::<Vec<_>>();
                Rel::LetRec {
                    bindings: ids.into_iter().zip(values).collect(),
                    body,
                }
            }
            Rel::LocalGet { .. } => {
                assert!(new.is_empty());
                self.clone()
            }
        }
    }

    /// Total node count (size of the tree). Used as a structural tie-breaker in
    /// the cost model so that simplifications which remove a node are always
    /// strictly cheaper.
    pub fn node_count(&self) -> usize {
        1 + self
            .children()
            .iter()
            .map(|c| c.node_count())
            .sum::<usize>()
    }

    /// Pretty-print the plan as an indented tree.
    fn pretty(&self, f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result {
        let pad = "  ".repeat(indent);
        let scalars = |xs: &[EScalar]| {
            xs.iter()
                .map(|s| s.expr.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        };
        let aggs = |xs: &[AggregateExpr]| {
            xs.iter()
                .map(|a| a.expr.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        };
        match self {
            Rel::Constant { card, arity } => {
                writeln!(f, "{pad}Constant rows={card} arity={arity}")?;
            }
            Rel::Get { name, arity } => writeln!(f, "{pad}Get {name} arity={arity}")?,
            Rel::Opaque(m) => writeln!(f, "{pad}Opaque arity={}", m.arity())?,
            Rel::Project { input, outputs } => {
                writeln!(f, "{pad}Project {outputs:?}")?;
                input.pretty(f, indent + 1)?;
            }
            Rel::Map { input, scalars: s } => {
                writeln!(f, "{pad}Map [{}]", scalars(s))?;
                input.pretty(f, indent + 1)?;
            }
            Rel::Filter { input, predicates } => {
                writeln!(f, "{pad}Filter [{}]", scalars(predicates))?;
                input.pretty(f, indent + 1)?;
            }
            Rel::Reduce {
                input,
                group_key,
                aggregates,
                ..
            } => {
                writeln!(
                    f,
                    "{pad}Reduce group_by=[{}] aggregates=[{}]",
                    scalars(group_key),
                    aggs(aggregates)
                )?;
                input.pretty(f, indent + 1)?;
            }
            Rel::TopK { input, shape } => {
                writeln!(
                    f,
                    "{pad}TopK group_by={:?} limit={:?} offset={}",
                    shape.group_key, shape.limit, shape.offset
                )?;
                input.pretty(f, indent + 1)?;
            }
            Rel::Negate { input } => {
                writeln!(f, "{pad}Negate")?;
                input.pretty(f, indent + 1)?;
            }
            Rel::Threshold { input } => {
                writeln!(f, "{pad}Threshold")?;
                input.pretty(f, indent + 1)?;
            }
            Rel::Join {
                inputs,
                equivalences,
            }
            | Rel::WcoJoin {
                inputs,
                equivalences,
            } => {
                let kind = if matches!(self, Rel::WcoJoin { .. }) {
                    "WcoJoin"
                } else {
                    "Join"
                };
                let eqs = equivalences
                    .iter()
                    .map(|c| format!("({})", scalars(c)))
                    .collect::<Vec<_>>()
                    .join(" ");
                writeln!(f, "{pad}{kind} on={eqs}")?;
                for i in inputs {
                    i.pretty(f, indent + 1)?;
                }
            }
            Rel::Union { base, inputs } => {
                writeln!(f, "{pad}Union")?;
                base.pretty(f, indent + 1)?;
                for i in inputs {
                    i.pretty(f, indent + 1)?;
                }
            }
            Rel::Let { id, value, body } => {
                writeln!(f, "{pad}Let l{id} =")?;
                value.pretty(f, indent + 1)?;
                writeln!(f, "{pad}in")?;
                body.pretty(f, indent + 1)?;
            }
            Rel::LetRec { bindings, body } => {
                writeln!(f, "{pad}LetRec")?;
                for (id, value) in bindings {
                    writeln!(f, "{pad}  l{id} =")?;
                    value.pretty(f, indent + 2)?;
                }
                writeln!(f, "{pad}in")?;
                body.pretty(f, indent + 1)?;
            }
            Rel::LocalGet { id, arity, .. } => writeln!(f, "{pad}Get l{id} arity={arity}")?,
        }
        Ok(())
    }
}

impl fmt::Display for Rel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.pretty(f, 0)
    }
}

#[cfg(test)]
mod escalar_tests {
    use super::*;

    fn col(c: usize) -> MirScalarExpr {
        MirScalarExpr::column(c)
    }

    #[mz_ore::test]
    fn cols_and_is_col_are_live() {
        let s = EScalar::plain(col(3));
        assert_eq!(s.cols().into_iter().collect::<Vec<_>>(), vec![3]);
        assert_eq!(s.is_col(), Some(3));
    }

    #[mz_ore::test]
    fn permute_cols_remaps_the_expression() {
        // #3 shifted by -2 becomes #1; the lit fact is preserved.
        let s = EScalar::new(col(3), Some(true));
        let out = s.permute_cols(|c| i64::try_from(c).unwrap() - 2).unwrap();
        assert_eq!(out.is_col(), Some(1));
        assert_eq!(out.lit, Some(true));
    }

    #[mz_ore::test]
    fn permute_cols_rejects_negative_target() {
        let s = EScalar::plain(col(0));
        assert!(s.permute_cols(|c| i64::try_from(c).unwrap() - 1).is_err());
    }
}
