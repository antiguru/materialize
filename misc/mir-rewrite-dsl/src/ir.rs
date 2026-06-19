// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! A faithful *subset* of Materialize's MIR relational algebra.
//!
//! The goal of this crate is to reason about **relational** rewrites, so scalar
//! expressions are deliberately kept opaque: a [`Scalar`] carries its textual
//! rendering plus the set of columns it references. That is exactly the
//! information the relational rewrite rules need (e.g. to decide whether a
//! predicate can be pushed below a `Map`), and nothing more.
//!
//! The relational variants mirror [`mz_expr::MirRelationExpr`] one-to-one for
//! the operators we model, with one addition: [`Rel::WcoJoin`], a *physical*
//! marker for a worst-case-optimal (generic / leapfrog-triejoin) multiway join.
//! `Join` and `WcoJoin` are semantically identical; they differ only in cost.

use std::collections::BTreeSet;
use std::fmt;

/// A column index, 0-based, just like MIR's `#n`.
pub type Col = usize;

/// An opaque scalar expression.
///
/// We do not model scalar rewrites in this crate (the focus is relational), so
/// a scalar is reduced to the two facts the relational rules care about: how it
/// prints, and which input columns it reads.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Scalar {
    /// Textual rendering, e.g. `"(#0 + #1) = #3"`. Used for display and as the
    /// identity of the scalar when matching/printing rules.
    pub text: String,
    /// The set of columns this scalar reads. Drives side conditions such as
    /// "this predicate only references columns of the inner relation".
    pub cols: BTreeSet<Col>,
}

impl Scalar {
    /// A scalar reading the columns in `cols`, rendered as `text`.
    pub fn new(text: impl Into<String>, cols: impl IntoIterator<Item = Col>) -> Self {
        Scalar {
            text: text.into(),
            cols: cols.into_iter().collect(),
        }
    }

    /// The largest column referenced, plus one (0 if none) — a lower bound on
    /// the arity of any relation this scalar can be evaluated against.
    pub fn min_arity(&self) -> usize {
        self.cols.iter().copied().max().map_or(0, |c| c + 1)
    }
}

impl fmt::Display for Scalar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
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
    /// [`crate::cost::Stats`] oracle.
    Get { name: String, arity: usize },
    /// Retain only `outputs` columns, in that order.
    Project { input: Box<Rel>, outputs: Vec<Col> },
    /// Append `scalars` as new columns. Columns of `input` keep their indices.
    Map {
        input: Box<Rel>,
        scalars: Vec<Scalar>,
    },
    /// Keep rows where every predicate holds.
    Filter {
        input: Box<Rel>,
        predicates: Vec<Scalar>,
    },
    /// An equi-join over the cross product of `inputs`, constrained by
    /// `equivalences` (each class is a list of scalars that must be equal).
    /// `WcoJoin` is the same operator with a different physical strategy.
    Join {
        inputs: Vec<Rel>,
        equivalences: Vec<Vec<Scalar>>,
    },
    /// A worst-case-optimal (generic / leapfrog-triejoin) join. Semantically
    /// identical to [`Rel::Join`]; only the cost model treats it differently.
    WcoJoin {
        inputs: Vec<Rel>,
        equivalences: Vec<Vec<Scalar>>,
    },
    /// Group by `group_key`, producing `aggregates` opaque aggregate columns.
    Reduce {
        input: Box<Rel>,
        group_key: Vec<Scalar>,
        aggregates: Vec<Scalar>,
    },
    /// Add the multiplicities of `base` and every relation in `inputs`.
    Union { base: Box<Rel>, inputs: Vec<Rel> },
    /// Negate every multiplicity.
    Negate { input: Box<Rel> },
    /// Drop rows whose accumulated multiplicity is not positive.
    Threshold { input: Box<Rel> },
    /// Bind `value` to a local id, available as [`Rel::LocalGet`] in `body`.
    /// Introduced by extraction-time CSE (see [`crate::cse`]); the rewrite
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
    /// tree remains context-free to traverse.
    LocalGet { id: usize, arity: usize },
}

impl Rel {
    /// The number of output columns.
    pub fn arity(&self) -> usize {
        match self {
            Rel::Constant { arity, .. } | Rel::Get { arity, .. } => *arity,
            Rel::Project { outputs, .. } => outputs.len(),
            Rel::Map { input, scalars } => input.arity() + scalars.len(),
            Rel::Filter { input, .. } | Rel::Negate { input } | Rel::Threshold { input } => {
                input.arity()
            }
            Rel::Join { inputs, .. } | Rel::WcoJoin { inputs, .. } => {
                inputs.iter().map(Rel::arity).sum()
            }
            Rel::Reduce {
                group_key,
                aggregates,
                ..
            } => group_key.len() + aggregates.len(),
            Rel::Union { base, .. } => base.arity(),
            Rel::Let { body, .. } | Rel::LetRec { body, .. } => body.arity(),
            Rel::LocalGet { arity, .. } => *arity,
        }
    }

    /// The children of this node, for generic traversal.
    pub fn children(&self) -> Vec<&Rel> {
        match self {
            Rel::Constant { .. } | Rel::Get { .. } => vec![],
            Rel::Project { input, .. }
            | Rel::Map { input, .. }
            | Rel::Filter { input, .. }
            | Rel::Reduce { input, .. }
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
            Rel::Constant { .. } | Rel::Get { .. } => {
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
                ..
            } => Rel::Reduce {
                input: Box::new(take(0)),
                group_key: group_key.clone(),
                aggregates: aggregates.clone(),
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
        let scalars = |xs: &[Scalar]| {
            xs.iter()
                .map(|s| s.text.clone())
                .collect::<Vec<_>>()
                .join(", ")
        };
        match self {
            Rel::Constant { card, arity } => {
                writeln!(f, "{pad}Constant rows={card} arity={arity}")?;
            }
            Rel::Get { name, arity } => writeln!(f, "{pad}Get {name} arity={arity}")?,
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
            } => {
                writeln!(
                    f,
                    "{pad}Reduce group_by=[{}] aggregates=[{}]",
                    scalars(group_key),
                    scalars(aggregates)
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
            Rel::LocalGet { id, arity } => writeln!(f, "{pad}Get l{id} arity={arity}")?,
        }
        Ok(())
    }
}

impl fmt::Display for Rel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.pretty(f, 0)
    }
}
