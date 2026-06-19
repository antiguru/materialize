// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! A small **abstract-interpretation framework** for e-class analyses.
//!
//! Many rewrite side conditions are facts about a relation that no single
//! operator decides locally: non-negativity, unique keys, nullability,
//! monotonicity. Each is a *lattice-valued analysis* attached to every e-class
//! and solved by monotone fixpoint iteration — exactly egg's `Analysis`
//! concept. This module factors that shape out of the hand-rolled
//! `non_negative` pass so new analyses are drop-in.
//!
//! The defining property: because every e-node in a class denotes the *same*
//! relation, the per-class [`Analysis::merge`] combines facts across equivalent
//! forms toward **more precision** (a relation keeps a key proved by *any* of
//! its forms). That is why an e-graph analysis can be sharper than a
//! single-plan one — and it is the same reason `non_negative` asks for *some*
//! `Negate`-free representative.
//!
//! The driver lives on [`crate::eqsat::egraph::EGraph`] as `run_analysis`. The same
//! iteration is what a recursive `LetRec` binding would need (its analysis is a
//! fixpoint over the recursive variable); see `COVERAGE.md`.

use std::collections::{BTreeMap, BTreeSet};

use crate::eqsat::egraph::{ENode, Id};
use crate::eqsat::ir::{Col, Rel};

/// A lattice-valued analysis over e-classes.
pub trait Analysis {
    /// The lattice element attached to each e-class.
    type Domain: Clone + Eq;

    /// The least element (the starting point of the fixpoint iteration).
    fn bottom(&self) -> Self::Domain;

    /// The transfer function for one e-node, reading children's current facts
    /// via `get` (already canonicalized) and their arities via `arity`. Arities
    /// are structural and constant across the fixpoint, so they are supplied
    /// directly rather than as a lattice value (needed e.g. to offset a join
    /// input's columns into the join's output).
    fn make(
        &self,
        node: &ENode,
        get: &dyn Fn(Id) -> Self::Domain,
        arity: &dyn Fn(Id) -> usize,
    ) -> Self::Domain;

    /// Combine the facts of two e-nodes in the same class. Because the e-nodes
    /// are equal relations, this moves *up in precision*.
    fn merge(&self, a: Self::Domain, b: Self::Domain) -> Self::Domain;
}

/// Non-negativity: a relation has non-negative multiplicities everywhere.
/// Conservatively, it has *some* `Negate`-free representative.
///
/// `locals` carries facts for `LocalGet` references proven by the Rel-level
/// recursion fixpoint (see [`LocalFacts`]); within a fragment a recursive
/// reference is otherwise unknown.
#[derive(Default)]
pub struct NonNeg {
    pub locals: BTreeMap<usize, bool>,
}

impl Analysis for NonNeg {
    type Domain = bool;

    fn bottom(&self) -> bool {
        false
    }

    fn make(&self, node: &ENode, get: &dyn Fn(Id) -> bool, _arity: &dyn Fn(Id) -> usize) -> bool {
        match node {
            ENode::Negate { .. } => false,
            // A recursive reference: use the Rel-level recursion fact if we have
            // one, else assume nothing (the authoritative fact is the
            // greatest-fixpoint `rel_non_negative`).
            ENode::LocalGet { id, .. } => self.locals.get(id).copied().unwrap_or(false),
            // Leaves (Get/Constant) have no children, so `all` is vacuously
            // true; every other operator preserves non-negativity.
            other => other.children().iter().all(|c| get(*c)),
        }
    }

    fn merge(&self, a: bool, b: bool) -> bool {
        a || b
    }
}

/// Monotonicity: a relation is *insert-only* — its multiplicities never
/// decrease as inputs grow (no retractions). Conservatively, it is built from
/// monotone leaves (base `Get`s and `Constant`s, assumed insert-only) using
/// operators that preserve monotonicity.
///
/// This is genuinely sharper *and* coarser than [`NonNeg`] in different places:
/// `Threshold` and `Negate` are handled the same way, but a `Reduce` breaks
/// monotonicity (a group's aggregate can move both up and down under updates)
/// while it preserves non-negativity. So `Monotonic` is a distinct analysis,
/// not a re-skin of `NonNeg` — even though, on this static relational subset,
/// `monotonic ⟹ non_negative`, so it does not by itself unlock a rewrite that
/// `non_negative` cannot (its real consumers are *physical* monotonic-rendering
/// passes such as `TopK`, which this IR does not model). It is wired in as a
/// condition so those passes are a drop-in away.
#[derive(Default)]
pub struct Monotonic {
    pub locals: BTreeMap<usize, bool>,
}

impl Analysis for Monotonic {
    type Domain = bool;

    fn bottom(&self) -> bool {
        false
    }

    fn make(&self, node: &ENode, get: &dyn Fn(Id) -> bool, _arity: &dyn Fn(Id) -> usize) -> bool {
        match node {
            // A retraction (Negate), an aggregate that can move down (Reduce),
            // or a top-k whose output a higher-ranked row can evict (TopK) is
            // not insert-only.
            ENode::Negate { .. } | ENode::Reduce { .. } | ENode::TopK { .. } => false,
            // A recursive reference: use the Rel-level recursion fact if proven,
            // else conservative (the cross-binding fact comes from
            // `rel_monotonic`).
            ENode::LocalGet { id, .. } => self.locals.get(id).copied().unwrap_or(false),
            // Base collections and constants are taken to be insert-only.
            ENode::Constant { .. } | ENode::Get { .. } => true,
            // Map/Filter/Project/Threshold/Union/Join preserve monotonicity.
            other => other.children().iter().all(|c| get(*c)),
        }
    }

    fn merge(&self, a: bool, b: bool) -> bool {
        a || b
    }
}

/// A unique key: a set of columns that functionally determines a row, on a
/// relation whose rows have multiplicity at most one.
pub type Key = BTreeSet<Col>;
/// The set of known keys of a relation.
pub type KeySet = BTreeSet<Key>;

/// Unique-key analysis.
///
/// Conservative and sound: `Reduce` establishes its group-key columns as a key
/// (it emits one row per group); `Filter`/`Map`/`Project`/`Threshold` preserve
/// keys (with `Project` remapping column indices); a `Join` keys on the union
/// of one key per input (offset into the join's column layout); everything else
/// yields no keys. `merge` unions, since a key proved by any equivalent form
/// holds.
///
/// The join case is what brings key reasoning to `Join`s — the relational core
/// of `redundant_join`/`semijoin_idempotence`. It lets `reduce_elision` see
/// that grouping a join by a join-key is redundant. (Dropping a *whole* join
/// input — the rest of those transforms — needs reasoning about the join's
/// equivalence scalars, which the opaque-scalar design deliberately forbids;
/// see `COVERAGE.md`.)
#[derive(Default)]
pub struct Keys {
    pub locals: BTreeMap<usize, KeySet>,
}

impl Analysis for Keys {
    type Domain = KeySet;

    fn bottom(&self) -> KeySet {
        KeySet::new()
    }

    fn make(
        &self,
        node: &ENode,
        get: &dyn Fn(Id) -> KeySet,
        arity: &dyn Fn(Id) -> usize,
    ) -> KeySet {
        match node {
            // Grouping emits one row per group: cols 0..|group_key| are a key.
            ENode::Reduce { group_key, .. } => {
                let mut s = KeySet::new();
                s.insert((0..group_key.len()).collect());
                s
            }
            // Selecting/appending columns keeps the input's keys (input column
            // indices are unchanged).
            ENode::Filter { input, .. } | ENode::Map { input, .. } | ENode::Threshold { input } => {
                get(*input)
            }
            ENode::Project { input, outputs } => project_keys(&get(*input), outputs),
            // The columns of a join are the concatenation of its inputs', so a
            // key of each input (offset by that input's start column) unions to
            // a key of the join.
            ENode::Join { inputs, .. } | ENode::WcoJoin { inputs, .. } => {
                join_keys(inputs, &|i| get(i), &|i| arity(i))
            }
            // A recursive reference: keys proven by the Rel-level fixpoint.
            ENode::LocalGet { id, .. } => self.locals.get(id).cloned().unwrap_or_default(),
            // No keys established (or, for Negate/Constant/Get, not known).
            _ => KeySet::new(),
        }
    }

    fn merge(&self, mut a: KeySet, b: KeySet) -> KeySet {
        a.extend(b);
        a
    }
}

/// Keys of a join of `inputs`: pick one key per input, offset it by that
/// input's start column, and union across inputs. The result is the set of all
/// such combinations. If any input has no known key, the join has none (that
/// input's columns are then undetermined).
fn join_keys(
    inputs: &[Id],
    keys_of: &dyn Fn(Id) -> KeySet,
    arity_of: &dyn Fn(Id) -> usize,
) -> KeySet {
    let parts: Vec<(KeySet, usize)> = inputs.iter().map(|&i| (keys_of(i), arity_of(i))).collect();
    combine_join_keys(&parts)
}

/// Combine per-input `(keyset, arity)` (in column order) into the join's keys:
/// every choice of one key per input, each offset by the running column start.
/// If any input has no known key, the join has none.
fn combine_join_keys(parts: &[(KeySet, usize)]) -> KeySet {
    let mut offset = 0usize;
    let mut combos: Vec<Key> = vec![Key::new()];
    for (ks, ar) in parts {
        if ks.is_empty() {
            return KeySet::new();
        }
        let off = offset;
        let mut next = Vec::new();
        for base in &combos {
            for k in ks {
                let mut nk = base.clone();
                nk.extend(k.iter().map(|c| c + off));
                next.push(nk);
            }
        }
        combos = next;
        offset += ar;
    }
    combos.into_iter().collect()
}

// --- recursion: the same analysis as a fixpoint over `LetRec` ---------------
//
// The e-class `Analysis` above is solved by a monotone fixpoint over the
// e-graph (`EGraph::run_analysis`). A *recursive* binding needs the very same
// shape, just with the recursive `LocalGet` as the iterated variable: assume a
// fact for the binding, evaluate its body, and repeat to a fixpoint. That is
// what makes "the analysis framework and recursion are one mechanism" concrete
// rather than aspirational — this driver is a second instance of the identical
// idea, over `Rel` trees with `LetRec`/`LocalGet`.

/// The direction a recursive fixpoint is iterated from.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Least fixpoint: start a recursive variable at ⊥ and grow. Right for
    /// facts that *under-approximate* (e.g. the keys we can be sure of — a
    /// recursive reference contributes none until proven).
    Lfp,
    /// Greatest fixpoint: start a recursive variable at ⊤ and shrink. Right for
    /// *invariants* we want to guarantee of the whole fixpoint (non-negativity,
    /// monotonicity): assume the property holds of the recursive reference and
    /// keep it only if the body preserves it.
    Gfp,
}

/// An analysis evaluated over a `Rel` tree, recursion-aware. Mirrors the
/// e-class [`Analysis`]: a lattice `Domain` with a transfer function `make`,
/// but parameterized by the [`Direction`] from which a recursive binding's
/// fixpoint is approached.
pub trait RecAnalysis {
    type Domain: Clone + Eq;

    /// The starting fact for a recursive variable (⊥ for [`Direction::Lfp`], ⊤
    /// for [`Direction::Gfp`]).
    fn start(&self) -> Self::Domain;

    fn direction(&self) -> Direction;

    /// Transfer for a non-scope node, reading each child's fact via `child`.
    /// Never called on `Let`/`LetRec`/`LocalGet` — the driver handles those.
    fn make(&self, rel: &Rel, child: &dyn Fn(&Rel) -> Self::Domain) -> Self::Domain;
}

type Env<T> = BTreeMap<usize, T>;

/// Evaluate `a` over `rel`, resolving `LetRec`/`Let`/`LocalGet` against `env`.
fn rec_eval<A: RecAnalysis>(a: &A, rel: &Rel, env: &Env<A::Domain>) -> A::Domain {
    match rel {
        Rel::LocalGet { id, .. } => env.get(id).cloned().unwrap_or_else(|| a.start()),
        Rel::Let { id, value, body } => {
            let v = rec_eval(a, value, env);
            let mut env2 = env.clone();
            env2.insert(*id, v);
            rec_eval(a, body, &env2)
        }
        Rel::LetRec { bindings, body } => {
            let env2 = rec_solve(a, bindings, env);
            rec_eval(a, body, &env2)
        }
        _ => a.make(rel, &|c| rec_eval(a, c, env)),
    }
}

/// Solve a `LetRec`'s mutually-recursive bindings to a fixpoint (Gauss–Seidel:
/// each binding sees the latest facts). The transfer is monotone and the
/// lattice finite, so starting every binding at [`RecAnalysis::start`] for the
/// chosen direction converges to the least/greatest fixpoint.
fn rec_solve<A: RecAnalysis>(
    a: &A,
    bindings: &[(usize, Rel)],
    outer: &Env<A::Domain>,
) -> Env<A::Domain> {
    let mut env = outer.clone();
    for (id, _) in bindings {
        env.insert(*id, a.start());
    }
    loop {
        let mut changed = false;
        for (id, value) in bindings {
            let next = rec_eval(a, value, &env);
            if env.get(id) != Some(&next) {
                env.insert(*id, next);
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    env
}

/// Run a recursion-aware analysis over a whole plan.
pub fn rec_analyze<A: RecAnalysis>(a: &A, rel: &Rel) -> A::Domain {
    rec_eval(a, rel, &Env::new())
}

/// Facts about `LocalGet`-bound relations, proven by the recursion-aware
/// fixpoints, to seed the e-class analyses while saturating a binding fragment
/// (option B: the recursion analysis feeds the in-fragment rewriter, so an
/// analysis-gated rule can fire on a provably non-negative / monotone / keyed
/// recursive reference). Maps a bound id to its fact.
#[derive(Clone, Default, Debug)]
pub struct LocalFacts {
    pub nonneg: BTreeMap<usize, bool>,
    pub monotonic: BTreeMap<usize, bool>,
    pub keys: BTreeMap<usize, KeySet>,
}

/// Solve the recursion fixpoints for a `LetRec`'s `bindings`, given the facts
/// `outer` already known for enclosing bound ids. Returns facts for the bound
/// ids (and the inherited outer ones), ready to seed fragment saturation.
pub fn letrec_local_facts(bindings: &[(usize, Rel)], outer: &LocalFacts) -> LocalFacts {
    LocalFacts {
        nonneg: rec_solve(&NonNegRec, bindings, &outer.nonneg),
        monotonic: rec_solve(&MonotonicRec, bindings, &outer.monotonic),
        keys: rec_solve(&KeysRec, bindings, &outer.keys),
    }
}

/// Non-negativity as a recursion-aware (greatest-fixpoint) invariant.
struct NonNegRec;
impl RecAnalysis for NonNegRec {
    type Domain = bool;
    fn start(&self) -> bool {
        true
    }
    fn direction(&self) -> Direction {
        Direction::Gfp
    }
    fn make(&self, rel: &Rel, child: &dyn Fn(&Rel) -> bool) -> bool {
        !matches!(rel, Rel::Negate { .. }) && rel.children().iter().all(|c| child(c))
    }
}

/// Monotonicity (insert-only) as a recursion-aware (greatest-fixpoint)
/// invariant: assume the recursive collection is monotone and keep that only if
/// the binding body preserves it. A `Union(base, f(x))` recursion stays
/// monotone; introducing a `Negate(x)` or a `Reduce` over the cycle collapses
/// it to `false`.
struct MonotonicRec;
impl RecAnalysis for MonotonicRec {
    type Domain = bool;
    fn start(&self) -> bool {
        true
    }
    fn direction(&self) -> Direction {
        Direction::Gfp
    }
    fn make(&self, rel: &Rel, child: &dyn Fn(&Rel) -> bool) -> bool {
        match rel {
            Rel::Negate { .. } | Rel::Reduce { .. } | Rel::TopK { .. } => false,
            Rel::Constant { .. } | Rel::Get { .. } => true,
            other => other.children().iter().all(|c| child(c)),
        }
    }
}

/// Unique keys as a recursion-aware (least-fixpoint) under-approximation: a
/// recursive reference contributes no key until the body proves one, which is
/// the sound conservative direction. (The *precise* recursive key is a greatest
/// fixpoint, but its ⊤ — "every column subset is a key" — is unbounded, so we
/// keep the safe Lfp answer; see `COVERAGE.md`.)
struct KeysRec;
impl RecAnalysis for KeysRec {
    type Domain = KeySet;
    fn start(&self) -> KeySet {
        KeySet::new()
    }
    fn direction(&self) -> Direction {
        Direction::Lfp
    }
    fn make(&self, rel: &Rel, child: &dyn Fn(&Rel) -> KeySet) -> KeySet {
        match rel {
            Rel::Reduce { group_key, .. } => {
                let mut s = KeySet::new();
                s.insert((0..group_key.len()).collect());
                s
            }
            Rel::Filter { input, .. } | Rel::Map { input, .. } | Rel::Threshold { input } => {
                child(input)
            }
            Rel::Project { input, outputs } => project_keys(&child(input), outputs),
            Rel::Join { inputs, .. } | Rel::WcoJoin { inputs, .. } => {
                let parts: Vec<(KeySet, usize)> =
                    inputs.iter().map(|r| (child(r), r.arity())).collect();
                combine_join_keys(&parts)
            }
            _ => KeySet::new(),
        }
    }
}

/// Push a key set through a projection `outputs` (a list of source columns):
/// a key survives iff all its columns are retained, remapped to their output
/// positions.
pub(crate) fn project_keys(keys: &KeySet, outputs: &[usize]) -> KeySet {
    let retained: BTreeSet<usize> = outputs.iter().copied().collect();
    let mut out = KeySet::new();
    for k in keys {
        if k.is_subset(&retained) {
            let mapped: Key = (0..outputs.len())
                .filter(|&i| k.contains(&outputs[i]))
                .collect();
            out.insert(mapped);
        }
    }
    out
}

/// Whether `cand` (a candidate key, as a column set) is a superkey of some
/// known key in `keys` — i.e. it determines the row.
pub fn is_superkey(keys: &KeySet, cand: &Key) -> bool {
    keys.iter().any(|k| k.is_subset(cand))
}

// --- the same analyses over a plain `Rel` tree (for the optimizer's scope
//     handling and for recursive bindings): each is an instance of `RecAnalysis` --

/// Unique keys of a `Rel` (recursion-aware; conservative across `LetRec`).
pub fn rel_keys(rel: &Rel) -> KeySet {
    rec_analyze(&KeysRec, rel)
}

/// Non-negativity of a `Rel` (recursion-aware greatest fixpoint; conservative:
/// `Negate`-free, and assumed-then-verified across a `LetRec`).
pub fn rel_non_negative(rel: &Rel) -> bool {
    rec_analyze(&NonNegRec, rel)
}

/// Monotonicity (insert-only) of a `Rel` (recursion-aware greatest fixpoint:
/// no `Negate`/`Reduce` over the recursive cycle).
pub fn rel_monotonic(rel: &Rel) -> bool {
    rec_analyze(&MonotonicRec, rel)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eqsat::ir::EScalar;
    use mz_expr::MirScalarExpr;

    fn get(name: &str, arity: usize) -> Rel {
        Rel::Get {
            name: name.into(),
            arity,
        }
    }

    fn col0() -> EScalar {
        EScalar::plain(MirScalarExpr::column(0))
    }

    #[mz_ore::test]
    fn reduce_establishes_a_key_preserved_by_filter() {
        // Filter(Reduce by #0 of R): the group key {0} is a key, kept by Filter.
        let plan = Rel::Filter {
            predicates: vec![col0()],
            input: Box::new(Rel::Reduce {
                input: Box::new(get("R", 2)),
                group_key: vec![col0()],
                aggregates: vec![],
                monotonic: false,
                expected_group_size: None,
            }),
        };
        let keys = rel_keys(&plan);
        assert!(is_superkey(&keys, &[0].into_iter().collect()));
        // A plain Get has no known keys.
        assert!(rel_keys(&get("R", 2)).is_empty());
    }

    #[mz_ore::test]
    fn join_keys_union_offset_input_keys() {
        // Join two relations each keyed on {0}. The first input occupies cols
        // 0.. and the second cols 1.. (each Reduce has arity 1), so the join's
        // key is {0, 1}.
        let keyed = |name: &str| Rel::Reduce {
            input: Box::new(get(name, 2)),
            group_key: vec![col0()],
            aggregates: vec![],
            monotonic: false,
            expected_group_size: None,
        };
        let join = Rel::Join {
            inputs: vec![keyed("R"), keyed("S")],
            equivalences: vec![],
        };
        let keys = rel_keys(&join);
        assert!(is_superkey(&keys, &[0, 1].into_iter().collect()));
        // Neither single column alone determines the join row.
        assert!(!is_superkey(&keys, &[0].into_iter().collect()));

        // A join in which one input has no known key has no known key.
        let join_unkeyed = Rel::Join {
            inputs: vec![keyed("R"), get("S", 2)],
            equivalences: vec![],
        };
        assert!(rel_keys(&join_unkeyed).is_empty());
    }

    #[mz_ore::test]
    fn recursion_fixpoint_decides_monotonicity() {
        // x = R + Filter(p, x): the recursive collection is insert-only, so the
        // greatest-fixpoint analysis (assume monotone, verify the body) keeps
        // `monotonic` true — and non-negative too.
        let mono_rec = Rel::LetRec {
            bindings: vec![(
                0,
                Rel::Union {
                    base: Box::new(get("R", 2)),
                    inputs: vec![Rel::Filter {
                        predicates: vec![col0()],
                        input: Box::new(Rel::LocalGet {
                            id: 0,
                            arity: 2,
                            get: None,
                        }),
                    }],
                },
            )],
            body: Box::new(Rel::LocalGet {
                id: 0,
                arity: 2,
                get: None,
            }),
        };
        assert!(rel_monotonic(&mono_rec));
        assert!(rel_non_negative(&mono_rec));

        // y = R + Negate(y): a retraction over the cycle. The assumption is
        // retracted, so both invariants collapse to false.
        let neg_rec = Rel::LetRec {
            bindings: vec![(
                0,
                Rel::Union {
                    base: Box::new(get("R", 2)),
                    inputs: vec![Rel::Negate {
                        input: Box::new(Rel::LocalGet {
                            id: 0,
                            arity: 2,
                            get: None,
                        }),
                    }],
                },
            )],
            body: Box::new(Rel::LocalGet {
                id: 0,
                arity: 2,
                get: None,
            }),
        };
        assert!(!rel_monotonic(&neg_rec));
        assert!(!rel_non_negative(&neg_rec));
    }

    #[mz_ore::test]
    fn monotonic_breaks_under_negate_and_reduce() {
        assert!(rel_monotonic(&get("R", 2)));
        assert!(rel_monotonic(&Rel::Filter {
            predicates: vec![col0()],
            input: Box::new(get("R", 2)),
        }));
        assert!(!rel_monotonic(&Rel::Negate {
            input: Box::new(get("R", 2)),
        }));
        // A Reduce breaks monotonicity even though it preserves non-negativity.
        let reduced = Rel::Reduce {
            input: Box::new(get("R", 2)),
            group_key: vec![col0()],
            aggregates: vec![],
            monotonic: false,
            expected_group_size: None,
        };
        assert!(!rel_monotonic(&reduced));
        assert!(rel_non_negative(&reduced));
    }
}
