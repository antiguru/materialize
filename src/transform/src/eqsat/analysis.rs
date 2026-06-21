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

use mz_expr::{BinaryFunc, Columns, MirScalarExpr};
use mz_repr::{Datum, ReprScalarType};

use crate::analysis::equivalences::{EquivalenceClasses, aggregate_is_input};
use crate::eqsat::egraph::{ENode, Id};
use crate::eqsat::ir::{Col, EScalar, Rel};

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

/// Equivalence-class analysis: per e-class, the scalar-expression equivalences
/// known to hold over the relation the class denotes.
///
/// Reuses Materialize's `EquivalenceClasses` (the same type the production
/// `Equivalences` analysis produces), built here by mirroring that analysis's
/// per-operator derivation in `analysis/equivalences.rs`.
///
/// `None` means the relation is empty (vacuously all expressions equivalent),
/// the top of the lattice. `Some(default)` means no equivalences are known, the
/// bottom. `merge` combines the facts of two e-nodes that denote the same
/// relation, so it asserts all equivalences of both and re-minimizes.
pub struct Equivalences {
    pub locals: BTreeMap<usize, Option<EquivalenceClasses>>,
}

impl Analysis for Equivalences {
    type Domain = Option<EquivalenceClasses>;

    fn bottom(&self) -> Self::Domain {
        Some(EquivalenceClasses::default())
    }

    fn make(
        &self,
        node: &ENode,
        get: &dyn Fn(Id) -> Self::Domain,
        arity: &dyn Fn(Id) -> usize,
    ) -> Self::Domain {
        // Mirror `Equivalences::derive` in analysis/equivalences.rs, arm by arm.
        // Scalar payloads are `EScalar`; use `.expr` to get the `MirScalarExpr`.
        match node {
            // Leaves with no useful row data: emit empty equivalences (bottom).
            // The eqsat engine bails Constant and Get to opaque leaves; trawling
            // rows or type schemas is not available here.
            ENode::Constant { .. } | ENode::Get { .. } | ENode::Opaque(_) => {
                Some(EquivalenceClasses::default())
            }

            // A recursive reference: use the Rel-level recursion fact if we have
            // one, else assume nothing (conservative).
            ENode::LocalGet { id, .. } => self
                .locals
                .get(id)
                .cloned()
                .unwrap_or_else(|| Some(EquivalenceClasses::default())),

            // Project: restrict equivalences to the surviving columns, introducing
            // equivalences for repeated output positions.
            ENode::Project { input, outputs } => {
                let mut equivalences = get(*input);
                equivalences
                    .as_mut()
                    .map(|e| e.project(outputs.iter().cloned()));
                equivalences
            }

            // Map: for each new scalar at position (input_arity + pos), record
            // the equivalence [column(input_arity+pos), scalar_expr].
            ENode::Map { input, scalars } => {
                let mut equivalences = get(*input);
                if let Some(equivalences) = &mut equivalences {
                    let input_arity = arity(*input);
                    for (pos, e) in scalars.iter().enumerate() {
                        equivalences.classes.push(vec![
                            MirScalarExpr::column(input_arity + pos),
                            e.expr.clone(),
                        ]);
                    }
                }
                equivalences
            }

            // Filter: add a class that equates all predicates with literal true.
            ENode::Filter { input, predicates } => {
                let mut equivalences = get(*input);
                if let Some(equivalences) = &mut equivalences {
                    let mut class: Vec<MirScalarExpr> =
                        predicates.iter().map(|p| p.expr.clone()).collect();
                    class.push(MirScalarExpr::literal_ok(Datum::True, ReprScalarType::Bool));
                    equivalences.classes.push(class);
                }
                equivalences
            }

            // Join: permute each input's equivalences to the join's column
            // layout, then extend with the join's own equivalence scalars.
            // If any input is None (empty), the whole join is None.
            ENode::Join {
                inputs,
                equivalences: join_equivs,
            }
            | ENode::WcoJoin {
                inputs,
                equivalences: join_equivs,
            } => {
                let mut result = Some(EquivalenceClasses::default());
                let mut columns: usize = 0;
                for &inp in inputs.iter() {
                    let input_arity = arity(inp);
                    let child_equivs = get(inp);
                    if let Some(mut child_equivs) = child_equivs {
                        let permutation: Vec<usize> = (columns..(columns + input_arity)).collect();
                        child_equivs.permute(&permutation);
                        result
                            .as_mut()
                            .map(|e| e.classes.extend(child_equivs.classes));
                    } else {
                        result = None;
                    }
                    columns += input_arity;
                }
                // Fold the join's own equivalence scalars (each vec is one class).
                result.as_mut().map(|e| {
                    e.classes.extend(
                        join_equivs
                            .iter()
                            .map(|class| class.iter().map(|s| s.expr.clone()).collect()),
                    )
                });
                result
            }

            // Reduce: mirror lines 204-252 of derive.
            // Add group-key column equivalences as if a Map, minimize, project
            // to key columns, then handle input-passthrough aggregates.
            ENode::Reduce {
                input,
                group_key,
                aggregates,
                ..
            } => {
                let input_arity = arity(*input);
                let mut equivalences = get(*input);
                if let Some(equivalences) = &mut equivalences {
                    // Introduce key-column equivalences at positions input_arity + pos.
                    for (pos, expr) in group_key.iter().enumerate() {
                        equivalences.classes.push(vec![
                            MirScalarExpr::column(input_arity + pos),
                            expr.expr.clone(),
                        ]);
                    }
                    // Minimize before projecting so cross-class information is folded.
                    equivalences.minimize(None);

                    // Keep a copy for aggregate reasoning before narrowing.
                    let extended = equivalences.clone();

                    // Project down to the group-key output columns.
                    equivalences.project(input_arity..(input_arity + group_key.len()));

                    // For aggregates that pass through an input datum (MIN/MAX/ANY/ALL),
                    // propagate their equivalences into the output.
                    for (index, aggregate) in aggregates.iter().enumerate() {
                        if aggregate_is_input(&aggregate.func) {
                            let mut temp_equivs = extended.clone();
                            temp_equivs.classes.push(vec![
                                MirScalarExpr::column(input_arity + group_key.len()),
                                aggregate.expr.clone(),
                            ]);
                            temp_equivs.minimize(None);
                            temp_equivs.project(input_arity..(input_arity + group_key.len() + 1));
                            let columns: Vec<usize> = (0..group_key.len())
                                .chain(std::iter::once(group_key.len() + index))
                                .collect();
                            temp_equivs.permute(&columns[..]);
                            equivalences.classes.extend(temp_equivs.classes);
                        }
                    }
                }
                equivalences
            }

            // Passthrough: these operators do not change which rows are present
            // (Negate flips signs but not values; TopK/Threshold filter rows but
            // do not change column values of surviving rows).
            ENode::Negate { input } | ENode::Threshold { input } | ENode::TopK { input, .. } => {
                get(*input)
            }

            // Union: intersection of equivalences across all non-empty branches.
            // Mirrors derive's `flat_map(|c| &results[c])`: None children (empty
            // relations) are vacuously skipped because an empty branch cannot
            // constrain the union's equivalences. If all branches are None (all
            // empty), the union is also None.
            ENode::Union { inputs } => {
                // Collect only the Some values; None (empty relation) is skipped.
                let mut some_iter = inputs.iter().filter_map(|&inp| get(inp));
                let Some(first) = some_iter.next() else {
                    // All children were None (empty): union of empty relations is empty.
                    return None;
                };
                Some(first.union_many(some_iter.collect::<Vec<_>>().iter()))
            }
        }
    }

    fn merge(&self, a: Self::Domain, b: Self::Domain) -> Self::Domain {
        match (a, b) {
            // None = empty relation = absorbing top (vacuously all equivalences hold).
            (None, _) | (_, None) => None,
            (Some(mut a), Some(b)) => {
                a.classes.extend(b.classes);
                // The e-graph can force-equate arbitrary expressions via Union
                // nodes, so a single merge's minimize is bounded to prevent
                // non-termination. Stopping early is a sound under-approximation:
                // fewer known equivalences, never incorrect ones.
                a.minimize_bounded(None, 100);
                Some(a)
            }
        }
    }
}

/// Map from an output column index to the constant scalar value it is known to
/// hold on every row of the relation.
pub type ConstCols = BTreeMap<usize, EScalar>;

/// Constant-column analysis: per e-class, the output columns proven to hold a
/// fixed scalar value on every row.
///
/// The value is stored as a full [`EScalar`], not a bare `Datum`, so that a
/// later generalization from "column equals literal" to "column equals an
/// arbitrary scalar expression" needs no change to the domain.
///
/// Conservative and sound: a column is recorded constant only where an operator
/// forces it (a `Filter` predicate `#i = literal`, a `Map`/`Reduce` key that is
/// itself a literal or a column already known constant). `merge` and `Union`
/// intersect on `(column, value)`, keeping a fact only where both forms agree.
///
/// `locals` carries facts for `LocalGet` references, mirroring [`NonNeg`] and
/// [`Keys`]; within a fragment a recursive reference contributes nothing.
#[derive(Default)]
pub struct ConstantColumns {
    pub locals: BTreeMap<usize, ConstCols>,
}

/// If `pred` is `#i = <literal>` or `<literal> = #i`, return `(i, literal)`.
///
/// The literal is returned as the [`EScalar`] for that side, so the value the
/// analysis stores is the scalar payload itself (preserving its `lit` fact).
fn col_eq_literal(pred: &EScalar) -> Option<(usize, EScalar)> {
    let MirScalarExpr::CallBinary {
        func: BinaryFunc::Eq(_),
        expr1,
        expr2,
    } = &pred.expr
    else {
        return None;
    };
    // One side a bare column reference, the other an `Ok` literal.
    match (expr1.as_column(), expr2.as_column()) {
        (Some(i), None) if expr2.is_literal_ok() => Some((i, EScalar::plain((**expr2).clone()))),
        (None, Some(i)) if expr1.is_literal_ok() => Some((i, EScalar::plain((**expr1).clone()))),
        _ => None,
    }
}

/// Intersect two constant-column maps on `(column, value)`: keep a column only
/// where both maps agree it is constant and agree on the value. This is the
/// sound combination for a `Union` (a column is constant only if every branch
/// agrees on the same value).
fn intersect_const_cols(a: ConstCols, b: &ConstCols) -> ConstCols {
    a.into_iter().filter(|(k, v)| b.get(k) == Some(v)).collect()
}

/// Join two constant-column maps proven of the SAME relation (two e-node forms
/// of one e-class): keep every column EITHER form proves constant. This moves
/// toward more precision, matching the other e-class analyses (`NonNeg`'s `||`,
/// `Keys`' set union), and makes the empty map a left/right identity so the
/// `run_analysis` fold from `bottom` does not annihilate facts. A value conflict
/// means the two forms disagree on a column they both pin, so the relation is
/// contradictory hence empty: drop that column (any value is then vacuously
/// true, and dropping is the safe choice).
fn union_const_cols(mut a: ConstCols, b: &ConstCols) -> ConstCols {
    for (k, v) in b {
        match a.get(k) {
            None => {
                a.insert(*k, v.clone());
            }
            Some(existing) if existing == v => {}
            Some(_) => {
                a.remove(k);
            }
        }
    }
    a
}

impl Analysis for ConstantColumns {
    type Domain = ConstCols;

    fn bottom(&self) -> ConstCols {
        ConstCols::new()
    }

    fn make(
        &self,
        node: &ENode,
        get: &dyn Fn(Id) -> ConstCols,
        arity: &dyn Fn(Id) -> usize,
    ) -> ConstCols {
        match node {
            // Opaque leaves: no column is known constant.
            ENode::Constant { .. } | ENode::Get { .. } | ENode::Opaque(_) => ConstCols::new(),

            // A recursive reference: use the Rel-level recursion fact if we have
            // one, else assume nothing (missing knowledge, never wrong).
            ENode::LocalGet { id, .. } => self.locals.get(id).cloned().unwrap_or_default(),

            // Filter: the input's constants survive, plus any column pinned to a
            // literal by an equality predicate.
            ENode::Filter { input, predicates } => {
                let mut cols = get(*input);
                for pred in predicates {
                    if let Some((i, lit)) = col_eq_literal(pred) {
                        cols.insert(i, lit);
                    }
                }
                cols
            }

            // Map: input columns keep their indices; each appended scalar lands
            // at `input_arity + pos`. A scalar that is itself a literal is
            // constant; a bare column reference inherits the input column's
            // constant (if any).
            ENode::Map { input, scalars } => {
                let cols = get(*input);
                let input_arity = arity(*input);
                let mut out = cols.clone();
                for (pos, e) in scalars.iter().enumerate() {
                    let out_col = input_arity + pos;
                    if e.expr.is_literal_ok() {
                        out.insert(out_col, e.clone());
                    } else if let Some(j) = e.is_col() {
                        if let Some(v) = cols.get(&j) {
                            out.insert(out_col, v.clone());
                        }
                    }
                }
                out
            }

            // Project: remap input-column constants to their output positions. A
            // repeated source column independently pins each output position.
            ENode::Project { input, outputs } => {
                let cols = get(*input);
                let mut out = ConstCols::new();
                for (new, &old) in outputs.iter().enumerate() {
                    if let Some(v) = cols.get(&old) {
                        out.insert(new, v.clone());
                    }
                }
                out
            }

            // Join: the output columns are the concatenation of the inputs', so
            // shift each input's constants by its running column offset. A join
            // equivalence that links a column to a literal also pins that column.
            ENode::Join {
                inputs,
                equivalences: join_equivs,
            }
            | ENode::WcoJoin {
                inputs,
                equivalences: join_equivs,
            } => {
                let mut out = ConstCols::new();
                let mut offset = 0usize;
                for &inp in inputs.iter() {
                    let input_arity = arity(inp);
                    for (col, v) in get(inp) {
                        out.insert(offset + col, v);
                    }
                    offset += input_arity;
                }
                // Forward pass over join equivalences: a class that contains a
                // literal pins every bare-column member of that class.
                for class in join_equivs {
                    let lit = class.iter().find(|s| s.expr.is_literal_ok());
                    if let Some(lit) = lit {
                        for s in class {
                            if let Some(col) = s.is_col() {
                                out.insert(col, EScalar::plain(lit.expr.clone()));
                            }
                        }
                    }
                }
                out
            }

            // Reduce: the output columns `0..group_key.len()` are the group-key
            // expressions. A key that is a literal, or a bare column reference to
            // an input column already known constant, is constant in the output.
            // Aggregate columns (after the key) are generally not constant.
            ENode::Reduce {
                input, group_key, ..
            } => {
                let cols = get(*input);
                let mut out = ConstCols::new();
                for (pos, gk) in group_key.iter().enumerate() {
                    if gk.expr.is_literal_ok() {
                        out.insert(pos, gk.clone());
                    } else if let Some(j) = gk.is_col() {
                        if let Some(v) = cols.get(&j) {
                            out.insert(pos, v.clone());
                        }
                    }
                }
                out
            }

            // Passthrough: these change multiplicities or row presence but not
            // the column values of surviving rows.
            ENode::Negate { input } | ENode::Threshold { input } | ENode::TopK { input, .. } => {
                get(*input)
            }

            // Union: a column is constant only where every branch agrees on its
            // value. Intersect across branches. Empty input yields no constants.
            ENode::Union { inputs } => {
                let mut iter = inputs.iter().map(|&inp| get(inp));
                let Some(first) = iter.next() else {
                    return ConstCols::new();
                };
                iter.fold(first, |acc, next| intersect_const_cols(acc, &next))
            }
        }
    }

    fn merge(&self, a: ConstCols, b: ConstCols) -> ConstCols {
        // The two e-nodes denote the same relation, so a column proven constant
        // in EITHER form is constant in the relation: join both forms' facts.
        // This is a meet toward more precision (like the other analyses), and
        // crucially makes `bottom` the identity so the fold from `bottom` in
        // `run_analysis` preserves facts rather than annihilating them. A value
        // conflict means an empty relation, so that column is dropped. The map
        // grows monotonically and is bounded by the relation's arity, so the
        // fixpoint terminates.
        union_const_cols(a, &b)
    }
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
    /// Per-binding equivalence classes, seeded `Some(default)` for LetRec
    /// bindings (conservative: we assume no equivalences are known for the
    /// recursive reference until a fixpoint is computed). A full recursion-aware
    /// equivalences fixpoint is left as future work; the conservative seeding
    /// is sound but misses facts provable only across recursive steps.
    pub equivalences: BTreeMap<usize, Option<EquivalenceClasses>>,
    /// Per-binding constant columns. Recursive bindings are seeded absent
    /// (empty), which is sound: missing knowledge, never wrong knowledge. A full
    /// recursion-aware fixpoint is unnecessary because the analysis only ever
    /// loses facts across a recursive step.
    pub constant_columns: BTreeMap<usize, ConstCols>,
}

/// Solve the recursion fixpoints for a `LetRec`'s `bindings`, given the facts
/// `outer` already known for enclosing bound ids. Returns facts for the bound
/// ids (and the inherited outer ones), ready to seed fragment saturation.
pub fn letrec_local_facts(bindings: &[(usize, Rel)], outer: &LocalFacts) -> LocalFacts {
    // Seed each binding's equivalences conservatively at Some(default). A full
    // recursion-aware fixpoint for EquivalenceClasses is future work; for now we
    // inherit only the outer facts and seed new bindings as unknown.
    let mut equivalences = outer.equivalences.clone();
    for (id, _) in bindings {
        equivalences
            .entry(*id)
            .or_insert_with(|| Some(EquivalenceClasses::default()));
    }
    LocalFacts {
        nonneg: rec_solve(&NonNegRec, bindings, &outer.nonneg),
        monotonic: rec_solve(&MonotonicRec, bindings, &outer.monotonic),
        keys: rec_solve(&KeysRec, bindings, &outer.keys),
        equivalences,
        // Inherit only the outer constant columns; recursive bindings are seeded
        // absent (sound under-approximation). No recursion fixpoint needed.
        constant_columns: outer.constant_columns.clone(),
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

    // --- Equivalences analysis tests ------------------------------------------

    /// Check that a Filter over a bottom input (no equivalences known on the
    /// input) with predicate `#0 = #1` yields a class containing
    /// `{#0, #1, true}` after minimize. The equality predicate is pushed into a
    /// class together with literal true, so minimize unpacks `Eq(#0,#1) = true`
    /// into the class `[#0, #1]`.
    #[mz_ore::test]
    fn equivalences_filter_equality_class() {
        let analysis = Equivalences {
            locals: BTreeMap::new(),
        };
        // ENode ids: 0 = input leaf (Get), 1 = Filter over it.
        // Input: 2 columns (arity 2), no known equivalences.
        let input_id: Id = 0;
        let input_arity = 2usize;

        // Predicate: #0 = #1
        let pred = EScalar::plain(MirScalarExpr::column(0).call_binary(
            MirScalarExpr::column(1),
            mz_expr::BinaryFunc::Eq(mz_expr::func::Eq),
        ));

        let filter_node = ENode::Filter {
            input: input_id,
            predicates: vec![pred],
        };

        // get(input_id) returns bottom (no equivalences).
        let get = |_: Id| analysis.bottom();
        let arity = |_: Id| input_arity;

        let result = analysis.make(&filter_node, &get, &arity);
        let mut result = result.expect("Filter over non-empty input must be Some");
        result.minimize(None);

        // After minimize, the class [Eq(#0,#1), true] is unpacked:
        // minimize_once detects Eq(x,y)=true and adds class [x, y], so
        // columns 0 and 1 must be in the same equivalence class.
        let col0 = MirScalarExpr::column(0);
        let col1 = MirScalarExpr::column(1);
        let reducer = result.reducer();
        let canon0 = reducer.get(&col0).unwrap_or(&col0);
        let canon1 = reducer.get(&col1).unwrap_or(&col1);
        assert_eq!(
            canon0, canon1,
            "Filter[#0=#1]: columns 0 and 1 must be equivalent; classes={:?}",
            result.classes
        );
    }

    /// A Join of two arity-1 inputs with join equivalence [#0, #1] (equating
    /// the first column of each input at the join's combined layout) must yield
    /// a class equating columns 0 and 1 of the joined output.
    #[mz_ore::test]
    fn equivalences_join_equiv_offsets() {
        let analysis = Equivalences {
            locals: BTreeMap::new(),
        };
        // Two inputs each with arity 1 at ids 0 and 1.
        let left_id: Id = 0;
        let right_id: Id = 1;

        // Join equivalence: column 0 (from left, offset 0) = column 1 (from right, offset 1).
        let join_node = ENode::Join {
            inputs: vec![left_id, right_id],
            equivalences: vec![vec![
                EScalar::plain(MirScalarExpr::column(0)),
                EScalar::plain(MirScalarExpr::column(1)),
            ]],
        };

        let get = |_: Id| Some(EquivalenceClasses::default());
        let arity = |_: Id| 1usize;

        let result = analysis.make(&join_node, &get, &arity);
        let mut result = result.expect("Join of non-empty inputs must be Some");
        result.minimize(None);

        let col0 = MirScalarExpr::column(0);
        let col1 = MirScalarExpr::column(1);
        let reducer = result.reducer();
        let canon0 = reducer.get(&col0).unwrap_or(&col0);
        let canon1 = reducer.get(&col1).unwrap_or(&col1);
        assert_eq!(
            canon0, canon1,
            "Join[#0=#1]: columns 0 and 1 must be equivalent; classes={:?}",
            result.classes
        );
    }

    /// `merge(Some(default), x) == x` (bottom is the identity) and
    /// `merge(None, x) == None` (None is absorbing).
    #[mz_ore::test]
    fn equivalences_merge_identity_and_absorbing() {
        let analysis = Equivalences {
            locals: BTreeMap::new(),
        };
        let bottom = analysis.bottom();
        let top: Option<EquivalenceClasses> = None;

        // Build a non-trivial Some value: {#0, #1} in one class.
        let mut ec = EquivalenceClasses::default();
        ec.classes
            .push(vec![MirScalarExpr::column(0), MirScalarExpr::column(1)]);

        // bottom is identity for merge.
        let merged = analysis.merge(bottom.clone(), Some(ec.clone()));
        let merged = merged.expect("merge(bottom, Some(_)) must be Some");
        let col0 = MirScalarExpr::column(0);
        let col1 = MirScalarExpr::column(1);
        let reducer = merged.reducer();
        let canon0 = reducer.get(&col0).unwrap_or(&col0);
        let canon1 = reducer.get(&col1).unwrap_or(&col1);
        assert_eq!(
            canon0, canon1,
            "merge(bottom, Some([#0,#1])): columns 0 and 1 must be equivalent"
        );

        // None is absorbing.
        assert_eq!(
            analysis.merge(top.clone(), Some(ec.clone())),
            None,
            "merge(None, x) must be None"
        );
        assert_eq!(
            analysis.merge(Some(ec.clone()), top),
            None,
            "merge(x, None) must be None"
        );
    }

    /// `merge` of `{#0=#1}` and `{#1=#2}` yields a single class `{#0,#1,#2}`
    /// after minimize (transitivity closure).
    #[mz_ore::test]
    fn equivalences_merge_transitive_closure() {
        let analysis = Equivalences {
            locals: BTreeMap::new(),
        };

        let mut ec1 = EquivalenceClasses::default();
        ec1.classes
            .push(vec![MirScalarExpr::column(0), MirScalarExpr::column(1)]);

        let mut ec2 = EquivalenceClasses::default();
        ec2.classes
            .push(vec![MirScalarExpr::column(1), MirScalarExpr::column(2)]);

        let merged = analysis.merge(Some(ec1), Some(ec2));
        let merged = merged.expect("merge of two Some must be Some");

        let col0 = MirScalarExpr::column(0);
        let col1 = MirScalarExpr::column(1);
        let col2 = MirScalarExpr::column(2);
        let reducer = merged.reducer();
        let canon0 = reducer.get(&col0).unwrap_or(&col0);
        let canon1 = reducer.get(&col1).unwrap_or(&col1);
        let canon2 = reducer.get(&col2).unwrap_or(&col2);
        assert_eq!(
            canon0, canon1,
            "merge of [#0=#1] and [#1=#2]: 0 and 1 must be equivalent; classes={:?}",
            merged.classes
        );
        assert_eq!(
            canon1, canon2,
            "merge of [#0=#1] and [#1=#2]: 1 and 2 must be equivalent; classes={:?}",
            merged.classes
        );
        assert_eq!(
            canon0, canon2,
            "merge of [#0=#1] and [#1=#2]: 0 and 2 must be equivalent (transitivity); classes={:?}",
            merged.classes
        );
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

    // --- ConstantColumns analysis tests --------------------------------------

    /// A literal `1` as an `EScalar` (for building predicates and group keys).
    fn lit1() -> EScalar {
        EScalar::plain(MirScalarExpr::literal_ok(
            Datum::Int64(1),
            ReprScalarType::Int64,
        ))
    }

    /// The predicate `#col = 1`.
    fn col_eq_one(col: usize) -> EScalar {
        EScalar::plain(MirScalarExpr::column(col).call_binary(
            MirScalarExpr::literal_ok(Datum::Int64(1), ReprScalarType::Int64),
            BinaryFunc::Eq(mz_expr::func::Eq),
        ))
    }

    fn cc() -> ConstantColumns {
        ConstantColumns {
            locals: BTreeMap::new(),
        }
    }

    /// `Filter[#1 = 1]` pins output column 1 to the literal `1`; other columns
    /// stay unknown.
    #[mz_ore::test]
    fn constant_columns_filter_pins_column() {
        let analysis = cc();
        let node = ENode::Filter {
            input: 0,
            predicates: vec![col_eq_one(1)],
        };
        let get = |_: Id| analysis.bottom();
        let arity = |_: Id| 2usize;
        let result = analysis.make(&node, &get, &arity);
        assert_eq!(result.get(&1), Some(&lit1()));
        assert_eq!(result.get(&0), None);
    }

    /// A non-equality predicate, or an equality between two columns, pins
    /// nothing.
    #[mz_ore::test]
    fn constant_columns_filter_ignores_non_literal_equality() {
        let analysis = cc();
        // #0 = #1: no literal side.
        let pred = EScalar::plain(
            MirScalarExpr::column(0)
                .call_binary(MirScalarExpr::column(1), BinaryFunc::Eq(mz_expr::func::Eq)),
        );
        let node = ENode::Filter {
            input: 0,
            predicates: vec![pred],
        };
        let get = |_: Id| analysis.bottom();
        let arity = |_: Id| 2usize;
        assert!(analysis.make(&node, &get, &arity).is_empty());
    }

    /// `Map` appends a literal at `input_arity + pos` and propagates a constant
    /// through a bare column reference.
    #[mz_ore::test]
    fn constant_columns_map_literal_and_column_ref() {
        let analysis = cc();
        // Input arity 2, with column 0 known constant `1`.
        let mut input_cols = ConstCols::new();
        input_cols.insert(0, lit1());
        // Scalars: [literal 1, #0]. Output columns 2 and 3.
        let node = ENode::Map {
            input: 0,
            scalars: vec![lit1(), EScalar::plain(MirScalarExpr::column(0))],
        };
        let get = |_: Id| input_cols.clone();
        let arity = |_: Id| 2usize;
        let result = analysis.make(&node, &get, &arity);
        // Input column 0 stays constant.
        assert_eq!(result.get(&0), Some(&lit1()));
        // Appended literal at column 2.
        assert_eq!(result.get(&2), Some(&lit1()));
        // Appended #0 inherits column 0's constant at column 3.
        assert_eq!(result.get(&3), Some(&lit1()));
    }

    /// `Project` remaps input-column constants to output positions, including a
    /// repeated source column.
    #[mz_ore::test]
    fn constant_columns_project_remaps() {
        let analysis = cc();
        let mut input_cols = ConstCols::new();
        input_cols.insert(2, lit1());
        // outputs = [2, 0, 2]: positions 0 and 2 both come from source column 2.
        let node = ENode::Project {
            input: 0,
            outputs: vec![2, 0, 2],
        };
        let get = |_: Id| input_cols.clone();
        let arity = |_: Id| 3usize;
        let result = analysis.make(&node, &get, &arity);
        assert_eq!(result.get(&0), Some(&lit1()));
        assert_eq!(result.get(&1), None);
        assert_eq!(result.get(&2), Some(&lit1()));
    }

    /// `Join` shifts each input's constants by its running column offset.
    #[mz_ore::test]
    fn constant_columns_join_offsets_inputs() {
        let analysis = cc();
        // Two inputs, each arity 2. Each input has column 0 constant `1`.
        let mut per_input = ConstCols::new();
        per_input.insert(0, lit1());
        let node = ENode::Join {
            inputs: vec![0, 1],
            equivalences: vec![],
        };
        let get = |_: Id| per_input.clone();
        let arity = |_: Id| 2usize;
        let result = analysis.make(&node, &get, &arity);
        // Left input column 0 -> output 0; right input column 0 -> output 2.
        assert_eq!(result.get(&0), Some(&lit1()));
        assert_eq!(result.get(&2), Some(&lit1()));
        assert_eq!(result.get(&1), None);
        assert_eq!(result.get(&3), None);
    }

    /// A join equivalence class containing a literal pins every bare-column
    /// member of that class.
    #[mz_ore::test]
    fn constant_columns_join_equivalence_literal() {
        let analysis = cc();
        // Class: [#0, literal 1]: column 0 must equal 1.
        let node = ENode::Join {
            inputs: vec![0, 1],
            equivalences: vec![vec![EScalar::plain(MirScalarExpr::column(0)), lit1()]],
        };
        let get = |_: Id| ConstCols::new();
        let arity = |_: Id| 1usize;
        let result = analysis.make(&node, &get, &arity);
        assert_eq!(result.get(&0), Some(&lit1()));
    }

    /// `Reduce` pins a group-key position that is a literal or a column already
    /// constant in the input.
    #[mz_ore::test]
    fn constant_columns_reduce_group_key() {
        let analysis = cc();
        let mut input_cols = ConstCols::new();
        input_cols.insert(3, lit1());
        // group_key = [literal 1, #3]: output positions 0 and 1 are constant.
        let node = ENode::Reduce {
            input: 0,
            group_key: vec![lit1(), EScalar::plain(MirScalarExpr::column(3))],
            aggregates: vec![],
            monotonic: false,
            expected_group_size: None,
        };
        let get = |_: Id| input_cols.clone();
        let arity = |_: Id| 4usize;
        let result = analysis.make(&node, &get, &arity);
        assert_eq!(result.get(&0), Some(&lit1()));
        assert_eq!(result.get(&1), Some(&lit1()));
    }

    /// Passthrough operators carry the input's constants unchanged.
    #[mz_ore::test]
    fn constant_columns_passthrough() {
        let analysis = cc();
        let mut input_cols = ConstCols::new();
        input_cols.insert(0, lit1());
        let get = |_: Id| input_cols.clone();
        let arity = |_: Id| 2usize;
        for node in [ENode::Negate { input: 0 }, ENode::Threshold { input: 0 }] {
            let result = analysis.make(&node, &get, &arity);
            assert_eq!(result.get(&0), Some(&lit1()));
        }
    }

    /// `Union` keeps a column constant only where every branch agrees on the
    /// value.
    #[mz_ore::test]
    fn constant_columns_union_intersects() {
        let analysis = cc();
        // Branch 0 (id 0): cols {0:1, 1:1}. Branch 1 (id 1): cols {0:1, 1:2}.
        let lit2 = EScalar::plain(MirScalarExpr::literal_ok(
            Datum::Int64(2),
            ReprScalarType::Int64,
        ));
        let mut b0 = ConstCols::new();
        b0.insert(0, lit1());
        b0.insert(1, lit1());
        let mut b1 = ConstCols::new();
        b1.insert(0, lit1());
        b1.insert(1, lit2.clone());
        let node = ENode::Union { inputs: vec![0, 1] };
        let get = |id: Id| if id == 0 { b0.clone() } else { b1.clone() };
        let arity = |_: Id| 2usize;
        let result = analysis.make(&node, &get, &arity);
        // Column 0 agrees (both 1); column 1 disagrees (1 vs 2) so it drops.
        assert_eq!(result.get(&0), Some(&lit1()));
        assert_eq!(result.get(&1), None);
    }

    /// `merge` joins two forms of the same relation: a column proven constant in
    /// EITHER form survives, a value conflict drops the column, and the empty map
    /// is the identity (so folding from `bottom` does not annihilate facts).
    #[mz_ore::test]
    fn constant_columns_merge_unions() {
        let analysis = cc();
        let lit2 = EScalar::plain(MirScalarExpr::literal_ok(
            Datum::Int64(2),
            ReprScalarType::Int64,
        ));
        let mut a = ConstCols::new();
        a.insert(0, lit1());
        a.insert(1, lit1());
        let mut b = ConstCols::new();
        b.insert(0, lit1());
        b.insert(1, lit2);
        b.insert(2, lit1());
        let merged = analysis.merge(a, b);
        // Column 0 agrees (kept); 1 disagrees (dropped); 2 is in only one form
        // (kept: a fact proven by either form holds of the relation).
        assert_eq!(merged.get(&0), Some(&lit1()));
        assert_eq!(merged.get(&1), None);
        assert_eq!(merged.get(&2), Some(&lit1()));
    }

    /// Regression guard for the `run_analysis` fold `d = merge(bottom, make(n))`:
    /// `bottom` (the empty map) must be the identity for `merge`, otherwise every
    /// class collapses to empty and the analysis produces no facts. An opaque
    /// `LocalGet` contributing the empty map must likewise not poison a class it
    /// is unioned into.
    #[mz_ore::test]
    fn constant_columns_merge_bottom_is_identity() {
        let analysis = cc();
        let mut x = ConstCols::new();
        x.insert(0, lit1());
        x.insert(5, lit1());
        assert_eq!(analysis.merge(ConstCols::new(), x.clone()), x);
        assert_eq!(analysis.merge(x.clone(), ConstCols::new()), x);
    }
}
