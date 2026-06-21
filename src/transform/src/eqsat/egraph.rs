// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! An e-graph with **equality saturation** and **relational e-matching via a
//! generic (worst-case-optimal) join**.
//!
//! Greedy, cost-monotone rewriting (apply a rule only if it lowers cost) gets
//! stuck in local minima: a beneficial rewrite is often reachable only through
//! a cost-neutral or cost-increasing intermediate step. To avoid that, the
//! optimizer instead **saturates** an e-graph: it applies *every* rule
//! wherever it matches, regardless of cost, recording the resulting
//! equivalences compactly. Only at the end does it extract the cheapest plan.
//!
//! Finding all matches of a rule's left-hand side is the bottleneck, and it is
//! exactly a **conjunctive query** over the e-graph: each operator in the
//! pattern is an atom over the relation of e-nodes with that operator, and
//! shared pattern variables are join variables. Following *relational
//! e-matching* (Zhang et al.), we evaluate that query with a **generic join**,
//! which is worst-case optimal: it binds one variable at a time, intersecting
//! the candidate values across all atoms that mention it, never materializing a
//! larger intermediate than the final result could justify. That is the sense
//! in which "the engine itself explores the transform graph in a WCOJ manner".

use std::collections::{BTreeMap, HashMap, HashSet};

use mz_expr::{AggregateExpr, Columns, MirRelationExpr, MirScalarExpr};
use mz_repr::ReprColumnType;

use crate::analysis::equivalences::{EquivalenceClasses, ExpressionReducer};
use crate::eqsat::analysis::{
    Analysis, ConstCols, ConstantColumns, Equivalences, KeySet, Keys, LocalFacts, Monotonic,
    NonNeg, is_superkey,
};
use crate::eqsat::cost::{Cost, CostModel};
use crate::eqsat::ir::{EScalar, Rel};
use crate::eqsat::matcher::Payload;
use crate::eqsat::rules::CompiledRuleSet;

/// An e-class identifier.
pub type Id = usize;

/// E-node budget for [`EGraph::saturate`]. Saturation stops growing the e-graph
/// once the total e-node count crosses this bound, then extracts from the
/// partially saturated graph. This caps the worst-case time and memory of an
/// otherwise combinatorial search; extraction from an incomplete saturation is
/// still sound (it just may miss rewrites a fuller search would have found).
///
/// The per-iteration generic join and, especially, the final extraction are
/// superlinear in the e-node count (extraction is a multi-pass DP that rebuilds
/// candidate plans per node per pass), so the bound is kept low: a plan that
/// explodes to a large e-graph costs seconds, which is unacceptable in the live
/// optimizer. Small plans saturate fully well under this bound and are
/// unaffected.
const MAX_ENODES: usize = 600;

/// Per-rule, per-iteration match cap. A rule whose left-hand side matches
/// combinatorially can enumerate an unbounded number of assignments in a single
/// generic join, which is the dominant saturation cost. The enumeration stops at
/// this many matches, and a rule that hits the cap is banned for a growing
/// number of iterations (see [`EGraph::saturate`]). Modeled on egg's
/// `BackoffScheduler`: throttle the offending rule, keep the rest running.
const MATCH_LIMIT: usize = 1_000;

/// Initial ban length (in iterations) for a rule that exceeds [`MATCH_LIMIT`].
/// The ban doubles on each re-offense.
const INITIAL_BAN_LEN: usize = 4;

/// Maximum iterations for [`EGraph::run_analysis`] on the three cheap
/// finite-height analyses (`NonNeg`, `Monotonic`, `Keys`). Those lattices
/// have height bounded by the plan size, so they converge in a handful of
/// rounds -- well under this cap.
const MAX_ANALYSIS_ITERS: usize = 100;

/// Maximum inner fixpoint rounds for the `Equivalences` analysis when it is
/// run inside the saturation loop (once per outer iteration). The analysis is
/// NOT finite-height, so bounding iterations prevents non-termination.
///
/// This cap is intentionally much smaller than [`MAX_ANALYSIS_ITERS`]:
/// each inner round of the Equivalences fixpoint calls
/// `minimize_bounded(None, 100)` per merge, which is itself expensive
/// (expand/implications/minimize_once over `MirScalarExpr` sets). A tight
/// cap keeps per-round cost proportional to plan size. Stopping early is
/// sound: every derived equivalence reflects real node structure, and
/// both consumers (Phase 2a canonicalization and `Unsatisfiable`) are
/// correct with fewer known equivalences -- they miss optimizations, never
/// produce incorrect plans.
///
/// The outer saturation loop repeats the analysis on later rounds when the
/// e-graph changes, so equivalences that require multiple inner rounds to
/// derive still emerge over time -- just spread across outer iterations.
const MAX_EQUIVALENCES_ANALYSIS_ITERS: usize = 4;

/// A node in the e-graph: an operator whose children are e-class ids. Mirrors
/// [`Rel`], with `Union` flattened to a single non-empty input list.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ENode {
    Constant {
        card: u64,
        arity: usize,
        /// The real column types of the relation this node stands in for, when
        /// known. Saturation rules synthesize an `Empty(r)` as a
        /// `Constant { card: 0, .. }`; capturing `r`'s column types here lets
        /// raise emit an empty relation of the correct type. `None` marks a node
        /// with no captured types (raise falls back to an arity-only placeholder).
        col_types: Option<Vec<ReprColumnType>>,
    },
    Get {
        name: String,
        arity: usize,
    },
    Project {
        input: Id,
        outputs: Vec<usize>,
    },
    Map {
        input: Id,
        scalars: Vec<EScalar>,
    },
    Filter {
        input: Id,
        predicates: Vec<EScalar>,
    },
    Reduce {
        input: Id,
        group_key: Vec<EScalar>,
        aggregates: Vec<AggregateExpr>,
        monotonic: bool,
        expected_group_size: Option<u64>,
    },
    TopK {
        input: Id,
        shape: crate::eqsat::ir::TopKShape,
    },
    Negate {
        input: Id,
    },
    Threshold {
        input: Id,
    },
    Join {
        inputs: Vec<Id>,
        equivalences: Vec<Vec<EScalar>>,
    },
    WcoJoin {
        inputs: Vec<Id>,
        equivalences: Vec<Vec<EScalar>>,
    },
    Union {
        inputs: Vec<Id>,
    },
    /// An unsupported subtree carried verbatim (see [`Rel::Opaque`]). An opaque
    /// leaf; hash-consing dedups identical subtrees.
    Opaque(MirRelationExpr),
    /// A reference to a `LetRec`/`Let`-bound local. An opaque leaf inside a
    /// Let-free fragment (the structural optimizer saturates fragments and
    /// peels the binding scopes around them). `get` carries the original node
    /// for raise (`None` for engine scope placeholders).
    LocalGet {
        id: usize,
        arity: usize,
        get: Option<Box<MirRelationExpr>>,
    },
}

/// The operator symbol of an e-node (its relation in the relational view).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Sym {
    Constant,
    Get,
    Project,
    Map,
    Filter,
    Reduce,
    Negate,
    Threshold,
    Join,
    WcoJoin,
    Union,
    TopK,
    Opaque,
    LocalGet,
}

impl ENode {
    fn sym(&self) -> Sym {
        match self {
            ENode::Constant { .. } => Sym::Constant,
            ENode::Get { .. } => Sym::Get,
            ENode::Project { .. } => Sym::Project,
            ENode::Map { .. } => Sym::Map,
            ENode::Filter { .. } => Sym::Filter,
            ENode::Reduce { .. } => Sym::Reduce,
            ENode::Negate { .. } => Sym::Negate,
            ENode::Threshold { .. } => Sym::Threshold,
            ENode::Join { .. } => Sym::Join,
            ENode::WcoJoin { .. } => Sym::WcoJoin,
            ENode::Union { .. } => Sym::Union,
            ENode::TopK { .. } => Sym::TopK,
            ENode::Opaque(_) => Sym::Opaque,
            ENode::LocalGet { .. } => Sym::LocalGet,
        }
    }

    pub fn children(&self) -> Vec<Id> {
        match self {
            ENode::Constant { .. }
            | ENode::Get { .. }
            | ENode::Opaque(_)
            | ENode::LocalGet { .. } => vec![],
            ENode::Project { input, .. }
            | ENode::Map { input, .. }
            | ENode::Filter { input, .. }
            | ENode::Reduce { input, .. }
            | ENode::TopK { input, .. }
            | ENode::Negate { input }
            | ENode::Threshold { input } => vec![*input],
            ENode::Join { inputs, .. }
            | ENode::WcoJoin { inputs, .. }
            | ENode::Union { inputs } => inputs.clone(),
        }
    }

    fn map_children(&self, f: impl Fn(Id) -> Id) -> ENode {
        let mut n = self.clone();
        match &mut n {
            ENode::Constant { .. }
            | ENode::Get { .. }
            | ENode::Opaque(_)
            | ENode::LocalGet { .. } => {}
            ENode::Project { input, .. }
            | ENode::Map { input, .. }
            | ENode::Filter { input, .. }
            | ENode::Reduce { input, .. }
            | ENode::TopK { input, .. }
            | ENode::Negate { input }
            | ENode::Threshold { input } => *input = f(*input),
            ENode::Join { inputs, .. }
            | ENode::WcoJoin { inputs, .. }
            | ENode::Union { inputs } => {
                for i in inputs.iter_mut() {
                    *i = f(*i);
                }
            }
        }
        n
    }
}

/// The polarity demand an operator imposes on a child during extraction.
///
/// Extraction is parameterized by this demand so a multiplicity-signed
/// (`Negate`-rooted) representative is never placed directly under an operator
/// that is unsound over signed multiplicities (a non-linear reduce or a TopK).
///
/// No rule in the current set repositions a `Negate` into a new structural
/// position, so today no extraction can place a `Negate`-rooted representative
/// where this demand would forbid it. The machinery is kept as the soundness
/// foundation for a future negate-repositioning rule: such a rule may merge a
/// `Negate`-rooted form into an arbitrary class only because this demand
/// guarantees the extractor will not then route that form under a non-linear
/// operator. It is the prerequisite, not dead code.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Demand {
    /// No sign constraint: the cheapest representative wins.
    Any,
    /// The representative's output multiplicities must be non-negative.
    Nonneg,
}

/// The polarity demand a reduce imposes on its input.
///
/// A reduce with at least one aggregate is non-linear and requires a `Nonneg`
/// input, because `reduce(r) != negate(reduce(negate(r)))` for a non-linear
/// aggregate (MIN/MAX/ANY/ALL). A reduce with no aggregates is a distinct, which
/// is polarity-insensitive and takes `Any`.
///
/// This is conservative: it demands `Nonneg` for ANY aggregate. Future work can
/// refine this to allow `Any` when every aggregate is linear, via
/// `aggregate_is_input` from `crate::analysis::equivalences`.
fn reduce_input_demand(node: &ENode) -> Demand {
    match node {
        ENode::Reduce { aggregates, .. } if aggregates.is_empty() => Demand::Any,
        ENode::Reduce { .. } => Demand::Nonneg,
        _ => Demand::Any,
    }
}

/// The e-graph.
#[derive(Default)]
pub struct EGraph {
    uf: Vec<Id>,
    pub(crate) classes: HashMap<Id, HashSet<ENode>>,
    memo: HashMap<ENode, Id>,
}

/// The relational view of the e-graph that the compiled rule matchers scan:
/// every e-node grouped by its operator symbol, paired with its canonical
/// parent class. Built by [`EGraph::index`].
pub(crate) type Index = HashMap<Sym, Vec<(Id, ENode)>>;

impl EGraph {
    pub fn new() -> Self {
        EGraph::default()
    }

    /// The canonical id of `id`.
    pub fn find(&self, mut id: Id) -> Id {
        while self.uf[id] != id {
            id = self.uf[id];
        }
        id
    }

    fn new_class(&mut self) -> Id {
        let id = self.uf.len();
        self.uf.push(id);
        self.classes.insert(id, HashSet::new());
        id
    }

    fn canon_enode(&self, n: &ENode) -> ENode {
        n.map_children(|c| self.find(c))
    }

    /// Add an e-node, returning its (canonical) e-class. Hash-conses.
    pub fn add(&mut self, node: ENode) -> Id {
        let node = self.canon_enode(&node);
        if let Some(&id) = self.memo.get(&node) {
            return self.find(id);
        }
        let id = self.new_class();
        self.classes.get_mut(&id).unwrap().insert(node.clone());
        self.memo.insert(node, id);
        id
    }

    /// Add an entire [`Rel`], returning the e-class of its root.
    pub fn add_rel(&mut self, rel: &Rel) -> Id {
        let node = match rel {
            Rel::Constant {
                card,
                arity,
                col_types,
            } => ENode::Constant {
                card: *card,
                arity: *arity,
                col_types: col_types.clone(),
            },
            Rel::Get { name, arity } => ENode::Get {
                name: name.clone(),
                arity: *arity,
            },
            Rel::Project { input, outputs } => ENode::Project {
                input: self.add_rel(input),
                outputs: outputs.clone(),
            },
            Rel::Map { input, scalars } => ENode::Map {
                input: self.add_rel(input),
                scalars: scalars.clone(),
            },
            Rel::Filter { input, predicates } => ENode::Filter {
                input: self.add_rel(input),
                predicates: predicates.clone(),
            },
            Rel::Reduce {
                input,
                group_key,
                aggregates,
                monotonic,
                expected_group_size,
            } => ENode::Reduce {
                input: self.add_rel(input),
                group_key: group_key.clone(),
                aggregates: aggregates.clone(),
                monotonic: *monotonic,
                expected_group_size: *expected_group_size,
            },
            Rel::TopK { input, shape } => ENode::TopK {
                input: self.add_rel(input),
                shape: shape.clone(),
            },
            Rel::Negate { input } => ENode::Negate {
                input: self.add_rel(input),
            },
            Rel::Threshold { input } => ENode::Threshold {
                input: self.add_rel(input),
            },
            Rel::Join {
                inputs,
                equivalences,
            } => ENode::Join {
                inputs: inputs.iter().map(|r| self.add_rel(r)).collect(),
                equivalences: equivalences.clone(),
            },
            Rel::WcoJoin {
                inputs,
                equivalences,
            } => ENode::WcoJoin {
                inputs: inputs.iter().map(|r| self.add_rel(r)).collect(),
                equivalences: equivalences.clone(),
            },
            Rel::Union { base, inputs } => {
                let mut ids = vec![self.add_rel(base)];
                ids.extend(inputs.iter().map(|r| self.add_rel(r)));
                ENode::Union { inputs: ids }
            }
            // An unsupported subtree, carried verbatim.
            Rel::Opaque(m) => ENode::Opaque((**m).clone()),
            // A recursive/CSE reference is an opaque leaf within a Let-free
            // fragment.
            Rel::LocalGet { id, arity, get } => ENode::LocalGet {
                id: *id,
                arity: *arity,
                get: get.clone(),
            },
            // The binding scopes are peeled by the structural optimizer; a whole
            // `Let`/`LetRec` is never added to the e-graph.
            Rel::Let { .. } | Rel::LetRec { .. } => {
                panic!("Let/LetRec are binding scopes and cannot be added to the e-graph")
            }
        };
        self.add(node)
    }

    /// Merge two e-classes. Returns true if they were distinct.
    pub fn union(&mut self, a: Id, b: Id) -> bool {
        let (ra, rb) = (self.find(a), self.find(b));
        if ra == rb {
            return false;
        }
        // Fold rb into ra.
        self.uf[rb] = ra;
        let nodes = self.classes.remove(&rb).unwrap_or_default();
        self.classes.entry(ra).or_default().extend(nodes);
        true
    }

    /// Restore the e-graph invariants (canonical children, congruence) after a
    /// batch of unions.
    pub fn rebuild(&mut self) {
        loop {
            let mut merged = false;
            let mut memo: HashMap<ENode, Id> = HashMap::new();
            let ids: Vec<Id> = self.classes.keys().copied().collect();
            for id in ids {
                let rep = self.find(id);
                let nodes: Vec<ENode> = self
                    .classes
                    .get(&id)
                    .map(|s| s.iter().cloned().collect())
                    .unwrap_or_default();
                for n in nodes {
                    let cn = self.canon_enode(&n);
                    if let Some(&other) = memo.get(&cn) {
                        if self.union(other, rep) {
                            merged = true;
                        }
                    } else {
                        memo.insert(cn, rep);
                    }
                }
            }
            // Recanonicalize class contents.
            let mut new_classes: HashMap<Id, HashSet<ENode>> = HashMap::new();
            let old: Vec<(Id, HashSet<ENode>)> = self.classes.drain().collect();
            for (id, nodes) in old {
                let rep = self.find(id);
                let entry = new_classes.entry(rep).or_default();
                for n in nodes {
                    entry.insert(self.canon_enode(&n));
                }
            }
            self.classes = new_classes;
            if !merged {
                break;
            }
        }
        // Rebuild the hashcons table.
        self.memo.clear();
        for (&id, nodes) in &self.classes {
            for n in nodes {
                self.memo.insert(n.clone(), id);
            }
        }
    }

    /// The arity of a class (invariant across equivalent e-nodes).
    ///
    /// Equality saturation can make a class cyclic (e.g. `threshold_elision`
    /// unions `Threshold r` into `r`'s class, so that class then contains a
    /// `Threshold` node pointing back at itself). Arity is invariant across a
    /// class's e-nodes, so we determine it from *any* e-node with an acyclic
    /// derivation, using a visited guard to break cycles.
    pub fn arity(&self, id: Id) -> usize {
        self.arity_guarded(id, &mut HashSet::new())
            .expect("class has a well-defined arity")
    }

    fn arity_guarded(&self, id: Id, visiting: &mut HashSet<Id>) -> Option<usize> {
        let id = self.find(id);
        if !visiting.insert(id) {
            // Reached `id` again on this path: this derivation is cyclic and
            // can't pin the arity. Another e-node of the class may still.
            return None;
        }
        let mut result = None;
        if let Some(nodes) = self.classes.get(&id) {
            for node in nodes {
                let a = match node {
                    ENode::Constant { arity, .. }
                    | ENode::Get { arity, .. }
                    | ENode::LocalGet { arity, .. } => Some(*arity),
                    ENode::Opaque(m) => Some(m.arity()),
                    ENode::Project { outputs, .. } => Some(outputs.len()),
                    ENode::Map { input, scalars } => self
                        .arity_guarded(*input, visiting)
                        .map(|a| a + scalars.len()),
                    ENode::Filter { input, .. }
                    | ENode::TopK { input, .. }
                    | ENode::Negate { input }
                    | ENode::Threshold { input } => self.arity_guarded(*input, visiting),
                    ENode::Reduce {
                        group_key,
                        aggregates,
                        ..
                    } => Some(group_key.len() + aggregates.len()),
                    ENode::Join { inputs, .. } | ENode::WcoJoin { inputs, .. } => inputs
                        .iter()
                        .map(|i| self.arity_guarded(*i, visiting))
                        .sum::<Option<usize>>(),
                    ENode::Union { inputs } => self.arity_guarded(inputs[0], visiting),
                };
                if a.is_some() {
                    result = a;
                    break;
                }
            }
        }
        visiting.remove(&id);
        result
    }

    /// The column types of a class, structurally derived over the e-graph, or
    /// `None` when no e-node of the class yields a derivation.
    ///
    /// Mirrors [`mz_expr::MirRelationExpr::try_col_with_input_cols`] over the
    /// e-graph rather than over a single MIR tree. The typed leaves are
    /// `Opaque` (the carried `MirRelationExpr`) and `LocalGet` with a stored
    /// `Get` node; operators derive their types from their inputs the same way
    /// MIR does. Used at synthesis time to capture the real column types of the
    /// relation an `Empty(r)` replaces.
    ///
    /// Like [`Self::arity_guarded`], a class can be cyclic; the visited guard
    /// breaks cycles and another e-node of the class may still pin the types.
    /// Returns `None` (rather than defaulting) for any case it cannot derive, so
    /// callers can fall back deliberately.
    pub(crate) fn column_types(&self, id: Id) -> Option<Vec<ReprColumnType>> {
        self.column_types_guarded(id, &mut HashSet::new())
    }

    fn column_types_guarded(
        &self,
        id: Id,
        visiting: &mut HashSet<Id>,
    ) -> Option<Vec<ReprColumnType>> {
        let id = self.find(id);
        if !visiting.insert(id) {
            // Reached `id` again on this path: this derivation is cyclic and
            // can't pin the types. Another e-node of the class may still.
            return None;
        }
        let mut result = None;
        if let Some(nodes) = self.classes.get(&id) {
            for node in nodes {
                let t = self.node_column_types(node, visiting);
                if t.is_some() {
                    result = t;
                    break;
                }
            }
        }
        visiting.remove(&id);
        result
    }

    /// Derive the column types of a single e-node, recursing into inputs through
    /// [`Self::column_types_guarded`]. Returns `None` for any operator or leaf
    /// whose types cannot be derived (e.g. a child class with no acyclic
    /// derivation, or a nested `Constant` with no captured types).
    fn node_column_types(
        &self,
        node: &ENode,
        visiting: &mut HashSet<Id>,
    ) -> Option<Vec<ReprColumnType>> {
        match node {
            // A synthesized empty carries its types directly; an empty without
            // captured types cannot pin them.
            ENode::Constant { col_types, .. } => col_types.clone(),
            // Test-only base relation; its types are not modeled.
            ENode::Get { .. } => None,
            // Typed leaves: read the column types off the carried MIR node.
            ENode::Opaque(m) => Some(m.typ().column_types),
            ENode::LocalGet { get, .. } => get.as_ref().map(|g| g.typ().column_types),
            ENode::Project { input, outputs } => {
                let input = self.column_types_guarded(*input, visiting)?;
                outputs.iter().map(|&i| input.get(i).cloned()).collect()
            }
            ENode::Map { input, scalars } => {
                let mut result = self.column_types_guarded(*input, visiting)?;
                for scalar in scalars {
                    let t = MirScalarExpr::typ(&scalar.expr, &result);
                    result.push(t);
                }
                Some(result)
            }
            // Filter/TopK/Negate/Threshold pass their input types through. Filter
            // can strengthen nullability, but a weaker (still-nullable) type is
            // sound for an empty relation, so the plain passthrough suffices.
            ENode::Filter { input, .. }
            | ENode::TopK { input, .. }
            | ENode::Negate { input }
            | ENode::Threshold { input } => self.column_types_guarded(*input, visiting),
            ENode::Reduce {
                input,
                group_key,
                aggregates,
                ..
            } => {
                let input = self.column_types_guarded(*input, visiting)?;
                let mut result: Vec<ReprColumnType> = group_key
                    .iter()
                    .map(|e| MirScalarExpr::typ(&e.expr, &input))
                    .collect();
                result.extend(aggregates.iter().map(|agg| agg.typ(&input)));
                Some(result)
            }
            // Join/WcoJoin concatenate input types. The nullability tightening MIR
            // applies across equivalence classes is omitted: a weaker type is
            // sound for an empty relation.
            ENode::Join { inputs, .. } | ENode::WcoJoin { inputs, .. } => {
                let mut result = Vec::new();
                for i in inputs {
                    result.extend(self.column_types_guarded(*i, visiting)?);
                }
                Some(result)
            }
            // Union takes the least upper bound of its inputs' column types,
            // mirroring MIR. Any input that fails to derive, or a width or union
            // mismatch, yields `None`.
            ENode::Union { inputs } => {
                let mut iter = inputs.iter();
                let first = iter.next()?;
                let mut result = self.column_types_guarded(*first, visiting)?;
                for i in iter {
                    let other = self.column_types_guarded(*i, visiting)?;
                    if other.len() != result.len() {
                        return None;
                    }
                    for (base, col) in result.iter_mut().zip(other.iter()) {
                        *base = base.union(col).ok()?;
                    }
                }
                Some(result)
            }
        }
    }

    /// A snapshot of the relational view: every e-node grouped by its operator
    /// symbol, paired with its (canonical) parent class. This is the set of
    /// database relations the generic join scans.
    pub(crate) fn index(&self) -> Index {
        let mut idx: Index = HashMap::new();
        for (&id, nodes) in &self.classes {
            for n in nodes {
                idx.entry(n.sym()).or_default().push((id, n.clone()));
            }
        }
        idx
    }
}

// --- compiled-rule bindings -----------------------------------------------

/// All e-graph bindings produced by matching a rule's left-hand side.
#[derive(Clone, Debug, Default)]
pub struct EBindings {
    pub rels: HashMap<String, Id>,
    pub payloads: BTreeMap<String, Payload>,
    pub rests: HashMap<String, Vec<Id>>,
    /// The class at which the pattern's root matched.
    pub root: Id,
}

// --- side conditions ------------------------------------------------------
//
// Each function mirrors a `Cond` arm of the former AST interpreter. The
// compiled `find` matchers (see [`crate::eqsat::rules`]) call these directly
// instead of walking a `Cond` list.

/// `uses_only_input`: every column referenced by the payload is an output
/// column of the bound relation (which has arity `rel_arity`).
pub(crate) fn cond_uses_only_input(p: &Payload, rel_arity: usize) -> bool {
    p.columns().into_iter().all(|c| c < rel_arity)
}

/// `cols_in_range`: every column referenced by the payload lies in `[lo, hi)`.
pub(crate) fn cond_cols_in_range(p: &Payload, lo: i64, hi: i64) -> bool {
    p.columns()
        .into_iter()
        .all(|c| (c as i64) >= lo && (c as i64) < hi)
}

/// `all_columns`: every scalar in the payload is a bare column reference.
pub(crate) fn cond_all_columns(p: &Payload) -> bool {
    p.scalars()
        .is_some_and(|s| s.iter().all(|x| x.is_col().is_some()))
}

/// `any_false`: some scalar constant-folds to the literal `false`.
pub(crate) fn cond_any_false(p: &Payload) -> bool {
    p.scalars()
        .is_some_and(|s| s.iter().any(|x| x.lit == Some(false)))
}

/// `no_false`: no scalar in the payload is a known-false literal (vacuously
/// true for an empty list).
pub(crate) fn cond_no_false(p: &Payload) -> bool {
    p.scalars()
        .is_some_and(|s| s.iter().all(|x| x.lit != Some(false)))
}

/// `all_true`: every scalar constant-folds to the literal `true`.
pub(crate) fn cond_all_true(p: &Payload) -> bool {
    p.scalars()
        .is_some_and(|s| s.iter().all(|x| x.lit == Some(true)))
}

impl EGraph {
    /// `non_negative`: the bound relation has non-negative multiplicities.
    pub(crate) fn cond_non_negative(&self, an: &Analyses, id: Id) -> bool {
        an.nn.get(&self.find(id)).copied().unwrap_or(false)
    }

    /// `monotonic`: the bound relation is insert-only. No rule currently uses
    /// this condition, but the analysis and check are kept for the physical
    /// monotonic rewrites the rule file is expected to grow.
    #[allow(dead_code)]
    pub(crate) fn cond_monotonic(&self, an: &Analyses, id: Id) -> bool {
        an.mono.get(&self.find(id)).copied().unwrap_or(false)
    }

    /// `is_unique_key`: the payload's columns form a unique key of the relation.
    pub(crate) fn cond_is_unique_key(&self, an: &Analyses, p: &Payload, id: Id) -> bool {
        let cand = p.columns().into_iter().collect();
        an.keys
            .get(&self.find(id))
            .is_some_and(|ks| is_superkey(ks, &cand))
    }

    /// `is_rel_empty`: the relation has a zero-row `Constant` in its e-class.
    pub(crate) fn cond_is_rel_empty(&self, id: Id) -> bool {
        let rep = self.find(id);
        self.classes.get(&rep).is_some_and(|ns| {
            ns.iter()
                .any(|n| matches!(n, ENode::Constant { card: 0, .. }))
        })
    }

    /// `not_rel_empty`: the relation has no zero-row `Constant` in its e-class.
    pub(crate) fn cond_not_rel_empty(&self, id: Id) -> bool {
        let rep = self.find(id);
        self.classes.get(&rep).is_some_and(|ns| {
            !ns.iter()
                .any(|n| matches!(n, ENode::Constant { card: 0, .. }))
        })
    }

    /// `unsatisfiable`: the relation's equivalence analysis is contradictory.
    pub(crate) fn cond_unsatisfiable(&self, an: &Analyses, id: Id) -> bool {
        matches!(an.eq.get(&self.find(id)), Some(Some(ec)) if ec.unsatisfiable())
    }

    /// `join_is_cyclic`: the root e-class has a `Join` whose constraint
    /// hypergraph is cyclic. `root` is the class the rule's root matched.
    pub(crate) fn cond_join_is_cyclic(&self, root: Id) -> bool {
        let rep = self.find(root);
        self.classes.get(&rep).is_some_and(|ns| {
            ns.iter().any(|n| match n {
                ENode::Join {
                    inputs,
                    equivalences,
                } => {
                    let arities: Vec<usize> = inputs.iter().map(|&c| self.arity(c)).collect();
                    crate::eqsat::cost::join_is_cyclic(&arities, equivalences)
                }
                _ => false,
            })
        })
    }
}

// --- saturation -----------------------------------------------------------

/// Per-class analysis results computed once per saturation round and consulted
/// by analysis-backed side conditions.
pub(crate) struct Analyses {
    pub(crate) nn: HashMap<Id, bool>,
    pub(crate) keys: HashMap<Id, KeySet>,
    // Read only by `cond_monotonic`, which no rule uses yet; kept for the
    // monotonic physical rewrites the rule file is expected to grow.
    #[allow(dead_code)]
    pub(crate) mono: HashMap<Id, bool>,
    pub(crate) eq: HashMap<Id, Option<EquivalenceClasses>>,
    // Consumed by Cond::ScalarEquiv (Task 2); unread for now.
    #[allow(dead_code)]
    pub(crate) cc: HashMap<Id, ConstCols>,
}

impl EGraph {
    /// Apply all `rules` everywhere they match, to a fixpoint (or until
    /// `max_iters` is reached). This is equality saturation; it never removes
    /// information, so it cannot get stuck in a local minimum.
    ///
    /// Each rule's matching, side-condition checking, and right-hand-side
    /// instantiation are compiled functions generated at build time from the
    /// rule file (see [`crate::eqsat::rules`]); this loop only drives them.
    pub fn saturate(
        &mut self,
        rules: &CompiledRuleSet,
        max_iters: usize,
        locals: &LocalFacts,
    ) -> usize {
        let compiled = rules.rules();

        // Per-rule backoff state: the iteration index up to which a rule is
        // banned, and its current ban length (doubles on each re-offense). A
        // rule is banned when its match enumeration hits `MATCH_LIMIT`, so an
        // explosive rule is throttled while the rest keep firing.
        let mut banned_until = vec![0usize; compiled.len()];
        let mut ban_len = vec![INITIAL_BAN_LEN; compiled.len()];

        // Cached Equivalences analysis result, recomputed only when the
        // e-graph changes between rounds. The analysis is expensive (each
        // inner fixpoint round calls `minimize_bounded` per merge), so
        // avoiding redundant recomputation is the dominant performance win.
        // Soundness: the cache is invalidated whenever `changed` is true, so
        // the cached value always reflects a state at least as old as the
        // current e-graph but never newer than the previous round's result.
        // Both consumers (Phase 2a canonicalization and `Unsatisfiable`) are
        // monotone: stale (under-approximate) equivalences miss optimizations
        // but never produce incorrect plans.
        let mut cached_eq: Option<HashMap<Id, Option<EquivalenceClasses>>> = None;

        let mut iters = 0;
        for iter in 0..max_iters {
            iters += 1;
            self.rebuild();
            // Bound runaway e-graph growth: equality saturation can explode
            // combinatorially, and an unbounded e-graph costs seconds to
            // saturate and extract. Once the e-node count crosses the budget,
            // stop growing and extract from what we have (a sound, if
            // incomplete, saturation).
            let n_nodes: usize = self.classes.values().map(|ns| ns.len()).sum();
            if n_nodes > MAX_ENODES {
                break;
            }
            let index = self.index();

            // Recompute the Equivalences analysis only on the first iteration
            // and after rounds where the e-graph changed. On stable rounds the
            // cache holds a result that is still valid.
            if cached_eq.is_none() {
                cached_eq = Some(self.run_analysis_bounded(
                    &Equivalences {
                        locals: locals.equivalences.clone(),
                    },
                    MAX_EQUIVALENCES_ANALYSIS_ITERS,
                ));
            }
            // The `if` above ensures `cached_eq` is `Some`.
            let eq = cached_eq.as_ref().unwrap().clone();

            // Phase 1 (read-only): collect every rewrite to apply.
            let analyses = Analyses {
                nn: self.run_analysis(&NonNeg {
                    locals: locals.nonneg.clone(),
                }),
                keys: self.run_analysis(&Keys {
                    locals: locals.keys.clone(),
                }),
                mono: self.run_analysis(&Monotonic {
                    locals: locals.monotonic.clone(),
                }),
                eq,
                cc: self.run_analysis(&ConstantColumns {
                    locals: locals.constant_columns.clone(),
                }),
            };
            let mut pending: Vec<(usize, EBindings)> = Vec::new();
            for (qi, rule) in compiled.iter().enumerate() {
                // Skip rules currently serving a ban.
                if iter < banned_until[qi] {
                    continue;
                }
                // Enumerate at most `MATCH_LIMIT` matches. Asking for one extra
                // lets the matcher report that it hit the cap (explosive) so we
                // ban it for a growing number of iterations.
                let (matches, hit_limit) = (rule.find)(self, &index, &analyses, MATCH_LIMIT + 1);
                if hit_limit {
                    banned_until[qi] = iter + ban_len[qi];
                    ban_len[qi] = ban_len[qi].saturating_mul(2);
                }
                for b in matches.into_iter().take(MATCH_LIMIT) {
                    pending.push((qi, b));
                }
            }

            // Phase 2 (mutate): two sub-phases.
            let mut changed = false;

            // Phase 2a: equivalence-reducer canonicalization. For each e-class
            // whose equivalence analysis produced a non-trivial reducer, rewrite
            // the scalar payloads of every e-node in that class to their
            // canonical representatives and union the result back into the class.
            //
            // Runs BEFORE the rule application (phase 2b) so that analyses.eq IDs
            // are still the current canonical IDs.
            for (canon_id, ec_opt) in &analyses.eq {
                let Some(ec) = ec_opt else {
                    continue;
                };
                let reducer = ec.reducer();
                if reducer.is_empty() {
                    continue;
                }
                // analyses.eq is keyed by canonical IDs from the last rebuild.
                // A union earlier in this same loop can merge one of those
                // classes into another, after which its id is no longer a key in
                // self.classes; the lookup then misses and we skip it (recovered
                // on the next saturation iteration).
                let Some(nodes) = self.classes.get(canon_id) else {
                    continue;
                };
                let nodes: Vec<ENode> = nodes.iter().cloned().collect();
                for node in nodes {
                    // For Filter nodes, use the input's reducer to avoid the circular
                    // rewrite where the Filter's own predicates are used to derive
                    // equivalences that are then fed back into those same predicates.
                    // See the doc-comment on `rewrite_escalars` for details.
                    let filter_input_reducer = if let ENode::Filter { input, .. } = &node {
                        let canon_input = self.find(*input);
                        analyses
                            .eq
                            .get(&canon_input)
                            .and_then(|ec| ec.as_ref())
                            .map(|ec| ec.reducer())
                    } else {
                        None
                    };
                    let Some(new_node) =
                        rewrite_escalars(&node, reducer, filter_input_reducer, &|id| {
                            self.arity(id)
                        })
                    else {
                        continue;
                    };
                    let new_id = self.add(new_node);
                    if self.union(new_id, *canon_id) {
                        changed = true;
                    }
                }
            }

            // Phase 2b (compiled rule application): instantiate right-hand sides
            // and union. Runs after canonicalization so that the next iteration's
            // analyses see both the canonical rewrites and the rule rewrites.
            //
            // The e-node budget is rechecked here because Phase 2b can add many
            // new nodes in one pass. Stopping mid-pass when the budget is reached
            // is sound: already-applied rewrites are unioned, skipped ones are
            // conservatively omitted (same semantics as the outer MAX_ENODES guard).
            for (qi, b) in pending {
                if let Ok(new_id) = (compiled[qi].apply)(self, &b) {
                    if self.union(new_id, b.root) {
                        changed = true;
                    }
                }
                let n_nodes: usize = self.classes.values().map(|ns| ns.len()).sum();
                if n_nodes > MAX_ENODES {
                    break;
                }
            }

            if !changed {
                break;
            }
            // The e-graph changed this round. Invalidate the Equivalences cache
            // so the next round recomputes it against the updated structure.
            cached_eq = None;
        }
        self.rebuild();
        iters
    }

    /// Arities of the bound relation metavariables.
    pub(crate) fn binding_arities(&self, b: &EBindings) -> BTreeMap<String, usize> {
        b.rels
            .iter()
            .map(|(n, &id)| (n.clone(), self.arity(id)))
            .collect()
    }

    /// Run a lattice-valued [`Analysis`] to a fixpoint, one fact per e-class.
    ///
    /// Iteration stops when no value changes (true fixpoint) or when
    /// `max_iters` rounds have elapsed. Early termination yields a sound
    /// under-approximation: all derived facts are individually sound, and both
    /// consumers (canonicalization and `unsatisfiable`) are correct with fewer
    /// known facts than a full fixpoint.
    fn run_analysis_bounded<A: Analysis>(&self, a: &A, max_iters: usize) -> HashMap<Id, A::Domain> {
        let mut m: HashMap<Id, A::Domain> =
            self.classes.keys().map(|&id| (id, a.bottom())).collect();
        for iter in 0..max_iters {
            let mut updates: Vec<(Id, A::Domain)> = Vec::new();
            for (&id, nodes) in &self.classes {
                let get = |c: Id| m.get(&self.find(c)).cloned().unwrap_or_else(|| a.bottom());
                let arity = |c: Id| self.arity(c);
                let mut d = a.bottom();
                for n in nodes {
                    d = a.merge(d, a.make(n, &get, &arity));
                }
                if m.get(&id) != Some(&d) {
                    updates.push((id, d));
                }
            }
            if updates.is_empty() {
                break;
            }
            if iter + 1 == max_iters {
                tracing::debug!(
                    "run_analysis: did not converge after {max_iters} iterations; \
                     returning partial (under-approximate) result"
                );
                for (id, d) in updates {
                    m.insert(id, d);
                }
                break;
            }
            for (id, d) in updates {
                m.insert(id, d);
            }
        }
        m
    }

    /// Run a lattice-valued [`Analysis`] to a fixpoint, one fact per e-class.
    ///
    /// Iteration stops when no value changes (true fixpoint) or when
    /// `MAX_ANALYSIS_ITERS` rounds have elapsed. Early termination yields a
    /// sound under-approximation: all derived facts are individually sound, and
    /// both consumers (canonicalization and `unsatisfiable`) are correct with
    /// fewer known facts than a full fixpoint.
    pub fn run_analysis<A: Analysis>(&self, a: &A) -> HashMap<Id, A::Domain> {
        self.run_analysis_bounded(a, MAX_ANALYSIS_ITERS)
    }

    /// Extract the cheapest plan rooted at `root` under `model`, using the
    /// memory-first comparator (the default scarce-resource ordering).
    ///
    /// Bottom-up dynamic programming: each class records the cheapest plan
    /// among its e-nodes whose children have themselves been costed, iterated
    /// to a fixpoint.  (The e-graphs we build are acyclic, so this converges
    /// in at most the depth of the e-graph.)
    ///
    /// Returns `None` when the root class has no buildable representative under
    /// the polarity constraints (a non-linear `Reduce`/`TopK` whose input class
    /// has no non-negative form), so the caller can fall back to the
    /// un-optimized fragment rather than failing.
    pub fn extract(&self, root: Id, model: &CostModel) -> Option<Rel> {
        self.extract_with(root, model, true)
    }

    /// Extract the cheapest plan rooted at `root` under `model`.
    ///
    /// `memory_first` selects the comparator:
    /// * `true`: memory-first ordering (default; memory is the scarce resource).
    /// * `false`: time-first ordering (minimises CPU work, may use more memory).
    ///
    /// Returns `None` when the root class cannot be extracted under the polarity
    /// constraints (see [`Self::extract`]).
    pub fn extract_with(&self, root: Id, model: &CostModel, memory_first: bool) -> Option<Rel> {
        let cmp: &dyn Fn(&Cost, &Cost) -> std::cmp::Ordering = if memory_first {
            &|a, b| a.cmp_memory_first(b)
        } else {
            &|a, b| a.cmp_time_first(b)
        };

        // Cost is a pure, compositional function of the built `Rel`. Extraction
        // evaluates the same `(node, children-best)` combination many times
        // across passes, and `model.cost` recomputes the whole subtree each call
        // (including the exponential `binary_join_terms` for every join in it).
        // Memoize by the built plan so each distinct `Rel` is costed once. This
        // turns the dominant `O(classes^2)` re-evaluation into one cost per
        // distinct plan, and preserves the result exactly.
        let mut cost_cache: BTreeMap<Rel, Cost> = BTreeMap::new();
        // Two best-of-class maps, one per polarity demand. `best_any` is the
        // cheapest representative with no sign constraint; `best_nonneg` is the
        // cheapest representative whose output multiplicities are non-negative.
        // Both are filled in the same fixpoint so each class can serve whichever
        // demand its parent imposes. The soundness rule (see `build_rel`) pulls a
        // non-linear reduce or TopK input from `best_nonneg`, never `best_any`.
        let mut best_any: HashMap<Id, (Cost, Rel)> = HashMap::new();
        let mut best_nonneg: HashMap<Id, (Cost, Rel)> = HashMap::new();
        for _ in 0..(self.classes.len() + 1) {
            let mut changed = false;
            for (&id, nodes) in &self.classes {
                for node in nodes {
                    // Attempt to build each node under both demands. A node may
                    // satisfy `Any` but not `Nonneg` (e.g. a `Negate`, or any
                    // node whose nonneg-required child has no nonneg form yet).
                    for demand in [Demand::Any, Demand::Nonneg] {
                        if let Some(rel) = self.build_rel(node, demand, &best_any, &best_nonneg) {
                            let c = match cost_cache.get(&rel) {
                                Some(c) => c.clone(),
                                None => {
                                    let c = model.cost(&rel);
                                    cost_cache.insert(rel.clone(), c.clone());
                                    c
                                }
                            };
                            let best = match demand {
                                Demand::Any => &mut best_any,
                                Demand::Nonneg => &mut best_nonneg,
                            };
                            // Break cost ties on the plan itself, so extraction
                            // is deterministic despite randomized hash-map order.
                            let better = match best.get(&id) {
                                None => true,
                                Some((bc, br)) => {
                                    cmp(&c, bc) == std::cmp::Ordering::Less
                                        || (c == *bc && rel < *br)
                                }
                            };
                            if better {
                                best.insert(id, (c, rel));
                                changed = true;
                            }
                        }
                    }
                }
            }
            if !changed {
                break;
            }
        }
        // The root has no parent, so no polarity demand: extract from `best_any`.
        // `None` when no representative could be built (the root, or some node it
        // requires, has no form satisfying the polarity constraint); the caller
        // falls back to the un-optimized fragment, which is always sound.
        best_any.get(&self.find(root)).map(|(_, r)| r.clone())
    }

    /// Rebuild a [`Rel`] from an e-node, substituting each child with its
    /// currently-best extracted plan for the demand that child imposes. Returns
    /// `None` if the chosen child map lacks a child yet, or if `demand` is
    /// `Nonneg` and `node` is a `Negate` (the one node that cannot be made
    /// non-negative).
    ///
    /// `best_any` and `best_nonneg` are the per-class cheapest representatives
    /// without and with a non-negative-multiplicity guarantee, respectively. Each
    /// child is pulled from `best_nonneg` when its computed demand is `Nonneg`,
    /// from `best_any` otherwise.
    fn build_rel(
        &self,
        node: &ENode,
        demand: Demand,
        best_any: &HashMap<Id, (Cost, Rel)>,
        best_nonneg: &HashMap<Id, (Cost, Rel)>,
    ) -> Option<Rel> {
        // Only `Negate` cannot satisfy a `Nonneg` demand; every other node is
        // either sign-preserving (and so relies on its children's nonneg forms,
        // enforced by the per-child demands below) or a barrier whose output is
        // non-negative regardless of input (`Reduce`/`TopK`/`Threshold`).
        if demand == Demand::Nonneg && matches!(node, ENode::Negate { .. }) {
            return None;
        }
        // Pull a child under its own demand: `best_nonneg` for `Nonneg`,
        // `best_any` otherwise. Returns `None` until that map has costed the
        // child.
        let get = |id: Id, child_demand: Demand| {
            let best = match child_demand {
                Demand::Any => best_any,
                Demand::Nonneg => best_nonneg,
            };
            best.get(&self.find(id)).map(|(_, r)| r.clone())
        };
        Some(match node {
            ENode::Constant {
                card,
                arity,
                col_types,
            } => Rel::Constant {
                card: *card,
                arity: *arity,
                col_types: col_types.clone(),
            },
            ENode::Get { name, arity } => Rel::Get {
                name: name.clone(),
                arity: *arity,
            },
            ENode::Opaque(m) => Rel::Opaque(Box::new(m.clone())),
            ENode::LocalGet { id, arity, get } => Rel::LocalGet {
                id: *id,
                arity: *arity,
                get: get.clone(),
            },
            // Project/Map/Filter are sign-preserving: propagate the parent demand
            // to the input.
            ENode::Project { input, outputs } => Rel::Project {
                input: Box::new(get(*input, demand)?),
                outputs: outputs.clone(),
            },
            ENode::Map { input, scalars } => Rel::Map {
                input: Box::new(get(*input, demand)?),
                scalars: scalars.clone(),
            },
            ENode::Filter { input, predicates } => Rel::Filter {
                input: Box::new(get(*input, demand)?),
                predicates: predicates.clone(),
            },
            // A reduce is a barrier (its output is non-negative regardless of
            // input), so it satisfies a `Nonneg` parent on its own. Its input
            // demand comes from the soundness rule, not the parent: a non-linear
            // reduce (>=1 aggregate) requires a `Nonneg` input, because
            // `reduce(r) != negate(reduce(negate(r)))` for non-linear aggregates.
            // A distinct (no aggregates) is polarity-insensitive and takes `Any`.
            ENode::Reduce {
                input,
                group_key,
                aggregates,
                monotonic,
                expected_group_size,
            } => Rel::Reduce {
                input: Box::new(get(*input, reduce_input_demand(node))?),
                group_key: group_key.clone(),
                aggregates: aggregates.clone(),
                monotonic: *monotonic,
                expected_group_size: *expected_group_size,
            },
            // A TopK is a barrier and always requires a `Nonneg` input (its
            // per-group ordering is meaningless over signed multiplicities).
            ENode::TopK { input, shape } => Rel::TopK {
                input: Box::new(get(*input, Demand::Nonneg)?),
                shape: shape.clone(),
            },
            // A negate flips the sign, so its input takes `Any`. A `Nonneg`
            // demand on a negate itself was already rejected above.
            ENode::Negate { input } => Rel::Negate {
                input: Box::new(get(*input, Demand::Any)?),
            },
            // A threshold is a barrier: its output is non-negative regardless of
            // input, so the input takes `Any`.
            ENode::Threshold { input } => Rel::Threshold {
                input: Box::new(get(*input, Demand::Any)?),
            },
            // Join/WcoJoin/Union are sign-preserving in every input: propagate
            // the parent demand to all of them.
            ENode::Join {
                inputs,
                equivalences,
            } => Rel::Join {
                inputs: inputs
                    .iter()
                    .map(|i| get(*i, demand))
                    .collect::<Option<_>>()?,
                equivalences: equivalences.clone(),
            },
            ENode::WcoJoin {
                inputs,
                equivalences,
            } => Rel::WcoJoin {
                inputs: inputs
                    .iter()
                    .map(|i| get(*i, demand))
                    .collect::<Option<_>>()?,
                equivalences: equivalences.clone(),
            },
            ENode::Union { inputs } => {
                let mut rels = inputs
                    .iter()
                    .map(|i| get(*i, demand))
                    .collect::<Option<Vec<_>>>()?;
                let base = Box::new(rels.remove(0));
                Rel::Union { base, inputs: rels }
            }
        })
    }
}

/// Rewrite the scalar payloads (predicates, map scalars, join equivalences) of
/// an [`ENode`] by applying equivalence-class reducers, returning the rewritten
/// node or `None` if nothing changed.
///
/// Returns `Some(new_node)` if any payload changed (at least one `EScalar`
/// expression was rewritten), `None` if no rewriting occurred. The `lit` hint
/// is set to `None` for any rewritten scalar because the rewritten expression
/// may no longer evaluate to the same literal, and the column types needed to
/// re-reduce it are not available at saturation time.
///
/// Only operators that carry scalar payloads are rewritten: `Filter`
/// (predicates), `Map` (scalars), and `Join`/`WcoJoin` (equivalences). All
/// other e-node variants are returned as `None` (they have no scalar payloads
/// to rewrite).
///
/// # Reducer selection
///
/// `reducer` is the reducer derived from the *node's own e-class*. For `Map`
/// this is correct: the node's output equivalences are a strict superset of the
/// input's, and the column-range guard in `apply` prevents the circular
/// rewrites that would otherwise be possible.
///
/// `filter_input_reducer` is the reducer derived from the *Filter input's
/// e-class*. Filter predicates must be simplified only by the input's reducer,
/// never the Filter's own reducer. The Filter's output equivalences include
/// facts derived directly from the predicates themselves (e.g. `#2 = f()` makes
/// `f()` equivalent to `#2`). Feeding those back into the predicates is circular
/// and unsound: it rewrites `#2 = f()` to `#2 = #2`, making the predicate
/// trivially true and causing the filter to be dropped entirely (this silently
/// removed security-relevant `WHERE x = current_user` guards).
///
/// `Join`/`WcoJoin` have an analogous circularity (their join conditions are
/// part of their own output equivalences), so those variants return `None`
/// unconditionally (no scalar rewrite at saturation time).
///
/// # Canonicalization validity invariant
///
/// A rewritten scalar is accepted only if every column reference it contains is
/// strictly less than the column index that is valid in the scalar's evaluation
/// context. A rewrite that would produce an out-of-range column reference is
/// silently dropped (the original scalar is kept). This is always sound: fewer
/// rewrites means fewer canonicalizations, never an incorrect plan.
///
/// The specific bounds, per node type:
/// * `Map` scalar at position `pos`: valid columns are `0..(input_arity + pos)`.
///   The scalar may reference input columns and earlier scalars defined by the
///   same `Map`, but never its own output column or a later one.
/// * `Filter` predicate: valid columns are `0..input_arity`.
/// * `Join`/`WcoJoin` equivalence scalar: valid columns are `0..total_input_arity`.
///
/// This guard prevents a common pathology: the `Equivalences` analysis adds
/// `[column(input_arity+pos), defining_expr]` for each `Map` scalar and then
/// `minimize` picks `column(input_arity+pos)` as the canonical representative,
/// giving a reducer entry `defining_expr -> column(input_arity+pos)`. Applying
/// that reducer to the Map's own scalar at `pos` would replace the definition
/// with a forward reference to the column the Map is still constructing.
fn rewrite_escalars(
    node: &ENode,
    reducer: &BTreeMap<mz_expr::MirScalarExpr, mz_expr::MirScalarExpr>,
    filter_input_reducer: Option<&BTreeMap<mz_expr::MirScalarExpr, mz_expr::MirScalarExpr>>,
    arity_fn: &dyn Fn(Id) -> usize,
) -> Option<ENode> {
    /// Apply the reducer to a single `EScalar`, returning `(changed, new_escalar)`.
    /// Rejects the rewrite (keeps the original) if the result references any
    /// column index `>= max_col`.
    fn apply(
        escalar: &EScalar,
        reducer: &BTreeMap<mz_expr::MirScalarExpr, mz_expr::MirScalarExpr>,
        max_col: usize,
    ) -> (bool, EScalar) {
        let mut expr = escalar.expr.clone();
        let changed = reducer.reduce_expr(&mut expr);
        if changed {
            // Reject the rewrite if it produces a column reference that is out
            // of range for the scalar's evaluation context (see invariant above).
            if expr.support().into_iter().all(|c| c < max_col) {
                // The lit hint is cleared because we cannot recompute it without
                // column type information (not available at saturation time).
                (true, EScalar::plain(expr))
            } else {
                (false, escalar.clone())
            }
        } else {
            (false, escalar.clone())
        }
    }

    /// Apply the reducer to a list of `EScalar`s with a uniform `max_col` bound.
    /// Returns `(any_changed, new_list)`.
    fn apply_list(
        scalars: &[EScalar],
        reducer: &BTreeMap<mz_expr::MirScalarExpr, mz_expr::MirScalarExpr>,
        max_col: usize,
    ) -> (bool, Vec<EScalar>) {
        let mut any_changed = false;
        let new_scalars: Vec<EScalar> = scalars
            .iter()
            .map(|s| {
                let (changed, ns) = apply(s, reducer, max_col);
                any_changed = any_changed || changed;
                ns
            })
            .collect();
        (any_changed, new_scalars)
    }

    match node {
        ENode::Filter { input, predicates } => {
            // Predicates are evaluated over the input's columns: valid range is 0..input_arity.
            // Use the INPUT's reducer, not the Filter's own. The Filter's own equivalences
            // include facts derived from the predicates (e.g. `#2 = f()` → `f() ≡ #2`).
            // Applying those back to the predicates is circular: it rewrites `#2 = f()` to
            // `#2 = #2`, makes the predicate trivially true, and drops the filter entirely.
            let input_reducer = filter_input_reducer?;
            let input_arity = arity_fn(*input);
            let (changed, new_preds) = apply_list(predicates, input_reducer, input_arity);
            changed.then(|| ENode::Filter {
                input: *input,
                predicates: new_preds,
            })
        }
        ENode::Map { input, scalars } => {
            // Scalar at position `pos` is evaluated over input columns and the
            // earlier same-Map scalars: valid range is 0..(input_arity + pos).
            // Using the Map's own reducer is safe: the Map adds equivalences for new
            // output columns at indices >= input_arity, but the column-range guard in
            // `apply` rejects rewrites that would introduce such out-of-range references
            // into the scalars themselves (each scalar at position pos has max_col =
            // input_arity + pos, and a new-column reference has index >= input_arity).
            let input_arity = arity_fn(*input);
            let mut any_changed = false;
            let new_scalars: Vec<EScalar> = scalars
                .iter()
                .enumerate()
                .map(|(pos, s)| {
                    let max_col = input_arity + pos;
                    let (changed, ns) = apply(s, reducer, max_col);
                    any_changed = any_changed || changed;
                    ns
                })
                .collect();
            any_changed.then(|| ENode::Map {
                input: *input,
                scalars: new_scalars,
            })
        }
        // Join/WcoJoin: do NOT rewrite equivalence scalars using any e-class reducer.
        // The Equivalences analysis for a Join e-class derives facts from the join
        // conditions themselves (e.g. from `[#a, #b]` it concludes `#a = #b` and maps
        // `#b -> #a`). Feeding that reducer back into the join conditions is circular
        // and unsound: it rewrites `[#a, #b]` to `[#a, #a]`, silently dropping the
        // constraint that #a and #b must be equal — effectively turning an equijoin
        // into a broader (potentially cross-product-like) join that produces more rows.
        // The extra rows then propagate through the outer-join Threshold/Union/Negate
        // pattern and produce negative multiplicities that crash dataflow rendering.
        //
        // Canonicalizing a join's equivalences is a separate, sound step that must
        // use only facts from the join's INPUTS, not from the join's own output.
        // That step is deferred to the typed/physical phase where column types are
        // available.
        ENode::Join { .. } | ENode::WcoJoin { .. } => None,
        // No scalar payloads to rewrite.
        ENode::Constant { .. }
        | ENode::Get { .. }
        | ENode::Project { .. }
        | ENode::Reduce { .. }
        | ENode::TopK { .. }
        | ENode::Negate { .. }
        | ENode::Threshold { .. }
        | ENode::Union { .. }
        | ENode::Opaque(_)
        | ENode::LocalGet { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use mz_expr::MirScalarExpr;

    use super::*;
    use crate::eqsat::ir::EScalar;

    /// `rewrite_escalars` replaces `#1` with `#0` inside a Filter predicate
    /// when the reducer maps `Column(1) → Column(0)`.
    /// A dummy arity function for tests: returns the id itself as the arity
    /// (e.g., id=2 means input has 2 columns). This lets tests control input
    /// arities by choosing their input id.
    fn arity_by_id(id: Id) -> usize {
        id
    }

    #[mz_ore::test]
    fn rewrite_escalars_rewrites_filter_predicate() {
        let mut reducer = BTreeMap::new();
        reducer.insert(MirScalarExpr::column(1), MirScalarExpr::column(0));

        // Node: Filter[#1] with input id=2 (arity 2 via arity_by_id).
        // Column 1 < input_arity (2), so the rewrite is valid.
        let node = ENode::Filter {
            input: 2,
            predicates: vec![EScalar::plain(MirScalarExpr::column(1))],
        };
        // Pass the reducer as `filter_input_reducer` (simulating the input's reducer).
        let result = rewrite_escalars(&node, &reducer, Some(&reducer), &arity_by_id);
        let Some(ENode::Filter { predicates, .. }) = result else {
            panic!("expected rewritten Filter node");
        };
        assert_eq!(
            predicates[0].expr,
            MirScalarExpr::column(0),
            "predicate #1 must be rewritten to #0"
        );
    }

    /// `rewrite_escalars` returns `None` when no scalar in the node is in the
    /// reducer's domain (the node is already canonical).
    #[mz_ore::test]
    fn rewrite_escalars_returns_none_when_already_canonical() {
        let mut reducer = BTreeMap::new();
        reducer.insert(MirScalarExpr::column(1), MirScalarExpr::column(0));

        // Node: Filter[#0] with input id=2 (arity 2). #0 is not in the reducer.
        let node = ENode::Filter {
            input: 2,
            predicates: vec![EScalar::plain(MirScalarExpr::column(0))],
        };
        assert!(
            rewrite_escalars(&node, &reducer, Some(&reducer), &arity_by_id).is_none(),
            "a node with canonical scalars must not be rewritten"
        );
    }

    /// A Filter must be governed by its INPUT's reducer, never the node's own
    /// reducer. This locks in the fix for the bug where a Filter's own
    /// equivalences (`predicate` equivalent to `true`) trivialized and dropped
    /// the predicate, silently removing security-relevant
    /// `WHERE x = current_user` guards.
    #[mz_ore::test]
    fn rewrite_escalars_filter_ignores_own_reducer() {
        // `own` would rewrite #1 to #0, standing in for a Filter's own
        // equivalences that trivialize the predicate. With no input reducer the
        // predicate must be preserved.
        let mut own = BTreeMap::new();
        own.insert(MirScalarExpr::column(1), MirScalarExpr::column(0));
        let node = ENode::Filter {
            input: 2,
            predicates: vec![EScalar::plain(MirScalarExpr::column(1))],
        };
        assert!(
            rewrite_escalars(&node, &own, None, &arity_by_id).is_none(),
            "filter predicates must be governed by the input reducer, not the node's own"
        );
    }

    /// `rewrite_escalars` never rewrites `Join`/`WcoJoin` conditions (returns
    /// `None`): a join's own equivalences would trivialize its equijoin
    /// condition to a tautology and widen the join to a cross-product.
    #[mz_ore::test]
    fn rewrite_escalars_never_rewrites_join() {
        let mut reducer = BTreeMap::new();
        reducer.insert(MirScalarExpr::column(1), MirScalarExpr::column(0));
        let node = ENode::Join {
            inputs: vec![1, 1],
            equivalences: vec![vec![
                EScalar::plain(MirScalarExpr::column(0)),
                EScalar::plain(MirScalarExpr::column(1)),
            ]],
        };
        assert!(
            rewrite_escalars(&node, &reducer, Some(&reducer), &arity_by_id).is_none(),
            "join conditions must never be canonicalized"
        );
    }

    /// `rewrite_escalars` rewrites inside a Map scalar.
    #[mz_ore::test]
    fn rewrite_escalars_rewrites_map_scalar() {
        let mut reducer = BTreeMap::new();
        reducer.insert(MirScalarExpr::column(1), MirScalarExpr::column(0));

        // Map with input id=2 (arity 2). Scalar at pos=0: #1+#1.
        // Valid range for pos=0 is 0..(2+0)=2, so column 1 is in range and
        // the rewrite to column 0 is accepted.
        let add64 = mz_expr::BinaryFunc::AddInt64(mz_expr::func::AddInt64);
        let scalar_expr =
            MirScalarExpr::column(1).call_binary(MirScalarExpr::column(1), add64.clone());
        let node = ENode::Map {
            input: 2,
            scalars: vec![EScalar::plain(scalar_expr)],
        };
        // Map nodes do not use `filter_input_reducer`; pass `None`.
        let result = rewrite_escalars(&node, &reducer, None, &arity_by_id);
        let Some(ENode::Map { scalars, .. }) = result else {
            panic!("expected rewritten Map node");
        };
        let expected = MirScalarExpr::column(0).call_binary(MirScalarExpr::column(0), add64);
        assert_eq!(
            scalars[0].expr, expected,
            "both #1 occurrences must become #0"
        );
    }

    /// A reducer that maps `defining_expr -> column(input_arity+pos)` (the
    /// pathological case from `Equivalences` analysis) MUST NOT be applied to
    /// the Map scalar at `pos`, because the result would self-reference the
    /// column the Map is still constructing.
    ///
    /// Concretely: Map over input of arity 1. The scalar at pos=0 is some expr
    /// `e`. The Equivalences analysis adds `[column(1), e]` and `minimize` picks
    /// `column(1)` as canonical, giving `reducer[e] = column(1)`. Applying that
    /// to the Map's scalar would rewrite `e` to `column(1)`, a self-reference.
    /// The guard in `rewrite_escalars` must detect that column 1 >= max_col (=
    /// input_arity + pos = 1 + 0 = 1) and keep the original scalar.
    #[mz_ore::test]
    fn rewrite_escalars_rejects_map_self_reference() {
        // input_arity = 1 (input id=1, arity_by_id returns 1).
        // Scalar at pos=0: some expression that the reducer would map to #1.
        let defining_expr = MirScalarExpr::column(0); // the original scalar
        let mut reducer = BTreeMap::new();
        // The pathological reducer entry: defining_expr -> column(input_arity+pos) = column(1).
        reducer.insert(defining_expr.clone(), MirScalarExpr::column(1));

        let node = ENode::Map {
            input: 1, // arity_by_id(1) = 1, so input_arity = 1
            scalars: vec![EScalar::plain(defining_expr.clone())],
        };
        // Before the fix: rewrite_escalars would return Some(Map { scalars: [#1] }),
        // a self-reference. After the fix: it must return None (no valid rewrite).
        // Map nodes do not use `filter_input_reducer`; pass `None`.
        let result = rewrite_escalars(&node, &reducer, None, &arity_by_id);
        assert!(
            result.is_none(),
            "rewrite must be rejected: #1 is out of range for Map scalar at pos=0 \
             with input_arity=1 (max_col=1, column 1 is not < 1)"
        );
        // Verify the original scalar is unchanged.
        if let Some(ENode::Map { scalars, .. }) = result {
            assert_eq!(
                scalars[0].expr, defining_expr,
                "original scalar must be preserved when rewrite is rejected"
            );
        }
    }

    /// The validity guard must NOT reject legitimate rewrites. A Map scalar at
    /// pos=0 with input_arity=2 may reference column 0 or 1; a reducer that
    /// maps column(1)->column(0) is valid and must be accepted.
    #[mz_ore::test]
    fn rewrite_escalars_accepts_in_range_map_rewrite() {
        // input_arity = 2 (input id=2, arity_by_id returns 2).
        // Scalar at pos=0: #1. Reducer: column(1)->column(0).
        // max_col = 2 + 0 = 2. Column 0 < 2, so the rewrite is valid.
        let mut reducer = BTreeMap::new();
        reducer.insert(MirScalarExpr::column(1), MirScalarExpr::column(0));

        let node = ENode::Map {
            input: 2,
            scalars: vec![EScalar::plain(MirScalarExpr::column(1))],
        };
        // Map nodes do not use `filter_input_reducer`; pass `None`.
        let result = rewrite_escalars(&node, &reducer, None, &arity_by_id);
        let Some(ENode::Map { scalars, .. }) = result else {
            panic!("expected rewritten Map node; the rewrite is in range and must be accepted");
        };
        assert_eq!(scalars[0].expr, MirScalarExpr::column(0));
    }

    use crate::eqsat::cost::CostModel;
    use crate::eqsat::ir::Rel;

    /// A `MAX` aggregate over column 0 (a non-linear aggregate).
    fn max_aggregate() -> mz_expr::AggregateExpr {
        mz_expr::AggregateExpr {
            func: mz_expr::AggregateFunc::MaxInt64,
            expr: MirScalarExpr::column(0),
            distinct: false,
        }
    }

    /// A reduce input class that holds BOTH a cheap `Negate`-rooted plan and a
    /// costlier non-negative plan must be extracted as the non-negative plan when
    /// it feeds a non-linear reduce. Picking the cheaper `Negate` form would be
    /// unsound: `reduce(r) != negate(reduce(negate(r)))` for a `MAX` aggregate.
    ///
    /// Before the polarity-aware extractor this test fails: a single best-of-class
    /// map picks the cheaper `Negate(Get)` (2 nodes, fewer time terms) over
    /// `Filter(Filter(Get))` (3 nodes), placing a `Negate` directly under the
    /// reduce.
    #[mz_ore::test]
    fn reduce_input_avoids_negate_representative() {
        let mut eg = EGraph::new();
        // Base relation `a`.
        let a = eg.add(ENode::Get {
            name: "a".to_string(),
            arity: 1,
        });
        // Cheap, sign-negative representative: Negate(a). 2 nodes.
        let neg = eg.add(ENode::Negate { input: a });
        // Costlier non-negative representative: Filter(Filter(a)). 3 nodes, more
        // time terms, so strictly costlier than the negate form.
        let f1 = eg.add(ENode::Filter {
            input: a,
            predicates: vec![EScalar::plain(MirScalarExpr::column(0))],
        });
        let pos = eg.add(ENode::Filter {
            input: f1,
            predicates: vec![EScalar::plain(MirScalarExpr::column(0))],
        });
        // Merge the two representatives into one class `c`.
        eg.union(neg, pos);
        eg.rebuild();
        let c = eg.find(neg);
        // A reduce with a MAX aggregate over `c`.
        let root = eg.add(ENode::Reduce {
            input: c,
            group_key: vec![],
            aggregates: vec![max_aggregate()],
            monotonic: false,
            expected_group_size: None,
        });

        let model = CostModel::new();
        let extracted = eg
            .extract(root, &model)
            .expect("well-formed root must extract");
        let Rel::Reduce { input, .. } = extracted else {
            panic!("root must extract to a Reduce");
        };
        assert!(
            !matches!(*input, Rel::Negate { .. }),
            "the non-linear reduce must not have a Negate directly as its input; got {input:?}"
        );
    }

    /// A negate-free graph extracts identically to a direct cost-minimizing
    /// extraction: the polarity machinery must not perturb the common path. Here
    /// the class holds two plans and the cheaper one (fewer nodes) is picked, as
    /// before.
    #[mz_ore::test]
    fn negate_free_graph_extracts_cheapest() {
        let mut eg = EGraph::new();
        let a = eg.add(ENode::Get {
            name: "a".to_string(),
            arity: 1,
        });
        // Cheap plan: a single filter.
        let cheap = eg.add(ENode::Filter {
            input: a,
            predicates: vec![EScalar::plain(MirScalarExpr::column(0))],
        });
        // Costlier plan: two stacked filters.
        let mid = eg.add(ENode::Filter {
            input: a,
            predicates: vec![EScalar::plain(MirScalarExpr::column(0))],
        });
        let costly = eg.add(ENode::Filter {
            input: mid,
            predicates: vec![EScalar::plain(MirScalarExpr::column(0))],
        });
        eg.union(cheap, costly);
        eg.rebuild();
        let root = eg.find(cheap);

        let model = CostModel::new();
        let extracted = eg
            .extract(root, &model)
            .expect("well-formed root must extract");
        // The cheapest plan is the single filter directly over the Get.
        let Rel::Filter { input, .. } = extracted else {
            panic!("root must extract to a Filter");
        };
        assert!(
            matches!(*input, Rel::Get { .. }),
            "negate-free extraction must pick the single-filter plan; got {input:?}"
        );
    }

    /// A reduce whose input class has only non-negative representatives extracts
    /// that input unchanged: the nonneg demand is satisfied by the ordinary best
    /// plan.
    #[mz_ore::test]
    fn reduce_input_only_nonneg_extracts_unchanged() {
        let mut eg = EGraph::new();
        let a = eg.add(ENode::Get {
            name: "a".to_string(),
            arity: 1,
        });
        let pos = eg.add(ENode::Filter {
            input: a,
            predicates: vec![EScalar::plain(MirScalarExpr::column(0))],
        });
        let root = eg.add(ENode::Reduce {
            input: pos,
            group_key: vec![],
            aggregates: vec![max_aggregate()],
            monotonic: false,
            expected_group_size: None,
        });

        let model = CostModel::new();
        let extracted = eg
            .extract(root, &model)
            .expect("well-formed root must extract");
        let Rel::Reduce { input, .. } = extracted else {
            panic!("root must extract to a Reduce");
        };
        let Rel::Filter { input: inner, .. } = *input else {
            panic!("reduce input must be the Filter, unchanged");
        };
        assert!(
            matches!(*inner, Rel::Get { .. }),
            "the nonneg input must extract as Filter(Get), unchanged; got {inner:?}"
        );
    }

    use crate::eqsat::analysis::{ConstCols, ConstantColumns, LocalFacts};
    use mz_repr::{Datum, ReprScalarType};

    /// The ck480 shape `Let l0 = Filter[#0=123, #1=234](Get u1) in Union[Get l0,
    /// Get l0, Get l0]`, returned as `(definition, body)` `Rel`s. The body's
    /// references are opaque `LocalGet { id: 0 }` (no `get`, matching the engine's
    /// scope placeholders). The definition pins output columns 0 and 1 to the
    /// literals 123 and 234.
    fn ck480_def_and_body() -> (Rel, Rel) {
        fn col_eq(col: usize, val: i64) -> EScalar {
            EScalar::plain(MirScalarExpr::column(col).call_binary(
                MirScalarExpr::literal_ok(Datum::Int64(val), ReprScalarType::Int64),
                mz_expr::BinaryFunc::Eq(mz_expr::func::Eq),
            ))
        }
        let def = Rel::Filter {
            input: Box::new(Rel::Get {
                name: "u1".to_string(),
                arity: 3,
            }),
            predicates: vec![col_eq(0, 123), col_eq(1, 234)],
        };
        let get_l0 = || Rel::LocalGet {
            id: 0,
            arity: 3,
            get: None,
        };
        let body = Rel::Union {
            base: Box::new(get_l0()),
            inputs: vec![get_l0(), get_l0()],
        };
        (def, body)
    }

    /// The constant 123 as a stored `EScalar`, for asserting analysis output.
    fn lit_i64(val: i64) -> EScalar {
        EScalar::plain(MirScalarExpr::literal_ok(
            Datum::Int64(val),
            ReprScalarType::Int64,
        ))
    }

    /// Unioning the non-recursive `Let` definition into the body e-graph un-traps
    /// the definition's constant-column facts onto the body's `Get l0` class.
    ///
    /// This is the point of the Let-union step: on the pre-step-2 separate-fragment
    /// path the body's `Get l0` is an opaque `LocalGet` that proves no constant, so
    /// the fact `{0: 123, 1: 234}` is unreachable across the binding boundary. After
    /// adding the definition into the body's e-graph and unioning the `Get l0` class
    /// with the definition root, the fact reaches the `Get l0` class via congruence.
    #[mz_ore::test]
    fn let_union_untraps_constant_columns() {
        let (def, body) = ck480_def_and_body();
        let cc = ConstantColumns {
            locals: BTreeMap::new(),
        };
        let no_facts = LocalFacts::default();

        // Locate the body's `Get l0` class.
        let get_l0 = Rel::LocalGet {
            id: 0,
            arity: 3,
            get: None,
        };

        // Baseline (today's separate-fragment path): the body alone, `Get l0`
        // opaque. The fact must NOT be present.
        let mut baseline = EGraph::new();
        let _root = baseline.add_rel(&body);
        let get_class = baseline.add_rel(&get_l0);
        baseline.rebuild();
        baseline.saturate(&crate::eqsat::default_ruleset(), 100, &no_facts);
        let baseline_cc = baseline.run_analysis(&cc);
        let baseline_fact = baseline_cc
            .get(&baseline.find(get_class))
            .cloned()
            .unwrap_or_default();
        assert!(
            baseline_fact.is_empty(),
            "separate-fragment baseline must NOT carry the constant fact on Get l0; \
             got {baseline_fact:?}"
        );

        // Prototype (step 2): body + definition in ONE e-graph, `Get l0` unioned
        // with the definition root. The fact MUST reach the `Get l0` class.
        let mut unioned = EGraph::new();
        let _root = unioned.add_rel(&body);
        let get_class = unioned.add_rel(&get_l0);
        let def_class = unioned.add_rel(&def);
        unioned.union(get_class, def_class);
        unioned.rebuild();
        unioned.saturate(&crate::eqsat::default_ruleset(), 100, &no_facts);
        let unioned_cc = unioned.run_analysis(&cc);
        let fact = unioned_cc
            .get(&unioned.find(get_class))
            .cloned()
            .unwrap_or_default();
        let mut expected = ConstCols::new();
        expected.insert(0, lit_i64(123));
        expected.insert(1, lit_i64(234));
        assert_eq!(
            fact, expected,
            "after the union, Get l0 must carry {{0: 123, 1: 234}}; got {fact:?}"
        );
    }
}
