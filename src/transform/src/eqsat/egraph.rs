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

use mz_expr::{AggregateExpr, Columns, MirRelationExpr};

use crate::analysis::equivalences::{EquivalenceClasses, ExpressionReducer};
use crate::eqsat::analysis::{
    Analysis, Equivalences, KeySet, Keys, LocalFacts, Monotonic, NonNeg, is_superkey,
};
use crate::eqsat::cost::{Cost, CostModel};
use crate::eqsat::dsl::*;
use crate::eqsat::ir::{EScalar, Rel};
use crate::eqsat::matcher::{Payload, eval_ixexpr, eval_pexpr};

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

/// The e-graph.
#[derive(Default)]
pub struct EGraph {
    uf: Vec<Id>,
    classes: HashMap<Id, HashSet<ENode>>,
    memo: HashMap<ENode, Id>,
}

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
            Rel::Constant { card, arity } => ENode::Constant {
                card: *card,
                arity: *arity,
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

    /// A snapshot of the relational view: every e-node grouped by its operator
    /// symbol, paired with its (canonical) parent class. This is the set of
    /// database relations the generic join scans.
    fn index(&self) -> HashMap<Sym, Vec<(Id, ENode)>> {
        let mut idx: HashMap<Sym, Vec<(Id, ENode)>> = HashMap::new();
        for (&id, nodes) in &self.classes {
            for n in nodes {
                idx.entry(n.sym()).or_default().push((id, n.clone()));
            }
        }
        idx
    }
}

// --- the compiled conjunctive query for a pattern -------------------------

type VarId = usize;

/// One atom of the conjunctive query: an e-node of operator `sym` whose parent
/// and structural children occupy `slots` (`slots[0]` is the parent). `exact`
/// requires the e-node to have exactly `slots.len()-1` children; otherwise it
/// only needs at least that many (variadic, with a `rest`).
struct Atom {
    sym: Sym,
    slots: Vec<VarId>,
    exact: bool,
    /// The originating pattern node, used to read payloads and `rest` bindings
    /// off the concrete e-node once a structural match is found.
    pat: Pat,
}

/// A pattern compiled to a conjunctive query.
struct Query {
    atoms: Vec<Atom>,
    root: VarId,
    relvars: Vec<(String, VarId)>,
    n_vars: usize,
}

struct Compiler {
    next: VarId,
    relvars: HashMap<String, VarId>,
    atoms: Vec<Atom>,
}

impl Compiler {
    fn fresh(&mut self) -> VarId {
        let v = self.next;
        self.next += 1;
        v
    }

    /// Compile `pat`, returning the variable that denotes the relation it
    /// matches, and appending atoms for any operator nodes.
    fn compile(&mut self, pat: &Pat) -> VarId {
        match pat {
            Pat::RelVar(name) => {
                if let Some(&v) = self.relvars.get(name) {
                    v
                } else {
                    let v = self.fresh();
                    self.relvars.insert(name.clone(), v);
                    v
                }
            }
            Pat::Filter { input, .. }
            | Pat::Map { input, .. }
            | Pat::Project { input, .. }
            | Pat::Reduce { input, .. } => self.unary(pat, input),
            Pat::Negate(input) | Pat::Threshold(input) | Pat::TopK(input) => self.unary(pat, input),
            Pat::Join { inputs, .. } | Pat::WcoJoin { inputs, .. } | Pat::Union { inputs, .. } => {
                let v = self.fresh();
                let mut slots = vec![v];
                for item in &inputs.items {
                    slots.push(self.compile(item));
                }
                self.atoms.push(Atom {
                    sym: sym_of_pat(pat),
                    slots,
                    exact: inputs.rest.is_none(),
                    pat: pat.clone(),
                });
                v
            }
        }
    }

    fn unary(&mut self, pat: &Pat, input: &Pat) -> VarId {
        let v = self.fresh();
        let ci = self.compile(input);
        self.atoms.push(Atom {
            sym: sym_of_pat(pat),
            slots: vec![v, ci],
            exact: true,
            pat: pat.clone(),
        });
        v
    }
}

fn sym_of_pat(pat: &Pat) -> Sym {
    match pat {
        Pat::RelVar(_) => unreachable!("relvars have no atom"),
        Pat::Filter { .. } => Sym::Filter,
        Pat::Map { .. } => Sym::Map,
        Pat::Project { .. } => Sym::Project,
        Pat::Reduce { .. } => Sym::Reduce,
        Pat::Negate(_) => Sym::Negate,
        Pat::Threshold(_) => Sym::Threshold,
        Pat::TopK(_) => Sym::TopK,
        Pat::Join { .. } => Sym::Join,
        Pat::WcoJoin { .. } => Sym::WcoJoin,
        Pat::Union { .. } => Sym::Union,
    }
}

fn compile_pattern(pat: &Pat) -> Query {
    let mut c = Compiler {
        next: 0,
        relvars: HashMap::new(),
        atoms: Vec::new(),
    };
    let root = c.compile(pat);
    Query {
        atoms: c.atoms,
        root,
        relvars: c.relvars.into_iter().collect(),
        n_vars: c.next,
    }
}

// --- generic (worst-case-optimal) join ------------------------------------

/// All e-graph bindings produced by matching a rule's left-hand side.
#[derive(Clone, Debug, Default)]
pub struct EBindings {
    pub rels: HashMap<String, Id>,
    pub payloads: BTreeMap<String, Payload>,
    pub rests: HashMap<String, Vec<Id>>,
    /// The class at which the pattern's root matched.
    pub root: Id,
}

/// Read every structural variable value for `tuple` (an e-node's
/// `[parent, children…]`) into a per-variable map, enforcing repeated-variable
/// equality and consistency with `bound`. Returns `None` on conflict.
fn unify_tuple(
    atom: &Atom,
    tuple: &[Id],
    bound: &HashMap<VarId, Id>,
) -> Option<HashMap<VarId, Id>> {
    if atom.exact {
        if tuple.len() != atom.slots.len() {
            return None;
        }
    } else if tuple.len() < atom.slots.len() {
        return None;
    }
    let mut local = HashMap::new();
    for (pos, &v) in atom.slots.iter().enumerate() {
        let val = tuple[pos];
        if let Some(&b) = bound.get(&v) {
            if b != val {
                return None;
            }
        }
        if let Some(&l) = local.get(&v) {
            if l != val {
                return None;
            }
        }
        local.insert(v, val);
    }
    Some(local)
}

/// Generic join: enumerate every assignment of query variables to e-classes
/// that satisfies all atoms. Variables are bound one at a time; for each, the
/// candidate set is the intersection, over all atoms mentioning it, of the
/// values it could take given the bindings so far (the worst-case-optimal
/// join strategy).
fn generic_join(
    query: &Query,
    index: &HashMap<Sym, Vec<(Id, ENode)>>,
    all_ids: &HashSet<Id>,
    limit: usize,
) -> Vec<HashMap<VarId, Id>> {
    // Variable order: most-constrained-first (appears in the most atoms).
    let mut occ = vec![0usize; query.n_vars];
    for atom in &query.atoms {
        for &v in &atom.slots {
            occ[v] += 1;
        }
    }
    let mut order: Vec<VarId> = (0..query.n_vars).collect();
    order.sort_by(|&a, &b| occ[b].cmp(&occ[a]).then(a.cmp(&b)));

    let empty = Vec::new();
    let mut results = Vec::new();
    let mut assignment = HashMap::new();
    solve(
        query,
        index,
        all_ids,
        &order,
        0,
        &mut assignment,
        &mut results,
        &empty,
        limit,
    );
    results
}

fn solve(
    query: &Query,
    index: &HashMap<Sym, Vec<(Id, ENode)>>,
    all_ids: &HashSet<Id>,
    order: &[VarId],
    depth: usize,
    assignment: &mut HashMap<VarId, Id>,
    out: &mut Vec<HashMap<VarId, Id>>,
    empty: &Vec<(Id, ENode)>,
    limit: usize,
) {
    // Cap the match enumeration: a rule whose pattern matches combinatorially
    // can produce an unbounded number of assignments in a single iteration,
    // which dominates saturation time. Stop once the cap is reached; the partial
    // result set keeps saturation sound (just incomplete this iteration), and
    // the caller bans a rule that hits the cap from later iterations.
    if out.len() >= limit {
        return;
    }
    if depth == order.len() {
        out.push(assignment.clone());
        return;
    }
    let var = order[depth];

    // Intersect candidate values for `var` across every atom that mentions it.
    // A variable with no constraining atoms is unconstrained and ranges over
    // all e-class IDs. This allows pure-RelVar LHS patterns (rules whose entire
    // left-hand side is a single relation metavariable, with the condition doing
    // all the work) to enumerate candidates.
    let mut candidates: Option<HashSet<Id>> = None;
    for atom in &query.atoms {
        if !atom.slots.contains(&var) {
            continue;
        }
        let rel = index.get(&atom.sym).unwrap_or(empty);
        let mut vals = HashSet::new();
        for (parent, node) in rel {
            let mut tuple = vec![*parent];
            tuple.extend(node.children());
            if let Some(local) = unify_tuple(atom, &tuple, assignment) {
                if let Some(&val) = local.get(&var) {
                    vals.insert(val);
                }
            }
        }
        candidates = Some(match candidates {
            None => vals,
            Some(prev) => prev.intersection(&vals).copied().collect(),
        });
        if candidates.as_ref().is_some_and(|c| c.is_empty()) {
            return;
        }
    }

    // When no atom mentions this variable, fall back to all e-class IDs so that
    // pure-RelVar patterns (e.g. `r => Empty(r) where unsatisfiable(r)`) can
    // fire on any class.
    let candidates = candidates.unwrap_or_else(|| all_ids.clone());
    for val in candidates {
        if out.len() >= limit {
            break;
        }
        assignment.insert(var, val);
        solve(
            query,
            index,
            all_ids,
            order,
            depth + 1,
            assignment,
            out,
            empty,
            limit,
        );
    }
    assignment.remove(&var);
}

// --- turning a structural match into payload bindings ---------------------

/// For a complete variable assignment, produce every concrete [`EBindings`]
/// (there can be several when e-nodes differ only in their payloads).
fn expand_bindings(
    query: &Query,
    index: &HashMap<Sym, Vec<(Id, ENode)>>,
    assignment: &HashMap<VarId, Id>,
) -> Vec<EBindings> {
    // For each atom, find the concrete e-nodes consistent with the assignment.
    let empty = Vec::new();
    let mut per_atom: Vec<Vec<&ENode>> = Vec::with_capacity(query.atoms.len());
    for atom in &query.atoms {
        let rel = index.get(&atom.sym).unwrap_or(&empty);
        let mut matching = Vec::new();
        for (parent, node) in rel {
            let mut tuple = vec![*parent];
            tuple.extend(node.children());
            if unify_tuple(atom, &tuple, assignment).is_some() {
                matching.push(node);
            }
        }
        if matching.is_empty() {
            return vec![];
        }
        per_atom.push(matching);
    }

    // Base bindings: relvars and the root.
    let mut base = EBindings {
        root: assignment[&query.root],
        ..Default::default()
    };
    for (name, var) in &query.relvars {
        base.rels.insert(name.clone(), assignment[var]);
    }

    // Cartesian product over each atom's concrete e-node choices, reading
    // payloads and `rest` lists off the chosen node.
    let mut acc = vec![base];
    for (atom, choices) in query.atoms.iter().zip(&per_atom) {
        let mut next = Vec::new();
        for b in &acc {
            for node in choices {
                let mut nb = b.clone();
                if read_payloads(&atom.pat, node, &mut nb).is_some() {
                    next.push(nb);
                }
            }
        }
        acc = next;
        if acc.is_empty() {
            break;
        }
    }
    acc
}

/// Read the payload and `rest` metavariables of a pattern operator off a
/// matching e-node.
fn read_payloads(pat: &Pat, node: &ENode, b: &mut EBindings) -> Option<()> {
    match (pat, node) {
        (Pat::Filter { preds, .. }, ENode::Filter { predicates, .. }) => {
            b.payloads
                .insert(preds.clone(), Payload::Predicates(predicates.clone()));
        }
        (Pat::Map { scalars, .. }, ENode::Map { scalars: s, .. }) => {
            b.payloads
                .insert(scalars.clone(), Payload::Scalars(s.clone()));
        }
        (Pat::Project { outputs, .. }, ENode::Project { outputs: o, .. }) => {
            b.payloads
                .insert(outputs.clone(), Payload::Outputs(o.clone()));
        }
        (
            Pat::Reduce {
                group_key,
                aggregates,
                ..
            },
            ENode::Reduce {
                group_key: gk,
                aggregates: ag,
                ..
            },
        ) => {
            b.payloads
                .insert(group_key.clone(), Payload::GroupKey(gk.clone()));
            b.payloads
                .insert(aggregates.clone(), Payload::Aggregates(ag.clone()));
        }
        (Pat::Negate(_), ENode::Negate { .. })
        | (Pat::Threshold(_), ENode::Threshold { .. })
        | (Pat::TopK(_), ENode::TopK { .. }) => {}
        (
            Pat::Join {
                equivalences,
                inputs,
            },
            ENode::Join {
                equivalences: e,
                inputs: ins,
            },
        )
        | (
            Pat::WcoJoin {
                equivalences,
                inputs,
            },
            ENode::WcoJoin {
                equivalences: e,
                inputs: ins,
            },
        ) => {
            b.payloads
                .insert(equivalences.clone(), Payload::Equivalences(e.clone()));
            if let Some(rest) = &inputs.rest {
                b.rests
                    .insert(rest.clone(), ins[inputs.items.len()..].to_vec());
            }
        }
        (Pat::Union { inputs }, ENode::Union { inputs: ins }) => {
            if let Some(rest) = &inputs.rest {
                b.rests
                    .insert(rest.clone(), ins[inputs.items.len()..].to_vec());
            }
        }
        _ => return None,
    }
    Some(())
}

// --- saturation -----------------------------------------------------------

/// Per-class analysis results computed once per saturation round and consulted
/// by analysis-backed side conditions.
struct Analyses {
    nn: HashMap<Id, bool>,
    keys: HashMap<Id, KeySet>,
    mono: HashMap<Id, bool>,
    eq: HashMap<Id, Option<EquivalenceClasses>>,
}

impl EGraph {
    /// Apply all `rules` everywhere they match, to a fixpoint (or until
    /// `max_iters` is reached). This is equality saturation; it never removes
    /// information, so it cannot get stuck in a local minimum.
    pub fn saturate(&mut self, rules: &RuleSet, max_iters: usize, locals: &LocalFacts) -> usize {
        let queries: Vec<(usize, Query)> = rules
            .rules
            .iter()
            .enumerate()
            .map(|(i, r)| (i, compile_pattern(&r.lhs)))
            .collect();

        // Per-rule backoff state: the iteration index up to which a rule is
        // banned, and its current ban length (doubles on each re-offense). A
        // rule is banned when its match enumeration hits `MATCH_LIMIT`, so an
        // explosive rule is throttled while the rest keep firing.
        let mut banned_until = vec![0usize; queries.len()];
        let mut ban_len = vec![INITIAL_BAN_LEN; queries.len()];

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
            // The canonical set of all e-class IDs, used by the generic join
            // to enumerate candidates for unconstrained relation metavariables
            // (pure-RelVar LHS patterns such as `r => Empty(r) where ...`).
            let all_ids: HashSet<Id> = self.classes.keys().copied().collect();

            // Recompute the Equivalences analysis only on the first iteration
            // and after rounds where the e-graph changed (new e-nodes or
            // unions). On stable rounds the cache holds a result that is still
            // valid: no structural change means no new equivalences can arise.
            // Using MAX_EQUIVALENCES_ANALYSIS_ITERS (<<100) keeps each
            // recomputation cheap while the outer loop spreads the work over
            // multiple saturation rounds.
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
            };
            let mut pending: Vec<(usize, EBindings)> = Vec::new();
            for (qi, (ri, query)) in queries.iter().enumerate() {
                // Skip rules currently serving a ban.
                if iter < banned_until[qi] {
                    continue;
                }
                let rule = &rules.rules[*ri];
                // Enumerate at most `MATCH_LIMIT` matches. Asking for one extra
                // lets us detect that the rule hit the cap (explosive) and ban
                // it for a growing number of iterations.
                let assignments = generic_join(query, &index, &all_ids, MATCH_LIMIT + 1);
                if assignments.len() > MATCH_LIMIT {
                    banned_until[qi] = iter + ban_len[qi];
                    ban_len[qi] = ban_len[qi].saturating_mul(2);
                }
                for assignment in assignments.iter().take(MATCH_LIMIT) {
                    for b in expand_bindings(query, &index, assignment) {
                        if self.check_conds(&rule.conds, &b, &analyses) {
                            pending.push((*ri, b));
                        }
                    }
                }
            }

            // Phase 2 (mutate): two sub-phases.
            let mut changed = false;

            // Phase 2a: equivalence-reducer canonicalization. For each e-class
            // whose equivalence analysis produced a non-trivial reducer, rewrite
            // the scalar payloads of every e-node in that class to their
            // canonical representatives and union the result back into the class.
            // This is the e-graph form of EquivalencePropagation's reducer
            // application.
            //
            // Runs BEFORE the DSL rules (phase 2b) so that analyses.eq IDs are
            // still the current canonical IDs (rebuild() at the top of the loop
            // stabilizes them; no mutations have happened yet in this iteration).
            //
            // Loop-safety: the reducer is convergent under repeated application.
            // One application may not fully canonicalize (the reducer reflects
            // the previous iteration's equivalences), but full canonicalization
            // happens over repeated rounds, backstopped by the iteration caps.
            // After each round, a rewritten e-node hash-consed to an existing
            // one causes `self.union` to return `false`, and the saturation
            // loop's `!changed` guard terminates normally.
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
                // self.classes. So each canon_id either still resolves to its own
                // class (untouched so far this iteration) or has been subsumed, in
                // which case the lookup misses and we skip it. A skipped class is
                // recovered on the next saturation iteration.
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

            // Phase 2b (DSL rule application): instantiate right-hand sides and
            // union. Runs after canonicalization so that the next iteration's
            // analyses see both the canonical rewrites and the DSL rewrites.
            //
            // The e-node budget is rechecked here because Phase 2b can add many
            // new nodes in one pass (each `instantiate` call may hash-cons to a
            // fresh node). rebuild() at the top of the loop collapses equivalent
            // nodes, so the count before Phase 2b can be far below MAX_ENODES
            // even after Phase 2b last iteration added thousands. Stopping mid-pass
            // when the budget is reached is sound: already-applied rewrites are
            // unioned, and skipped ones are conservatively omitted (same semantics
            // as the outer MAX_ENODES guard).
            for (ri, b) in pending {
                let rule = &rules.rules[ri];
                let arities = self.binding_arities(&b);
                if let Ok(new_id) = self.instantiate(&rule.rhs, &b, None, &arities) {
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
            // The e-graph changed this round (new nodes or unions). Invalidate
            // the Equivalences cache so the next round recomputes it against the
            // updated structure.
            cached_eq = None;
        }
        self.rebuild();
        iters
    }

    /// Arities of the bound relation metavariables.
    fn binding_arities(&self, b: &EBindings) -> BTreeMap<String, usize> {
        b.rels
            .iter()
            .map(|(n, &id)| (n.clone(), self.arity(id)))
            .collect()
    }

    fn check_conds(&self, conds: &[Cond], b: &EBindings, an: &Analyses) -> bool {
        let arities = self.binding_arities(b);
        conds.iter().all(|c| match c {
            Cond::UsesOnlyInput { payload, rel } => {
                let (Some(p), Some(&r)) = (b.payloads.get(payload), b.rels.get(rel)) else {
                    return false;
                };
                let arity = self.arity(r);
                p.columns().into_iter().all(|c| c < arity)
            }
            Cond::ColsInRange { payload, lo, hi } => {
                let Some(p) = b.payloads.get(payload) else {
                    return false;
                };
                let (Ok(lo), Ok(hi)) = (eval_ixexpr(lo, &arities), eval_ixexpr(hi, &arities))
                else {
                    return false;
                };
                p.columns()
                    .into_iter()
                    .all(|c| (c as i64) >= lo && (c as i64) < hi)
            }
            Cond::NonNegative { rel } => b
                .rels
                .get(rel)
                .is_some_and(|&id| an.nn.get(&self.find(id)).copied().unwrap_or(false)),
            Cond::IsUniqueKey { payload, rel } => {
                let (Some(p), Some(&id)) = (b.payloads.get(payload), b.rels.get(rel)) else {
                    return false;
                };
                let cand = p.columns().into_iter().collect();
                an.keys
                    .get(&self.find(id))
                    .is_some_and(|ks| is_superkey(ks, &cand))
            }
            Cond::Monotonic { rel } => b
                .rels
                .get(rel)
                .is_some_and(|&id| an.mono.get(&self.find(id)).copied().unwrap_or(false)),
            Cond::Empty { payload } => b.payloads.get(payload).is_some_and(|p| p.is_empty()),
            Cond::AllColumns { payload } => b
                .payloads
                .get(payload)
                .and_then(|p| p.scalars())
                .is_some_and(|s| s.iter().all(|x| x.is_col().is_some())),
            Cond::AnyFalse { payload } => b
                .payloads
                .get(payload)
                .and_then(|p| p.scalars())
                .is_some_and(|s| s.iter().any(|x| x.lit == Some(false))),
            Cond::NoFalse { payload } => b
                .payloads
                .get(payload)
                .and_then(|p| p.scalars())
                // Vacuously true for an empty predicate list; true when no
                // scalar is a known-false literal.
                .is_some_and(|s| s.iter().all(|x| x.lit != Some(false))),
            Cond::AllTrue { payload } => b
                .payloads
                .get(payload)
                .and_then(|p| p.scalars())
                .is_some_and(|s| s.iter().all(|x| x.lit == Some(true))),
            Cond::IsRelEmpty { rel } => b.rels.get(rel).is_some_and(|&id| {
                let rep = self.find(id);
                self.classes.get(&rep).is_some_and(|ns| {
                    ns.iter()
                        .any(|n| matches!(n, ENode::Constant { card: 0, .. }))
                })
            }),
            Cond::NotRelEmpty { rel } => b.rels.get(rel).is_some_and(|&id| {
                let rep = self.find(id);
                self.classes.get(&rep).is_some_and(|ns| {
                    !ns.iter()
                        .any(|n| matches!(n, ENode::Constant { card: 0, .. }))
                })
            }),
            Cond::Unsatisfiable { rel } => {
                let Some(&id) = b.rels.get(rel) else {
                    return false;
                };
                // `None` already means the relation is empty; this rule fires
                // on the new fact: Some(ec) where ec itself is contradictory.
                matches!(an.eq.get(&self.find(id)), Some(Some(ec)) if ec.unsatisfiable())
            }
        })
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
    /// [`MAX_ANALYSIS_ITERS`] rounds have elapsed. Early termination yields a
    /// sound under-approximation: all derived facts are individually sound, and
    /// both consumers (canonicalization and `unsatisfiable`) are correct with
    /// fewer known facts than a full fixpoint.
    pub fn run_analysis<A: Analysis>(&self, a: &A) -> HashMap<Id, A::Domain> {
        self.run_analysis_bounded(a, MAX_ANALYSIS_ITERS)
    }

    /// Instantiate a template against e-graph bindings, adding the result and
    /// returning its e-class. `hole` is the element bound to `_` inside a
    /// `map(...)` list combinator.
    fn instantiate(
        &mut self,
        tmpl: &Tmpl,
        b: &EBindings,
        hole: Option<Id>,
        arities: &BTreeMap<String, usize>,
    ) -> Result<Id, String> {
        let node = match tmpl {
            Tmpl::RelVar(name) => {
                return b
                    .rels
                    .get(name)
                    .copied()
                    .ok_or_else(|| format!("unbound relation metavariable `{name}`"));
            }
            Tmpl::Hole => {
                return hole.ok_or_else(|| "`_` used outside a map(...) combinator".to_string());
            }
            Tmpl::Empty(name) => {
                let arity = *arities
                    .get(name)
                    .ok_or_else(|| format!("Empty of unbound relation `{name}`"))?;
                ENode::Constant { card: 0, arity }
            }
            Tmpl::Filter { preds, input } => ENode::Filter {
                predicates: eval_pexpr(preds, &b.payloads, arities)?.into_predicates()?,
                input: self.instantiate(input, b, hole, arities)?,
            },
            Tmpl::Map { scalars, input } => ENode::Map {
                scalars: eval_pexpr(scalars, &b.payloads, arities)?.into_scalars()?,
                input: self.instantiate(input, b, hole, arities)?,
            },
            Tmpl::Project { outputs, input } => ENode::Project {
                outputs: eval_pexpr(outputs, &b.payloads, arities)?.into_outputs()?,
                input: self.instantiate(input, b, hole, arities)?,
            },
            Tmpl::Reduce {
                group_key,
                aggregates,
                input,
            } => ENode::Reduce {
                group_key: eval_pexpr(group_key, &b.payloads, arities)?.into_group_key()?,
                aggregates: eval_pexpr(aggregates, &b.payloads, arities)?.into_aggregates()?,
                input: self.instantiate(input, b, hole, arities)?,
                // No rule produces a Reduce, so these hints have no source; a
                // synthesized Reduce defaults to the neutral physical hints.
                monotonic: false,
                expected_group_size: None,
            },
            Tmpl::Negate(t) => ENode::Negate {
                input: self.instantiate(t, b, hole, arities)?,
            },
            Tmpl::Threshold(t) => ENode::Threshold {
                input: self.instantiate(t, b, hole, arities)?,
            },
            Tmpl::Join {
                equivalences,
                inputs,
            } => ENode::Join {
                equivalences: eval_pexpr(equivalences, &b.payloads, arities)?
                    .into_equivalences()?,
                inputs: self.instantiate_list(inputs, b, hole, arities)?,
            },
            Tmpl::WcoJoin {
                equivalences,
                inputs,
            } => ENode::WcoJoin {
                equivalences: eval_pexpr(equivalences, &b.payloads, arities)?
                    .into_equivalences()?,
                inputs: self.instantiate_list(inputs, b, hole, arities)?,
            },
            Tmpl::Union { inputs } => {
                let ids = self.instantiate_list(inputs, b, hole, arities)?;
                if ids.is_empty() {
                    return Err("Union template produced no inputs".into());
                }
                ENode::Union { inputs: ids }
            }
        };
        Ok(self.add(node))
    }

    fn instantiate_list(
        &mut self,
        list: &ListTmpl,
        b: &EBindings,
        hole: Option<Id>,
        arities: &BTreeMap<String, usize>,
    ) -> Result<Vec<Id>, String> {
        let mut out = Vec::new();
        for elem in &list.elems {
            match elem {
                TElem::Item(t) => out.push(self.instantiate(t, b, hole, arities)?),
                TElem::Splice(name) => {
                    let extra = b
                        .rests
                        .get(name)
                        .ok_or_else(|| format!("unbound rest metavariable `{name}`"))?
                        .clone();
                    out.extend(extra);
                }
                TElem::MapSplice { func, list: name } => {
                    let elems = b
                        .rests
                        .get(name)
                        .ok_or_else(|| format!("unbound rest metavariable `{name}`"))?
                        .clone();
                    for e in elems {
                        out.push(self.instantiate(func, b, Some(e), arities)?);
                    }
                }
            }
        }
        Ok(out)
    }

    /// Extract the cheapest plan rooted at `root` under `model`, using the
    /// memory-first comparator (the default scarce-resource ordering).
    ///
    /// Bottom-up dynamic programming: each class records the cheapest plan
    /// among its e-nodes whose children have themselves been costed, iterated
    /// to a fixpoint.  (The e-graphs we build are acyclic, so this converges
    /// in at most the depth of the e-graph.)
    pub fn extract(&self, root: Id, model: &CostModel) -> Rel {
        self.extract_with(root, model, true)
    }

    /// Extract the cheapest plan rooted at `root` under `model`.
    ///
    /// `memory_first` selects the comparator:
    /// * `true`: memory-first ordering (default; memory is the scarce resource).
    /// * `false`: time-first ordering (minimises CPU work, may use more memory).
    pub fn extract_with(&self, root: Id, model: &CostModel, memory_first: bool) -> Rel {
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
        let mut best: HashMap<Id, (Cost, Rel)> = HashMap::new();
        for _ in 0..(self.classes.len() + 1) {
            let mut changed = false;
            for (&id, nodes) in &self.classes {
                for node in nodes {
                    if let Some(rel) = self.build_rel(node, &best) {
                        let c = match cost_cache.get(&rel) {
                            Some(c) => c.clone(),
                            None => {
                                let c = model.cost(&rel);
                                cost_cache.insert(rel.clone(), c.clone());
                                c
                            }
                        };
                        // Break cost ties on the plan itself, so extraction is
                        // deterministic despite randomized hash-map order.
                        let better = match best.get(&id) {
                            None => true,
                            Some((bc, br)) => {
                                cmp(&c, bc) == std::cmp::Ordering::Less || (c == *bc && rel < *br)
                            }
                        };
                        if better {
                            best.insert(id, (c, rel));
                            changed = true;
                        }
                    }
                }
            }
            if !changed {
                break;
            }
        }
        best.get(&self.find(root))
            .map(|(_, r)| r.clone())
            .expect("root class could not be extracted")
    }

    /// Rebuild a [`Rel`] from an e-node, substituting each child with its
    /// currently-best extracted plan. Returns `None` if a child is not yet
    /// costed.
    fn build_rel(&self, node: &ENode, best: &HashMap<Id, (Cost, Rel)>) -> Option<Rel> {
        let get = |id: Id| best.get(&self.find(id)).map(|(_, r)| r.clone());
        Some(match node {
            ENode::Constant { card, arity } => Rel::Constant {
                card: *card,
                arity: *arity,
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
            ENode::Project { input, outputs } => Rel::Project {
                input: Box::new(get(*input)?),
                outputs: outputs.clone(),
            },
            ENode::Map { input, scalars } => Rel::Map {
                input: Box::new(get(*input)?),
                scalars: scalars.clone(),
            },
            ENode::Filter { input, predicates } => Rel::Filter {
                input: Box::new(get(*input)?),
                predicates: predicates.clone(),
            },
            ENode::Reduce {
                input,
                group_key,
                aggregates,
                monotonic,
                expected_group_size,
            } => Rel::Reduce {
                input: Box::new(get(*input)?),
                group_key: group_key.clone(),
                aggregates: aggregates.clone(),
                monotonic: *monotonic,
                expected_group_size: *expected_group_size,
            },
            ENode::TopK { input, shape } => Rel::TopK {
                input: Box::new(get(*input)?),
                shape: shape.clone(),
            },
            ENode::Negate { input } => Rel::Negate {
                input: Box::new(get(*input)?),
            },
            ENode::Threshold { input } => Rel::Threshold {
                input: Box::new(get(*input)?),
            },
            ENode::Join {
                inputs,
                equivalences,
            } => Rel::Join {
                inputs: inputs.iter().map(|i| get(*i)).collect::<Option<_>>()?,
                equivalences: equivalences.clone(),
            },
            ENode::WcoJoin {
                inputs,
                equivalences,
            } => Rel::WcoJoin {
                inputs: inputs.iter().map(|i| get(*i)).collect::<Option<_>>()?,
                equivalences: equivalences.clone(),
            },
            ENode::Union { inputs } => {
                let mut rels = inputs.iter().map(|i| get(*i)).collect::<Option<Vec<_>>>()?;
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
/// input's, and the column-range guard in [`apply`] prevents the circular
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
}
