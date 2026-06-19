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
//! optimizer instead **saturates** an e-graph — it applies *every* rule
//! wherever it matches, regardless of cost, recording the resulting
//! equivalences compactly — and only at the end extracts the cheapest plan.
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

use mz_expr::{AggregateExpr, MirRelationExpr};

use crate::eqsat::analysis::{Analysis, KeySet, Keys, LocalFacts, Monotonic, NonNeg, is_superkey};
use crate::eqsat::cost::{Cost, CostModel};
use crate::eqsat::dsl::*;
use crate::eqsat::ir::{EScalar, Rel};
use crate::eqsat::matcher::{Payload, eval_ixexpr, eval_pexpr};

/// An e-class identifier.
pub type Id = usize;

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
/// values it could take given the bindings so far — the worst-case-optimal
/// join strategy.
fn generic_join(query: &Query, index: &HashMap<Sym, Vec<(Id, ENode)>>) -> Vec<HashMap<VarId, Id>> {
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
        &order,
        0,
        &mut assignment,
        &mut results,
        &empty,
    );
    results
}

fn solve(
    query: &Query,
    index: &HashMap<Sym, Vec<(Id, ENode)>>,
    order: &[VarId],
    depth: usize,
    assignment: &mut HashMap<VarId, Id>,
    out: &mut Vec<HashMap<VarId, Id>>,
    empty: &Vec<(Id, ENode)>,
) {
    if depth == order.len() {
        out.push(assignment.clone());
        return;
    }
    let var = order[depth];

    // Intersect candidate values for `var` across every atom that mentions it.
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

    let candidates = candidates.unwrap_or_default();
    for val in candidates {
        assignment.insert(var, val);
        solve(query, index, order, depth + 1, assignment, out, empty);
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

        let mut iters = 0;
        for _ in 0..max_iters {
            iters += 1;
            self.rebuild();
            let index = self.index();

            // Phase 1: read-only — collect every rewrite to apply.
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
            };
            let mut pending: Vec<(usize, EBindings)> = Vec::new();
            for (ri, query) in &queries {
                let rule = &rules.rules[*ri];
                for assignment in generic_join(query, &index) {
                    for b in expand_bindings(query, &index, &assignment) {
                        if self.check_conds(&rule.conds, &b, &analyses) {
                            pending.push((*ri, b));
                        }
                    }
                }
            }

            // Phase 2: mutate — instantiate right-hand sides and union.
            let mut changed = false;
            for (ri, b) in pending {
                let rule = &rules.rules[ri];
                let arities = self.binding_arities(&b);
                if let Ok(new_id) = self.instantiate(&rule.rhs, &b, None, &arities) {
                    if self.union(new_id, b.root) {
                        changed = true;
                    }
                }
            }

            if !changed {
                break;
            }
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
        })
    }

    /// Run a lattice-valued [`Analysis`] to a fixpoint, one fact per e-class.
    pub fn run_analysis<A: Analysis>(&self, a: &A) -> HashMap<Id, A::Domain> {
        let mut m: HashMap<Id, A::Domain> =
            self.classes.keys().map(|&id| (id, a.bottom())).collect();
        loop {
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
            for (id, d) in updates {
                m.insert(id, d);
            }
        }
        m
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
    /// * `true`  — memory-first ordering (default; memory is the scarce resource).
    /// * `false` — time-first ordering (minimise CPU work, may use more memory).
    pub fn extract_with(&self, root: Id, model: &CostModel, memory_first: bool) -> Rel {
        let cmp: &dyn Fn(&Cost, &Cost) -> std::cmp::Ordering = if memory_first {
            &|a, b| a.cmp_memory_first(b)
        } else {
            &|a, b| a.cmp_time_first(b)
        };

        let mut best: HashMap<Id, (Cost, Rel)> = HashMap::new();
        for _ in 0..(self.classes.len() + 1) {
            let mut changed = false;
            for (&id, nodes) in &self.classes {
                for node in nodes {
                    if let Some(rel) = self.build_rel(node, &best) {
                        let c = model.cost(&rel);
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
