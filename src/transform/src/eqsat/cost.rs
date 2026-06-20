// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! A two-axis, cardinality-free cost model.
//!
//! Plans are scored along two independent axes:
//!
//! * **MEMORY** — the size of arranged (indexed) collections that must be kept
//!   in memory.  Specifically, every operator that arranges its input contributes
//!   a term equal to the worst-case size-degree of that input.  Memory is the
//!   primary scarce resource: memory-first ordering is the default.
//! * **TIME** — the worst-case asymptotic work, measured as the degree (exponent
//!   of `N`) of every operator's processing term.  Time is the secondary axis
//!   used to break memory ties.
//!
//! Both axes use the same representation: a multiset of degrees, compared
//! lexicographically largest-first so the dominant term wins (a plan with a
//! smaller leading degree, or fewer terms at equal leading degree, is cheaper).
//!
//! ## Memory terms per operator
//!
//! * [`Rel::Reduce`] arranges its input by the group key — one term at
//!   `size_degree(input)`.
//! * [`Rel::Join`] materialises the intermediates of the chosen join order —
//!   one term per pairwise intermediate, via [`CostModel::binary_join_terms`]
//!   (so a triangle binary-join contributes a term at degree 2.0).
//! * [`Rel::WcoJoin`] arranges every input for the leapfrog/generic join —
//!   one term per input at `size_degree(input_i)` (so a triangle WcoJoin
//!   contributes [1.0, 1.0, 1.0]).
//! * All other operators do not arrange their inputs; they have no memory term.
//!
//! ## Time terms per operator
//!
//! Unchanged from the original single-axis model: every operator contributes a
//! work term equal to the degree of the data it processes (joins use the
//! AGM-bound degree of the full join, binary joins use the intermediates of the
//! best left-deep order).
//!
//! ## WcoJoin vs binary-Join on the triangle
//!
//! Binary join: TIME max term = 2.0, MEMORY max term = 2.0.
//! WcoJoin:     TIME max term = 1.5, MEMORY max term = 1.0.
//!
//! WcoJoin dominates on **both** axes.

use crate::eqsat::ir::{EScalar, Rel};
use mz_expr::{Columns, Id, MirRelationExpr, MirScalarExpr};
use mz_repr::GlobalId;
use std::collections::{BTreeMap, BTreeSet};

/// Numerical slack for comparing degrees.
const EPS: f64 = 1e-9;

/// Maximum join arity for the exact `2^n` subset-DP join-order search in
/// [`CostModel::binary_join_terms`]. Above this, the DP (and its per-subset
/// combinatorial LP) is unaffordable, so a left-deep chain estimate is used
/// instead. Real joins are far below this; wide joins are rare and tolerate the
/// coarser estimate.
const MAX_EXACT_JOIN_INPUTS: usize = 8;

/// The two-axis abstract cost of a plan.
#[derive(Clone, Debug, PartialEq)]
pub struct Cost {
    /// Memory-term degrees (sorted descending, entries ≤ EPS dropped).
    ///
    /// Each entry is the worst-case size-degree of an arranged collection that
    /// must live in memory.  A larger entry, or an extra entry, means more
    /// memory pressure.
    pub memory: Vec<f64>,
    /// Time-term degrees (sorted descending, entries ≤ EPS dropped).
    ///
    /// Each entry represents the work done by one operator proportional to
    /// `N^degree`.  A larger entry, or an extra entry, means more CPU work.
    pub time: Vec<f64>,
    /// Total node count; a structural tie-breaker so that algebraic
    /// simplifications that delete a node are strictly preferred.
    pub nodes: usize,
}

impl Cost {
    /// Compare two costs with memory as the primary axis (memory first, then
    /// time, then nodes).
    ///
    /// This is the **default ordering**: memory is the scarce resource.
    pub fn cmp_memory_first(&self, other: &Cost) -> std::cmp::Ordering {
        let ord = cmp_vecs(&self.memory, &other.memory);
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
        let ord = cmp_vecs(&self.time, &other.time);
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
        self.nodes.cmp(&other.nodes)
    }

    /// Compare two costs with time as the primary axis (time first, then
    /// memory, then nodes).
    ///
    /// Use this when optimizing for throughput at the cost of extra memory.
    pub fn cmp_time_first(&self, other: &Cost) -> std::cmp::Ordering {
        let ord = cmp_vecs(&self.time, &other.time);
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
        let ord = cmp_vecs(&self.memory, &other.memory);
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
        self.nodes.cmp(&other.nodes)
    }

    /// Total ordering delegating to the default (memory-first) comparator.
    #[allow(clippy::should_implement_trait)]
    pub fn cmp(&self, other: &Cost) -> std::cmp::Ordering {
        self.cmp_memory_first(other)
    }

    /// Whether `self` is strictly cheaper than `other` under the default
    /// (memory-first) ordering.
    pub fn lt(&self, other: &Cost) -> bool {
        self.cmp_memory_first(other) == std::cmp::Ordering::Less
    }
}

/// Lexicographic comparison of two degree vectors (descending, missing entry =
/// 0.0): returns Less if `a` has a smaller dominant term (or fewer terms at the
/// same dominant degree), Greater if larger.
fn cmp_vecs(a: &[f64], b: &[f64]) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    let n = a.len().max(b.len());
    for i in 0..n {
        let av = a.get(i).copied().unwrap_or(0.0);
        let bv = b.get(i).copied().unwrap_or(0.0);
        if av > bv + EPS {
            return Greater;
        }
        if bv > av + EPS {
            return Less;
        }
    }
    Equal
}

/// The abstract cost model.
///
/// Optionally carries arrangement availability derived from `ctx.indexes`:
/// for each global relation, the set of available index keys (each key is an
/// ordered list of [`MirScalarExpr`]s).  When non-empty, the WcoJoin memory
/// cost for an input whose join key is already covered by an available
/// arrangement is zeroed, making WcoJoin correctly cheap when arrangements
/// exist for free.
///
/// `CostModel::default()` and `CostModel::new()` produce an index-blind model
/// (empty availability), preserving the existing behavior for the logical pass
/// and for callers that do not have index information.
#[derive(Clone, Debug, Default)]
pub struct CostModel {
    /// Available arrangement keys, keyed by the `GlobalId` of the relation they
    /// belong to.  Each inner `Vec<MirScalarExpr>` is one index key (the ordered
    /// list of key columns/expressions reported by the [`IndexOracle`]).
    ///
    /// [`IndexOracle`]: crate::IndexOracle
    available: BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>,
}

impl CostModel {
    /// Create an index-blind cost model (empty availability).
    pub fn new() -> Self {
        CostModel::default()
    }

    /// Create a cost model seeded with index availability.
    ///
    /// `available` maps each global relation id to the list of index keys
    /// available on it.  The WcoJoin memory cost for an input that is a direct
    /// global `Get` whose join key is covered by one of these index keys is
    /// zeroed.
    pub fn with_available(available: BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>) -> Self {
        CostModel { available }
    }

    /// The worst-case output-size degree of `rel` (exponent of `N`).
    pub fn size_degree(&self, rel: &Rel) -> f64 {
        match rel {
            Rel::Constant { .. } => 0.0,
            // A base collection or an opaque bailed subtree: treat as a base.
            Rel::Get { .. } | Rel::Opaque(_) => 1.0,
            // In the worst case none of these reduce the row count.
            Rel::Project { input, .. }
            | Rel::Map { input, .. }
            | Rel::Filter { input, .. }
            | Rel::Reduce { input, .. }
            | Rel::TopK { input, .. }
            | Rel::Negate { input }
            | Rel::Threshold { input } => self.size_degree(input),
            Rel::Union { base, inputs } => {
                let mut d = self.size_degree(base);
                for r in inputs {
                    d = d.max(self.size_degree(r));
                }
                d
            }
            Rel::Join {
                inputs,
                equivalences,
            }
            | Rel::WcoJoin {
                inputs,
                equivalences,
            } => self.join_degree(inputs, equivalences),
            Rel::Let { body, .. } | Rel::LetRec { body, .. } => self.size_degree(body),
            // Approximation: a local reference is treated as a base relation
            // (CSE'd plans are not the cost model's optimization target).
            Rel::LocalGet { .. } => 1.0,
        }
    }

    /// The abstract cost of an entire plan (both axes).
    pub fn cost(&self, rel: &Rel) -> Cost {
        let mut time = Vec::new();
        self.collect_work(rel, &mut time);
        time.retain(|d| *d > EPS);
        time.sort_by(|a, b| b.partial_cmp(a).unwrap());

        let mut memory = Vec::new();
        self.collect_memory(rel, &mut memory);
        memory.retain(|d| *d > EPS);
        memory.sort_by(|a, b| b.partial_cmp(a).unwrap());

        Cost {
            memory,
            time,
            nodes: rel.node_count(),
        }
    }

    /// Accumulate the work-term degrees (TIME axis) of every node into `out`.
    fn collect_work(&self, rel: &Rel, out: &mut Vec<f64>) {
        match rel {
            Rel::Constant { .. } | Rel::Get { .. } | Rel::Opaque(_) => {}
            Rel::Project { input, .. }
            | Rel::Map { input, .. }
            | Rel::Filter { input, .. }
            | Rel::Reduce { input, .. }
            | Rel::TopK { input, .. }
            | Rel::Negate { input }
            | Rel::Threshold { input } => out.push(self.size_degree(input)),
            Rel::Union { base, inputs } => {
                let mut d = self.size_degree(base);
                for r in inputs {
                    d = d.max(self.size_degree(r));
                }
                out.push(d);
            }
            Rel::WcoJoin {
                inputs,
                equivalences,
            } => out.push(self.join_degree(inputs, equivalences)),
            Rel::Join {
                inputs,
                equivalences,
            } => out.extend(self.binary_join_terms(inputs, equivalences)),
            // A `Let` computes its value once; both children are charged via the
            // recursion below, so the shared value is counted a single time. A
            // `LetRec` is charged its per-iteration body cost (the bindings
            // and body, via the recursion below); the iteration count is an
            // unknown multiplier we deliberately abstract away, exactly as we do
            // cardinalities.
            Rel::Let { .. } | Rel::LetRec { .. } | Rel::LocalGet { .. } => {}
        }
        for c in rel.children() {
            self.collect_work(c, out);
        }
    }

    /// Accumulate the memory-term degrees (MEMORY axis) of every node into
    /// `out`.
    ///
    /// A term is emitted for each arranged (indexed) collection that must
    /// reside in memory.
    fn collect_memory(&self, rel: &Rel, out: &mut Vec<f64>) {
        match rel {
            // Reduce arranges its input by the group key; TopK maintains
            // per-group top-k state arranged by the group and order keys.
            Rel::Reduce { input, .. } | Rel::TopK { input, .. } => {
                out.push(self.size_degree(input))
            }
            // Binary join materialises intermediate results; each intermediate
            // is arranged.  binary_join_terms returns one degree per pairwise
            // step, mirroring what collect_work does for the time axis.
            // The terms come from the time-optimal left-deep order, so this
            // charges that order's memory, assuming the engine evaluates a join
            // with one order on both axes rather than choosing a memory-optimal
            // order independently.
            Rel::Join {
                inputs,
                equivalences,
            } => out.extend(self.binary_join_terms(inputs, equivalences)),
            // WcoJoin (leapfrog/generic join) arranges every input.
            // An input whose join key is already covered by an available index
            // is not charged the arrangement-build memory term: the arrangement
            // exists for free.
            Rel::WcoJoin {
                inputs,
                equivalences,
            } => {
                let mut offset = 0usize;
                for input in inputs.iter() {
                    if !self.input_already_arranged(input, offset, equivalences) {
                        out.push(self.size_degree(input));
                    }
                    offset += input.arity();
                }
            }
            // All other operators do not arrange their inputs.
            _ => {}
        }
        // Recurse into children for all variants.
        for c in rel.children() {
            self.collect_memory(c, out);
        }
    }

    /// Whether `input` (the join input at `inp_idx` with concatenated-column
    /// `offset`) is already arranged by the key required by `equivalences`.
    ///
    /// Returns `true` only when:
    /// 1. `input` is directly a `Rel::Opaque` wrapping a global `Get` (a base
    ///    relation with no intervening projections or filters that would shift
    ///    columns), AND
    /// 2. the join key derived from `equivalences` for this input (the set of
    ///    local column indices the equivalences reference inside this input)
    ///    matches the column set of some available index on that global id.
    ///
    /// Only direct opaque-global-get inputs are matched; wrapped inputs (with
    /// a filter or project on top) are conservatively treated as unarranged.
    fn input_already_arranged(
        &self,
        input: &Rel,
        offset: usize,
        equivalences: &[Vec<EScalar>],
    ) -> bool {
        if self.available.is_empty() {
            return false;
        }
        // Only match a bare opaque global Get.
        let gid = match global_id_from_leaf(input) {
            Some(g) => g,
            None => return false,
        };
        let keys = match self.available.get(&gid) {
            Some(ks) => ks,
            None => return false,
        };
        // Compute the set of local column indices (relative to this input's
        // arity) that the equivalences require this input to be keyed by.
        let input_arity = input.arity();
        let join_key_cols = join_key_cols_for_input(offset, input_arity, equivalences);
        if join_key_cols.is_empty() {
            // No join constraint on this input: no arrangement needed.
            return true;
        }
        // Check whether any available index covers exactly the join key columns.
        // An index whose key column set equals the join key column set means the
        // collection is already arranged by exactly what is needed.
        keys.iter().any(|key| {
            // Collect the column indices in this index key (only plain column
            // references; expressions are not matched).
            let idx_cols: BTreeSet<usize> = key.iter().filter_map(|e| e.as_column()).collect();
            idx_cols == join_key_cols
        })
    }

    /// The AGM-bound degree of the full join.
    fn join_degree(&self, inputs: &[Rel], equivalences: &[Vec<EScalar>]) -> f64 {
        let degs: Vec<f64> = inputs.iter().map(|r| self.size_degree(r)).collect();
        let hg = Hypergraph::build(inputs, equivalences);
        let full = if inputs.len() >= 32 {
            u32::MAX
        } else {
            (1u32 << inputs.len()) - 1
        };
        hg.agm_degree_subset(&degs, full)
    }

    /// The work terms of a binary-join plan: the intermediate degrees of the
    /// best left-deep order, where "best" minimises the cost vector.
    fn binary_join_terms(&self, inputs: &[Rel], equivalences: &[Vec<EScalar>]) -> Vec<f64> {
        let n = inputs.len();
        let degs: Vec<f64> = inputs.iter().map(|r| self.size_degree(r)).collect();
        let hg = Hypergraph::build(inputs, equivalences);
        if n <= 1 {
            return vec![];
        }
        if n > MAX_EXACT_JOIN_INPUTS {
            // Fallback: a left-deep chain in input order. The exact subset DP is
            // `2^n` and each subset runs a combinatorial LP, so for wide joins it
            // is unaffordable. The left-deep chain costs `n-1` LP solves and is a
            // valid (if coarser) estimate.
            let mut set = 1u32;
            let mut terms = Vec::new();
            for i in 1..n {
                set |= 1 << i;
                terms.push(hg.agm_degree_subset(&degs, set));
            }
            return terms;
        }
        // DP over subsets: best[S] is the cost vector to materialise the join
        // of the inputs in S, choosing the cheapest order.
        let full = (1u32 << n) - 1;
        let mut best: Vec<Option<Vec<f64>>> = vec![None; 1 << n];
        for i in 0..n {
            best[1 << i] = Some(vec![]); // a single input needs no join work
        }
        for s in 1..=full {
            if s.count_ones() < 2 {
                continue;
            }
            let agm = hg.agm_degree_subset(&degs, s);
            let mut sub = s;
            while sub > 0 {
                let i = sub.trailing_zeros();
                let rest = s & !(1 << i);
                if rest != 0 {
                    if let Some(rest_terms) = &best[rest as usize] {
                        let mut cand = rest_terms.clone();
                        cand.push(agm);
                        let cur = &best[s as usize];
                        if cur
                            .as_ref()
                            .is_none_or(|c| terms_cost(&cand).lt(&terms_cost(c)))
                        {
                            best[s as usize] = Some(cand);
                        }
                    }
                }
                sub &= sub - 1;
            }
        }
        best[full as usize].clone().unwrap_or_default()
    }
}

/// Extract the `GlobalId` from a leaf `Rel` if it is a direct opaque global
/// `Get`.
///
/// Returns `Some(gid)` only for `Rel::Opaque(MirRelationExpr::Get { Id::Global(gid) })`.
/// All other leaves (local gets, filtered/projected inputs) return `None`.
fn global_id_from_leaf(rel: &Rel) -> Option<GlobalId> {
    if let Rel::Opaque(mir) = rel {
        if let MirRelationExpr::Get {
            id: Id::Global(gid),
            ..
        } = mir.as_ref()
        {
            return Some(*gid);
        }
    }
    None
}

/// Compute the set of local column indices (relative to the start of `input`)
/// that `equivalences` require as a join key for a specific join input.
///
/// A column `c` in the concatenated column space belongs to the input at
/// `[offset, offset + input_arity)`.  The local index is `c - offset`.  A
/// column is part of the join key for this input when its equivalence class
/// also references at least one column from another input (so it is actually
/// constrained, not merely appearing unshared).
fn join_key_cols_for_input(
    offset: usize,
    input_arity: usize,
    equivalences: &[Vec<EScalar>],
) -> BTreeSet<usize> {
    let mut key_cols = BTreeSet::new();
    for class in equivalences {
        // Gather columns from this class that fall inside this input's range.
        let mut local_cols: Vec<usize> = Vec::new();
        let mut touches_other = false;
        for escalar in class {
            for col in escalar.cols() {
                if col >= offset && col < offset + input_arity {
                    local_cols.push(col - offset);
                } else {
                    touches_other = true;
                }
            }
        }
        // Only include columns that are genuinely constrained across inputs.
        if touches_other {
            key_cols.extend(local_cols);
        }
    }
    key_cols
}

/// Wrap a list of degrees in a [`Cost`] for comparison (node count ignored,
/// used for the binary-join DP which only needs to compare time vectors).
fn terms_cost(terms: &[f64]) -> Cost {
    let mut time: Vec<f64> = terms.iter().copied().filter(|d| *d > EPS).collect();
    time.sort_by(|a, b| b.partial_cmp(a).unwrap());
    Cost {
        memory: vec![],
        time,
        nodes: 0,
    }
}

/// The join hypergraph: vertices are join attributes (equivalence classes) plus
/// a private vertex per input with payload columns; edges are the inputs.
struct Hypergraph {
    n_inputs: usize,
    arities: Vec<usize>,
    classes: Vec<BTreeSet<usize>>,
}

impl Hypergraph {
    fn build(inputs: &[Rel], equivalences: &[Vec<EScalar>]) -> Self {
        let arities: Vec<usize> = inputs.iter().map(Rel::arity).collect();
        let mut offsets = Vec::with_capacity(inputs.len());
        let mut acc = 0usize;
        for a in &arities {
            offsets.push(acc);
            acc += a;
        }
        let input_of =
            |col: usize| -> Option<usize> { (0..arities.len()).rev().find(|&i| col >= offsets[i]) };

        let mut classes = Vec::new();
        for class in equivalences {
            let mut members: BTreeSet<usize> = BTreeSet::new();
            for scalar in class {
                for col in scalar.cols() {
                    if let Some(i) = input_of(col) {
                        members.insert(i);
                    }
                }
            }
            if !members.is_empty() {
                classes.push(members);
            }
        }
        Hypergraph {
            n_inputs: inputs.len(),
            arities,
            classes,
        }
    }

    /// AGM-bound degree for the sub-join over the inputs selected in `subset`,
    /// with input `i` weighted by `degs[i]` (its size degree).  Solves the
    /// fractional-edge-cover LP `min Σ λᵢ degᵢ s.t. cover`.
    fn agm_degree_subset(&self, degs: &[f64], subset: u32) -> f64 {
        let edges: Vec<usize> = (0..self.n_inputs)
            .filter(|i| subset & (1 << i) != 0)
            .collect();
        if edges.is_empty() {
            return 0.0;
        }
        if edges.len() == 1 {
            return degs[edges[0]];
        }
        let var_of: BTreeMap<usize, usize> =
            edges.iter().enumerate().map(|(v, &i)| (i, v)).collect();

        let mut rows: Vec<BTreeSet<usize>> = Vec::new();
        let mut shared_touch = vec![0usize; self.n_inputs];
        for members in &self.classes {
            let in_subset: Vec<usize> = members
                .iter()
                .copied()
                .filter(|i| var_of.contains_key(i))
                .collect();
            if in_subset.len() >= 2 {
                for &i in &in_subset {
                    shared_touch[i] += 1;
                }
                rows.push(in_subset.iter().map(|i| var_of[i]).collect());
            }
        }
        // Private vertices: an input whose columns are not all join attributes
        // contributes a private attribute, forcing its cover weight to ≥ 1.
        for &i in &edges {
            if shared_touch[i] < self.arities[i] {
                let mut s = BTreeSet::new();
                s.insert(var_of[&i]);
                rows.push(s);
            }
        }
        let weights: Vec<f64> = edges.iter().map(|&i| degs[i]).collect();
        solve_cover_lp(edges.len(), &rows, &weights)
    }
}

/// Solve `min Σ wᵢ xᵢ s.t. (each row) Σ_{i∈row} xᵢ ≥ 1, x ≥ 0` exactly, by
/// enumerating vertices of the feasible polyhedron (each makes `n_vars`
/// constraints tight).  The instances are tiny, so this is fast and exact.
fn solve_cover_lp(n_vars: usize, rows: &[BTreeSet<usize>], weights: &[f64]) -> f64 {
    if rows.is_empty() {
        return 0.0;
    }
    struct Constraint {
        coeffs: Vec<f64>,
        rhs: f64,
    }
    let mut cons: Vec<Constraint> = Vec::new();
    for r in rows {
        let mut c = vec![0.0; n_vars];
        for &i in r {
            c[i] = 1.0;
        }
        cons.push(Constraint {
            coeffs: c,
            rhs: 1.0,
        });
    }
    for i in 0..n_vars {
        let mut c = vec![0.0; n_vars];
        c[i] = 1.0;
        cons.push(Constraint {
            coeffs: c,
            rhs: 0.0,
        });
    }

    let m = cons.len();
    let mut best = f64::INFINITY;
    let mut idx: Vec<usize> = (0..n_vars).collect();
    // The vertex enumeration is `C(m, n_vars)`, which is combinatorial: a wide
    // join with many equivalence rows can make this astronomically large and
    // hang the optimizer. Cap the number of vertices examined; on overflow,
    // fall back to the trivial cover (every edge weight 1, i.e. the cross
    // product). That bound is an over-estimate, so it never makes a bad join
    // look cheap, and it only triggers on joins far larger than any we plan
    // exactly.
    let mut budget = MAX_LP_VERTICES;
    loop {
        let mut a = vec![vec![0.0; n_vars]; n_vars];
        let mut b = vec![0.0; n_vars];
        for (r, &ci) in idx.iter().enumerate() {
            a[r].copy_from_slice(&cons[ci].coeffs);
            b[r] = cons[ci].rhs;
        }
        if let Some(x) = gaussian_solve(a, b) {
            let feasible = x.iter().all(|&v| v >= -1e-9)
                && cons.iter().all(|c| {
                    let lhs: f64 = c.coeffs.iter().zip(&x).map(|(a, b)| a * b).sum();
                    lhs >= c.rhs - 1e-9
                });
            if feasible {
                let obj: f64 = weights.iter().zip(&x).map(|(w, v)| w * v.max(0.0)).sum();
                if obj < best {
                    best = obj;
                }
            }
        }
        budget -= 1;
        if budget == 0 {
            // Enumeration too large: use the conservative trivial bound.
            return weights.iter().sum();
        }
        if !next_combination(&mut idx, m) {
            break;
        }
    }
    if best.is_finite() {
        best
    } else {
        weights.iter().sum()
    }
}

/// Maximum number of LP feasible-basis vertices [`solve_cover_lp`] enumerates
/// before falling back to the trivial cover bound. Keeps the combinatorial
/// vertex enumeration from hanging on wide joins.
const MAX_LP_VERTICES: usize = 200_000;

/// Gaussian elimination with partial pivoting; `None` if singular.
#[allow(clippy::needless_range_loop)]
fn gaussian_solve(mut a: Vec<Vec<f64>>, mut b: Vec<f64>) -> Option<Vec<f64>> {
    let n = b.len();
    for col in 0..n {
        let mut piv = col;
        for r in (col + 1)..n {
            if a[r][col].abs() > a[piv][col].abs() {
                piv = r;
            }
        }
        if a[piv][col].abs() < 1e-12 {
            return None;
        }
        a.swap(col, piv);
        b.swap(col, piv);
        for r in 0..n {
            if r == col {
                continue;
            }
            let f = a[r][col] / a[col][col];
            if f != 0.0 {
                for c in col..n {
                    a[r][c] -= f * a[col][c];
                }
                b[r] -= f * b[col];
            }
        }
    }
    let mut x = vec![0.0; n];
    for i in 0..n {
        x[i] = b[i] / a[i][i];
    }
    Some(x)
}

/// Advance `idx` (strictly increasing `k` indices in `0..m`) to the next
/// combination.  Returns false when exhausted.
fn next_combination(idx: &mut [usize], m: usize) -> bool {
    let k = idx.len();
    if k == 0 {
        return false;
    }
    let mut i = k;
    while i > 0 {
        i -= 1;
        if idx[i] != i + m - k {
            idx[i] += 1;
            for j in (i + 1)..k {
                idx[j] = idx[j - 1] + 1;
            }
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_expr::MirScalarExpr;

    fn get(name: &str, arity: usize) -> Rel {
        Rel::Get {
            name: name.into(),
            arity,
        }
    }

    fn col(c: usize) -> EScalar {
        EScalar::plain(MirScalarExpr::column(c))
    }

    #[mz_ore::test]
    fn triangle_wcoj_beats_binary() {
        let model = CostModel::new();
        let eq = |a: usize, b: usize| vec![col(a), col(b)];
        // R(#0,#1) S(#2,#3) T(#4,#5); a:#0=#4 b:#1=#2 c:#3=#5 — a pure triangle.
        let inputs = vec![get("R", 2), get("S", 2), get("T", 2)];
        let equivalences = vec![eq(0, 4), eq(1, 2), eq(3, 5)];

        let join = Rel::Join {
            inputs: inputs.clone(),
            equivalences: equivalences.clone(),
        };
        let wcoj = Rel::WcoJoin {
            inputs,
            equivalences,
        };

        let cj = model.cost(&join);
        let cw = model.cost(&wcoj);
        // WCOJ's dominant time term is N^1.5; binary's is N^2.
        assert!((cw.time[0] - 1.5).abs() < 1e-6, "wcoj time={:?}", cw.time);
        assert!((cj.time[0] - 2.0).abs() < 1e-6, "join time={:?}", cj.time);
        assert!(cw.lt(&cj));
    }

    #[mz_ore::test]
    fn merge_filters_is_cheaper_structurally() {
        let model = CostModel::new();
        let p = col;
        let two = Rel::Filter {
            predicates: vec![p(0)],
            input: Box::new(Rel::Filter {
                predicates: vec![p(1)],
                input: Box::new(get("R", 2)),
            }),
        };
        let one = Rel::Filter {
            predicates: vec![p(0), p(1)],
            input: Box::new(get("R", 2)),
        };
        // Same dominant degree, but fewer nodes => strictly cheaper.
        assert!(model.cost(&one).lt(&model.cost(&two)));
    }

    #[mz_ore::test]
    fn triangle_wcojoin_dominates() {
        let model = CostModel::new();
        let eq = |a: usize, b: usize| vec![col(a), col(b)];
        let inputs = vec![get("R", 2), get("S", 2), get("T", 2)];
        let equivalences = vec![eq(0, 4), eq(1, 2), eq(3, 5)];

        let binary = Rel::Join {
            inputs: inputs.clone(),
            equivalences: equivalences.clone(),
        };
        let wcoj = Rel::WcoJoin {
            inputs,
            equivalences,
        };

        let cb = model.cost(&binary);
        let cw = model.cost(&wcoj);

        // WcoJoin memory: one term per input at degree 1.0 each.
        // Binary join memory: intermediate at degree 2.0.
        assert!(
            cw.memory.first().copied().unwrap_or(0.0) < cb.memory.first().copied().unwrap_or(0.0),
            "WcoJoin memory max={:?} must be < binary memory max={:?}",
            cw.memory.first(),
            cb.memory.first(),
        );
        // WcoJoin time: AGM bound 1.5.  Binary time: worst intermediate 2.0.
        assert!(
            cw.time.first().copied().unwrap_or(0.0) < cb.time.first().copied().unwrap_or(0.0),
            "WcoJoin time max={:?} must be < binary time max={:?}",
            cw.time.first(),
            cb.time.first(),
        );
        // Default (memory-first) ordering: WcoJoin strictly cheaper.
        assert_eq!(
            cw.cmp_memory_first(&cb),
            std::cmp::Ordering::Less,
            "WcoJoin must dominate binary join under memory-first ordering"
        );
    }

    #[mz_ore::test]
    fn memory_first_picks_lower_memory() {
        // Plan A: lower memory, higher time.
        // Plan B: lower time, higher memory.
        // Default ordering must prefer A.
        let a = Cost {
            memory: vec![1.0],
            time: vec![2.0],
            nodes: 1,
        };
        let b = Cost {
            memory: vec![2.0],
            time: vec![1.0],
            nodes: 1,
        };
        assert_eq!(
            a.cmp_memory_first(&b),
            std::cmp::Ordering::Less,
            "memory-first: lower-memory plan A must beat higher-memory plan B"
        );
        assert!(a.lt(&b), "lt() must agree with memory-first ordering");
        // Time-first ordering reverses the preference.
        assert_eq!(
            a.cmp_time_first(&b),
            std::cmp::Ordering::Greater,
            "time-first: lower-time plan B must beat higher-time plan A"
        );
    }

    #[mz_ore::test]
    fn recommendation_logic_direct() {
        // Directly verify the recommendation decision logic used in engine.rs:
        // if time-first plan differs from memory-first plan, and is strictly
        // faster (lower time) but uses more memory, a recommendation fires.

        // memory-optimal plan: good memory, high time.
        let mem_cost = Cost {
            memory: vec![1.0],
            time: vec![2.0],
            nodes: 3,
        };
        // time-optimal plan: bad memory, low time.
        let time_cost = Cost {
            memory: vec![2.0],
            time: vec![1.0],
            nodes: 3,
        };

        // The time plan is strictly faster.
        assert_eq!(
            time_cost.cmp_time_first(&mem_cost),
            std::cmp::Ordering::Less,
            "time-optimal plan must have strictly lower time cost"
        );
        // The time plan uses more memory.
        assert_eq!(
            cmp_vecs(&time_cost.memory, &mem_cost.memory),
            std::cmp::Ordering::Greater,
            "time-optimal plan must use more memory"
        );
        // The memory plan is what the default ordering picks.
        assert_eq!(
            mem_cost.cmp_memory_first(&time_cost),
            std::cmp::Ordering::Less,
            "memory-first ordering must prefer the memory-optimal plan"
        );
    }

    // Helpers for index-aware cost model tests.

    use mz_expr::{AccessStrategy, Id};
    use mz_repr::{GlobalId, ReprRelationType, ReprScalarType};

    /// Build a `Rel::Opaque` wrapping a global `Get` for the given transient id
    /// and arity.  This mirrors how `lower` handles `MirRelationExpr::Get { Id::Global }`.
    fn global_opaque(id: u64, arity: usize) -> Rel {
        let typ = ReprRelationType::new(
            (0..arity)
                .map(|_| ReprScalarType::Int64.nullable(false))
                .collect(),
        );
        Rel::Opaque(Box::new(MirRelationExpr::Get {
            id: Id::Global(GlobalId::Transient(id)),
            typ,
            access_strategy: AccessStrategy::UnknownOrLocal,
        }))
    }

    /// Build an availability map with a single index key for a global relation.
    fn avail_one(id: u64, key_cols: &[usize]) -> BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>> {
        let key: Vec<MirScalarExpr> = key_cols.iter().map(|&c| MirScalarExpr::column(c)).collect();
        let mut m = BTreeMap::new();
        m.insert(GlobalId::Transient(id), vec![key]);
        m
    }

    /// The WcoJoin memory terms for the triangle with the given cost model.
    /// Triangle: R(2), S(2), T(2) with equivalences #0=#4, #1=#2, #3=#5.
    fn triangle_wcoj_memory(model: &CostModel) -> Vec<f64> {
        let eq = |a: usize, b: usize| vec![col(a), col(b)];
        let inputs = vec![
            global_opaque(1, 2),
            global_opaque(2, 2),
            global_opaque(3, 2),
        ];
        let equivalences = vec![eq(0, 4), eq(1, 2), eq(3, 5)];
        let wcoj = Rel::WcoJoin {
            inputs,
            equivalences,
        };
        let cost = model.cost(&wcoj);
        cost.memory
    }

    #[mz_ore::test]
    fn index_aware_no_arrangement_term_for_indexed_input() {
        // When input R (id=1) has an index on its join key (columns 0 and 1,
        // the local columns referenced in the #0=#4 and #1=#2 equivalences),
        // the cost model must NOT charge R's arrangement-build memory term.
        //
        // R's join key in the triangle: #0 (from eq #0=#4) and #1 (from eq
        // #1=#2).  The local columns are 0 and 1, so the index key is [0, 1].
        let available = avail_one(1, &[0, 1]);
        let model_aware = CostModel::with_available(available);
        let model_blind = CostModel::new();

        let mem_aware = triangle_wcoj_memory(&model_aware);
        let mem_blind = triangle_wcoj_memory(&model_blind);

        // Blind model: 3 memory terms (one per input, each at degree 1.0).
        assert_eq!(
            mem_blind.len(),
            3,
            "blind model must charge all 3 WcoJoin inputs; got {mem_blind:?}"
        );
        // Index-aware model: 2 memory terms (R is free, S and T are charged).
        assert_eq!(
            mem_aware.len(),
            2,
            "index-aware model must omit R's arrangement term; got {mem_aware:?}"
        );
    }

    #[mz_ore::test]
    fn index_aware_wrong_key_still_charges_arrangement() {
        // An index on R with the wrong key (only column 0, not [0,1]) does not
        // cover the full join key for R, so the arrangement term is still charged.
        let available = avail_one(1, &[0]); // partial key only
        let model = CostModel::with_available(available);
        let mem = triangle_wcoj_memory(&model);
        // All 3 inputs must still be charged.
        assert_eq!(
            mem.len(),
            3,
            "partial-key index must not suppress the arrangement term; got {mem:?}"
        );
    }

    #[mz_ore::test]
    fn index_blind_triangle_still_prefers_wcoj() {
        // Regression guard: without any index availability, the index-blind
        // model (CostModel::new()) must still prefer WcoJoin over binary join
        // for the triangle.  This ensures the new index-aware path does not
        // alter the default (index-blind) behavior.
        let model = CostModel::new();
        let eq = |a: usize, b: usize| vec![col(a), col(b)];
        let inputs = vec![
            global_opaque(1, 2),
            global_opaque(2, 2),
            global_opaque(3, 2),
        ];
        let equivalences = vec![eq(0, 4), eq(1, 2), eq(3, 5)];

        let join = Rel::Join {
            inputs: inputs.clone(),
            equivalences: equivalences.clone(),
        };
        let wcoj = Rel::WcoJoin {
            inputs,
            equivalences,
        };

        let cj = model.cost(&join);
        let cw = model.cost(&wcoj);
        // WcoJoin must still dominate binary join on both axes.
        assert!(
            cw.lt(&cj),
            "index-blind WcoJoin must dominate binary join; wcoj={cw:?} join={cj:?}"
        );
        assert!(
            cw.memory.first().copied().unwrap_or(0.0) < cj.memory.first().copied().unwrap_or(0.0),
            "WcoJoin memory max must be lower than binary join memory max"
        );
    }
}
