// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! An **abstract**, cardinality-free cost model.
//!
//! We deliberately do *not* know the cardinalities of base relations. Instead a
//! plan is scored by its **worst-case asymptotic cost** as a function of an
//! unknown input size `N` (all base relations treated as size `N`). Every
//! operator contributes a *work term* equal to the degree (exponent of `N`) of
//! the data it must process; the cost of a plan is the multiset of those
//! degrees, compared lexicographically largest-first (so the dominant term
//! wins), with the node count as a structural tie-breaker.
//!
//! This makes the **worst-case-optimal join** the central lever:
//!
//! * A worst-case-optimal join ([`Rel::WcoJoin`]) runs in time proportional to
//!   the **AGM bound** of the whole join — the minimum, over fractional edge
//!   covers of the join hypergraph, of `Σ λₑ·deg(Rₑ)`.
//! * A binary join ([`Rel::Join`]) must additionally materialize the
//!   intermediate results of *some* join order, whose degrees can strictly
//!   exceed the AGM bound on cyclic joins (the triangle: intermediate `N²`
//!   versus output `N^1.5`).
//!
//! Crucially, this ordering holds for *all* input sizes, so it needs no
//! cardinality estimates — the abstract cost is a statement about the worst
//! case, which is what worst-case optimality is about.

use crate::ir::{Rel, Scalar};
use std::collections::{BTreeMap, BTreeSet};

/// Numerical slack for comparing degrees.
const EPS: f64 = 1e-9;

/// The abstract cost of a plan: the work-term degrees (descending) plus the
/// node count.
#[derive(Clone, Debug, PartialEq)]
pub struct Cost {
    /// Work-term degrees, sorted descending. A larger entry, or an extra
    /// entry, means more worst-case work.
    pub degrees: Vec<f64>,
    /// Total node count; a structural tie-breaker so that algebraic
    /// simplifications which delete a node are strictly preferred.
    pub nodes: usize,
}

impl Cost {
    /// Total ordering: compare degree vectors lexicographically (largest term
    /// first), then break ties on node count.
    #[allow(clippy::should_implement_trait)]
    pub fn cmp(&self, other: &Cost) -> std::cmp::Ordering {
        use std::cmp::Ordering::*;
        let n = self.degrees.len().max(other.degrees.len());
        for i in 0..n {
            // A missing entry counts as 0 (no work); fewer/smaller terms win.
            let a = self.degrees.get(i).copied().unwrap_or(0.0);
            let b = other.degrees.get(i).copied().unwrap_or(0.0);
            if a > b + EPS {
                return Greater;
            }
            if b > a + EPS {
                return Less;
            }
        }
        self.nodes.cmp(&other.nodes)
    }

    /// Whether `self` is strictly cheaper than `other`.
    pub fn lt(&self, other: &Cost) -> bool {
        self.cmp(other) == std::cmp::Ordering::Less
    }
}

/// The abstract cost model. Currently parameter-free, but kept as a struct so
/// policy knobs (e.g. treating `Reduce` as size-reducing) can be added later.
#[derive(Clone, Debug, Default)]
pub struct CostModel;

impl CostModel {
    pub fn new() -> Self {
        CostModel
    }

    /// The worst-case output-size degree of `rel` (exponent of `N`).
    pub fn size_degree(&self, rel: &Rel) -> f64 {
        match rel {
            Rel::Constant { .. } => 0.0,
            Rel::Get { .. } => 1.0,
            // In the worst case none of these reduce the row count.
            Rel::Project { input, .. }
            | Rel::Map { input, .. }
            | Rel::Filter { input, .. }
            | Rel::Reduce { input, .. }
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

    /// The abstract cost of an entire plan.
    pub fn cost(&self, rel: &Rel) -> Cost {
        let mut degrees = Vec::new();
        self.collect_work(rel, &mut degrees);
        degrees.retain(|d| *d > EPS);
        degrees.sort_by(|a, b| b.partial_cmp(a).unwrap());
        Cost {
            degrees,
            nodes: rel.node_count(),
        }
    }

    /// Accumulate the work-term degrees of every node into `out`.
    fn collect_work(&self, rel: &Rel, out: &mut Vec<f64>) {
        match rel {
            Rel::Constant { .. } | Rel::Get { .. } => {}
            Rel::Project { input, .. }
            | Rel::Map { input, .. }
            | Rel::Filter { input, .. }
            | Rel::Reduce { input, .. }
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
            // `LetRec` is charged its *per-iteration* body cost (the bindings
            // and body, via the recursion below); the iteration count is an
            // unknown multiplier we deliberately abstract away, exactly as we do
            // cardinalities.
            Rel::Let { .. } | Rel::LetRec { .. } | Rel::LocalGet { .. } => {}
        }
        for c in rel.children() {
            self.collect_work(c, out);
        }
    }

    /// The AGM-bound degree of the full join.
    fn join_degree(&self, inputs: &[Rel], equivalences: &[Vec<Scalar>]) -> f64 {
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
    /// best left-deep order, where "best" minimizes the cost vector.
    fn binary_join_terms(&self, inputs: &[Rel], equivalences: &[Vec<Scalar>]) -> Vec<f64> {
        let n = inputs.len();
        let degs: Vec<f64> = inputs.iter().map(|r| self.size_degree(r)).collect();
        let hg = Hypergraph::build(inputs, equivalences);
        if n <= 1 {
            return vec![];
        }
        if n > 12 {
            // Fallback: a left-deep chain in input order.
            let mut set = 1u32;
            let mut terms = Vec::new();
            for i in 1..n {
                set |= 1 << i;
                terms.push(hg.agm_degree_subset(&degs, set));
            }
            return terms;
        }
        // DP over subsets: best[S] is the cost vector to materialize the join
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

/// Wrap a list of degrees in a [`Cost`] for comparison (node count ignored).
fn terms_cost(terms: &[f64]) -> Cost {
    let mut degrees: Vec<f64> = terms.iter().copied().filter(|d| *d > EPS).collect();
    degrees.sort_by(|a, b| b.partial_cmp(a).unwrap());
    Cost { degrees, nodes: 0 }
}

/// The join hypergraph: vertices are join attributes (equivalence classes) plus
/// a private vertex per input with payload columns; edges are the inputs.
struct Hypergraph {
    n_inputs: usize,
    arities: Vec<usize>,
    classes: Vec<BTreeSet<usize>>,
}

impl Hypergraph {
    fn build(inputs: &[Rel], equivalences: &[Vec<Scalar>]) -> Self {
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
                for &col in &scalar.cols {
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
    /// with input `i` weighted by `degs[i]` (its size degree). Solves the
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
/// constraints tight). The instances are tiny, so this is fast and exact.
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
/// combination. Returns false when exhausted.
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

    fn get(name: &str, arity: usize) -> Rel {
        Rel::Get {
            name: name.into(),
            arity,
        }
    }

    #[test]
    fn triangle_wcoj_beats_binary() {
        let model = CostModel::new();
        let eq = |a: usize, b: usize| {
            vec![
                Scalar::new(format!("#{a}"), [a]),
                Scalar::new(format!("#{b}"), [b]),
            ]
        };
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
        // WCOJ's dominant term is N^1.5; binary's is N^2.
        assert!((cw.degrees[0] - 1.5).abs() < 1e-6, "wcoj={cw:?}");
        assert!((cj.degrees[0] - 2.0).abs() < 1e-6, "join={cj:?}");
        assert!(cw.lt(&cj));
    }

    #[test]
    fn merge_filters_is_cheaper_structurally() {
        let model = CostModel::new();
        let p = |c: usize| Scalar::new(format!("#{c}"), [c]);
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
        // Same dominant degree, but fewer nodes ⇒ strictly cheaper.
        assert!(model.cost(&one).lt(&model.cost(&two)));
    }
}
