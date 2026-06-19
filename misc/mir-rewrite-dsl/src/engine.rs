// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! The optimizer compiled from a rule set.
//!
//! [`Optimizer`] is the real one: it saturates an e-graph (exploring the
//! transform graph in a worst-case-optimal manner via [`crate::egraph`]) and
//! extracts the cheapest plan, so it never gets stuck in a local minimum.
//!
//! [`GreedyOptimizer`] is a foil kept for comparison: it applies the single
//! cost-reducing rewrite at each step (steepest descent). Because some
//! beneficial rewrites are reachable only through cost-neutral intermediate
//! steps, it can terminate at a worse plan than [`Optimizer`] — see the tests
//! and `README.md`.

use std::collections::BTreeMap;

use crate::analysis::{letrec_local_facts, LocalFacts};
use crate::cost::{Cost, CostModel};
use crate::dsl::{Rule, RuleSet};
use crate::egraph::EGraph;
use crate::ir::{Rel, Scalar};
use crate::matcher::{check_conds, instantiate, match_pat, Bindings};

/// Bound on the outer fixpoint that re-analyzes a binding scope after rewriting
/// it (a rewrite can reveal a stronger recursive invariant that enables another
/// rewrite). Convergence is fast — the analyses are monotone and the rewrites
/// idempotent — so a small cap suffices.
const SCOPE_REFINE_ROUNDS: usize = 4;

/// The outcome of optimizing a plan.
#[derive(Clone, Debug)]
pub struct Outcome {
    pub plan: Rel,
    pub initial_cost: Cost,
    pub final_cost: Cost,
    /// Number of saturation (or descent) iterations performed.
    pub iterations: usize,
}

/// The saturating optimizer: equality saturation + cheapest-plan extraction.
#[derive(Clone, Debug)]
pub struct Optimizer {
    rules: RuleSet,
    model: CostModel,
    max_iters: usize,
}

impl Optimizer {
    pub fn new(rules: RuleSet, model: CostModel) -> Self {
        Optimizer {
            rules,
            model,
            max_iters: 100,
        }
    }

    /// Optimize `plan` by saturating an e-graph and extracting the cheapest
    /// equivalent plan.
    ///
    /// `Let`/`LetRec` are *binding scopes*, not relational operators, and a
    /// recursive `LetRec` closes a cycle that finite saturation cannot
    /// represent. So they are handled **structurally**: the optimizer treats
    /// each scope as a boundary, recursively optimizing every maximal Let-free
    /// fragment (binding values and bodies) by equality saturation, then
    /// reassembling the scope. Recursive references are opaque `LocalGet` leaves
    /// within a fragment. Analyses still flow *through* the recursion via the
    /// recursion-aware fixpoint in [`crate::analysis`].
    pub fn optimize(&self, plan: Rel) -> Outcome {
        let initial_cost = self.model.cost(&plan);
        // Scalar layer: constant-fold every scalar in place (the analog of
        // Materialize's FoldConstants / canonicalize_mfp). This is deliberately
        // *not* a relational rewrite — it only normalizes scalars. The
        // relational consequences (drop an all-true filter, empty a false one,
        // turn a column-only Map into a Project) are DSL rules gated by the
        // `all_true` / `any_false` / `all_columns` scalar-structure conditions.
        let plan = canonicalize_scalars(plan);
        let (best, iterations) = self.optimize_node(plan, &LocalFacts::default());
        let final_cost = self.model.cost(&best);
        Outcome {
            plan: best,
            initial_cost,
            final_cost,
            iterations,
        }
    }

    /// Optimize one node under the recursion facts `facts` (the proven
    /// non-negativity / monotonicity / keys of every in-scope `LocalGet`).
    /// Saturate it if it is a Let-free fragment; otherwise walk past the binding
    /// scope. Returns the optimized plan and the saturation iterations spent.
    fn optimize_node(&self, plan: Rel, facts: &LocalFacts) -> (Rel, usize) {
        // Normalize first: push any single-input operator sitting directly above
        // a binding scope *into* the scope's body. `O(LetRec x = b in B)` denotes
        // `O(B[x*])`, and so does `LetRec x = b in O(B)` — the bindings (hence
        // the fixpoint `x*`) are untouched — so this is unconditionally sound. It
        // moves the operator inside the scope, where the recursion facts can act
        // on it. (It does *not* push into the recursive bindings; see
        // `COVERAGE.md` on why predicate pushdown *through* a recursion is
        // unsound without a commutation side-condition.)
        let plan = normalize_push_into_scopes(plan);
        match &plan {
            // A binding scope: analyze the recursion and optimize its fragments
            // with those facts injected.
            Rel::Let { .. } | Rel::LetRec { .. } => self.optimize_scope(plan, facts),
            // A node *above* a scope (necessarily multi-input, e.g. a Union/Join
            // with a scope argument — unary ones were pushed in above): rewrite
            // the region around the recursion, treating each maximal scope
            // subtree as an opaque value carrying its proven properties (A).
            _ if contains_scope(&plan) => self.optimize_around_scopes(plan, facts),
            // A maximal Let-free fragment: saturate and extract, seeding the
            // analyses with the recursion facts so analysis-gated rules can fire
            // on recursive references.
            _ => {
                let mut eg = EGraph::new();
                let root = eg.add_rel(&plan);
                let iterations = eg.saturate(&self.rules, self.max_iters, facts);
                (eg.extract(root, &self.model), iterations)
            }
        }
    }

    /// Optimize a `Let`/`LetRec` scope. We solve the recursion-aware analyses
    /// for the bound ids (extending `outer`), inject the resulting facts while
    /// optimizing each binding value and the body, and repeat: a rewrite can
    /// expose a stronger invariant that unlocks another. This is option B — the
    /// recursion fixpoint feeding the in-fragment rewriter — and it is sound
    /// because each fact is a greatest-/least-fixpoint certificate on the
    /// current syntactic form, and equality rewrites preserve the underlying
    /// property.
    fn optimize_scope(&self, plan: Rel, outer: &LocalFacts) -> (Rel, usize) {
        let (mut bindings, mut body, recursive) = match plan {
            Rel::LetRec { bindings, body } => (bindings, *body, true),
            Rel::Let { id, value, body } => (vec![(id, *value)], *body, false),
            _ => unreachable!("optimize_scope on a non-scope node"),
        };

        let mut total = 0;
        for _ in 0..SCOPE_REFINE_ROUNDS {
            let facts = letrec_local_facts(&bindings, outer);
            let mut changed = false;

            let mut next = Vec::with_capacity(bindings.len());
            for (id, value) in &bindings {
                let (v, i) = self.optimize_node(value.clone(), &facts);
                total += i;
                changed |= v != *value;
                next.push((*id, v));
            }
            let (nb, i) = self.optimize_node(body.clone(), &facts);
            total += i;
            changed |= nb != body;

            bindings = next;
            body = nb;
            if !changed {
                break;
            }
        }

        let result = if recursive {
            Rel::LetRec {
                bindings,
                body: Box::new(body),
            }
        } else {
            let (id, value) = bindings.into_iter().next().unwrap();
            Rel::Let {
                id,
                value: Box::new(value),
                body: Box::new(body),
            }
        };
        (result, total)
    }

    /// Optimize a fragment that sits *above* one or more binding scopes (option
    /// A, sound core). We cannot e-match *through* the recursive back-edge
    /// soundly — the fixpoint equation `x = body(x)` holds only at the fixpoint,
    /// so rewriting with it can change the denoted least fixpoint (see
    /// `COVERAGE.md`). What *is* sound is to treat each maximal recursion as an
    /// opaque relation that carries the properties the recursion fixpoint
    /// proves (non-negative / monotone / keyed), then let the ordinary rules
    /// rewrite the surrounding fragment using those facts — e.g. eliding a
    /// `Threshold` wrapped around a provably non-negative recursion.
    fn optimize_around_scopes(&self, plan: Rel, facts: &LocalFacts) -> (Rel, usize) {
        // Replace each maximal scope subtree with a fresh opaque `LocalGet`
        // placeholder (ids chosen above any real bound id, so they cannot clash
        // with genuine recursive references).
        let mut next_id = max_local_id(&plan) + 1;
        let mut scopes: Vec<(usize, Rel)> = Vec::new();
        let placeholder_plan = hoist_scopes(plan, &mut next_id, &mut scopes);

        // Optimize each scope and record its proven properties under the
        // placeholder id, extending the incoming facts.
        let mut ext = facts.clone();
        let mut subst: BTreeMap<usize, Rel> = BTreeMap::new();
        let mut iters = 0;
        for (id, scope) in scopes {
            let (opt, i) = self.optimize_node(scope, facts);
            iters += i;
            ext.nonneg.insert(id, crate::analysis::rel_non_negative(&opt));
            ext.monotonic.insert(id, crate::analysis::rel_monotonic(&opt));
            ext.keys.insert(id, crate::analysis::rel_keys(&opt));
            subst.insert(id, opt);
        }

        // Saturate the (now Let-free) surrounding fragment with those facts,
        // then splice the optimized scopes back in.
        let mut eg = EGraph::new();
        let root = eg.add_rel(&placeholder_plan);
        iters += eg.saturate(&self.rules, self.max_iters, &ext);
        let extracted = eg.extract(root, &self.model);
        (substitute_locals(extracted, &subst), iters)
    }
}

/// The scalar layer: constant-fold every scalar in `rel` in place, and drop
/// predicates that fold to the literal `true` from `Filter` conjunctions
/// (canonicalizing the predicate list, like Materialize's `canonicalize_mfp`).
/// This performs **no** relational rewrite — emptying a false filter, dropping
/// an all-true filter, and turning a column-only Map into a Project are DSL
/// rules (`empty_false_filter` / `drop_true_filter` / `map_columns_to_projection`).
/// A scalar is rewritten only when folding changes it, so symbolic/opaque
/// scalars are left byte-for-byte intact.
fn canonicalize_scalars(rel: Rel) -> Rel {
    // Bottom-up: canonicalize children first.
    let children: Vec<Rel> = rel
        .children()
        .into_iter()
        .cloned()
        .map(canonicalize_scalars)
        .collect();
    let rel = rel.with_children(children);

    match rel {
        Rel::Filter { input, predicates } => {
            let kept: Vec<Scalar> = predicates
                .iter()
                .filter_map(|p| {
                    let parsed = crate::scalar::parse(&p.text);
                    let folded = crate::scalar::fold(parsed.clone());
                    if folded.is_true() {
                        None // `q AND true` = `q`: drop the redundant conjunct
                    } else if folded == parsed {
                        Some(p.clone())
                    } else {
                        Some(to_scalar(&folded))
                    }
                })
                .collect();
            Rel::Filter {
                input,
                predicates: kept,
            }
        }
        Rel::Map { input, scalars } => Rel::Map {
            input,
            scalars: scalars.iter().map(fold_scalar).collect(),
        },
        Rel::Reduce {
            input,
            group_key,
            aggregates,
        } => Rel::Reduce {
            input,
            group_key: group_key.iter().map(fold_scalar).collect(),
            aggregates: aggregates.iter().map(fold_scalar).collect(),
        },
        Rel::Join {
            inputs,
            equivalences,
        } => Rel::Join {
            inputs,
            equivalences: fold_equivalences(equivalences),
        },
        Rel::WcoJoin {
            inputs,
            equivalences,
        } => Rel::WcoJoin {
            inputs,
            equivalences: fold_equivalences(equivalences),
        },
        other => other,
    }
}

/// Fold a single scalar, leaving it untouched unless folding changed it.
fn fold_scalar(s: &Scalar) -> Scalar {
    let parsed = crate::scalar::parse(&s.text);
    let folded = crate::scalar::fold(parsed.clone());
    if folded == parsed {
        s.clone()
    } else {
        to_scalar(&folded)
    }
}

fn fold_equivalences(classes: Vec<Vec<Scalar>>) -> Vec<Vec<Scalar>> {
    classes
        .iter()
        .map(|c| c.iter().map(fold_scalar).collect())
        .collect()
}

/// Render a folded [`crate::scalar::Expr`] back to an opaque [`Scalar`] payload.
fn to_scalar(e: &crate::scalar::Expr) -> Scalar {
    Scalar::new(crate::scalar::render(e), e.cols())
}

/// Push every single-input operator that sits directly above a binding scope
/// *into* that scope's body, to a fixpoint: `O(LetRec x = b in B)` becomes
/// `LetRec x = b in O(B)` (and likewise for `Let`). Sound for any unary
/// operator because it is a function of its one input and the bindings are
/// untouched. Multi-input operators (`Union`/`Join`) are left in place (pushing
/// them in would pull their other arguments into the scope).
fn normalize_push_into_scopes(rel: Rel) -> Rel {
    if !contains_scope(&rel) {
        return rel;
    }
    // Normalize children first.
    let children: Vec<Rel> = rel
        .children()
        .into_iter()
        .cloned()
        .map(normalize_push_into_scopes)
        .collect();
    let rel = rel.with_children(children);

    // A single-input operator (not itself a scope) directly over a scope: push.
    let unary_over_scope = rel.children().len() == 1
        && !matches!(rel, Rel::Let { .. } | Rel::LetRec { .. })
        && matches!(rel.children()[0], Rel::Let { .. } | Rel::LetRec { .. });
    if !unary_over_scope {
        return rel;
    }
    let scope = rel.children()[0].clone();
    match scope {
        Rel::LetRec { bindings, body } => {
            let new_body = normalize_push_into_scopes(rel.with_children(vec![*body]));
            Rel::LetRec {
                bindings,
                body: Box::new(new_body),
            }
        }
        Rel::Let { id, value, body } => {
            let new_body = normalize_push_into_scopes(rel.with_children(vec![*body]));
            Rel::Let {
                id,
                value,
                body: Box::new(new_body),
            }
        }
        _ => unreachable!("unary_over_scope guaranteed a scope child"),
    }
}

/// Replace every maximal `Let`/`LetRec` subtree of `rel` with a fresh opaque
/// `LocalGet` placeholder, collecting the `(id, subtree)` pairs in `out`.
fn hoist_scopes(rel: Rel, next_id: &mut usize, out: &mut Vec<(usize, Rel)>) -> Rel {
    if matches!(rel, Rel::Let { .. } | Rel::LetRec { .. }) {
        let id = *next_id;
        *next_id += 1;
        let arity = rel.arity();
        out.push((id, rel));
        return Rel::LocalGet { id, arity };
    }
    let children: Vec<Rel> = rel
        .children()
        .into_iter()
        .cloned()
        .map(|c| hoist_scopes(c, next_id, out))
        .collect();
    rel.with_children(children)
}

/// Splice scope subtrees back in for their placeholder `LocalGet`s.
fn substitute_locals(rel: Rel, subst: &BTreeMap<usize, Rel>) -> Rel {
    if let Rel::LocalGet { id, .. } = &rel {
        if let Some(r) = subst.get(id) {
            return r.clone();
        }
    }
    let children: Vec<Rel> = rel
        .children()
        .into_iter()
        .cloned()
        .map(|c| substitute_locals(c, subst))
        .collect();
    rel.with_children(children)
}

/// The largest local id (binding or `LocalGet`) anywhere in `rel`, for choosing
/// fresh, non-clashing placeholder ids.
fn max_local_id(rel: &Rel) -> usize {
    let here = match rel {
        Rel::LocalGet { id, .. } | Rel::Let { id, .. } => *id,
        Rel::LetRec { bindings, .. } => bindings.iter().map(|(id, _)| *id).max().unwrap_or(0),
        _ => 0,
    };
    rel.children()
        .iter()
        .map(|c| max_local_id(c))
        .fold(here, usize::max)
}

/// Whether `rel` is, or contains anywhere, a binding scope (`Let`/`LetRec`).
/// Such trees cannot be added to the e-graph wholesale; the structural
/// optimizer peels the scopes and saturates the Let-free fragments between.
fn contains_scope(rel: &Rel) -> bool {
    matches!(rel, Rel::Let { .. } | Rel::LetRec { .. })
        || rel.children().iter().any(|c| contains_scope(c))
}

/// A greedy steepest-descent optimizer (cost-monotone fixpoint). Kept to
/// illustrate the local-minima problem that saturation avoids.
#[derive(Clone, Debug)]
pub struct GreedyOptimizer {
    rules: RuleSet,
    model: CostModel,
    max_steps: usize,
}

impl GreedyOptimizer {
    pub fn new(rules: RuleSet, model: CostModel) -> Self {
        GreedyOptimizer {
            rules,
            model,
            max_steps: 1_000,
        }
    }

    pub fn optimize(&self, plan: Rel) -> Outcome {
        let initial_cost = self.model.cost(&plan);
        let mut current = plan;
        let mut current_cost = initial_cost.clone();
        let mut iterations = 0;

        for _ in 0..self.max_steps {
            iterations += 1;
            let mut best: Option<(Rel, Cost)> = None;
            for candidate in self.all_rewrites(&current) {
                let cost = self.model.cost(&candidate);
                if cost.lt(&current_cost) {
                    match &best {
                        Some((_, bc)) if !cost.lt(bc) => {}
                        _ => best = Some((candidate, cost)),
                    }
                }
            }
            match best {
                Some((candidate, cost)) => {
                    current = candidate;
                    current_cost = cost;
                }
                None => break,
            }
        }

        Outcome {
            plan: current,
            initial_cost,
            final_cost: current_cost,
            iterations,
        }
    }

    /// All single-step rewrites of `rel`: any rule applied at any one node.
    fn all_rewrites(&self, rel: &Rel) -> Vec<Rel> {
        let mut out = Vec::new();
        for rule in &self.rules.rules {
            if let Some(rewritten) = try_rule_at_root(rule, rel) {
                out.push(rewritten);
            }
        }
        let children: Vec<Rel> = rel.children().into_iter().cloned().collect();
        for (i, child) in children.iter().enumerate() {
            for new_child in self.all_rewrites(child) {
                let mut new_children = children.clone();
                new_children[i] = new_child;
                out.push(rel.with_children(new_children));
            }
        }
        out
    }
}

/// Apply `rule` at the root of `rel` (matcher-based), if it matches, its side
/// conditions hold, and the template instantiates cleanly.
fn try_rule_at_root(rule: &Rule, rel: &Rel) -> Option<Rel> {
    let mut b = Bindings::default();
    if !match_pat(&rule.lhs, rel, &mut b) {
        return None;
    }
    if !check_conds(&rule.conds, &b) {
        return None;
    }
    instantiate(&rule.rhs, &b, None).ok()
}
