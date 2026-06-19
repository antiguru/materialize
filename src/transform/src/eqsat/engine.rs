// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! The optimizer compiled from a rule set.
//!
//! [`Optimizer`] saturates an e-graph (exploring the transform graph in a
//! worst-case-optimal manner via [`crate::eqsat::egraph`]) and extracts the cheapest
//! plan, so it never gets stuck in a local minimum that a greedy
//! cost-monotone rewriter would. Conditions are evaluated by a single
//! evaluator, the e-class one in [`crate::eqsat::egraph`], over the saturated graph.

use std::collections::BTreeMap;

use crate::eqsat::analysis::{LocalFacts, letrec_local_facts};
use crate::eqsat::cost::{Cost, CostModel};
use crate::eqsat::dsl::RuleSet;
use crate::eqsat::egraph::EGraph;
use crate::eqsat::ir::Rel;

/// Bound on the outer fixpoint that re-analyzes a binding scope after rewriting
/// it (a rewrite can reveal a stronger recursive invariant that enables another
/// rewrite). Convergence is fast — the analyses are monotone and the rewrites
/// idempotent — so a small cap suffices.
const SCOPE_REFINE_ROUNDS: usize = 4;

/// A faster-but-heavier alternative plan: switching to it would improve
/// time (CPU work) at the cost of more memory (arranged collections).
#[derive(Clone, Debug)]
pub struct Recommendation {
    /// The time-optimal alternative plan.
    pub plan: Rel,
    /// Cost of the alternative plan.
    pub cost: Cost,
}

/// The outcome of optimizing a plan.
#[derive(Clone, Debug)]
pub struct Outcome {
    pub plan: Rel,
    pub initial_cost: Cost,
    pub final_cost: Cost,
    /// Number of saturation (or descent) iterations performed.
    pub iterations: usize,
    /// A faster-but-heavier alternative, if the time-optimal plan differs
    /// from the memory-optimal default and offers a strict time improvement at
    /// the cost of more memory.
    ///
    /// `None` when both orderings agree, or when there is no strict time
    /// benefit.
    ///
    /// Note: recommendation is computed only for the top-level Let-free
    /// fragment; scoped/recursive fragments are not covered here.
    pub recommendation: Option<Recommendation>,
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
    /// represent.  So they are handled **structurally**: the optimizer treats
    /// each scope as a boundary, recursively optimizing every maximal Let-free
    /// fragment (binding values and bodies) by equality saturation, then
    /// reassembling the scope.  Recursive references are opaque `LocalGet`
    /// leaves within a fragment.  Analyses still flow *through* the recursion
    /// via the recursion-aware fixpoint in [`crate::eqsat::analysis`].
    ///
    /// After saturation the memory-first plan (default) is returned.  A
    /// time-first alternative is also extracted; if it is strictly faster but
    /// uses more memory it is reported in [`Outcome::recommendation`].
    pub fn optimize(&self, plan: Rel) -> Outcome {
        let initial_cost = self.model.cost(&plan);
        // Scalars are opaque MirScalarExpr in-tree; scalar folding is left to
        // the existing FoldConstants transform.
        let (best, time_alt, iterations) =
            self.optimize_node_with_alt(plan, &LocalFacts::default());
        let final_cost = self.model.cost(&best);

        // Compute a recommendation if the time-first alternative is strictly
        // faster on time but uses more memory than the chosen plan.
        let recommendation = time_alt.and_then(|alt| {
            if alt == best {
                return None;
            }
            let alt_cost = self.model.cost(&alt);
            // Strictly faster on time?
            let faster = alt_cost.cmp_time_first(&final_cost) == std::cmp::Ordering::Less;
            // Uses strictly more memory?
            let heavier = alt_cost.cmp_memory_first(&final_cost) == std::cmp::Ordering::Greater;
            if faster && heavier {
                Some(Recommendation {
                    plan: alt,
                    cost: alt_cost,
                })
            } else {
                None
            }
        });

        Outcome {
            plan: best,
            initial_cost,
            final_cost,
            iterations,
            recommendation,
        }
    }

    /// Like [`optimize_node`] but also returns the time-first alternative
    /// extracted from the same saturated e-graph (for Let-free fragments only;
    /// scoped fragments return `None` for the alternative).
    ///
    /// The triple is `(memory_first_plan, time_first_plan, iterations)`.
    fn optimize_node_with_alt(&self, plan: Rel, facts: &LocalFacts) -> (Rel, Option<Rel>, usize) {
        let plan = normalize_push_into_scopes(plan);
        match &plan {
            Rel::Let { .. } | Rel::LetRec { .. } => {
                let (p, i) = self.optimize_scope(plan, facts);
                (p, None, i)
            }
            _ if contains_scope(&plan) => {
                let (p, i) = self.optimize_around_scopes(plan, facts);
                (p, None, i)
            }
            _ => {
                let mut eg = EGraph::new();
                let root = eg.add_rel(&plan);
                let iterations = eg.saturate(&self.rules, self.max_iters, facts);
                let mem_plan = eg.extract_with(root, &self.model, true);
                let time_plan = eg.extract_with(root, &self.model, false);
                (mem_plan, Some(time_plan), iterations)
            }
        }
    }

    /// Optimize one node under the recursion facts `facts` (the proven
    /// non-negativity / monotonicity / keys of every in-scope `LocalGet`).
    /// Saturate it if it is a Let-free fragment; otherwise walk past the binding
    /// scope.  Returns the optimized plan and the saturation iterations spent.
    fn optimize_node(&self, plan: Rel, facts: &LocalFacts) -> (Rel, usize) {
        // Normalize first: push any single-input operator sitting directly above
        // a binding scope *into* the scope's body. `O(LetRec x = b in B)` denotes
        // `O(B[x*])`, and so does `LetRec x = b in O(B)` — the bindings (hence
        // the fixpoint `x*`) are untouched — so this is unconditionally sound. It
        // moves the operator inside the scope, where the recursion facts can act
        // on it.  (It does *not* push into the recursive bindings; see
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
            ext.nonneg
                .insert(id, crate::eqsat::analysis::rel_non_negative(&opt));
            ext.monotonic
                .insert(id, crate::eqsat::analysis::rel_monotonic(&opt));
            ext.keys.insert(id, crate::eqsat::analysis::rel_keys(&opt));
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
        return Rel::LocalGet {
            id,
            arity,
            get: None,
        };
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
