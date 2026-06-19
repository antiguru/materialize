// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Extraction-time common-subexpression elimination.
//!
//! The e-graph already *subsumes* CSE: equal subterms are hash-consed into a
//! single e-class, so sharing is implicit while the optimizer runs. Extracting
//! a plan back to a tree (see [`crate::egraph::EGraph::extract`]) loses that
//! sharing; this pass re-introduces it, binding every subexpression that occurs
//! more than once with a [`Rel::Let`] and replacing its occurrences with
//! [`Rel::LocalGet`]. This is the relational analogue of `transform/src/cse`
//! (ANF + `NormalizeLets`), reduced to its essence: binding shared structure.
//!
//! `LetRec` (mutually-recursive bindings, the substrate for Materialize's
//! `WITH MUTUALLY RECURSIVE`) is *not* modeled here — equality saturation over
//! a finite e-graph does not represent fixpoints, so recursive bindings would
//! need first-class support in the IR, cost model, and Lean semantics. See
//! `COVERAGE.md`.

use std::collections::BTreeMap;

use crate::ir::Rel;

/// Bind every subexpression that occurs more than once in `rel` with a `Let`,
/// turning a tree back into a DAG-with-sharing.
pub fn eliminate_common_subexpressions(rel: &Rel) -> Rel {
    // 1. Count occurrences of every distinct subtree.
    let mut counts: BTreeMap<Rel, usize> = BTreeMap::new();
    count(rel, &mut counts);

    // 2. Pick the shared, non-trivial subtrees. Order by size ascending so a
    //    shared subtree is bound *after* (inside) any larger one is impossible;
    //    rather, smaller ones are bound first (outermost) and are therefore in
    //    scope for the larger values that reference them.
    let mut shared: Vec<Rel> = counts
        .into_iter()
        .filter(|(r, n)| *n >= 2 && worth_binding(r))
        .map(|(r, _)| r)
        .collect();
    shared.sort_by(|a, b| a.node_count().cmp(&b.node_count()).then_with(|| a.cmp(b)));
    if shared.is_empty() {
        return rel.clone();
    }

    // 3. Assign a local id to each shared subtree.
    let ids: BTreeMap<Rel, usize> = shared
        .iter()
        .enumerate()
        .map(|(i, r)| (r.clone(), i))
        .collect();

    // 4. The body, with every shared subtree replaced by its `LocalGet`.
    let mut result = subst(rel, &ids);

    // 5. Wrap in `Let` bindings, largest (innermost) first so the fold leaves
    //    the smallest binding outermost — keeping every value's references in
    //    scope.
    for r in shared.iter().rev() {
        let id = ids[r];
        let value = Box::new(subst_children(r, &ids));
        result = Rel::Let {
            id,
            value,
            body: Box::new(result),
        };
    }
    result
}

/// A subtree is worth binding if it is compound (sharing a `Get`/`Constant`/
/// `LocalGet` saves nothing).
fn worth_binding(rel: &Rel) -> bool {
    !matches!(
        rel,
        Rel::Get { .. } | Rel::Constant { .. } | Rel::LocalGet { .. }
    )
}

fn count(rel: &Rel, counts: &mut BTreeMap<Rel, usize>) {
    *counts.entry(rel.clone()).or_insert(0) += 1;
    for c in rel.children() {
        count(c, counts);
    }
}

/// Replace `rel` itself with a `LocalGet` if it is shared, else recurse.
fn subst(rel: &Rel, ids: &BTreeMap<Rel, usize>) -> Rel {
    if let Some(&id) = ids.get(rel) {
        return Rel::LocalGet {
            id,
            arity: rel.arity(),
        };
    }
    subst_children(rel, ids)
}

/// Substitute within `rel`'s children, keeping `rel`'s own operator.
fn subst_children(rel: &Rel, ids: &BTreeMap<Rel, usize>) -> Rel {
    let new: Vec<Rel> = rel.children().into_iter().map(|c| subst(c, ids)).collect();
    rel.with_children(new)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::Scalar;

    fn get(name: &str, arity: usize) -> Rel {
        Rel::Get {
            name: name.into(),
            arity,
        }
    }

    #[test]
    fn shares_a_repeated_subexpression() {
        // A self-join of `Filter(R)`: the filtered relation appears twice.
        let filtered = Rel::Filter {
            predicates: vec![Scalar::new("#0", [0usize])],
            input: Box::new(get("R", 2)),
        };
        let plan = Rel::Join {
            inputs: vec![filtered.clone(), filtered.clone()],
            equivalences: vec![],
        };
        let out = eliminate_common_subexpressions(&plan);

        // The result is a single Let binding the shared Filter, with two
        // LocalGets in the join.
        match &out {
            Rel::Let { id, value, body } => {
                assert_eq!(**value, filtered);
                match &**body {
                    Rel::Join { inputs, .. } => {
                        for i in inputs {
                            assert!(matches!(i, Rel::LocalGet { id: gid, .. } if gid == id));
                        }
                    }
                    other => panic!("expected Join body, got {other}"),
                }
            }
            other => panic!("expected a Let, got {other}"),
        }
        // Arity is preserved.
        assert_eq!(out.arity(), plan.arity());
    }

    #[test]
    fn leaves_unshared_plans_alone() {
        let plan = Rel::Join {
            inputs: vec![get("R", 2), get("S", 2)],
            equivalences: vec![],
        };
        assert_eq!(eliminate_common_subexpressions(&plan), plan);
    }
}
