// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Extraction-time common-subexpression elimination.
//!
//! The e-graph already *subsumes* CSE: equal subterms are hash-consed into a
//! single e-class, so sharing is implicit while the optimizer runs. Extracting
//! a plan back to a tree (see [`crate::eqsat::egraph::EGraph::extract`]) loses that
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

use mz_expr::{Id, MirRelationExpr};
use mz_ore::cast::CastFrom;

use crate::eqsat::ir::Rel;

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

    // 3. Assign a local id to each shared subtree. Ids start above the maximum
    //    id already present in the tree so CSE-introduced ids never clash with
    //    lowered Let/LocalGet ids (real LocalId numbers).
    let max_existing = max_local_id(rel);
    let id_base = max_existing.saturating_add(1);
    let ids: BTreeMap<Rel, usize> = shared
        .iter()
        .enumerate()
        .map(|(i, r)| (r.clone(), id_base + i))
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
/// `Opaque`/`LocalGet` saves nothing) AND closed (contains no references to
/// lower'd local ids via `LocalGet { get: Some }` or nested `Let` bindings).
/// Open subtrees reference locals from an outer scope and cannot be hoisted
/// outside that scope without breaking the scoping invariant.
fn worth_binding(rel: &Rel) -> bool {
    !matches!(
        rel,
        Rel::Get { .. } | Rel::Constant { .. } | Rel::Opaque(_) | Rel::LocalGet { .. }
    ) && is_closed(rel)
}

/// Returns true iff `rel` contains no `LocalGet { get: Some }` references and
/// no `Rel::Let` nodes. Such subtrees depend on an outer Let scope and cannot
/// be hoisted above it.
fn is_closed(rel: &Rel) -> bool {
    match rel {
        Rel::LocalGet { get: Some(_), .. } => false,
        Rel::LocalGet { get: None, .. } => true,
        Rel::Let { .. } => false,
        _ => rel.children().iter().all(|c| is_closed(c)),
    }
}

/// Walk `rel` and return the maximum `LocalId` reachable anywhere in the plan,
/// or 0 if none are present. Used to pick a fresh id base for CSE bindings.
///
/// This must account for *every* `LocalId` in scope, including those buried in
/// the verbatim `MirRelationExpr` of a [`Rel::Opaque`] leaf. `lower` bails
/// unsupported nodes (notably `LetRec`) into an opaque leaf carrying the
/// original MIR, which can contain `Let`/`LetRec` binding ids and
/// `Get { Id::Local }` references. Those leaves have no `Rel` children, so a
/// plain `Rel`-only walk never sees their ids. If a CSE-introduced id were to
/// collide with one of them it would shadow an existing binding, and a later
/// `Demand`/`NormalizeLets` pass that asserts no shadowing (see
/// `transform/src/demand.rs`) would panic on recursive CTEs.
fn max_local_id(rel: &Rel) -> usize {
    fn walk(rel: &Rel, max: &mut usize) {
        match rel {
            Rel::Let { id, value, body } => {
                if *id > *max {
                    *max = *id;
                }
                walk(value, max);
                walk(body, max);
            }
            Rel::LocalGet { id, .. } => {
                if *id > *max {
                    *max = *id;
                }
            }
            Rel::Opaque(mir) => {
                let m = max_mir_local_id(mir);
                if m > *max {
                    *max = m;
                }
            }
            _ => {
                for c in rel.children() {
                    walk(c, max);
                }
            }
        }
    }
    let mut max = 0;
    walk(rel, &mut max);
    max
}

/// The maximum `LocalId` (as `usize`) appearing anywhere in `mir`, or 0 if none.
///
/// Covers both binding sites (`Let`/`LetRec` ids) and references
/// (`Get { Id::Local }`); the two coincide in well-formed MIR, but we take the
/// max over both so an unreferenced binding still counts.
pub(crate) fn max_mir_local_id(mir: &MirRelationExpr) -> usize {
    let mut max = 0;
    mir.visit_pre(|e| {
        let mut note = |local: &mz_expr::LocalId| {
            let id = usize::cast_from(u64::from(local));
            if id > max {
                max = id;
            }
        };
        match e {
            MirRelationExpr::Get {
                id: Id::Local(local),
                ..
            } => note(local),
            MirRelationExpr::Let { id, .. } => note(id),
            MirRelationExpr::LetRec { ids, .. } => {
                for id in ids {
                    note(id);
                }
            }
            _ => {}
        }
    });
    max
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
            get: None,
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
    use crate::eqsat::ir::EScalar;
    use mz_expr::MirScalarExpr;

    fn get(name: &str, arity: usize) -> Rel {
        Rel::Get {
            name: name.into(),
            arity,
        }
    }

    #[mz_ore::test]
    fn shares_a_repeated_subexpression() {
        // A self-join of `Filter(R)`: the filtered relation appears twice.
        let filtered = Rel::Filter {
            predicates: vec![EScalar::plain(MirScalarExpr::column(0))],
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

    #[mz_ore::test]
    fn leaves_unshared_plans_alone() {
        let plan = Rel::Join {
            inputs: vec![get("R", 2), get("S", 2)],
            equivalences: vec![],
        };
        assert_eq!(eliminate_common_subexpressions(&plan), plan);
    }

    /// Build an opaque leaf carrying a `LetRec` that binds (and references)
    /// `local_id`. This is exactly what `lower` produces for an unsupported
    /// `WITH MUTUALLY RECURSIVE` subtree.
    fn opaque_letrec(local_id: u64, arity: usize) -> Rel {
        use mz_expr::{Id, LocalId, MirRelationExpr};
        use mz_repr::{ReprRelationType, ReprScalarType};

        let typ = ReprRelationType::new(
            (0..arity)
                .map(|_| ReprScalarType::Int64.nullable(false))
                .collect(),
        );
        let lid = LocalId::new(local_id);
        // LetRec x = Get(x) in Get(x): a self-reference closing the cycle. The
        // body and value both Get the bound LocalId, so the id appears as a
        // binding site and as a reference.
        let mir = MirRelationExpr::LetRec {
            ids: vec![lid.clone()],
            values: vec![MirRelationExpr::Get {
                id: Id::Local(lid.clone()),
                typ: typ.clone(),
                access_strategy: mz_expr::AccessStrategy::UnknownOrLocal,
            }],
            limits: vec![None],
            body: Box::new(MirRelationExpr::Get {
                id: Id::Local(lid),
                typ,
                access_strategy: mz_expr::AccessStrategy::UnknownOrLocal,
            }),
        };
        Rel::Opaque(Box::new(mir))
    }

    /// Collect every `LocalId` (as usize) reachable in `rel`, including those
    /// buried in opaque MIR leaves. Mirrors what a real shadowing check sees.
    fn all_local_ids(rel: &Rel) -> std::collections::BTreeSet<usize> {
        let mut out = std::collections::BTreeSet::new();
        fn walk(rel: &Rel, out: &mut std::collections::BTreeSet<usize>) {
            match rel {
                Rel::Let { id, .. } => {
                    out.insert(*id);
                }
                Rel::LocalGet { id, .. } => {
                    out.insert(*id);
                }
                Rel::Opaque(mir) => {
                    out.insert(max_mir_local_id(mir));
                }
                _ => {}
            }
            for c in rel.children() {
                walk(c, out);
            }
        }
        walk(rel, &mut out);
        out
    }

    /// Regression: a CSE-introduced `Let` id must not collide with a `LocalId`
    /// hidden inside a `Rel::Opaque` leaf (e.g. a bailed `LetRec`). Before the
    /// fix, `max_local_id` ignored opaque leaves, so the fresh-id base could
    /// equal a binding id inside the opaque MIR, shadowing it. Downstream the
    /// production `Demand` pass asserts no shadowing and panics on recursive
    /// CTEs.
    #[mz_ore::test]
    fn cse_id_does_not_collide_with_opaque_letrec_local() {
        // Pick the opaque LocalId to be exactly what the OLD allocation would
        // hand out as the first "fresh" id. The only non-opaque ids here are
        // none, so the old `max_local_id` returns 0 and id_base would be 1.
        let opaque = opaque_letrec(1, 2);

        // A CSE opportunity: a compound subterm referenced twice, forcing a
        // fresh-id allocation. The opaque leaf rides along in the body.
        let filtered = Rel::Filter {
            predicates: vec![EScalar::plain(MirScalarExpr::column(0))],
            input: Box::new(get("R", 2)),
        };
        let plan = Rel::Union {
            base: Box::new(Rel::Join {
                inputs: vec![filtered.clone(), filtered.clone()],
                equivalences: vec![],
            }),
            inputs: vec![opaque],
        };

        let out = eliminate_common_subexpressions(&plan);

        // CSE must have fired (a Let exists somewhere).
        fn has_let(rel: &Rel) -> bool {
            matches!(rel, Rel::Let { .. }) || rel.children().iter().any(|c| has_let(c))
        }
        assert!(has_let(&out), "expected CSE to introduce a Let: {out}");

        // No CSE-introduced Let id may equal a LocalId inside the opaque leaf.
        // Collect the new Let ids and assert they are all strictly above the
        // opaque's LocalId (1).
        fn let_ids(rel: &Rel, out: &mut Vec<usize>) {
            if let Rel::Let { id, .. } = rel {
                out.push(*id);
            }
            for c in rel.children() {
                let_ids(c, out);
            }
        }
        let mut ids = Vec::new();
        let_ids(&out, &mut ids);
        assert!(!ids.is_empty());
        for id in &ids {
            assert!(
                *id > 1,
                "CSE Let id {id} collides with opaque LocalId 1: {out}"
            );
        }

        // Arity is preserved and the opaque LocalId is still distinct from
        // every Let id (no shadowing).
        assert_eq!(out.arity(), plan.arity());
        let locals = all_local_ids(&out);
        assert!(locals.contains(&1), "opaque LocalId should survive");
        for id in &ids {
            assert!(locals.contains(id));
        }
    }
}
