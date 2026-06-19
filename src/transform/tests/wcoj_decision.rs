// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Experiment: does the e-graph's cardinality-free AGM cost model make a
//! different worst-case-optimal-join decision than Materialize's real
//! `JoinImplementation` transform?
//!
//! The triangle join R(a,b) ⋈ S(b,c) ⋈ T(c,a) is the canonical example where
//! worst-case-optimal joins (AGM bound N^1.5) beat binary joins (N^2
//! intermediate). This test checks:
//!
//! * E-graph: lowers the triangle, saturates, extracts — reports whether the
//!   extracted plan is `Rel::WcoJoin` (AGM) or `Rel::Join` (binary).
//! * Materialize: runs `JoinImplementation` on the same triangle and reports
//!   whether it chose `Differential`, `DeltaQuery`, or left it `Unimplemented`.
//!   Both `enable_eager_delta_joins = false` and `= true` are tested.
//!
//! A clean result (e-graph picks WcoJoin; Materialize picks Differential on a
//! bare join with no arrangements) is the valuable outcome.

use mz_expr::{AccessStrategy, Id, JoinImplementation, MirRelationExpr, MirScalarExpr};
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::{GlobalId, ReprRelationType, ReprScalarType};
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::eqsat::cost::CostModel;
use mz_transform::eqsat::default_ruleset;
use mz_transform::eqsat::engine::Optimizer;
use mz_transform::eqsat::ir::Rel;
use mz_transform::eqsat::lower::lower;
use mz_transform::join_implementation::JoinImplementation as JoinImplementationTransform;
use mz_transform::{Optimizer as PipelineOptimizer, Transform, TransformCtx, typecheck};

/// Build a source relation with `arity` Int64 non-nullable columns and a
/// unique transient global id `id`.
///
/// Uses a global `Get` so `FoldConstants` cannot collapse it. Both eqsat and
/// the real pipeline treat an unknown `Get` as an opaque leaf of degree 1.
fn src(id: u64, arity: usize) -> MirRelationExpr {
    MirRelationExpr::Get {
        id: Id::Global(GlobalId::Transient(id)),
        typ: ReprRelationType::new(
            (0..arity)
                .map(|_| ReprScalarType::Int64.nullable(false))
                .collect(),
        ),
        access_strategy: AccessStrategy::UnknownOrLocal,
    }
}

/// Build the triangle join: R(a,b) ⋈ S(b,c) ⋈ T(c,a).
///
/// Column layout: R=#0,#1  S=#2,#3  T=#4,#5
/// Equivalences:  a: #0=#4,  b: #1=#2,  c: #3=#5
fn triangle() -> MirRelationExpr {
    let r = src(1, 2); // cols 0..1
    let s = src(2, 2); // cols 2..3
    let t = src(3, 2); // cols 4..5
    MirRelationExpr::join_scalars(
        vec![r, s, t],
        vec![
            vec![MirScalarExpr::column(0), MirScalarExpr::column(4)], // a = a
            vec![MirScalarExpr::column(1), MirScalarExpr::column(2)], // b = b
            vec![MirScalarExpr::column(3), MirScalarExpr::column(5)], // c = c
        ],
    )
}

/// Run `JoinImplementation` on a triangle and return the implementation variant
/// chosen for the top-level join. Returns `None` if the plan is not a join.
fn join_impl_choice(eager_delta: bool) -> Option<JoinImplementation> {
    let mut plan = triangle();
    let mut features = OptimizerFeatures::default();
    features.enable_eager_delta_joins = eager_delta;
    let tc = typecheck::empty_typechecking_context();
    let mut df = DataflowMetainfo::default();
    let mut ctx = TransformCtx::local(&features, &tc, &mut df, None, Some(GlobalId::Transient(99)));
    // Ignore errors: a bare triangle of unknown Gets may trigger soft-asserts
    // inside the transform, but the resulting implementation is still valid to
    // inspect.
    let _ = JoinImplementationTransform::default().transform(&mut plan, &mut ctx);
    match plan {
        MirRelationExpr::Join { implementation, .. } => Some(implementation),
        _ => None,
    }
}

/// Run the real `JoinImplementation` transform on `plan` and return it. Errors
/// are ignored (a bare triangle of unknown Gets can trip soft-asserts), as in
/// [`join_impl_choice`].
fn run_join_implementation(mut plan: MirRelationExpr) -> MirRelationExpr {
    let features = OptimizerFeatures::default();
    let tc = typecheck::empty_typechecking_context();
    let mut df = DataflowMetainfo::default();
    let mut ctx = TransformCtx::local(&features, &tc, &mut df, None, Some(GlobalId::Transient(98)));
    let _ = JoinImplementationTransform::default().transform(&mut plan, &mut ctx);
    plan
}

/// The implementation of the first `Join` found in `expr`, if any.
fn first_join_impl(expr: &MirRelationExpr) -> Option<JoinImplementation> {
    let mut found = None;
    expr.visit_pre(|e| {
        if found.is_none() {
            if let MirRelationExpr::Join { implementation, .. } = e {
                found = Some(implementation.clone());
            }
        }
    });
    found
}

#[mz_ore::test]
fn triangle_raises_to_delta_query() {
    // The e-graph picks WcoJoin for the triangle; raise commits that decision by
    // tagging the join DeltaQuery (synthesized by the real delta planner).
    let out = mz_transform::eqsat::optimize(triangle());
    assert_eq!(out.arity(), 6, "arity preserved");
    assert!(
        matches!(
            first_join_impl(&out),
            Some(JoinImplementation::DeltaQuery(_))
        ),
        "triangle must raise to a DeltaQuery-tagged join, got {out:?}"
    );
}

/// Run the full logical optimizer on the triangle with the eqsat flag set to
/// `enable_eqsat`, returning the optimized plan.
fn logical_optimize_triangle(enable_eqsat: bool) -> MirRelationExpr {
    let mut features = OptimizerFeatures::default();
    features.enable_eqsat_optimizer = enable_eqsat;
    let tc = typecheck::empty_typechecking_context();
    let mut df = DataflowMetainfo::default();
    let mut ctx = TransformCtx::local(&features, &tc, &mut df, None, Some(GlobalId::Transient(97)));
    #[allow(deprecated)]
    let optimizer = PipelineOptimizer::logical_optimizer(&mut ctx);
    optimizer
        .optimize(triangle(), &mut ctx)
        .expect("logical optimize")
        .into_inner()
}

#[mz_ore::test]
fn eqsat_flag_gates_delta_query_in_logical_optimizer() {
    // Flag on: the logical optimizer runs EqSatTransform, which commits the
    // triangle to a delta join. Flag off: the logical optimizer leaves the join
    // unimplemented (implementation is chosen in the physical optimizer), so it
    // is not a DeltaQuery. This proves the flag actually gates the pass.
    let on = logical_optimize_triangle(true);
    assert!(
        matches!(
            first_join_impl(&on),
            Some(JoinImplementation::DeltaQuery(_))
        ),
        "flag on: expected a DeltaQuery join, got {on:?}"
    );
    let off = logical_optimize_triangle(false);
    assert!(
        !matches!(
            first_join_impl(&off),
            Some(JoinImplementation::DeltaQuery(_))
        ),
        "flag off: must not be a DeltaQuery join, got {off:?}"
    );
}

#[mz_ore::test]
fn delta_query_survives_join_implementation() {
    // The payoff: a DeltaQuery-tagged join is not re-planned by the real
    // JoinImplementation transform (it only (re)plans Unimplemented and
    // Differential joins), so the e-graph's worst-case-optimal decision survives
    // the downstream pipeline rather than being replaced by Differential.
    let out = mz_transform::eqsat::optimize(triangle());
    let after = run_join_implementation(out);
    assert!(
        matches!(
            first_join_impl(&after),
            Some(JoinImplementation::DeltaQuery(_))
        ),
        "DeltaQuery must survive JoinImplementation, got {after:?}"
    );
}

#[mz_ore::test]
fn egraph_picks_wcoj_for_triangle() {
    // Lower the triangle, saturate, extract and inspect the Rel directly.
    let tri = triangle();
    let rel = lower(&tri);
    let model = CostModel::new();

    let outcome = Optimizer::new(default_ruleset(), model.clone()).optimize(rel);
    let best = &outcome.plan;

    // Classify the top-level node.
    let picked_wcoj = matches!(best, Rel::WcoJoin { .. });
    let picked_join = matches!(best, Rel::Join { .. });
    let best_cost = model.cost(best);

    println!();
    println!("=== E-GRAPH DECISION ===");
    println!(
        "Top-level node: {}",
        if picked_wcoj {
            "WcoJoin (AGM)"
        } else if picked_join {
            "Join (binary)"
        } else {
            "other"
        }
    );
    println!(
        "Cost time: {:?}  memory: {:?}  nodes: {}",
        best_cost.time, best_cost.memory, best_cost.nodes
    );
    println!();

    // === Materialize JoinImplementation ===
    let choice_eager_off = join_impl_choice(false);
    let choice_eager_on = join_impl_choice(true);

    let impl_name = |c: &Option<JoinImplementation>| match c {
        Some(JoinImplementation::Differential(..)) => "Differential".to_owned(),
        Some(JoinImplementation::DeltaQuery(..)) => "DeltaQuery".to_owned(),
        Some(JoinImplementation::Unimplemented) => "Unimplemented".to_owned(),
        Some(JoinImplementation::IndexedFilter(..)) => "IndexedFilter".to_owned(),
        None => "not a join".to_owned(),
    };

    println!("=== MATERIALIZE JoinImplementation DECISION ===");
    println!("eager_delta_joins=false: {}", impl_name(&choice_eager_off));
    println!("eager_delta_joins=true:  {}", impl_name(&choice_eager_on));
    println!();

    // === Verdict ===
    println!("=== VERDICT ===");
    let egraph_label = if picked_wcoj {
        "WcoJoin (AGM N^1.5)"
    } else {
        "Join (binary)"
    };
    let mz_label_off = impl_name(&choice_eager_off);
    let mz_label_on = impl_name(&choice_eager_on);
    let decisions_differ = picked_wcoj
        && matches!(
            &choice_eager_off,
            Some(JoinImplementation::Differential(..))
        );

    println!("E-graph decision:           {egraph_label}");
    println!("Materialize (eager off):    {mz_label_off}");
    println!("Materialize (eager on):     {mz_label_on}");
    println!("Decisions differ:           {}", decisions_differ);
    if decisions_differ {
        println!("WHY: The e-graph's AGM cost model picks WcoJoin because the triangle");
        println!("     join is cyclic and N^1.5 < N^2. Materialize's JoinImplementation");
        println!("     has no AGM awareness; on a bare join with no available arrangements,");
        println!("     it falls back to Differential regardless of join shape.");
    } else {
        println!("WHY: Both optimizers converge — either egraph did not pick WcoJoin,");
        println!("     or Materialize picked a delta join that aligns with AGM reasoning.");
    }
    println!();

    // Hard assertion: the e-graph must pick WcoJoin (this is the AGM invariant).
    // The dominant cost degree must be 1.5 (the triangle AGM bound), not 2.0.
    assert!(
        picked_wcoj,
        "e-graph must extract WcoJoin for the triangle join; got {best:?}"
    );
    assert!(
        best_cost.time.first().copied().unwrap_or(0.0) < 1.6,
        "WcoJoin dominant time degree must be ~1.5 (AGM); got time={:?}",
        best_cost.time
    );
}
