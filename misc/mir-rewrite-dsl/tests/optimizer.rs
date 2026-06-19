// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! End-to-end tests for the rewrite DSL: parse the built-in rules, saturate,
//! extract, and check both the cost outcomes and the worst-case-optimal-join
//! behaviour.

use mz_mir_rewrite_dsl::cost::CostModel;
use mz_mir_rewrite_dsl::default_ruleset;
use mz_mir_rewrite_dsl::engine::{GreedyOptimizer, Optimizer};
use mz_mir_rewrite_dsl::ir::{Rel, Scalar};

fn get(name: &str, arity: usize) -> Rel {
    Rel::Get {
        name: name.into(),
        arity,
    }
}

fn col(c: usize) -> Scalar {
    Scalar::new(format!("#{c}"), [c])
}

fn triangle() -> Rel {
    let eq = |a: usize, b: usize| vec![col(a), col(b)];
    Rel::Join {
        inputs: vec![get("R", 2), get("S", 2), get("T", 2)],
        equivalences: vec![eq(0, 4), eq(1, 2), eq(3, 5)],
    }
}

fn filtered_union() -> Rel {
    Rel::Filter {
        predicates: vec![col(0)],
        input: Box::new(Rel::Union {
            base: Box::new(Rel::Filter {
                predicates: vec![col(1)],
                input: Box::new(get("R", 2)),
            }),
            inputs: vec![Rel::Filter {
                predicates: vec![col(1)],
                input: Box::new(get("S", 2)),
            }],
        }),
    }
}

fn optimizer() -> Optimizer {
    Optimizer::new(default_ruleset(), CostModel::new())
}

#[test]
fn builtin_rules_parse() {
    let rs = default_ruleset();
    assert_eq!(rs.rules.len(), 25);
}

#[test]
fn triangle_becomes_worst_case_optimal_join() {
    let out = optimizer().optimize(triangle());
    assert!(
        matches!(out.plan, Rel::WcoJoin { .. }),
        "expected a WcoJoin, got:\n{}",
        out.plan
    );
    // The dominant term dropped from N^2 to N^1.5.
    assert!(out.final_cost.lt(&out.initial_cost));
    assert!((out.final_cost.degrees[0] - 1.5).abs() < 1e-6);
}

#[test]
fn saturation_escapes_a_local_minimum() {
    let model = CostModel::new();
    let sat = Optimizer::new(default_ruleset(), model.clone());
    let greedy = GreedyOptimizer::new(default_ruleset(), model);

    let s = sat.optimize(filtered_union());
    let g = greedy.optimize(filtered_union());

    // Saturation distributes the filter (a cost-increasing step greedy refuses)
    // and then merges, ending strictly cheaper than greedy's stuck plan.
    assert!(
        s.final_cost.lt(&g.final_cost),
        "saturating={:?} greedy={:?}",
        s.final_cost,
        g.final_cost
    );
}

#[test]
fn saturation_is_never_worse_than_greedy() {
    let model = CostModel::new();
    let sat = Optimizer::new(default_ruleset(), model.clone());
    let greedy = GreedyOptimizer::new(default_ruleset(), model.clone());

    for plan in [triangle(), filtered_union()] {
        let s = sat.optimize(plan.clone());
        let g = greedy.optimize(plan);
        // saturating cost <= greedy cost
        assert!(
            !g.final_cost.lt(&s.final_cost),
            "greedy beat saturation: sat={:?} greedy={:?}",
            s.final_cost,
            g.final_cost
        );
    }
}

#[test]
fn optimization_preserves_arity() {
    let opt = optimizer();
    for plan in [triangle(), filtered_union()] {
        let before = plan.arity();
        let out = opt.optimize(plan);
        assert_eq!(before, out.plan.arity());
    }
}

#[test]
fn flattens_nested_first_join() {
    // join(e1, join(e2, R, S), T) should flatten to a single 3-way join, which
    // is then eligible for the WCOJ conversion on the (cyclic) triangle.
    let eq = |a: usize, b: usize| vec![col(a), col(b)];
    let inner = Rel::Join {
        inputs: vec![get("R", 2), get("S", 2)],
        equivalences: vec![eq(1, 2)], // b: R.#1 = S.#0(=#2)
    };
    let plan = Rel::Join {
        inputs: vec![inner, get("T", 2)],
        // columns: R=#0,#1 S=#2,#3 T=#4,#5 ; a:#0=#4 c:#3=#5
        equivalences: vec![eq(0, 4), eq(3, 5)],
    };
    let out = optimizer().optimize(plan);
    // The cheapest equivalent is the flattened worst-case-optimal triangle.
    match &out.plan {
        Rel::WcoJoin { inputs, .. } => assert_eq!(inputs.len(), 3),
        other => panic!("expected a flattened 3-way WcoJoin, got:\n{other}"),
    }
}

#[test]
fn distributes_filter_over_nary_union() {
    // filter(p, R + S + T) with a 3-arm union exercises the list-map combinator.
    let plan = Rel::Filter {
        predicates: vec![col(0)],
        input: Box::new(Rel::Union {
            base: Box::new(get("R", 2)),
            inputs: vec![get("S", 2), get("T", 2)],
        }),
    };
    let out = optimizer().optimize(plan.clone());
    // Equivalent and same arity; the optimizer explored the distributed form.
    assert_eq!(out.plan.arity(), plan.arity());
}

#[test]
fn column_arithmetic_rules_fire_without_breaking() {
    // A filter over the 2nd join input (cols #2,#3) and a filter over a
    // projection — exercises shift/remap during saturation. We assert the
    // optimizer terminates, preserves arity, and is idempotent.
    let push_into_join = Rel::Filter {
        predicates: vec![Scalar::new("#2", [2usize])],
        input: Box::new(Rel::Join {
            inputs: vec![get("R", 2), get("S", 2)],
            equivalences: vec![],
        }),
    };
    let push_past_project = Rel::Filter {
        predicates: vec![Scalar::new("#0", [0usize])],
        input: Box::new(Rel::Project {
            input: Box::new(get("R", 3)),
            outputs: vec![2, 0],
        }),
    };
    let opt = optimizer();
    for plan in [push_into_join, push_past_project] {
        let out = opt.optimize(plan.clone());
        assert_eq!(out.plan.arity(), plan.arity());
        let twice = opt.optimize(out.plan.clone());
        assert_eq!(twice.plan, out.plan);
    }
}

#[test]
fn union_with_negation_cancels_to_empty() {
    // R + negate(R) = 0. The two R's share a metavariable, so the e-graph must
    // recognize them as the same relation.
    let plan = Rel::Union {
        base: Box::new(get("R", 2)),
        inputs: vec![Rel::Negate {
            input: Box::new(get("R", 2)),
        }],
    };
    let out = optimizer().optimize(plan);
    assert!(
        matches!(out.plan, Rel::Constant { card: 0, arity: 2 }),
        "expected empty constant, got:\n{}",
        out.plan
    );
}

#[test]
fn threshold_elided_only_when_non_negative() {
    let opt = optimizer();
    // Threshold over a plain Get (non-negative) is elided.
    let elidable = Rel::Threshold {
        input: Box::new(get("R", 2)),
    };
    assert_eq!(opt.optimize(elidable).plan, get("R", 2));

    // Threshold over a Negate is *not* elided (could be negative).
    let not_elidable = Rel::Threshold {
        input: Box::new(Rel::Negate {
            input: Box::new(get("R", 2)),
        }),
    };
    assert!(matches!(
        opt.optimize(not_elidable).plan,
        Rel::Threshold { .. }
    ));
}

#[test]
fn reduce_elided_only_when_grouping_a_unique_key() {
    let opt = optimizer();
    let reduce_by0 = |r: Rel| Rel::Reduce {
        input: Box::new(r),
        group_key: vec![col(0)],
        aggregates: vec![],
    };

    // The inner reduce makes the relation distinct on #0, so the outer reduce
    // over that key is elided to a projection.
    let nested = reduce_by0(reduce_by0(get("R", 2)));
    assert!(
        matches!(opt.optimize(nested).plan, Rel::Project { .. }),
        "outer reduce should become a projection"
    );

    // Over a plain Get (no known key) the reduce is *not* elided.
    let single = reduce_by0(get("R", 2));
    assert!(matches!(opt.optimize(single).plan, Rel::Reduce { .. }));
}

#[test]
fn grouping_a_join_by_its_key_is_elided() {
    // Join two relations each made distinct on #0 (so each is keyed on {0}).
    // The join's columns are the concatenation, so {0, 1} is a key of the join;
    // grouping the join by {0, 1} is therefore redundant and elides to a
    // projection. This is key reasoning flowing *through* the join — the
    // relational core of redundant_join / semijoin_idempotence.
    let opt = optimizer();
    let keyed = |name: &str| Rel::Reduce {
        input: Box::new(get(name, 2)),
        group_key: vec![col(0)],
        aggregates: vec![],
    };
    let plan = Rel::Reduce {
        input: Box::new(Rel::Join {
            inputs: vec![keyed("R"), keyed("S")],
            equivalences: vec![],
        }),
        group_key: vec![col(0), col(1)],
        aggregates: vec![],
    };
    let out = opt.optimize(plan.clone());
    assert_eq!(out.plan.arity(), plan.arity());
    assert!(
        matches!(out.plan, Rel::Project { .. }),
        "grouping a join by its key should elide to a projection, got:\n{}",
        out.plan
    );
}

#[test]
fn optimizes_inside_a_recursive_binding() {
    // letrec x = filter(#0, filter(#1, x)) in x. The optimizer treats the
    // LetRec as a scope boundary and saturates the (Let-free) binding body,
    // where merge_filters fuses the two filters — over the recursive LocalGet.
    let opt = optimizer();
    let plan = Rel::LetRec {
        bindings: vec![(
            0,
            Rel::Filter {
                predicates: vec![col(0)],
                input: Box::new(Rel::Filter {
                    predicates: vec![col(1)],
                    input: Box::new(Rel::LocalGet { id: 0, arity: 2 }),
                }),
            },
        )],
        body: Box::new(Rel::LocalGet { id: 0, arity: 2 }),
    };
    let out = opt.optimize(plan.clone());
    assert_eq!(out.plan.arity(), plan.arity());
    match &out.plan {
        Rel::LetRec { bindings, body } => {
            assert!(matches!(**body, Rel::LocalGet { id: 0, .. }));
            assert_eq!(bindings.len(), 1);
            match &bindings[0].1 {
                // The two filters fused into one (two predicates), directly over
                // the recursive reference — not a nested Filter(Filter(...)).
                Rel::Filter { predicates, input } => {
                    assert_eq!(predicates.len(), 2);
                    assert!(matches!(**input, Rel::LocalGet { id: 0, .. }));
                }
                other => panic!("expected a single fused Filter, got:\n{other}"),
            }
        }
        other => panic!("expected the LetRec to be preserved, got:\n{other}"),
    }

    // Optimizing again changes nothing.
    let twice = opt.optimize(out.plan.clone());
    assert_eq!(twice.plan, out.plan);
}

#[test]
fn recursion_invariant_enables_a_rewrite_inside_the_loop() {
    // letrec x = threshold(R + filter(p, x)) in x.
    // The binding is insert-only, so the recursion fixpoint proves it
    // non-negative; injecting that fact lets threshold_elision fire on the
    // recursive reference *inside* the loop, eliding the Threshold. Without the
    // injected fact, `LocalGet x` reads as "unknown" and the Threshold stays.
    let opt = optimizer();
    let plan = Rel::LetRec {
        bindings: vec![(
            0,
            Rel::Threshold {
                input: Box::new(Rel::Union {
                    base: Box::new(get("R", 2)),
                    inputs: vec![Rel::Filter {
                        predicates: vec![col(0)],
                        input: Box::new(Rel::LocalGet { id: 0, arity: 2 }),
                    }],
                }),
            },
        )],
        body: Box::new(Rel::LocalGet { id: 0, arity: 2 }),
    };
    let out = opt.optimize(plan.clone());
    assert_eq!(out.plan.arity(), plan.arity());
    match &out.plan {
        Rel::LetRec { bindings, .. } => match &bindings[0].1 {
            // The Threshold was elided; the binding is now just the union.
            Rel::Union { .. } => {}
            other => panic!("expected the Threshold to be elided to a Union, got:\n{other}"),
        },
        other => panic!("expected a LetRec, got:\n{other}"),
    }
    // Idempotent.
    assert_eq!(opt.optimize(out.plan.clone()).plan, out.plan);
}

#[test]
fn threshold_around_a_nonnegative_recursion_is_elided() {
    // threshold(letrec x = R + filter(p, x) in x).
    // The recursion is insert-only, so its fixpoint is non-negative; treating
    // the whole LetRec as an opaque value carrying that property lets
    // threshold_elision fire on the region *around* the recursion, dropping the
    // outer Threshold — a rewrite at the recursion boundary (option A's sound
    // core), distinct from rewriting inside the bindings (option B).
    let opt = optimizer();
    let rec = Rel::LetRec {
        bindings: vec![(
            0,
            Rel::Union {
                base: Box::new(get("R", 2)),
                inputs: vec![Rel::Filter {
                    predicates: vec![col(0)],
                    input: Box::new(Rel::LocalGet { id: 0, arity: 2 }),
                }],
            },
        )],
        body: Box::new(Rel::LocalGet { id: 0, arity: 2 }),
    };
    let plan = Rel::Threshold {
        input: Box::new(rec.clone()),
    };
    let out = opt.optimize(plan.clone());
    assert_eq!(out.plan.arity(), plan.arity());
    assert!(
        matches!(out.plan, Rel::LetRec { .. }),
        "the outer Threshold should be elided, leaving the LetRec, got:\n{}",
        out.plan
    );
    // A Map wrapper (with a compound scalar, so it stays a Map) has no
    // applicable rule, but it is still pushed past the `in` into the recursion
    // body (sound: the bindings are untouched), so it is preserved as the
    // LetRec's body rather than eliminated.
    let mapped = Rel::Map {
        scalars: vec![Scalar::new("(#0 + #1)", [0usize, 1])],
        input: Box::new(rec),
    };
    match opt.optimize(mapped).plan {
        Rel::LetRec { body, .. } => assert!(matches!(*body, Rel::Map { .. })),
        other => panic!("expected the Map pushed into the LetRec body, got:\n{other}"),
    }
}

#[test]
fn filter_is_pushed_past_the_in_but_not_into_the_loop() {
    // filter(p, letrec x = R + filter(q, x) in x).
    // The filter pushes past the `in` onto the body use of x — sound, since the
    // bindings are untouched — giving `letrec x = R + filter(q, x) in filter(p,
    // x)`. It is NOT pushed into the recursive binding (that would be unsound
    // without a commutation side-condition), so the binding is unchanged.
    let opt = optimizer();
    let plan = Rel::Filter {
        predicates: vec![col(0)],
        input: Box::new(Rel::LetRec {
            bindings: vec![(
                0,
                Rel::Union {
                    base: Box::new(get("R", 2)),
                    inputs: vec![Rel::Filter {
                        predicates: vec![col(1)],
                        input: Box::new(Rel::LocalGet { id: 0, arity: 2 }),
                    }],
                },
            )],
            body: Box::new(Rel::LocalGet { id: 0, arity: 2 }),
        }),
    };
    let out = opt.optimize(plan.clone());
    assert_eq!(out.plan.arity(), plan.arity());
    match &out.plan {
        Rel::LetRec { bindings, body } => {
            // The filter landed on the body's use of x.
            match &**body {
                Rel::Filter { predicates, input } => {
                    assert_eq!(predicates.len(), 1);
                    assert!(matches!(**input, Rel::LocalGet { id: 0, .. }));
                }
                other => panic!("expected filter(p, x) as the body, got:\n{other}"),
            }
            // The recursive binding is untouched (filter did NOT enter the loop).
            assert!(matches!(bindings[0].1, Rel::Union { .. }));
        }
        other => panic!("expected a LetRec, got:\n{other}"),
    }
}

#[test]
fn constant_predicates_are_folded_away() {
    let opt = optimizer();
    let lit = |t: &str| Scalar::new(t, []);

    // filter(1 = 1, R) — the predicate folds to true and is dropped.
    let always = Rel::Filter {
        predicates: vec![lit("(1 = 1)")],
        input: Box::new(get("R", 2)),
    };
    assert_eq!(opt.optimize(always).plan, get("R", 2));

    // filter(1 = 2, R) — folds to false, so the whole thing is empty.
    let never = Rel::Filter {
        predicates: vec![lit("(1 = 2)")],
        input: Box::new(get("R", 2)),
    };
    assert!(matches!(
        opt.optimize(never).plan,
        Rel::Constant { card: 0, arity: 2 }
    ));

    // A real predicate beside a constant-true one keeps only the real predicate.
    let mixed = Rel::Filter {
        predicates: vec![col(0), lit("(2 < 3)")],
        input: Box::new(get("R", 2)),
    };
    match opt.optimize(mixed).plan {
        Rel::Filter { predicates, .. } => assert_eq!(predicates, vec![col(0)]),
        other => panic!("expected a single-predicate filter, got:\n{other}"),
    }
}

#[test]
fn a_bare_column_map_becomes_a_projection() {
    let opt = optimizer();
    // map(#0, R) over arity-2 R appends a copy of column 0, so it is really
    // project[0, 1, 0]. The scalar IR sees the appended scalar is a bare Col,
    // turning the Map into a Project (projection_extraction).
    let plan = Rel::Map {
        scalars: vec![col(0)],
        input: Box::new(get("R", 2)),
    };
    match opt.optimize(plan).plan {
        Rel::Project { outputs, input } => {
            assert_eq!(outputs, vec![0, 1, 0]);
            assert!(matches!(*input, Rel::Get { .. }));
        }
        other => panic!("expected a Project, got:\n{other}"),
    }

    // A map with a non-trivial scalar stays a Map.
    let compound = Rel::Map {
        scalars: vec![Scalar::new("(#0 + #1)", [0usize, 1])],
        input: Box::new(get("R", 2)),
    };
    assert!(matches!(opt.optimize(compound).plan, Rel::Map { .. }));
}

#[test]
fn optimization_is_idempotent() {
    let opt = optimizer();
    let once = opt.optimize(triangle()).plan;
    let twice = opt.optimize(once.clone()).plan;
    assert_eq!(once, twice);
}
