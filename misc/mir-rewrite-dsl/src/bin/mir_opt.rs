// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! A small demonstration driver for the relational rewrite optimizer.
//!
//! ```text
//! cargo run --bin mir-opt
//! ```
//!
//! It runs the saturating optimizer (and the greedy one, for contrast) on a
//! couple of representative plans and prints the before/after cost and plan.

use mz_mir_rewrite_dsl::cost::{Cost, CostModel};
use mz_mir_rewrite_dsl::engine::{GreedyOptimizer, Optimizer};
use mz_mir_rewrite_dsl::ir::{Rel, Scalar};
use mz_mir_rewrite_dsl::{default_ruleset, RULES_SRC};

fn get(name: &str, arity: usize) -> Rel {
    Rel::Get {
        name: name.into(),
        arity,
    }
}

fn col(c: usize) -> Scalar {
    Scalar::new(format!("#{c}"), [c])
}

/// The classic triangle join R(a,b) ⋈ S(b,c) ⋈ T(c,a). A binary plan pays N²;
/// the worst-case-optimal join pays N^1.5.
fn triangle() -> Rel {
    let eq = |a: usize, b: usize| vec![col(a), col(b)];
    Rel::Join {
        inputs: vec![get("R", 2), get("S", 2), get("T", 2)],
        // a:#0=#4  b:#1=#2  c:#3=#5
        equivalences: vec![eq(0, 4), eq(1, 2), eq(3, 5)],
    }
}

/// `filter(p, A ∪ B)` where each branch is itself filtered. Reaching the cheap
/// plan requires first *distributing* the filter (which adds an operator, so a
/// greedy optimizer refuses) and only then *merging* filters in each branch.
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

fn fmt_cost(c: &Cost) -> String {
    let degs: Vec<String> = c.degrees.iter().map(|d| format!("N^{d}")).collect();
    format!("[{}] (nodes={})", degs.join(" + "), c.nodes)
}

fn main() {
    let rules = default_ruleset();
    let model = CostModel::new();
    let sat = Optimizer::new(rules.clone(), model.clone());
    let greedy = GreedyOptimizer::new(rules, model);

    println!("== rule set ==\n{RULES_SRC}");

    for (name, plan) in [
        ("triangle join", triangle()),
        ("filtered union", filtered_union()),
    ] {
        println!("\n================ {name} ================");
        println!("-- input --\n{plan}");

        let s = sat.optimize(plan.clone());
        let g = greedy.optimize(plan);

        println!(
            "saturating: {} -> {}  ({} iters)",
            fmt_cost(&s.initial_cost),
            fmt_cost(&s.final_cost),
            s.iterations
        );
        println!("-- optimized (saturating) --\n{}", s.plan);
        println!(
            "greedy:     {} -> {}  ({} iters)",
            fmt_cost(&g.initial_cost),
            fmt_cost(&g.final_cost),
            g.iterations
        );
        if s.final_cost.lt(&g.final_cost) {
            println!("=> saturation found a strictly cheaper plan than greedy (greedy hit a local minimum).");
        }
    }
}
