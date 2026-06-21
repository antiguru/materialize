// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Soundness gate and capability proof for the negate-join rewrite rules
//! (`distribute_negate_join` / `factor_negate_join`).
//!
//! These rules merge `join(negate(a), rest)` and `negate(join(a, rest))` into
//! one e-class. The merge is sound ONLY because the polarity-aware extractor
//! refuses to place a `Negate`-rooted representative directly under a non-linear
//! `Reduce` or a `TopK`. The first test is the regression gate for that
//! property: re-enabling the rules without the polarity-aware extractor (Task 1)
//! would make it fail. The second test proves the win the rules unlock: pulling
//! the negate out of a join lets `union_cancel` collapse
//! `Union(join(a, rest), join(negate(a), rest))` to empty.
//!
//! The negate-join rules are annotated `phase physical`, so they no longer fire
//! in the live logical pass (where pushing a Negate into a join breaks
//! arrangement reuse). These tests therefore drive the offline `optimize` entry
//! point, which runs the full rule set across all phases, to keep exercising
//! the rules.

use mz_compute_types::plan::reduce::{ReductionType, reduction_type};
use mz_expr::{AccessStrategy, AggregateExpr, AggregateFunc, Id, MirRelationExpr, MirScalarExpr};
use mz_repr::{GlobalId, ReprRelationType, ReprScalarType};

/// Build a source relation with `arity` Int64 non-nullable columns and a unique
/// transient global id, raised as an opaque `Get` leaf so eqsat does not fold it
/// away.
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

/// Build the cross `Join` of `left` and `right` with no equivalences. The first
/// input occupies the leading columns of the output, which is what the
/// negate-join rules rely on (they move the `Negate` across the first input
/// only, with no reindexing).
fn cross_join(left: MirRelationExpr, right: MirRelationExpr) -> MirRelationExpr {
    MirRelationExpr::join(vec![left, right], vec![])
}

/// Returns `true` iff any `Reduce` or `TopK` in `expr` has an immediate input
/// that is `Negate`-rooted. This is exactly the unsound shape the polarity-aware
/// extractor must never produce: `reduce(r) != negate(reduce(negate(r)))` for a
/// non-linear aggregate, and likewise for `TopK`.
fn has_negate_under_non_linear(expr: &MirRelationExpr) -> bool {
    let mut found = false;
    expr.visit_pre(|e| match e {
        // A Reduce with no aggregates is a Distinct, which is polarity
        // insensitive, so it is not a violation. Only aggregating reduces are.
        MirRelationExpr::Reduce {
            input, aggregates, ..
        } if !aggregates.is_empty() => {
            if matches!(**input, MirRelationExpr::Negate { .. }) {
                found = true;
            }
        }
        MirRelationExpr::TopK { input, .. } => {
            if matches!(**input, MirRelationExpr::Negate { .. }) {
                found = true;
            }
        }
        _ => {}
    });
    found
}

/// Count the `Union` nodes in `expr`.
fn count_unions(expr: &MirRelationExpr) -> usize {
    let mut n = 0;
    expr.visit_pre(|e| {
        if matches!(e, MirRelationExpr::Union { .. }) {
            n += 1;
        }
    });
    n
}

/// Returns `true` iff every `Constant` leaf in `expr` is the empty constant, and
/// `expr` contains at least one such leaf.
fn collapses_to_empty(expr: &MirRelationExpr) -> bool {
    let mut saw_constant = false;
    let mut all_empty = true;
    expr.visit_pre(|e| {
        if let MirRelationExpr::Constant { rows, .. } = e {
            saw_constant = true;
            match rows {
                Ok(rows) if rows.is_empty() => {}
                _ => all_empty = false,
            }
        }
    });
    saw_constant && all_empty
}

/// A non-linear `MAX` aggregate over column #0.
fn max_agg() -> AggregateExpr {
    AggregateExpr {
        func: AggregateFunc::MaxInt64,
        expr: MirScalarExpr::column(0),
        distinct: false,
    }
}

/// SOUNDNESS GATE. A non-linear `Reduce(MAX)` sits over a plain `join(a, rest)`.
/// The same `join(a, rest)` also appears negated as `join(negate(a), rest)` in a
/// sibling union arm, so `factor_negate_join` rewrites that arm to
/// `negate(join(a, rest))` and merges it into the e-class of `join(a, rest)`. The
/// Reduce's input class therefore contains a `Negate`-rooted representative.
/// Without the polarity-aware extractor the extractor could pick that signed
/// form as the Reduce input, producing `Reduce_MAX(negate(join(a, rest)))`,
/// which is incorrect because `reduce(r) != negate(reduce(negate(r)))` for a
/// non-linear aggregate. This test asserts the extracted plan never does so.
///
/// Scope. This is an end-to-end INVARIANT assertion (defense in depth), not a
/// true extractor regression gate. The cost model already prefers a non-negated
/// representative because a `Negate` is an extra node that raises cost, so a
/// cost-only extractor would also satisfy this assertion even without the
/// polarity machinery. It therefore cannot, on its own, catch a regression in
/// the polarity-aware extractor. The genuine regression gate for that extractor
/// is the `reduce_input_avoids_negate_representative` unit test in
/// `eqsat::egraph`, which constructs a class where the `Negate`-rooted
/// representative is CHEAPER than its non-negated equivalent under a reduce, so
/// only the polarity constraint, not cost, can reject it. Keep this invariant
/// assertion regardless: it guards the property end-to-end. Do NOT weaken it or
/// add a rule guard: the rules are sound only because the extractor forbids the
/// signed form here.
#[mz_ore::test]
fn negate_join_soundness_gate_reduce_max() {
    // Sanity: MAX really is a non-linear (Hierarchical) aggregate, otherwise the
    // gate would not be exercising the unsound shape.
    assert_eq!(
        reduction_type(&AggregateFunc::MaxInt64),
        ReductionType::Hierarchical
    );

    let a = src(1, 1);
    let rest = src(2, 1);
    // The Reduce input is a plain join, which has a valid non-negative form.
    let reduce = cross_join(a.clone(), rest.clone()).reduce(vec![0], vec![max_agg()], None);
    // A sibling arm holds the negated join, which triggers `factor_negate_join`
    // and contaminates the shared `join(a, rest)` e-class with a Negate-rooted
    // representative. Both arms have arity 2 (group key + MAX, vs. two columns).
    let negated_join = cross_join(a.negate(), rest);
    let input = reduce.union(negated_join);

    let out = mz_transform::eqsat::optimize(input);

    assert!(
        !has_negate_under_non_linear(&out),
        "polarity-aware extraction must never place a Negate-rooted \
         representative directly under a non-linear Reduce/TopK; got {out:?}"
    );
}

/// SOUNDNESS GATE (TopK variant). Same shape as above but the barrier over the
/// plain join is a `TopK` instead of a non-linear `Reduce`. `TopK` is likewise
/// unsound over signed multiplicities, so the extractor must not feed it a
/// `Negate`-rooted input even after the negated sibling arm merges one into the
/// shared join's e-class.
#[mz_ore::test]
fn negate_join_soundness_gate_topk() {
    let a = src(1, 1);
    let rest = src(2, 1);
    // TopK over a plain join, grouped on nothing, limit 1, ordered by #0. Arity
    // 2, matching the negated-join sibling arm below.
    let topk = cross_join(a.clone(), rest.clone()).top_k(
        vec![],
        vec![mz_expr::ColumnOrder {
            column: 0,
            desc: false,
            nulls_last: false,
        }],
        Some(MirScalarExpr::literal_ok(
            mz_repr::Datum::Int64(1),
            mz_repr::ReprScalarType::Int64,
        )),
        0,
        None,
    );
    let negated_join = cross_join(a.negate(), rest);
    let input = topk.union(negated_join);

    let out = mz_transform::eqsat::optimize(input);

    assert!(
        !has_negate_under_non_linear(&out),
        "polarity-aware extraction must never place a Negate-rooted \
         representative directly under a non-linear Reduce/TopK; got {out:?}"
    );
}

/// GRACEFUL NO-OP. A non-linear `Reduce(MAX)` whose input is irredeemably signed
/// (a bare `Negate(Get)` with no non-negative equivalent in its e-class) has no
/// extractable plan: the polarity-aware extractor demands a non-negative input
/// for the reduce, the input class offers only the `Negate`-rooted form, so the
/// root class has no representative. Extraction returns `None` and the optimizer
/// must fall back to the un-optimized input, a sound no-op. This previously
/// PANICKED with "root class could not be extracted"; that must now be a no-op.
///
/// We assert the optimizer does not panic and leaves the `Negate` directly under
/// the non-linear reduce (it neither dropped the constraint nor rewrote the
/// fragment), which is exactly the un-optimized input shape.
#[mz_ore::test]
fn negate_under_reduce_extraction_is_noop() {
    let a = src(1, 1);
    // A non-linear MAX over a directly negated input. Nothing makes this input
    // non-negative, so the reduce input class has no non-negative form.
    let input = a.negate().reduce(vec![], vec![max_agg()], None);

    // Precondition: the input really has the unsound-looking shape the extractor
    // refuses to optimize, so a passing test exercises the fallback.
    assert!(
        has_negate_under_non_linear(&input),
        "input must have a Negate directly under the non-linear reduce"
    );

    // Must not panic. The optimizer returns the un-optimized fragment.
    let out = mz_transform::eqsat::optimize(input);

    assert!(
        has_negate_under_non_linear(&out),
        "graceful no-op must leave the Negate under the non-linear reduce \
         (the un-optimized input), never drop the polarity constraint; got {out:?}"
    );
}

/// WIN. `Union(join(a, rest), join(negate(a), rest))` collapses to empty.
/// `factor_negate_join` rewrites the second arm to `negate(join(a, rest))`,
/// turning the union into `Union(x, negate(x))`, on which `union_cancel` fires
/// (`a + negate(a) = 0`). Without the negate-join rules the union would not
/// collapse, so this is the capability the rules unlock.
#[mz_ore::test]
fn negate_join_unlocks_union_cancel() {
    let a = src(1, 1);
    let rest = src(2, 1);
    // The two arms share the same `join(a, rest)` subterm (hash-consed into one
    // e-class). The negate sits on the FIRST input of the second arm's join, the
    // only position the rules move it across.
    let arm_plain = cross_join(a.clone(), rest.clone());
    let arm_negated = cross_join(a.negate(), rest);
    // union_cancel matches `Union(x, Negate x)`, so the Negate-bearing arm is the
    // second union input.
    let input = arm_plain.union(arm_negated);

    // Precondition: the input is a single Union of two joins (no cancellation
    // yet), so a passing test really exercises the collapse.
    assert_eq!(count_unions(&input), 1, "input is a single Union");

    let out = mz_transform::eqsat::optimize(input);

    assert!(
        collapses_to_empty(&out),
        "factor_negate_join + union_cancel must collapse \
         Union(join(a, rest), join(negate(a), rest)) to empty; got {out:?}"
    );
}
