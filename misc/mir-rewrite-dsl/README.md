# `mz-mir-rewrite-dsl`

A small language for describing **equality-preserving rewrites of MIR
relational expressions**, an optimizer **generated** from that language, and a
**Lean 4 specification** that validates the rewrites.

It is a self-contained, dependency-free crate (its own Cargo workspace) modeling
a faithful *subset* of Materialize's `MirRelationExpr`. The point is to make the
three ideas in the brief concrete and testable:

1. a DSL for cost-oriented, equality-preserving relational rewrites;
2. an optimizer that explores the rewrite space **worst-case-optimally** and runs
   to a fixpoint while cost decreases — without getting stuck in local minima;
3. a machine-checkable spec of *why each rewrite is allowed* (it preserves
   results), in Lean 4.

```
rules/relational.rewrite        ← the DSL: the single source of truth
        │
   parser::parse_ruleset
        │
   dsl::RuleSet
      ╱        ╲
engine::Optimizer        lean::emit_lean
 (e-graph saturation  →  one theorem per rule, checked by `lake build`
  + WCOJ e-matching       in lean/MirRewrite/Generated.lean)
  + abstract cost
  + extraction)
```

## The DSL

A rule is an oriented rewrite `lhs => rhs` over the relational subset, with
optional side conditions. Metavariables are lowercase; operators are
capitalized; payloads (predicate lists, projections, equivalences) are bound in
`[...]` and treated as **opaque whole lists** — the language is deliberately
*relational* and never destructures a scalar.

```text
rule merge_filters {
    doc "filter(p, filter(q, r)) = filter(p && q, r)"
    Filter[p] (Filter[q] r) => Filter[concat(q, p)] r
}

rule push_filter_through_map {
    Filter[p] (Map[s] r) => Map[s] (Filter[p] r)
    where uses_only_input(p, r)
}

rule join_to_wcoj {
    Join[e](rs...) => WcoJoin[e](rs...)
}
```

The full rule set is in [`rules/relational.rewrite`](rules/relational.rewrite):
filter/map/projection fusion, filter/negate/threshold commutation, filter/union
and negate/union distribution, union flattening, double-negation, threshold
idempotence, filter-into-join pushdown, and the worst-case-optimal-join
conversion. Every rule is an identity of the multiplicity semantics (see Lean
below).

[`COVERAGE.md`](COVERAGE.md) maps these (and the gaps) against Materialize's
actual `src/transform/` passes: what the DSL covers, what is *hard* (and which
bounded extension would unlock it — column-arithmetic combinators, a list-map
combinator, arity-carrying constructors, analysis-backed side conditions), and
what is *impossible* without abandoning opaque scalars (anything that reads or
rewrites inside a scalar expression).

Payload combinators (`concat`, `compose`) and side conditions
(`uses_only_input`) are small and extensible; a production version would add a
list-`map` combinator to express, e.g., n-ary union distribution.

## Cost is abstract — no cardinalities

We do **not** assume we know base-table cardinalities. A plan is scored by its
**worst-case asymptotic cost** as a function of an unknown input size `N`: every
operator contributes a *work term* equal to the degree (exponent of `N`) of the
data it processes, and a plan's cost is the multiset of those degrees, compared
largest-term-first with node count as a tie-breaker (`src/cost.rs`).

This makes the **worst-case-optimal join** the central, cardinality-free lever:

* The output size of a multiway join is the **AGM bound** — the minimum, over
  fractional edge covers of the join hypergraph, of `Σ λₑ·deg(Rₑ)` (solved
  exactly by a tiny LP).
* A worst-case-optimal join (`WcoJoin`) evaluates in time proportional to that
  bound.
* A binary `Join` additionally pays for the intermediate results of its best
  join order, whose degree can strictly exceed the AGM bound on cyclic joins.

For the **triangle** `R(a,b) ⋈ S(b,c) ⋈ T(c,a)` this is `N^1.5` (WCOJ) versus
`N^2` (binary) — an improvement that holds for *all* input sizes, which is
exactly why it needs no cardinality estimate. (Consequently, filter pushdown is
cost-*neutral* here: without selectivity it gives no worst-case win. It is kept
because it is valid and harmless, and unlocks merges.)

## The engine explores the rewrite space worst-case-optimally

Greedy, cost-monotone rewriting (take a step only if it lowers cost) gets stuck:
some wins are reachable only *through* a cost-increasing step. The `filtered
union` example needs to first **distribute** a filter over a union (one more
operator — greedy refuses) before it can **merge** filters in each branch for a
net win.

So the optimizer instead does **equality saturation** (`src/egraph.rs`): it adds
the plan to an e-graph and applies *every* rule wherever it matches, recording
equivalences compactly, regardless of cost — then extracts the cheapest plan at
the end. It cannot get stuck in a local minimum.

Finding all matches of a rule is a **conjunctive query** over the e-graph (one
atom per pattern operator; shared pattern variables are join variables).
Following *relational e-matching* (Zhang et al., 2022), we evaluate it with a
**generic join** — the worst-case-optimal join algorithm — binding one variable
at a time and intersecting candidates across the atoms that mention it. So the
engine literally **explores the transform graph in a WCOJ manner**: the same
worst-case-optimal-join idea appears twice, once as a *plan operator* and once
as the optimizer's *own search strategy*.

Run the demo:

```console
$ cargo run --bin mir-opt
…
================ triangle join ================
saturating: [N^2 + N^1.5] (nodes=4) -> [N^1.5] (nodes=4)   # picks WcoJoin
…
================ filtered union ================
saturating: [...] (nodes=6) -> [...] (nodes=5)
greedy:     [...] (nodes=6) -> [...] (nodes=6)
=> saturation found a strictly cheaper plan than greedy (greedy hit a local minimum).
```

## The Lean 4 spec

A relation denotes a **multiplicity function** `Row → Int` — a signed multiset,
matching differential dataflow (the substrate Materialize compiles to). `union`
adds multiplicities, `negate` flips sign, `threshold` drops non-positive rows,
`filter` masks rows. Two relations are equal iff these functions are equal, so a
rewrite is *equality preserving* exactly when it is an identity of them.

* [`lean/MirRewrite/Semantics.lean`](lean/MirRewrite/Semantics.lean) — the
  hand-written semantics (Mathlib-free; Lean core only).
* [`lean/MirRewrite/Generated.lean`](lean/MirRewrite/Generated.lean) — one
  theorem **per DSL rule**, generated by `cargo run --bin gen-lean`. The
  multiplicity-structural rules are proved; `Map`/`Project` rules that need
  column-structure reasoning are emitted with `sorry`, making those obligations
  explicit rather than hidden.

```console
$ cargo run --bin gen-lean      # regenerate Generated.lean from the DSL
$ cd lean && lake build         # check the proofs (requires a Lean toolchain)
```

## Layout

| File | Role |
|------|------|
| `rules/relational.rewrite` | the DSL rule file (source of truth) |
| `src/ir.rs` | the MIR relational subset (`Rel`, opaque `Scalar`) |
| `src/scalar.rs` | a structured scalar IR (`Expr`) — parse / constant-fold / render |
| `src/dsl.rs`, `src/parser.rs` | rule AST and a hand-written parser |
| `src/matcher.rs` | payload combinators, side conditions, term instantiation |
| `src/cost.rs` | abstract worst-case cost + AGM / fractional-edge-cover LP |
| `src/analysis.rs` | e-class analyses (`Analysis`) + recursion-aware analyses (`RecAnalysis`, lfp/gfp): non-negativity, unique keys (across joins), monotonicity |
| `src/egraph.rs` | e-graph, generic-join e-matching, saturation, extraction |
| `src/engine.rs` | the saturating `Optimizer` (+ a greedy foil) |
| `src/cse.rs` | extraction-time common-subexpression elimination (`Let`) |
| `src/lean.rs` | DSL → Lean 4 theorem generator |
| `lean/` | the Lean project (semantics + generated theorems) |

## Relationship to real MIR, and limitations

The `Rel` enum mirrors `mz_expr::MirRelationExpr` for the modeled operators, and
`Rel::Join` keeps MIR's `equivalences` representation, so the rule shapes
transfer directly. To stay decidable and self-contained the model simplifies:
scalars are opaque (no scalar rewrites); the cost model assumes uniform base
sizes; and the AGM cover LP folds private/payload columns into a single private
vertex per input. Saturation *does* make the e-graph cyclic (e.g.
`threshold_elision` unions `Threshold r` into `r`'s class), so the structural
passes — arity, extraction — are cycle-safe (they read any acyclic
representative of a class). Genuine recursion (`LetRec`) is handled one level
up, as an explicit binding scope around saturated Let-free fragments, with
analyses carried through the recursion by a least/greatest-fixpoint
([`src/analysis.rs`](src/analysis.rs)); cross-binding e-matching through the
recursive back-edge is future work. Wiring this to the real optimizer would mean translating
`MirRelationExpr` to/from `ENode`, replacing the abstract cost with one that can
optionally consume `StatisticsOracle` estimates, and reusing Materialize's
existing `Fixpoint`/`Transform` plumbing to host the saturating pass.
