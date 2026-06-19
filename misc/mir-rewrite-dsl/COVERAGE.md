# Coverage of Materialize's MIR transforms

This maps the real transforms in [`src/transform/src/`](../../src/transform/src/)
onto the DSL, and — more importantly — pins down **what is hard and what cannot
be expressed**, with the specific missing capability in each case.

> **Progress (Phases 1–3).** The list-combinator, column-arithmetic, and
> analysis/arity-constructor extensions below are now implemented, so several
> rows that were "hard" are covered: n-ary union flattening/distribution
> (`map(F[_], xs)`), first-input join flattening, predicate pushdown past a
> projection and into the first/second join input (`shift`/`remap`), and the
> analysis-gated `threshold_elision` and `union_cancel` (`non_negative`
> analysis + `Empty(r)` constructor). `non_negative` is even reflected as a
> proved *conditional* Lean theorem. The e-class **analysis framework**
> (`src/analysis.rs`) now carries `non_negative`, `unique_keys` (flowing through
> joins, which brings `reduce_elision` to grouped joins — the relational core of
> `redundant_join`/`semijoin_idempotence`), and `monotonic`. The remaining hard
> rows need `Let`/`LetRec` (Phase 4) or a scalar IR; `non_null` specifically is
> blocked because it has no introduction rule without scalar predicate reading.

The DSL's deliberate design choices are the boundary:

* **opaque scalars** — a rule moves whole payload lists but never reads *inside*
  a predicate / map expression;
* **no column arithmetic** — the only payload combinators are `concat` and
  `compose`; there is no "shift indices by k" or "map a function over a list";
* **no analyses** — the only side condition is `uses_only_input` (a column-set
  ⊆ arity check); there is no key / nullability / monotonicity / cardinality
  oracle;
* **abstract, cardinality-free cost** — good for logical, worst-case reasoning
  (WCOJ), useless for physical/arrangement decisions;
* **no `Let`/`LetRec`, `TopK`, `FlatMap`** in the modeled IR.

## Summary

| Transform | Status | Rule / blocker |
|---|---|---|
| fusion/filter | ✅ covered | `merge_filters` (structural core; predicate canonicalization is scalar) |
| fusion/map | ✅ covered | `fuse_maps` |
| fusion/project | ✅ covered | `fuse_projects` (`compose`) |
| fusion/negate | ✅ covered | `negate_negate` |
| fusion/union | ✅ covered | `flatten_union` + `flatten_union_nary` (Phase 1) |
| compound/union (negate∘union) | ✅ covered | `distribute_negate_union` (+ n-ary, Phase 1) |
| predicate_pushdown | ✅ mostly | `push_filter_through_map`/`_negate`/`_threshold`, `_past_project` (remap), `_into_join_first`/`_second` (shift); general i-th input is the same shape with a prefix-sum offset |
| threshold idempotence | ✅ covered | `threshold_idempotent` |
| join → WCOJ (physical) | ✅ covered | `join_to_wcoj` |
| fusion/join (flatten joins) | 🟡 partial | `flatten_join_first` (Phase 1); inner-not-first needs the prefix-sum shift |
| movement/projection_pushdown, projection_lifting | 🟡 partial | filter-past-project done (`remap`); pushing `Project` itself below operators still needs more index algebra |
| reduce_elision | 🟡 partial | `reduce_elision` (no-aggregate case) via the `Keys` e-class analysis + `is_unique_key`/`cols_of`; now also fires *over joins* (keys flow through `Join`); the aggregate case needs scalar synthesis |
| redundant_join, semijoin_idempotence | 🟡 partial | `Keys` now flows through `Join`/`WcoJoin` (a join-key is the offset-union of one key per input), so grouping a join by a join-key is elided — the relational core. Dropping a whole join input (the rest) needs reasoning about the join's *equivalence scalars*, which opaque scalars forbid |
| non_null_requirements | ❌ impossible\* | a `NonNull` analysis is computable, but it has **no introduction rule** without reading inside a scalar predicate (`is_not_null(#k)`) — so it is vacuously ⊥ here. Blocked by opaque scalars, like the category-B transforms |
| monotonic | 🟡 analysis only | `Monotonic` e-class analysis (insert-only) + `monotonic(rel)` condition, distinct from `non_negative` (a `Reduce` breaks monotonicity but not non-negativity). On this static subset `monotonic ⟹ non_negative`, so it unlocks no *new* equality; its real consumers are monotonic *physical* rewrites (`TopK`), out of scope here |
| union_cancel | ✅ covered | `union_cancel` (Phase 3): `Empty(a)` constructor + shared-metavariable match |
| threshold_elision | ✅ covered | `threshold_elision` (Phase 3): `non_negative` analysis (also a *proved* conditional Lean theorem) |
| equivalence_propagation | ❌ impossible\* | reads/rewrites **inside scalars** |
| literal_lifting, literal_constraints | ❌ impossible\* | inspects/synthesizes **scalar literals** |
| column_knowledge | ❌ impossible\* | per-column value/range **scalar analysis** |
| fold_constants | 🟡 partial | the scalar layer (`canonicalize_scalars`) folds scalars; the *relational* consequences are **DSL rules** — `drop_true_filter` / `empty_false_filter` gated by the scalar-IR `all_true` / `any_false` conditions. Deeper scalar passes below are the follow-ups |
| canonicalize_mfp (scalar CSE) | ❌ impossible\* | **scalar** normalization/CSE |
| canonicalization/projection_extraction | ✅ covered | **DSL rule** `map_columns_to_projection`: `Map[s] r => Project[iota(\|r\|) ++ cols_of(s)] r where all_columns(s)` — the `all_columns` condition reads scalar structure via the scalar IR |
| flat_map_elimination | ❌ out of scope | no `FlatMap` in the IR (+ scalar eval) |
| top_k fusion / elision | ❌ out of scope | no `TopK` in the IR |
| reduction_pushdown, reduce_reduction, demand, join_implementation | ❌ out of scope | **physical/cardinality/arrangement** planning, not equality rewriting |
| cse/anf | 🟡 partial | extraction-time CSE (`crate::cse`, Phase 4): the e-graph shares; extraction re-binds repeats with `Let` |
| LetRec (recursion) | 🟡 partial | `Rel::LetRec` is modeled as a **binding scope**: the optimizer saturates each Let-free fragment and reassembles the scope; analyses flow *through* the recursion (`RecAnalysis`, `Direction::{Lfp,Gfp}`) and feed both the in-binding rewriter (B) and rewrites *around* the recursion (A, eliding an op wrapped over a provably non-negative recursion). E-matching *through* the back-edge is unsound with plain saturation (see §5) and left as the frontier |
| relation_cse, normalize_lets | ❌ out of scope | whole-program `Let` normalization (the e-graph already shares subterms; `Let` is an extraction concern) |

\* *impossible **as a relational rewrite with opaque scalars**; possible only by
adding a scalar-expression IR and scalar rewrites — a different language layer.*

## Now covered

Thirteen rules in [`rules/relational.rewrite`](rules/relational.rewrite). The
fusions (`merge_filters`, `fuse_maps`, `fuse_projects`, `negate_negate`,
`flatten_union`) and the movement rules (`push_filter_through_{map,negate,threshold}`,
`distribute_{filter,negate}_union`, `push_filter_into_join_first`) are the
structural cores of the corresponding transforms. They are expressible precisely
because they need **no column renumbering**:

* `Filter`/`Map` *append* or *mask* — the inner relation's columns keep their
  indices, so `concat` suffices and predicates that read only input columns
  (`uses_only_input`) ride along unchanged;
* `push_filter_into_join_first` targets the **first** input, whose columns are
  `0..arity(a)` in the join output — same indices below the join.

`join_to_wcoj` is the physical lever the abstract cost model rewards.

## Hard — needs a bounded DSL extension (still relational, still opaque-scalar)

These have a clear structural shape but need one new, *scalar-agnostic*
capability:

1. **Column-arithmetic combinators** (`shift(p, k)`, offset-aware `compose`).
   The single biggest unlock: it gives the *general* `predicate_pushdown`
   (push a predicate onto the *i*-th join input by subtracting the input's
   column offset), `fusion/join` (flatten `Join(Join(a,b), c)` by renumbering
   the inner equivalences), and all of `projection_pushdown`/`_lifting`. These
   are pure index bookkeeping — no scalar *values* are inspected — so they fit
   the opaque-scalar discipline; the DSL just lacks the index algebra today.

2. **A list-map combinator** (`map(λx. F[x], xs)`). Lets a `rest...` capture be
   transformed element-wise, generalizing `flatten_union`/`distribute_*` to the
   n-ary `Union`/`Join` that MIR actually produces.

3. **Arity-carrying constructors** (e.g. `Empty(a)` → a `Constant` with
   `arity(a)`). Unlocks `union_cancel` (`Union(a, Negate(a)) => Empty(a)`, whose
   LHS the matcher already handles via repeated relvars) and folding of empty
   inputs.

4. **Analysis-backed side conditions** — now a small framework. [`src/analysis.rs`](src/analysis.rs)
   defines an `Analysis` trait (lattice `Domain`, transfer `make`, `merge`) run
   to a fixpoint per e-class by `EGraph::run_analysis`. Because all e-nodes in a
   class are equal relations, `merge` combines facts *toward more precision*, so
   the analysis is sharper than a single-plan one. Three instances are wired in:
   `NonNeg` (non-negativity), `Keys` (unique keys, now flowing through joins),
   and `Monotonic` (insert-only), feeding the `non_negative` / `is_unique_key` /
   `monotonic` conditions (→ `threshold_elision`, `reduce_elision`). Adding one
   is genuinely drop-in: a struct, an `impl Analysis`, a field in `Analyses`,
   and a `Cond` arm. The `redundant_join`/`semijoin_idempotence` family is the
   `Keys`-through-`Join` case above; `monotonic` is implemented but unlocks no
   new equality on this subset (see the table); `non_null` is the one that
   genuinely can't be introduced without scalar predicate reading. This same
   `run_analysis` fixpoint is exactly the engine a recursive `LetRec` binding
   needs (its analysis is a fixpoint over the recursive variable), so the
   analysis framework and recursion are one mechanism.

   Note an e-graph subtlety this exposed: saturation makes classes *cyclic*
   (`threshold_elision` unions `Threshold r` into `r`'s class, so that class
   gains a `Threshold` node pointing back at itself). Any pass that walks the
   structure — including the arity used to offset join-keys — must be
   cycle-safe; `EGraph::arity` determines a class's (invariant) arity from any
   acyclic e-node, with a visited guard.

5. **`Let`/`LetRec` in the IR.** Needed to *represent* `normalize_lets` and CSE.
   Note, though, that the **e-graph already subsumes the *purpose* of CSE**:
   equal subterms are hash-consed into one e-class, so sharing is automatic
   inside the optimizer — `Let`-binding is really an *extraction-time* concern
   (emit a `Let` when a subexpression is referenced more than once). This is now
   implemented as a post-extraction pass in [`src/cse.rs`](src/cse.rs).

   **`LetRec` (recursion) is now modeled** as an explicit *binding scope* rather
   than dissolved into the e-graph (a finite saturated e-graph cannot represent
   a fixpoint). The optimizer treats each scope as a boundary: it saturates
   every maximal Let-free fragment — binding values and bodies, with recursive
   references as opaque `LocalGet` leaves — and reassembles the scope
   ([`src/engine.rs`](src/engine.rs)). Rewriting a binding *body* with the
   ordinary relational rules is sound because the least fixpoint depends on the
   body only as a function (`Semantics.lean`'s `letRec_congr`). Crucially,
   **analyses flow through the recursion**: [`RecAnalysis`](src/analysis.rs) is
   the *same* monotone-fixpoint idea as the e-class `Analysis`, but with the
   recursive `LocalGet` as the iterated variable and a [`Direction`] axis —
   *greatest* fixpoint for invariants we want to guarantee (`non_negative`,
   `monotonic`: assume-then-verify over the cycle) and *least* fixpoint for
   under-approximations (`unique_keys`). That is the concrete sense in which
   "the analysis framework and recursion are one mechanism": recursion is not a
   new subsystem, it is the analysis fixpoint with the back-edge as its
   variable.

   Two further steps build on this, one done and one fenced off:

   * **(B) The recursion facts feed the in-fragment rewriter.** While saturating
     a binding body, the e-class analyses are *seeded* with the proven facts for
     each `LocalGet` (`LocalFacts`, injected through `EGraph::saturate`), so an
     analysis-gated rule fires on a recursive reference exactly when the
     fixpoint proves the invariant — e.g. `threshold_elision` inside
     `letrec x = threshold(R + filter(p, x))`. An outer fixpoint re-analyzes
     after each rewrite (a rewrite can expose a stronger invariant). Sound: each
     fact is a fixpoint certificate on the current syntactic form, and equality
     rewrites preserve the underlying property.

   * **(A) Rewriting the region *around* a recursion.** The optimizer also
     treats a whole `LetRec` as an opaque value that *carries* the properties
     its fixpoint proves, and rewrites the surrounding fragment with them — e.g.
     eliding a `Threshold` wrapped around a provably non-negative recursion
     ([`optimize_around_scopes`](src/engine.rs)). This is the **sound core** of
     "cross-binding e-matching".

   * **Pushing an operator past the `in`.** A single-input operator directly
     above a scope is pushed *into* the scope body before optimizing:
     `O(LetRec x = b in B) ⇒ LetRec x = b in O(B)`
     ([`normalize_push_into_scopes`](src/engine.rs)). Unconditionally sound —
     the bindings, hence the fixpoint `x*`, are untouched — and it slides the
     operator inside where the recursion facts (B) can act on it. **Note the
     contrast with pushing *into the recursive bindings*** (`σ_p` through the
     loop): that is the classic predicate-pushdown-into-recursion problem and is
     *unsound* in general. By the least-fixpoint **fusion law** (`Filter` is
     linear/strict/continuous), `σ_p(μ body) = μ(σ_p ∘ body)` holds **iff**
     `σ_p ∘ body = σ_p ∘ body ∘ σ_p` (the filter commutes with the recursive
     step); otherwise one only has `μ(σ_p ∘ body) ⊆ σ_p(μ body)` — eager
     filtering drops the "stepping-stone" rows the recursion needs (the
     transitive-closure / magic-sets failure). Checking that commutation needs
     the predicate's column structure, which opaque scalars deny, so we push
     only as far as the `in`, never into the loop.

     The *full* version — e-matching **through** the recursive back-edge — is
     **unsound** with plain equality saturation, and it is worth stating exactly
     why. The tempting move is to union the recursive reference with the body,
     `LocalGet x ≡ body`, and saturate. But that equation holds only at the
     fixpoint value `x*`: it certifies `def(x*) = x*` (that `x*` is *a* fixed
     point of any extracted definition `def`), **not** `lfp(def) = x*`. A
     rewrite that uses the equation off the fixpoint can change the function and
     hence the least fixpoint it denotes. The sound subset — rewrites that are
     identities of the body *as a function of `x`* (with `x` a free leaf) — is
     precisely what (B) already does. So genuine through-the-back-edge
     saturation needs **fixpoint-aware** rewriting: rules guarded to *commute
     with `lfp`* (monotone, linear operators), plus a recursive extraction that
     reads a cyclic class back as a `LetRec`. That is a separate research design,
     not an equality-saturation tweak, and is deliberately left as the frontier.

## Needs the scalar IR (now started)

Everything in category B of the survey — `equivalence_propagation`,
`literal_lifting`, `literal_constraints`, `column_knowledge`, `fold_constants`,
scalar CSE in `canonicalize_mfp`, and `projection_extraction` — requires
**reading or rewriting the structure of scalar expressions** (is this predicate
`col = literal`? what is the literal? is this map scalar a bare `#k`?). This is
the same split Materialize draws: `MirScalarExpr` rewrites are a separate concern
from relational reshaping.

[`src/scalar.rs`](src/scalar.rs) is the start of that layer: a structured
`Expr` (columns, literals, a modeled operator set), a total `parse`, a
constant-`fold`, and `render`. It is split the way the rest of the system is:

* the **scalar layer** (`canonicalize_scalars` in the engine) only *folds*
  scalars in place — the analog of Materialize's `FoldConstants`/
  `canonicalize_mfp`, never a relational rewrite;
* the **relational consequences are DSL rules**, gated by scalar-IR-backed
  *conditions* (`all_true`, `any_false`, `all_columns`) and combinators (`iota`),
  exactly the way analyses back `non_negative` / `is_unique_key`. So
  **`fold_constants`** (`drop_true_filter`, `empty_false_filter`) and
  **`projection_extraction`** (`map_columns_to_projection`) live in
  `rules/relational.rewrite` and get generated Lean theorems — the two filter
  rules are *proved* conditional theorems (`∀ x, p x = true ⇒ filterB p r = r`).

That keeps the single-source-of-truth property: a relational transform is a rule,
not hand-written Rust. The remaining category-B transforms follow the same shape:
`column_knowledge` and `equivalence_propagation` want a per-column fact lattice
over `Expr` (an `Analysis` instance), `non_null` is "does this `Expr` evaluate
non-null given its inputs' nullability". The honest remaining cost is depth (each
is its own condition/analysis), not a missing representation. Making `Expr` the *primary* payload (replacing the opaque
`(text, columns)` storage) and saturating scalars in their own e-graph is the
larger structural follow-up.

## Out of scope by design

`reduction_pushdown`, `reduce_reduction`, `demand`, and `join_implementation`
are **physical/cost/arrangement** planning. The abstract cost here is a
worst-case *logical* measure (deliberately cardinality-free); choosing
arrangements or pushing reductions for cardinality reasons is exactly the kind
of statistics-driven decision this model abstracts away. They would be driven by
a different cost oracle, not by equality-preserving rewrite rules.
