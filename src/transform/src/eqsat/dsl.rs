// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! The abstract syntax of the rewrite DSL.
//!
//! A rule file is a sequence of [`Rule`]s. Each rule is an oriented,
//! equality-preserving rewrite `lhs => rhs` over the relational subset, plus
//! optional side [`Cond`]itions. The left-hand side is a [`Pat`]tern with
//! metavariables; the right-hand side is a [`Tmpl`] that reuses those
//! metavariables, optionally combining payloads with [`PExpr`] operators.
//!
//! Scalars are opaque (see [`crate::eqsat::ir`]): the DSL never destructures a
//! predicate or a map expression, it only moves whole *payload lists* around.
//! That keeps the language squarely focused on **relational** rewrites.

/// A pattern: the left-hand side of a rule. Operator nodes bind their payload
/// to a named metavariable in `[...]`; lowercase leaves bind whole relations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Pat {
    /// A relation metavariable; matches and binds any subtree.
    RelVar(String),
    Filter {
        preds: String,
        input: Box<Pat>,
    },
    Map {
        scalars: String,
        input: Box<Pat>,
    },
    Project {
        outputs: String,
        input: Box<Pat>,
    },
    Reduce {
        group_key: String,
        aggregates: String,
        input: Box<Pat>,
    },
    Negate(Box<Pat>),
    Threshold(Box<Pat>),
    /// A `TopK` over `input`, matching any shape. Used by `topk_empty`; the
    /// shape is opaque so it is not bound.
    TopK(Box<Pat>),
    Join {
        equivalences: String,
        inputs: ListPat,
    },
    WcoJoin {
        equivalences: String,
        inputs: ListPat,
    },
    Union {
        inputs: ListPat,
    },
}

/// A list of child patterns, optionally ending in a `rest...` metavariable that
/// captures the remaining relations (used for variadic `Join`/`Union`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListPat {
    pub items: Vec<Pat>,
    pub rest: Option<String>,
}

/// A payload expression, used on the right-hand side to build a new payload
/// from bound metavariables.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PExpr {
    /// Reuse a bound payload metavariable verbatim.
    Var(String),
    /// Concatenate two payload lists (predicates, scalars, equivalences, …).
    Concat(Box<PExpr>, Box<PExpr>),
    /// Compose two projection lists: `compose(a, b)[i] = b[a[i]]`, i.e. apply
    /// the outer projection `a` on top of the inner projection `b`.
    Compose(Box<PExpr>, Box<PExpr>),
    /// Shift every column index in a payload by an (affine) amount. Used to
    /// move a payload across a column-offset boundary, e.g. pushing a predicate
    /// onto a join input that does not start at column 0.
    Shift(Box<PExpr>, IxExpr),
    /// Remap every column index `c` of a payload to `outs[c]`, where `outs` is
    /// a projection payload. Inverts a `Project`, e.g. pushing a predicate
    /// below a projection (`c` is a projected position; `outs[c]` is the
    /// underlying column).
    Remap(Box<PExpr>, Box<PExpr>),
    /// Turn a payload of *bare column references* (e.g. a `Reduce` group key
    /// `[#2, #0]`) into the corresponding projection `[2, 0]`. Fails if any
    /// scalar is not a single-column reference.
    ColsOf(Box<PExpr>),
    /// `iota(n)`: the identity projection `[0, 1, …, n-1]`. Builds the leading
    /// "keep all input columns" part of a projection.
    Iota(IxExpr),
}

/// An integer index expression for [`PExpr::Shift`]: literals, the arity of a
/// bound relation metavariable, and `+`/`-`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IxExpr {
    Lit(i64),
    /// The arity (column count) of a bound relation metavariable.
    Arity(String),
    Add(Box<IxExpr>, Box<IxExpr>),
    Sub(Box<IxExpr>, Box<IxExpr>),
    Neg(Box<IxExpr>),
}

/// A template: the right-hand side of a rule.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Tmpl {
    RelVar(String),
    /// The element placeholder `_` inside a `map(F[_], xs)` list combinator.
    Hole,
    /// `Empty(r)`: an empty collection with the same arity as the bound
    /// relation `r`. Used as the right-hand side of cancellation rules.
    Empty(String),
    Filter {
        preds: PExpr,
        input: Box<Tmpl>,
    },
    Map {
        scalars: PExpr,
        input: Box<Tmpl>,
    },
    Project {
        outputs: PExpr,
        input: Box<Tmpl>,
    },
    Reduce {
        group_key: PExpr,
        aggregates: PExpr,
        input: Box<Tmpl>,
    },
    Negate(Box<Tmpl>),
    Threshold(Box<Tmpl>),
    Join {
        equivalences: PExpr,
        inputs: ListTmpl,
    },
    WcoJoin {
        equivalences: PExpr,
        inputs: ListTmpl,
    },
    Union {
        inputs: ListTmpl,
    },
}

/// An element of a template input list. Lists are ordered sequences of these,
/// so multiple splices can be concatenated (e.g. flattening a nested join).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TElem {
    /// A single child template.
    Item(Tmpl),
    /// Splice a captured `rest...` list verbatim.
    Splice(String),
    /// `map(func, list)`: apply `func` (which contains the `_` [`Tmpl::Hole`])
    /// to each element of the captured `list`, splicing the results.
    MapSplice { func: Box<Tmpl>, list: String },
}

/// A template input list: an ordered sequence of [`TElem`]s.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ListTmpl {
    pub elems: Vec<TElem>,
}

/// A side condition guarding a rule.
#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)]
pub enum Cond {
    /// `uses_only_input(payload, rel)`: every column referenced by the payload
    /// metavariable is an output column of the relation metavariable. This is
    /// what makes `Filter`-through-`Map` pushdown sound (the predicate must not
    /// reference the columns the `Map` appends).
    UsesOnlyInput { payload: String, rel: String },
    /// `cols_in_range(payload, lo, hi)`: every column referenced by the payload
    /// lies in the half-open range `[lo, hi)`. Used to confirm a predicate
    /// references exactly one (offset) join input before pushing it down.
    ColsInRange {
        payload: String,
        lo: IxExpr,
        hi: IxExpr,
    },
    /// `non_negative(rel)`: the bound relation has non-negative multiplicities
    /// everywhere (conservatively: it is built without `Negate`). Lets
    /// `Threshold` be elided.
    NonNegative { rel: String },
    /// `monotonic(rel)`: the bound relation is insert-only — its multiplicities
    /// never decrease (conservatively: no `Negate` or `Reduce` on the path to
    /// its leaves), via the [`crate::eqsat::analysis::Monotonic`] analysis. The hook
    /// for monotonic *physical* rewrites (e.g. `TopK`); see `COVERAGE.md`.
    Monotonic { rel: String },
    /// `is_unique_key(payload, rel)`: the columns referenced by the payload form
    /// a unique key of the bound relation (via the [`crate::eqsat::analysis::Keys`]
    /// analysis). Lets a grouping be turned into a projection.
    IsUniqueKey { payload: String, rel: String },
    /// `empty(payload)`: the payload list is empty (e.g. a `Reduce` with no
    /// aggregates).
    Empty { payload: String },
    /// `all_true(payload)`: every scalar in the payload constant-folds to the
    /// literal `true`, so a `Filter` by it is the identity.
    /// Vacuously holds for an empty predicate list.
    AllTrue { payload: String },
    /// `any_false(payload)`: some scalar constant-folds to the literal `false`,
    /// so a `Filter` by it is empty.
    AnyFalse { payload: String },
    /// `no_false(payload)`: no scalar in the payload is a known-false literal.
    ///
    /// This is the negation of `any_false`. Used to guard distribution rules so
    /// they do not fire on predicates that `empty_false_filter` will already
    /// handle, which would otherwise create unbounded predicate-list growth via
    /// `merge_filters`.
    NoFalse { payload: String },
    /// `all_columns(payload)`: every scalar constant-folds to a bare column
    /// reference `#k`, so a `Map` of them is really a projection.
    AllColumns { payload: String },
    /// `is_rel_empty(rel)`: the bound relation is an empty constant (zero rows).
    ///
    /// Guards empty-propagation rules so they fire only when the input is
    /// already a zero-row Constant, produced by `empty_false_filter` or
    /// `union_cancel`. Avoids interaction loops: without this guard, rules like
    /// `Threshold e => Empty(e)` could fire on any non-trivial input.
    IsRelEmpty { rel: String },
    /// `not_rel_empty(rel)`: the bound relation has no zero-row Constant node
    /// in its e-class. Used to guard Union-drop rules so they only fire when
    /// the kept branch is a non-trivially-empty relation, preventing the cyclic
    /// class merges that cause `merge_filters` to grow predicate lists without
    /// bound.
    NotRelEmpty { rel: String },
    /// `unsatisfiable(rel)`: the equivalence analysis for the bound relation
    /// contains a contradiction: some equivalence class has two distinct
    /// non-error literals forced equal (e.g. `#0 = 1` and `#0 = 2` in one
    /// filter). Any relation with contradictory equivalences is empty, so it
    /// can be replaced by `Empty(rel)`.
    Unsatisfiable { rel: String },
}

/// One rewrite rule.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rule {
    pub name: String,
    pub doc: Option<String>,
    pub lhs: Pat,
    pub rhs: Tmpl,
    pub conds: Vec<Cond>,
}

/// A parsed rule file.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct RuleSet {
    pub rules: Vec<Rule>,
}

impl RuleSet {
    /// Returns the name of every rule in this set, in order.
    pub fn rule_names(&self) -> Vec<&str> {
        self.rules.iter().map(|r| r.name.as_str()).collect()
    }
}
