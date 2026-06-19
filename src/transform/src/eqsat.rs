// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Equality-saturation rewrites over a subset of MIR relational expressions.
//! See `docs/superpowers/specs/2026-06-19-mir-egraph-saturation-pass-design.md`.

// This module ports a self-contained prototype; the repo-wide bans on std hash
// collections and `Iterator::zip` (determinism conventions elsewhere) do not
// apply to the ported engine. The ported modules also use numeric `as` casts
// pervasively; rewriting them would obscure the prototype's intent. The crate's
// `missing_docs`/`missing_debug_implementations` lints likewise do not fit the
// ported code, so they are relaxed for this module subtree.
#![allow(
    clippy::disallowed_types,
    clippy::disallowed_methods,
    clippy::as_conversions,
    missing_docs,
    missing_debug_implementations
)]

pub mod analysis;
pub mod cost;
pub mod cse;
pub mod dsl;
pub mod egraph;
pub mod engine;
pub mod ir;
pub mod lower;
pub mod matcher;
pub mod parser;
pub mod raise;
pub mod transform;

pub use transform::EqSatTransform;

use mz_expr::MirRelationExpr;

/// The built-in rule file, embedded at compile time.
pub const RULES_SRC: &str = include_str!("eqsat/rules/relational.rewrite");

/// Parse the built-in rule set, panicking on a malformed embedded file.
pub fn default_ruleset() -> dsl::RuleSet {
    parser::parse_ruleset(RULES_SRC).expect("built-in rules must parse")
}

/// Optimize `expr` by equality saturation over the supported relational subset,
/// bailing per-subtree on unsupported variants. Functionally equivalent output.
pub fn optimize(expr: MirRelationExpr) -> MirRelationExpr {
    let rel = lower::lower(&expr);
    let optimizer = engine::Optimizer::new(default_ruleset(), cost::CostModel::new());
    let best = optimizer.optimize(rel).plan;
    // The equivalence-preserving arity guard lives at the live transform
    // boundary (`EqSatTransform`), which adopts this output only if its arity
    // matches the input. Direct test callers assert arity themselves.
    raise::raise(&best)
}
