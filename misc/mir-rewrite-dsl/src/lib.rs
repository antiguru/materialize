// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! A DSL for cost-oriented, equality-preserving rewrites of (a subset of) MIR
//! relational expressions, the optimizer generated from it, and a Lean 4
//! specification that validates the rewrites.
//!
//! The pipeline, all driven from one rule file (`rules/relational.rewrite`):
//!
//! ```text
//!   rules/relational.rewrite  (the DSL — the single source of truth)
//!            │
//!     parser::parse_ruleset
//!            ▼
//!        dsl::RuleSet
//!          ╱        ╲
//!  engine::Optimizer   lean::emit_lean
//!  (e-graph saturation     (one theorem per rule,
//!   + WCOJ e-matching        checked in lean/)
//!   + abstract cost
//!     extraction)
//! ```
//!
//! See `README.md` for the design rationale and the worst-case-optimal-join
//! story (both as a *plan* — [`ir::Rel::WcoJoin`] — and as the *search
//! strategy* — generic-join e-matching in [`egraph`]).

// This crate is a self-contained workspace, not a member of the core `mz-*`
// workspace, so the repo-wide bans on `std` hash collections and `Iterator::zip`
// (which exist to enforce determinism conventions elsewhere) do not apply here.
#![allow(clippy::disallowed_types, clippy::disallowed_methods)]

pub mod analysis;
pub mod cost;
pub mod cse;
pub mod dsl;
pub mod egraph;
pub mod engine;
pub mod ir;
pub mod lean;
pub mod matcher;
pub mod parser;
pub mod scalar;

/// The built-in rule file, embedded at compile time.
pub const RULES_SRC: &str = include_str!("../rules/relational.rewrite");

/// Parse the built-in rule set. Panics if the embedded rules fail to parse
/// (a build-time invariant, exercised by the tests).
pub fn default_ruleset() -> dsl::RuleSet {
    parser::parse_ruleset(RULES_SRC).expect("built-in rules must parse")
}

/// Build the default saturating optimizer (abstract cost model + built-in
/// rules).
pub fn default_optimizer() -> engine::Optimizer {
    engine::Optimizer::new(default_ruleset(), cost::CostModel::new())
}
