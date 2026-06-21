// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! The compiled rewrite rules, generated at build time from
//! `eqsat/rules/relational.rewrite`.
//!
//! `build.rs` parses the rule file with a [`chumsky`](https://crates.io/crates/chumsky)
//! grammar and emits this module's body into `$OUT_DIR/eqsat_rules.rs`. Nothing
//! parses the rule file at run time: each rule becomes a pair of generated
//! functions, a `find` that enumerates left-hand-side matches (checking side
//! conditions) and an `apply` that instantiates the right-hand side. The
//! generated `rules_ast()` reconstructs the rule set as AST literals for the
//! Lean emitter (`super::lean`).

// The generated matchers favor uniformity over the lints the rest of the crate
// observes; they are machine-written, not hand-tuned.
#![allow(clippy::all, unused)]

use crate::eqsat::dsl::Phase;
use crate::eqsat::egraph::{
    Analyses, EBindings, EGraph, ENode, Id, Index, Sym, cond_all_columns, cond_all_true,
    cond_any_false, cond_cols_in_range, cond_no_false, cond_uses_only_input,
};
use crate::eqsat::matcher::{
    Payload, cols_of_payload, compose_payload, concat_payload, iota_payload, remap_payload,
    shift_payload,
};

/// One rewrite rule, compiled to a matcher (`find`) and an instantiator
/// (`apply`). Both are functions generated from `relational.rewrite`.
#[derive(Clone, Copy)]
pub struct CompiledRule {
    /// The rule's name (for `rule_names` and diagnostics).
    pub name: &'static str,
    /// The eqsat pass(es) the rule is active in.
    pub phase: Phase,
    /// Enumerate every left-hand-side match in the e-graph that also satisfies
    /// the side conditions, up to `limit` matches. The `bool` is `true` when
    /// the cap was reached (so the caller can throttle an explosive rule).
    pub(crate) find: fn(&EGraph, &Index, &Analyses, usize) -> (Vec<EBindings>, bool),
    /// Instantiate the right-hand side for a match, returning the new e-class.
    pub(crate) apply: fn(&mut EGraph, &EBindings) -> Result<Id, String>,
}

impl std::fmt::Debug for CompiledRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledRule")
            .field("name", &self.name)
            .field("phase", &self.phase)
            .finish_non_exhaustive()
    }
}

/// A set of compiled rules, the run-time replacement for the former AST
/// `dsl::RuleSet`.
#[derive(Clone, Debug)]
pub struct CompiledRuleSet {
    rules: Vec<&'static CompiledRule>,
}

impl CompiledRuleSet {
    /// The compiled rules, in declaration order.
    pub(crate) fn rules(&self) -> &[&'static CompiledRule] {
        &self.rules
    }

    /// The name of every rule in this set, in order.
    pub fn rule_names(&self) -> Vec<&'static str> {
        self.rules.iter().map(|r| r.name).collect()
    }

    /// The rules active in `phase`: those declared for that phase or for `Both`.
    pub fn for_phase(&self, phase: Phase) -> CompiledRuleSet {
        CompiledRuleSet {
            rules: self
                .rules
                .iter()
                .copied()
                .filter(|r| r.phase == Phase::Both || r.phase == phase)
                .collect(),
        }
    }
}

/// The full built-in rule set.
pub fn all() -> CompiledRuleSet {
    CompiledRuleSet {
        rules: COMPILED_RULES.iter().collect(),
    }
}

include!(concat!(env!("OUT_DIR"), "/eqsat_rules.rs"));

#[cfg(test)]
mod tests {
    /// The two generated backends (compiled `COMPILED_RULES` and the AST
    /// `rules_ast()`) come from the same source, so their rule names and phases
    /// must agree.
    #[mz_ore::test]
    fn compiled_and_ast_agree() {
        let ast = super::rules_ast();
        let compiled = super::all();
        let ast_names: Vec<&str> = ast.rules.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(compiled.rule_names(), ast_names);
        for (c, a) in super::COMPILED_RULES.iter().zip(&ast.rules) {
            assert_eq!(c.name, a.name);
            assert_eq!(c.phase, a.phase, "phase mismatch for {}", c.name);
        }
    }
}
