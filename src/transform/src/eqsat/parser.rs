// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! A hand-written tokenizer and recursive-descent parser for the rewrite DSL.
//!
//! Grammar (EBNF-ish):
//! ```text
//! ruleset  := rule*
//! rule     := "rule" ident "{" ("doc" string)? pat "=>" tmpl ("where" cond)* "}"
//! pat      := relvar | op_pat
//! op_pat   := "Filter"    "[" ident "]" child
//!           | "Map"       "[" ident "]" child
//!           | "Project"   "[" ident "]" child
//!           | "Reduce"    "[" ident "," ident "]" child
//!           | "Negate"    child
//!           | "Threshold" child
//!           | "Join"      "[" ident "]" "(" listpat ")"
//!           | "WcoJoin"   "[" ident "]" "(" listpat ")"
//!           | "Union"     "(" listpat ")"
//! child    := relvar | "(" pat ")" | op_pat
//! listpat  := (pat ("," pat)*)? ("," ident "...")?  |  ident "..."
//! tmpl     := analogous to pat, with pexpr payloads
//! pexpr    := ident | "concat" "(" pexpr "," pexpr ")" | "compose" "(" pexpr "," pexpr ")"
//! cond     := "uses_only_input" "(" ident "," ident ")"
//! ```
//! Lines beginning with `#` (after optional whitespace) and trailing `# …`
//! comments are ignored.

use crate::eqsat::dsl::*;

/// Parse a complete rule file.
pub fn parse_ruleset(src: &str) -> Result<RuleSet, String> {
    let toks = tokenize(src)?;
    let mut p = Parser { toks, pos: 0 };
    let mut rules = Vec::new();
    while !p.at_end() {
        rules.push(p.parse_rule()?);
    }
    Ok(RuleSet { rules })
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Tok {
    Ident(String),
    Str(String),
    Int(i64),
    LBrack,
    RBrack,
    LParen,
    RParen,
    LBrace,
    RBrace,
    Comma,
    Arrow,
    Ellipsis,
    Plus,
    Minus,
}

fn tokenize(src: &str) -> Result<Vec<Tok>, String> {
    let mut toks = Vec::new();
    let bytes = src.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        match c {
            '#' => {
                // Comment to end of line.
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            c if c.is_whitespace() => i += 1,
            '[' => {
                toks.push(Tok::LBrack);
                i += 1;
            }
            ']' => {
                toks.push(Tok::RBrack);
                i += 1;
            }
            '(' => {
                toks.push(Tok::LParen);
                i += 1;
            }
            ')' => {
                toks.push(Tok::RParen);
                i += 1;
            }
            '{' => {
                toks.push(Tok::LBrace);
                i += 1;
            }
            '}' => {
                toks.push(Tok::RBrace);
                i += 1;
            }
            ',' => {
                toks.push(Tok::Comma);
                i += 1;
            }
            '=' if i + 1 < bytes.len() && bytes[i + 1] == b'>' => {
                toks.push(Tok::Arrow);
                i += 2;
            }
            '.' if i + 2 < bytes.len() && bytes[i + 1] == b'.' && bytes[i + 2] == b'.' => {
                toks.push(Tok::Ellipsis);
                i += 3;
            }
            '+' => {
                toks.push(Tok::Plus);
                i += 1;
            }
            '-' => {
                toks.push(Tok::Minus);
                i += 1;
            }
            '"' => {
                let start = i + 1;
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    i += 1;
                }
                if i >= bytes.len() {
                    return Err("unterminated string literal".into());
                }
                toks.push(Tok::Str(src[start..i].to_string()));
                i += 1;
            }
            c if c.is_ascii_digit() => {
                let start = i;
                while i < bytes.len() && (bytes[i] as char).is_ascii_digit() {
                    i += 1;
                }
                let n = src[start..i]
                    .parse::<i64>()
                    .map_err(|e| format!("bad integer literal: {e}"))?;
                toks.push(Tok::Int(n));
            }
            c if c.is_alphabetic() || c == '_' => {
                let start = i;
                while i < bytes.len() {
                    let d = bytes[i] as char;
                    if d.is_alphanumeric() || d == '_' {
                        i += 1;
                    } else {
                        break;
                    }
                }
                toks.push(Tok::Ident(src[start..i].to_string()));
            }
            other => return Err(format!("unexpected character {other:?}")),
        }
    }
    Ok(toks)
}

struct Parser {
    toks: Vec<Tok>,
    pos: usize,
}

impl Parser {
    fn at_end(&self) -> bool {
        self.pos >= self.toks.len()
    }

    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }

    fn bump(&mut self) -> Result<Tok, String> {
        let t = self
            .toks
            .get(self.pos)
            .cloned()
            .ok_or_else(|| "unexpected end of input".to_string())?;
        self.pos += 1;
        Ok(t)
    }

    fn expect(&mut self, want: &Tok) -> Result<(), String> {
        let got = self.bump()?;
        if &got == want {
            Ok(())
        } else {
            Err(format!("expected {want:?}, got {got:?}"))
        }
    }

    fn ident(&mut self) -> Result<String, String> {
        match self.bump()? {
            Tok::Ident(s) => Ok(s),
            other => Err(format!("expected identifier, got {other:?}")),
        }
    }

    /// Expect a specific keyword identifier.
    fn keyword(&mut self, kw: &str) -> Result<(), String> {
        let id = self.ident()?;
        if id == kw {
            Ok(())
        } else {
            Err(format!("expected `{kw}`, got `{id}`"))
        }
    }

    fn parse_rule(&mut self) -> Result<Rule, String> {
        self.keyword("rule")?;
        let name = self.ident()?;
        self.expect(&Tok::LBrace)?;

        let mut doc = None;
        if matches!(self.peek(), Some(Tok::Ident(s)) if s == "doc") {
            self.bump()?;
            match self.bump()? {
                Tok::Str(s) => doc = Some(s),
                other => return Err(format!("expected doc string, got {other:?}")),
            }
        }

        let lhs = self.parse_pat()?;
        self.expect(&Tok::Arrow)?;
        let rhs = self.parse_tmpl()?;

        let mut conds = Vec::new();
        while matches!(self.peek(), Some(Tok::Ident(s)) if s == "where") {
            self.bump()?;
            conds.push(self.parse_cond()?);
        }

        self.expect(&Tok::RBrace)?;
        Ok(Rule {
            name,
            doc,
            lhs,
            rhs,
            conds,
        })
    }

    fn parse_cond(&mut self) -> Result<Cond, String> {
        let name = self.ident()?;
        match name.as_str() {
            "uses_only_input" => {
                self.expect(&Tok::LParen)?;
                let payload = self.ident()?;
                self.expect(&Tok::Comma)?;
                let rel = self.ident()?;
                self.expect(&Tok::RParen)?;
                Ok(Cond::UsesOnlyInput { payload, rel })
            }
            "cols_in_range" => {
                self.expect(&Tok::LParen)?;
                let payload = self.ident()?;
                self.expect(&Tok::Comma)?;
                let lo = self.parse_ixexpr()?;
                self.expect(&Tok::Comma)?;
                let hi = self.parse_ixexpr()?;
                self.expect(&Tok::RParen)?;
                Ok(Cond::ColsInRange { payload, lo, hi })
            }
            "non_negative" => {
                self.expect(&Tok::LParen)?;
                let rel = self.ident()?;
                self.expect(&Tok::RParen)?;
                Ok(Cond::NonNegative { rel })
            }
            "monotonic" => {
                self.expect(&Tok::LParen)?;
                let rel = self.ident()?;
                self.expect(&Tok::RParen)?;
                Ok(Cond::Monotonic { rel })
            }
            "is_unique_key" => {
                self.expect(&Tok::LParen)?;
                let payload = self.ident()?;
                self.expect(&Tok::Comma)?;
                let rel = self.ident()?;
                self.expect(&Tok::RParen)?;
                Ok(Cond::IsUniqueKey { payload, rel })
            }
            "empty" => {
                self.expect(&Tok::LParen)?;
                let payload = self.ident()?;
                self.expect(&Tok::RParen)?;
                Ok(Cond::Empty { payload })
            }
            "is_rel_empty" => {
                self.expect(&Tok::LParen)?;
                let rel = self.ident()?;
                self.expect(&Tok::RParen)?;
                Ok(Cond::IsRelEmpty { rel })
            }
            "not_rel_empty" => {
                self.expect(&Tok::LParen)?;
                let rel = self.ident()?;
                self.expect(&Tok::RParen)?;
                Ok(Cond::NotRelEmpty { rel })
            }
            "all_true" | "any_false" | "all_columns" | "no_false" => {
                self.expect(&Tok::LParen)?;
                let payload = self.ident()?;
                self.expect(&Tok::RParen)?;
                Ok(match name.as_str() {
                    "all_true" => Cond::AllTrue { payload },
                    "any_false" => Cond::AnyFalse { payload },
                    "no_false" => Cond::NoFalse { payload },
                    _ => Cond::AllColumns { payload },
                })
            }
            other => Err(format!("unknown side condition `{other}`")),
        }
    }

    /// Parse an index expression: `term (('+' | '-') term)*`.
    fn parse_ixexpr(&mut self) -> Result<IxExpr, String> {
        let mut acc = self.parse_ixterm()?;
        loop {
            match self.peek() {
                Some(Tok::Plus) => {
                    self.bump()?;
                    acc = IxExpr::Add(Box::new(acc), Box::new(self.parse_ixterm()?));
                }
                Some(Tok::Minus) => {
                    self.bump()?;
                    acc = IxExpr::Sub(Box::new(acc), Box::new(self.parse_ixterm()?));
                }
                _ => break,
            }
        }
        Ok(acc)
    }

    fn parse_ixterm(&mut self) -> Result<IxExpr, String> {
        match self.peek().cloned() {
            Some(Tok::Minus) => {
                self.bump()?;
                Ok(IxExpr::Neg(Box::new(self.parse_ixterm()?)))
            }
            Some(Tok::Int(n)) => {
                self.bump()?;
                Ok(IxExpr::Lit(n))
            }
            Some(Tok::Ident(name)) if name == "arity" => {
                self.bump()?;
                self.expect(&Tok::LParen)?;
                let rel = self.ident()?;
                self.expect(&Tok::RParen)?;
                Ok(IxExpr::Arity(rel))
            }
            Some(Tok::LParen) => {
                self.bump()?;
                let e = self.parse_ixexpr()?;
                self.expect(&Tok::RParen)?;
                Ok(e)
            }
            other => Err(format!("expected index expression, got {other:?}")),
        }
    }

    // --- patterns ---------------------------------------------------------

    fn parse_pat(&mut self) -> Result<Pat, String> {
        match self.peek().cloned() {
            Some(Tok::LParen) => {
                self.bump()?;
                let p = self.parse_pat()?;
                self.expect(&Tok::RParen)?;
                Ok(p)
            }
            Some(Tok::Ident(name)) => {
                if is_operator(&name) {
                    self.parse_op_pat(&name)
                } else {
                    self.bump()?;
                    Ok(Pat::RelVar(name))
                }
            }
            other => Err(format!("expected pattern, got {other:?}")),
        }
    }

    fn parse_op_pat(&mut self, name: &str) -> Result<Pat, String> {
        self.bump()?; // operator ident
        match name {
            "Filter" => {
                let preds = self.bracket_ident()?;
                let input = Box::new(self.parse_pat()?);
                Ok(Pat::Filter { preds, input })
            }
            "Map" => {
                let scalars = self.bracket_ident()?;
                let input = Box::new(self.parse_pat()?);
                Ok(Pat::Map { scalars, input })
            }
            "Project" => {
                let outputs = self.bracket_ident()?;
                let input = Box::new(self.parse_pat()?);
                Ok(Pat::Project { outputs, input })
            }
            "Reduce" => {
                self.expect(&Tok::LBrack)?;
                let group_key = self.ident()?;
                self.expect(&Tok::Comma)?;
                let aggregates = self.ident()?;
                self.expect(&Tok::RBrack)?;
                let input = Box::new(self.parse_pat()?);
                Ok(Pat::Reduce {
                    group_key,
                    aggregates,
                    input,
                })
            }
            "Negate" => Ok(Pat::Negate(Box::new(self.parse_pat()?))),
            "Threshold" => Ok(Pat::Threshold(Box::new(self.parse_pat()?))),
            "TopK" => Ok(Pat::TopK(Box::new(self.parse_pat()?))),
            "Join" => {
                let equivalences = self.bracket_ident()?;
                self.expect(&Tok::LParen)?;
                let inputs = self.parse_listpat()?;
                self.expect(&Tok::RParen)?;
                Ok(Pat::Join {
                    equivalences,
                    inputs,
                })
            }
            "WcoJoin" => {
                let equivalences = self.bracket_ident()?;
                self.expect(&Tok::LParen)?;
                let inputs = self.parse_listpat()?;
                self.expect(&Tok::RParen)?;
                Ok(Pat::WcoJoin {
                    equivalences,
                    inputs,
                })
            }
            "Union" => {
                self.expect(&Tok::LParen)?;
                let inputs = self.parse_listpat()?;
                self.expect(&Tok::RParen)?;
                Ok(Pat::Union { inputs })
            }
            other => Err(format!("unknown operator `{other}`")),
        }
    }

    fn parse_listpat(&mut self) -> Result<ListPat, String> {
        let mut items = Vec::new();
        let mut rest = None;
        if matches!(self.peek(), Some(Tok::RParen)) {
            return Ok(ListPat { items, rest });
        }
        loop {
            // A `rest...` element.
            if let Some(Tok::Ident(name)) = self.peek().cloned() {
                if !is_operator(&name) && self.toks.get(self.pos + 1) == Some(&Tok::Ellipsis) {
                    self.bump()?; // ident
                    self.bump()?; // ellipsis
                    rest = Some(name);
                    break;
                }
            }
            items.push(self.parse_pat()?);
            if matches!(self.peek(), Some(Tok::Comma)) {
                self.bump()?;
            } else {
                break;
            }
        }
        Ok(ListPat { items, rest })
    }

    fn bracket_ident(&mut self) -> Result<String, String> {
        self.expect(&Tok::LBrack)?;
        let id = self.ident()?;
        self.expect(&Tok::RBrack)?;
        Ok(id)
    }

    // --- templates --------------------------------------------------------

    fn parse_tmpl(&mut self) -> Result<Tmpl, String> {
        match self.peek().cloned() {
            Some(Tok::LParen) => {
                self.bump()?;
                let t = self.parse_tmpl()?;
                self.expect(&Tok::RParen)?;
                Ok(t)
            }
            Some(Tok::Ident(name)) => {
                if is_operator(&name) {
                    self.parse_op_tmpl(&name)
                } else if name == "_" {
                    self.bump()?;
                    Ok(Tmpl::Hole)
                } else {
                    self.bump()?;
                    Ok(Tmpl::RelVar(name))
                }
            }
            other => Err(format!("expected template, got {other:?}")),
        }
    }

    fn parse_op_tmpl(&mut self, name: &str) -> Result<Tmpl, String> {
        self.bump()?;
        match name {
            "Filter" => {
                let preds = self.bracket_pexpr()?;
                let input = Box::new(self.parse_tmpl()?);
                Ok(Tmpl::Filter { preds, input })
            }
            "Map" => {
                let scalars = self.bracket_pexpr()?;
                let input = Box::new(self.parse_tmpl()?);
                Ok(Tmpl::Map { scalars, input })
            }
            "Project" => {
                let outputs = self.bracket_pexpr()?;
                let input = Box::new(self.parse_tmpl()?);
                Ok(Tmpl::Project { outputs, input })
            }
            "Reduce" => {
                self.expect(&Tok::LBrack)?;
                let group_key = self.parse_pexpr()?;
                self.expect(&Tok::Comma)?;
                let aggregates = self.parse_pexpr()?;
                self.expect(&Tok::RBrack)?;
                let input = Box::new(self.parse_tmpl()?);
                Ok(Tmpl::Reduce {
                    group_key,
                    aggregates,
                    input,
                })
            }
            "Negate" => Ok(Tmpl::Negate(Box::new(self.parse_tmpl()?))),
            "Threshold" => Ok(Tmpl::Threshold(Box::new(self.parse_tmpl()?))),
            "Join" => {
                let equivalences = self.bracket_pexpr()?;
                self.expect(&Tok::LParen)?;
                let inputs = self.parse_listtmpl()?;
                self.expect(&Tok::RParen)?;
                Ok(Tmpl::Join {
                    equivalences,
                    inputs,
                })
            }
            "WcoJoin" => {
                let equivalences = self.bracket_pexpr()?;
                self.expect(&Tok::LParen)?;
                let inputs = self.parse_listtmpl()?;
                self.expect(&Tok::RParen)?;
                Ok(Tmpl::WcoJoin {
                    equivalences,
                    inputs,
                })
            }
            "Union" => {
                self.expect(&Tok::LParen)?;
                let inputs = self.parse_listtmpl()?;
                self.expect(&Tok::RParen)?;
                Ok(Tmpl::Union { inputs })
            }
            "Empty" => {
                self.expect(&Tok::LParen)?;
                let rel = self.ident()?;
                self.expect(&Tok::RParen)?;
                Ok(Tmpl::Empty(rel))
            }
            other => Err(format!("unknown operator `{other}`")),
        }
    }

    fn parse_listtmpl(&mut self) -> Result<ListTmpl, String> {
        let mut elems = Vec::new();
        if matches!(self.peek(), Some(Tok::RParen)) {
            return Ok(ListTmpl { elems });
        }
        loop {
            elems.push(self.parse_telem()?);
            if matches!(self.peek(), Some(Tok::Comma)) {
                self.bump()?;
            } else {
                break;
            }
        }
        Ok(ListTmpl { elems })
    }

    fn parse_telem(&mut self) -> Result<TElem, String> {
        // `xs...` — splice a captured rest list.
        if let Some(Tok::Ident(name)) = self.peek().cloned() {
            if !is_operator(&name) && self.toks.get(self.pos + 1) == Some(&Tok::Ellipsis) {
                self.bump()?;
                self.bump()?;
                return Ok(TElem::Splice(name));
            }
            // `map(func, list)` — element-wise transform of a rest list.
            if name == "map" && self.toks.get(self.pos + 1) == Some(&Tok::LParen) {
                self.bump()?; // map
                self.bump()?; // (
                let func = Box::new(self.parse_tmpl()?);
                self.expect(&Tok::Comma)?;
                let list = self.ident()?;
                self.expect(&Tok::RParen)?;
                return Ok(TElem::MapSplice { func, list });
            }
        }
        Ok(TElem::Item(self.parse_tmpl()?))
    }

    fn bracket_pexpr(&mut self) -> Result<PExpr, String> {
        self.expect(&Tok::LBrack)?;
        let e = self.parse_pexpr()?;
        self.expect(&Tok::RBrack)?;
        Ok(e)
    }

    fn parse_pexpr(&mut self) -> Result<PExpr, String> {
        let id = self.ident()?;
        match id.as_str() {
            "concat" | "compose" => {
                self.expect(&Tok::LParen)?;
                let a = Box::new(self.parse_pexpr()?);
                self.expect(&Tok::Comma)?;
                let b = Box::new(self.parse_pexpr()?);
                self.expect(&Tok::RParen)?;
                Ok(if id == "concat" {
                    PExpr::Concat(a, b)
                } else {
                    PExpr::Compose(a, b)
                })
            }
            "shift" => {
                self.expect(&Tok::LParen)?;
                let p = Box::new(self.parse_pexpr()?);
                self.expect(&Tok::Comma)?;
                let k = self.parse_ixexpr()?;
                self.expect(&Tok::RParen)?;
                Ok(PExpr::Shift(p, k))
            }
            "remap" => {
                self.expect(&Tok::LParen)?;
                let p = Box::new(self.parse_pexpr()?);
                self.expect(&Tok::Comma)?;
                let outs = Box::new(self.parse_pexpr()?);
                self.expect(&Tok::RParen)?;
                Ok(PExpr::Remap(p, outs))
            }
            "cols_of" => {
                self.expect(&Tok::LParen)?;
                let p = Box::new(self.parse_pexpr()?);
                self.expect(&Tok::RParen)?;
                Ok(PExpr::ColsOf(p))
            }
            "iota" => {
                self.expect(&Tok::LParen)?;
                let n = self.parse_ixexpr()?;
                self.expect(&Tok::RParen)?;
                Ok(PExpr::Iota(n))
            }
            _ => Ok(PExpr::Var(id)),
        }
    }
}

/// Operator keywords start with an uppercase letter; metavariables do not.
fn is_operator(name: &str) -> bool {
    name.chars().next().is_some_and(|c| c.is_uppercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn parses_merge_filters() {
        let src = r#"
            rule merge_filters {
                doc "two filters become one"
                Filter[p] (Filter[q] r) => Filter[concat(q, p)] r
            }
        "#;
        let rs = parse_ruleset(src).unwrap();
        assert_eq!(rs.rules.len(), 1);
        assert_eq!(rs.rules[0].name, "merge_filters");
        assert_eq!(rs.rules[0].doc.as_deref(), Some("two filters become one"));
    }

    #[mz_ore::test]
    fn parses_join_with_rest_and_condition() {
        let src = r#"
            rule to_wcoj {
                Join[e](rs...) => WcoJoin[e](rs...)
            }
            rule push {
                Filter[p] (Map[s] r) => Map[s] (Filter[p] r)
                where uses_only_input(p, r)
            }
        "#;
        let rs = parse_ruleset(src).unwrap();
        assert_eq!(rs.rules.len(), 2);
        assert_eq!(rs.rules[1].conds.len(), 1);
    }
}
