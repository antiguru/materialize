// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! A small **structured scalar IR** for the modeled scalar expressions.
//!
//! The rest of the crate stores a scalar as an opaque `(text, columns)` pair
//! ([`crate::ir::Scalar`]) — deliberately, so the *relational* rules never have
//! to look inside one. But several real transforms (`fold_constants`,
//! `non_null_requirements`, `literal_constraints`, …) fundamentally need to read
//! the scalar's structure. This module is the first step toward them: it parses
//! a scalar's text into an [`Expr`], constant-**folds** it, and renders it back.
//!
//! Integration (see [`crate::engine`]) is a normalization pass that folds every
//! scalar and simplifies filters whose predicates fold to a constant — the
//! relational shadow of `fold_constants`. The structured form is also the hook
//! for the deeper scalar-aware transforms; making [`Expr`] the *primary* payload
//! (and saturating scalars in their own e-graph) is the larger follow-up.

use std::collections::BTreeSet;

use crate::ir::Col;

/// A scalar literal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Lit {
    Bool(bool),
    Int(i64),
    Null,
}

/// A scalar operator (a small, modeled subset).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Not,
    Neg,
    And,
    Or,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Add,
    Sub,
}

/// A structured scalar expression.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Expr {
    /// A column reference `#n`.
    Col(Col),
    /// A literal.
    Lit(Lit),
    /// An operator applied to arguments.
    Call(Op, Vec<Expr>),
    /// A scalar we do not model structurally: its original text and the columns
    /// it reads. Folding leaves it untouched.
    Opaque(String, BTreeSet<Col>),
}

impl Expr {
    /// The columns this expression reads.
    pub fn cols(&self) -> BTreeSet<Col> {
        match self {
            Expr::Col(c) => BTreeSet::from([*c]),
            Expr::Lit(_) => BTreeSet::new(),
            Expr::Call(_, args) => args.iter().flat_map(|a| a.cols()).collect(),
            Expr::Opaque(_, cols) => cols.clone(),
        }
    }

    /// Whether this is the literal `true`.
    pub fn is_true(&self) -> bool {
        matches!(self, Expr::Lit(Lit::Bool(true)))
    }

    /// Whether this is the literal `false`.
    pub fn is_false(&self) -> bool {
        matches!(self, Expr::Lit(Lit::Bool(false)))
    }
}

/// Parse a scalar's text into an [`Expr`]. Anything outside the modeled grammar
/// becomes an [`Expr::Opaque`] (with columns scanned from the text), so this is
/// total and never loses information.
pub fn parse(text: &str) -> Expr {
    let mut p = Parser {
        toks: tokenize(text),
        pos: 0,
    };
    match p.expr() {
        Some(e) if p.pos == p.toks.len() => e,
        _ => Expr::Opaque(text.to_string(), scan_cols(text)),
    }
}

/// Constant-fold an expression: evaluate operators on literal arguments and
/// apply the obvious algebraic identities (`x && true = x`, …).
pub fn fold(e: Expr) -> Expr {
    match e {
        Expr::Call(op, args) => {
            let args: Vec<Expr> = args.into_iter().map(fold).collect();
            eval(op, &args).unwrap_or(Expr::Call(op, args))
        }
        other => other,
    }
}

/// Whether `text` constant-folds to the literal `true`.
pub fn folds_true(text: &str) -> bool {
    fold(parse(text)).is_true()
}

/// Whether `text` constant-folds to the literal `false`.
pub fn folds_false(text: &str) -> bool {
    fold(parse(text)).is_false()
}

/// If `text` constant-folds to a bare column reference `#k`, its index.
pub fn folds_col(text: &str) -> Option<Col> {
    match fold(parse(text)) {
        Expr::Col(c) => Some(c),
        _ => None,
    }
}

/// Render an expression back to text (round-trips the grammar; columns render as
/// `#n`, so bare column references stay recognizable).
pub fn render(e: &Expr) -> String {
    match e {
        Expr::Col(c) => format!("#{c}"),
        Expr::Lit(Lit::Bool(b)) => b.to_string(),
        Expr::Lit(Lit::Int(n)) => n.to_string(),
        Expr::Lit(Lit::Null) => "null".to_string(),
        Expr::Call(Op::Not, a) => format!("not {}", render(&a[0])),
        Expr::Call(Op::Neg, a) => format!("-{}", render(&a[0])),
        Expr::Call(op, a) if a.len() == 2 => {
            format!("({} {} {})", render(&a[0]), sym(*op), render(&a[1]))
        }
        Expr::Call(op, _) => format!("{op:?}"),
        Expr::Opaque(t, _) => t.clone(),
    }
}

fn sym(op: Op) -> &'static str {
    match op {
        Op::And => "and",
        Op::Or => "or",
        Op::Eq => "=",
        Op::Ne => "!=",
        Op::Lt => "<",
        Op::Le => "<=",
        Op::Gt => ">",
        Op::Ge => ">=",
        Op::Add => "+",
        Op::Sub => "-",
        Op::Not => "not",
        Op::Neg => "-",
    }
}

/// Try to evaluate `op` on already-folded `args`. Returns `None` to keep the
/// call symbolic.
fn eval(op: Op, args: &[Expr]) -> Option<Expr> {
    use Expr::Lit as L;
    use Lit::*;
    let bool_lit = |b| Some(L(Bool(b)));
    let int_lit = |n| Some(L(Int(n)));
    match (op, args) {
        // Logical identities and constants (short-circuit first).
        (Op::And, [a, b]) => {
            if a.is_false() || b.is_false() {
                bool_lit(false)
            } else if a.is_true() {
                Some(b.clone())
            } else if b.is_true() {
                Some(a.clone())
            } else {
                None
            }
        }
        (Op::Or, [a, b]) => {
            if a.is_true() || b.is_true() {
                bool_lit(true)
            } else if a.is_false() {
                Some(b.clone())
            } else if b.is_false() {
                Some(a.clone())
            } else {
                None
            }
        }
        (Op::Not, [L(Bool(b))]) => bool_lit(!b),
        (Op::Neg, [L(Int(n))]) => int_lit(-n),
        // Null propagation for the arithmetic/comparison operators.
        (_, args) if args.iter().any(|a| matches!(a, L(Null))) && op != Op::And && op != Op::Or => {
            Some(L(Null))
        }
        // Integer arithmetic and comparison on literals.
        (Op::Add, [L(Int(x)), L(Int(y))]) => int_lit(x + y),
        (Op::Sub, [L(Int(x)), L(Int(y))]) => int_lit(x - y),
        (Op::Lt, [L(Int(x)), L(Int(y))]) => bool_lit(x < y),
        (Op::Le, [L(Int(x)), L(Int(y))]) => bool_lit(x <= y),
        (Op::Gt, [L(Int(x)), L(Int(y))]) => bool_lit(x > y),
        (Op::Ge, [L(Int(x)), L(Int(y))]) => bool_lit(x >= y),
        (Op::Eq, [L(x), L(y)]) => bool_lit(x == y),
        (Op::Ne, [L(x), L(y)]) => bool_lit(x != y),
        _ => None,
    }
}

// --- tokenizer + precedence-climbing parser -------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
enum Tok {
    Col(usize),
    Int(i64),
    Word(String),
    LParen,
    RParen,
    Op(Op),
}

fn tokenize(s: &str) -> Vec<Tok> {
    let b = s.as_bytes();
    let mut i = 0;
    let mut out = Vec::new();
    while i < b.len() {
        let c = b[i] as char;
        match c {
            c if c.is_whitespace() => i += 1,
            '(' => {
                out.push(Tok::LParen);
                i += 1;
            }
            ')' => {
                out.push(Tok::RParen);
                i += 1;
            }
            '+' => {
                out.push(Tok::Op(Op::Add));
                i += 1;
            }
            '-' => {
                out.push(Tok::Op(Op::Sub));
                i += 1;
            }
            '=' => {
                out.push(Tok::Op(Op::Eq));
                i += 1;
            }
            '!' if i + 1 < b.len() && b[i + 1] == b'=' => {
                out.push(Tok::Op(Op::Ne));
                i += 2;
            }
            '<' if i + 1 < b.len() && b[i + 1] == b'=' => {
                out.push(Tok::Op(Op::Le));
                i += 2;
            }
            '>' if i + 1 < b.len() && b[i + 1] == b'=' => {
                out.push(Tok::Op(Op::Ge));
                i += 2;
            }
            '<' => {
                out.push(Tok::Op(Op::Lt));
                i += 1;
            }
            '>' => {
                out.push(Tok::Op(Op::Gt));
                i += 1;
            }
            '#' if i + 1 < b.len() && (b[i + 1] as char).is_ascii_digit() => {
                let start = i + 1;
                i += 1;
                while i < b.len() && (b[i] as char).is_ascii_digit() {
                    i += 1;
                }
                out.push(Tok::Col(s[start..i].parse().unwrap()));
            }
            c if c.is_ascii_digit() => {
                let start = i;
                while i < b.len() && (b[i] as char).is_ascii_digit() {
                    i += 1;
                }
                out.push(Tok::Int(s[start..i].parse().unwrap()));
            }
            c if c.is_alphabetic() => {
                let start = i;
                while i < b.len() && (b[i] as char).is_alphanumeric() {
                    i += 1;
                }
                out.push(Tok::Word(s[start..i].to_lowercase()));
            }
            // Anything else: bail (the whole text becomes opaque).
            _ => {
                out.push(Tok::Word("\u{0}bad".into()));
                i += 1;
            }
        }
    }
    out
}

fn scan_cols(text: &str) -> BTreeSet<Col> {
    tokenize(text)
        .into_iter()
        .filter_map(|t| match t {
            Tok::Col(c) => Some(c),
            _ => None,
        })
        .collect()
}

struct Parser {
    toks: Vec<Tok>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }

    fn eat_word(&mut self, w: &str) -> bool {
        if matches!(self.peek(), Some(Tok::Word(x)) if x == w) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expr(&mut self) -> Option<Expr> {
        self.or()
    }

    fn or(&mut self) -> Option<Expr> {
        let mut a = self.and()?;
        while self.eat_word("or") {
            let b = self.and()?;
            a = Expr::Call(Op::Or, vec![a, b]);
        }
        Some(a)
    }

    fn and(&mut self) -> Option<Expr> {
        let mut a = self.not()?;
        while self.eat_word("and") {
            let b = self.not()?;
            a = Expr::Call(Op::And, vec![a, b]);
        }
        Some(a)
    }

    fn not(&mut self) -> Option<Expr> {
        if self.eat_word("not") {
            Some(Expr::Call(Op::Not, vec![self.not()?]))
        } else {
            self.cmp()
        }
    }

    fn cmp(&mut self) -> Option<Expr> {
        let a = self.add()?;
        if let Some(Tok::Op(op @ (Op::Eq | Op::Ne | Op::Lt | Op::Le | Op::Gt | Op::Ge))) =
            self.peek().cloned()
        {
            self.pos += 1;
            let b = self.add()?;
            return Some(Expr::Call(op, vec![a, b]));
        }
        Some(a)
    }

    fn add(&mut self) -> Option<Expr> {
        let mut a = self.unary()?;
        while let Some(Tok::Op(op @ (Op::Add | Op::Sub))) = self.peek().cloned() {
            self.pos += 1;
            let b = self.unary()?;
            a = Expr::Call(op, vec![a, b]);
        }
        Some(a)
    }

    fn unary(&mut self) -> Option<Expr> {
        if matches!(self.peek(), Some(Tok::Op(Op::Sub))) {
            self.pos += 1;
            return Some(Expr::Call(Op::Neg, vec![self.unary()?]));
        }
        self.atom()
    }

    fn atom(&mut self) -> Option<Expr> {
        match self.peek().cloned()? {
            Tok::Col(c) => {
                self.pos += 1;
                Some(Expr::Col(c))
            }
            Tok::Int(n) => {
                self.pos += 1;
                Some(Expr::Lit(Lit::Int(n)))
            }
            Tok::Word(w) => {
                self.pos += 1;
                match w.as_str() {
                    "true" => Some(Expr::Lit(Lit::Bool(true))),
                    "false" => Some(Expr::Lit(Lit::Bool(false))),
                    "null" => Some(Expr::Lit(Lit::Null)),
                    _ => None,
                }
            }
            Tok::LParen => {
                self.pos += 1;
                let e = self.expr()?;
                if matches!(self.peek(), Some(Tok::RParen)) {
                    self.pos += 1;
                    Some(e)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(text: &str) -> Expr {
        fold(parse(text))
    }

    #[test]
    fn folds_constants() {
        assert_eq!(f("(1 = 1)"), Expr::Lit(Lit::Bool(true)));
        assert_eq!(f("(1 = 2)"), Expr::Lit(Lit::Bool(false)));
        assert_eq!(f("(2 + 3)"), Expr::Lit(Lit::Int(5)));
        assert_eq!(f("(#0 and false)"), Expr::Lit(Lit::Bool(false)));
        // `x and true` collapses to `x`.
        assert_eq!(f("(#0 and true)"), Expr::Col(0));
        // `(2 < 3) or #0` short-circuits to true.
        assert_eq!(f("((2 < 3) or #0)"), Expr::Lit(Lit::Bool(true)));
    }

    #[test]
    fn leaves_symbolic_and_opaque_alone() {
        // No constant to fold: unchanged, columns intact.
        let e = f("(#0 = #1)");
        assert_eq!(e, Expr::Call(Op::Eq, vec![Expr::Col(0), Expr::Col(1)]));
        assert_eq!(e.cols(), BTreeSet::from([0, 1]));
        // Unmodeled syntax round-trips as opaque, columns still scanned.
        let o = parse("sqrt(#3)");
        assert!(matches!(o, Expr::Opaque(..)));
        assert_eq!(o.cols(), BTreeSet::from([3]));
    }
}
