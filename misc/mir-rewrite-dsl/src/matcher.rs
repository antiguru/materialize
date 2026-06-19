// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Matching a [`Pat`] against a [`Rel`], checking side conditions, and
//! instantiating a [`Tmpl`]. Together these turn a parsed [`Rule`] into a
//! function `Rel -> Option<Rel>` that rewrites the *root* of a relation (the
//! traversal that applies it everywhere lives in [`crate::engine`]).

use std::collections::BTreeMap;

use crate::dsl::*;
use crate::ir::{Col, Rel, Scalar};

/// The payload captured by a metavariable. The variant records which operator
/// the payload came from so the template can rebuild a well-typed operator.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Payload {
    Predicates(Vec<Scalar>),
    Scalars(Vec<Scalar>),
    Outputs(Vec<Col>),
    Equivalences(Vec<Vec<Scalar>>),
    GroupKey(Vec<Scalar>),
    Aggregates(Vec<Scalar>),
}

impl Payload {
    /// The number of list elements in the payload.
    pub fn len(&self) -> usize {
        match self {
            Payload::Predicates(s)
            | Payload::Scalars(s)
            | Payload::GroupKey(s)
            | Payload::Aggregates(s) => s.len(),
            Payload::Outputs(o) => o.len(),
            Payload::Equivalences(c) => c.len(),
        }
    }

    /// Whether the payload list is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The underlying scalar list, for the scalar-bearing payload kinds.
    pub fn scalars(&self) -> Option<&[Scalar]> {
        match self {
            Payload::Predicates(s)
            | Payload::Scalars(s)
            | Payload::GroupKey(s)
            | Payload::Aggregates(s) => Some(s),
            Payload::Outputs(_) | Payload::Equivalences(_) => None,
        }
    }

    /// All columns referenced by this payload (used by side conditions).
    pub fn columns(&self) -> Vec<Col> {
        match self {
            Payload::Predicates(s)
            | Payload::Scalars(s)
            | Payload::GroupKey(s)
            | Payload::Aggregates(s) => s.iter().flat_map(|x| x.cols.iter().copied()).collect(),
            Payload::Outputs(o) => o.clone(),
            Payload::Equivalences(classes) => classes
                .iter()
                .flat_map(|c| c.iter().flat_map(|x| x.cols.iter().copied()))
                .collect(),
        }
    }
}

/// The substitution produced by a successful match.
#[derive(Clone, Debug, Default)]
pub struct Bindings {
    rels: BTreeMap<String, Rel>,
    payloads: BTreeMap<String, Payload>,
    rests: BTreeMap<String, Vec<Rel>>,
}

impl Bindings {
    fn bind_rel(&mut self, name: &str, rel: &Rel) -> bool {
        match self.rels.get(name) {
            Some(existing) => existing == rel,
            None => {
                self.rels.insert(name.to_string(), rel.clone());
                true
            }
        }
    }

    fn bind_payload(&mut self, name: &str, p: Payload) -> bool {
        match self.payloads.get(name) {
            Some(existing) => existing == &p,
            None => {
                self.payloads.insert(name.to_string(), p);
                true
            }
        }
    }
}

/// Try to match `pat` against `rel`, extending `b`. Returns false (leaving `b`
/// in an unspecified-but-safe state) on failure; callers discard `b` on false.
pub fn match_pat(pat: &Pat, rel: &Rel, b: &mut Bindings) -> bool {
    match (pat, rel) {
        (Pat::RelVar(name), _) => b.bind_rel(name, rel),
        (
            Pat::Filter { preds, input },
            Rel::Filter {
                predicates,
                input: ri,
            },
        ) => {
            b.bind_payload(preds, Payload::Predicates(predicates.clone()))
                && match_pat(input, ri, b)
        }
        (
            Pat::Map { scalars, input },
            Rel::Map {
                scalars: rs,
                input: ri,
            },
        ) => b.bind_payload(scalars, Payload::Scalars(rs.clone())) && match_pat(input, ri, b),
        (
            Pat::Project { outputs, input },
            Rel::Project {
                outputs: ro,
                input: ri,
            },
        ) => b.bind_payload(outputs, Payload::Outputs(ro.clone())) && match_pat(input, ri, b),
        (
            Pat::Reduce {
                group_key,
                aggregates,
                input,
            },
            Rel::Reduce {
                group_key: rk,
                aggregates: ra,
                input: ri,
            },
        ) => {
            b.bind_payload(group_key, Payload::GroupKey(rk.clone()))
                && b.bind_payload(aggregates, Payload::Aggregates(ra.clone()))
                && match_pat(input, ri, b)
        }
        (Pat::Negate(p), Rel::Negate { input }) => match_pat(p, input, b),
        (Pat::Threshold(p), Rel::Threshold { input }) => match_pat(p, input, b),
        (
            Pat::Join {
                equivalences,
                inputs,
            },
            Rel::Join {
                equivalences: re,
                inputs: ri,
            },
        ) => {
            b.bind_payload(equivalences, Payload::Equivalences(re.clone()))
                && match_listpat(inputs, ri, b)
        }
        (
            Pat::WcoJoin {
                equivalences,
                inputs,
            },
            Rel::WcoJoin {
                equivalences: re,
                inputs: ri,
            },
        ) => {
            b.bind_payload(equivalences, Payload::Equivalences(re.clone()))
                && match_listpat(inputs, ri, b)
        }
        (Pat::Union { inputs }, Rel::Union { base, inputs: ui }) => {
            let mut flat = vec![(**base).clone()];
            flat.extend(ui.iter().cloned());
            match_listpat(inputs, &flat, b)
        }
        _ => false,
    }
}

fn match_listpat(pat: &ListPat, rels: &[Rel], b: &mut Bindings) -> bool {
    match &pat.rest {
        None => {
            if pat.items.len() != rels.len() {
                return false;
            }
            pat.items.iter().zip(rels).all(|(p, r)| match_pat(p, r, b))
        }
        Some(rest) => {
            if rels.len() < pat.items.len() {
                return false;
            }
            for (p, r) in pat.items.iter().zip(rels) {
                if !match_pat(p, r, b) {
                    return false;
                }
            }
            let remainder = rels[pat.items.len()..].to_vec();
            match b.rests.get(rest) {
                Some(existing) => existing == &remainder,
                None => {
                    b.rests.insert(rest.clone(), remainder);
                    true
                }
            }
        }
    }
}

impl Bindings {
    /// The arities of the bound relation metavariables (for index expressions
    /// and column-range conditions).
    fn arities(&self) -> BTreeMap<String, usize> {
        self.rels
            .iter()
            .map(|(n, r)| (n.clone(), r.arity()))
            .collect()
    }
}

/// Check a rule's side conditions against a successful match.
pub fn check_conds(conds: &[Cond], b: &Bindings) -> bool {
    let arities = b.arities();
    conds.iter().all(|c| match c {
        Cond::UsesOnlyInput { payload, rel } => {
            let (Some(p), Some(r)) = (b.payloads.get(payload), b.rels.get(rel)) else {
                return false;
            };
            let arity = r.arity();
            p.columns().into_iter().all(|c| c < arity)
        }
        Cond::ColsInRange { payload, lo, hi } => {
            let Some(p) = b.payloads.get(payload) else {
                return false;
            };
            let (Ok(lo), Ok(hi)) = (eval_ixexpr(lo, &arities), eval_ixexpr(hi, &arities)) else {
                return false;
            };
            p.columns()
                .into_iter()
                .all(|c| (c as i64) >= lo && (c as i64) < hi)
        }
        Cond::NonNegative { rel } => b
            .rels
            .get(rel)
            .is_some_and(crate::analysis::rel_non_negative),
        Cond::Monotonic { rel } => b
            .rels
            .get(rel)
            .is_some_and(crate::analysis::rel_monotonic),
        Cond::IsUniqueKey { payload, rel } => {
            let (Some(p), Some(r)) = (b.payloads.get(payload), b.rels.get(rel)) else {
                return false;
            };
            let cand = p.columns().into_iter().collect();
            crate::analysis::is_superkey(&crate::analysis::rel_keys(r), &cand)
        }
        Cond::Empty { payload } => b.payloads.get(payload).is_some_and(|p| p.is_empty()),
        Cond::AllTrue { payload } => scalar_payload_all(b, payload, crate::scalar::folds_true),
        Cond::AnyFalse { payload } => scalar_payload_any(b, payload, crate::scalar::folds_false),
        Cond::AllColumns { payload } => {
            scalar_payload_all(b, payload, |t| crate::scalar::folds_col(t).is_some())
        }
    })
}

/// Whether every scalar in a bound payload satisfies `pred` (vacuously true for
/// an empty list; false if the metavariable is unbound or not scalar-bearing).
fn scalar_payload_all(b: &Bindings, payload: &str, pred: impl Fn(&str) -> bool) -> bool {
    b.payloads
        .get(payload)
        .and_then(|p| p.scalars())
        .is_some_and(|xs| xs.iter().all(|s| pred(&s.text)))
}

/// Whether some scalar in a bound payload satisfies `pred`.
fn scalar_payload_any(b: &Bindings, payload: &str, pred: impl Fn(&str) -> bool) -> bool {
    b.payloads
        .get(payload)
        .and_then(|p| p.scalars())
        .is_some_and(|xs| xs.iter().any(|s| pred(&s.text)))
}

/// Instantiate a template against a set of bindings, producing the rewritten
/// relation. `hole` is the current element bound to `_` inside a `map(...)`
/// list combinator (if any). Returns an error string if the template
/// references an unbound metavariable or combines payloads of mismatched kinds.
pub fn instantiate(tmpl: &Tmpl, b: &Bindings, hole: Option<&Rel>) -> Result<Rel, String> {
    let arities = b.arities();
    instantiate_inner(tmpl, b, hole, &arities)
}

fn instantiate_inner(
    tmpl: &Tmpl,
    b: &Bindings,
    hole: Option<&Rel>,
    arities: &BTreeMap<String, usize>,
) -> Result<Rel, String> {
    let go = |t: &Tmpl| instantiate_inner(t, b, hole, arities);
    let ev = |e: &PExpr| eval_pexpr(e, &b.payloads, arities);
    Ok(match tmpl {
        Tmpl::RelVar(name) => b
            .rels
            .get(name)
            .cloned()
            .ok_or_else(|| format!("unbound relation metavariable `{name}`"))?,
        Tmpl::Hole => hole
            .cloned()
            .ok_or_else(|| "`_` used outside a map(...) combinator".to_string())?,
        Tmpl::Empty(name) => {
            let arity = *arities
                .get(name)
                .ok_or_else(|| format!("Empty of unbound relation `{name}`"))?;
            Rel::Constant { card: 0, arity }
        }
        Tmpl::Filter { preds, input } => Rel::Filter {
            predicates: ev(preds)?.into_predicates()?,
            input: Box::new(go(input)?),
        },
        Tmpl::Map { scalars, input } => Rel::Map {
            scalars: ev(scalars)?.into_scalars()?,
            input: Box::new(go(input)?),
        },
        Tmpl::Project { outputs, input } => Rel::Project {
            outputs: ev(outputs)?.into_outputs()?,
            input: Box::new(go(input)?),
        },
        Tmpl::Reduce {
            group_key,
            aggregates,
            input,
        } => Rel::Reduce {
            group_key: ev(group_key)?.into_group_key()?,
            aggregates: ev(aggregates)?.into_aggregates()?,
            input: Box::new(go(input)?),
        },
        Tmpl::Negate(t) => Rel::Negate {
            input: Box::new(go(t)?),
        },
        Tmpl::Threshold(t) => Rel::Threshold {
            input: Box::new(go(t)?),
        },
        Tmpl::Join {
            equivalences,
            inputs,
        } => Rel::Join {
            equivalences: ev(equivalences)?.into_equivalences()?,
            inputs: instantiate_list(inputs, b, hole, arities)?,
        },
        Tmpl::WcoJoin {
            equivalences,
            inputs,
        } => Rel::WcoJoin {
            equivalences: ev(equivalences)?.into_equivalences()?,
            inputs: instantiate_list(inputs, b, hole, arities)?,
        },
        Tmpl::Union { inputs } => {
            let mut flat = instantiate_list(inputs, b, hole, arities)?;
            if flat.is_empty() {
                return Err("Union template produced no inputs".into());
            }
            let base = Box::new(flat.remove(0));
            Rel::Union { base, inputs: flat }
        }
    })
}

fn instantiate_list(
    list: &ListTmpl,
    b: &Bindings,
    hole: Option<&Rel>,
    arities: &BTreeMap<String, usize>,
) -> Result<Vec<Rel>, String> {
    let mut out = Vec::new();
    for elem in &list.elems {
        match elem {
            TElem::Item(t) => out.push(instantiate_inner(t, b, hole, arities)?),
            TElem::Splice(name) => {
                let extra = b
                    .rests
                    .get(name)
                    .ok_or_else(|| format!("unbound rest metavariable `{name}`"))?;
                out.extend(extra.iter().cloned());
            }
            TElem::MapSplice { func, list: name } => {
                let elems = b
                    .rests
                    .get(name)
                    .ok_or_else(|| format!("unbound rest metavariable `{name}`"))?
                    .clone();
                for e in &elems {
                    out.push(instantiate_inner(func, b, Some(e), arities)?);
                }
            }
        }
    }
    Ok(out)
}

/// Evaluate a payload expression against a payload environment and the arities
/// of the bound relation metavariables (needed by `shift`'s index
/// expressions). Shared by the `Rel`-based instantiation here and the e-graph
/// instantiation in [`crate::egraph`].
pub fn eval_pexpr(
    e: &PExpr,
    payloads: &BTreeMap<String, Payload>,
    arities: &BTreeMap<String, usize>,
) -> Result<Payload, String> {
    match e {
        PExpr::Var(name) => payloads
            .get(name)
            .cloned()
            .ok_or_else(|| format!("unbound payload metavariable `{name}`")),
        PExpr::Concat(a, c) => concat_payload(
            eval_pexpr(a, payloads, arities)?,
            eval_pexpr(c, payloads, arities)?,
        ),
        PExpr::Compose(a, c) => compose_payload(
            eval_pexpr(a, payloads, arities)?,
            eval_pexpr(c, payloads, arities)?,
        ),
        PExpr::Shift(p, k) => {
            let k = eval_ixexpr(k, arities)?;
            shift_payload(eval_pexpr(p, payloads, arities)?, k)
        }
        PExpr::Remap(p, outs) => {
            let outs = eval_pexpr(outs, payloads, arities)?.into_outputs()?;
            remap_payload(eval_pexpr(p, payloads, arities)?, &outs)
        }
        PExpr::ColsOf(p) => cols_of_payload(eval_pexpr(p, payloads, arities)?),
        PExpr::Iota(n) => {
            let n = eval_ixexpr(n, arities)?;
            if n < 0 {
                return Err("iota of negative length".into());
            }
            Ok(Payload::Outputs((0..n as usize).collect()))
        }
    }
}

/// Turn a payload of bare column references into a projection (`Outputs`).
fn cols_of_payload(p: Payload) -> Result<Payload, String> {
    let scalars = match p {
        Payload::GroupKey(s) | Payload::Predicates(s) | Payload::Scalars(s) => s,
        Payload::Outputs(o) => return Ok(Payload::Outputs(o)),
        _ => return Err("cols_of expects a list of scalars".into()),
    };
    let mut cols = Vec::with_capacity(scalars.len());
    for s in &scalars {
        let only = s.cols.iter().copied().next();
        match only {
            Some(c) if s.cols.len() == 1 && s.text == format!("#{c}") => cols.push(c),
            _ => {
                return Err(format!(
                    "cols_of: `{}` is not a bare column reference",
                    s.text
                ))
            }
        }
    }
    Ok(Payload::Outputs(cols))
}

/// Evaluate an index expression to an integer using the bound arities.
pub fn eval_ixexpr(e: &IxExpr, arities: &BTreeMap<String, usize>) -> Result<i64, String> {
    Ok(match e {
        IxExpr::Lit(n) => *n,
        IxExpr::Arity(rel) => *arities
            .get(rel)
            .ok_or_else(|| format!("arity of unbound relation `{rel}`"))?
            as i64,
        IxExpr::Add(a, b) => eval_ixexpr(a, arities)? + eval_ixexpr(b, arities)?,
        IxExpr::Sub(a, b) => eval_ixexpr(a, arities)? - eval_ixexpr(b, arities)?,
        IxExpr::Neg(a) => -eval_ixexpr(a, arities)?,
    })
}

/// Apply a column-index function to a scalar (rewriting both its column set and
/// its `#n` rendering).
fn map_scalar_cols(s: &Scalar, mut f: impl FnMut(Col) -> i64) -> Result<Scalar, String> {
    let mut cols = std::collections::BTreeSet::new();
    for &c in &s.cols {
        let nc = f(c);
        if nc < 0 {
            return Err(format!(
                "column shift produced negative index for `{}`",
                s.text
            ));
        }
        cols.insert(nc as usize);
    }
    Ok(Scalar {
        text: rewrite_text_cols(&s.text, &mut f)?,
        cols,
    })
}

/// Rewrite every `#n` occurrence in `text` by applying `f` to `n`.
fn rewrite_text_cols(text: &str, f: &mut impl FnMut(Col) -> i64) -> Result<String, String> {
    let mut out = String::with_capacity(text.len());
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'#' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() {
            let start = i + 1;
            let mut j = start;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            let n: usize = text[start..j].parse().unwrap();
            let nn = f(n);
            if nn < 0 {
                return Err(format!("column shift produced negative index in `{text}`"));
            }
            out.push('#');
            out.push_str(&nn.to_string());
            i = j;
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    Ok(out)
}

fn map_payload_cols(p: Payload, mut f: impl FnMut(Col) -> i64) -> Result<Payload, String> {
    let map_scalars =
        |xs: Vec<Scalar>, f: &mut dyn FnMut(Col) -> i64| -> Result<Vec<Scalar>, String> {
            xs.iter().map(|s| map_scalar_cols(s, &mut *f)).collect()
        };
    Ok(match p {
        Payload::Predicates(xs) => Payload::Predicates(map_scalars(xs, &mut f)?),
        Payload::Scalars(xs) => Payload::Scalars(map_scalars(xs, &mut f)?),
        Payload::GroupKey(xs) => Payload::GroupKey(map_scalars(xs, &mut f)?),
        Payload::Aggregates(xs) => Payload::Aggregates(map_scalars(xs, &mut f)?),
        Payload::Outputs(o) => {
            let mut out = Vec::with_capacity(o.len());
            for c in o {
                let nc = f(c);
                if nc < 0 {
                    return Err("column shift produced negative index".into());
                }
                out.push(nc as usize);
            }
            Payload::Outputs(out)
        }
        Payload::Equivalences(classes) => {
            let mut out = Vec::with_capacity(classes.len());
            for class in classes {
                out.push(map_scalars(class, &mut f)?);
            }
            Payload::Equivalences(out)
        }
    })
}

/// Shift every column index in `p` by `k`.
fn shift_payload(p: Payload, k: i64) -> Result<Payload, String> {
    map_payload_cols(p, |c| c as i64 + k)
}

/// Remap every column index `c` of `p` to `outs[c]` (inverting a projection).
fn remap_payload(p: Payload, outs: &[usize]) -> Result<Payload, String> {
    map_payload_cols(p, |c| match outs.get(c) {
        Some(&nc) => nc as i64,
        None => -1, // out of range ⇒ surfaced as an error by the callers
    })
}

fn concat_payload(a: Payload, c: Payload) -> Result<Payload, String> {
    use Payload::*;
    Ok(match (a, c) {
        (Predicates(mut x), Predicates(y)) => {
            x.extend(y);
            Predicates(x)
        }
        (Scalars(mut x), Scalars(y)) => {
            x.extend(y);
            Scalars(x)
        }
        (Outputs(mut x), Outputs(y)) => {
            x.extend(y);
            Outputs(x)
        }
        (Equivalences(mut x), Equivalences(y)) => {
            x.extend(y);
            Equivalences(x)
        }
        _ => return Err("concat of mismatched payload kinds".into()),
    })
}

/// `compose(outer, inner)`: the projection that first applies `inner` then
/// `outer`, i.e. `result[i] = inner[outer[i]]`.
fn compose_payload(outer: Payload, inner: Payload) -> Result<Payload, String> {
    match (outer, inner) {
        (Payload::Outputs(o), Payload::Outputs(i)) => {
            let mut out = Vec::with_capacity(o.len());
            for idx in o {
                let mapped = *i
                    .get(idx)
                    .ok_or_else(|| format!("compose index {idx} out of range"))?;
                out.push(mapped);
            }
            Ok(Payload::Outputs(out))
        }
        _ => Err("compose expects two projection lists".into()),
    }
}

impl Payload {
    pub fn into_predicates(self) -> Result<Vec<Scalar>, String> {
        match self {
            Payload::Predicates(s) => Ok(s),
            _ => Err("expected a predicate payload".into()),
        }
    }
    pub fn into_scalars(self) -> Result<Vec<Scalar>, String> {
        match self {
            Payload::Scalars(s) => Ok(s),
            _ => Err("expected a scalar payload".into()),
        }
    }
    pub fn into_outputs(self) -> Result<Vec<Col>, String> {
        match self {
            Payload::Outputs(o) => Ok(o),
            _ => Err("expected a projection payload".into()),
        }
    }
    pub fn into_equivalences(self) -> Result<Vec<Vec<Scalar>>, String> {
        match self {
            Payload::Equivalences(e) => Ok(e),
            _ => Err("expected an equivalences payload".into()),
        }
    }
    pub fn into_group_key(self) -> Result<Vec<Scalar>, String> {
        match self {
            Payload::GroupKey(s) => Ok(s),
            _ => Err("expected a group-key payload".into()),
        }
    }
    pub fn into_aggregates(self) -> Result<Vec<Scalar>, String> {
        match self {
            Payload::Aggregates(s) => Ok(s),
            _ => Err("expected an aggregates payload".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::{IxExpr, PExpr};

    fn pred(text: &str, cols: &[Col]) -> Payload {
        Payload::Predicates(vec![Scalar::new(text, cols.iter().copied())])
    }

    #[test]
    fn shift_rewrites_columns_and_text() {
        // p reads #2,#3 ; shifting by -arity(a) with arity(a)=2 yields #0,#1.
        let mut payloads = BTreeMap::new();
        payloads.insert("p".to_string(), pred("(#2 + #3)", &[2, 3]));
        let arities = BTreeMap::from([("a".to_string(), 2usize)]);
        let e = PExpr::Shift(
            Box::new(PExpr::Var("p".into())),
            IxExpr::Neg(Box::new(IxExpr::Arity("a".into()))),
        );
        let out = eval_pexpr(&e, &payloads, &arities).unwrap();
        let scalars = out.into_predicates().unwrap();
        assert_eq!(scalars[0].text, "(#0 + #1)");
        assert_eq!(
            scalars[0].cols.iter().copied().collect::<Vec<_>>(),
            vec![0, 1]
        );
    }

    #[test]
    fn remap_inverts_a_projection() {
        // p reads projected positions #0,#1 ; project outputs = [5, 7] ; so the
        // underlying columns are #5,#7.
        let mut payloads = BTreeMap::new();
        payloads.insert("p".to_string(), pred("(#0 = #1)", &[0, 1]));
        payloads.insert("o".to_string(), Payload::Outputs(vec![5, 7]));
        let arities = BTreeMap::new();
        let e = PExpr::Remap(
            Box::new(PExpr::Var("p".into())),
            Box::new(PExpr::Var("o".into())),
        );
        let out = eval_pexpr(&e, &payloads, &arities).unwrap();
        let scalars = out.into_predicates().unwrap();
        assert_eq!(scalars[0].text, "(#5 = #7)");
        assert_eq!(
            scalars[0].cols.iter().copied().collect::<Vec<_>>(),
            vec![5, 7]
        );
    }
}
