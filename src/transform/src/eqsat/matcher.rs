// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! The payload algebra for rule instantiation.
//!
//! A rule's matching and side-condition checking happen on the e-graph (see
//! [`crate::eqsat::egraph`]); this module supplies the value layer those use: the
//! [`Payload`] a metavariable binds, and the payload-combining operations
//! (concatenating, shifting, or remapping columns) the compiled rule matchers
//! (`crate::eqsat::rules`) call when instantiating a right-hand side. Column
//! remapping is the real `MirScalarExpr::permute`, so a remapped predicate is a
//! faithful expression.

use std::collections::BTreeMap;

use mz_expr::{AggregateExpr, Columns};

use crate::eqsat::ir::{Col, EScalar};

/// The payload captured by a metavariable. The variant records which operator
/// the payload came from so the template can rebuild a well-typed operator.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Payload {
    Predicates(Vec<EScalar>),
    Scalars(Vec<EScalar>),
    Outputs(Vec<Col>),
    Equivalences(Vec<Vec<EScalar>>),
    GroupKey(Vec<EScalar>),
    Aggregates(Vec<AggregateExpr>),
}

impl Payload {
    /// The number of list elements in the payload.
    pub fn len(&self) -> usize {
        match self {
            Payload::Predicates(s) | Payload::Scalars(s) | Payload::GroupKey(s) => s.len(),
            Payload::Aggregates(a) => a.len(),
            Payload::Outputs(o) => o.len(),
            Payload::Equivalences(c) => c.len(),
        }
    }

    /// Whether the payload list is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The underlying scalar list, for the scalar-bearing payload kinds.
    /// Aggregates are `AggregateExpr`, not scalars, so they are excluded.
    pub fn scalars(&self) -> Option<&[EScalar]> {
        match self {
            Payload::Predicates(s) | Payload::Scalars(s) | Payload::GroupKey(s) => Some(s),
            Payload::Aggregates(_) | Payload::Outputs(_) | Payload::Equivalences(_) => None,
        }
    }

    /// All columns referenced by this payload (used by side conditions).
    pub fn columns(&self) -> Vec<Col> {
        match self {
            Payload::Predicates(s) | Payload::Scalars(s) | Payload::GroupKey(s) => {
                s.iter().flat_map(|x| x.cols()).collect()
            }
            Payload::Aggregates(a) => a.iter().flat_map(|x| x.expr.support()).collect(),
            Payload::Outputs(o) => o.clone(),
            Payload::Equivalences(classes) => classes
                .iter()
                .flat_map(|c| c.iter().flat_map(|x| x.cols()))
                .collect(),
        }
    }
}

/// `iota(n)`: the identity projection `[0, 1, …, n-1]`. Builds the leading
/// "keep all input columns" part of a projection.
pub(crate) fn iota_payload(n: i64) -> Result<Payload, String> {
    if n < 0 {
        return Err("iota of negative length".into());
    }
    Ok(Payload::Outputs((0..n as usize).collect()))
}

/// Turn a payload of bare column references into a projection (`Outputs`).
pub(crate) fn cols_of_payload(p: Payload) -> Result<Payload, String> {
    let scalars = match p {
        Payload::GroupKey(s) | Payload::Predicates(s) | Payload::Scalars(s) => s,
        Payload::Outputs(o) => return Ok(Payload::Outputs(o)),
        _ => return Err("cols_of expects a list of scalars".into()),
    };
    let cols = scalars
        .iter()
        .map(|s| {
            s.is_col()
                .ok_or_else(|| format!("cols_of: `{}` is not a bare column reference", s.expr))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Payload::Outputs(cols))
}

/// Apply a column-index function to an aggregate (rewriting the columns of its
/// inner expression). The aggregate function and flags are untouched.
fn map_aggregate_cols(
    a: &AggregateExpr,
    mut f: impl FnMut(Col) -> i64,
) -> Result<AggregateExpr, String> {
    let mut map: BTreeMap<usize, usize> = BTreeMap::new();
    for c in a.expr.support() {
        let nc = f(c);
        if nc < 0 {
            return Err(format!(
                "column shift produced negative index for aggregate `{}`",
                a.expr
            ));
        }
        map.insert(
            c,
            usize::try_from(nc).expect("non-negative index fits usize"),
        );
    }
    let mut out = a.clone();
    out.expr.permute_map(&map);
    Ok(out)
}

fn map_payload_cols(p: Payload, mut f: impl FnMut(Col) -> i64) -> Result<Payload, String> {
    let map_scalars =
        |xs: Vec<EScalar>, f: &mut dyn FnMut(Col) -> i64| -> Result<Vec<EScalar>, String> {
            xs.iter().map(|s| s.permute_cols(&mut *f)).collect()
        };
    Ok(match p {
        Payload::Predicates(xs) => Payload::Predicates(map_scalars(xs, &mut f)?),
        Payload::Scalars(xs) => Payload::Scalars(map_scalars(xs, &mut f)?),
        Payload::GroupKey(xs) => Payload::GroupKey(map_scalars(xs, &mut f)?),
        Payload::Aggregates(xs) => Payload::Aggregates(
            xs.iter()
                .map(|a| map_aggregate_cols(a, &mut f))
                .collect::<Result<_, _>>()?,
        ),
        Payload::Outputs(o) => {
            let mut out = Vec::with_capacity(o.len());
            for c in o {
                let nc = f(c);
                if nc < 0 {
                    return Err("column shift produced negative index".into());
                }
                out.push(usize::try_from(nc).expect("non-negative index fits usize"));
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
pub(crate) fn shift_payload(p: Payload, k: i64) -> Result<Payload, String> {
    map_payload_cols(p, |c| c as i64 + k)
}

/// Remap every column index `c` of `p` to `outs[c]` (inverting a projection).
pub(crate) fn remap_payload(p: Payload, outs: &[usize]) -> Result<Payload, String> {
    map_payload_cols(p, |c| match outs.get(c) {
        Some(&nc) => nc as i64,
        None => -1, // out of range ⇒ surfaced as an error by the callers
    })
}

pub(crate) fn concat_payload(a: Payload, c: Payload) -> Result<Payload, String> {
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
pub(crate) fn compose_payload(outer: Payload, inner: Payload) -> Result<Payload, String> {
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
    pub fn into_predicates(self) -> Result<Vec<EScalar>, String> {
        match self {
            Payload::Predicates(s) => Ok(s),
            _ => Err("expected a predicate payload".into()),
        }
    }
    pub fn into_scalars(self) -> Result<Vec<EScalar>, String> {
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
    pub fn into_equivalences(self) -> Result<Vec<Vec<EScalar>>, String> {
        match self {
            Payload::Equivalences(e) => Ok(e),
            _ => Err("expected an equivalences payload".into()),
        }
    }
    pub fn into_group_key(self) -> Result<Vec<EScalar>, String> {
        match self {
            Payload::GroupKey(s) => Ok(s),
            _ => Err("expected a group-key payload".into()),
        }
    }
    pub fn into_aggregates(self) -> Result<Vec<AggregateExpr>, String> {
        match self {
            Payload::Aggregates(s) => Ok(s),
            _ => Err("expected an aggregates payload".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_expr::MirScalarExpr;

    /// A predicate payload of bare column references `#c` (one scalar per
    /// column), enough to exercise the per-scalar column remapping.
    fn pred(cols: &[Col]) -> Payload {
        Payload::Predicates(
            cols.iter()
                .map(|&c| EScalar::plain(MirScalarExpr::column(c)))
                .collect(),
        )
    }

    #[mz_ore::test]
    fn shift_rewrites_columns() {
        // p reads #2,#3 ; shifting by -arity(a) with arity(a)=2 yields #0,#1.
        let out = shift_payload(pred(&[2, 3]), -2).unwrap();
        let scalars = out.into_predicates().unwrap();
        assert_eq!(scalars[0].is_col(), Some(0));
        assert_eq!(scalars[1].is_col(), Some(1));
    }

    #[mz_ore::test]
    fn remap_inverts_a_projection() {
        // p reads projected positions #0,#1 ; project outputs = [5, 7] ; so the
        // underlying columns are #5,#7.
        let out = remap_payload(pred(&[0, 1]), &[5, 7]).unwrap();
        let scalars = out.into_predicates().unwrap();
        assert_eq!(scalars[0].is_col(), Some(5));
        assert_eq!(scalars[1].is_col(), Some(7));
    }
}
