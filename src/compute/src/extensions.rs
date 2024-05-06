// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Operator extensions to Timely and Differential

use differential_dataflow::difference::Semigroup;
use differential_dataflow::{AsCollection, Collection};
use timely::dataflow::{Scope, Stream, StreamCore};
use timely::Data;

pub(crate) mod arrange;
pub(crate) mod reduce;

#[derive(Clone, Debug)]
pub enum MzCollection<G: Scope, D, R: Semigroup> {
    Collection(Collection<G, D, R>),
}

/// Conversion to a differential dataflow Collection.
pub trait AsMzCollection<G: Scope, D: Data, R: Semigroup> {
    /// Converts the type to a differential dataflow collection.
    fn as_mz_collection(&self) -> MzCollection<G, D, R>;
}

impl<G: Scope, D: Data, R: Semigroup> AsMzCollection<G, D, R>
    for StreamCore<G, Vec<(D, G::Timestamp, R)>>
{
    fn as_mz_collection(&self) -> MzCollection<G, D, R> {
        MzCollection::Collection(self.as_collection())
    }
}
