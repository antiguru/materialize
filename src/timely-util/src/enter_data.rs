// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Operators that separate one stream into two streams based on some condition

use crate::containers::{Column, ColumnBuilder};
use columnar::Columnar;
use timely::container::Container;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Enter, Operator};
use timely::dataflow::scopes::Child;
use timely::dataflow::{Scope, Stream, StreamCore};
use timely::progress::timestamp::Refines;
use timely::progress::Timestamp;
use timely::Data;

/// Extension trait for `Stream`.
pub trait EnterData<S: Scope, T, CI: Container, CO> {
    fn enter_data<'a>(&self, child: &Child<'a, S, T>) -> StreamCore<Child<'a, S, T>, CO>
    where
        T: Timestamp + Refines<S::Timestamp>;
}

impl<S, T, D, R> EnterData<S, T, Vec<(D, S::Timestamp, R)>, Vec<(D, T, R)>>
    for Stream<S, (D, S::Timestamp, R)>
where
    S: Scope,
    D: Data,
    R: Data,
    T: Timestamp + Refines<S::Timestamp>,
{
    fn enter_data<'a>(
        &self,
        child: &Child<'a, S, T>,
    ) -> StreamCore<Child<'a, S, T>, Vec<(D, T, R)>>
where {
        self.enter(child)
            .unary(Pipeline, "enter_data_vec", move |_, _| {
                move |input, output| {
                    input.for_each(|time, data| {
                        output
                            .session(&time)
                            .give_iterator(data.drain(..).map(|(d, t, r)| (d, T::to_inner(t), r)));
                    });
                }
            })
    }
}

impl<S, T, D, R> EnterData<S, T, Column<(D, S::Timestamp, R)>, Column<(D, T, R)>>
    for StreamCore<S, Column<(D, S::Timestamp, R)>>
where
    S: Scope,
    S::Timestamp: Columnar,
    <S::Timestamp as Columnar>::Container: Clone,
    D: Data + Columnar,
    D::Container: Clone,
    R: Data + Columnar,
    R::Container: Clone,
    T: Timestamp + Refines<S::Timestamp> + Columnar,
    T::Container: Clone,
    Column<(D, S::Timestamp, R)>: Data,
{
    fn enter_data<'a>(
        &self,
        child: &Child<'a, S, T>,
    ) -> StreamCore<Child<'a, S, T>, Column<(D, T, R)>>
where {
        self.enter(child)
            .unary::<ColumnBuilder<(D, T, R)>, _, _, _>(Pipeline, "enter_data_vec", move |_, _| {
                move |input, output| {
                    while let Some((time, data)) = input.next() {
                        let mut session = output.session_with_builder(&time);
                        for (d, t, r) in data.iter() {
                            let t = Columnar::into_owned(t);
                            session.give((d, &T::to_inner(t), r));
                        }
                    }
                }
            })
    }
}
