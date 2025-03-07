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

use timely::container::{Container, ContainerBuilder};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::channels::pushers::buffer::Session;
use timely::dataflow::channels::pushers::{Counter, Tee};
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::{Scope, StreamCore};
use timely::Data;

/// Extension trait for `Stream`.
pub trait OkErrCB<S: Scope, C: Container> {
    /// Takes one input stream and splits it into two output streams.
    /// For each record, the supplied closure is called with the data.
    /// If it returns `Ok(x)`, then `x` will be sent
    /// to the first returned stream; otherwise, if it returns `Err(e)`,
    /// then `e` will be sent to the second.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::ToStream;
    /// use timely::dataflow::operators::core::{OkErr, Inspect};
    ///
    /// timely::example(|scope| {
    ///     let (odd, even) = (0..10)
    ///         .to_stream(scope)
    ///         .ok_err(|x| if x % 2 == 0 { Ok(x) } else { Err(x) });
    ///
    ///     even.container::<Vec<_>>().inspect(|x| println!("even: {:?}", x));
    ///     odd.container::<Vec<_>>().inspect(|x| println!("odd: {:?}", x));
    /// });
    /// ```
    fn ok_err_cb<CB1, CB2, L>(
        &self,
        logic: L,
    ) -> (StreamCore<S, CB1::Container>, StreamCore<S, CB2::Container>)
    where
        CB1: ContainerBuilder,
        CB2: ContainerBuilder,
        L: FnMut(
                C::Item<'_>,
                &mut Session<
                    S::Timestamp,
                    CB1,
                    Counter<S::Timestamp, CB1::Container, Tee<S::Timestamp, CB1::Container>>,
                >,
                &mut Session<
                    S::Timestamp,
                    CB2,
                    Counter<S::Timestamp, CB2::Container, Tee<S::Timestamp, CB2::Container>>,
                >,
            ) + 'static;
}

impl<S: Scope, C: Container + Data> OkErrCB<S, C> for StreamCore<S, C> {
    fn ok_err_cb<CB1, CB2, L>(
        &self,
        mut logic: L,
    ) -> (StreamCore<S, CB1::Container>, StreamCore<S, CB2::Container>)
    where
        CB1: ContainerBuilder,
        CB2: ContainerBuilder,
        L: FnMut(
                C::Item<'_>,
                &mut Session<
                    S::Timestamp,
                    CB1,
                    Counter<S::Timestamp, CB1::Container, Tee<S::Timestamp, CB1::Container>>,
                >,
                &mut Session<
                    S::Timestamp,
                    CB2,
                    Counter<S::Timestamp, CB2::Container, Tee<S::Timestamp, CB2::Container>>,
                >,
            ) + 'static,
    {
        let mut builder = OperatorBuilder::new("OkErrCB".to_owned(), self.scope());

        let mut input = builder.new_input(self, Pipeline);
        let (mut output1, stream1) = builder.new_output();
        let (mut output2, stream2) = builder.new_output();

        builder.build(move |_| {
            move |_frontiers| {
                let mut output1_handle = output1.activate();
                let mut output2_handle = output2.activate();

                input.for_each(|time, data| {
                    let mut out1 = output1_handle.session_with_builder(&time);
                    let mut out2 = output2_handle.session_with_builder(&time);
                    for datum in data.drain() {
                        logic(datum, &mut out1, &mut out2);
                    }
                });
            }
        });

        (stream1, stream2)
    }
}
