// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![warn(missing_docs)]

//! Materialize's compute layer.

use columnar::Columnar;
use differential_dataflow::containers::Columnation;

pub mod server;

mod arrangement;
mod command_channel;
mod compute_state;
mod extensions;
mod logging;
mod metrics;
mod render;
mod row_spine;
mod sink;
mod typedefs;

pub(crate) trait MzData: timely::Data + Columnar + Columnation {}
impl<T: timely::Data + Columnar + Columnation> MzData for T {}
