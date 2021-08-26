// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![warn(missing_docs)]

//! Driver for timely/differential dataflow.

mod arrangement;
mod decode;
mod metrics;
mod operator;
mod render;
mod server;
mod sink;

pub mod logging;
pub mod source;

pub use render::plan::Plan;
pub use server::{serve, Command, Config, Response, TimestampBindingFeedback, WorkerFeedback};
use std::time::{Duration, Instant};

/// A timer to track the duration of a code span until it is dropped
pub struct Timer {
    start: Instant,
    f: Box<dyn Fn(Duration)>,
}

impl Timer {
    /// Construct a new timer with a callback. The callback will receive the duration since creation
    pub fn new<F: Fn(Duration) + 'static>(f: F) -> Self {
        Self {
            start: Instant::now(),
            f: Box::new(f),
        }
    }

    /// Construct a new timer with a default print function.
    pub fn new_default(name: &str) -> Self {
        let name = name.to_string();
        Self::new(move |d: Duration| println!("{} -> {:.6}s", name, d.as_secs_f32()))
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        if elapsed >= Duration::from_millis(1) {
            (self.f)(elapsed)
        }
    }
}
