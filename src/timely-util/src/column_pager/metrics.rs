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

//! Prometheus metrics for the column pager.
//!
//! One process-wide [`PagerMetrics`] singleton, installed by compute init via
//! [`register`]. Counter observers (`observe_*`) are no-ops until that call
//! lands; the lazy initialization keeps tests and benches that don't wire a
//! [`MetricsRegistry`] free of bookkeeping.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use mz_ore::metric;
use mz_ore::metrics::{ComputedUIntGauge, IntCounter, MetricsRegistry};

use crate::column_pager::policy::TieredPolicy;

/// Process-wide pager metrics. Counters track cumulative observations since
/// process start; gauges read the live policy atomics at scrape time.
#[derive(Debug)]
pub struct PagerMetrics {
    /// Number of decisions that kept the chunk resident.
    pub skip_decisions_total: IntCounter,
    /// Total bytes kept resident by skip decisions.
    pub skip_bytes_total: IntCounter,
    /// Number of decisions that paged the chunk out.
    pub pageouts_total: IntCounter,
    /// Uncompressed body bytes handed to the pager for pageout.
    pub paged_bytes_in_total: IntCounter,
    /// On-storage payload bytes after codec / padding.
    pub paged_bytes_out_total: IntCounter,
    /// Number of page-ins from `ColumnPager::take`.
    pub pageins_total: IntCounter,
    /// Total uncompressed bytes delivered by page-in.
    pub pagein_bytes_total: IntCounter,
    /// Resident-ticket drops returning bytes to the budget.
    pub resident_released_total: IntCounter,
    /// Total bytes returned to the budget by ticket drops.
    pub resident_released_bytes_total: IntCounter,
    // Computed gauges are registered with the registry but not held here —
    // their collectors are owned by the prometheus registry.
}

static METRICS: OnceLock<PagerMetrics> = OnceLock::new();

/// Install the pager metrics into `registry`. Idempotent — repeated calls
/// after the first one are no-ops. Computed gauges read the singleton
/// [`TieredPolicy`] atomics at scrape time; their values reflect the live
/// policy whether or not the column-paged batcher is currently enabled.
pub fn register(registry: &MetricsRegistry, policy: &'static TieredPolicy) {
    let _ = METRICS.get_or_init(|| {
        // Computed gauges: closures hold the &'static policy reference.
        let _budget_remaining: ComputedUIntGauge = registry.register_computed_gauge(
            metric!(
                name: "mz_column_pager_budget_remaining_bytes",
                help: "Bytes the column-pager tiered policy currently has \
                       available for resident columns.",
            ),
            move || u64::try_from(policy.budget_remaining()).unwrap_or(u64::MAX),
        );
        let _budget_configured: ComputedUIntGauge = registry.register_computed_gauge(
            metric!(
                name: "mz_column_pager_budget_configured_bytes",
                help: "Most-recently-configured total budget for the \
                       column-pager tiered policy.",
            ),
            move || u64::try_from(policy.configured_total()).unwrap_or(u64::MAX),
        );

        PagerMetrics {
            skip_decisions_total: registry.register(metric!(
                name: "mz_column_pager_skip_decisions_total",
                help: "Pager decisions that kept the chunk resident.",
            )),
            skip_bytes_total: registry.register(metric!(
                name: "mz_column_pager_skip_bytes_total",
                help: "Total bytes kept resident by skip decisions.",
            )),
            pageouts_total: registry.register(metric!(
                name: "mz_column_pager_pageouts_total",
                help: "Pager decisions that paged the chunk out.",
            )),
            paged_bytes_in_total: registry.register(metric!(
                name: "mz_column_pager_paged_bytes_in_total",
                help: "Total uncompressed bytes handed to the pager for \
                       pageout, before any codec is applied.",
            )),
            paged_bytes_out_total: registry.register(metric!(
                name: "mz_column_pager_paged_bytes_out_total",
                help: "Total on-storage bytes after codec / padding.",
            )),
            pageins_total: registry.register(metric!(
                name: "mz_column_pager_pageins_total",
                help: "Successful page-ins from `ColumnPager::take`.",
            )),
            pagein_bytes_total: registry.register(metric!(
                name: "mz_column_pager_pagein_bytes_total",
                help: "Total uncompressed bytes delivered by page-in.",
            )),
            resident_released_total: registry.register(metric!(
                name: "mz_column_pager_resident_released_total",
                help: "Resident-ticket drops returning budget.",
            )),
            resident_released_bytes_total: registry.register(metric!(
                name: "mz_column_pager_resident_released_bytes_total",
                help: "Total bytes returned to the budget by ticket drops.",
            )),
        }
    });
}

#[inline]
fn metrics() -> Option<&'static PagerMetrics> {
    METRICS.get()
}

pub(crate) fn observe_skip(bytes: usize) {
    if let Some(m) = metrics() {
        m.skip_decisions_total.inc();
        m.skip_bytes_total.inc_by(bytes_to_u64(bytes));
    }
}

pub(crate) fn observe_pageout(bytes_in: usize, bytes_out: usize) {
    if let Some(m) = metrics() {
        m.pageouts_total.inc();
        m.paged_bytes_in_total.inc_by(bytes_to_u64(bytes_in));
        m.paged_bytes_out_total.inc_by(bytes_to_u64(bytes_out));
    }
}

pub(crate) fn observe_pagein(bytes: usize) {
    if let Some(m) = metrics() {
        m.pageins_total.inc();
        m.pagein_bytes_total.inc_by(bytes_to_u64(bytes));
    }
}

pub(crate) fn observe_resident_released(bytes: usize) {
    if let Some(m) = metrics() {
        m.resident_released_total.inc();
        m.resident_released_bytes_total.inc_by(bytes_to_u64(bytes));
    }
}

fn bytes_to_u64(b: usize) -> u64 {
    u64::try_from(b).unwrap_or(u64::MAX)
}

/// Number of buckets in the pageout→pagein dwell-time histogram.
pub const PAGEIN_DWELL_BUCKETS: usize = 8;

/// Upper bounds (inclusive, in microseconds) for each dwell bucket; the last
/// bucket is unbounded. Index `i` of [`pagein_dwell_histogram`] counts page-ins
/// whose chunk sat spilled for a time `<= PAGEIN_DWELL_BOUNDS_US[i]`.
pub const PAGEIN_DWELL_LABELS: [&str; PAGEIN_DWELL_BUCKETS] = [
    "<1us", "<10us", "<100us", "<1ms", "<10ms", "<100ms", "<1s", ">=1s",
];

/// Always-on (registry-independent) histogram of pageout→pagein dwell time —
/// how long a spilled chunk sits before being read back. Short dwells indicate
/// `MADV_COLD` is evicting chunks the merge front re-reads almost immediately.
static PAGEIN_DWELL: [AtomicU64; PAGEIN_DWELL_BUCKETS] =
    [const { AtomicU64::new(0) }; PAGEIN_DWELL_BUCKETS];

pub(crate) fn observe_pagein_latency(d: Duration) {
    let bucket = match d.as_micros() {
        0 => 0,
        1..=9 => 1,
        10..=99 => 2,
        100..=999 => 3,
        1_000..=9_999 => 4,
        10_000..=99_999 => 5,
        100_000..=999_999 => 6,
        _ => 7,
    };
    PAGEIN_DWELL[bucket].fetch_add(1, Ordering::Relaxed);
}

/// Snapshot the dwell-time histogram (cumulative counts per bucket since process
/// start). See [`PAGEIN_DWELL_LABELS`] for the bucket boundaries.
pub fn pagein_dwell_histogram() -> [u64; PAGEIN_DWELL_BUCKETS] {
    std::array::from_fn(|i| PAGEIN_DWELL[i].load(Ordering::Relaxed))
}
