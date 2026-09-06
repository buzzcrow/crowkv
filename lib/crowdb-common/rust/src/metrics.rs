// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Metrics module: lightweight atomic counters, gauges, bandwidth,
//! latency histograms, and latency summaries with periodic flush to a
//! dedicated metrics log file.

pub mod bandwidth;
pub mod counter;
pub mod histogram;
pub mod precise;
pub mod summary;
pub mod system;

#[cfg(target_os = "linux")]
pub mod perf;

mod flush;
pub mod name;
pub mod point;
pub mod registry;
pub mod runner;
pub mod timestamp;

pub use bandwidth::{Bandwidth, BandwidthSnapshot};
pub use counter::{Counter, CounterSnapshot, Gauge};
pub use histogram::{HistogramSnapshot, LatencyHistogram};
pub use name::MetricName;
pub use point::MetricPoint;
pub use precise::PreciseHistogram;
pub use registry::{
    global, global_bandwidth, global_counter, global_gauge, global_histogram, global_summary, MetricsRegistry,
};
pub use runner::MetricsRunner;
pub use summary::{LatencySummary, SummarySnapshot};
pub use system::{flush_system, SystemCollector, SystemMetrics};
pub use timestamp::iso8601_now;
