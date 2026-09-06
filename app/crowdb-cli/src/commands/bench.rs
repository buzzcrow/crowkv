// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `bench` command — KV workload + RPC echo benchmark.
//!
//! Module index only; workload implementations live in `bench/`.

pub mod chunkdb;
pub mod chunkio;
pub mod diskdb;
pub mod diskdb_workload;
pub mod kv_client;
pub mod kv_prepare;
pub mod kv_read;
pub mod kv_scan;
pub mod kv_write;
pub mod loader;
pub mod metrics;
pub mod result;
pub mod rpc;
pub mod verb;

pub use verb::{run_bench_verb, BenchVerb};
