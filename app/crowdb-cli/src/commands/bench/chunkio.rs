// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Thin CLI adapter for the library-owned chunk IO benchmark.

use std::process::ExitCode;
use std::sync::Arc;

use crowdb_chunk_client::{
    run_large_write_benchmark, ChunkClientConfig, ChunkIoClient, ChunkIoClientConfig,
    LargeWriteBenchmarkConfig, LargeWritePolicy,
};
use crowdb_common::ec::EcScheme;

use super::metrics::BenchMetrics;
use super::verb::ChunkioBenchVerb;
use crate::Cli;

pub async fn run(cli: &Cli, verb: ChunkioBenchVerb) -> ExitCode {
    let ChunkioBenchVerb::Write(args) = verb;
    if args.objects == 0
        || args.object_size == 0
        || args.concurrency == 0
        || args.block_size == 0
        || args.data_num == 0
        || args.code_num == 0
    {
        eprintln!("chunkio benchmark values must be non-zero");
        return ExitCode::from(2);
    }
    let config = match crate::commands::load_config(cli) {
        Ok(config) => config,
        Err(code) => return code,
    };
    let mut seeds = vec![format!("http://{}:{}", cli.sysmd_ip, cli.sysmd_port)];
    for server in config
        .servers
        .iter()
        .filter(|server| server.service_type == crowdb_console_shared::config::ServiceType::Kv)
    {
        if !seeds.contains(&server.url) {
            seeds.push(server.url.clone());
        }
    }
    let client = match ChunkIoClient::connect(ChunkIoClientConfig {
        management_seeds: seeds,
    })
    .await
    {
        Ok(client) => client,
        Err(error) => {
            eprintln!("chunkio discovery failed: {error}");
            return ExitCode::FAILURE;
        }
    };
    let policy = LargeWritePolicy {
        ec_scheme: EcScheme::new(args.data_num, args.code_num),
        client: Arc::new(ChunkClientConfig {
            max_chunk_size: args.chunk_size,
            read_buffer_size: args.block_size,
            max_cached_buffer: args.block_size.saturating_mul(args.data_num),
            ..ChunkClientConfig::default()
        }),
    };
    let mut metrics = BenchMetrics::new(&cli.log_dir, args.metrics_interval);
    metrics.start();
    let result = run_large_write_benchmark(
        client,
        LargeWriteBenchmarkConfig {
            object_count: args.objects,
            object_size: args.object_size,
            concurrency: args.concurrency,
            seed: args.seed,
            policy,
        },
    )
    .await;
    metrics.stop().await;
    if cli.json {
        match serde_json::to_string_pretty(&result) {
            Ok(json) => println!("{json}"),
            Err(error) => {
                eprintln!("encode chunkio result: {error}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        println!(
            "chunkio write: requested={} object_size={} objects={} errors={} incomplete={} stop={} objects_s={:.2} logical_mib_s={:.1} physical_mib_s={:.1} p50_us={} p99_us={} prep_stalls={} prep_stall_us={}",
            result.requested_objects,
            args.object_size,
            result.objects,
            result.errors,
            result.incomplete_objects,
            result.stop_reason,
            result.objects_per_sec,
            result.logical_mib_per_sec,
            result.physical_mib_per_sec,
            result.latency_p50_us,
            result.latency_p99_us,
            result.preparation_stalls,
            result.preparation_stall_us,
        );
    }
    if result.errors == 0 && result.incomplete_objects == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
