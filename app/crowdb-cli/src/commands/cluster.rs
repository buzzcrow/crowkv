// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `cluster` domain — cluster-level ops: init, reset, clean, status,
//! topology, plus hardware subcommands (rack/node/disk-group/disk).

use std::process::ExitCode;

use clap::Subcommand;

use crate::commands::{
    commit_config, op_context, print_json, run_disk_group_verb, run_disk_verb, run_node_verb, run_rack_verb,
    DiskGroupVerb, DiskVerb, NodeVerb, RackVerb,
};
use crate::Cli;

#[derive(Subcommand, Debug)]
pub enum ClusterVerb {
    /// Initialize the cluster by bootstrapping group 0 on the listed nodes.
    Init {
        #[arg(short = 'n', long, value_delimiter = ',')]
        nodes: Vec<String>,
    },
    /// Deploy a local N-node KV cluster on 127.0.0.1 (forks
    /// `crowdb-kv-server` on each node, bootstraps group 0).
    /// With `-t rpc`, deploys a standalone crowdb-rpc-fb-server (echo)
    /// for RPC regression bench.
    #[command(name = "local-deploy")]
    LocalDeploy {
        #[arg(short = 'n', long, default_value_t = 3)]
        nodes: usize,
        /// Service type: `kv`, `diskdb`, `chunkdb`, `combined`, or `rpc`.
        #[arg(short = 't', long, default_value = "kv")]
        service_type: String,
        /// [rpc] Listen port. 0 = auto-allocate.
        #[arg(long, default_value_t = 0)]
        rpc_port: u16,
        /// [rpc] Independent epoll instances (round-robin).
        #[arg(long, default_value_t = 1)]
        io_engines: u32,
        /// [rpc] Total I/O worker threads.
        #[arg(long, default_value_t = 1)]
        io_workers: u32,
        /// [rpc] Enable Nagle's algorithm (disable `TCP_NODELAY`).
        #[arg(long, default_value_t = false)]
        enable_nagle: bool,
        /// [kv] `--rpc-workers` for the spawned server. 0 = server default (2).
        #[arg(long, default_value_t = 0)]
        rpc_workers: u32,
        /// [kv] `--peer-pool-size` for the spawned server. 0 = server default (2).
        #[arg(long, default_value_t = 0)]
        peer_pool_size: usize,
        /// [kv] `--max-inflight` for the spawned server. 0 = server default.
        #[arg(long, default_value_t = 0)]
        max_inflight: usize,
        /// [kv] `--coalesce-max-keys` for the spawned server. 0 = server default (32).
        #[arg(long, default_value_t = 0)]
        coalesce_max_keys: usize,
        /// [kv] Enable `--event-write` on the spawned server.
        #[arg(long, default_value_t = false)]
        event_write: bool,
        /// [kv] `--send-queue-capacity` for the spawned server. 0 = server default (4096).
        #[arg(long, default_value_t = 0)]
        send_queue_capacity: u32,
        /// [kv] `--metrics-interval` for the spawned server (seconds). 0 = server default (5).
        #[arg(long, default_value_t = 0)]
        metrics_interval: u64,
        /// [kv] `--kv-backend` for the spawned server (file|block|mem-block).
        /// Empty = server default (file).
        #[arg(long, default_value = "")]
        kv_backend: String,
        /// [kv] `--wal-backend` for the spawned server (file|mem-block|block-device).
        /// Empty = server default.
        #[arg(long, default_value = "")]
        wal_backend: String,
        /// [kv] Enable `--no-fsync` on the spawned server.
        #[arg(long, default_value_t = false)]
        no_fsync: bool,
        /// [diskdb] Disk-groups provisioned per node.
        #[arg(long, default_value_t = 1)]
        disk_groups_per_node: usize,
        /// [diskdb] Disks provisioned per disk-group.
        #[arg(long, default_value_t = 4)]
        disks_per_group: usize,
        /// [diskdb] Logical capacity of each disk.
        #[arg(long, default_value_t = 1_099_511_627_776_u64)]
        disk_capacity_bytes: u64,
        /// [diskdb] Logical zone size of each disk.
        #[arg(long, default_value_t = 274_877_906_944_u64)]
        disk_zone_size_bytes: u64,
        /// [diskdb] Allocation unit size.
        #[arg(long, default_value_t = 1_048_576_u32)]
        disk_unit_size_bytes: u32,
        /// [diskdb] Existing store-0 KV groups used for allocation records.
        #[arg(long, value_delimiter = ',', default_value = "1")]
        data_groups: Vec<u64>,
        /// [diskdb] KV client connections kept per endpoint. 0 = server default (1).
        #[arg(long, default_value_t = 0)]
        kv_connections: usize,
        /// [diskdb] KV client crowdb-rpc I/O workers. 0 = server default (2).
        #[arg(long, default_value_t = 0)]
        kv_client_rpc_workers: u32,
        /// [chunkdb] `DiskDB` client connections kept per endpoint. 0 = server default (1).
        #[arg(long, default_value_t = 0)]
        diskdb_connections: usize,
        /// [chunkdb] `DiskDB` client crowdb-rpc I/O workers. 0 = server default (2).
        #[arg(long, default_value_t = 0)]
        diskdb_client_rpc_workers: u32,
        /// [chunkdb] Number of instances to deploy.
        #[arg(long, default_value_t = 3)]
        chunkdb_instances: usize,
        /// [chunkdb] Permit unsafe EC placement fallback.
        #[arg(long, default_value_t = false)]
        allow_unsafe_ec: bool,
    },
    /// Tear down the entire cluster (all groups, stores, servers, sysdata).
    Destroy,
    /// Remove orphaned sysdata entries without stopping running servers.
    Reset,
    /// Wipe user data on every node + wait for re-election. Preserves
    /// group-0 sysdata + topology — servers stay running. Use --store/--group
    /// to target a non-system group (recommended for benchmarks).
    Clean {
        /// Target store ID (default 0 = system store).
        #[arg(long, default_value_t = 0)]
        store: u64,
        /// Target group ID (default 0 = system group; use 1+ for bench groups).
        #[arg(long, default_value_t = 0)]
        group: u64,
    },
    /// Show cluster status (list all stores from group-0 sysdata).
    Status,
    /// Show the topology view from a node's `/topology` endpoint.
    Topology {
        #[arg(short = 'n', long)]
        node: String,
    },
    /// Hardware: rack management.
    Rack {
        #[command(subcommand)]
        verb: RackVerb,
    },
    /// Hardware: node management.
    Node {
        #[command(subcommand)]
        verb: NodeVerb,
    },
    /// Hardware: disk-group management.
    #[command(name = "disk-group")]
    DiskGroup {
        #[command(subcommand)]
        verb: DiskGroupVerb,
    },
    /// Hardware: disk management.
    Disk {
        #[command(subcommand)]
        verb: DiskVerb,
    },
}

#[allow(clippy::too_many_lines)]
pub async fn run_cluster_verb(cli: &Cli, verb: ClusterVerb) -> ExitCode {
    match verb {
        ClusterVerb::Init { nodes } => {
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            let node_ids: Vec<u64> = match nodes.iter().map(|s| s.parse::<u64>()).collect::<Result<_, _>>() {
                Ok(ids) => ids,
                Err(e) => {
                    eprintln!("error: invalid node id: {e}");
                    return ExitCode::from(1);
                }
            };
            match crowdb_console_shared::ops::cluster::init(&ctx, &node_ids).await {
                Ok(summary) => {
                    if let Err(c) = commit_config(cli, &ctx) {
                        return c;
                    }
                    if cli.json {
                        return print_json(cli, &summary);
                    }
                    println!(
                        "cluster initialized: store {}, group {}, {} nodes",
                        summary.store_id,
                        summary.group_id,
                        summary.nodes.len()
                    );
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: cluster init: {e}");
                    ExitCode::from(2)
                }
            }
        }
        ClusterVerb::LocalDeploy {
            nodes,
            service_type,
            rpc_port,
            io_engines,
            io_workers,
            enable_nagle,
            rpc_workers,
            peer_pool_size,
            max_inflight,
            coalesce_max_keys,
            event_write,
            send_queue_capacity,
            metrics_interval,
            kv_backend,
            wal_backend,
            no_fsync,
            disk_groups_per_node,
            disks_per_group,
            disk_capacity_bytes,
            disk_zone_size_bytes,
            disk_unit_size_bytes,
            data_groups,
            kv_connections,
            kv_client_rpc_workers,
            diskdb_connections,
            diskdb_client_rpc_workers,
            chunkdb_instances,
            allow_unsafe_ec,
        } => match service_type.as_str() {
            "combined" => {
                let ctx = match op_context(cli) {
                    Ok(context) => context,
                    Err(code) => return code,
                };
                let workspace =
                    deploy_workspace(cli).unwrap_or_else(|| std::path::PathBuf::from("cli-deploy"));
                let tunables = crowdb_console_shared::ops::cluster::KvDeployTunables {
                    rpc_workers: nonzero(rpc_workers),
                    peer_pool_size: nonzero(peer_pool_size),
                    max_inflight: nonzero(max_inflight),
                    coalesce_max_keys: nonzero(coalesce_max_keys),
                    event_write: event_write.then_some(true),
                    send_queue_capacity: nonzero(send_queue_capacity),
                    metrics_interval: nonzero(metrics_interval),
                    kv_backend: (!kv_backend.is_empty()).then_some(kv_backend),
                    wal_backend: (!wal_backend.is_empty()).then_some(wal_backend),
                    no_fsync: no_fsync.then_some(true),
                };
                let disk = crowdb_console_shared::ops::cluster::LocalDiskdbDeployConfig {
                    disk_groups_per_node,
                    disks_per_group,
                    capacity_bytes: disk_capacity_bytes,
                    zone_size_bytes: disk_zone_size_bytes,
                    unit_size_bytes: disk_unit_size_bytes,
                    data_groups,
                    rpc_workers: nonzero(rpc_workers),
                    kv_connections: nonzero(kv_connections),
                    kv_client_rpc_workers: nonzero(kv_client_rpc_workers),
                };
                let chunk = crowdb_console_shared::ops::cluster::LocalChunkdbDeployConfig {
                    instance_count: chunkdb_instances,
                    allow_unsafe_ec,
                    rpc_workers: nonzero(rpc_workers),
                    kv_connections: nonzero(kv_connections),
                    kv_client_rpc_workers: nonzero(kv_client_rpc_workers),
                    diskdb_connections: nonzero(diskdb_connections),
                    diskdb_client_rpc_workers: nonzero(diskdb_client_rpc_workers),
                    metrics_interval: nonzero(metrics_interval),
                };
                match crowdb_console_shared::ops::cluster::local_deploy_combined(
                    &ctx,
                    &workspace,
                    Some(&tunables),
                    &disk,
                    &chunk,
                )
                .await
                {
                    Ok(summary) => {
                        if let Err(code) = commit_config(cli, &ctx) {
                            return code;
                        }
                        if cli.json {
                            return print_json(cli, &summary);
                        }
                        println!(
                            "local-deploy combined: {} KV nodes, {} racks, {} DiskDB, {} ChunkDB, {} DiskIO",
                            summary.kv_nodes,
                            summary.racks,
                            summary.diskdb_instances,
                            summary.chunkdb_instances,
                            summary.diskio_instances
                        );
                        ExitCode::SUCCESS
                    }
                    Err(error) => {
                        eprintln!("error: local-deploy combined: {error}");
                        ExitCode::from(2)
                    }
                }
            }
            "kv" => {
                let ctx = match op_context(cli) {
                    Ok(c) => c,
                    Err(c) => return c,
                };
                let tunables = crowdb_console_shared::ops::cluster::KvDeployTunables {
                    rpc_workers: nonzero(rpc_workers),
                    peer_pool_size: nonzero(peer_pool_size),
                    max_inflight: nonzero(max_inflight),
                    coalesce_max_keys: nonzero(coalesce_max_keys),
                    event_write: if event_write { Some(true) } else { None },
                    send_queue_capacity: nonzero(send_queue_capacity),
                    metrics_interval: nonzero(metrics_interval),
                    kv_backend: if kv_backend.is_empty() {
                        None
                    } else {
                        Some(kv_backend)
                    },
                    wal_backend: if wal_backend.is_empty() {
                        None
                    } else {
                        Some(wal_backend)
                    },
                    no_fsync: if no_fsync { Some(true) } else { None },
                };
                let workspace = deploy_workspace(cli);
                match crowdb_console_shared::ops::cluster::local_deploy(
                    &ctx,
                    nodes,
                    workspace.as_deref(),
                    Some(&tunables),
                )
                .await
                {
                    Ok(summary) => {
                        if let Err(c) = commit_config(cli, &ctx) {
                            return c;
                        }
                        if cli.json {
                            return print_json(cli, &summary);
                        }
                        println!(
                            "local-deploy complete: {} nodes (rack {}, nodes [{}]), group 0 bootstrapped",
                            summary.node_count,
                            summary.rack_id,
                            summary
                                .node_ids
                                .iter()
                                .map(std::string::ToString::to_string)
                                .collect::<Vec<_>>()
                                .join(", ")
                        );
                        ExitCode::SUCCESS
                    }
                    Err(e) => {
                        eprintln!("error: local-deploy: {e}");
                        ExitCode::from(2)
                    }
                }
            }
            "rpc" => {
                let ctx = match op_context(cli) {
                    Ok(c) => c,
                    Err(c) => return c,
                };
                let rpc_cfg = crowdb_console_shared::ops::cluster::RpcDeployConfig {
                    port: rpc_port,
                    io_engines,
                    io_workers,
                    enable_nagle,
                    ..Default::default()
                };
                let workspace = deploy_workspace(cli);
                match crowdb_console_shared::ops::cluster::local_deploy_rpc(
                    &ctx,
                    &rpc_cfg,
                    workspace.as_deref(),
                )
                .await
                {
                    Ok(summary) => {
                        if let Err(c) = commit_config(cli, &ctx) {
                            return c;
                        }
                        if cli.json {
                            return print_json(cli, &summary);
                        }
                        println!(
                            "local-deploy rpc: port={}, pid={}, io_engines={}, io_workers={}, nagle={}",
                            summary.port, summary.pid, summary.io_engines, summary.io_workers, summary.nagle
                        );
                        ExitCode::SUCCESS
                    }
                    Err(e) => {
                        eprintln!("error: local-deploy rpc: {e}");
                        ExitCode::from(2)
                    }
                }
            }
            "diskdb" => {
                let ctx = match op_context(cli) {
                    Ok(c) => c,
                    Err(c) => return c,
                };
                let workspace =
                    deploy_workspace(cli).unwrap_or_else(|| std::path::PathBuf::from("cli-deploy"));
                let config = crowdb_console_shared::ops::cluster::LocalDiskdbDeployConfig {
                    disk_groups_per_node,
                    disks_per_group,
                    capacity_bytes: disk_capacity_bytes,
                    zone_size_bytes: disk_zone_size_bytes,
                    unit_size_bytes: disk_unit_size_bytes,
                    data_groups,
                    rpc_workers: nonzero(rpc_workers),
                    kv_connections: nonzero(kv_connections),
                    kv_client_rpc_workers: nonzero(kv_client_rpc_workers),
                };
                match crowdb_console_shared::ops::cluster::local_deploy_diskdb(&ctx, &workspace, &config)
                    .await
                {
                    Ok(summary) => {
                        if let Err(code) = commit_config(cli, &ctx) {
                            return code;
                        }
                        if cli.json {
                            return print_json(cli, &summary);
                        }
                        println!(
                            "local-deploy diskdb: {} instances, {} disk-groups, {} disks, data-groups {:?}",
                            summary.instance_count,
                            summary.disk_group_count,
                            summary.disk_count,
                            summary.data_groups
                        );
                        ExitCode::SUCCESS
                    }
                    Err(error) => {
                        eprintln!("error: local-deploy diskdb: {error}");
                        ExitCode::from(2)
                    }
                }
            }
            "chunkdb" => {
                let ctx = match op_context(cli) {
                    Ok(context) => context,
                    Err(code) => return code,
                };
                let workspace =
                    deploy_workspace(cli).unwrap_or_else(|| std::path::PathBuf::from("cli-deploy"));
                let chunk = crowdb_console_shared::ops::cluster::LocalChunkdbDeployConfig {
                    instance_count: chunkdb_instances,
                    allow_unsafe_ec,
                    rpc_workers: nonzero(rpc_workers),
                    kv_connections: nonzero(kv_connections),
                    kv_client_rpc_workers: nonzero(kv_client_rpc_workers),
                    diskdb_connections: nonzero(diskdb_connections),
                    diskdb_client_rpc_workers: nonzero(diskdb_client_rpc_workers),
                    metrics_interval: nonzero(metrics_interval),
                };
                match crowdb_console_shared::ops::cluster::local_deploy_chunkdb(&ctx, &workspace, &chunk)
                    .await
                {
                    Ok(summary) => {
                        if let Err(code) = commit_config(cli, &ctx) {
                            return code;
                        }
                        if cli.json {
                            return print_json(cli, &summary);
                        }
                        println!("local-deploy chunkdb: {} instances", summary.instance_count);
                        ExitCode::SUCCESS
                    }
                    Err(error) => {
                        eprintln!("error: local-deploy chunkdb: {error}");
                        ExitCode::from(2)
                    }
                }
            }
            other => {
                eprintln!(
                    "error: local-deploy: unsupported service type `{other}` (expected kv, diskdb, chunkdb, combined, or rpc)"
                );
                ExitCode::from(1)
            }
        },
        ClusterVerb::Destroy => {
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::cluster::destroy(&ctx).await {
                Ok(()) => {
                    if let Err(c) = commit_config(cli, &ctx) {
                        return c;
                    }
                    if !cli.json {
                        println!("cluster destroy complete");
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: cluster destroy: {e}");
                    ExitCode::from(2)
                }
            }
        }
        ClusterVerb::Reset => {
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::cluster::reset(&ctx).await {
                Ok(()) => {
                    if !cli.json {
                        println!("cluster reset complete");
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: cluster reset: {e}");
                    ExitCode::from(2)
                }
            }
        }
        ClusterVerb::Clean { store, group } => {
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::cluster::clean(&ctx, store, group).await {
                Ok(result) => {
                    if cli.json {
                        return print_json(cli, &result);
                    }
                    println!(
                        "cluster clean: wiped {} nodes, leader = {}",
                        result.wiped_nodes, result.new_leader
                    );
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: cluster clean: {e}");
                    ExitCode::from(2)
                }
            }
        }
        ClusterVerb::Status => {
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::cluster::status(&ctx).await {
                Ok(stores) => {
                    if cli.json {
                        return print_json(cli, &stores);
                    }
                    if stores.is_empty() {
                        println!("(no stores)");
                    } else {
                        println!("{:<12}  {:<12}  NODES", "STORE", "REPLICAS");
                        for s in &stores {
                            println!(
                                "{:<12}  {:<12}  {}",
                                s.store_id,
                                s.node_ids.len(),
                                s.node_ids
                                    .iter()
                                    .map(std::string::ToString::to_string)
                                    .collect::<Vec<_>>()
                                    .join(",")
                            );
                        }
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: cluster status: {e}");
                    ExitCode::from(2)
                }
            }
        }
        ClusterVerb::Topology { node } => {
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            let node_id: u64 = match node.parse() {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("error: invalid node id: {e}");
                    return ExitCode::from(1);
                }
            };
            match crowdb_console_shared::ops::cluster::topology(&ctx, node_id).await {
                Ok(stores) => {
                    if cli.json {
                        return print_json(cli, &stores);
                    }
                    for s in &stores {
                        println!(
                            "store {} listen={}",
                            s.store_id,
                            s.listen_addr.as_deref().unwrap_or("-")
                        );
                        for g in &s.groups {
                            println!("  group {} leader={}", g.group_id, g.local_replica_id);
                        }
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: cluster topology: {e}");
                    ExitCode::from(2)
                }
            }
        }
        ClusterVerb::Rack { verb } => run_rack_verb(cli, verb).await,
        ClusterVerb::Node { verb } => run_node_verb(cli, verb).await,
        ClusterVerb::DiskGroup { verb } => run_disk_group_verb(cli, verb).await,
        ClusterVerb::Disk { verb } => run_disk_verb(cli, verb).await,
    }
}

/// Convert 0 to `None` (use server default), pass through nonzero values.
fn nonzero<T: Copy + PartialEq + Default>(v: T) -> Option<T> {
    (v != T::default()).then_some(v)
}

/// Resolve the `local-deploy` workspace from the CLI's per-invocation
/// log dir. The workspace lands at `<log_dir>/deploy/` so server data
/// dirs stay separate from the CLI's own log files. Returns `None`
/// (falling back to the lib default) when `log_dir` is unset.
fn deploy_workspace(cli: &Cli) -> Option<std::path::PathBuf> {
    if cli.log_dir.as_os_str().is_empty() {
        None
    } else {
        Some(cli.log_dir.join("deploy"))
    }
}
