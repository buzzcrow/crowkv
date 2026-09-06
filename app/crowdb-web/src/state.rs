// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crowdb_console_shared::error::{Error, Result};
use crowdb_console_shared::monitor::MonitorCache;
use crowdb_console_shared::ops::OpContext;
use crowdb_console_shared::{
    config::{ConsoleConfigEngine, ServerEntry, TomlFileEngine},
    ConsoleConfig,
};

/// Shared, mutable console state.
///
/// `config` carries the full `ConsoleConfig` (racks, nodes, servers)
/// behind a `RwLock`; mutations are persisted via `ConsoleConfig::save`
/// to `config_path` when present.
///
/// `diskdb_client` is lazily initialized on the first `/api/diskdb/*`
/// request (the service registry may not be ready at console startup).
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<RwLock<ConsoleConfig>>,
    pub config_engine: Option<Arc<dyn ConsoleConfigEngine>>,
    pub runtime_root: Arc<PathBuf>,
    pub monitor_cache: Arc<MonitorCache>,
    pub runtime_pids: Arc<std::sync::Mutex<HashMap<String, u32>>>,
    pub diskdb_client: Arc<tokio::sync::RwLock<Option<crowdb_diskdb_client::DiskdbClient>>>,
    /// Cached crowdb-rpc transport reused across KV requests to avoid
    /// spawning 6+ threads per request. Shared by the cached `kv_client`.
    pub kv_rpc_transport: Arc<tokio::sync::RwLock<Option<Arc<crowdb_kv_client::KvRpcTransport>>>>,
    /// Cached `CrowdbKvClient` reused across KV requests so the topology
    /// cache persists — avoids re-discovering the leader from seeds on
    /// every put/get/delete. Invalidated on `/internal/reset`.
    pub kv_client: Arc<tokio::sync::RwLock<Option<Arc<crowdb_kv_client::CrowdbKvClient>>>>,
    /// Cached `ServiceDiscoveryClient` reused across requests so the
    /// discovery cache (service registry entries) persists. Built from
    /// the same `CrowdbKvClient` as `kv_client` so topology + connection
    /// pool are shared.
    pub discovery_client: Arc<tokio::sync::RwLock<Option<Arc<crowdb_kv_client::ServiceDiscoveryClient>>>>,
    /// Rate-limiter for repeated crowdb-rpc failure warnings: maps
    /// `endpoint` → last-warned timestamp. Prevents flooding the
    /// console with identical "instance query failed" warnings every
    /// poll cycle when a diskdb instance is unreachable.
    pub warn_dedup: Arc<std::sync::Mutex<HashMap<String, std::time::Instant>>>,
    /// Enables faster spawned-process intervals for E2E runs.
    pub test_mode: bool,
}

impl Default for AppState {
    fn default() -> Self {
        Self::with_config(ConsoleConfig::default(), None)
    }
}

impl AppState {
    /// Build state from a list of pre-registered server URLs. Each URL
    /// becomes a synthetic `ServerEntry` with id `srv{i}`. Used by tests
    /// and by callers that don't need a persistent registry.
    #[must_use]
    pub fn new(default_servers: Vec<String>) -> Self {
        let mut cfg = ConsoleConfig::default();
        for (i, url) in default_servers.into_iter().enumerate() {
            let _ = cfg.add_server(ServerEntry::new(format!("srv{i}"), url));
        }
        Self::with_config(cfg, None)
    }

    /// Build state from an already-loaded `ConsoleConfig`. `path` is the
    /// on-disk location used by mutating handlers to persist changes;
    /// pass `None` for in-memory-only state (tests).
    #[must_use]
    pub fn with_config(config: ConsoleConfig, path: Option<PathBuf>) -> Self {
        let runtime_root = path
            .as_ref()
            .and_then(|path| path.parent().map(std::path::Path::to_path_buf))
            .unwrap_or_else(|| PathBuf::from("runtime-data"));
        let engine = path.map(|path| Arc::new(TomlFileEngine::new(path)) as Arc<dyn ConsoleConfigEngine>);
        Self::with_config_engine(config, engine, runtime_root)
    }

    #[must_use]
    pub fn with_config_engine(
        config: ConsoleConfig,
        engine: Option<Arc<dyn ConsoleConfigEngine>>,
        runtime_root: PathBuf,
    ) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
            config_engine: engine,
            runtime_root: Arc::new(runtime_root),
            monitor_cache: Arc::new(MonitorCache::new()),
            runtime_pids: Arc::new(std::sync::Mutex::new(HashMap::new())),
            diskdb_client: Arc::new(tokio::sync::RwLock::new(None)),
            kv_rpc_transport: Arc::new(tokio::sync::RwLock::new(None)),
            kv_client: Arc::new(tokio::sync::RwLock::new(None)),
            discovery_client: Arc::new(tokio::sync::RwLock::new(None)),
            warn_dedup: Arc::new(std::sync::Mutex::new(HashMap::new())),
            test_mode: false,
        }
    }

    /// Enable or disable E2E test-mode behavior.
    #[must_use]
    pub fn with_test_mode(mut self, test_mode: bool) -> Self {
        self.test_mode = test_mode;
        self
    }

    /// Persist the current config to `config_path`, if one was provided.
    /// No-op for in-memory state.
    ///
    /// # Panics
    /// Panics if the `RwLock` is poisoned.
    ///
    /// # Errors
    /// Returns an error if config saving fails.
    pub fn persist(&self) -> crowdb_console_shared::error::Result<()> {
        if let Some(engine) = self.config_engine.as_ref() {
            let cfg = self.config.read().unwrap();
            cfg.save_with_engine(engine.as_ref())?;
        }
        Ok(())
    }

    /// Get the runtime PID for a node.
    ///
    /// # Panics
    /// Panics if the `Mutex` is poisoned.
    #[must_use]
    pub fn runtime_pid(&self, node_id: impl std::fmt::Display) -> Option<u32> {
        self.runtime_pids
            .lock()
            .unwrap()
            .get(&node_id.to_string())
            .copied()
    }

    /// Set the runtime PID for a node.
    ///
    /// # Panics
    /// Panics if the `Mutex` is poisoned.
    pub fn set_runtime_pid(&self, node_id: impl std::fmt::Display, pid: u32) {
        self.runtime_pids.lock().unwrap().insert(node_id.to_string(), pid);
    }

    /// Clear the runtime PID for a node.
    ///
    /// # Panics
    /// Panics if the `Mutex` is poisoned.
    pub fn clear_runtime_pid(&self, node_id: impl std::fmt::Display) {
        self.runtime_pids.lock().unwrap().remove(&node_id.to_string());
    }

    /// Snapshot of all tracked KV server PIDs, keyed by node id.
    /// Excludes DDB PIDs (keyed as `diskdb-{node_id}`).
    ///
    /// # Panics
    /// Panics if the `Mutex` is poisoned.
    #[must_use]
    pub fn kv_pid_snapshot(&self) -> std::collections::HashMap<u64, u32> {
        self.runtime_pids
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(k, v)| {
                if k.starts_with("diskdb-") {
                    None
                } else {
                    k.parse::<u64>().ok().map(|n| (n, *v))
                }
            })
            .collect()
    }

    /// Get the runtime PID for a diskdb instance on a node (R77).
    /// Keyed separately from kv-server PIDs.
    ///
    /// # Panics
    /// Panics if the `Mutex` is poisoned.
    #[must_use]
    pub fn diskdb_runtime_pid(&self, node_id: impl std::fmt::Display) -> Option<u32> {
        self.runtime_pids
            .lock()
            .unwrap()
            .get(&format!("diskdb-{node_id}"))
            .copied()
    }

    /// Snapshot of all tracked `DiskDB` PIDs, keyed by node id.
    ///
    /// # Panics
    /// Panics if the runtime PID mutex is poisoned.
    #[must_use]
    pub fn diskdb_pid_snapshot(&self) -> std::collections::HashMap<u64, u32> {
        self.runtime_pids
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(key, pid)| key.strip_prefix("diskdb-")?.parse().ok().map(|id| (id, *pid)))
            .collect()
    }

    /// Set the runtime PID for a diskdb instance on a node (R77).
    ///
    /// # Panics
    /// Panics if the `Mutex` is poisoned.
    pub fn set_diskdb_runtime_pid(&self, node_id: impl std::fmt::Display, pid: u32) {
        self.runtime_pids
            .lock()
            .unwrap()
            .insert(format!("diskdb-{node_id}"), pid);
    }

    /// Clear the runtime PID for a diskdb instance on a node (R77).
    ///
    /// # Panics
    /// Panics if the `Mutex` is poisoned.
    pub fn clear_diskdb_runtime_pid(&self, node_id: impl std::fmt::Display) {
        self.runtime_pids
            .lock()
            .unwrap()
            .remove(&format!("diskdb-{node_id}"));
    }

    #[must_use]
    pub fn node_workspace_dir(&self, node_id: impl std::fmt::Display) -> PathBuf {
        self.runtime_root.join(format!("N-{node_id}"))
    }

    /// Rate-limited warning: returns `true` if the caller should warn
    /// (at most once per `interval` per `key`), `false` if the same
    /// key was warned recently and should be suppressed.
    ///
    /// # Panics
    /// Panics if the `Mutex` is poisoned.
    #[must_use]
    pub fn should_warn(&self, key: &str, interval: std::time::Duration) -> bool {
        let mut map = self.warn_dedup.lock().unwrap();
        let now = std::time::Instant::now();
        if let Some(last) = map.get(key) {
            if now.duration_since(*last) < interval {
                return false;
            }
        }
        map.insert(key.to_string(), now);
        true
    }

    /// Remove all node workspace directories under `runtime_root`.
    /// Called by the internal reset endpoint after stopping servers.
    ///
    /// # Errors
    ///
    /// Returns an error if a directory cannot be removed.
    pub fn clear_workspaces(&self) -> Result<()> {
        if !self.runtime_root.exists() {
            return Ok(());
        }
        for entry in std::fs::read_dir(&*self.runtime_root).map_err(Error::Io)? {
            let entry = entry.map_err(Error::Io)?;
            let name = entry.file_name();
            if name.to_string_lossy().starts_with("N-") {
                std::fs::remove_dir_all(entry.path()).map_err(Error::Io)?;
            }
        }
        Ok(())
    }

    /// Prepares the workspace directory for a node.
    ///
    /// # Errors
    ///
    /// Returns an error if directory creation fails due to I/O errors.
    pub fn prepare_node_workspace(&self, node_id: impl std::fmt::Display) -> Result<PathBuf> {
        let base = self.node_workspace_dir(node_id);
        std::fs::create_dir_all(&base).map_err(Error::Io)?;
        std::fs::create_dir_all(base.join("bin")).map_err(Error::Io)?;
        std::fs::create_dir_all(base.join("log")).map_err(Error::Io)?;
        std::fs::create_dir_all(base.join("waldata")).map_err(Error::Io)?;
        std::fs::canonicalize(base).map_err(Error::Io)
    }

    /// Get or create a cached `KvRpcTransport`. The transport (and its
    /// crowdb-rpc server/client + I/O thread) is reused across KV requests
    /// to avoid spawning threads per request. Shared by the cached
    /// `kv_client`.
    pub async fn kv_rpc_transport(&self) -> Arc<crowdb_kv_client::KvRpcTransport> {
        if let Some(t) = self.kv_rpc_transport.read().await.as_ref() {
            return Arc::clone(t);
        }
        let mut guard = self.kv_rpc_transport.write().await;
        if let Some(t) = guard.as_ref() {
            return Arc::clone(t);
        }
        let t = Arc::new(crowdb_kv_client::KvRpcTransport::new());
        *guard = Some(Arc::clone(&t));
        t
    }

    /// Get or create a cached `CrowdbKvClient` sharing the cached RPC
    /// transport. The topology cache persists across requests so the
    /// leader endpoint is not re-discovered from seeds on every call.
    /// Call `seed_leader` before each use to keep the hint fresh.
    pub async fn kv_client(&self) -> Arc<crowdb_kv_client::CrowdbKvClient> {
        if let Some(c) = self.kv_client.read().await.as_ref() {
            return Arc::clone(c);
        }
        let mut guard = self.kv_client.write().await;
        if let Some(c) = guard.as_ref() {
            return Arc::clone(c);
        }
        let transport = self.kv_rpc_transport().await;
        let c = Arc::new(crowdb_kv_client::CrowdbKvClient::new_with_rpc_transport(
            crowdb_kv_client::ClientConfig::new(Vec::new()),
            transport,
        ));
        *guard = Some(Arc::clone(&c));
        c
    }

    /// Drop every client that retains cluster topology so the next
    /// request rebuilds from the post-reset cluster. The discovery and
    /// `DiskDB` clients both retain the shared `CrowdbKvClient`, so
    /// clearing only `kv_client` would leave its old group-0 leader hint
    /// reachable through those wrappers.
    pub async fn clear_cluster_clients(&self) {
        *self.diskdb_client.write().await = None;
        *self.discovery_client.write().await = None;
        *self.kv_client.write().await = None;
        if let Some(transport) = self.kv_rpc_transport.read().await.as_ref() {
            transport.clear_connections();
        }
    }

    /// Get or lazily build the cached `ServiceDiscoveryClient`. Shares
    /// the same `CrowdbKvClient` as `kv_client()` so topology + connection
    /// pool are shared. Used by web handlers that need to discover living
    /// service instances (diskdb, chunkdb) from the group-0 registry.
    pub async fn discovery_client(&self) -> Arc<crowdb_kv_client::ServiceDiscoveryClient> {
        if let Some(d) = self.discovery_client.read().await.as_ref() {
            return Arc::clone(d);
        }
        let mut guard = self.discovery_client.write().await;
        if let Some(d) = guard.as_ref() {
            return Arc::clone(d);
        }
        let kv = self.kv_client().await;
        let d = Arc::new(crowdb_kv_client::ServiceDiscoveryClient::from_shared_kv(kv));
        *guard = Some(Arc::clone(&d));
        d
    }

    /// Re-seed the cached `CrowdbKvClient`'s mgmt seed list from the
    /// current config. Called after cluster init (or server deploy) so
    /// the shared client can discover leaders for the new topology.
    /// No-op if no client is cached (it will be created with config
    /// seeds on the next `kv_client()` call).
    ///
    /// # Panics
    /// Panics if the config read lock is poisoned.
    pub async fn reseed_kv_client(&self) {
        let seeds = {
            let cfg = self.config.read().unwrap();
            cfg.servers
                .iter()
                .filter(|s| s.node_id.is_some())
                .map(|s| s.url.clone())
                .collect::<Vec<_>>()
        };
        if let Some(c) = self.kv_client.read().await.as_ref() {
            if !seeds.is_empty() {
                c.set_mgmt_seeds(seeds);
            }
        }
    }

    /// Build an [`OpContext`] for a single request, sharing the cached
    /// `CrowdbKvClient` (topology cache + connection pool) and a
    /// snapshot of the persisted [`ConsoleConfig`].
    ///
    /// The group-0 endpoint is resolved from the config's store-0
    /// hosting nodes. If group 0 is not yet initialized, the first
    /// deployed server's mgmt URL is used as a bootstrap seed. If no
    /// server is deployed at all (the rack/node add bootstrap path),
    /// a dummy endpoint is used — sysdata syncs silently fail, which
    /// is correct since there is no group-0 to sync to.
    ///
    /// Mutations inside `ops::*` functions mutate the `OpContext`'s
    /// own config snapshot; call [`Self::commit_op_context`] after a
    /// successful `ops::*` call to write the changes back to
    /// `AppState.config` + persist.
    ///
    /// # Errors
    /// Returns [`Error::Config`] if the config read lock is poisoned.
    pub async fn op_context(&self) -> Result<OpContext> {
        let kv = self.kv_client().await;

        // Snapshot the current config.
        let config_snapshot = {
            let cfg = self
                .config
                .read()
                .map_err(|e| Error::Config(format!("config read lock poisoned: {e}")))?;
            cfg.clone()
        };

        // Resolve mgmt seeds + a config-derived group-0 endpoint. The
        // config endpoint is the first node hosting store 0 — not
        // necessarily the leader — so it is NOT used to seed the leader
        // hint (see below). Only a live endpoint from the monitor cache
        // (the actual leader) overwrites the hint.
        let (_, mgmt_seeds) = resolve_group0_endpoint(&config_snapshot);
        let live_endpoint = self.monitor_cache.group0_leader_endpoint().await;

        // Only overwrite the leader hint with a live endpoint from the
        // monitor cache (the actual leader). A config-derived fallback
        // (`group0_endpoint`) is just the first node hosting store 0 —
        // not necessarily the leader. Overwriting with it clobbers the
        // correct hint set by a prior `ops::cluster::init` or
        // `NotLeaderHint`, causing sysdata writes to hit a non-leader
        // that returns "not leader" with no hint (mid-election),
        // exhausting retries. When no live endpoint is available,
        // preserve the existing hint; `resolve_leader` will discover
        // via `mgmt_seeds` if no hint is set.
        if let Some(ep) = live_endpoint {
            // Remap 0.0.0.0 → 127.0.0.1 and strip any http:// scheme so
            // the endpoint is a bare host:port for the RPC transport.
            let bare = crate::mgmt::strip_scheme(crate::mgmt::remap_zero_host(&ep));
            Ok(OpContext::with_shared_client(
                kv,
                bare,
                &mgmt_seeds,
                config_snapshot,
            ))
        } else {
            Ok(OpContext::with_shared_client_preserving_hint(
                kv,
                &mgmt_seeds,
                config_snapshot,
            ))
        }
    }

    /// Refresh the monitor cache for all nodes hosting group 0, then poll
    /// until a self-reported leader is observed among Up nodes. Returns
    /// `true` if a leader was found, `false` if the poll budget was
    /// exhausted. Used by handlers that do sysdata writes after stopping a
    /// node (e.g. `http_remove_node`) so the write targets the
    /// post-election leader instead of a stale hint.
    ///
    /// # Panics
    /// Panics if the `RwLock` is poisoned.
    pub async fn refresh_group0_leader(&self, exclude_node: Option<u64>) -> bool {
        let t0 = std::time::Instant::now();
        let group0_nodes: Vec<u64> = {
            let cfg = self.config.read().unwrap();
            cfg.group(0, 0)
                .map(|g| g.replicas.iter().map(|r| r.node_id).collect())
                .unwrap_or_default()
        };
        let alive: Vec<u64> = group0_nodes
            .iter()
            .filter(|&&n| exclude_node != Some(n) && self.runtime_pid(n).is_some())
            .copied()
            .collect();
        tracing::debug!(
            "refresh_group0_leader: start alive={:?} exclude={:?} group0_nodes={:?}",
            alive,
            exclude_node,
            group0_nodes
        );
        // No alive group-0 nodes to query — don't waste 2.25s on backoff
        // retries that can never find a leader (e.g. deleting the last
        // node, or cluster not yet initialized).
        if alive.is_empty() {
            tracing::debug!(
                "refresh_group0_leader: no alive nodes, skipping retries in {}ms",
                t0.elapsed().as_millis()
            );
            return false;
        }
        for attempt in 0..10u32 {
            futures::future::join_all(alive.iter().map(|&n| crate::mgmt::refresh_node_cache(self, n))).await;
            if self.monitor_cache.group0_leader_endpoint().await.is_some() {
                tracing::debug!(
                    "refresh_group0_leader: found leader after {} attempts in {}ms",
                    attempt + 1,
                    t0.elapsed().as_millis()
                );
                return true;
            }
            if attempt < 9 {
                tokio::time::sleep(std::time::Duration::from_millis(50u64 * (1 + u64::from(attempt)))).await;
            }
        }
        tracing::warn!(
            "refresh_group0_leader: no leader found after 10 attempts in {}ms alive={:?}",
            t0.elapsed().as_millis(),
            alive
        );
        false
    }

    /// Write a mutated [`OpContext`]'s config back to `AppState.config`
    /// and persist it. Called by handlers after a successful `ops::*`
    /// call that mutated the config.
    ///
    /// The write-back is a short critical section with no `await`
    /// inside the lock — the `OpContext`'s config is cloned in, the
    /// old config is replaced, and the lock is released before
    /// persistence (which may do file I/O).
    ///
    /// # Errors
    /// Returns an error if the config lock is poisoned or persistence
    /// fails.
    pub fn commit_op_context(&self, ctx: &OpContext) -> Result<()> {
        let new_config = ctx.config().clone();
        {
            let mut cfg = self
                .config
                .write()
                .map_err(|e| Error::Config(format!("config write lock poisoned: {e}")))?;
            *cfg = new_config;
        }
        self.persist()
    }
}

/// Resolve the group-0 RPC endpoint + mgmt seeds from a [`ConsoleConfig`]
/// snapshot.
///
/// Returns the `rpc_url` of a node hosting store 0, or `None` if no
/// store 0 exists or no hosting node has a known `rpc_url`. The mgmt
/// seeds (all deployed servers' HTTP URLs) are always returned for
/// topology discovery — `cluster init` uses them via `ServerClient`
/// (HTTP), not via the KV client (RPC).
///
/// Does NOT fall back to a server's HTTP mgmt URL: that is not an RPC
/// endpoint, and seeding it as the KV client's group-0 leader hint
/// causes puts to hang for the full RPC reaper timeout (5 s) before
/// the transport error triggers a topology refresh.
fn resolve_group0_endpoint(config: &ConsoleConfig) -> (Option<String>, Vec<String>) {
    let mgmt_seeds: Vec<String> = config
        .servers
        .iter()
        .filter(|s| s.node_id.is_some())
        .map(|s| s.url.clone())
        .filter(|u| !u.is_empty())
        .collect();

    // Only return a real RPC endpoint from a node hosting store 0.
    if let Some(store0) = config.stores.iter().find(|s| s.store_id == 0) {
        for node_id in &store0.nodes {
            if let Some(server) = config.server_for_node(*node_id) {
                if let Some(rpc_url) = &server.rpc_url {
                    if !rpc_url.is_empty() {
                        return (Some(rpc_url.clone()), mgmt_seeds);
                    }
                }
            }
        }
    }

    (None, mgmt_seeds)
}

/// Compile-time path to the React SPA build output. The `ui/`
/// project is within the `crowdb-web` crate; running
/// `npm run build` (or `make ui-build`) populates `dist/`.
pub const FRONTEND_DIST: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/ui/dist");

#[cfg(test)]
mod tests {
    use super::*;

    static CWD_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn tempdir(tag: &str) -> PathBuf {
        let base = crowdb_test_harness::test_dirs::test_data_dir();
        let unique = format!(
            "crowdb-web-state-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let dir = base.join(unique);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn prepare_node_workspace_returns_absolute_path_for_relative_runtime_root() {
        let _guard = CWD_LOCK.lock().unwrap();
        let original_cwd = std::env::current_dir().unwrap();
        let root = tempdir("relative-runtime-root");
        std::env::set_current_dir(&root).unwrap();

        let state =
            AppState::with_config_engine(ConsoleConfig::default(), None, PathBuf::from("runtime-data"));
        let workspace = state.prepare_node_workspace("n1").unwrap();

        assert!(workspace.is_absolute());
        assert!(workspace.ends_with(PathBuf::from("runtime-data/N-n1")));
        assert!(workspace.join("bin").is_dir());
        assert!(workspace.join("log").is_dir());
        assert!(workspace.join("waldata").is_dir());

        std::env::set_current_dir(original_cwd).unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn resolve_group0_endpoint_no_servers_returns_none() {
        let config = ConsoleConfig::default();
        let (endpoint, seeds) = resolve_group0_endpoint(&config);
        assert!(endpoint.is_none(), "no servers → no endpoint");
        assert!(seeds.is_empty(), "no servers → no seeds");
    }

    #[test]
    fn resolve_group0_endpoint_no_store0_returns_none() {
        let mut config = ConsoleConfig::default();
        let _ = config.add_server(ServerEntry {
            id: "s1".into(),
            url: "http://127.0.0.1:10000".into(),
            node_id: Some(1),
            rpc_url: None,
            rest_port: Some(10000),
            rpc_port: Some(10100),
            auto_start: true,
            binary: None,
            election_profile: None,
            pid: None,
            service_type: crowdb_console_shared::config::ServiceType::default(),
            rpc_workers: None,
            no_fsync: false,
        });
        let (endpoint, seeds) = resolve_group0_endpoint(&config);
        assert!(endpoint.is_none(), "no store 0 → no RPC endpoint to seed");
        assert_eq!(seeds, vec!["http://127.0.0.1:10000"]);
    }

    #[test]
    fn resolve_group0_endpoint_prefers_store0_rpc_url() {
        let mut config = ConsoleConfig::default();
        let _ = config.add_server(ServerEntry {
            id: "s1".into(),
            url: "http://127.0.0.1:10000".into(),
            node_id: Some(1),
            rpc_url: Some("http://127.0.0.1:10100".into()),
            rest_port: Some(10000),
            rpc_port: Some(10100),
            auto_start: true,
            binary: None,
            election_profile: None,
            pid: None,
            service_type: crowdb_console_shared::config::ServiceType::default(),
            rpc_workers: None,
            no_fsync: false,
        });
        config.record_store(0, vec![1]);
        let (endpoint, seeds) = resolve_group0_endpoint(&config);
        assert_eq!(endpoint.as_deref(), Some("http://127.0.0.1:10100"));
        assert_eq!(seeds, vec!["http://127.0.0.1:10000"]);
    }

    #[tokio::test]
    async fn op_context_no_deployed_server_succeeds_with_dummy_endpoint() {
        let state = AppState::default();
        let ctx = state.op_context().await.expect("op_context with no servers");
        // The OpContext should still be usable for config-only operations.
        // Sysdata syncs will silently fail (best-effort).
        let racks = ctx.config().racks.clone();
        assert!(racks.is_empty(), "default config has no racks");
    }

    #[tokio::test]
    async fn op_context_shares_cached_kv_client() {
        let mut config = ConsoleConfig::default();
        let _ = config.add_server(ServerEntry {
            id: "s1".into(),
            url: "http://127.0.0.1:10000".into(),
            node_id: Some(1),
            rpc_url: Some("http://127.0.0.1:10100".into()),
            rest_port: Some(10000),
            rpc_port: Some(10100),
            auto_start: true,
            binary: None,
            election_profile: None,
            pid: None,
            service_type: crowdb_console_shared::config::ServiceType::default(),
            rpc_workers: None,
            no_fsync: false,
        });
        let state = AppState::with_config(config, None);

        // Build the OpContext — it should share the cached kv_client.
        let ctx = state.op_context().await.expect("op_context with server");

        // The OpContext's kv Arc should be the same pointer as the
        // AppState's cached kv_client (no duplicate connection pool).
        let cached = state.kv_client().await;
        assert!(
            Arc::ptr_eq(ctx.kv_arc(), &cached),
            "OpContext must share the cached CrowdbKvClient"
        );
    }

    #[tokio::test]
    async fn clear_cluster_clients_drops_every_topology_owner() {
        let state = AppState::default();
        let old_kv = state.kv_client().await;
        let old_discovery = state.discovery_client().await;
        *state.diskdb_client.write().await = Some(crowdb_diskdb_client::DiskdbClient::new(
            old_discovery.registry().clone(),
            Arc::new(crowdb_diskdb_client::DiskdbRpcTransport::new()),
        ));

        state.clear_cluster_clients().await;

        assert!(state.kv_client.read().await.is_none());
        assert!(state.discovery_client.read().await.is_none());
        assert!(state.diskdb_client.read().await.is_none());

        let rebuilt_kv = state.kv_client().await;
        assert!(
            !Arc::ptr_eq(&old_kv, &rebuilt_kv),
            "a reset must not reuse the prior topology cache"
        );
    }

    #[tokio::test]
    async fn commit_op_context_writes_back_config() {
        let mut config = ConsoleConfig::default();
        let _ = config.add_server(ServerEntry {
            id: "s1".into(),
            url: "http://127.0.0.1:10000".into(),
            node_id: Some(1),
            rpc_url: Some("http://127.0.0.1:10100".into()),
            rest_port: Some(10000),
            rpc_port: Some(10100),
            auto_start: true,
            binary: None,
            election_profile: None,
            pid: None,
            service_type: crowdb_console_shared::config::ServiceType::default(),
            rpc_workers: None,
            no_fsync: false,
        });
        let state = AppState::with_config(config, None);

        let ctx = state.op_context().await.expect("op_context");
        // Mutate the OpContext's config snapshot (simulating an
        // ops::hardware::add_rack call).
        {
            let mut cfg = ctx.config_mut();
            let _ = cfg.add_rack(crowdb_console_shared::config::RackEntry {
                id: 42,
                name: "test-rack".into(),
            });
        }
        // Write back.
        state.commit_op_context(&ctx).expect("commit");

        // The AppState's config should now contain the rack.
        let cfg = state.config.read().unwrap();
        assert!(
            cfg.racks.iter().any(|r| r.id == 42),
            "commit_op_context must write the mutated config back"
        );
    }
}
