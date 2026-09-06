// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! [`OpContext`] — shared connection state for all `ops` functions.

use std::sync::{Arc, RwLock};

use crowdb_kv_client::{ClientConfig, CrowdbKvClient, CrowdbSysmdClient, ServiceDiscoveryClient};

use crate::config::{ConsoleConfig, NodeEntry, ServerEntry};
use crate::error::{Error, Result};

/// Shared context for all `ops` domain functions.
///
/// Holds:
/// - **`sysmd`** — a [`CrowdbSysmdClient`] for group-0 system metadata
///   (hardware hierarchy, KV-cluster topology, service registry).
/// - **`kv`** — a [`CrowdbKvClient`] for the KV data-plane (put/get/
///   delete/scan on user stores/groups).
/// - **`config`** — the local TOML [`ConsoleConfig`] (rack/node/server
///   entries, bootstrap state). Mutated under an `RwLock` and persisted
///   by the caller via the engine.
/// - **`discovery`** — an optional [`ServiceDiscoveryClient`] for
///   discovering living service instances (diskdb, chunkdb, etc.) via
///   the group-0 service registry. `None` when the caller (e.g. a unit
///   test) passes explicit addresses instead of using discovery.
///
/// Both `sysmd` and `kv` wrap the same underlying `Arc<CrowdbKvClient>`
/// so the topology cache and connection pool are shared.
pub struct OpContext {
    sysmd: CrowdbSysmdClient,
    kv: Arc<CrowdbKvClient>,
    config: RwLock<ConsoleConfig>,
    discovery: Option<Arc<ServiceDiscoveryClient>>,
    test_scenario: bool,
}

impl std::fmt::Debug for OpContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpContext")
            .field("sysmd", &"CrowdbSysmdClient")
            .field("kv", &"Arc<CrowdbKvClient>")
            .field("config", &self.config)
            .field("discovery", &self.discovery.is_some())
            .field("test_scenario", &self.test_scenario)
            .finish()
    }
}

impl OpContext {
    /// Build an `OpContext` from a group-0 endpoint (e.g.
    /// `127.0.0.1:10100`) and an initial [`ConsoleConfig`].
    ///
    /// The `group0_endpoint` is used to seed the topology cache for
    /// group 0 (store 0, group 0). The mgmt URLs of all group-0 hosting
    /// nodes are passed as topology-discovery seeds so the client can
    /// find a new leader when the seeded one is down.
    #[must_use]
    pub fn new(group0_endpoint: String, mgmt_seeds: Vec<String>, config: ConsoleConfig) -> Self {
        Self::build(group0_endpoint, mgmt_seeds, config, false)
    }

    /// Build an `OpContext` for tests that intentionally omit a live group-0
    /// cluster. Best-effort sysdata synchronization is skipped in this mode.
    #[must_use]
    pub fn new_for_test(group0_endpoint: String, mgmt_seeds: Vec<String>, config: ConsoleConfig) -> Self {
        Self::build(group0_endpoint, mgmt_seeds, config, true)
    }

    fn build(
        group0_endpoint: String,
        mgmt_seeds: Vec<String>,
        config: ConsoleConfig,
        test_scenario: bool,
    ) -> Self {
        let mut seeds = mgmt_seeds;
        if !seeds.iter().any(|s| s == &group0_endpoint) {
            seeds.push(group0_endpoint.clone());
        }
        let kv = CrowdbKvClient::new(ClientConfig::new(seeds));
        kv.seed_leader(0, 0, group0_endpoint);
        let shared = Arc::new(kv);
        let sysmd = CrowdbSysmdClient::from_shared(Arc::clone(&shared));
        let discovery = Some(Arc::new(ServiceDiscoveryClient::from_shared_kv(Arc::clone(
            &shared,
        ))));
        Self {
            sysmd,
            kv: shared,
            config: RwLock::new(config),
            discovery,
            test_scenario,
        }
    }

    /// Build an `OpContext` from a pre-built shared [`CrowdbKvClient`].
    ///
    /// Used by the web backend (`AppState::op_context`) to share the
    /// cached client (topology cache + connection pool) across requests
    /// rather than building a new one per request. `group0_endpoint`
    /// seeds the leader hint for store 0 / group 0; `mgmt_seeds` are
    /// additional topology-discovery seeds (already in the shared
    /// client's config — accepted for API symmetry with [`Self::new`]).
    #[must_use]
    pub fn with_shared_client(
        kv: Arc<CrowdbKvClient>,
        group0_endpoint: String,
        mgmt_seeds: &[String],
        config: ConsoleConfig,
    ) -> Self {
        if !mgmt_seeds.is_empty() {
            kv.set_mgmt_seeds(mgmt_seeds.to_vec());
        }
        kv.seed_leader(0, 0, group0_endpoint);
        let sysmd = CrowdbSysmdClient::from_shared(Arc::clone(&kv));
        let discovery = Some(Arc::new(ServiceDiscoveryClient::from_shared_kv(Arc::clone(&kv))));
        Self {
            sysmd,
            kv,
            config: RwLock::new(config),
            discovery,
            test_scenario: false,
        }
    }

    /// Build an `OpContext` from a pre-built shared [`CrowdbKvClient`]
    /// without overwriting the existing group-0 leader hint.
    ///
    /// Like [`Self::with_shared_client`] but does NOT call
    /// `kv.seed_leader(0, 0, ...)`. Use this when the shared client
    /// may already have a better leader hint from a prior
    /// `ops::cluster::init` or from a `NotLeaderHint` response, and
    /// the caller only has a fallback (possibly stale) endpoint.
    /// `mgmt_seeds` are still applied so topology discovery can find
    /// new nodes.
    #[must_use]
    pub fn with_shared_client_preserving_hint(
        kv: Arc<CrowdbKvClient>,
        mgmt_seeds: &[String],
        config: ConsoleConfig,
    ) -> Self {
        if !mgmt_seeds.is_empty() {
            kv.set_mgmt_seeds(mgmt_seeds.to_vec());
        }
        let sysmd = CrowdbSysmdClient::from_shared(Arc::clone(&kv));
        let discovery = Some(Arc::new(ServiceDiscoveryClient::from_shared_kv(Arc::clone(&kv))));
        Self {
            sysmd,
            kv,
            config: RwLock::new(config),
            discovery,
            test_scenario: false,
        }
    }

    /// Access the [`CrowdbSysmdClient`] for group-0 system metadata.
    #[must_use]
    pub fn sysmd(&self) -> &CrowdbSysmdClient {
        &self.sysmd
    }

    /// Whether this context intentionally represents a test scenario without
    /// a live group-0 cluster.
    #[must_use]
    pub fn is_test_scenario(&self) -> bool {
        self.test_scenario
    }

    /// Access the [`CrowdbKvClient`] for the KV data-plane.
    #[must_use]
    pub fn kv(&self) -> &CrowdbKvClient {
        &self.kv
    }

    /// Re-seed the group-0 leader hint. Called after deploying servers
    /// (e.g. `local_deploy`) so subsequent sysdata writes target the
    /// correct endpoint.
    pub fn seed_group0_leader(&self, endpoint: String) {
        self.kv.seed_leader(0, 0, endpoint);
    }

    /// Access the shared [`Arc<CrowdbKvClient>`] for the KV data-plane.
    /// Used by the web backend to verify the client is shared (not
    /// duplicated) between `AppState` and `OpContext`.
    #[must_use]
    pub fn kv_arc(&self) -> &Arc<CrowdbKvClient> {
        &self.kv
    }

    /// Read-lock the [`ConsoleConfig`].
    ///
    /// # Panics
    /// Panics if the `RwLock` is poisoned.
    pub fn config(&self) -> std::sync::RwLockReadGuard<'_, ConsoleConfig> {
        self.config.read().unwrap()
    }

    /// Write-lock the [`ConsoleConfig`].
    ///
    /// # Panics
    /// Panics if the `RwLock` is poisoned.
    pub fn config_mut(&self) -> std::sync::RwLockWriteGuard<'_, ConsoleConfig> {
        self.config.write().unwrap()
    }

    /// Look up a [`NodeEntry`] by id from the local config.
    ///
    /// # Errors
    /// Returns [`Error::NotFound`] if the node does not exist.
    pub fn node_entry(&self, node_id: u64) -> Result<NodeEntry> {
        self.config()
            .node(node_id)
            .cloned()
            .ok_or_else(|| Error::NotFound {
                kind: "node".into(),
                id: node_id.to_string(),
            })
    }

    /// Look up the [`ServerEntry`] deployed on a node, from the local
    /// config.
    ///
    /// # Errors
    /// Returns [`Error::NotFound`] if no server is deployed on the node.
    pub fn server_for_node(&self, node_id: u64) -> Result<ServerEntry> {
        self.config()
            .server_for_node(node_id)
            .cloned()
            .ok_or_else(|| Error::NotFound {
                kind: "server".into(),
                id: node_id.to_string(),
            })
    }

    /// Resolve the HTTP management URL for a node's deployed
    /// `crowdb-kv-server`. This is the `ServerEntry.url` field (e.g.
    /// `http://127.0.0.1:10000`).
    ///
    /// # Errors
    /// Returns [`Error::NotFound`] if no server is deployed on the node.
    pub fn node_mgmt_url(&self, node_id: u64) -> Result<String> {
        Ok(self.server_for_node(node_id)?.url)
    }

    /// Access the [`ServiceDiscoveryClient`] for discovering living
    /// service instances via the group-0 service registry. Returns
    /// `None` when the context was built without discovery (e.g. unit
    /// tests that pass explicit addresses).
    #[must_use]
    pub fn discovery(&self) -> Option<&Arc<ServiceDiscoveryClient>> {
        self.discovery.as_ref()
    }

    /// Access the [`ServiceDiscoveryClient`], returning an error if
    /// discovery is not configured. Used by ops functions that require
    /// discovery (diskdb/chunkdb commands). Callers that accept an
    /// explicit address override should check the override first and
    /// only call this when the override is `None`.
    ///
    /// # Errors
    /// Returns [`Error::NotImplemented`] with a message explaining that
    /// discovery is not configured, if the context was built without a
    /// discovery client.
    pub fn discovery_or_error(&self) -> Result<&Arc<ServiceDiscoveryClient>> {
        self.discovery.as_ref().ok_or_else(|| {
            Error::NotImplemented(
                "service discovery is not configured — this context was built without a \
             ServiceDiscoveryClient (unit test?). Pass an explicit address instead."
                    .into(),
            )
        })
    }
}
