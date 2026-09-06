// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Console configuration: persisted registry of `crowdb-kv-server` instances.
//!
//! C2 status: file-backed `[[server]]` list; later phases extend with
//! racks, nodes, ssh creds. The struct is the single source of truth so
//! the storage format can evolve without touching call sites.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crowdb_protocol::{NodeId, RackId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

static TMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

use crate::cluster::DiskGroupId;
use crate::error::{Error, Result};

use std::fmt;

/// Serde helper: serialize a `BTreeMap<u64, V>` with string keys (TOML
/// requires string keys) and deserialize back to `u64` keys.
mod int_key {
    use serde::de::{Deserialize, Deserializer, MapAccess, Visitor};
    use serde::ser::{Serialize, Serializer};
    use std::collections::BTreeMap;
    use std::fmt;
    use std::marker::PhantomData;
    use std::str::FromStr;

    pub fn serialize<K, V, S>(map: &BTreeMap<K, V>, serializer: S) -> Result<S::Ok, S::Error>
    where
        K: ToString + Ord,
        V: Serialize,
        S: Serializer,
    {
        let string_map: BTreeMap<String, &V> = map.iter().map(|(k, v)| (k.to_string(), v)).collect();
        string_map.serialize(serializer)
    }

    pub fn deserialize<'de, K, V, D>(deserializer: D) -> Result<BTreeMap<K, V>, D::Error>
    where
        K: FromStr + Ord,
        K::Err: fmt::Display,
        V: Deserialize<'de>,
        D: Deserializer<'de>,
    {
        struct IntKeyVisitor<K, V>(PhantomData<(K, V)>);

        impl<'de, K, V> Visitor<'de> for IntKeyVisitor<K, V>
        where
            K: FromStr + Ord,
            K::Err: fmt::Display,
            V: Deserialize<'de>,
        {
            type Value = BTreeMap<K, V>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a map with string-encoded integer keys")
            }

            fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut map = BTreeMap::new();
                while let Some((key, value)) = access.next_entry::<String, V>()? {
                    let k = K::from_str(&key).map_err(serde::de::Error::custom)?;
                    map.insert(k, value);
                }
                Ok(map)
            }
        }

        deserializer.deserialize_map(IntKeyVisitor::<K, V>(PhantomData))
    }
}

pub trait ConsoleConfigEngine: Send + Sync {
    /// Load the console configuration from the engine's storage.
    ///
    /// # Errors
    /// Returns an error if loading fails (e.g., file not found, parse error).
    fn load(&self) -> Result<ConsoleConfig>;

    /// Save the console configuration to the engine's storage.
    ///
    /// # Errors
    /// Returns an error if saving fails (e.g., permission denied, write error).
    fn save(&self, config: &ConsoleConfig) -> Result<()>;
}

#[derive(Clone, PartialEq, Eq)]
pub struct TomlFileEngine {
    path: PathBuf,
}

impl fmt::Debug for TomlFileEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TomlFileEngine")
            .field("path", &self.path)
            .finish()
    }
}

impl TomlFileEngine {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub fn default_path() -> Option<PathBuf> {
        Some(PathBuf::from("runtime-data/crowdb-kv.db.toml"))
    }

    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn from_default_path() -> Option<Self> {
        Self::default_path().map(Self::new)
    }
}

impl ConsoleConfigEngine for TomlFileEngine {
    fn load(&self) -> Result<ConsoleConfig> {
        match std::fs::read_to_string(&self.path) {
            Ok(body) => ConsoleConfig::from_toml_str(&body, &self.path),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(ConsoleConfig::default()),
            Err(e) => Err(Error::Io(e)),
        }
    }

    fn save(&self, config: &ConsoleConfig) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent).map_err(Error::Io)?;
        }
        let body = config.to_toml_string()?;
        let seq = TMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp = self.path.with_extension(format!("toml.tmp.{seq}"));
        std::fs::write(&tmp, body).map_err(Error::Io)?;
        std::fs::rename(&tmp, &self.path).map_err(Error::Io)?;
        Ok(())
    }
}

/// On-disk console config. New top-level fields land in later phases
/// (ssh defaults, etc.). Unknown fields are ignored on load and dropped
/// on save (`serde(default)` everywhere) to keep migrations easy.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsoleConfig {
    #[serde(default, rename = "rack")]
    pub racks: Vec<RackEntry>,
    #[serde(default, rename = "node")]
    pub nodes: Vec<NodeEntry>,
    #[serde(default, rename = "server")]
    pub servers: Vec<ServerEntry>,
    #[serde(default)]
    pub stores: Vec<StoreEntry>,
    #[serde(default)]
    pub groups: Vec<GroupEntry>,
    #[serde(default, rename = "disk_group")]
    pub disk_groups: Vec<DiskGroupEntry>,
    #[serde(default, rename = "disk")]
    pub disks: Vec<DiskEntry>,
    /// Optional `[bench]` section. Reserved for future use.
    #[serde(default, skip_serializing_if = "BenchConfig::is_empty")]
    pub(crate) bench: BenchConfig,
}

/// `[bench]` section. Reserved for future knobs (default reporting
/// dir, max threads, etc.); currently empty.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct BenchConfig {}

impl BenchConfig {
    #[must_use]
    #[allow(clippy::unused_self)]
    pub(crate) fn is_empty(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RackEntry {
    pub id: RackId,
    #[serde(default)]
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeEntry {
    pub id: NodeId,
    pub rack_id: RackId,
    /// Default `127.0.0.1` for local simulated nodes.
    pub host: String,
    /// SSH port. Defaults to 22.
    #[serde(default = "default_ssh_port")]
    pub ssh_port: u16,
    /// SSH user for lifecycle ops. Empty string disables SSH and falls
    /// back to local-fork lifecycle (C3 path) for tests.
    #[serde(default)]
    pub ssh_user: String,
    /// Optional explicit private-key path. `None` falls back to
    /// `~/.ssh/id_ed25519` then `~/.ssh/id_rsa`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssh_key: Option<String>,
    /// Optional password for password auth. Mutually exclusive with
    /// `ssh_key`. Plaintext on disk — operators are expected to rely on
    /// key auth in practice.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssh_password: Option<String>,
}

fn default_ssh_port() -> u16 {
    22
}

impl NodeEntry {
    /// `true` if this node is configured to use SSH for lifecycle ops.
    #[must_use]
    pub fn ssh_enabled(&self) -> bool {
        !self.ssh_user.is_empty()
    }
}

/// Console-side disk-group entry. Mirrors the group-0 `DiskGroupKey`
/// placement (`rack_id`, `node_id`, `disk_group_id`) plus a human-readable
/// name. The console config is the operator's intent; group-0 sysdata is
/// the derived view synced by the console handlers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskGroupEntry {
    pub id: DiskGroupId,
    pub rack_id: RackId,
    pub node_id: NodeId,
    #[serde(default)]
    pub name: String,
}

/// Console-side disk entry. `disk_id` is a UUID hex string (stable across
/// moves). `disk_type` is `"Hdd"` or `"Ssd"`. Capacity / zone / unit sizes
/// are the physical disk's parameters, captured at add time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskEntry {
    pub disk_id: String,
    pub disk_group_id: DiskGroupId,
    pub rack_id: RackId,
    pub node_id: NodeId,
    pub disk_type: String,
    pub capacity_bytes: u64,
    pub zone_size_bytes: u64,
    pub unit_size_bytes: u32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub device_path: String,
}

/// Discriminator for console-deployed server entries. `Kv` is the
/// default for backward compatibility with existing persisted configs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ServiceType {
    #[default]
    Kv,
    Diskdb,
    Chunkdb,
    Diskio,
    /// Standalone crowdb-rpc-fb-server (C++ echo server for RPC bench).
    /// Not a full KV server — no management port, no sysdata. Tracked
    /// in config only for PID/port lifecycle via `cluster destroy`.
    Rpc,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerEntry {
    /// Console-side identifier; must be unique within the file.
    pub id: String,
    /// Service URL. For KV this is the `crowdb-kv-server` management base
    /// URL; for `DiskDB` this is its public crowdb-rpc endpoint.
    pub url: String,
    /// Owning node id; populated for console-deployed instances. `None`
    /// for plain "registered external server" entries from C2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<NodeId>,
    /// crowdb-rpc base URL, e.g. `http://127.0.0.1:10100`. Populated for
    /// console-deployed instances.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rest_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_port: Option<u16>,
    #[serde(default)]
    pub auto_start: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub election_profile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    /// Service type discriminator (R77). Defaults to `Kv` for
    /// backward compatibility with pre-R77 persisted configs.
    #[serde(default, skip_serializing_if = "is_default_service_type")]
    pub service_type: ServiceType,
    /// `--rpc-workers` value passed to the spawned `crowdb-kv-server`.
    /// `None` means the server's default (2) is used. Persisted so
    /// restart reuses the same value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_workers: Option<u32>,
    /// `--no-fsync` flag passed to the spawned `crowdb-kv-server`.
    /// Persisted so restart reuses the same value.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub no_fsync: bool,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_default_service_type(st: &ServiceType) -> bool {
    *st == ServiceType::Kv
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreEntry {
    pub store_id: u64,
    #[serde(default)]
    pub nodes: Vec<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupEntry {
    pub store_id: u64,
    pub group_id: u64,
    #[serde(default)]
    pub replicas: Vec<ReplicaEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaEntry {
    pub replica_id: u64,
    pub node_id: NodeId,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedConsoleConfig {
    #[serde(default, with = "int_key", skip_serializing_if = "BTreeMap::is_empty")]
    rack: BTreeMap<RackId, PersistedRackEntry>,
    #[serde(default, with = "int_key", skip_serializing_if = "BTreeMap::is_empty")]
    node: BTreeMap<NodeId, PersistedNodeEntry>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    crowdb_kv_server: BTreeMap<String, PersistedServerEntry>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    store: BTreeMap<String, PersistedStoreEntry>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    group: BTreeMap<String, PersistedGroupEntry>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    disk_group: BTreeMap<String, PersistedDiskGroupEntry>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    disk: BTreeMap<String, PersistedDiskEntry>,
    #[serde(default, skip_serializing_if = "BenchConfig::is_empty")]
    bench: BenchConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedDiskGroupEntry {
    id: DiskGroupId,
    rack_id: RackId,
    node_id: NodeId,
    #[serde(default)]
    name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedDiskEntry {
    disk_id: String,
    disk_group_id: DiskGroupId,
    rack_id: RackId,
    node_id: NodeId,
    disk_type: String,
    capacity_bytes: u64,
    zone_size_bytes: u64,
    unit_size_bytes: u32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    device_path: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedRackEntry {
    #[serde(default)]
    name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedNodeEntry {
    rack_id: RackId,
    host: String,
    #[serde(default = "default_ssh_port")]
    ssh_port: u16,
    #[serde(default)]
    ssh_user: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ssh_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ssh_password: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedServerEntry {
    node_id: Option<NodeId>,
    url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    rpc_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    rest_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    rpc_port: Option<u16>,
    #[serde(default)]
    auto_start: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    binary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    election_profile: Option<String>,
    #[serde(default, skip_serializing_if = "is_default_service_type")]
    service_type: ServiceType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    rpc_workers: Option<u32>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    no_fsync: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pid: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedStoreEntry {
    store_id: u64,
    #[serde(default)]
    nodes: Vec<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedGroupEntry {
    store_id: u64,
    group_id: u64,
    #[serde(default)]
    replicas: Vec<ReplicaEntry>,
}

impl ServerEntry {
    /// Convenience constructor for a plain registered server (C2 style).
    #[must_use]
    pub fn new(id: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            url: url.into(),
            node_id: None,
            rpc_url: None,
            rest_port: None,
            rpc_port: None,
            auto_start: false,
            binary: None,
            election_profile: None,
            pid: None,
            service_type: ServiceType::Kv,
            rpc_workers: None,
            no_fsync: false,
        }
    }
}

impl ConsoleConfig {
    /// Default config file path.
    ///
    /// Config is persisted to `runtime-data/crowdb-kv.db.toml` in the project root.
    /// This file stores registered crowdb-kv-server instances for the console.
    #[must_use]
    pub(crate) fn default_path() -> Option<PathBuf> {
        TomlFileEngine::default_path()
    }

    /// Load the config from `path`. A missing file yields a default
    /// (empty) config so first-run is friendly.
    ///
    /// # Errors
    /// Returns `Error::Io` for non-`NotFound` filesystem errors and
    /// `Error::Config` for TOML parse failures.
    pub fn load(path: &Path) -> Result<Self> {
        TomlFileEngine::new(path).load()
    }

    /// Save the config atomically (write to a tempfile, then rename).
    ///
    /// # Errors
    /// Filesystem and TOML serialization errors are propagated.
    pub(crate) fn save(&self, path: &Path) -> Result<()> {
        TomlFileEngine::new(path).save(self)
    }

    /// Load configuration using the provided engine.
    ///
    /// # Errors
    /// Returns an error if the engine's load fails.
    pub fn load_with_engine(engine: &dyn ConsoleConfigEngine) -> Result<Self> {
        engine.load()
    }

    /// Save configuration using the provided engine.
    ///
    /// # Errors
    /// Returns an error if the engine's save fails.
    pub fn save_with_engine(&self, engine: &dyn ConsoleConfigEngine) -> Result<()> {
        engine.save(self)
    }

    /// Add a server entry. Rejects duplicate `id` and duplicate `url`.
    ///
    /// # Errors
    /// Returns `Error::Conflict` on duplicate id; `Error::Validation` on
    /// duplicate url.
    pub fn add_server(&mut self, entry: ServerEntry) -> Result<()> {
        if self.servers.iter().any(|s| s.id == entry.id) {
            return Err(Error::Conflict {
                kind: "server".into(),
                id: entry.id,
            });
        }
        if self.servers.iter().any(|s| s.url == entry.url) {
            return Err(Error::Validation {
                field: "url".into(),
                message: format!("url {} already registered", entry.url),
            });
        }
        self.servers.push(entry);
        Ok(())
    }

    /// Remove a server entry by id.
    ///
    /// # Errors
    /// Returns `Error::NotFound` if no entry has that id.
    pub(crate) fn remove_server(&mut self, id: &str) -> Result<ServerEntry> {
        let pos = self
            .servers
            .iter()
            .position(|s| s.id == id)
            .ok_or_else(|| Error::NotFound {
                kind: "server".into(),
                id: id.to_string(),
            })?;
        Ok(self.servers.remove(pos))
    }

    /// All server URLs in registration order.
    #[must_use]
    pub fn server_urls(&self) -> Vec<String> {
        self.servers.iter().map(|s| s.url.clone()).collect()
    }

    pub fn record_store(&mut self, store_id: u64, mut nodes: Vec<NodeId>) {
        nodes.sort_unstable();
        nodes.dedup();
        if let Some(store) = self.stores.iter_mut().find(|s| s.store_id == store_id) {
            store.nodes = nodes;
        } else {
            self.stores.push(StoreEntry { store_id, nodes });
        }
        self.stores.sort_by_key(|s| s.store_id);
    }

    pub fn ensure_store_node(&mut self, store_id: u64, node_id: NodeId) {
        if let Some(store) = self.stores.iter_mut().find(|s| s.store_id == store_id) {
            if !store.nodes.contains(&node_id) {
                store.nodes.push(node_id);
                store.nodes.sort_unstable();
            }
        } else {
            self.stores.push(StoreEntry {
                store_id,
                nodes: vec![node_id],
            });
            self.stores.sort_by_key(|s| s.store_id);
        }
    }

    pub fn remove_store_record(&mut self, store_id: u64) {
        self.stores.retain(|s| s.store_id != store_id);
        self.groups.retain(|g| g.store_id != store_id);
    }

    pub fn record_group(&mut self, store_id: u64, group_id: u64, mut replicas: Vec<ReplicaEntry>) {
        replicas.sort_by_key(|r| r.replica_id);
        if let Some(group) = self
            .groups
            .iter_mut()
            .find(|g| g.store_id == store_id && g.group_id == group_id)
        {
            group.replicas = replicas;
        } else {
            self.groups.push(GroupEntry {
                store_id,
                group_id,
                replicas,
            });
        }
        self.groups.sort_by_key(|g| (g.store_id, g.group_id));
    }

    pub fn remove_group_record(&mut self, store_id: u64, group_id: u64) {
        self.groups
            .retain(|g| !(g.store_id == store_id && g.group_id == group_id));
    }

    pub fn add_group_replica(&mut self, store_id: u64, group_id: u64, replica: ReplicaEntry) {
        if let Some(group) = self
            .groups
            .iter_mut()
            .find(|g| g.store_id == store_id && g.group_id == group_id)
        {
            if let Some(existing) = group
                .replicas
                .iter_mut()
                .find(|r| r.replica_id == replica.replica_id)
            {
                *existing = replica;
            } else {
                group.replicas.push(replica);
                group.replicas.sort_by_key(|r| r.replica_id);
            }
        } else {
            self.groups.push(GroupEntry {
                store_id,
                group_id,
                replicas: vec![replica],
            });
            self.groups.sort_by_key(|g| (g.store_id, g.group_id));
        }
    }

    pub fn remove_group_replica(&mut self, store_id: u64, group_id: u64, replica_id: u64) {
        if let Some(group) = self
            .groups
            .iter_mut()
            .find(|g| g.store_id == store_id && g.group_id == group_id)
        {
            group.replicas.retain(|r| r.replica_id != replica_id);
        }
        self.groups
            .retain(|g| !(g.store_id == store_id && g.group_id == group_id && g.replicas.is_empty()));
    }

    #[must_use]
    pub fn group(&self, store_id: u64, group_id: u64) -> Option<&GroupEntry> {
        self.groups
            .iter()
            .find(|g| g.store_id == store_id && g.group_id == group_id)
    }

    pub fn purge_node_topology(&mut self, node_id: NodeId) {
        for store in &mut self.stores {
            store.nodes.retain(|n| *n != node_id);
        }
        self.stores.retain(|s| !s.nodes.is_empty());
        for group in &mut self.groups {
            group.replicas.retain(|r| r.node_id != node_id);
        }
        self.groups.retain(|g| !g.replicas.is_empty());
    }

    /// Add a rack. Rejects duplicate id.
    ///
    /// # Errors
    /// `Error::Conflict` on duplicate id.
    pub fn add_rack(&mut self, entry: RackEntry) -> Result<()> {
        if self.racks.iter().any(|r| r.id == entry.id) {
            return Err(Error::Conflict {
                kind: "rack".into(),
                id: entry.id.to_string(),
            });
        }
        self.racks.push(entry);
        Ok(())
    }

    /// Remove a rack by id.
    ///
    /// # Errors
    /// `Error::NotFound` if no rack with that id; `Error::Conflict` if any
    /// node still references the rack.
    pub fn remove_rack(&mut self, id: RackId) -> Result<RackEntry> {
        if self.nodes.iter().any(|n| n.rack_id == id) {
            return Err(Error::Conflict {
                kind: "rack".into(),
                id: format!("{id}: rack still referenced by nodes"),
            });
        }
        let pos = self
            .racks
            .iter()
            .position(|r| r.id == id)
            .ok_or_else(|| Error::NotFound {
                kind: "rack".into(),
                id: id.to_string(),
            })?;
        Ok(self.racks.remove(pos))
    }

    /// Add a node. Rejects duplicate id and unknown rack.
    ///
    /// # Errors
    /// `Error::Conflict` on duplicate id; `Error::Validation` on unknown rack.
    pub fn add_node(&mut self, entry: NodeEntry) -> Result<()> {
        if self.nodes.iter().any(|n| n.id == entry.id) {
            return Err(Error::Conflict {
                kind: "node".into(),
                id: entry.id.to_string(),
            });
        }
        if !self.racks.iter().any(|r| r.id == entry.rack_id) {
            return Err(Error::Validation {
                field: "rack_id".into(),
                message: format!("unknown rack {}", entry.rack_id),
            });
        }
        self.nodes.push(entry);
        Ok(())
    }

    /// Remove a node by id.
    ///
    /// # Errors
    /// `Error::NotFound` if no node; `Error::Conflict` if a server is
    /// still deployed to the node.
    pub fn remove_node(&mut self, id: NodeId) -> Result<NodeEntry> {
        if self.servers.iter().any(|s| s.node_id == Some(id)) {
            return Err(Error::Conflict {
                kind: "node".into(),
                id: format!("{id}: node still hosts a deployed server"),
            });
        }
        let pos = self
            .nodes
            .iter()
            .position(|n| n.id == id)
            .ok_or_else(|| Error::NotFound {
                kind: "node".into(),
                id: id.to_string(),
            })?;
        Ok(self.nodes.remove(pos))
    }

    /// Look up a node by id.
    #[must_use]
    pub fn node(&self, id: NodeId) -> Option<&NodeEntry> {
        self.nodes.iter().find(|n| n.id == id)
    }

    /// Look up a server entry by id.
    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn server(&self, id: &str) -> Option<&ServerEntry> {
        self.servers.iter().find(|s| s.id == id)
    }

    /// Look up the server deployed on a given node.
    #[must_use]
    pub fn server_for_node(&self, node_id: NodeId) -> Option<&ServerEntry> {
        self.servers
            .iter()
            .find(|s| s.node_id == Some(node_id) && s.service_type == ServiceType::Kv)
    }

    /// Look up the server deployed on a given node (mutable).
    pub fn server_for_node_mut(&mut self, node_id: NodeId) -> Option<&mut ServerEntry> {
        self.servers
            .iter_mut()
            .find(|s| s.node_id == Some(node_id) && s.service_type == ServiceType::Kv)
    }

    /// Remove the KV server entry deployed on a given node.
    ///
    /// # Errors
    /// `Error::NotFound` if no KV server is deployed on this node.
    pub fn remove_server_for_node(&mut self, node_id: NodeId) -> Result<ServerEntry> {
        let pos = self
            .servers
            .iter()
            .position(|s| s.node_id == Some(node_id) && s.service_type == ServiceType::Kv)
            .ok_or_else(|| Error::NotFound {
                kind: "server".into(),
                id: format!("no server on node {node_id}"),
            })?;
        Ok(self.servers.remove(pos))
    }

    /// Mutable look-up for in-place updates (e.g. `pid` after restart).
    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn server_mut(&mut self, id: &str) -> Option<&mut ServerEntry> {
        self.servers.iter_mut().find(|s| s.id == id)
    }

    // ── disk-group ────────────────────────────────────────────────

    /// Add a disk-group. Rejects duplicate id and unknown node.
    ///
    /// # Errors
    /// `Error::Conflict` on duplicate id; `Error::Validation` on unknown
    /// node.
    pub fn add_disk_group(&mut self, entry: DiskGroupEntry) -> Result<()> {
        if self.disk_groups.iter().any(|dg| dg.id == entry.id) {
            return Err(Error::Conflict {
                kind: "disk_group".into(),
                id: entry.id.to_string(),
            });
        }
        if !self.nodes.iter().any(|n| n.id == entry.node_id) {
            return Err(Error::Validation {
                field: "node_id".into(),
                message: format!("unknown node {}", entry.node_id),
            });
        }
        self.disk_groups.push(entry);
        Ok(())
    }

    /// Remove a disk-group by id.
    ///
    /// # Errors
    /// `Error::NotFound` if no disk-group; `Error::Conflict` if any
    /// disk still references the disk-group.
    pub fn remove_disk_group(&mut self, id: DiskGroupId) -> Result<DiskGroupEntry> {
        if self.disks.iter().any(|d| d.disk_group_id == id) {
            return Err(Error::Conflict {
                kind: "disk_group".into(),
                id: format!("{id}: disk_group still has disks"),
            });
        }
        let pos = self
            .disk_groups
            .iter()
            .position(|dg| dg.id == id)
            .ok_or_else(|| Error::NotFound {
                kind: "disk_group".into(),
                id: id.to_string(),
            })?;
        Ok(self.disk_groups.remove(pos))
    }

    /// Look up a disk-group by id.
    #[must_use]
    pub fn disk_group(&self, id: DiskGroupId) -> Option<&DiskGroupEntry> {
        self.disk_groups.iter().find(|dg| dg.id == id)
    }

    /// List disk-groups on a node.
    #[must_use]
    pub fn disk_groups_on_node(&self, node_id: NodeId) -> Vec<&DiskGroupEntry> {
        self.disk_groups
            .iter()
            .filter(|dg| dg.node_id == node_id)
            .collect()
    }

    // ── disk ──────────────────────────────────────────────────────

    /// Add a disk. Rejects duplicate `disk_id` and unknown `disk_group`.
    ///
    /// # Errors
    /// `Error::Conflict` on duplicate `disk_id`; `Error::Validation` on
    /// unknown `disk_group`.
    pub fn add_disk(&mut self, entry: DiskEntry) -> Result<()> {
        if self.disks.iter().any(|d| d.disk_id == entry.disk_id) {
            return Err(Error::Conflict {
                kind: "disk".into(),
                id: entry.disk_id.clone(),
            });
        }
        if !self.disk_groups.iter().any(|dg| dg.id == entry.disk_group_id) {
            return Err(Error::Validation {
                field: "disk_group_id".into(),
                message: format!("unknown disk_group {}", entry.disk_group_id),
            });
        }
        self.disks.push(entry);
        Ok(())
    }

    /// Remove a disk by `disk_id`.
    ///
    /// # Errors
    /// `Error::NotFound` if no disk with that `disk_id`.
    pub fn remove_disk(&mut self, disk_id: &str) -> Result<DiskEntry> {
        let pos = self
            .disks
            .iter()
            .position(|d| d.disk_id == disk_id)
            .ok_or_else(|| Error::NotFound {
                kind: "disk".into(),
                id: disk_id.to_string(),
            })?;
        Ok(self.disks.remove(pos))
    }

    /// Look up a disk by `disk_id`.
    #[must_use]
    pub fn disk(&self, disk_id: &str) -> Option<&DiskEntry> {
        self.disks.iter().find(|d| d.disk_id == disk_id)
    }

    /// List disks in a disk-group.
    #[must_use]
    pub fn disks_in_group(&self, dg_id: DiskGroupId) -> Vec<&DiskEntry> {
        self.disks.iter().filter(|d| d.disk_group_id == dg_id).collect()
    }

    #[allow(clippy::too_many_lines)]
    fn to_persisted(&self) -> PersistedConsoleConfig {
        let rack = self
            .racks
            .iter()
            .map(|entry| {
                (
                    entry.id,
                    PersistedRackEntry {
                        name: entry.name.clone(),
                    },
                )
            })
            .collect();
        let node = self
            .nodes
            .iter()
            .map(|entry| {
                (
                    entry.id,
                    PersistedNodeEntry {
                        rack_id: entry.rack_id,
                        host: entry.host.clone(),
                        ssh_port: entry.ssh_port,
                        ssh_user: entry.ssh_user.clone(),
                        ssh_key: entry.ssh_key.clone(),
                        ssh_password: entry.ssh_password.clone(),
                    },
                )
            })
            .collect();
        let crowdb_kv_server = self
            .servers
            .iter()
            .map(|entry| {
                (
                    entry.id.clone(),
                    PersistedServerEntry {
                        node_id: entry.node_id,
                        url: entry.url.clone(),
                        rpc_url: entry.rpc_url.clone(),
                        rest_port: entry.rest_port,
                        rpc_port: entry.rpc_port,
                        auto_start: entry.auto_start,
                        binary: entry.binary.clone(),
                        election_profile: entry.election_profile.clone(),
                        service_type: entry.service_type,
                        rpc_workers: entry.rpc_workers,
                        no_fsync: entry.no_fsync,
                        pid: entry.pid,
                    },
                )
            })
            .collect();
        let store = self
            .stores
            .iter()
            .map(|entry| {
                (
                    entry.store_id.to_string(),
                    PersistedStoreEntry {
                        store_id: entry.store_id,
                        nodes: entry.nodes.clone(),
                    },
                )
            })
            .collect();
        let group = self
            .groups
            .iter()
            .map(|entry| {
                (
                    format!("{}:{}", entry.store_id, entry.group_id),
                    PersistedGroupEntry {
                        store_id: entry.store_id,
                        group_id: entry.group_id,
                        replicas: entry.replicas.clone(),
                    },
                )
            })
            .collect();
        let disk_group = self
            .disk_groups
            .iter()
            .map(|entry| {
                (
                    entry.id.to_string(),
                    PersistedDiskGroupEntry {
                        id: entry.id,
                        rack_id: entry.rack_id,
                        node_id: entry.node_id,
                        name: entry.name.clone(),
                    },
                )
            })
            .collect();
        let disk = self
            .disks
            .iter()
            .map(|entry| {
                (
                    entry.disk_id.clone(),
                    PersistedDiskEntry {
                        disk_id: entry.disk_id.clone(),
                        disk_group_id: entry.disk_group_id,
                        rack_id: entry.rack_id,
                        node_id: entry.node_id,
                        disk_type: entry.disk_type.clone(),
                        capacity_bytes: entry.capacity_bytes,
                        zone_size_bytes: entry.zone_size_bytes,
                        unit_size_bytes: entry.unit_size_bytes,
                        device_path: entry.device_path.clone(),
                    },
                )
            })
            .collect();
        PersistedConsoleConfig {
            rack,
            node,
            crowdb_kv_server,
            store,
            group,
            disk_group,
            disk,
            bench: self.bench.clone(),
        }
    }

    fn from_persisted(persisted: PersistedConsoleConfig) -> Self {
        let mut racks: Vec<RackEntry> = persisted
            .rack
            .into_iter()
            .map(|(id, entry)| RackEntry { id, name: entry.name })
            .collect();
        racks.sort_by_key(|r| r.id);
        let mut nodes: Vec<NodeEntry> = persisted
            .node
            .into_iter()
            .map(|(id, entry)| NodeEntry {
                id,
                rack_id: entry.rack_id,
                host: entry.host,
                ssh_port: entry.ssh_port,
                ssh_user: entry.ssh_user,
                ssh_key: entry.ssh_key,
                ssh_password: entry.ssh_password,
            })
            .collect();
        nodes.sort_by_key(|n| n.id);
        let mut servers: Vec<ServerEntry> = persisted
            .crowdb_kv_server
            .into_iter()
            .map(|(id, entry)| ServerEntry {
                id,
                url: entry.url,
                node_id: entry.node_id,
                rpc_url: entry.rpc_url,
                rest_port: entry.rest_port,
                rpc_port: entry.rpc_port,
                auto_start: entry.auto_start,
                binary: entry.binary,
                election_profile: entry.election_profile,
                pid: entry.pid,
                service_type: entry.service_type,
                rpc_workers: entry.rpc_workers,
                no_fsync: entry.no_fsync,
            })
            .collect();
        servers.sort_by(|a, b| a.id.cmp(&b.id));
        let mut stores: Vec<StoreEntry> = persisted
            .store
            .into_values()
            .map(|entry| StoreEntry {
                store_id: entry.store_id,
                nodes: entry.nodes,
            })
            .collect();
        stores.sort_by_key(|s| s.store_id);
        let mut groups: Vec<GroupEntry> = persisted
            .group
            .into_values()
            .map(|entry| GroupEntry {
                store_id: entry.store_id,
                group_id: entry.group_id,
                replicas: entry.replicas,
            })
            .collect();
        groups.sort_by_key(|g| (g.store_id, g.group_id));
        let mut disk_groups: Vec<DiskGroupEntry> = persisted
            .disk_group
            .into_values()
            .map(|entry| DiskGroupEntry {
                id: entry.id,
                rack_id: entry.rack_id,
                node_id: entry.node_id,
                name: entry.name,
            })
            .collect();
        disk_groups.sort_by_key(|dg| dg.id);
        let mut disks: Vec<DiskEntry> = persisted
            .disk
            .into_values()
            .map(|entry| DiskEntry {
                disk_id: entry.disk_id,
                disk_group_id: entry.disk_group_id,
                rack_id: entry.rack_id,
                node_id: entry.node_id,
                disk_type: entry.disk_type,
                capacity_bytes: entry.capacity_bytes,
                zone_size_bytes: entry.zone_size_bytes,
                unit_size_bytes: entry.unit_size_bytes,
                device_path: entry.device_path,
            })
            .collect();
        disks.sort_by(|a, b| a.disk_id.cmp(&b.disk_id));
        Self {
            racks,
            nodes,
            servers,
            stores,
            groups,
            disk_groups,
            disks,
            bench: persisted.bench,
        }
    }

    fn from_toml_str(body: &str, path: &Path) -> Result<Self> {
        let persisted: PersistedConsoleConfig =
            toml::from_str(body).map_err(|e| Error::Config(format!("{}: {e}", path.display())))?;
        Ok(Self::from_persisted(persisted))
    }

    fn to_toml_string(&self) -> Result<String> {
        toml::to_string_pretty(&self.to_persisted()).map_err(|e| Error::Config(format!("serialize: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::{ConsoleConfig, GroupEntry, ReplicaEntry, ServerEntry, StoreEntry, TomlFileEngine};
    use crowdb_test_harness::test_dirs;

    #[test]
    fn round_trip_load_save() {
        let dir = tempdir();
        let path = dir.join("console.toml");

        let mut cfg = ConsoleConfig::default();
        let mut a = ServerEntry::new("a", "http://127.0.0.1:10000");
        a.node_id = Some(1);
        a.rpc_url = Some("http://127.0.0.1:9921".into());
        a.rest_port = Some(10000);
        a.rpc_port = Some(9921);
        a.auto_start = true;
        a.election_profile = Some("test".into());
        a.pid = Some(12345);
        cfg.add_server(a).unwrap();
        cfg.add_server(ServerEntry::new("b", "http://127.0.0.1:10001"))
            .unwrap();
        cfg.stores.push(StoreEntry {
            store_id: 7,
            nodes: vec![1, 2],
        });
        cfg.groups.push(GroupEntry {
            store_id: 7,
            group_id: 70,
            replicas: vec![
                ReplicaEntry {
                    replica_id: 700,
                    node_id: 1,
                },
                ReplicaEntry {
                    replica_id: 701,
                    node_id: 2,
                },
            ],
        });

        cfg.save(&path).unwrap();
        let loaded = ConsoleConfig::load(&path).unwrap();
        let expected = cfg.clone();
        assert_eq!(expected, loaded);
    }

    #[test]
    fn pid_is_persisted_to_disk() {
        let dir = tempdir();
        let path = dir.join("console.toml");

        let mut cfg = ConsoleConfig::default();
        let mut entry = ServerEntry::new("a", "http://127.0.0.1:10000");
        entry.pid = Some(4242);
        cfg.add_server(entry).unwrap();

        cfg.save(&path).unwrap();
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(raw.contains("pid = 4242"), "runtime pid must be persisted: {raw}");
    }

    #[test]
    fn missing_file_yields_default() {
        let dir = tempdir();
        let path = dir.join("nope.toml");
        let cfg = ConsoleConfig::load(&path).unwrap();
        assert!(cfg.servers.is_empty());
    }

    #[test]
    fn toml_engine_round_trip() {
        let dir = tempdir();
        let path = dir.join("engine.toml");
        let engine = TomlFileEngine::new(path.clone());

        let mut cfg = ConsoleConfig::default();
        cfg.add_server(ServerEntry::new("a", "http://127.0.0.1:10000"))
            .unwrap();

        cfg.save_with_engine(&engine).unwrap();
        let loaded = ConsoleConfig::load_with_engine(&engine).unwrap();

        assert_eq!(cfg, loaded);
    }

    #[test]
    fn default_path_points_to_runtime_data() {
        assert_eq!(
            TomlFileEngine::default_path().unwrap(),
            std::path::PathBuf::from("runtime-data/crowdb-kv.db.toml")
        );
        assert_eq!(
            ConsoleConfig::default_path().unwrap(),
            std::path::PathBuf::from("runtime-data/crowdb-kv.db.toml")
        );
    }

    #[test]
    fn duplicate_id_rejected() {
        let mut cfg = ConsoleConfig::default();
        cfg.add_server(ServerEntry::new("a", "http://1")).unwrap();
        let err = cfg.add_server(ServerEntry::new("a", "http://2")).unwrap_err();
        assert!(matches!(err, crate::error::Error::Conflict { .. }));
    }

    #[test]
    fn duplicate_url_rejected() {
        let mut cfg = ConsoleConfig::default();
        cfg.add_server(ServerEntry::new("a", "http://1")).unwrap();
        let err = cfg.add_server(ServerEntry::new("b", "http://1")).unwrap_err();
        assert!(matches!(err, crate::error::Error::Validation { .. }));
    }

    #[test]
    fn remove_missing_is_not_found() {
        let mut cfg = ConsoleConfig::default();
        let err = cfg.remove_server("ghost").unwrap_err();
        assert!(matches!(err, crate::error::Error::NotFound { .. }));
    }

    fn tempdir() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let base = test_dirs::test_data_dir();
        let unique = format!(
            "crowdb-console-cfg-{}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let dir = base.join(unique);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
}
