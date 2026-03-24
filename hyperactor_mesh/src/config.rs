/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Configuration for Hyperactor Mesh.
//!
//! This module provides hyperactor_mesh-specific configuration attributes that extend
//! the base hyperactor configuration system.

use std::net::SocketAddr;
use std::time::Duration;

use hyperactor_config::AttrValue;
use hyperactor_config::CONFIG;
use hyperactor_config::ConfigAttr;
use hyperactor_config::attrs::declare_attrs;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

/// A socket address string usable as a `declare_attrs!` default.
///
/// Follows the [`hyperactor::config::Pem`] pattern: the `Static`
/// variant holds a `&'static str` so it can appear in a `static`
/// item, while `Value` holds a runtime `String` from environment
/// variables or Python `configure()`.
#[derive(Clone, Debug, Serialize, Named)]
#[named("hyperactor_mesh::config::SocketAddrStr")]
pub enum SocketAddrStr {
    /// Compile-time default (const-constructible).
    Static(&'static str),
    /// Runtime value from env / config.
    Value(String),
}

impl<'de> Deserialize<'de> for SocketAddrStr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        enum Helper {
            Static(String),
            Value(String),
        }
        match Helper::deserialize(deserializer)? {
            Helper::Static(s) | Helper::Value(s) => Ok(SocketAddrStr::Value(s)),
        }
    }
}

impl From<String> for SocketAddrStr {
    fn from(s: String) -> Self {
        SocketAddrStr::Value(s)
    }
}

impl From<SocketAddrStr> for String {
    fn from(s: SocketAddrStr) -> Self {
        s.as_ref().to_owned()
    }
}

impl AsRef<str> for SocketAddrStr {
    fn as_ref(&self) -> &str {
        match self {
            SocketAddrStr::Static(s) => s,
            SocketAddrStr::Value(s) => s,
        }
    }
}

impl std::fmt::Display for SocketAddrStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_ref())
    }
}

impl AttrValue for SocketAddrStr {
    fn display(&self) -> String {
        self.as_ref().to_owned()
    }

    fn parse(value: &str) -> Result<Self, anyhow::Error> {
        value.parse::<SocketAddr>()?;
        Ok(SocketAddrStr::Value(value.to_string()))
    }
}

impl SocketAddrStr {
    /// Parse the contained string as a `SocketAddr`.
    pub fn parse_socket_addr(&self) -> Result<SocketAddr, std::net::AddrParseError> {
        self.as_ref().parse()
    }
}

// Declare hyperactor_mesh-specific configuration keys
declare_attrs! {
    /// The maximium for a dimension size allowed for a folded shape
    /// when reshaping during casting to limit fanout.
    /// usize::MAX means no reshaping as any shape will always be below
    /// the limit so no dimension needs to be folded.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_MAX_CAST_DIMENSION_SIZE".to_string()),
        Some("max_cast_dimension_size".to_string()),
    ))
    pub attr MAX_CAST_DIMENSION_SIZE: usize = usize::MAX;

    /// Which builtin process launcher backend to use.
    /// Accepted values: "native" (default), "systemd".
    /// Trimmed and lowercased before matching.
    ///
    /// **Precedence:** Python spawner (via SetProcSpawner) overrides this.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_PROC_LAUNCHER_KIND".to_string()),
        Some("proc_launcher_kind".to_string()),
    ))
    pub attr MESH_PROC_LAUNCHER_KIND: String = String::new();

    /// Default socket address for the mesh admin HTTP server.
    ///
    /// Parsed as a `SocketAddr` (e.g. `[::]:1729`, `0.0.0.0:8080`).
    /// Used as the bind address when no explicit address is provided
    /// to `MeshAdminAgent`, and as the default address assumed by
    /// admin clients connecting via `mast_conda:///`.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_ADMIN_ADDR".to_string()),
        Some("mesh_admin_addr".to_string()),
    ))
    pub attr MESH_ADMIN_ADDR: SocketAddrStr = SocketAddrStr::Static("[::]:1729");

    /// Timeout for fallback queries to actors/procs that may have been
    /// recently destroyed. The second-chance paths in `resolve_proc_node`
    /// and `resolve_actor_node` fire after the fast QueryChild lookup
    /// fails. A short budget here prevents dead actors from blocking the
    /// single-threaded MeshAdminAgent message loop.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_ADMIN_RESOLVE_ACTOR_TIMEOUT".to_string()),
        Some("mesh_admin_resolve_actor_timeout".to_string()),
    ))
    pub attr MESH_ADMIN_RESOLVE_ACTOR_TIMEOUT: Duration = Duration::from_millis(200);

    /// Maximum number of concurrent resolve requests the HTTP bridge
    /// forwards to the MeshAdminAgent. Excess requests receive 503
    /// immediately. Protects the shared tokio runtime from query floods
    /// (e.g. multiple TUI clients, rapid polling). Increase if the admin
    /// server serves many concurrent clients that need low-latency
    /// responses; decrease if introspection queries interfere with the
    /// actor workload under churn.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_ADMIN_MAX_CONCURRENT_RESOLVES".to_string()),
        Some("mesh_admin_max_concurrent_resolves".to_string()),
    ))
    pub attr MESH_ADMIN_MAX_CONCURRENT_RESOLVES: usize = 2;

    /// Timeout for the config-push barrier during `HostMesh::attach()`.
    ///
    /// When attaching to pre-existing workers (simple bootstrap), the
    /// client pushes its propagatable config to each host agent and
    /// waits for confirmation. If the barrier does not complete within
    /// this duration, a warning is logged and attach continues without
    /// blocking — config push is best-effort.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_ATTACH_CONFIG_TIMEOUT".to_string()),
        Some("mesh_attach_config_timeout".to_string()),
    ))
    pub attr MESH_ATTACH_CONFIG_TIMEOUT: Duration = Duration::from_secs(10);

    /// Timeout for targeted introspection queries that hit a single,
    /// specific host. Kept short so a slow or dying actor cannot block
    /// the single-threaded MeshAdminAgent message loop.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_ADMIN_SINGLE_HOST_TIMEOUT".to_string()),
        Some("mesh_admin_single_host_timeout".to_string()),
    ))
    pub attr MESH_ADMIN_SINGLE_HOST_TIMEOUT: Duration = Duration::from_secs(3);

    /// Timeout for QueryChild snapshot lookups in resolve_actor_node.
    /// QueryChild is handled by a synchronous callback — it either
    /// returns immediately or returns Error. A short budget ensures
    /// the total time for resolve_actor_node stays well under
    /// `MESH_ADMIN_SINGLE_HOST_TIMEOUT`.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_ADMIN_QUERY_CHILD_TIMEOUT".to_string()),
        Some("mesh_admin_query_child_timeout".to_string()),
    ))
    pub attr MESH_ADMIN_QUERY_CHILD_TIMEOUT: Duration = Duration::from_millis(100);

    /// Timeout for the end-to-end `/v1/config/{proc}` bridge reply.
    /// The config-dump path forwards a `ConfigDump` message through
    /// the HostAgent bridge and waits for `ConfigDumpResult`. This is
    /// inter-process actor messaging — fundamentally slower than local
    /// `QueryChild` snapshot lookups (which use
    /// `MESH_ADMIN_QUERY_CHILD_TIMEOUT`). During startup, the
    /// HostAgent message loop may be busy processing actor
    /// registrations, so bridge latency can exceed several seconds.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_ADMIN_CONFIG_DUMP_BRIDGE_TIMEOUT".to_string()),
        Some("mesh_admin_config_dump_bridge_timeout".to_string()),
    ))
    pub attr MESH_ADMIN_CONFIG_DUMP_BRIDGE_TIMEOUT: Duration = Duration::from_secs(5);

    /// Timeout for py-spy dump requests. See PS-5 in `introspect`
    /// module doc. With `--native --native-all`, py-spy unwinds native
    /// stacks via libunwind which is significantly slower than
    /// Python-only capture (~100ms). 10s accommodates native unwinding
    /// on heavily loaded hosts. Independent of
    /// `MESH_ADMIN_SINGLE_HOST_TIMEOUT` because py-spy does real I/O
    /// (subprocess + ptrace) rather than actor messaging.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_ADMIN_PYSPY_TIMEOUT".to_string()),
        Some("mesh_admin_pyspy_timeout".to_string()),
    ))
    pub attr MESH_ADMIN_PYSPY_TIMEOUT: Duration = Duration::from_secs(10);

    /// Timeout for the `/v1/tree` fan-out. Kept generous because the
    /// tree dump walks every host and proc in the mesh.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_ADMIN_TREE_TIMEOUT".to_string()),
        Some("mesh_admin_tree_timeout".to_string()),
    ))
    pub attr MESH_ADMIN_TREE_TIMEOUT: Duration = Duration::from_secs(10);

    /// Bridge-side timeout for py-spy dump requests. Must exceed
    /// `MESH_ADMIN_PYSPY_TIMEOUT` to allow the subprocess kill/reap
    /// and reply delivery to arrive before declaring `gateway_timeout`.
    /// See PS-6 in `introspect` module doc.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_ADMIN_PYSPY_BRIDGE_TIMEOUT".to_string()),
        Some("mesh_admin_pyspy_bridge_timeout".to_string()),
    ))
    pub attr MESH_ADMIN_PYSPY_BRIDGE_TIMEOUT: Duration = Duration::from_secs(13);

    /// Client-side timeout for py-spy requests. Must exceed
    /// `MESH_ADMIN_PYSPY_BRIDGE_TIMEOUT` so the server can return a
    /// structured `PySpyResult` even when the subprocess uses the
    /// full budget. See PS-6 in `introspect` module doc.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESH_ADMIN_PYSPY_CLIENT_TIMEOUT".to_string()),
        Some("mesh_admin_pyspy_client_timeout".to_string()),
    ))
    pub attr MESH_ADMIN_PYSPY_CLIENT_TIMEOUT: Duration = Duration::from_secs(20);

    /// Path to the py-spy binary. When non-empty, tried before
    /// the fallback `"py-spy"` PATH lookup. See PS-3 in
    /// `introspect` module doc.
    ///
    /// Note: env var is `PYSPY_BIN` (not `HYPERACTOR_MESH_PYSPY_BIN`)
    /// to preserve backward compatibility with existing deployments
    /// that already set `PYSPY_BIN`.
    @meta(CONFIG = ConfigAttr::new(
        Some("PYSPY_BIN".to_string()),
        Some("pyspy_bin".to_string()),
    ))
    pub attr PYSPY_BIN: String = String::new();
}
