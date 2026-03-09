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
}
