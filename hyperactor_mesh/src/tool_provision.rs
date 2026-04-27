/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Per-host diagnostic tool provisioning actor.
//!
//! `ToolProvisionActor` is the mesh-facing owner for host-local
//! diagnostic tool state. It wraps the standalone `tool_fetch` crate in
//! actor messages so mesh admin routes and future workflows can
//! provision, resolve, and inspect tools without knowing cache layout.
//!
//! # Invariants
//!
//! - **TP-1 (host-local-state):** One actor owns tool state for one
//!   host/service proc.
//! - **TP-2 (desired-gates-resolve):** `ResolveTool` returns only the
//!   currently desired registered version of a tool. Cache entries
//!   discovered by scan but not registered are inventory only.
//! - **TP-3 (provision-registers-desired):** Successful or failed
//!   provision attempts record desired state for the requested tool.
//! - **TP-4 (cache-scan-inventory):** Actor init scans the persistent
//!   cache and exposes discovered artifacts as
//!   `CachedButNotRegistered`.
//! - **TP-5 (reply-best-effort):** Handlers send typed replies via
//!   caller-provided once ports. If the caller timed out, the actor logs
//!   and continues rather than failing.
//! - **TP-6 (tool-fetch-boundary):** Fetch, verification, extraction,
//!   and executable resolution are delegated to `tool_fetch`.
//! - **TP-7 (flight-recorder-observability):** Every operator-visible
//!   tool state transition emits a structured tracing event from inside
//!   the actor handler, so the actor-local flight recorder explains
//!   provisioning and resolution history in mesh-admin.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::SystemTime;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::RefClient;
use hyperactor::context::Actor as ActorContext;
use hyperactor_config::INTROSPECT;
use hyperactor_config::IntrospectAttr;
use hyperactor_config::declare_attrs;
use serde::Deserialize;
use serde::Serialize;
use tool_fetch::ProvisionError;
use tool_fetch::ToolCache;
use tool_fetch::ToolSpec;
use typeuri::Named;

/// Actor name used when spawning the per-host tool provisioner.
pub const TOOL_PROVISION_ACTOR_NAME: &str = "tool_provision";

declare_attrs! {
    /// Number of known tool entries in this host's inventory.
    @meta(INTROSPECT = IntrospectAttr {
        name: "tool_count".into(),
        desc: "Number of known tool entries in host-local inventory".into(),
    })
    pub attr TOOL_COUNT: usize = 0;

    /// Registered tools whose executable is available.
    @meta(INTROSPECT = IntrospectAttr {
        name: "tools_available".into(),
        desc: "Registered tool versions available on this host".into(),
    })
    pub attr TOOLS_AVAILABLE: Vec<String>;

    /// Registered tools whose last provision attempt failed.
    @meta(INTROSPECT = IntrospectAttr {
        name: "tools_failed".into(),
        desc: "Registered tool versions with failed provisioning on this host".into(),
    })
    pub attr TOOLS_FAILED: Vec<String>;
}

/// Request to provision a tool on the host.
#[derive(Debug, Serialize, Deserialize, Named, Handler, HandleClient, RefClient)]
pub struct ProvisionTool {
    /// Declarative tool spec to provision.
    pub spec: ToolSpec,
    /// Reply port receiving the provisioning result.
    #[reply]
    pub reply: hyperactor::reference::OncePortRef<ProvisionResult>,
}
wirevalue::register_type!(ProvisionTool);

/// Request to resolve a tool name/version to an executable path.
#[derive(Debug, Serialize, Deserialize, Named, Handler, HandleClient, RefClient)]
pub struct ResolveTool {
    /// Tool name, e.g. `py-spy`.
    pub tool: String,
    /// Optional version. `None` means the actor's currently desired
    /// registered version.
    pub version: Option<String>,
    /// Reply port receiving the resolve result.
    #[reply]
    pub reply: hyperactor::reference::OncePortRef<ResolveResult>,
}
wirevalue::register_type!(ResolveTool);

/// Request the full per-host tool inventory.
#[derive(Debug, Serialize, Deserialize, Named, Handler, HandleClient, RefClient)]
pub struct QueryToolInventory {
    /// Reply port receiving the inventory snapshot.
    #[reply]
    pub reply: hyperactor::reference::OncePortRef<ToolInventory>,
}
wirevalue::register_type!(QueryToolInventory);

/// Result of a `ProvisionTool` request.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    Named,
    schemars::JsonSchema
)]
pub enum ProvisionResult {
    /// The tool is available at the returned executable path.
    Available {
        /// Tool name.
        name: String,
        /// Tool version.
        version: String,
        /// Resolved executable path.
        executable: PathBuf,
        /// Downloaded artifact digest.
        artifact_digest: String,
    },
    /// Provisioning failed.
    Failed {
        /// Tool name.
        name: String,
        /// Tool version.
        version: String,
        /// Structured error string.
        error: String,
    },
}
wirevalue::register_type!(ProvisionResult);

/// Result of a `ResolveTool` request.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    Named,
    schemars::JsonSchema
)]
pub enum ResolveResult {
    /// Tool resolved successfully.
    Available {
        /// Tool name.
        name: String,
        /// Tool version.
        version: String,
        /// Resolved executable path.
        executable: PathBuf,
    },
    /// Tool exists in cache but is not registered as desired.
    NotProvisioned {
        /// Tool name requested by the caller.
        tool: String,
        /// Optional version requested by the caller.
        version: Option<String>,
    },
    /// Tool provisioning failed for the desired version.
    Failed {
        /// Tool name.
        name: String,
        /// Tool version.
        version: String,
        /// Last observed error.
        error: String,
    },
}
wirevalue::register_type!(ResolveResult);

/// Inventory snapshot for a host.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    Named,
    schemars::JsonSchema
)]
pub struct ToolInventory {
    /// Tool states known to this host.
    pub tools: Vec<ToolInventoryEntry>,
}
wirevalue::register_type!(ToolInventory);

/// One tool entry in host inventory.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ToolInventoryEntry {
    /// Tool name.
    pub name: String,
    /// Tool version.
    pub version: String,
    /// Human-readable state.
    pub state: ToolInventoryState,
}

/// Serializable inventory state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub enum ToolInventoryState {
    /// Tool is registered and available.
    Available {
        /// Executable path.
        executable: PathBuf,
        /// Downloaded artifact digest.
        artifact_digest: String,
    },
    /// Provisioning is in progress.
    Fetching,
    /// Last provision attempt failed.
    Failed {
        /// Error string.
        error: String,
    },
    /// Artifact exists in cache but no desired spec registered it.
    CachedButNotRegistered {
        /// Executable path.
        executable: PathBuf,
        /// Downloaded artifact digest.
        artifact_digest: String,
    },
}

#[derive(Debug, Clone)]
struct DesiredTool {
    version: String,
    _spec: ToolSpec,
}

#[derive(Debug, Clone)]
enum ObservedToolState {
    Fetching {
        _started_at: SystemTime,
    },
    Available {
        executable: PathBuf,
        artifact_digest: String,
        _provisioned_at: String,
    },
    Failed {
        error: String,
        _last_attempt: SystemTime,
    },
    CachedButNotRegistered {
        executable: PathBuf,
        artifact_digest: String,
        _provisioned_at: String,
    },
}

/// Per-host actor that provisions and resolves diagnostic tools.
#[hyperactor::export(handlers = [ProvisionTool, ResolveTool, QueryToolInventory])]
pub struct ToolProvisionActor {
    cache: ToolCache,
    desired: HashMap<String, DesiredTool>,
    observed: HashMap<String, ObservedToolState>,
}

impl ToolProvisionActor {
    /// Construct an actor using the default cache directory.
    pub fn new() -> Self {
        Self::with_cache(ToolCache::default())
    }

    /// Construct an actor with an explicit cache.
    pub fn with_cache(cache: ToolCache) -> Self {
        Self {
            cache,
            desired: HashMap::new(),
            observed: HashMap::new(),
        }
    }

    fn inventory(&self) -> ToolInventory {
        let mut tools: Vec<_> = self
            .observed
            .iter()
            .map(|(key, state)| {
                let (name, version) = split_key(key);
                ToolInventoryEntry {
                    name,
                    version,
                    state: state.to_inventory_state(),
                }
            })
            .collect();
        tools.sort_by(|a, b| a.name.cmp(&b.name).then(a.version.cmp(&b.version)));
        ToolInventory { tools }
    }

    fn resolve(&self, tool: &str, requested_version: Option<&str>) -> ResolveResult {
        let version = match requested_version {
            Some(version) => Some(version.to_string()),
            None => self
                .desired
                .get(tool)
                .map(|desired| desired.version.clone()),
        };

        if let Some(version) = version {
            let key = tool_key(tool, &version);
            match self.observed.get(&key) {
                Some(ObservedToolState::Available { executable, .. }) => ResolveResult::Available {
                    name: tool.to_string(),
                    version,
                    executable: executable.clone(),
                },
                Some(ObservedToolState::Failed { error, .. }) => ResolveResult::Failed {
                    name: tool.to_string(),
                    version,
                    error: error.clone(),
                },
                _ => ResolveResult::NotProvisioned {
                    tool: tool.to_string(),
                    version: Some(version),
                },
            }
        } else {
            ResolveResult::NotProvisioned {
                tool: tool.to_string(),
                version: requested_version.map(ToString::to_string),
            }
        }
    }

    fn publish_attrs(&self, cx: &Instance<Self>) {
        let inventory = self.inventory();
        let mut attrs = hyperactor_config::Attrs::new();
        attrs.set(crate::introspect::NODE_TYPE, "tool_provision".to_string());
        attrs.set(TOOL_COUNT, inventory.tools.len());
        attrs.set(
            TOOLS_AVAILABLE,
            inventory
                .tools
                .iter()
                .filter_map(|tool| match tool.state {
                    ToolInventoryState::Available { .. } => {
                        Some(format!("{}@{}", tool.name, tool.version))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>(),
        );
        attrs.set(
            TOOLS_FAILED,
            inventory
                .tools
                .iter()
                .filter_map(|tool| match tool.state {
                    ToolInventoryState::Failed { .. } => {
                        Some(format!("{}@{}", tool.name, tool.version))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>(),
        );
        cx.publish_attrs(attrs);
    }
}

impl Default for ToolProvisionActor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Actor for ToolProvisionActor {
    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        this.bind::<Self>();
        this.set_system();

        let mut scanned = 0usize;
        for artifact in self.cache.scan() {
            scanned += 1;
            let key = tool_key(&artifact.name, &artifact.version);
            tracing::info!(
                name = "ToolProvisionStatus",
                status = "CacheScan::Discovered",
                tool = %artifact.name,
                version = %artifact.version,
                executable = %artifact.executable.display(),
                artifact_digest = %artifact.digest,
                provisioned_at = %artifact.provisioned_at,
            );
            self.observed.insert(
                key,
                ObservedToolState::CachedButNotRegistered {
                    executable: artifact.executable,
                    artifact_digest: artifact.digest,
                    _provisioned_at: artifact.provisioned_at,
                },
            );
        }
        tracing::info!(
            name = "ToolProvisionStatus",
            status = "CacheScan::Complete",
            scanned,
        );
        self.publish_attrs(this);
        Ok(())
    }
}

#[async_trait]
impl Handler<ProvisionTool> for ToolProvisionActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: ProvisionTool,
    ) -> Result<(), anyhow::Error> {
        let spec = message.spec;
        let key = tool_key(&spec.name, &spec.version);
        tracing::info!(
            name = "ToolProvisionStatus",
            status = "Provision::Requested",
            tool = %spec.name,
            version = %spec.version,
        );
        self.desired.insert(
            spec.name.clone(),
            DesiredTool {
                version: spec.version.clone(),
                _spec: spec.clone(),
            },
        );
        self.observed.insert(
            key.clone(),
            ObservedToolState::Fetching {
                _started_at: SystemTime::now(),
            },
        );
        self.publish_attrs(cx.instance());

        let platform = tool_fetch::current_platform();
        let result = match platform {
            Ok(platform) => {
                let digest = spec
                    .platforms
                    .get(&platform)
                    .map(|entry| entry.digest.clone())
                    .unwrap_or_default();
                tracing::info!(
                    name = "ToolProvisionStatus",
                    status = "Provision::FetchStart",
                    tool = %spec.name,
                    version = %spec.version,
                    platform = ?platform,
                    artifact_digest = %digest,
                );
                (digest, self.cache.provision(&spec, platform).await)
            }
            Err(err) => {
                tracing::warn!(
                    name = "ToolProvisionStatus",
                    status = "Provision::PlatformUnsupported",
                    tool = %spec.name,
                    version = %spec.version,
                    error = %err,
                );
                (String::new(), Err(err))
            }
        };

        let reply = match result.1 {
            Ok(executable) => {
                let digest = result.0;
                tracing::info!(
                    name = "ToolProvisionStatus",
                    status = "Provision::Available",
                    tool = %spec.name,
                    version = %spec.version,
                    executable = %executable.display(),
                    artifact_digest = %digest,
                );
                self.observed.insert(
                    key,
                    ObservedToolState::Available {
                        executable: executable.clone(),
                        artifact_digest: digest.clone(),
                        _provisioned_at: timestamp_now(),
                    },
                );
                ProvisionResult::Available {
                    name: spec.name,
                    version: spec.version,
                    executable,
                    artifact_digest: digest,
                }
            }
            Err(err) => {
                let error = error_string(&err);
                tracing::warn!(
                    name = "ToolProvisionStatus",
                    status = "Provision::Failed",
                    tool = %spec.name,
                    version = %spec.version,
                    error = %error,
                );
                self.observed.insert(
                    key,
                    ObservedToolState::Failed {
                        error: error.clone(),
                        _last_attempt: SystemTime::now(),
                    },
                );
                ProvisionResult::Failed {
                    name: spec.name,
                    version: spec.version,
                    error,
                }
            }
        };
        self.publish_attrs(cx.instance());
        if let Err(e) = message.reply.send(cx, reply) {
            tracing::debug!(
                name = "ToolProvisionStatus",
                status = "Provision::ReplyDropped",
                error = %e,
                "ProvisionTool reply failed (caller gone?)",
            );
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<ResolveTool> for ToolProvisionActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: ResolveTool,
    ) -> Result<(), anyhow::Error> {
        let reply = self.resolve(&message.tool, message.version.as_deref());
        log_resolve_result(&reply);

        if let Err(e) = message.reply.send(cx, reply) {
            tracing::debug!(
                name = "ToolProvisionStatus",
                status = "Resolve::ReplyDropped",
                tool = %message.tool,
                requested_version = ?message.version,
                error = %e,
                "ResolveTool reply failed (caller gone?)",
            );
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<QueryToolInventory> for ToolProvisionActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: QueryToolInventory,
    ) -> Result<(), anyhow::Error> {
        let inventory = self.inventory();
        tracing::info!(
            name = "ToolProvisionStatus",
            status = "Inventory::Queried",
            tool_count = inventory.tools.len(),
        );
        if let Err(e) = message.reply.send(cx, inventory) {
            tracing::debug!(
                name = "ToolProvisionStatus",
                status = "Inventory::ReplyDropped",
                error = %e,
                "QueryToolInventory reply failed (caller gone?)",
            );
        }
        Ok(())
    }
}

impl ObservedToolState {
    fn to_inventory_state(&self) -> ToolInventoryState {
        match self {
            Self::Fetching { .. } => ToolInventoryState::Fetching,
            Self::Available {
                executable,
                artifact_digest,
                ..
            } => ToolInventoryState::Available {
                executable: executable.clone(),
                artifact_digest: artifact_digest.clone(),
            },
            Self::Failed { error, .. } => ToolInventoryState::Failed {
                error: error.clone(),
            },
            Self::CachedButNotRegistered {
                executable,
                artifact_digest,
                ..
            } => ToolInventoryState::CachedButNotRegistered {
                executable: executable.clone(),
                artifact_digest: artifact_digest.clone(),
            },
        }
    }
}

fn tool_key(name: &str, version: &str) -> String {
    format!("{name}@{version}")
}

fn split_key(key: &str) -> (String, String) {
    match key.split_once('@') {
        Some((name, version)) => (name.to_string(), version.to_string()),
        None => (key.to_string(), String::new()),
    }
}

fn timestamp_now() -> String {
    chrono::Utc::now().to_rfc3339()
}

fn error_string(err: &ProvisionError) -> String {
    err.to_string()
}

fn log_resolve_result(result: &ResolveResult) {
    match result {
        ResolveResult::Available {
            name,
            version,
            executable,
        } => {
            tracing::info!(
                name = "ToolProvisionStatus",
                status = "Resolve::Available",
                tool = %name,
                version = %version,
                executable = %executable.display(),
            );
        }
        ResolveResult::NotProvisioned { tool, version } => {
            tracing::info!(
                name = "ToolProvisionStatus",
                status = "Resolve::NotProvisioned",
                tool = %tool,
                requested_version = ?version,
            );
        }
        ResolveResult::Failed {
            name,
            version,
            error,
        } => {
            tracing::warn!(
                name = "ToolProvisionStatus",
                status = "Resolve::Failed",
                tool = %name,
                version = %version,
                error = %error,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use tool_fetch::ToolSpec;

    use super::*;

    fn empty_spec(name: &str, version: &str) -> ToolSpec {
        ToolSpec {
            name: name.to_string(),
            version: version.to_string(),
            platforms: HashMap::new(),
        }
    }

    #[test]
    fn inventory_projects_observed_states_in_stable_order() {
        let mut actor = ToolProvisionActor::new();
        actor.observed.insert(
            tool_key("z-tool", "2.0.0"),
            ObservedToolState::Failed {
                error: "boom".to_string(),
                _last_attempt: SystemTime::UNIX_EPOCH,
            },
        );
        actor.observed.insert(
            tool_key("a-tool", "1.0.0"),
            ObservedToolState::Available {
                executable: PathBuf::from("/tmp/a-tool"),
                artifact_digest: "abc".to_string(),
                _provisioned_at: "now".to_string(),
            },
        );

        // TP-4: inventory is a deterministic projection of observed
        // cache/provision state.
        let inventory = actor.inventory();

        assert_eq!(inventory.tools.len(), 2);
        assert_eq!(inventory.tools[0].name, "a-tool");
        assert_eq!(inventory.tools[1].name, "z-tool");
        assert_eq!(
            inventory.tools[0].state,
            ToolInventoryState::Available {
                executable: PathBuf::from("/tmp/a-tool"),
                artifact_digest: "abc".to_string(),
            }
        );
        assert_eq!(
            inventory.tools[1].state,
            ToolInventoryState::Failed {
                error: "boom".to_string(),
            }
        );
    }

    #[test]
    fn resolve_ignores_cache_only_entries_until_desired() {
        let mut actor = ToolProvisionActor::new();
        actor.observed.insert(
            tool_key("py-spy", "0.4.1"),
            ObservedToolState::CachedButNotRegistered {
                executable: PathBuf::from("/tmp/py-spy"),
                artifact_digest: "digest".to_string(),
                _provisioned_at: "now".to_string(),
            },
        );

        // TP-2: scan-discovered artifacts are inventory only until a
        // provision request registers desired state.
        assert_eq!(
            actor.resolve("py-spy", Some("0.4.1")),
            ResolveResult::NotProvisioned {
                tool: "py-spy".to_string(),
                version: Some("0.4.1".to_string()),
            }
        );

        actor.desired.insert(
            "py-spy".to_string(),
            DesiredTool {
                version: "0.4.1".to_string(),
                _spec: empty_spec("py-spy", "0.4.1"),
            },
        );
        actor.observed.insert(
            tool_key("py-spy", "0.4.1"),
            ObservedToolState::Available {
                executable: PathBuf::from("/tmp/py-spy"),
                artifact_digest: "digest".to_string(),
                _provisioned_at: "now".to_string(),
            },
        );

        assert_eq!(
            actor.resolve("py-spy", None),
            ResolveResult::Available {
                name: "py-spy".to_string(),
                version: "0.4.1".to_string(),
                executable: PathBuf::from("/tmp/py-spy"),
            }
        );
    }
}
