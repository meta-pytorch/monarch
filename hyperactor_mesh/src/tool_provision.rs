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
//! - **TP-7 (provision-non-blocking):** `ProvisionTool` records
//!   `Fetching` state and returns immediately; the underlying
//!   `cache.provision` runs in a spawned task and reports back via a
//!   local `ProvisionDone` message. `QueryToolInventory` and
//!   `ResolveTool` continue to answer while a provision is in flight,
//!   and the external `ProvisionResult` reply is sent only after the
//!   actor has committed the post-provision state.
//! - **TP-8 (single-flight-per-key):** At most one provision is in
//!   flight per `name@version` key. A `ProvisionTool` request that
//!   arrives while the same key is already `Fetching` is rejected
//!   immediately with a deterministic failure rather than spawning a
//!   duplicate worker, eliminating the same-key out-of-order
//!   completion race without needing per-attempt generation ids.

use std::collections::HashMap;
use std::path::PathBuf;

use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use hyperactor::Actor;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::OncePortRef;
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
use tracing::Instrument;
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
    pub reply: hyperactor::OncePortRef<ProvisionResult>,
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
    pub reply: hyperactor::OncePortRef<ResolveResult>,
}
wirevalue::register_type!(ResolveTool);

/// Request the full per-host tool inventory.
#[derive(Debug, Serialize, Deserialize, Named, Handler, HandleClient, RefClient)]
pub struct QueryToolInventory {
    /// Reply port receiving the inventory snapshot.
    #[reply]
    pub reply: hyperactor::OncePortRef<ToolInventory>,
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

/// Tool state observed by this actor and surfaced through inventory.
///
/// Each variant carries an RFC 3339 UTC timestamp so the operator-facing
/// inventory can show "fetching since 14:32", "provisioned at 09:15",
/// or "last failed at 12:47" without re-deriving age from a bare flag.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub enum ToolInventoryState {
    /// Tool is registered and available.
    Available {
        /// Executable path.
        executable: PathBuf,
        /// Downloaded artifact digest.
        artifact_digest: String,
        /// When the artifact was last successfully provisioned.
        provisioned_at: DateTime<Utc>,
    },
    /// Provisioning is in progress.
    Fetching {
        /// When the in-flight provision attempt started.
        started_at: DateTime<Utc>,
    },
    /// Last provision attempt failed.
    Failed {
        /// Error string.
        error: String,
        /// When the failure occurred.
        last_attempt: DateTime<Utc>,
    },
    /// Artifact exists in cache but no desired spec registered it.
    CachedButNotRegistered {
        /// Executable path.
        executable: PathBuf,
        /// Downloaded artifact digest.
        artifact_digest: String,
        /// When the cache metadata recorded provisioning.
        provisioned_at: DateTime<Utc>,
    },
}

/// Per-host actor that provisions and resolves diagnostic tools.
#[hyperactor::export(handlers = [ProvisionTool, ResolveTool, QueryToolInventory])]
pub struct ToolProvisionActor {
    cache: ToolCache,
    /// Desired version per tool name, populated by `ProvisionTool` and
    /// consulted by `ResolveTool` when the caller doesn't pin a version.
    desired: HashMap<String, String>,
    /// Observed state per tool key (`name@version`); doubles as the
    /// inventory projection because the wire and internal models share
    /// one shape.
    observed: HashMap<String, ToolInventoryState>,
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
                    state: state.clone(),
                }
            })
            .collect();
        tools.sort_by(|a, b| a.name.cmp(&b.name).then(a.version.cmp(&b.version)));
        ToolInventory { tools }
    }

    fn resolve(&self, tool: &str, requested_version: Option<&str>) -> ResolveResult {
        let version = match requested_version {
            Some(version) => Some(version.to_string()),
            None => self.desired.get(tool).cloned(),
        };

        if let Some(version) = version {
            let key = tool_key(tool, &version);
            match self.observed.get(&key) {
                Some(ToolInventoryState::Available { executable, .. }) => {
                    ResolveResult::Available {
                        name: tool.to_string(),
                        version,
                        executable: executable.clone(),
                    }
                }
                Some(ToolInventoryState::Failed { error, .. }) => ResolveResult::Failed {
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

        let mut scanned = 0_usize;
        for artifact in self.cache.scan() {
            let key = tool_key(&artifact.name, &artifact.version);
            self.observed.insert(
                key,
                ToolInventoryState::CachedButNotRegistered {
                    executable: artifact.executable,
                    artifact_digest: artifact.digest,
                    provisioned_at: artifact.provisioned_at,
                },
            );
            scanned += 1;
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

/// Local-only completion event. Not exported — delivered via
/// `PortHandle` from the provision worker back to the actor so the
/// actor stays the single serialization point for `observed` updates.
struct ProvisionDone {
    key: String,
    reply_port: OncePortRef<ProvisionResult>,
    result: ProvisionResult,
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

        // TP-8: reject duplicate same-key provisions while one is
        // already in flight, so two completions cannot race on
        // `observed[key]`.
        if matches!(
            self.observed.get(&key),
            Some(ToolInventoryState::Fetching { .. })
        ) {
            let reply = ProvisionResult::Failed {
                name: spec.name,
                version: spec.version,
                error: format!("provision already in progress for {key}"),
            };
            if let Err(e) = message.reply.send(cx, reply) {
                tracing::debug!("ProvisionTool single-flight reply failed: {e}");
            }
            return Ok(());
        }

        self.desired.insert(spec.name.clone(), spec.version.clone());
        self.observed.insert(
            key.clone(),
            ToolInventoryState::Fetching {
                started_at: Utc::now(),
            },
        );
        self.publish_attrs(cx.instance());

        // TP-7: drive the provision off the actor mailbox so concurrent
        // QueryToolInventory and ResolveTool requests can answer while
        // the artifact is being fetched/extracted. The detached task is
        // instrumented with this actor's `recording_span` so tracing
        // events emitted by `tool_fetch` during the provision land in
        // the actor's flight recorder rather than disappearing into the
        // global subscriber.
        let done_port = cx.port::<ProvisionDone>();
        let cache = self.cache.clone();
        let reply_port = message.reply;
        let task_key = key.clone();
        let recording_span = cx.instance().recording_span();
        tokio::spawn(
            async move {
                let result = run_provision(&cache, &spec).await;
                let client = Instance::<()>::self_client();
                let _ = done_port.send(
                    client,
                    ProvisionDone {
                        key: task_key,
                        reply_port,
                        result,
                    },
                );
            }
            .instrument(recording_span),
        );

        Ok(())
    }
}

#[async_trait]
impl Handler<ProvisionDone> for ToolProvisionActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: ProvisionDone,
    ) -> Result<(), anyhow::Error> {
        let ProvisionDone {
            key,
            reply_port,
            result,
        } = message;

        match &result {
            ProvisionResult::Available {
                executable,
                artifact_digest,
                ..
            } => {
                self.observed.insert(
                    key,
                    ToolInventoryState::Available {
                        executable: executable.clone(),
                        artifact_digest: artifact_digest.clone(),
                        provisioned_at: Utc::now(),
                    },
                );
            }
            ProvisionResult::Failed { error, .. } => {
                self.observed.insert(
                    key,
                    ToolInventoryState::Failed {
                        error: error.clone(),
                        last_attempt: Utc::now(),
                    },
                );
            }
        }
        self.publish_attrs(cx.instance());
        // TP-7: state is committed before the external reply, so a
        // caller that observes `Available`/`Failed` from this reply can
        // immediately query inventory and see the same state.
        if let Err(e) = reply_port.send(cx, result) {
            tracing::debug!("ProvisionTool reply failed (caller gone?): {e}");
        }
        Ok(())
    }
}

async fn run_provision(cache: &ToolCache, spec: &ToolSpec) -> ProvisionResult {
    let platform = match tool_fetch::current_platform() {
        Ok(platform) => platform,
        Err(err) => {
            return ProvisionResult::Failed {
                name: spec.name.clone(),
                version: spec.version.clone(),
                error: error_string(&err),
            };
        }
    };
    let digest = spec
        .platforms
        .get(&platform)
        .map(|entry| entry.digest.clone())
        .unwrap_or_default();
    match cache.provision(spec, platform).await {
        Ok(executable) => ProvisionResult::Available {
            name: spec.name.clone(),
            version: spec.version.clone(),
            executable,
            artifact_digest: digest,
        },
        Err(err) => ProvisionResult::Failed {
            name: spec.name.clone(),
            version: spec.version.clone(),
            error: error_string(&err),
        },
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

fn tool_key(name: &str, version: &str) -> String {
    format!("{name}@{version}")
}

fn split_key(key: &str) -> (String, String) {
    match key.split_once('@') {
        Some((name, version)) => (name.to_string(), version.to_string()),
        None => (key.to_string(), String::new()),
    }
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
    use std::time::Duration;

    use hyperactor::Proc;
    use hyperactor::channel::ChannelTransport;
    use hyperactor::mailbox::open_once_port;

    use super::*;

    fn fixed_timestamp() -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(0, 0).unwrap()
    }

    #[test]
    fn inventory_projects_observed_states_in_stable_order() {
        let mut actor = ToolProvisionActor::new();
        let timestamp = fixed_timestamp();
        actor.observed.insert(
            tool_key("z-tool", "2.0.0"),
            ToolInventoryState::Failed {
                error: "boom".to_string(),
                last_attempt: timestamp,
            },
        );
        actor.observed.insert(
            tool_key("a-tool", "1.0.0"),
            ToolInventoryState::Available {
                executable: PathBuf::from("/tmp/a-tool"),
                artifact_digest: "abc".to_string(),
                provisioned_at: timestamp,
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
                provisioned_at: timestamp,
            }
        );
        assert_eq!(
            inventory.tools[1].state,
            ToolInventoryState::Failed {
                error: "boom".to_string(),
                last_attempt: timestamp,
            }
        );
    }

    #[test]
    fn resolve_ignores_cache_only_entries_until_desired() {
        let mut actor = ToolProvisionActor::new();
        let timestamp = fixed_timestamp();
        actor.observed.insert(
            tool_key("py-spy", "0.4.1"),
            ToolInventoryState::CachedButNotRegistered {
                executable: PathBuf::from("/tmp/py-spy"),
                artifact_digest: "digest".to_string(),
                provisioned_at: timestamp,
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

        actor
            .desired
            .insert("py-spy".to_string(), "0.4.1".to_string());
        actor.observed.insert(
            tool_key("py-spy", "0.4.1"),
            ToolInventoryState::Available {
                executable: PathBuf::from("/tmp/py-spy"),
                artifact_digest: "digest".to_string(),
                provisioned_at: timestamp,
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

    /// Bind a TCP listener that accepts but never writes a response,
    /// keeping every accepted connection alive. Returns the bound URL
    /// and the spawned task handle (dropped on test exit).
    async fn slow_http_server() -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        });
        (format!("http://{addr}/artifact"), task)
    }

    #[tokio::test]
    async fn fetching_observable_and_single_flight_during_provision() {
        // TP-7: ProvisionTool returns immediately and does not block
        // the actor mailbox; QueryToolInventory keeps answering while
        // the underlying provision is in flight.
        // TP-8: a duplicate ProvisionTool for the same name@version
        // while one is already Fetching is rejected with a
        // deterministic single-flight failure rather than spawning a
        // second worker.
        let (url, _server_task) = slow_http_server().await;

        let bytes = b"#!/bin/sh\n";
        let mut platforms = HashMap::new();
        platforms.insert(
            tool_fetch::current_platform().unwrap(),
            tool_fetch::PlatformEntry {
                size: bytes.len() as u64,
                hash_algorithm: tool_fetch::HashAlgorithm::Sha256,
                digest: "irrelevant".to_string(),
                format: tool_fetch::ArtifactFormat::Plain,
                executable_path: None,
                providers: vec![tool_fetch::Provider::Http { url }],
            },
        );
        let spec = ToolSpec {
            name: "demo-tool".to_string(),
            version: "1.0.0".to_string(),
            platforms,
        };

        let temp = tempfile::TempDir::new().unwrap();
        let server_proc = Proc::direct(
            ChannelTransport::Unix.any(),
            "tool_provision_test_server".to_string(),
        )
        .unwrap();
        let actor = server_proc
            .spawn(
                TOOL_PROVISION_ACTOR_NAME,
                ToolProvisionActor::with_cache(ToolCache::new(temp.path())),
            )
            .unwrap();

        let client_proc = Proc::direct(
            ChannelTransport::Unix.any(),
            "tool_provision_test_client".to_string(),
        )
        .unwrap();
        let (client, _client_handle) = client_proc.instance("client").unwrap();

        // Fire-and-forget the first ProvisionTool. Don't await its
        // reply: the slow server holds the connection open so the
        // spawned worker stays in flight for the lifetime of the test.
        let (first_reply, _first_rx) = open_once_port::<ProvisionResult>(&client);
        let mut first_ref = first_reply.bind();
        first_ref.return_undeliverable(false);
        actor
            .send(
                &client,
                ProvisionTool {
                    spec: spec.clone(),
                    reply: first_ref,
                },
            )
            .unwrap();

        // TP-7: QueryToolInventory must answer immediately, not wait
        // behind the in-flight provision.
        let (inventory_reply, inventory_rx) = open_once_port::<ToolInventory>(&client);
        let mut inventory_ref = inventory_reply.bind();
        inventory_ref.return_undeliverable(false);
        actor
            .send(
                &client,
                QueryToolInventory {
                    reply: inventory_ref,
                },
            )
            .unwrap();
        let inventory = tokio::time::timeout(Duration::from_secs(5), inventory_rx.recv())
            .await
            .expect("TP-7: inventory must answer while provision is in flight")
            .unwrap();
        assert_eq!(inventory.tools.len(), 1);
        assert_eq!(inventory.tools[0].name, "demo-tool");
        assert!(matches!(
            inventory.tools[0].state,
            ToolInventoryState::Fetching { .. }
        ));

        // TP-8: a second ProvisionTool for the same key while the
        // first is in flight returns a single-flight failure.
        let (second_reply, second_rx) = open_once_port::<ProvisionResult>(&client);
        let mut second_ref = second_reply.bind();
        second_ref.return_undeliverable(false);
        actor
            .send(
                &client,
                ProvisionTool {
                    spec,
                    reply: second_ref,
                },
            )
            .unwrap();
        let second_reply = tokio::time::timeout(Duration::from_secs(5), second_rx.recv())
            .await
            .expect("TP-8: duplicate provision must fail fast")
            .unwrap();
        match second_reply {
            ProvisionResult::Failed {
                name,
                version,
                error,
            } => {
                assert_eq!(name, "demo-tool");
                assert_eq!(version, "1.0.0");
                assert!(
                    error.contains("already in progress"),
                    "unexpected error: {error}"
                );
            }
            other => panic!("expected single-flight Failed, got {other:?}"),
        }
    }
}
