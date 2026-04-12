/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Mesh-topology introspection types and attrs.
//!
//! This module owns the typed internal model used by mesh-admin and the
//! TUI: mesh-topology attr keys, typed attrs views, `NodeRef`, and the
//! domain `NodePayload` / `NodeProperties` / `FailureInfo` values derived
//! from `hyperactor::introspect::IntrospectResult`.
//!
//! These keys are published by `HostMeshAgent`, `ProcAgent`, and
//! `MeshAdminAgent` to describe mesh topology (hosts, procs, root).
//! Actor-runtime keys (status, actor_type, messages_processed, etc.) are
//! declared in `hyperactor::introspect`.
//!
//! The HTTP wire representations live in [`dto`]. That submodule owns the
//! curl-friendly JSON contract, schema/OpenAPI generation, and boundary
//! invariants for string-encoded references and timestamps. This module
//! keeps the internal typed invariants.
//!
//! See `hyperactor::introspect` for naming convention, invariant
//! labels, and the `IntrospectAttr` meta-attribute pattern.
//!
//! ## Mesh key invariants (MK-*)
//!
//! - **MK-1 (metadata completeness):** Every mesh-topology
//!   introspection key must carry `@meta(INTROSPECT = ...)` with
//!   non-empty `name` and `desc`.
//! - **MK-2 (short-name uniqueness):** Covered by
//!   `test_introspect_short_names_are_globally_unique` in
//!   `hyperactor::introspect` (cross-crate).
//!
//! ## HTTP boundary invariants (HB-*)
//!
//! These govern the HTTP DTO layer in [`dto`].
//!
//! - **HB-1 (typed-internal, string-external):** `NodeRef`, `ActorId`,
//!   `ProcId`, and `SystemTime` are typed Rust values internally. At the
//!   HTTP JSON boundary, [`dto::NodePayloadDto`],
//!   [`dto::NodePropertiesDto`], and [`dto::FailureInfoDto`] encode them
//!   as canonical strings.
//! - **HB-2 (round-trip):** The HTTP string forms round-trip through the
//!   internal typed parsers (`NodeRef::from_str`, `ActorId::from_str`,
//!   `humantime::parse_rfc3339`). Timestamps are formatted at
//!   millisecond precision; sub-millisecond values are truncated at
//!   the boundary.
//! - **HB-3 (schema-honesty):** Schema/OpenAPI are generated from the DTO
//!   types, so the published schema reflects the actual wire format rather
//!   than the internal domain representation.
//!
//! ## Attrs invariants (IA-*)
//!
//! These govern how `IntrospectResult.attrs` is built in
//! `hyperactor::introspect` and how `properties` is derived via
//! `derive_properties`.
//!
//! - **IA-1 (attrs-json):** `IntrospectResult.attrs` is always a
//!   valid JSON object string.
//! - **IA-2 (runtime-precedence):** Runtime-owned introspection keys
//!   override any same-named keys in published attrs.
//! - **IA-3 (status-shape):** `status_reason` is present in attrs
//!   iff the status string carries a reason.
//! - **IA-4 (failure-shape):** `failure_*` attrs are present iff
//!   effective status is `failed`.
//! - **IA-5 (payload-totality):** Every `IntrospectResult` sets
//!   `attrs` -- never omitted, never null.
//! - **IA-6 (open-row-forward-compat):** View decoders ignore
//!   unknown attrs keys; only required known keys and local
//!   invariants affect decoding outcome. Concretized by AV-3.
//!
//! ## Attrs view invariants (AV-*)
//!
//! These govern the typed view layer (`*AttrsView` structs).
//!
//! - **AV-1 (view-roundtrip):** For each view V,
//!   `V::from_attrs(&v.to_attrs()) == Ok(v)` (modulo documented
//!   normalization/defaulting).
//! - **AV-2 (required-key-strictness):** `from_attrs` fails iff
//!   required keys for that view are missing.
//! - **AV-3 (unknown-key-tolerance):** Unknown attrs keys must
//!   not affect successful decode outcome. Concretization of
//!   IA-6.
//!
//! ## Derive invariants (DP-*)
//!
//! - **DP-1 (derive-precedence):** `derive_properties` dispatches
//!   on `node_type` first, then falls back to `error_code`,
//!   then `status`, then unknown. This order is the canonical
//!   detection chain.
//! - **DP-2 (derive-totality-on-parse-failure):**
//!   `derive_properties` is total; malformed or incoherent attrs
//!   never panic and map to `NodeProperties::Error` with detail.
//! - **DP-3 (derive-precedence-stability):**
//!   `derive_properties` detection order is stable and explicit:
//!   `node_type` > `error_code` > `status` > unknown.
//! - **DP-4 (error-on-decode-failure):** Any view decode or
//!   invariant failure maps to a deterministic
//!   `NodeProperties::Error` with a `malformed_*` code family,
//!   without panic.
//!
//! ## py-spy integration (PS-*)
//!
//! - **PS-1 (target locality):** `PySpyDump` always targets
//!   `std::process::id()` of the handling ProcAgent process. No
//!   caller-supplied PID exists in the API.
//! - **PS-2 (deterministic failure shape):** Execution failures are
//!   classified into `BinaryNotFound { searched }` vs
//!   `Failed { pid, binary, exit_code, stderr }`, never collapsed.
//! - **PS-3 (binary resolution order):** Resolution order is exactly:
//!   `PYSPY_BIN` config attr (if non-empty) then `"py-spy"` on PATH.
//!   The attr is read via `hyperactor_config::global::get_cloned`;
//!   env var `PYSPY_BIN` feeds in through the config layer.
//!   If the first attempt is not found, the fallback attempt is
//!   required.
//! - **PS-4 (structured JSON output):** py-spy runs with `--json`;
//!   output is parsed into `Vec<PySpyStackTrace>`. Parse failure
//!   maps to `PySpyResult::Failed`.
//! - **PS-5 (subprocess timeout):** `try_exec` bounds the py-spy
//!   subprocess inside the worker to `MESH_ADMIN_PYSPY_TIMEOUT`
//!   (default 10s). The budget is sized for `--native --native-all`
//!   which unwinds native stacks via libunwind — significantly
//!   slower than Python-only capture on loaded hosts. On expiry the
//!   child is killed and reaped, and the worker returns
//!   `Failed { stderr: "…timed out…" }`.
//! - **PS-6 (bridge timeout):** The HTTP bridge uses a separate
//!   `MESH_ADMIN_PYSPY_BRIDGE_TIMEOUT` (default 13s), which must
//!   exceed `MESH_ADMIN_PYSPY_TIMEOUT` so the subprocess kill/reap
//!   and reply can arrive before the bridge declares
//!   `gateway_timeout`. Independent of
//!   `MESH_ADMIN_SINGLE_HOST_TIMEOUT`.
//! - **PS-7 (non-blocking delegation):** ProcAgent never awaits
//!   py-spy execution inline. On `PySpyDump` it spawns a child
//!   `PySpyWorker`, forwards the request, and returns immediately.
//! - **PS-8 (worker lifecycle):** Each `PySpyWorker` handles
//!   exactly one forwarded `RunPySpyDump`, replies directly to the
//!   forwarded `OncePortRef`, then self-terminates via
//!   `cx.stop()`. Clean exit, no supervision event.
//! - **PS-9 (concurrent dumps):** py-spy is spawn-per-request, so
//!   overlapping dumps on the same proc are allowed. Each worker
//!   runs independently.
//! - **PS-10 (nonblocking retry):** In nonblocking mode, `try_exec`
//!   retries up to 3 times with 100ms backoff on failure, because
//!   py-spy can segfault reading mutating process memory. All
//!   attempts share a single deadline bounded by
//!   `MESH_ADMIN_PYSPY_TIMEOUT` (PS-5).
//! - **PS-11a (native-all-immediate-downgrade):** If py-spy rejects
//!   `--native-all` with the recognized unsupported-flag signature
//!   (exit code 2, stderr mentions `--native-all`), `try_exec`
//!   retries immediately with `native_all = false` in the same outer
//!   attempt.
//! - **PS-11b (native-all-no-retry-consumption):** That downgrade
//!   retry does not consume an outer nonblocking retry slot (PS-10)
//!   and does not incur the 100ms inter-attempt backoff.
//! - **PS-11c (native-all-downgrade-warning):** A successful
//!   downgraded result includes the warning `"--native-all
//!   unsupported by this py-spy; fell back to --native"`.
//! - **PS-11d (native-all-failure-passthrough):** If the downgraded
//!   retry also fails, the failure flows through the normal
//!   nonblocking retry logic (PS-10) unchanged.
//! - **PS-11e (native-all-sticky-downgrade):** Once the
//!   unsupported-flag signature is detected,
//!   `effective_opts.native_all` remains `false` for all subsequent
//!   outer retries. The flag is not re-tested on later attempts.
//! - **PS-12 (universal py-spy):** Worker procs and the service
//!   proc can handle `PySpyDump`. Worker procs handle it via
//!   ProcAgent; the service proc handles it via HostAgent (same
//!   spawn-worker pattern). `pyspy_bridge` routes by proc name:
//!   if `proc_id.base_name() == SERVICE_PROC_NAME`, the target
//!   is `host_agent`; otherwise `proc_agent[0]`. Procs lacking
//!   either agent (e.g. mesh-admin) fast-fail via PS-13.
//! - **PS-13 (defensive probe):** Before sending `PySpyDump`,
//!   `pyspy_bridge` probes the selected actor with an introspect
//!   query bounded by `MESH_ADMIN_QUERY_CHILD_TIMEOUT` (default
//!   100ms). Three outcomes: (a) probe reply arrives — proceed
//!   with `PySpyDump`; (b) probe times out or recv closes —
//!   return `not_found` (actor absent/unreachable); (c) probe
//!   send itself fails — return `internal_error` (bridge-side
//!   infrastructure failure). Cases (b) and (c) fast-fail
//!   instead of waiting the full 13s
//!   `MESH_ADMIN_PYSPY_BRIDGE_TIMEOUT`.
//! - **PS-14 (reachability-based capability):** A proc supports
//!   py-spy iff its stable handler actor is reachable: the
//!   service proc requires a reachable `host_agent`; non-service
//!   procs require a reachable `proc_agent[0]`. `PySpyWorker` is
//!   transient per-request machinery (spawned on `PySpyDump`,
//!   stopped after replying) and is not part of the reachability
//!   contract.
//!
//! v1 contract notes:
//! - The current py-spy bridge expects a ProcId-form reference and
//!   rejects other forms as `bad_request`. This may be broadened in
//!   future versions.
//! - If `worker.send()` fails after the reply port has moved into
//!   `RunPySpyDump`, the caller receives no explicit
//!   `PySpyResult::Failed` — they observe a timeout.
//!   `MailboxSenderError` does not carry the unsent message, so the
//!   port is irrecoverable on this path.
//! - **Contract change (D96756537 follow-up):** `PySpyResult::Ok`
//!   replaced `stack: String` (raw py-spy text) with
//!   `stack_traces: Vec<PySpyStackTrace>` (structured JSON) and
//!   added `warnings: Vec<String>`. Clients reading the old `stack`
//!   field will see it absent; they must migrate to `stack_traces`.
//!
//! ## py-spy profiling (PP-*)
//!
//! Profile capture (`py-spy record`) is a separate contract from
//! dump (`py-spy dump`). Types, messages, workers, and routes are
//! independent — no shared state, no shared timeout budget.
//!
//! - **PP-1 (input validation):** `duration_s` (u32) must be
//!   non-zero and at most `MESH_ADMIN_PYSPY_MAX_PROFILE_DURATION`.
//!   `rate_hz` must be 1..1000. Violations → 400 before any
//!   actor messaging.
//! - **PP-2 (dynamic timeout cascade):** Subprocess timeout =
//!   `duration_s + 15s`. Bridge timeout = subprocess + 5s.
//!   Computed per-request from validated opts, not static config.
//! - **PP-3 (temp file lifecycle):** `py-spy record` writes to a
//!   temp file; the worker reads it after successful exit and
//!   deletes via tempfile drop. On failure or timeout, stderr is
//!   captured. On timeout, the child is explicitly killed and
//!   reaped via `start_kill()` + `wait().await`. If the file is
//!   missing, empty, or unreadable after successful exit, the
//!   result is `OutputMissing`, `OutputEmpty`, or
//!   `OutputReadFailure`, not `Ok`.
//! - **PP-4 (target locality):** Inherits PS-1 — always targets
//!   `std::process::id()`, never a caller-supplied PID.
//! - **PP-5 (separate worker):** `PySpyProfileWorker` is a
//!   distinct actor from `PySpyWorker`. Profile durations block
//!   for seconds to minutes; isolation prevents starving dumps.
//! - **PP-6 (wire projection):** `ProfileExecOutcome` maps to
//!   `PySpyProfileResult` 1:1 via `From`. Every internal variant
//!   has an identically-named wire variant. The only shape change
//!   is `TimedOut.timeout: Duration` → `TimedOut.timeout_s: u64`.
//!
//! ## Mesh-admin config (MA-*)
//!
//! - **MA-C1 (timeout config centralization):** Mesh-admin timeout
//!   budgets are read from config attrs at call-time, with defaults
//!   in `config.rs`. No hardcoded timeout constants in
//!   `mesh_admin.rs`.

pub mod dto;

use hyperactor_config::Attrs;
use hyperactor_config::INTROSPECT;
use hyperactor_config::IntrospectAttr;
use hyperactor_config::declare_attrs;

// See MK-1, MK-2, IA-1..IA-5 in module doc.
declare_attrs! {
    /// Topology role of this node: "root", "host", "proc", "error".
    @meta(INTROSPECT = IntrospectAttr {
        name: "node_type".into(),
        desc: "Topology role: root, host, proc, error".into(),
    })
    pub attr NODE_TYPE: String;

    /// Host network address (e.g. "10.0.0.1:8080").
    @meta(INTROSPECT = IntrospectAttr {
        name: "addr".into(),
        desc: "Host network address".into(),
    })
    pub attr ADDR: String;

    /// Number of procs on a host.
    @meta(INTROSPECT = IntrospectAttr {
        name: "num_procs".into(),
        desc: "Number of procs on a host".into(),
    })
    pub attr NUM_PROCS: usize = 0;

    /// Human-readable proc name.
    @meta(INTROSPECT = IntrospectAttr {
        name: "proc_name".into(),
        desc: "Human-readable proc name".into(),
    })
    pub attr PROC_NAME: String;

    /// Number of actors in a proc.
    @meta(INTROSPECT = IntrospectAttr {
        name: "num_actors".into(),
        desc: "Number of actors in a proc".into(),
    })
    pub attr NUM_ACTORS: usize = 0;

    /// References of system/infrastructure children.
    @meta(INTROSPECT = IntrospectAttr {
        name: "system_children".into(),
        desc: "References of system/infrastructure children".into(),
    })
    pub attr SYSTEM_CHILDREN: Vec<NodeRef>;

    /// References of stopped children (proc only).
    @meta(INTROSPECT = IntrospectAttr {
        name: "stopped_children".into(),
        desc: "References of stopped children".into(),
    })
    pub attr STOPPED_CHILDREN: Vec<NodeRef>;

    /// Cap on stopped children retention.
    @meta(INTROSPECT = IntrospectAttr {
        name: "stopped_retention_cap".into(),
        desc: "Maximum number of stopped children retained".into(),
    })
    pub attr STOPPED_RETENTION_CAP: usize = 0;

    /// Whether this proc is refusing new spawns due to actor
    /// failures.
    @meta(INTROSPECT = IntrospectAttr {
        name: "is_poisoned".into(),
        desc: "Whether this proc is poisoned (refusing new spawns)".into(),
    })
    pub attr IS_POISONED: bool = false;

    /// Count of failed actors in a proc.
    @meta(INTROSPECT = IntrospectAttr {
        name: "failed_actor_count".into(),
        desc: "Number of failed actors in this proc".into(),
    })
    pub attr FAILED_ACTOR_COUNT: usize = 0;

    /// Timestamp when the mesh was started.
    @meta(INTROSPECT = IntrospectAttr {
        name: "started_at".into(),
        desc: "Timestamp when the mesh was started".into(),
    })
    pub attr STARTED_AT: std::time::SystemTime;

    /// Username who started the mesh.
    @meta(INTROSPECT = IntrospectAttr {
        name: "started_by".into(),
        desc: "Username who started the mesh".into(),
    })
    pub attr STARTED_BY: String;

    /// Number of hosts in the mesh (root only).
    @meta(INTROSPECT = IntrospectAttr {
        name: "num_hosts".into(),
        desc: "Number of hosts in the mesh".into(),
    })
    pub attr NUM_HOSTS: usize = 0;

}

use hyperactor::introspect::AttrsViewError;

/// Typed view over attrs for a root node.
#[derive(Debug, Clone, PartialEq)]
pub struct RootAttrsView {
    pub num_hosts: usize,
    pub started_at: SystemTime,
    pub started_by: String,
    pub system_children: Vec<NodeRef>,
}

impl RootAttrsView {
    /// Decode from an `Attrs` bag (AV-2, AV-3). Requires
    /// `STARTED_AT` and `STARTED_BY`; `NUM_HOSTS` defaults to 0,
    /// `SYSTEM_CHILDREN` defaults to empty.
    pub fn from_attrs(attrs: &Attrs) -> Result<Self, AttrsViewError> {
        let num_hosts = *attrs.get(NUM_HOSTS).unwrap_or(&0);
        let started_at = *attrs
            .get(STARTED_AT)
            .ok_or_else(|| AttrsViewError::missing("started_at"))?;
        let started_by = attrs
            .get(STARTED_BY)
            .ok_or_else(|| AttrsViewError::missing("started_by"))?
            .clone();
        let system_children = attrs.get(SYSTEM_CHILDREN).cloned().unwrap_or_default();
        Ok(Self {
            num_hosts,
            started_at,
            started_by,
            system_children,
        })
    }

    /// Encode into an `Attrs` bag (AV-1 round-trip producer).
    pub fn to_attrs(&self) -> Attrs {
        let mut attrs = Attrs::new();
        attrs.set(NODE_TYPE, "root".to_string());
        attrs.set(NUM_HOSTS, self.num_hosts);
        attrs.set(STARTED_AT, self.started_at);
        attrs.set(STARTED_BY, self.started_by.clone());
        attrs.set(SYSTEM_CHILDREN, self.system_children.clone());
        attrs
    }
}

/// Typed view over attrs for a host node.
#[derive(Debug, Clone, PartialEq)]
pub struct HostAttrsView {
    pub addr: String,
    pub num_procs: usize,
    pub system_children: Vec<NodeRef>,
}

impl HostAttrsView {
    /// Decode from an `Attrs` bag (AV-2, AV-3). Requires `ADDR`;
    /// `NUM_PROCS` defaults to 0, `SYSTEM_CHILDREN` defaults to
    /// empty.
    pub fn from_attrs(attrs: &Attrs) -> Result<Self, AttrsViewError> {
        let addr = attrs
            .get(ADDR)
            .ok_or_else(|| AttrsViewError::missing("addr"))?
            .clone();
        let num_procs = *attrs.get(NUM_PROCS).unwrap_or(&0);
        let system_children = attrs.get(SYSTEM_CHILDREN).cloned().unwrap_or_default();
        Ok(Self {
            addr,
            num_procs,
            system_children,
        })
    }

    /// Encode into an `Attrs` bag (AV-1 round-trip producer).
    pub fn to_attrs(&self) -> Attrs {
        let mut attrs = Attrs::new();
        attrs.set(NODE_TYPE, "host".to_string());
        attrs.set(ADDR, self.addr.clone());
        attrs.set(NUM_PROCS, self.num_procs);
        attrs.set(SYSTEM_CHILDREN, self.system_children.clone());
        attrs
    }
}

/// Typed view over attrs for a proc node.
#[derive(Debug, Clone, PartialEq)]
pub struct ProcAttrsView {
    pub proc_name: String,
    pub num_actors: usize,
    pub system_children: Vec<NodeRef>,
    pub stopped_children: Vec<NodeRef>,
    pub stopped_retention_cap: usize,
    pub is_poisoned: bool,
    pub failed_actor_count: usize,
}

impl ProcAttrsView {
    /// Decode from an `Attrs` bag (AV-2, AV-3). Requires
    /// `PROC_NAME`; remaining fields have defaults. Checks FI-5
    /// coherence.
    pub fn from_attrs(attrs: &Attrs) -> Result<Self, AttrsViewError> {
        let proc_name = attrs
            .get(PROC_NAME)
            .ok_or_else(|| AttrsViewError::missing("proc_name"))?
            .clone();
        let num_actors = *attrs.get(NUM_ACTORS).unwrap_or(&0);
        let system_children = attrs.get(SYSTEM_CHILDREN).cloned().unwrap_or_default();
        let stopped_children = attrs.get(STOPPED_CHILDREN).cloned().unwrap_or_default();
        let stopped_retention_cap = *attrs.get(STOPPED_RETENTION_CAP).unwrap_or(&0);
        let is_poisoned = *attrs.get(IS_POISONED).unwrap_or(&false);
        let failed_actor_count = *attrs.get(FAILED_ACTOR_COUNT).unwrap_or(&0);

        // FI-5: is_poisoned iff failed_actor_count > 0.
        if is_poisoned != (failed_actor_count > 0) {
            return Err(AttrsViewError::invariant(
                "FI-5",
                format!("is_poisoned={is_poisoned} but failed_actor_count={failed_actor_count}"),
            ));
        }

        Ok(Self {
            proc_name,
            num_actors,
            system_children,
            stopped_children,
            stopped_retention_cap,
            is_poisoned,
            failed_actor_count,
        })
    }

    /// Encode into an `Attrs` bag (AV-1 round-trip producer).
    pub fn to_attrs(&self) -> Attrs {
        let mut attrs = Attrs::new();
        attrs.set(NODE_TYPE, "proc".to_string());
        attrs.set(PROC_NAME, self.proc_name.clone());
        attrs.set(NUM_ACTORS, self.num_actors);
        attrs.set(SYSTEM_CHILDREN, self.system_children.clone());
        attrs.set(STOPPED_CHILDREN, self.stopped_children.clone());
        attrs.set(STOPPED_RETENTION_CAP, self.stopped_retention_cap);
        attrs.set(IS_POISONED, self.is_poisoned);
        attrs.set(FAILED_ACTOR_COUNT, self.failed_actor_count);
        attrs
    }
}

/// Typed view over attrs for an error node.
#[derive(Debug, Clone, PartialEq)]
pub struct ErrorAttrsView {
    pub code: String,
    pub message: String,
}

impl ErrorAttrsView {
    /// Decode from an `Attrs` bag (AV-2, AV-3). Requires
    /// `ERROR_CODE`; `ERROR_MESSAGE` defaults to empty.
    pub fn from_attrs(attrs: &Attrs) -> Result<Self, AttrsViewError> {
        use hyperactor::introspect::ERROR_CODE;
        use hyperactor::introspect::ERROR_MESSAGE;

        let code = attrs
            .get(ERROR_CODE)
            .ok_or_else(|| AttrsViewError::missing("error_code"))?
            .clone();
        let message = attrs.get(ERROR_MESSAGE).cloned().unwrap_or_default();
        Ok(Self { code, message })
    }

    /// Encode into an `Attrs` bag (AV-1 round-trip producer).
    pub fn to_attrs(&self) -> Attrs {
        use hyperactor::introspect::ERROR_CODE;
        use hyperactor::introspect::ERROR_MESSAGE;

        let mut attrs = Attrs::new();
        attrs.set(ERROR_CODE, self.code.clone());
        attrs.set(ERROR_MESSAGE, self.message.clone());
        attrs
    }
}

// --- API / presentation types ---

use std::fmt;
use std::str::FromStr;
use std::time::SystemTime;

use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

/// Typed reference to a node in the mesh-admin navigation tree.
///
/// Extends `IntrospectRef` with mesh-only concepts (`Root`, `Host`).
/// hyperactor does not know about these variants.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Named)]
pub enum NodeRef {
    /// Synthetic mesh root node.
    /// Serializes as lowercase `"root"` to match the HTTP path convention.
    #[serde(rename = "root")]
    Root,
    /// A host in the mesh, identified by its `HostAgent` actor ID.
    Host(hyperactor::reference::ActorId),
    /// A proc running on a host.
    Proc(hyperactor::reference::ProcId),
    /// An actor instance within a proc.
    Actor(hyperactor::reference::ActorId),
}

hyperactor_config::impl_attrvalue!(NodeRef);

impl fmt::Display for NodeRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Root => write!(f, "root"),
            Self::Host(id) => write!(f, "host:{}", id),
            Self::Proc(id) => fmt::Display::fmt(id, f),
            Self::Actor(id) => fmt::Display::fmt(id, f),
        }
    }
}

/// Error parsing a `NodeRef` from a string.
#[derive(Debug, thiserror::Error)]
pub enum NodeRefParseError {
    #[error("empty reference string")]
    Empty,
    #[error("invalid host reference: {0}")]
    InvalidHost(hyperactor::reference::ReferenceParsingError),
    #[error("port references are not valid node references")]
    PortNotAllowed,
    #[error(transparent)]
    Reference(#[from] hyperactor::reference::ReferenceParsingError),
}

impl FromStr for NodeRef {
    type Err = NodeRefParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(NodeRefParseError::Empty);
        }
        if s == "root" {
            return Ok(Self::Root);
        }
        if let Some(rest) = s.strip_prefix("host:") {
            let actor_id: hyperactor::reference::ActorId =
                rest.parse().map_err(NodeRefParseError::InvalidHost)?;
            return Ok(Self::Host(actor_id));
        }
        let r: hyperactor::reference::Reference = s.parse()?;
        match r {
            hyperactor::reference::Reference::Proc(id) => Ok(Self::Proc(id)),
            hyperactor::reference::Reference::Actor(id) => Ok(Self::Actor(id)),
            hyperactor::reference::Reference::Port(_) => Err(NodeRefParseError::PortNotAllowed),
        }
    }
}

impl From<hyperactor::introspect::IntrospectRef> for NodeRef {
    fn from(r: hyperactor::introspect::IntrospectRef) -> Self {
        match r {
            hyperactor::introspect::IntrospectRef::Proc(id) => Self::Proc(id),
            hyperactor::introspect::IntrospectRef::Actor(id) => Self::Actor(id),
        }
    }
}

/// Uniform response for any node in the mesh topology.
///
/// Every addressable entity (root, host, proc, actor) is represented
/// as a `NodePayload`. The client navigates the mesh by fetching a
/// node and following its `children` references.
///
/// See IA-1..IA-5 in module doc.
// Serialize/Deserialize required by wirevalue::register_type! and
// ResolveReferenceResponse actor messaging. HTTP serialization uses
// dto::NodePayloadDto, not these derives.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named)]
pub struct NodePayload {
    /// Canonical node reference identifying this node.
    pub identity: NodeRef,
    /// Node-specific metadata (type, status, metrics, etc.).
    pub properties: NodeProperties,
    /// Child node references for downward navigation.
    pub children: Vec<NodeRef>,
    /// Parent node reference for upward navigation.
    pub parent: Option<NodeRef>,
    /// When this payload was captured.
    pub as_of: SystemTime,
}
wirevalue::register_type!(NodePayload);

/// Node-specific metadata. Externally-tagged enum — the variant
/// name is the discriminator (Root, Host, Proc, Actor, Error).
// Serialize/Deserialize required by wirevalue::register_type! and
// ResolveReferenceResponse actor messaging. HTTP serialization uses
// dto::NodePropertiesDto, not these derives.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named)]
pub enum NodeProperties {
    /// Synthetic mesh root node (not a real actor/proc).
    Root {
        num_hosts: usize,
        started_at: SystemTime,
        started_by: String,
        system_children: Vec<NodeRef>,
    },
    /// A host in the mesh, represented by its `HostAgent`.
    Host {
        addr: String,
        num_procs: usize,
        system_children: Vec<NodeRef>,
    },
    /// Properties describing a proc running on a host.
    Proc {
        proc_name: String,
        num_actors: usize,
        system_children: Vec<NodeRef>,
        stopped_children: Vec<NodeRef>,
        stopped_retention_cap: usize,
        is_poisoned: bool,
        failed_actor_count: usize,
    },
    /// Runtime metadata for a single actor instance.
    Actor {
        actor_status: String,
        actor_type: String,
        messages_processed: u64,
        created_at: Option<SystemTime>,
        last_message_handler: Option<String>,
        total_processing_time_us: u64,
        flight_recorder: Option<String>,
        is_system: bool,
        failure_info: Option<FailureInfo>,
    },
    /// Error sentinel returned when a child reference cannot be resolved.
    Error { code: String, message: String },
}
wirevalue::register_type!(NodeProperties);

/// Structured failure information for failed actors.
// Serialize/Deserialize required by wirevalue::register_type! and
// ResolveReferenceResponse actor messaging. HTTP serialization uses
// dto::FailureInfoDto, not these derives.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named)]
pub struct FailureInfo {
    /// Error message describing the failure.
    pub error_message: String,
    /// Actor that caused the failure (root cause).
    pub root_cause_actor: hyperactor::reference::ActorId,
    /// Display name of the root-cause actor, if available.
    pub root_cause_name: Option<String>,
    /// When the failure occurred.
    pub occurred_at: SystemTime,
    /// Whether this failure was propagated from a child.
    pub is_propagated: bool,
}
wirevalue::register_type!(FailureInfo);

/// Mesh-layer conversion from a typed attrs view to `NodeProperties`.
///
/// Defined here so that `hyperactor` views (e.g. `ActorAttrsView`) can
/// produce `NodeProperties` without depending on the mesh crate.
trait IntoNodeProperties {
    fn into_node_properties(self) -> NodeProperties;
}

impl IntoNodeProperties for RootAttrsView {
    fn into_node_properties(self) -> NodeProperties {
        NodeProperties::Root {
            num_hosts: self.num_hosts,
            started_at: self.started_at,
            started_by: self.started_by,
            system_children: self.system_children,
        }
    }
}

impl IntoNodeProperties for HostAttrsView {
    fn into_node_properties(self) -> NodeProperties {
        NodeProperties::Host {
            addr: self.addr,
            num_procs: self.num_procs,
            system_children: self.system_children,
        }
    }
}

impl IntoNodeProperties for ProcAttrsView {
    fn into_node_properties(self) -> NodeProperties {
        NodeProperties::Proc {
            proc_name: self.proc_name,
            num_actors: self.num_actors,
            system_children: self.system_children,
            stopped_children: self.stopped_children,
            stopped_retention_cap: self.stopped_retention_cap,
            is_poisoned: self.is_poisoned,
            failed_actor_count: self.failed_actor_count,
        }
    }
}

impl IntoNodeProperties for ErrorAttrsView {
    fn into_node_properties(self) -> NodeProperties {
        NodeProperties::Error {
            code: self.code,
            message: self.message,
        }
    }
}

impl IntoNodeProperties for hyperactor::introspect::ActorAttrsView {
    fn into_node_properties(self) -> NodeProperties {
        let actor_status = match &self.status_reason {
            Some(reason) => format!("{}: {}", self.status, reason),
            None => self.status.clone(),
        };

        let failure_info = self.failure.map(|fi| FailureInfo {
            error_message: fi.error_message,
            root_cause_actor: fi.root_cause_actor,
            root_cause_name: fi.root_cause_name,
            occurred_at: fi.occurred_at,
            is_propagated: fi.is_propagated,
        });

        NodeProperties::Actor {
            actor_status,
            actor_type: self.actor_type,
            messages_processed: self.messages_processed,
            created_at: self.created_at,
            last_message_handler: self.last_handler,
            total_processing_time_us: self.total_processing_time_us,
            flight_recorder: self.flight_recorder,
            is_system: self.is_system,
            failure_info,
        }
    }
}

/// Derive `NodeProperties` from a JSON-serialized attrs string.
///
/// Detection precedence (DP-1, DP-3):
/// 1. `node_type` = "root" / "host" / "proc" → corresponding variant
/// 2. `error_code` present → Error
/// 3. `STATUS` key present → Actor
/// 4. none of the above → Error("unknown_node_type")
///
/// DP-2 / DP-4: this function is total — malformed attrs never
/// panic; view decode failures map to `NodeProperties::Error`
/// with a `malformed_*` code.
/// AV-3 / IA-6: view decoders ignore unknown keys.
pub fn derive_properties(attrs_json: &str) -> NodeProperties {
    let attrs: Attrs = match serde_json::from_str(attrs_json) {
        Ok(a) => a,
        Err(_) => {
            return NodeProperties::Error {
                code: "parse_error".into(),
                message: "failed to parse attrs JSON".into(),
            };
        }
    };

    let node_type = attrs.get(NODE_TYPE).cloned().unwrap_or_default();

    match node_type.as_str() {
        "root" => match RootAttrsView::from_attrs(&attrs) {
            Ok(v) => v.into_node_properties(),
            Err(e) => NodeProperties::Error {
                code: "malformed_root".into(),
                message: e.to_string(),
            },
        },
        "host" => match HostAttrsView::from_attrs(&attrs) {
            Ok(v) => v.into_node_properties(),
            Err(e) => NodeProperties::Error {
                code: "malformed_host".into(),
                message: e.to_string(),
            },
        },
        "proc" => match ProcAttrsView::from_attrs(&attrs) {
            Ok(v) => v.into_node_properties(),
            Err(e) => NodeProperties::Error {
                code: "malformed_proc".into(),
                message: e.to_string(),
            },
        },
        _ => {
            // DP-1: error_code → Error, STATUS present → Actor,
            // else → Error("unknown_node_type").
            use hyperactor::introspect::ERROR_CODE;
            use hyperactor::introspect::STATUS;

            if attrs.get(ERROR_CODE).is_some() {
                return match ErrorAttrsView::from_attrs(&attrs) {
                    Ok(v) => v.into_node_properties(),
                    Err(e) => NodeProperties::Error {
                        code: "malformed_error".into(),
                        message: e.to_string(),
                    },
                };
            }

            if attrs.get(STATUS).is_none() {
                return NodeProperties::Error {
                    code: "unknown_node_type".into(),
                    message: format!("unrecognized node_type: {:?}", node_type),
                };
            }

            match hyperactor::introspect::ActorAttrsView::from_attrs(&attrs) {
                Ok(v) => v.into_node_properties(),
                Err(e) => NodeProperties::Error {
                    code: "malformed_actor".into(),
                    message: e.to_string(),
                },
            }
        }
    }
}

/// Convert an `IntrospectResult` to a presentation `NodePayload`.
/// Lifts `IntrospectRef` → `NodeRef` and passes through typed timestamps.
pub fn to_node_payload(result: hyperactor::introspect::IntrospectResult) -> NodePayload {
    NodePayload {
        identity: result.identity.into(),
        properties: derive_properties(&result.attrs),
        children: result.children.into_iter().map(NodeRef::from).collect(),
        parent: result.parent.map(NodeRef::from),
        as_of: result.as_of,
    }
}

/// Convert an `IntrospectResult` to a `NodePayload`, overriding
/// identity and parent for correct tree navigation.
pub fn to_node_payload_with(
    result: hyperactor::introspect::IntrospectResult,
    identity: NodeRef,
    parent: Option<NodeRef>,
) -> NodePayload {
    NodePayload {
        identity,
        properties: derive_properties(&result.attrs),
        children: result.children.into_iter().map(NodeRef::from).collect(),
        parent,
        as_of: result.as_of,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Enforces MK-1 (metadata completeness) for all mesh-topology
    /// introspection keys.
    #[test]
    fn test_mesh_introspect_keys_are_tagged() {
        let cases = vec![
            ("node_type", NODE_TYPE.attrs()),
            ("addr", ADDR.attrs()),
            ("num_procs", NUM_PROCS.attrs()),
            ("proc_name", PROC_NAME.attrs()),
            ("num_actors", NUM_ACTORS.attrs()),
            ("system_children", SYSTEM_CHILDREN.attrs()),
            ("stopped_children", STOPPED_CHILDREN.attrs()),
            ("stopped_retention_cap", STOPPED_RETENTION_CAP.attrs()),
            ("is_poisoned", IS_POISONED.attrs()),
            ("failed_actor_count", FAILED_ACTOR_COUNT.attrs()),
            ("started_at", STARTED_AT.attrs()),
            ("started_by", STARTED_BY.attrs()),
            ("num_hosts", NUM_HOSTS.attrs()),
        ];

        for (expected_name, meta) in &cases {
            // MK-1: every key must have INTROSPECT with non-empty
            // name and desc.
            let introspect = meta
                .get(INTROSPECT)
                .unwrap_or_else(|| panic!("{expected_name}: missing INTROSPECT meta-attr"));
            assert_eq!(
                introspect.name, *expected_name,
                "short name mismatch for {expected_name}"
            );
            assert!(
                !introspect.desc.is_empty(),
                "{expected_name}: desc should not be empty"
            );
        }

        // Exhaustiveness: verify cases covers all INTROSPECT-tagged
        // keys declared in this module.
        use hyperactor_config::attrs::AttrKeyInfo;
        let registry_count = inventory::iter::<AttrKeyInfo>()
            .filter(|info| {
                info.name.starts_with("hyperactor_mesh::introspect::")
                    && info.meta.get(INTROSPECT).is_some()
            })
            .count();
        assert_eq!(
            cases.len(),
            registry_count,
            "test must cover all INTROSPECT-tagged keys in this module"
        );
    }

    fn test_actor_ref(proc_name: &str, actor_name: &str, pid: usize) -> NodeRef {
        use hyperactor::channel::ChannelAddr;
        use hyperactor::reference::ProcId;
        NodeRef::Actor(
            ProcId::with_name(ChannelAddr::Local(0), proc_name).actor_id(actor_name, pid),
        )
    }

    fn root_view() -> RootAttrsView {
        RootAttrsView {
            num_hosts: 3,
            started_at: std::time::UNIX_EPOCH,
            started_by: "testuser".into(),
            system_children: vec![test_actor_ref("proc", "child1", 0)],
        }
    }

    fn host_view() -> HostAttrsView {
        HostAttrsView {
            addr: "10.0.0.1:8080".into(),
            num_procs: 2,
            system_children: vec![test_actor_ref("proc", "sys", 0)],
        }
    }

    fn proc_view() -> ProcAttrsView {
        ProcAttrsView {
            proc_name: "worker".into(),
            num_actors: 5,
            system_children: vec![],
            stopped_children: vec![test_actor_ref("proc", "old", 0)],
            stopped_retention_cap: 10,
            is_poisoned: false,
            failed_actor_count: 0,
        }
    }

    fn error_view() -> ErrorAttrsView {
        ErrorAttrsView {
            code: "not_found".into(),
            message: "child not found".into(),
        }
    }

    /// AV-1: from_attrs(to_attrs(v)) == v.
    #[test]
    fn test_root_view_round_trip() {
        let view = root_view();
        let rt = RootAttrsView::from_attrs(&view.to_attrs()).unwrap();
        assert_eq!(rt, view);
    }

    /// AV-1.
    #[test]
    fn test_host_view_round_trip() {
        let view = host_view();
        let rt = HostAttrsView::from_attrs(&view.to_attrs()).unwrap();
        assert_eq!(rt, view);
    }

    /// AV-1.
    #[test]
    fn test_proc_view_round_trip() {
        let view = proc_view();
        let rt = ProcAttrsView::from_attrs(&view.to_attrs()).unwrap();
        assert_eq!(rt, view);
    }

    /// AV-1.
    #[test]
    fn test_error_view_round_trip() {
        let view = error_view();
        let rt = ErrorAttrsView::from_attrs(&view.to_attrs()).unwrap();
        assert_eq!(rt, view);
    }

    /// AV-2: missing required key rejected.
    #[test]
    fn test_root_view_missing_started_at() {
        let mut attrs = Attrs::new();
        attrs.set(NODE_TYPE, "root".into());
        attrs.set(STARTED_BY, "user".into());
        let err = RootAttrsView::from_attrs(&attrs).unwrap_err();
        assert_eq!(err, AttrsViewError::MissingKey { key: "started_at" });
    }

    /// AV-2.
    #[test]
    fn test_root_view_missing_started_by() {
        let mut attrs = Attrs::new();
        attrs.set(NODE_TYPE, "root".into());
        attrs.set(STARTED_AT, std::time::UNIX_EPOCH);
        let err = RootAttrsView::from_attrs(&attrs).unwrap_err();
        assert_eq!(err, AttrsViewError::MissingKey { key: "started_by" });
    }

    /// AV-2.
    #[test]
    fn test_host_view_missing_addr() {
        let attrs = Attrs::new();
        let err = HostAttrsView::from_attrs(&attrs).unwrap_err();
        assert_eq!(err, AttrsViewError::MissingKey { key: "addr" });
    }

    /// AV-2.
    #[test]
    fn test_proc_view_missing_proc_name() {
        let attrs = Attrs::new();
        let err = ProcAttrsView::from_attrs(&attrs).unwrap_err();
        assert_eq!(err, AttrsViewError::MissingKey { key: "proc_name" });
    }

    /// FI-5: poisoned without failures rejected.
    #[test]
    fn test_proc_view_fi5_poisoned_but_no_failures() {
        let mut attrs = Attrs::new();
        attrs.set(PROC_NAME, "bad".into());
        attrs.set(IS_POISONED, true);
        attrs.set(FAILED_ACTOR_COUNT, 0usize);
        let err = ProcAttrsView::from_attrs(&attrs).unwrap_err();
        assert!(matches!(
            err,
            AttrsViewError::InvariantViolation { label: "FI-5", .. }
        ));
    }

    /// FI-5: failures without poisoned rejected.
    #[test]
    fn test_proc_view_fi5_failures_but_not_poisoned() {
        let mut attrs = Attrs::new();
        attrs.set(PROC_NAME, "bad".into());
        attrs.set(IS_POISONED, false);
        attrs.set(FAILED_ACTOR_COUNT, 2usize);
        let err = ProcAttrsView::from_attrs(&attrs).unwrap_err();
        assert!(matches!(
            err,
            AttrsViewError::InvariantViolation { label: "FI-5", .. }
        ));
    }

    /// DP-2 / DP-4: unparseable JSON → Error.
    #[test]
    fn test_derive_properties_unparseable_json() {
        let props = derive_properties("not json");
        assert!(matches!(props, NodeProperties::Error { code, .. } if code == "parse_error"));
    }

    /// DP-3: unknown node_type → Error.
    #[test]
    fn test_derive_properties_unknown_node_type() {
        let attrs = Attrs::new();
        let json = serde_json::to_string(&attrs).unwrap();
        let props = derive_properties(&json);
        assert!(matches!(props, NodeProperties::Error { code, .. } if code == "unknown_node_type"));
    }

    /// DP-4: view decode failure → malformed_* Error.
    #[test]
    fn test_derive_properties_malformed_root() {
        let mut attrs = Attrs::new();
        attrs.set(NODE_TYPE, "root".into());
        let json = serde_json::to_string(&attrs).unwrap();
        let props = derive_properties(&json);
        assert!(matches!(props, NodeProperties::Error { code, .. } if code == "malformed_root"));
    }

    /// DP-4: invariant violation → malformed_* Error.
    #[test]
    fn test_derive_properties_malformed_proc_fi5() {
        let mut attrs = Attrs::new();
        attrs.set(NODE_TYPE, "proc".into());
        attrs.set(PROC_NAME, "bad".into());
        attrs.set(IS_POISONED, true);
        attrs.set(FAILED_ACTOR_COUNT, 0usize);
        let json = serde_json::to_string(&attrs).unwrap();
        let props = derive_properties(&json);
        assert!(matches!(props, NodeProperties::Error { code, .. } if code == "malformed_proc"));
    }

    /// DP-3: node_type "root" → Root variant.
    #[test]
    fn test_derive_properties_valid_root() {
        let view = root_view();
        let json = serde_json::to_string(&view.to_attrs()).unwrap();
        let props = derive_properties(&json);
        assert!(matches!(props, NodeProperties::Root { num_hosts: 3, .. }));
    }

    /// DP-3: node_type "host" → Host variant.
    #[test]
    fn test_derive_properties_valid_host() {
        let view = host_view();
        let json = serde_json::to_string(&view.to_attrs()).unwrap();
        let props = derive_properties(&json);
        assert!(matches!(props, NodeProperties::Host { num_procs: 2, .. }));
    }

    /// DP-3: node_type "proc" → Proc variant.
    #[test]
    fn test_derive_properties_valid_proc() {
        let view = proc_view();
        let json = serde_json::to_string(&view.to_attrs()).unwrap();
        let props = derive_properties(&json);
        assert!(matches!(props, NodeProperties::Proc { num_actors: 5, .. }));
    }

    /// DP-3: error_code present → Error variant.
    #[test]
    fn test_derive_properties_valid_error() {
        let view = error_view();
        let json = serde_json::to_string(&view.to_attrs()).unwrap();
        let props = derive_properties(&json);
        assert!(matches!(props, NodeProperties::Error { .. }));
        if let NodeProperties::Error { code, message } = props {
            assert_eq!(code, "not_found");
            assert_eq!(message, "child not found");
        }
    }

    /// DP-3: STATUS present → Actor variant.
    #[test]
    fn test_derive_properties_valid_actor() {
        use hyperactor::introspect::ACTOR_TYPE;
        use hyperactor::introspect::MESSAGES_PROCESSED;
        use hyperactor::introspect::STATUS;

        let mut attrs = Attrs::new();
        attrs.set(STATUS, "running".into());
        attrs.set(ACTOR_TYPE, "TestActor".into());
        attrs.set(MESSAGES_PROCESSED, 7u64);
        let json = serde_json::to_string(&attrs).unwrap();
        let props = derive_properties(&json);
        assert!(matches!(
            props,
            NodeProperties::Actor {
                messages_processed: 7,
                ..
            }
        ));
    }

    /// Injects an unknown key into serialized attrs JSON and
    /// verifies that derive_properties still decodes successfully.
    /// Exercises IA-6 (open-row-forward-compat) for each view.
    fn inject_unknown_key(attrs: &Attrs) -> String {
        let mut map: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(&serde_json::to_string(attrs).unwrap()).unwrap();
        map.insert(
            "unknown_future_key".into(),
            serde_json::Value::String("surprise".into()),
        );
        serde_json::to_string(&map).unwrap()
    }

    #[test]
    fn test_ia6_root_ignores_unknown_keys() {
        let json = inject_unknown_key(&root_view().to_attrs());
        let props = derive_properties(&json);
        assert!(matches!(props, NodeProperties::Root { num_hosts: 3, .. }));
    }

    #[test]
    fn test_ia6_host_ignores_unknown_keys() {
        let json = inject_unknown_key(&host_view().to_attrs());
        let props = derive_properties(&json);
        assert!(matches!(props, NodeProperties::Host { num_procs: 2, .. }));
    }

    #[test]
    fn test_ia6_proc_ignores_unknown_keys() {
        let json = inject_unknown_key(&proc_view().to_attrs());
        let props = derive_properties(&json);
        assert!(matches!(props, NodeProperties::Proc { num_actors: 5, .. }));
    }

    #[test]
    fn test_ia6_error_ignores_unknown_keys() {
        let json = inject_unknown_key(&error_view().to_attrs());
        let props = derive_properties(&json);
        assert!(matches!(props, NodeProperties::Error { .. }));
    }

    #[test]
    fn test_ia6_actor_ignores_unknown_keys() {
        use hyperactor::introspect::ACTOR_TYPE;
        use hyperactor::introspect::STATUS;

        let mut attrs = Attrs::new();
        attrs.set(STATUS, "running".into());
        attrs.set(ACTOR_TYPE, "TestActor".into());
        let json = inject_unknown_key(&attrs);
        let props = derive_properties(&json);
        assert!(matches!(props, NodeProperties::Actor { .. }));
    }

    /// SC-1 / SC-2: schema is derived from types and matches the
    /// checked-in snapshot.
    ///
    /// To update after intentional type changes:
    /// ```sh
    /// buck run fbcode//monarch/hyperactor_mesh:generate_api_artifacts \
    ///   @fbcode//mode/dev-nosan -- \
    ///   fbcode/monarch/hyperactor_mesh/src/testdata
    /// ```
    /// Strip the `$comment` field (containing the @\u{200B}generated marker)
    /// from a JSON value so snapshot comparisons ignore it.
    fn strip_comment(mut value: serde_json::Value) -> serde_json::Value {
        if let Some(obj) = value.as_object_mut() {
            obj.remove("$comment");
        }
        value
    }

    #[test]
    fn test_node_payload_schema_snapshot() {
        let schema = schemars::schema_for!(dto::NodePayloadDto);
        let actual: serde_json::Value = serde_json::to_value(&schema).unwrap();
        let expected: serde_json::Value = strip_comment(
            serde_json::from_str(include_str!("testdata/node_payload_schema.json"))
                .expect("snapshot must be valid JSON"),
        );
        assert_eq!(
            actual, expected,
            "schema changed — review and update snapshot if intentional"
        );
    }

    /// SC-3: real payloads validate against the generated schema.
    #[test]
    fn test_payloads_validate_against_schema() {
        use hyperactor::channel::ChannelAddr;
        use hyperactor::reference::ProcId;

        let schema = schemars::schema_for!(dto::NodePayloadDto);
        let schema_value = serde_json::to_value(&schema).unwrap();
        let compiled = jsonschema::JSONSchema::compile(&schema_value).expect("schema must compile");

        let epoch = std::time::UNIX_EPOCH;
        let proc_id = ProcId::with_name(ChannelAddr::Local(0), "worker");
        let actor_id = proc_id.actor_id("actor", 0);

        let samples = [
            NodePayload {
                identity: NodeRef::Root,
                properties: NodeProperties::Root {
                    num_hosts: 2,
                    started_at: epoch,
                    started_by: "testuser".into(),
                    system_children: vec![],
                },
                children: vec![NodeRef::Host(actor_id.clone())],
                parent: None,
                as_of: epoch,
            },
            NodePayload {
                identity: NodeRef::Host(actor_id.clone()),
                properties: NodeProperties::Host {
                    addr: "10.0.0.1:8080".into(),
                    num_procs: 2,
                    system_children: vec![test_actor_ref("proc", "sys", 0)],
                },
                children: vec![NodeRef::Proc(proc_id.clone())],
                parent: Some(NodeRef::Root),
                as_of: epoch,
            },
            NodePayload {
                identity: NodeRef::Proc(proc_id.clone()),
                properties: NodeProperties::Proc {
                    proc_name: "worker".into(),
                    num_actors: 5,
                    system_children: vec![],
                    stopped_children: vec![],
                    stopped_retention_cap: 10,
                    is_poisoned: false,
                    failed_actor_count: 0,
                },
                children: vec![NodeRef::Actor(actor_id.clone())],
                parent: Some(NodeRef::Host(actor_id.clone())),
                as_of: epoch,
            },
            NodePayload {
                identity: NodeRef::Actor(actor_id.clone()),
                properties: NodeProperties::Actor {
                    actor_status: "running".into(),
                    actor_type: "MyActor".into(),
                    messages_processed: 42,
                    created_at: Some(epoch),
                    last_message_handler: Some("handle_ping".into()),
                    total_processing_time_us: 1000,
                    flight_recorder: None,
                    is_system: false,
                    failure_info: None,
                },
                children: vec![],
                parent: Some(NodeRef::Proc(proc_id.clone())),
                as_of: epoch,
            },
            NodePayload {
                identity: NodeRef::Actor(actor_id.clone()),
                properties: NodeProperties::Error {
                    code: "not_found".into(),
                    message: "child not found".into(),
                },
                children: vec![],
                parent: None,
                as_of: epoch,
            },
        ];

        for (i, payload) in samples.iter().enumerate() {
            let dto = dto::NodePayloadDto::from(payload.clone());
            let value = serde_json::to_value(&dto).unwrap();
            assert!(
                compiled.is_valid(&value),
                "sample {i} failed schema validation"
            );
        }
    }

    /// SC-4: `$id` is injected only at the serve boundary.
    /// Stripping `$id` from the served schema must yield the raw
    /// schemars output.
    #[test]
    fn test_served_schema_is_raw_plus_id() {
        let raw: serde_json::Value =
            serde_json::to_value(schemars::schema_for!(dto::NodePayloadDto)).unwrap();

        // Simulate what the endpoint does.
        let mut served = raw.clone();
        served.as_object_mut().unwrap().insert(
            "$id".into(),
            serde_json::Value::String("https://monarch.meta.com/schemas/v1/node_payload".into()),
        );

        // Strip $id — remainder must equal raw.
        let mut stripped = served;
        stripped.as_object_mut().unwrap().remove("$id");
        assert_eq!(raw, stripped, "served schema differs from raw beyond $id");
    }

    /// SC-2: error envelope schema matches checked-in snapshot.
    #[test]
    fn test_error_schema_snapshot() {
        use crate::mesh_admin::ApiErrorEnvelope;

        let schema = schemars::schema_for!(ApiErrorEnvelope);
        let actual: serde_json::Value = serde_json::to_value(&schema).unwrap();
        let expected: serde_json::Value = strip_comment(
            serde_json::from_str(include_str!("testdata/error_schema.json"))
                .expect("error snapshot must be valid JSON"),
        );
        assert_eq!(
            actual, expected,
            "error schema changed — review and update snapshot if intentional"
        );
    }

    /// SC-2: AdminInfo schema matches checked-in snapshot.
    #[test]
    fn test_admin_info_schema_snapshot() {
        use crate::mesh_admin::AdminInfo;

        let schema = schemars::schema_for!(AdminInfo);
        let actual: serde_json::Value = serde_json::to_value(&schema).unwrap();
        let expected: serde_json::Value = strip_comment(
            serde_json::from_str(include_str!("testdata/admin_info_schema.json"))
                .expect("admin info snapshot must be valid JSON"),
        );
        assert_eq!(
            actual, expected,
            "AdminInfo schema changed — review and update snapshot if intentional"
        );
    }

    /// SC-2: OpenAPI spec matches checked-in snapshot.
    #[test]
    fn test_openapi_spec_snapshot() {
        let actual = crate::mesh_admin::build_openapi_spec();
        let expected: serde_json::Value = strip_comment(
            serde_json::from_str(include_str!("testdata/openapi.json"))
                .expect("OpenAPI snapshot must be valid JSON"),
        );
        assert_eq!(
            actual, expected,
            "OpenAPI spec changed — review and update snapshot if intentional"
        );
    }
}
