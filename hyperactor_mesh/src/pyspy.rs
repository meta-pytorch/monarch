/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! py-spy integration for remote Python stack dumps and profiles.
//!
//! See PS-* and PP-* invariants in `introspect` module doc.

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::RefClient;
use hyperactor::reference as hyperactor_reference;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::config::MESH_ADMIN_PYSPY_TIMEOUT;
use crate::config::PYSPY_BIN;

/// Result of a py-spy stack dump request.
///
/// See PS-2, PS-4 in `introspect` module doc.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    Named,
    schemars::JsonSchema
)]
pub enum PySpyResult {
    /// Successful stack dump with structured traces.
    Ok {
        /// OS process ID that was dumped.
        pid: u32,
        /// Path or name of the py-spy binary that produced the dump.
        binary: String,
        /// Per-thread stack traces from py-spy.
        stack_traces: Vec<PySpyStackTrace>,
        /// Non-fatal warnings from the capture (e.g., flag
        /// fallbacks). Empty when the capture completed without
        /// caveats.
        warnings: Vec<String>,
    },
    /// py-spy binary not found in environment.
    BinaryNotFound {
        /// Candidate paths that were tried before giving up.
        searched: Vec<String>,
    },
    /// py-spy exited with an error.
    Failed {
        /// OS process ID that was targeted.
        pid: u32,
        /// Path or name of the py-spy binary that failed.
        binary: String,
        /// Exit code from the py-spy process, if available.
        exit_code: Option<i32>,
        /// Captured stderr output.
        stderr: String,
    },
}
wirevalue::register_type!(PySpyResult);

/// A single thread's stack trace from py-spy `--json` output.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct PySpyStackTrace {
    /// OS process ID that owns this thread.
    pub pid: i32,
    /// Python-level thread identifier (`threading.get_ident()`).
    pub thread_id: u64,
    /// Python thread name, if set.
    pub thread_name: Option<String>,
    /// OS-level thread ID (e.g., `gettid()` on Linux).
    pub os_thread_id: Option<u64>,
    /// Whether the thread is actively running (not idle/waiting).
    pub active: bool,
    /// Whether the thread currently holds the GIL.
    pub owns_gil: bool,
    /// Stack frames, innermost first.
    pub frames: Vec<PySpyFrame>,
}

/// A single frame in a py-spy stack trace.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct PySpyFrame {
    /// Function or method name.
    pub name: String,
    /// Absolute path to the source file.
    pub filename: String,
    /// Python module name, if known.
    pub module: Option<String>,
    /// Basename or abbreviated path.
    pub short_filename: Option<String>,
    /// Source line number.
    pub line: i32,
    /// Local variables captured in this frame, if available.
    pub locals: Option<Vec<PySpyLocalVariable>>,
    /// Whether this frame is an entry point (e.g., module `__main__`).
    pub is_entry: bool,
}

/// A local variable captured in a py-spy frame.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct PySpyLocalVariable {
    /// Variable name.
    pub name: String,
    /// Memory address of the Python object.
    pub addr: usize,
    /// Whether this variable is a function argument.
    pub arg: bool,
    /// `repr()` of the value, if captured.
    pub repr: Option<String>,
}

/// Options controlling py-spy capture behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PySpyOpts {
    /// Include per-thread stacks (`--threads`).
    pub threads: bool,
    /// Include native C/C++ frames (`--native`).
    pub native: bool,
    /// Include native frames for all threads, not just those with
    /// Python frames (`--native-all`).
    pub native_all: bool,
    /// Use nonblocking mode — py-spy reads without pausing the
    /// target process (`--nonblocking`). Enables retry logic (PS-10).
    pub nonblocking: bool,
}

/// Public JSON-facing options for a py-spy profile capture.
///
/// Deserialized from the HTTP POST body. Validated and converted to
/// `ValidatedProfileRequest` before any actor messaging.
///
/// See PP-1 in `introspect` module doc.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct PySpyProfileOpts {
    /// Sampling duration in whole seconds. py-spy `--duration`
    /// accepts integers only. Must be >= 1; upper bound enforced
    /// at runtime by `MESH_ADMIN_PYSPY_MAX_PROFILE_DURATION`.
    #[schemars(range(min = 1))]
    pub duration_s: u32,
    /// Sampling rate in Hz. Must be 1..=1000.
    #[schemars(range(min = 1, max = 1000))]
    pub rate_hz: u32,
    /// Include native C/C++ frames.
    pub native: bool,
    /// Include per-thread stacks.
    pub threads: bool,
    /// Use nonblocking mode.
    pub nonblocking: bool,
}

/// Validated profile duration. Guaranteed non-zero.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) struct ProfileDurationSecs(std::num::NonZeroU32);

impl ProfileDurationSecs {
    pub fn get(self) -> u32 {
        self.0.get()
    }
}

/// Validated sample rate. Guaranteed 1..=1000.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) struct SampleRateHz(std::num::NonZeroU32);

impl SampleRateHz {
    pub fn get(self) -> u32 {
        self.0.get()
    }
}

/// Validated profile request. If this exists, it is valid.
/// Construct only via `try_new`. See PP-1, PP-2.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ValidatedProfileRequest {
    /// Sampling duration (guaranteed non-zero, within max).
    duration: ProfileDurationSecs,
    /// Sampling rate (guaranteed 1..=1000).
    rate: SampleRateHz,
    /// Include native C/C++ frames.
    native: bool,
    /// Include per-thread stacks.
    threads: bool,
    /// Use nonblocking mode.
    nonblocking: bool,
    /// Kill deadline for the py-spy subprocess.
    subprocess_timeout: std::time::Duration,
    /// Bridge reply wait deadline (subprocess + margin).
    bridge_timeout: std::time::Duration,
}

impl ValidatedProfileRequest {
    pub fn duration(&self) -> ProfileDurationSecs {
        self.duration
    }
    pub fn rate(&self) -> SampleRateHz {
        self.rate
    }
    pub fn native(&self) -> bool {
        self.native
    }
    pub fn threads(&self) -> bool {
        self.threads
    }
    pub fn nonblocking(&self) -> bool {
        self.nonblocking
    }
    pub fn subprocess_timeout(&self) -> std::time::Duration {
        self.subprocess_timeout
    }
    pub fn bridge_timeout(&self) -> std::time::Duration {
        self.bridge_timeout
    }

    pub fn try_new(
        opts: &PySpyProfileOpts,
        max_duration: std::time::Duration,
    ) -> Result<Self, String> {
        let duration = std::num::NonZeroU32::new(opts.duration_s)
            .map(ProfileDurationSecs)
            .ok_or_else(|| "duration_s must be positive".to_string())?;
        if std::time::Duration::from_secs(u64::from(duration.get())) > max_duration {
            return Err(format!(
                "duration_s {}s exceeds max {}s",
                duration.get(),
                max_duration.as_secs()
            ));
        }
        let rate = std::num::NonZeroU32::new(opts.rate_hz)
            .filter(|n| n.get() <= 1000)
            .map(SampleRateHz)
            .ok_or_else(|| format!("rate_hz must be 1..=1000, got {}", opts.rate_hz))?;
        let subprocess_timeout = std::time::Duration::from_secs(u64::from(duration.get()) + 15);
        let bridge_timeout = subprocess_timeout + std::time::Duration::from_secs(5);
        Ok(Self {
            duration,
            rate,
            native: opts.native,
            threads: opts.threads,
            nonblocking: opts.nonblocking,
            subprocess_timeout,
            bridge_timeout,
        })
    }
}

/// Wire result of a py-spy profile capture. The HTTP handler
/// unwraps this to produce `image/svg+xml` or `ApiError`.
/// Not a public JSON contract. See PP-2, PP-3.
#[derive(Debug, Clone, Serialize, Deserialize, Named)]
pub enum PySpyProfileResult {
    Ok {
        pid: u32,
        binary: String,
        svg: Vec<u8>,
    },
    BinaryNotFound {
        searched: Vec<String>,
    },
    TimedOut {
        pid: u32,
        binary: String,
        timeout_s: u64,
        stderr: String,
    },
    ExitFailure {
        pid: u32,
        binary: String,
        exit_code: Option<i32>,
        stderr: String,
    },
    OutputMissing {
        pid: u32,
        binary: String,
    },
    OutputEmpty {
        pid: u32,
        binary: String,
    },
    OutputReadFailure {
        pid: u32,
        binary: String,
        error: String,
    },
    WorkerSpawnFailure {
        error: String,
    },
    SubprocessSpawnFailure {
        pid: u32,
        binary: String,
        error: String,
    },
    WaitFailure {
        pid: u32,
        binary: String,
        error: String,
    },
    TempDirFailure {
        pid: u32,
        binary: String,
        error: String,
    },
}
wirevalue::register_type!(PySpyProfileResult);

/// Internal profile execution outcome. Converted to
/// `PySpyProfileResult` at the actor reply boundary.
#[derive(Debug)]
pub(crate) enum ProfileExecOutcome {
    Ok {
        pid: u32,
        binary: String,
        svg: Vec<u8>,
    },
    BinaryNotFound {
        searched: Vec<String>,
    },
    TimedOut {
        pid: u32,
        binary: String,
        timeout: std::time::Duration,
        stderr: String,
    },
    ExitFailure {
        pid: u32,
        binary: String,
        exit_code: Option<i32>,
        stderr: String,
    },
    OutputMissing {
        pid: u32,
        binary: String,
    },
    OutputEmpty {
        pid: u32,
        binary: String,
    },
    OutputReadFailure {
        pid: u32,
        binary: String,
        error: String,
    },
    WorkerSpawnFailure {
        error: String,
    },
    SubprocessSpawnFailure {
        pid: u32,
        binary: String,
        error: String,
    },
    WaitFailure {
        pid: u32,
        binary: String,
        error: String,
    },
    TempDirFailure {
        pid: u32,
        binary: String,
        error: String,
    },
}

impl From<ProfileExecOutcome> for PySpyProfileResult {
    fn from(outcome: ProfileExecOutcome) -> Self {
        match outcome {
            ProfileExecOutcome::Ok { pid, binary, svg } => {
                PySpyProfileResult::Ok { pid, binary, svg }
            }
            ProfileExecOutcome::BinaryNotFound { searched } => {
                PySpyProfileResult::BinaryNotFound { searched }
            }
            ProfileExecOutcome::TimedOut {
                pid,
                binary,
                timeout,
                stderr,
            } => PySpyProfileResult::TimedOut {
                pid,
                binary,
                timeout_s: timeout.as_secs(),
                stderr,
            },
            ProfileExecOutcome::ExitFailure {
                pid,
                binary,
                exit_code,
                stderr,
            } => PySpyProfileResult::ExitFailure {
                pid,
                binary,
                exit_code,
                stderr,
            },
            ProfileExecOutcome::OutputMissing { pid, binary } => {
                PySpyProfileResult::OutputMissing { pid, binary }
            }
            ProfileExecOutcome::OutputEmpty { pid, binary } => {
                PySpyProfileResult::OutputEmpty { pid, binary }
            }
            ProfileExecOutcome::OutputReadFailure { pid, binary, error } => {
                PySpyProfileResult::OutputReadFailure { pid, binary, error }
            }
            ProfileExecOutcome::WorkerSpawnFailure { error } => {
                PySpyProfileResult::WorkerSpawnFailure { error }
            }
            ProfileExecOutcome::SubprocessSpawnFailure { pid, binary, error } => {
                PySpyProfileResult::SubprocessSpawnFailure { pid, binary, error }
            }
            ProfileExecOutcome::WaitFailure { pid, binary, error } => {
                PySpyProfileResult::WaitFailure { pid, binary, error }
            }
            ProfileExecOutcome::TempDirFailure { pid, binary, error } => {
                PySpyProfileResult::TempDirFailure { pid, binary, error }
            }
        }
    }
}

/// Request a py-spy stack dump from this process.
///
/// Both ProcAgent and HostAgent handle this message. The handler
/// delegates to [`PySpyWorker::spawn_and_forward`] which runs py-spy
/// against `std::process::id()`.
///
/// See PS-1 in `introspect` module doc.
#[derive(Debug, Serialize, Deserialize, Named, Handler, HandleClient, RefClient)]
pub struct PySpyDump {
    /// Capture options (threads, native frames, nonblocking mode).
    pub opts: PySpyOpts,
    /// Reply port for the result.
    #[reply]
    pub result: hyperactor_reference::OncePortRef<PySpyResult>,
}
wirevalue::register_type!(PySpyDump);

/// Request a py-spy profile capture from this process.
///
/// Runs `py-spy record` for the requested duration. Separate contract
/// from `PySpyDump` — does not affect the existing dump pipeline.
///
/// See PP-4, PP-5 in `introspect` module doc.
#[allow(private_interfaces)] // pub required by hyperactor macros; actual use is crate-internal
#[derive(Debug, Serialize, Deserialize, Named, Handler, HandleClient, RefClient)]
pub struct PySpyProfile {
    /// Validated profile request (opts + derived timeouts).
    pub request: ValidatedProfileRequest,
    /// Reply port for the result.
    #[reply]
    pub result: hyperactor_reference::OncePortRef<PySpyProfileResult>,
}
wirevalue::register_type!(PySpyProfile);

/// Runs py-spy against the current process.
///
/// See PS-1, PS-3 in `introspect` module doc.
pub struct PySpyRunner;

impl PySpyRunner {
    /// Dump Python stacks for this process.
    ///
    /// Resolves the py-spy binary (PS-3), attaches to
    /// `std::process::id()` (PS-1), and returns structured JSON
    /// output (PS-4).
    ///
    /// PS-1 is structurally enforced: `PySpyDump` carries no PID
    /// field, and this method hardcodes `std::process::id()`. There
    /// is no code path that could substitute a different PID.
    pub async fn dump_self(&self, opts: &PySpyOpts) -> PySpyResult {
        let pid = std::process::id();
        let pyspy_bin: String = hyperactor_config::global::get_cloned(PYSPY_BIN);
        let candidates = resolve_candidates(if pyspy_bin.is_empty() {
            None
        } else {
            Some(pyspy_bin)
        });
        let mut searched = vec![];

        for (binary, label) in &candidates {
            searched.push(label.clone());
            if let Some(result) = try_exec(
                binary,
                pid,
                opts,
                hyperactor_config::global::get(MESH_ADMIN_PYSPY_TIMEOUT),
            )
            .await
            {
                return result;
            }
        }

        PySpyResult::BinaryNotFound { searched }
    }

    /// Profile Python stacks for this process over a duration.
    /// See PP-3, PP-4.
    pub(crate) async fn profile_self(
        &self,
        request: &ValidatedProfileRequest,
    ) -> ProfileExecOutcome {
        let pid = std::process::id();
        let pyspy_bin: String = hyperactor_config::global::get_cloned(PYSPY_BIN);
        let candidates = resolve_candidates(if pyspy_bin.is_empty() {
            None
        } else {
            Some(pyspy_bin)
        });
        let mut searched = vec![];

        for (binary, label) in &candidates {
            searched.push(label.clone());
            if let Some(result) = try_profile(binary, pid, request).await {
                return result;
            }
        }

        ProfileExecOutcome::BinaryNotFound { searched }
    }
}

/// Internal message from ProcAgent to a spawned PySpyWorker.
/// Carries the original caller's reply port so the worker
/// responds directly without routing back through ProcAgent.
#[derive(Debug, Serialize, Deserialize, Named)]
pub struct RunPySpyDump {
    pub opts: PySpyOpts,
    /// The original caller's reply port, forwarded from PySpyDump.
    pub reply_port: hyperactor::reference::OncePortRef<PySpyResult>,
}
wirevalue::register_type!(RunPySpyDump);

/// Short-lived child actor that runs py-spy off the ProcAgent
/// handler path. Spawned per-request; self-terminates after reply.
/// Concurrent instances are permitted — py-spy attaches read-only
/// via `process_vm_readv` and multiple concurrent dumps are safe.
#[hyperactor::export(handlers = [RunPySpyDump])]
pub struct PySpyWorker;

impl Actor for PySpyWorker {}

impl PySpyWorker {
    /// Spawn a PySpyWorker, forward the py-spy request, and let
    /// the worker reply directly to the caller. On spawn failure,
    /// sends a `Failed` result back via `reply_port`.
    pub(crate) fn spawn_and_forward(
        cx: &impl hyperactor::context::Actor,
        opts: PySpyOpts,
        reply_port: hyperactor::reference::OncePortRef<PySpyResult>,
    ) -> Result<(), anyhow::Error> {
        let worker = match Self.spawn(cx) {
            Ok(handle) => handle,
            Err(e) => {
                let fail = PySpyResult::Failed {
                    pid: std::process::id(),
                    binary: String::new(),
                    exit_code: None,
                    stderr: format!("failed to spawn pyspy worker: {}", e),
                };
                reply_port.send(cx, fail)?;
                return Ok(());
            }
        };
        // Once reply_port moves into RunPySpyDump, we lose it.
        // MailboxSenderError does not carry the unsent message, so
        // on send failure the caller will observe a timeout rather
        // than an explicit Failed reply.
        if let Err(e) = worker.send(cx, RunPySpyDump { opts, reply_port }) {
            tracing::error!("failed to send to pyspy worker: {}", e);
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<RunPySpyDump> for PySpyWorker {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: RunPySpyDump,
    ) -> Result<(), anyhow::Error> {
        let result = PySpyRunner.dump_self(&message.opts).await;
        message.reply_port.send(cx, result)?;
        cx.stop("pyspy dump complete")?;
        Ok(())
    }
}

/// Internal forwarded message for profile capture.
#[allow(private_interfaces)] // pub required by hyperactor macros; actual use is crate-internal
#[derive(Debug, Serialize, Deserialize, Named)]
pub struct RunPySpyProfile {
    pub request: ValidatedProfileRequest,
    pub reply_port: hyperactor::reference::OncePortRef<PySpyProfileResult>,
}
wirevalue::register_type!(RunPySpyProfile);

/// Short-lived child actor for profile capture. Separate from
/// `PySpyWorker` (PP-5).
#[hyperactor::export(handlers = [RunPySpyProfile])]
pub struct PySpyProfileWorker;

impl Actor for PySpyProfileWorker {}

impl PySpyProfileWorker {
    /// Spawn a profile worker and forward the request. On spawn
    /// failure, sends `WorkerSpawnFailure` back via `reply_port`.
    pub(crate) fn spawn_and_forward(
        cx: &impl hyperactor::context::Actor,
        request: ValidatedProfileRequest,
        reply_port: hyperactor::reference::OncePortRef<PySpyProfileResult>,
    ) -> Result<(), anyhow::Error> {
        let worker = match Self.spawn(cx) {
            Ok(handle) => handle,
            Err(e) => {
                let fail = ProfileExecOutcome::WorkerSpawnFailure {
                    error: e.to_string(),
                };
                reply_port.send(cx, PySpyProfileResult::from(fail))?;
                return Ok(());
            }
        };
        // Once reply_port moves into RunPySpyProfile, we lose it.
        // MailboxSenderError does not carry the unsent message, so
        // on send failure the caller observes a bridge timeout
        // rather than a typed error. Same limitation as PySpyWorker.
        if let Err(e) = worker.send(
            cx,
            RunPySpyProfile {
                request,
                reply_port,
            },
        ) {
            tracing::error!("failed to send to profile worker: {}", e);
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<RunPySpyProfile> for PySpyProfileWorker {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: RunPySpyProfile,
    ) -> Result<(), anyhow::Error> {
        let outcome = PySpyRunner.profile_self(&message.request).await;
        message
            .reply_port
            .send(cx, PySpyProfileResult::from(outcome))?;
        cx.stop("pyspy profile complete")?;
        Ok(())
    }
}

/// Return the ordered list of py-spy binary candidates to try.
/// See PS-3 in `introspect` module doc.
fn resolve_candidates(pyspy_bin_env: Option<String>) -> Vec<(String, String)> {
    let mut candidates = vec![];
    if let Some(path) = pyspy_bin_env {
        if !path.is_empty() {
            let label = format!("PYSPY_BIN={}", path);
            candidates.push((path, label));
        }
    }
    candidates.push(("py-spy".to_string(), "py-spy on PATH".to_string()));
    candidates
}

/// Build the py-spy command for a given binary path.
fn build_command(binary: &str, pid: u32, opts: &PySpyOpts) -> tokio::process::Command {
    let mut cmd = tokio::process::Command::new(binary);
    cmd.arg("dump")
        .arg("--pid")
        .arg(pid.to_string())
        .arg("--json");
    if opts.threads {
        cmd.arg("--threads");
    }
    if opts.native {
        cmd.arg("--native");
    }
    if opts.native_all {
        cmd.arg("--native-all");
    }
    if opts.nonblocking {
        cmd.arg("--nonblocking");
    }
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    cmd
}

/// Map a process::Output to a PySpyResult, parsing the `--json`
/// output into structured `PySpyStackTrace` values.
/// See PS-2, PS-4 in `introspect` module doc.
fn map_output(output: std::process::Output, pid: u32, binary: &str) -> PySpyResult {
    if output.status.success() {
        match serde_json::from_slice::<Vec<PySpyStackTrace>>(&output.stdout) {
            Ok(stack_traces) => PySpyResult::Ok {
                pid,
                binary: binary.to_string(),
                stack_traces,
                warnings: vec![],
            },
            Err(e) => PySpyResult::Failed {
                pid,
                binary: binary.to_string(),
                exit_code: None,
                stderr: format!("failed to parse py-spy JSON output: {}", e),
            },
        }
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        PySpyResult::Failed {
            pid,
            binary: binary.to_string(),
            exit_code: output.status.code(),
            stderr,
        }
    }
}

/// Returns true if the failure indicates the py-spy binary does not
/// support `--native-all` (exit code 2, stderr mentions the flag).
/// Used by `try_exec` to downgrade and retry (PS-11).
fn is_unsupported_native_all(result: &PySpyResult) -> bool {
    matches!(
        result,
        PySpyResult::Failed {
            exit_code: Some(2),
            stderr,
            ..
        } if stderr.contains("--native-all")
    )
}

/// Result of a single spawn → collect execution step.
enum ExecOnce {
    /// py-spy produced a result (success or failure).
    Result(PySpyResult),
    /// The binary was not found (NotFound from spawn).
    NotFound,
}

/// Spawn the py-spy binary once, collect output, and return the
/// result. Factored out of `try_exec` so both the normal attempt
/// path and the PS-11 native-all downgrade path share one
/// implementation of deadline check → spawn → collect.
async fn exec_once(
    binary: &str,
    pid: u32,
    opts: &PySpyOpts,
    deadline: tokio::time::Instant,
    timeout: std::time::Duration,
) -> ExecOnce {
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        return ExecOnce::Result(PySpyResult::Failed {
            pid,
            binary: binary.to_string(),
            exit_code: None,
            stderr: format!("py-spy subprocess timed out after {}s", timeout.as_secs()),
        });
    }
    let child = match build_command(binary, pid, opts).spawn() {
        Ok(child) => child,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return ExecOnce::NotFound,
        Err(e) => {
            return ExecOnce::Result(PySpyResult::Failed {
                pid,
                binary: binary.to_string(),
                exit_code: None,
                stderr: format!("failed to execute: {}", e),
            });
        }
    };
    ExecOnce::Result(collect_with_timeout(child, pid, binary, remaining).await)
}

/// Try to execute py-spy with the given binary path. Returns `None`
/// if the binary was not found (NotFound error), allowing the caller
/// to try the next candidate.
///
/// In nonblocking mode, retries up to 3 times with 100ms backoff
/// because py-spy can segfault reading mutating process memory
/// (PS-10). All attempts share a single deadline so total wall time
/// never exceeds the caller's timeout budget (PS-5).
///
/// If `native_all` is requested but the py-spy binary does not
/// support `--native-all` (exit code 2), the flag is dropped and the
/// command is retried immediately within the same attempt (PS-11a
/// through PS-11e).
async fn try_exec(
    binary: &str,
    pid: u32,
    opts: &PySpyOpts,
    timeout: std::time::Duration,
) -> Option<PySpyResult> {
    let deadline = tokio::time::Instant::now() + timeout;
    let retries = if opts.nonblocking { 3 } else { 1 };
    let mut last_result = None;
    let mut effective_opts = opts.clone();

    for attempt in 0..retries {
        if attempt > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let mut result = match exec_once(binary, pid, &effective_opts, deadline, timeout).await {
            ExecOnce::NotFound => return None,
            ExecOnce::Result(r) => r,
        };
        // PS-11a: py-spy too old for --native-all; downgrade and
        // retry immediately within the same attempt (PS-11b: no
        // backoff, no retry slot consumed).
        if is_unsupported_native_all(&result) && effective_opts.native_all {
            // PS-11e: sticky downgrade — later outer retries keep
            // native_all = false.
            effective_opts.native_all = false;
            result = match exec_once(binary, pid, &effective_opts, deadline, timeout).await {
                ExecOnce::NotFound => return None,
                ExecOnce::Result(r) => r,
            };
            // PS-11c: inject warning on successful downgraded result.
            if let PySpyResult::Ok { warnings, .. } = &mut result {
                warnings.push(
                    "--native-all unsupported by this py-spy; fell back to --native".to_string(),
                );
            }
            // PS-11d: if the downgraded retry also failed, fall
            // through to the normal last_result path below.
        }
        match &result {
            PySpyResult::Ok { .. } => return Some(result),
            _ => {
                last_result = Some(result);
            }
        }
    }

    last_result
}

/// Collect stdout/stderr from a spawned child concurrently with wait,
/// bounded by `timeout`. On expiry the child is killed and reaped.
///
/// Reads stdout/stderr concurrently with wait to avoid pipe-buffer
/// deadlock. Keeps the `Child` handle alive so we can `start_kill`
/// and `wait` on timeout for deterministic termination and reaping.
///
/// See PS-5 in `introspect` module doc.
async fn collect_with_timeout(
    mut child: tokio::process::Child,
    pid: u32,
    binary: &str,
    timeout: std::time::Duration,
) -> PySpyResult {
    let mut stdout_handle = child.stdout.take();
    let mut stderr_handle = child.stderr.take();

    let collect = async {
        let stdout_fut = async {
            let mut buf = Vec::new();
            if let Some(ref mut r) = stdout_handle {
                let _ = tokio::io::AsyncReadExt::read_to_end(r, &mut buf).await;
            }
            buf
        };
        let stderr_fut = async {
            let mut buf = Vec::new();
            if let Some(ref mut r) = stderr_handle {
                let _ = tokio::io::AsyncReadExt::read_to_end(r, &mut buf).await;
            }
            buf
        };
        let (stdout_bytes, stderr_bytes, status) =
            tokio::join!(stdout_fut, stderr_fut, child.wait());
        (stdout_bytes, stderr_bytes, status)
    };

    match tokio::time::timeout(timeout, collect).await {
        Ok((stdout_bytes, stderr_bytes, Ok(status))) => {
            let output = std::process::Output {
                status,
                stdout: stdout_bytes,
                stderr: stderr_bytes,
            };
            map_output(output, pid, binary)
        }
        Ok((_, _, Err(e))) => PySpyResult::Failed {
            pid,
            binary: binary.to_string(),
            exit_code: None,
            stderr: format!("failed to wait for child: {}", e),
        },
        Err(_) => {
            // Timeout — kill and reap deterministically.
            let _ = child.start_kill();
            let _ = child.wait().await;
            PySpyResult::Failed {
                pid,
                binary: binary.to_string(),
                exit_code: None,
                stderr: format!("py-spy subprocess timed out after {}s", timeout.as_secs()),
            }
        }
    }
}

/// Build a `py-spy record --format flamegraph` command.
fn build_record_command(
    binary: &str,
    pid: u32,
    request: &ValidatedProfileRequest,
    output_path: &std::path::Path,
) -> tokio::process::Command {
    let mut cmd = tokio::process::Command::new(binary);
    cmd.arg("record")
        .arg("--pid")
        .arg(pid.to_string())
        .arg("--duration")
        .arg(request.duration().get().to_string())
        .arg("--rate")
        .arg(request.rate().get().to_string())
        .arg("--format")
        .arg("flamegraph")
        .arg("--output")
        .arg(output_path);
    if request.native() {
        cmd.arg("--native");
    }
    if request.threads() {
        cmd.arg("--threads");
    }
    if request.nonblocking() {
        cmd.arg("--nonblocking");
    }
    // py-spy record writes output to a file, not stdout. Do NOT
    // pipe stdout — an undrained pipe can deadlock the child.
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::piped());
    cmd
}

/// Collect stderr and wait for exit, bounded by `timeout`. On
/// expiry the child is explicitly killed and reaped. See PP-2, PP-3.
async fn collect_profile_with_timeout(
    mut child: tokio::process::Child,
    pid: u32,
    binary: &str,
    timeout: std::time::Duration,
) -> Result<(std::process::ExitStatus, String), ProfileExecOutcome> {
    // Drain stderr on a separate task so it does not block the
    // child.wait() path and so `child` stays in this scope for
    // explicit kill/reap on timeout.
    let stderr_handle = child.stderr.take();
    let stderr_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        if let Some(mut r) = stderr_handle {
            let _ = tokio::io::AsyncReadExt::read_to_end(&mut r, &mut buf).await;
        }
        buf
    });

    match tokio::time::timeout(timeout, child.wait()).await {
        Ok(Ok(status)) => {
            let stderr_bytes = stderr_task.await.unwrap_or_default();
            let stderr = String::from_utf8_lossy(&stderr_bytes).into_owned();
            Ok((status, stderr))
        }
        Ok(Err(e)) => {
            stderr_task.abort();
            Err(ProfileExecOutcome::WaitFailure {
                pid,
                binary: binary.to_string(),
                error: e.to_string(),
            })
        }
        Err(_) => {
            // Timeout — explicit kill and reap.
            let _ = child.start_kill();
            let _ = child.wait().await;
            let stderr_bytes = stderr_task.await.unwrap_or_default();
            let stderr = String::from_utf8_lossy(&stderr_bytes).into_owned();
            Err(ProfileExecOutcome::TimedOut {
                pid,
                binary: binary.to_string(),
                timeout,
                stderr,
            })
        }
    }
}

/// Try to run a profile capture with the given binary. Returns `None`
/// if the binary was not found (caller tries next candidate).
async fn try_profile(
    binary: &str,
    pid: u32,
    request: &ValidatedProfileRequest,
) -> Option<ProfileExecOutcome> {
    let timeout = request.subprocess_timeout();
    let tmp_dir = match tempfile::tempdir() {
        Ok(d) => d,
        Err(e) => {
            return Some(ProfileExecOutcome::TempDirFailure {
                pid,
                binary: binary.to_string(),
                error: e.to_string(),
            });
        }
    };
    let svg_path = tmp_dir.path().join("profile.svg");

    let child = match build_record_command(binary, pid, request, &svg_path).spawn() {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(e) => {
            return Some(ProfileExecOutcome::SubprocessSpawnFailure {
                pid,
                binary: binary.to_string(),
                error: e.to_string(),
            });
        }
    };

    let (status, stderr) = match collect_profile_with_timeout(child, pid, binary, timeout).await {
        Ok(pair) => pair,
        Err(outcome) => return Some(outcome),
    };

    if !status.success() {
        return Some(ProfileExecOutcome::ExitFailure {
            pid,
            binary: binary.to_string(),
            exit_code: status.code(),
            stderr,
        });
    }

    match std::fs::read(&svg_path) {
        Ok(bytes) if bytes.is_empty() => Some(ProfileExecOutcome::OutputEmpty {
            pid,
            binary: binary.to_string(),
        }),
        Ok(svg) => Some(ProfileExecOutcome::Ok {
            pid,
            binary: binary.to_string(),
            svg,
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Some(ProfileExecOutcome::OutputMissing {
                pid,
                binary: binary.to_string(),
            })
        }
        Err(e) => Some(ProfileExecOutcome::OutputReadFailure {
            pid,
            binary: binary.to_string(),
            error: e.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::process::ExitStatusExt;
    use std::time::Duration;

    use tokio::process::Command;

    use super::*;

    #[test]
    fn pyspy_result_wirevalue_roundtrip() {
        // Regression test: #[serde(skip_serializing_if)] is
        // incompatible with bincode (positional format). Empty
        // warnings must still round-trip correctly through wirevalue
        // Multipart encoding.
        let original = PySpyResult::Ok {
            pid: 42,
            binary: "py-spy".to_string(),
            stack_traces: vec![PySpyStackTrace {
                pid: 42,
                thread_id: 1,
                thread_name: Some("main".to_string()),
                os_thread_id: Some(100),
                active: true,
                owns_gil: true,
                frames: vec![PySpyFrame {
                    name: "do_work".to_string(),
                    filename: "test.py".to_string(),
                    module: None,
                    short_filename: None,
                    line: 10,
                    locals: None,
                    is_entry: false,
                }],
            }],
            warnings: vec![],
        };
        let any = wirevalue::Any::serialize(&original).expect("serialize");
        let restored: PySpyResult = any.deserialized().expect("deserialize");
        assert_eq!(original, restored);
    }

    #[test]
    fn candidates_no_env() {
        // PS-3: no PYSPY_BIN → only PATH candidate.
        let candidates = resolve_candidates(None);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].0, "py-spy");
        assert_eq!(candidates[0].1, "py-spy on PATH");
    }

    #[test]
    fn candidates_env_first_then_path() {
        // PS-3: PYSPY_BIN first, then PATH.
        let candidates = resolve_candidates(Some("/custom/py-spy".to_string()));
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].0, "/custom/py-spy");
        assert!(candidates[0].1.contains("PYSPY_BIN=/custom/py-spy"));
        assert_eq!(candidates[1].0, "py-spy");
    }

    #[test]
    fn candidates_empty_env_ignored() {
        // PS-3: empty PYSPY_BIN is treated as unset.
        let candidates = resolve_candidates(Some(String::new()));
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].0, "py-spy");
    }

    #[test]
    fn output_success_parses_json() {
        // PS-4: py-spy --json stdout is parsed into structured traces.
        let json = serde_json::json!([{
            "pid": 42,
            "thread_id": 1234,
            "thread_name": "MainThread",
            "os_thread_id": 5678,
            "active": true,
            "owns_gil": true,
            "frames": [{
                "name": "do_work",
                "filename": "foo.py",
                "module": null,
                "short_filename": null,
                "line": 10,
                "locals": null,
                "is_entry": false
            }]
        }]);
        let output = std::process::Output {
            status: std::process::ExitStatus::default(),
            stdout: serde_json::to_vec(&json).unwrap(),
            stderr: vec![],
        };
        let result = map_output(output, 42, "/usr/bin/py-spy");
        match result {
            PySpyResult::Ok {
                pid,
                binary,
                stack_traces,
                ..
            } => {
                assert_eq!(pid, 42);
                assert_eq!(binary, "/usr/bin/py-spy");
                assert_eq!(stack_traces.len(), 1);
                assert_eq!(stack_traces[0].thread_id, 1234);
                assert_eq!(stack_traces[0].thread_name.as_deref(), Some("MainThread"));
                assert!(stack_traces[0].owns_gil);
                assert_eq!(stack_traces[0].frames.len(), 1);
                assert_eq!(stack_traces[0].frames[0].name, "do_work");
                assert_eq!(stack_traces[0].frames[0].filename, "foo.py");
                assert_eq!(stack_traces[0].frames[0].line, 10);
            }
            other => panic!("expected Ok, got {:?}", other),
        }
    }

    #[test]
    fn output_invalid_json_maps_to_failed() {
        // PS-4: unparseable JSON maps to Failed.
        let output = std::process::Output {
            status: std::process::ExitStatus::default(),
            stdout: b"not valid json".to_vec(),
            stderr: vec![],
        };
        let result = map_output(output, 42, "py-spy");
        match result {
            PySpyResult::Failed { pid, stderr, .. } => {
                assert_eq!(pid, 42);
                assert!(
                    stderr.contains("failed to parse py-spy JSON output"),
                    "unexpected stderr: {stderr}"
                );
            }
            other => panic!("expected Failed, got {:?}", other),
        }
    }

    #[test]
    fn output_nonzero_exit_maps_to_failed() {
        // PS-2: nonzero exit → Failed with stderr.
        let status = std::process::ExitStatus::from_raw(256); // exit code 1
        let output = std::process::Output {
            status,
            stdout: vec![],
            stderr: b"Permission denied".to_vec(),
        };
        let result = map_output(output, 99, "py-spy");
        match result {
            PySpyResult::Failed {
                pid,
                binary,
                exit_code,
                stderr,
            } => {
                assert_eq!(pid, 99);
                assert_eq!(binary, "py-spy");
                assert_eq!(exit_code, Some(1));
                assert_eq!(stderr, "Permission denied");
            }
            other => panic!("expected Failed, got {:?}", other),
        }
    }

    #[test]
    fn output_preserves_caller_pid() {
        // PS-1: pid in result is exactly what the caller passes.
        let json = serde_json::json!([]);
        let output = std::process::Output {
            status: std::process::ExitStatus::default(),
            stdout: serde_json::to_vec(&json).unwrap(),
            stderr: vec![],
        };
        let result = map_output(output, 12345, "bin");
        match result {
            PySpyResult::Ok { pid, .. } => assert_eq!(pid, 12345),
            other => panic!("expected Ok, got {:?}", other),
        }
    }

    fn default_opts() -> PySpyOpts {
        PySpyOpts {
            threads: false,
            native: false,
            native_all: false,
            nonblocking: false,
        }
    }

    #[tokio::test]
    async fn exec_missing_binary_returns_none() {
        // PS-3: NotFound from exec → None (triggers fallback).
        let result = try_exec(
            "/definitely/not/a/real/binary",
            1,
            &default_opts(),
            std::time::Duration::from_secs(5),
        )
        .await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn exec_present_binary_returns_some() {
        // "true" exits 0 with empty stdout, which is not valid
        // py-spy JSON. We expect a Failed result from parse error.
        let result = try_exec(
            "true",
            1,
            &default_opts(),
            std::time::Duration::from_secs(5),
        )
        .await;
        match result {
            Some(PySpyResult::Failed { stderr, .. }) => {
                assert!(
                    stderr.contains("parse"),
                    "expected JSON parse error, got: {stderr}"
                );
            }
            other => panic!("expected Some(Failed{{parse..}}), got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn collect_timeout_kills_child_and_returns_failed() {
        // PS-5: subprocess that hangs past timeout → Failed with
        // "timed out" message; child is killed and reaped.
        let child = Command::new("sleep")
            .arg("100")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("sleep must be available");

        let result = collect_with_timeout(
            child,
            std::process::id(),
            "sleep",
            std::time::Duration::from_millis(100),
        )
        .await;

        match result {
            PySpyResult::Failed { stderr, .. } => {
                assert!(
                    stderr.contains("timed out"),
                    "expected timeout message, got: {stderr}"
                );
            }
            other => panic!("expected Failed, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn exec_failing_binary_returns_failed() {
        // "false" exists on all unix systems and exits 1.
        let result = try_exec(
            "false",
            42,
            &default_opts(),
            std::time::Duration::from_secs(5),
        )
        .await;
        assert!(result.is_some());
        match result.unwrap() {
            PySpyResult::Failed {
                pid,
                binary,
                exit_code,
                ..
            } => {
                assert_eq!(pid, 42);
                assert_eq!(binary, "false");
                assert!(exit_code.is_some());
            }
            other => panic!("expected Failed, got {:?}", other),
        }
    }

    /// Write a fake py-spy shell script to a temp file, make it
    /// executable, and return the path. The script logs each
    /// invocation's argv to `<script>.log`.
    ///
    /// Returns a `TempPath` (not `NamedTempFile`) so the write fd is
    /// closed before exec — Linux returns ETXTBSY if a file with an
    /// open write fd is executed.
    fn write_fake_pyspy(script_body: &str) -> tempfile::TempPath {
        let mut f = tempfile::NamedTempFile::new().expect("create temp file");
        write!(f, "#!/bin/sh\n{script_body}").expect("write script");
        f.as_file().sync_all().expect("sync");
        std::fs::set_permissions(f.path(), std::fs::Permissions::from_mode(0o755))
            .expect("chmod +x");
        f.into_temp_path()
    }

    /// Read the argv log written by the fake script. Each line is one
    /// invocation's `$@`.
    fn read_log(script_path: &std::path::Path) -> Vec<String> {
        let log_path = format!("{}.log", script_path.display());
        match std::fs::read_to_string(&log_path) {
            Ok(contents) => contents.lines().map(String::from).collect(),
            Err(_) => vec![],
        }
    }

    #[tokio::test]
    async fn native_all_downgrade_succeeds() {
        // PS-11a, PS-11b, PS-11c: unsupported --native-all triggers
        // immediate downgrade in the same attempt, and the successful
        // result carries the fallback warning.
        let script = write_fake_pyspy(
            r#"
echo "$@" >> "$0.log"
for arg in "$@"; do
    if [ "$arg" = "--native-all" ]; then
        echo "unrecognized option --native-all" >&2
        exit 2
    fi
done
echo "[]"
exit 0
"#,
        );
        let opts = PySpyOpts {
            threads: false,
            native: true,
            native_all: true,
            nonblocking: false,
        };
        let result = try_exec(
            script.to_str().unwrap(),
            1,
            &opts,
            std::time::Duration::from_secs(5),
        )
        .await;
        // Must succeed with the downgraded result.
        let result = result.expect("expected Some");
        match &result {
            PySpyResult::Ok { warnings, .. } => {
                assert!(
                    warnings.iter().any(|w| w.contains("fell back to --native")),
                    "PS-11c: expected fallback warning, got: {warnings:?}"
                );
            }
            other => panic!("expected Ok, got: {other:?}"),
        }
        // Check invocation log.
        let log = read_log(&script);
        assert_eq!(
            log.len(),
            2,
            "PS-11b: expected exactly 2 invocations, got {}",
            log.len()
        );
        assert!(
            log[0].contains("--native-all"),
            "PS-11a: first invocation must include --native-all, got: {}",
            log[0]
        );
        assert!(
            !log[1].contains("--native-all"),
            "PS-11a: second invocation must NOT include --native-all, got: {}",
            log[1]
        );
    }

    #[tokio::test]
    async fn native_all_downgrade_fails_retries_continue() {
        // PS-11d, PS-11e: downgraded retry fails, outer nonblocking
        // retries continue with native_all = false.
        let script = write_fake_pyspy(
            r#"
echo "$@" >> "$0.log"
for arg in "$@"; do
    if [ "$arg" = "--native-all" ]; then
        echo "unrecognized option --native-all" >&2
        exit 2
    fi
done
echo "Permission denied" >&2
exit 1
"#,
        );
        let opts = PySpyOpts {
            threads: false,
            native: true,
            native_all: true,
            nonblocking: true, // 3 outer retries
        };
        let result = try_exec(
            script.to_str().unwrap(),
            1,
            &opts,
            std::time::Duration::from_secs(10),
        )
        .await;
        // Must be a generic failure, not the native-all error.
        let result = result.expect("expected Some");
        match &result {
            PySpyResult::Failed {
                stderr, exit_code, ..
            } => {
                assert!(
                    stderr.contains("Permission denied"),
                    "PS-11d: expected generic failure, got: {stderr}"
                );
                assert_eq!(*exit_code, Some(1));
            }
            other => panic!("expected Failed, got: {other:?}"),
        }
        // Check invocation log: 4 calls total.
        //   Attempt 0: --native-all (fail) → downgrade (fail)
        //   Attempt 1: without --native-all (fail)
        //   Attempt 2: without --native-all (fail)
        let log = read_log(&script);
        assert_eq!(log.len(), 4, "expected 4 invocations, got {}", log.len());
        assert!(
            log[0].contains("--native-all"),
            "PS-11a: first invocation must include --native-all, got: {}",
            log[0]
        );
        for (i, line) in log[1..].iter().enumerate() {
            assert!(
                !line.contains("--native-all"),
                "PS-11e: invocation {} must NOT include --native-all, got: {}",
                i + 1,
                line
            );
        }
    }

    /// PP-2: subprocess timeout yields `TimedOut` with partial stderr.
    #[tokio::test]
    async fn profile_collect_timeout_returns_timed_out() {
        let child = Command::new("sh")
            .arg("-c")
            .arg("echo diag >&2; sleep 60")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("sh must be available");

        let result = collect_profile_with_timeout(
            child,
            std::process::id(),
            "sh",
            std::time::Duration::from_millis(200),
        )
        .await;

        match result {
            Err(ProfileExecOutcome::TimedOut { stderr, .. }) => {
                assert!(
                    stderr.contains("diag"),
                    "expected partial stderr captured after kill, got: {stderr}"
                );
            }
            other => panic!("expected TimedOut, got: {other:?}"),
        }
    }

    fn test_request() -> ValidatedProfileRequest {
        ValidatedProfileRequest::try_new(
            &PySpyProfileOpts {
                duration_s: 1,
                rate_hz: 100,
                native: false,
                threads: false,
                nonblocking: false,
            },
            std::time::Duration::from_secs(300),
        )
        .unwrap()
    }

    /// PP-4, PS-3: missing binary yields `None` (try next candidate).
    #[tokio::test]
    async fn profile_try_missing_binary_returns_none() {
        let result = try_profile("/definitely/not/a/real/binary", 1, &test_request()).await;
        assert!(result.is_none(), "missing binary must return None");
    }

    /// PP-3: successful exit with empty output yields `OutputEmpty`.
    #[tokio::test]
    async fn profile_success_exit_empty_file_returns_output_empty() {
        let script = write_fake_pyspy(
            r#"
output=""
while [ $# -gt 0 ]; do
    case "$1" in
        --output) shift; output="$1" ;;
    esac
    shift
done
touch "$output"
exit 0
"#,
        );
        let result = try_profile(script.to_str().unwrap(), 1, &test_request()).await;
        assert!(
            matches!(result, Some(ProfileExecOutcome::OutputEmpty { .. })),
            "PP-3: expected OutputEmpty, got: {result:?}"
        );
    }

    /// PP-3: successful exit with missing output yields `OutputMissing`.
    #[tokio::test]
    async fn profile_success_exit_missing_file_returns_output_missing() {
        let script = write_fake_pyspy("exit 0\n");
        let result = try_profile(script.to_str().unwrap(), 1, &test_request()).await;
        assert!(
            matches!(result, Some(ProfileExecOutcome::OutputMissing { .. })),
            "PP-3: expected OutputMissing, got: {result:?}"
        );
    }

    /// PP-1: zero duration rejected.
    #[test]
    fn validated_request_rejects_zero_duration() {
        let opts = PySpyProfileOpts {
            duration_s: 0,
            rate_hz: 100,
            native: false,
            threads: false,
            nonblocking: false,
        };
        let err = ValidatedProfileRequest::try_new(&opts, std::time::Duration::from_secs(300));
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("positive"));
    }

    /// PP-1: over-max duration rejected.
    #[test]
    fn validated_request_rejects_over_max_duration() {
        let opts = PySpyProfileOpts {
            duration_s: 999,
            rate_hz: 100,
            native: false,
            threads: false,
            nonblocking: false,
        };
        let err = ValidatedProfileRequest::try_new(&opts, std::time::Duration::from_secs(300));
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("exceeds max"));
    }

    /// PP-1: zero rate rejected.
    #[test]
    fn validated_request_rejects_zero_rate() {
        let opts = PySpyProfileOpts {
            duration_s: 5,
            rate_hz: 0,
            native: false,
            threads: false,
            nonblocking: false,
        };
        let err = ValidatedProfileRequest::try_new(&opts, std::time::Duration::from_secs(300));
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("rate_hz"));
    }

    /// PP-1: excessive rate rejected.
    #[test]
    fn validated_request_rejects_excessive_rate() {
        let opts = PySpyProfileOpts {
            duration_s: 5,
            rate_hz: 9999,
            native: false,
            threads: false,
            nonblocking: false,
        };
        let err = ValidatedProfileRequest::try_new(&opts, std::time::Duration::from_secs(300));
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("rate_hz"));
    }

    /// PP-2: timeout arithmetic is correct and deterministic.
    #[test]
    fn validated_request_computes_exact_timeouts() {
        let opts = PySpyProfileOpts {
            duration_s: 30,
            rate_hz: 100,
            native: true,
            threads: false,
            nonblocking: false,
        };
        let req =
            ValidatedProfileRequest::try_new(&opts, std::time::Duration::from_secs(300)).unwrap();
        assert_eq!(req.duration().get(), 30);
        assert_eq!(req.rate().get(), 100);
        assert!(req.native());
        assert_eq!(req.subprocess_timeout(), std::time::Duration::from_secs(45));
        assert_eq!(req.bridge_timeout(), std::time::Duration::from_secs(50));
    }

    /// PP-6: internal-to-wire conversion is near-identity.
    #[test]
    fn profile_exec_outcome_conversion_is_identity() {
        // Each internal outcome maps to the identically-named wire variant.
        let r = PySpyProfileResult::from(ProfileExecOutcome::Ok {
            pid: 1,
            binary: "b".into(),
            svg: vec![1],
        });
        assert!(matches!(r, PySpyProfileResult::Ok { pid: 1, .. }));

        let r = PySpyProfileResult::from(ProfileExecOutcome::BinaryNotFound {
            searched: vec!["x".into()],
        });
        assert!(matches!(r, PySpyProfileResult::BinaryNotFound { .. }));

        let r = PySpyProfileResult::from(ProfileExecOutcome::TimedOut {
            pid: 1,
            binary: "b".into(),
            timeout: Duration::from_secs(10),
            stderr: "s".into(),
        });
        assert!(matches!(
            r,
            PySpyProfileResult::TimedOut { timeout_s: 10, .. }
        ));

        let r = PySpyProfileResult::from(ProfileExecOutcome::ExitFailure {
            pid: 1,
            binary: "b".into(),
            exit_code: Some(2),
            stderr: "e".into(),
        });
        assert!(matches!(
            r,
            PySpyProfileResult::ExitFailure {
                exit_code: Some(2),
                ..
            }
        ));

        let r = PySpyProfileResult::from(ProfileExecOutcome::OutputMissing {
            pid: 1,
            binary: "b".into(),
        });
        assert!(matches!(
            r,
            PySpyProfileResult::OutputMissing { pid: 1, .. }
        ));

        let r = PySpyProfileResult::from(ProfileExecOutcome::OutputEmpty {
            pid: 1,
            binary: "b".into(),
        });
        assert!(matches!(r, PySpyProfileResult::OutputEmpty { pid: 1, .. }));

        let r = PySpyProfileResult::from(ProfileExecOutcome::OutputReadFailure {
            pid: 1,
            binary: "b".into(),
            error: "permission denied".into(),
        });
        assert!(matches!(r, PySpyProfileResult::OutputReadFailure { .. }));

        let r =
            PySpyProfileResult::from(ProfileExecOutcome::WorkerSpawnFailure { error: "w".into() });
        assert!(matches!(r, PySpyProfileResult::WorkerSpawnFailure { .. }));

        let r = PySpyProfileResult::from(ProfileExecOutcome::SubprocessSpawnFailure {
            pid: 1,
            binary: "b".into(),
            error: "s".into(),
        });
        assert!(matches!(
            r,
            PySpyProfileResult::SubprocessSpawnFailure { .. }
        ));

        let r = PySpyProfileResult::from(ProfileExecOutcome::WaitFailure {
            pid: 1,
            binary: "b".into(),
            error: "w".into(),
        });
        assert!(matches!(r, PySpyProfileResult::WaitFailure { .. }));

        let r = PySpyProfileResult::from(ProfileExecOutcome::TempDirFailure {
            pid: 1,
            binary: "b".into(),
            error: "t".into(),
        });
        assert!(matches!(r, PySpyProfileResult::TempDirFailure { .. }));
    }
}
