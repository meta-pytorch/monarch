/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! py-spy integration for remote Python stack dumps.
//!
//! See PS-* invariants in `introspect` module doc.

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

#[cfg(test)]
mod tests {
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
        use std::os::unix::process::ExitStatusExt;
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
        use tokio::process::Command;

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
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;

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
}
