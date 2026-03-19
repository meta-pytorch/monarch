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

use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

/// Result of a py-spy stack dump request.
///
/// See PS-2, PS-4 in `introspect` module doc.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named)]
pub enum PySpyResult {
    /// Successful stack dump.
    Ok {
        pid: u32,
        binary: String,
        stack: String,
    },
    /// py-spy binary not found in environment.
    BinaryNotFound { searched: Vec<String> },
    /// py-spy exited with an error.
    Failed {
        pid: u32,
        binary: String,
        exit_code: Option<i32>,
        stderr: String,
    },
}
wirevalue::register_type!(PySpyResult);

/// Runs py-spy against the current process.
///
/// See PS-1, PS-3 in `introspect` module doc.
pub struct PySpyRunner;

impl PySpyRunner {
    /// Dump Python stacks for this process.
    ///
    /// Resolves the py-spy binary (PS-3), attaches to
    /// `std::process::id()` (PS-1), and returns raw output (PS-4).
    ///
    /// PS-1 is structurally enforced: `PySpyDump` carries no PID
    /// field, and this method hardcodes `std::process::id()`. There
    /// is no code path that could substitute a different PID.
    pub async fn dump_self(&self, threads: bool) -> PySpyResult {
        let pid = std::process::id();
        let candidates = resolve_candidates(std::env::var("PYSPY_BIN").ok());
        let mut searched = vec![];

        for (binary, label) in &candidates {
            searched.push(label.clone());
            if let Some(result) = try_exec(
                binary,
                pid,
                threads,
                hyperactor_config::global::get(crate::config::MESH_ADMIN_PYSPY_TIMEOUT),
            )
            .await
            {
                return result;
            }
        }

        PySpyResult::BinaryNotFound { searched }
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
fn build_command(binary: &str, pid: u32, threads: bool) -> tokio::process::Command {
    let mut cmd = tokio::process::Command::new(binary);
    cmd.arg("dump").arg("--pid").arg(pid.to_string());
    if threads {
        cmd.arg("--threads");
    }
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    cmd
}

/// Map a process::Output to a PySpyResult.
/// See PS-2, PS-4 in `introspect` module doc.
fn map_output(output: std::process::Output, pid: u32, binary: &str) -> PySpyResult {
    if output.status.success() {
        let stack = String::from_utf8_lossy(&output.stdout).into_owned();
        PySpyResult::Ok {
            pid,
            binary: binary.to_string(),
            stack,
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

/// Try to execute py-spy with the given binary path. Returns `None`
/// if the binary was not found (NotFound error), allowing the caller
/// to try the next candidate.
async fn try_exec(
    binary: &str,
    pid: u32,
    threads: bool,
    timeout: std::time::Duration,
) -> Option<PySpyResult> {
    let child = match build_command(binary, pid, threads).spawn() {
        Ok(child) => child,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(e) => {
            return Some(PySpyResult::Failed {
                pid,
                binary: binary.to_string(),
                exit_code: None,
                stderr: format!("failed to execute: {}", e),
            });
        }
    };
    Some(collect_with_timeout(child, pid, binary, timeout).await)
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
    fn output_success_passthrough() {
        // PS-4: raw stdout passthrough, no transformation.
        let output = std::process::Output {
            status: std::process::ExitStatus::default(),
            stdout: b"Thread 0x7f abc\n  foo.py:10\n".to_vec(),
            stderr: vec![],
        };
        let result = map_output(output, 42, "/usr/bin/py-spy");
        match result {
            PySpyResult::Ok { pid, binary, stack } => {
                assert_eq!(pid, 42);
                assert_eq!(binary, "/usr/bin/py-spy");
                assert_eq!(stack, "Thread 0x7f abc\n  foo.py:10\n");
            }
            other => panic!("expected Ok, got {:?}", other),
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
        let output = std::process::Output {
            status: std::process::ExitStatus::default(),
            stdout: b"stack".to_vec(),
            stderr: vec![],
        };
        let result = map_output(output, 12345, "bin");
        match result {
            PySpyResult::Ok { pid, .. } => assert_eq!(pid, 12345),
            other => panic!("expected Ok, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn exec_missing_binary_returns_none() {
        // PS-3: NotFound from exec → None (triggers fallback).
        let result = try_exec(
            "/definitely/not/a/real/binary",
            1,
            false,
            std::time::Duration::from_secs(5),
        )
        .await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn exec_present_binary_returns_some() {
        // "true" exists on all unix systems and exits 0.
        let result = try_exec("true", 1, false, std::time::Duration::from_secs(5)).await;
        assert!(result.is_some());
        match result.unwrap() {
            PySpyResult::Ok { binary, .. } => assert_eq!(binary, "true"),
            other => panic!("expected Ok, got {:?}", other),
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
        let result = try_exec("false", 42, false, std::time::Duration::from_secs(5)).await;
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
}
