/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Integration test for ProcessAllocator child process cleanup behavior.
//! Tests that when a ProcessAllocator parent process is killed, its children
//! are properly cleaned up.

use std::process::Command;
use std::process::Stdio;
use std::time::Duration;
use std::time::Instant;

use hyperactor_mesh::alloc::ProcState;
use nix::sys::signal::Signal;
use nix::sys::signal::{self};
use nix::unistd::Pid;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::process::Command as TokioCommand;
use tokio::time::sleep;
use tokio::time::timeout;

/// Test that ProcessAllocator children are cleaned up when parent is killed
#[tokio::test]
#[cfg_attr(not(fbcode_build), ignore)]
async fn test_process_allocator_child_cleanup() {
    let test_binary_path = buck_resources::get("monarch/hyperactor_mesh/test_bin").unwrap();
    eprintln!("Starting test process allocator at: {:?}", test_binary_path);

    // Start the test process allocator with JSON output
    let mut child = TokioCommand::new(&test_binary_path)
        .stdout(Stdio::piped())
        // Let stderr through, for easier debugging
        .spawn()
        .expect("Failed to start test process allocator");

    let parent_pid = child.id().expect("Failed to get child PID");
    eprintln!("Parent process started with PID: {}", parent_pid);

    // Set up stdout reader for JSON events
    let stdout = child.stdout.take().expect("Failed to get stdout");
    let mut reader = BufReader::new(stdout).lines();

    let expected_running_count = 4; // We know we're allocating 4 child processes
    let mut running_count = 0;
    let mut child_pids = Vec::new();

    // Read events until we have enough running children
    loop {
        #[allow(clippy::disallowed_methods)]
        match timeout(Duration::from_secs(30), reader.next_line()).await {
            Ok(Ok(Some(line))) => {
                if let Ok(proc_state) = serde_json::from_str::<ProcState>(&line) {
                    eprintln!("Received ProcState: {:?}", proc_state);

                    match proc_state {
                        ProcState::Created { pid, .. } => {
                            if pid != 0 {
                                child_pids.push(pid);
                                eprintln!("Collected child PID: {}", pid);
                            }
                        }
                        ProcState::Running { .. } => {
                            running_count += 1;
                            eprintln!(
                                "Child {} of {} is running",
                                running_count, expected_running_count
                            );

                            if running_count >= expected_running_count {
                                eprintln!("All {} children are running!", expected_running_count);
                                break;
                            }
                        }
                        ProcState::Failed { description, .. } => {
                            panic!("Allocation failed: {}", description);
                        }
                        _ => {}
                    }
                }
            }
            Ok(Ok(None)) => {
                eprintln!("Child process stdout closed");
                break;
            }
            Ok(Err(e)) => {
                eprintln!("Error reading from child stdout: {}", e);
                break;
            }
            Err(_) => {
                eprintln!("Timeout waiting for child events");
                break;
            }
        }
    }

    // Ensure we got all the running children we expected
    assert_eq!(
        running_count, expected_running_count,
        "Expected {} running children but only got {}",
        expected_running_count, running_count
    );

    // Ensure we collected PIDs from Created events
    eprintln!("Collected child PIDs from Created events: {:?}", child_pids);
    assert!(
        !child_pids.is_empty(),
        "No child PIDs were collected from Created events"
    );

    // Kill the parent process with SIGKILL
    eprintln!("Killing parent process with PID: {}", parent_pid);
    signal::kill(Pid::from_raw(parent_pid as i32), Signal::SIGKILL)
        .expect("Failed to kill parent process");

    // Wait for the parent to be killed
    #[allow(clippy::disallowed_methods)]
    let wait_result = timeout(Duration::from_secs(5), child.wait()).await;
    match wait_result {
        Ok(Ok(status)) => eprintln!("Parent process exited with status: {:?}", status),
        Ok(Err(e)) => panic!("Error waiting for parent process: {}", e),
        Err(_) => {
            panic!("Parent process did not exit within timeout");
        }
    }

    eprintln!("Waiting longer to see if children eventually exit due to channel hangup...");
    let timeout = Duration::from_mins(1);
    let start = Instant::now();

    loop {
        if Instant::now() - start >= timeout {
            panic!("ProcessAllocator children not cleaned up after 60s");
        }

        #[allow(clippy::disallowed_methods)]
        sleep(Duration::from_secs(2)).await;

        let still_running: Vec<_> = child_pids
            .iter()
            .filter(|&&pid| is_process_running(pid))
            .cloned()
            .collect();

        if still_running.is_empty() {
            eprintln!("All children have exited!");
            return;
        }
    }
}

/// Check if a process with the given PID is still running
fn is_process_running(pid: u32) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}
