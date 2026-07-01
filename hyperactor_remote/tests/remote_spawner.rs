/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::path::Path;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::ExitStatus;
use std::process::Output;
use std::process::Stdio;
use std::time::Duration;
use std::time::SystemTime;

struct DemoRun {
    driver: Output,
    spawner: Output,
}

#[test]
fn test_remote_spawner_demo_add() {
    let run = run_demo("driver");

    assert!(
        run.driver.status.success(),
        "driver failed:\n{}\nspawner:\n{}",
        output_text(&run.driver),
        output_text(&run.spawner)
    );
    assert!(
        output_text(&run.driver).contains("driver requested proc"),
        "driver output did not include proc spawn request:\n{}\nspawner:\n{}",
        output_text(&run.driver),
        output_text(&run.spawner)
    );
    assert!(
        output_text(&run.driver).contains("driver received result: 40 + 2 = 42"),
        "driver output did not include calculation result:\n{}\nspawner:\n{}",
        output_text(&run.driver),
        output_text(&run.spawner)
    );
}

#[test]
fn test_remote_spawner_demo_overflow() {
    let run = run_demo("overflow");

    assert!(
        !run.driver.status.success(),
        "overflow driver unexpectedly succeeded:\n{}\nspawner:\n{}",
        output_text(&run.driver),
        output_text(&run.spawner)
    );
    let driver_output = output_text(&run.driver);
    assert!(
        driver_output.contains("driver observed supervision event and will propagate it"),
        "driver output did not include propagated supervision event:\n{}\nspawner:\n{}",
        driver_output,
        output_text(&run.spawner)
    );
    assert!(
        driver_output.contains("integer overflow while adding"),
        "driver output did not include expected overflow failure:\n{}\nspawner:\n{}",
        driver_output,
        output_text(&run.spawner)
    );
}

#[test]
fn test_remote_spawner_demo_proc_death() {
    // The spawned actor exits its whole OS process (process::exit(1)). The
    // spawner-side worker observes the process exit and reports a terminal
    // supervision event, which propagates back to the driver.
    let run = run_demo("crash");

    assert!(
        !run.driver.status.success(),
        "crash driver unexpectedly succeeded:\n{}\nspawner:\n{}",
        output_text(&run.driver),
        output_text(&run.spawner)
    );
    let driver_output = output_text(&run.driver);
    assert!(
        driver_output.contains("driver observed supervision event and will propagate it"),
        "driver did not observe a supervision event for the dead proc:\n{}\nspawner:\n{}",
        driver_output,
        output_text(&run.spawner)
    );
    assert!(
        driver_output.contains("exited with code 1"),
        "driver supervision event did not reflect the proc's OS-process exit:\n{}\nspawner:\n{}",
        driver_output,
        output_text(&run.spawner)
    );
}

#[test]
fn test_remote_spawner_demo_graceful_stop() {
    // The driver computes once and exits cleanly; that clean exit triggers a
    // graceful stop of the spawned proc, which drains and exits on its own
    // rather than being hard-killed. The child records "STOPPED_CLEANLY" only
    // when it came down via a clean drain.
    let tmp = temp_dir();
    std::fs::create_dir_all(&tmp).unwrap();
    let token_file = tmp.join("spawner-token");
    let status_file = tmp.join("proc-status");
    let binary = remote_spawner_binary();

    let spawner = Command::new(&binary)
        .arg("spawner")
        .arg("--token-file")
        .arg(&token_file)
        .arg("--proc-status-file")
        .arg(&status_file)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|error| {
            panic!("failed to spawn proc spawner {}: {error}", binary.display())
        });

    if let Err(error) = wait_for_file(&token_file) {
        let spawner_output = stop_child(spawner);
        let _ = std::fs::remove_dir_all(&tmp);
        panic!("{error}\nspawner output:\n{}", output_text(&spawner_output));
    }

    let driver = Command::new(&binary)
        .arg("driver")
        .arg("--token-file")
        .arg(&token_file)
        .output()
        .unwrap_or_else(|error| panic!("failed to run driver {}: {error}", binary.display()));

    // The child writes its status only after draining; wait for it before
    // tearing down the spawner (which would otherwise hard-kill the child).
    let status_wait = wait_for_file(&status_file);
    let status_contents = std::fs::read_to_string(&status_file).unwrap_or_default();
    let spawner = stop_child(spawner);
    let _ = std::fs::remove_dir_all(&tmp);

    assert!(
        driver.status.success(),
        "driver failed:\n{}\nspawner:\n{}",
        output_text(&driver),
        output_text(&spawner)
    );
    assert!(
        status_wait.is_ok(),
        "spawned proc never recorded a clean exit:\n{}\nspawner:\n{}",
        output_text(&driver),
        output_text(&spawner)
    );
    assert!(
        status_contents.contains("STOPPED_CLEANLY"),
        "spawned proc did not stop cleanly (status file: {status_contents:?}):\n{}\nspawner:\n{}",
        output_text(&driver),
        output_text(&spawner)
    );
}

fn run_demo(mode: &str) -> DemoRun {
    let tmp = temp_dir();
    std::fs::create_dir_all(&tmp).unwrap();
    let token_file = tmp.join("spawner-token");
    let binary = remote_spawner_binary();

    let spawner = Command::new(&binary)
        .arg("spawner")
        .arg("--token-file")
        .arg(&token_file)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|error| {
            panic!("failed to spawn proc spawner {}: {error}", binary.display())
        });

    if let Err(error) = wait_for_file(&token_file) {
        let spawner_output = stop_child(spawner);
        let _ = std::fs::remove_dir_all(&tmp);
        panic!(
            "{error}\nspawner output:\n{}",
            output_text_from_parts(
                spawner_output.status,
                &spawner_output.stdout,
                &spawner_output.stderr
            )
        );
    }

    let driver = Command::new(&binary)
        .arg(mode)
        .arg("--token-file")
        .arg(&token_file)
        .output()
        .unwrap_or_else(|error| panic!("failed to run driver {}: {error}", binary.display()));

    let spawner = stop_child(spawner);
    let _ = std::fs::remove_dir_all(&tmp);

    DemoRun { driver, spawner }
}

#[cfg(not(fbcode_build))]
fn remote_spawner_binary() -> PathBuf {
    PathBuf::from(env!(
        "CARGO_BIN_EXE_hyperactor_remote_example_remote_spawner"
    ))
}

#[cfg(fbcode_build)]
fn remote_spawner_binary() -> PathBuf {
    buck_resources::get("monarch/hyperactor_remote/remote_spawner")
        .expect("remote_spawner resource not found")
        .to_path_buf()
}

fn temp_dir() -> PathBuf {
    let mut path = std::env::temp_dir();
    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    path.push(format!(
        "hyperactor_remote_spawner_test_{}_{}",
        std::process::id(),
        timestamp
    ));
    path
}

fn wait_for_file(path: &Path) -> anyhow::Result<()> {
    for _ in 0..100 {
        if path.exists() && path.metadata()?.len() > 0 {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    anyhow::bail!("timed out waiting for {}", path.display())
}

fn stop_child(mut child: Child) -> Output {
    let _ = child.kill();
    child.wait_with_output().unwrap()
}

fn output_text(output: &Output) -> String {
    output_text_from_parts(output.status, &output.stdout, &output.stderr)
}

fn output_text_from_parts(status: ExitStatus, stdout: &[u8], stderr: &[u8]) -> String {
    format!(
        "status: {status}\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(stdout),
        String::from_utf8_lossy(stderr)
    )
}
