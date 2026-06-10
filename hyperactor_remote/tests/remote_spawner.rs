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
    proc: Output,
}

#[test]
fn test_remote_spawner_demo_add() {
    let run = run_demo("driver");

    assert!(
        run.driver.status.success(),
        "driver failed:\n{}\nproc:\n{}",
        output_text(&run.driver),
        output_text(&run.proc)
    );
    assert!(
        output_text(&run.driver).contains("driver received result: 40 + 2 = 42"),
        "driver output did not include calculation result:\n{}\nproc:\n{}",
        output_text(&run.driver),
        output_text(&run.proc)
    );
}

#[test]
fn test_remote_spawner_demo_overflow() {
    let run = run_demo("overflow");

    assert!(
        !run.driver.status.success(),
        "overflow driver unexpectedly succeeded:\n{}\nproc:\n{}",
        output_text(&run.driver),
        output_text(&run.proc)
    );
    let driver_output = output_text(&run.driver);
    assert!(
        driver_output.contains("driver observed supervision event and will propagate it"),
        "driver output did not include propagated supervision event:\n{}\nproc:\n{}",
        driver_output,
        output_text(&run.proc)
    );
    assert!(
        driver_output.contains("integer overflow while adding"),
        "driver output did not include expected overflow failure:\n{}\nproc:\n{}",
        driver_output,
        output_text(&run.proc)
    );
}

fn run_demo(mode: &str) -> DemoRun {
    let tmp = temp_dir();
    std::fs::create_dir_all(&tmp).unwrap();
    let token_file = tmp.join("proc-token");
    let binary = remote_spawner_binary();

    let proc = Command::new(&binary)
        .arg("proc")
        .arg("--token-file")
        .arg(&token_file)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|error| panic!("failed to spawn proc {}: {error}", binary.display()));

    if let Err(error) = wait_for_file(&token_file) {
        let proc_output = stop_child(proc);
        let _ = std::fs::remove_dir_all(&tmp);
        panic!(
            "{error}\nproc output:\n{}",
            output_text_from_parts(proc_output.status, &proc_output.stdout, &proc_output.stderr)
        );
    }

    let driver = Command::new(&binary)
        .arg(mode)
        .arg("--token-file")
        .arg(&token_file)
        .output()
        .unwrap_or_else(|error| panic!("failed to run driver {}: {error}", binary.display()));

    let proc = stop_child(proc);
    let _ = std::fs::remove_dir_all(&tmp);

    DemoRun { driver, proc }
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
