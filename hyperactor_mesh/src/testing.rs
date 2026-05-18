/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![cfg(test)]

//! This module contains common testing utilities.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::Proc;
use hyperactor::actor::ActorError;
use hyperactor::actor::ActorErrorKind;
use hyperactor::actor::ActorStatus;
use hyperactor::actor::Signal;
use hyperactor::channel::ChannelTransport;
use hyperactor::mailbox::PortReceiver;
use hyperactor::proc::WorkCell;
use hyperactor::supervision::ActorSupervisionEvent;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::Bootstrap;
use crate::HostMeshRef;
use crate::host_mesh::HostMesh;
use crate::host_mesh::HostMeshShutdownGuard;
use crate::supervision::MeshFailure;

/// Guard that fails the test if it leaves new child processes behind.
pub struct ChildProcessGuard {
    baseline: BTreeSet<u32>,
    observed: Arc<Mutex<BTreeSet<u32>>>,
    stop_monitor: Arc<AtomicBool>,
    monitor: Option<std::thread::JoinHandle<()>>,
}

impl ChildProcessGuard {
    pub fn new() -> Self {
        let root_pid = std::process::id();
        let baseline: BTreeSet<u32> = current_descendant_processes(root_pid)
            .keys()
            .copied()
            .collect();
        let observed = Arc::new(Mutex::new(BTreeSet::new()));
        let stop_monitor = Arc::new(AtomicBool::new(false));
        let monitor_observed = Arc::clone(&observed);
        let monitor_stop = Arc::clone(&stop_monitor);
        let monitor_baseline = baseline.clone();
        let monitor = std::thread::spawn(move || {
            while !monitor_stop.load(Ordering::Acquire) {
                record_new_descendants(root_pid, &monitor_baseline, &monitor_observed);
                std::thread::sleep(Duration::from_millis(50));
            }
            record_new_descendants(root_pid, &monitor_baseline, &monitor_observed);
        });

        Self {
            baseline,
            observed,
            stop_monitor,
            monitor: Some(monitor),
        }
    }
}

impl Drop for ChildProcessGuard {
    fn drop(&mut self) {
        self.stop_monitor.store(true, Ordering::Release);
        if let Some(monitor) = self.monitor.take() {
            let _ = monitor.join();
        }

        record_new_descendants(std::process::id(), &self.baseline, &self.observed);
        let leaked: BTreeMap<_, _> = self
            .observed
            .lock()
            .expect("child process guard monitor should not panic")
            .iter()
            .filter_map(|pid| process_name(*pid).map(|name| (*pid, name)))
            .collect();
        if leaked.is_empty() {
            return;
        }

        let message = format!("test leaked child processes: {:?}", leaked);
        if std::thread::panicking() {
            eprintln!("{message}");
        } else {
            panic!("{message}");
        }
    }
}

fn record_new_descendants(
    root_pid: u32,
    baseline: &BTreeSet<u32>,
    observed: &Arc<Mutex<BTreeSet<u32>>>,
) {
    let descendants = current_descendant_processes(root_pid);
    let mut observed = observed
        .lock()
        .expect("child process guard monitor should not panic");
    observed.extend(
        descendants
            .keys()
            .copied()
            .filter(|pid| !baseline.contains(pid)),
    );
}

fn current_descendant_processes(root_pid: u32) -> BTreeMap<u32, String> {
    let mut descendants = BTreeSet::new();
    let mut pending = current_child_processes(root_pid);
    while let Some(pid) = pending.pop_first() {
        if descendants.insert(pid) {
            pending.extend(current_child_processes(pid));
        }
    }

    descendants
        .into_iter()
        .filter_map(|pid| process_name(pid).map(|name| (pid, name)))
        .collect()
}

fn current_child_processes(pid: u32) -> BTreeSet<u32> {
    let mut children = BTreeSet::new();
    let tasks_path = Path::new("/proc").join(pid.to_string()).join("task");
    let Ok(tasks) = std::fs::read_dir(tasks_path) else {
        return children;
    };
    for task in tasks.flatten() {
        let path = task.path().join("children");
        let Ok(contents) = std::fs::read_to_string(path) else {
            continue;
        };
        children.extend(
            contents
                .split_whitespace()
                .map(|pid| pid.parse::<u32>().expect("child pid should parse")),
        );
    }

    children
}

fn process_name(pid: u32) -> Option<String> {
    let proc_dir = Path::new("/proc").join(pid.to_string());
    let cmdline = std::fs::read(proc_dir.join("cmdline")).ok()?;
    let cmdline = cmdline
        .split(|byte| *byte == 0)
        .filter(|arg| !arg.is_empty())
        .map(|arg| String::from_utf8_lossy(arg))
        .collect::<Vec<_>>()
        .join(" ");
    if !cmdline.is_empty() {
        return Some(cmdline);
    }
    std::fs::read_to_string(proc_dir.join("comm"))
        .ok()
        .map(|comm| comm.trim().to_string())
}

#[derive(Debug)]
pub struct TestRootClient {
    signal_rx: PortReceiver<Signal>,
    supervision_rx: mpsc::UnboundedReceiver<ActorSupervisionEvent>,
    work_rx: mpsc::UnboundedReceiver<WorkCell<Self>>,
}

impl Actor for TestRootClient {}

#[async_trait]
impl Handler<MeshFailure> for TestRootClient {
    async fn handle(&mut self, _cx: &Context<Self>, msg: MeshFailure) -> Result<(), anyhow::Error> {
        // If a supervision failure reaches the root test client, the test has
        // failed.
        tracing::error!("got supervision event from child: {}", msg);
        panic!("got supervision event from child: {}", msg);
    }
}

impl TestRootClient {
    fn run(mut self, instance: &'static Instance<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let err = 'messages: loop {
                tokio::select! {
                    work = self.work_rx.recv() => {
                        let work = work.expect("inconsistent work queue state");
                        if let Err(err) = work.handle(&mut self, instance).await {
                            while let Ok(supervision_event) = self.supervision_rx.try_recv() {
                                if let Err(err) = instance.handle_supervision_event(&mut self, supervision_event).await {
                                    break 'messages err;
                                }
                            }
                            let kind = ActorErrorKind::processing(err);
                            break ActorError {
                                actor_id: Box::new(instance.self_addr().clone()),
                                kind: Box::new(kind),
                            };
                        }
                    }
                    _ = self.signal_rx.recv() => {
                        // TODO: do we need any signal handling for the root client?
                    }
                    Some(supervision_event) = self.supervision_rx.recv() => {
                        if let Err(err) = instance.handle_supervision_event(&mut self, supervision_event).await {
                            break err;
                        }
                    }
                };
            };
            let event = match *err.kind {
                ActorErrorKind::UnhandledSupervisionEvent(event) => *event,
                _ => {
                    let status = ActorStatus::generic_failure(err.kind.to_string());
                    ActorSupervisionEvent::new(
                        instance.self_addr().clone(),
                        Some("testclient".into()),
                        status,
                        None,
                    )
                }
            };
            instance
                .proc()
                .handle_unhandled_supervision_event(instance, event);
        })
    }
}

/// Returns a new test instance; it is initialized lazily.
pub fn fresh_instance() -> &'static Instance<TestRootClient> {
    static INSTANCE: OnceLock<Instance<TestRootClient>> = OnceLock::new();
    let proc = Proc::direct(ChannelTransport::Unix.any(), "testproc".to_string()).unwrap();
    let ai = proc.actor_instance("testclient").unwrap();
    // Use the OnceLock to get a 'static lifetime for the instance.
    INSTANCE
        .set(ai.instance)
        .map_err(|_| "already initialized root client instance")
        .unwrap();
    let instance = INSTANCE.get().unwrap();
    let client = TestRootClient {
        signal_rx: ai.signal,
        supervision_rx: ai.supervision,
        work_rx: ai.work,
    };
    client.run(instance);
    instance
}

/// Returns the singleton test instance; it is initialized lazily.
pub fn instance() -> &'static Instance<TestRootClient> {
    static INSTANCE: OnceLock<&'static Instance<TestRootClient>> = OnceLock::new();
    INSTANCE.get_or_init(fresh_instance)
}

/// Create a host mesh using multiple processes running on the test machine.
/// The transport used by the hosts is Unix channel.
///
/// # Examples
///
/// ```
/// let host_mesh = testing::host_mesh(4).await;
/// // spawn a process mesh on this host mesh with the name "test", abd per_host
/// // extent gpu = 8.
/// let proc_mesh = host_mesh
///     .spawn(instance, "test", extent!(gpu = 8), None)
///     .await
///     .unwrap();
/// // ... do something with the proc mesh ...
/// // shutdown the host mesh.
/// let _ = host_mesh.shutdown(&instance).await;
/// ```
#[cfg(fbcode_build)]
pub async fn host_mesh(n: usize) -> HostMeshShutdownGuard {
    use hyperactor::id::Label;

    use crate::mesh_id::HostMeshId;

    let program = crate::testresource::get("monarch/hyperactor_mesh/bootstrap");

    let mut host_addrs = vec![];
    for _ in 0..n {
        host_addrs.push(ChannelTransport::Unix.any());
    }

    for host in host_addrs.iter() {
        let mut cmd = Command::new(program.clone());
        let boot = Bootstrap::Host {
            addr: host.clone(),
            command: None, // use current binary
            config: None,
            exit_on_shutdown: false,
        };
        boot.to_env(&mut cmd);
        cmd.kill_on_drop(false);
        // SAFETY: Ensure the child process is killed by the kernel if the
        // parent process dies, even if the parent is SIGKILL'd. This is to
        // avoid resource leak after the test exited or crashed.
        unsafe {
            cmd.pre_exec(crate::bootstrap::install_pdeathsig_kill);
        }
        cmd.spawn().unwrap();
    }

    let host_mesh = HostMeshRef::from_hosts(
        HostMeshId::instance(Label::new("test").unwrap()),
        host_addrs,
    );
    HostMesh::take(host_mesh).shutdown_guard()
}
