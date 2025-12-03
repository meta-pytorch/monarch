/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![allow(dead_code)] // some things currently used only in tests

use std::collections::HashMap;
use std::future::Future;
use std::os::unix::process::ExitStatusExt;
use std::process::ExitStatus;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::OnceLock;

use async_trait::async_trait;
use enum_as_inner::EnumAsInner;
use hyperactor::ProcId;
use hyperactor::WorldId;
use hyperactor::channel;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::ChannelError;
use hyperactor::channel::ChannelTransport;
use hyperactor::channel::ChannelTx;
use hyperactor::channel::Rx;
use hyperactor::channel::Tx;
use hyperactor::channel::TxStatus;
use hyperactor::sync::flag;
use hyperactor::sync::monitor;
use ndslice::view::Extent;
use nix::sys::signal;
use nix::unistd::Pid;
use serde::Deserialize;
use serde::Serialize;
use tokio::io;
use tokio::process::Command;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

use super::Alloc;
use super::AllocSpec;
use super::Allocator;
use super::AllocatorError;
use super::ProcState;
use super::ProcStopReason;
use crate::assign::Ranks;
use crate::bootstrap;
use crate::bootstrap::Allocator2Process;
use crate::bootstrap::MESH_ENABLE_LOG_FORWARDING;
use crate::bootstrap::MESH_TAIL_LOG_LINES;
use crate::bootstrap::Process2Allocator;
use crate::bootstrap::Process2AllocatorMessage;
use crate::logging::OutputTarget;
use crate::logging::StreamFwder;
use crate::shortuuid::ShortUuid;

pub const CLIENT_TRACE_ID_LABEL: &str = "CLIENT_TRACE_ID";

/// An allocator that allocates procs by executing managed (local)
/// processes. ProcessAllocator is configured with a [`Command`] (template)
/// to spawn external processes. These processes must invoke [`hyperactor_mesh::bootstrap`] or
/// [`hyperactor_mesh::bootstrap_or_die`], which is responsible for coordinating
/// with the allocator.
///
/// The process allocator tees the stdout and stderr of each proc to the parent process.
pub struct ProcessAllocator {
    cmd: Arc<Mutex<Command>>,
}

impl ProcessAllocator {
    /// Create a new allocator using the provided command (template).
    /// The command is used to spawn child processes that host procs.
    /// The binary should yield control to [`hyperactor_mesh::bootstrap`]
    /// or [`hyperactor_mesh::bootstrap_or_die`] or after initialization.
    pub fn new(cmd: Command) -> Self {
        Self {
            cmd: Arc::new(Mutex::new(cmd)),
        }
    }
}

#[async_trait]
impl Allocator for ProcessAllocator {
    type Alloc = ProcessAlloc;

    #[hyperactor::instrument(fields(name = "process_allocate", monarch_client_trace_id = spec.constraints.match_labels.get(CLIENT_TRACE_ID_LABEL).cloned().unwrap_or_else(|| "".to_string())))]
    async fn allocate(&mut self, spec: AllocSpec) -> Result<ProcessAlloc, AllocatorError> {
        let (bootstrap_addr, rx) = channel::serve(ChannelAddr::any(ChannelTransport::Unix))
            .map_err(anyhow::Error::from)?;

        if spec.transport == ChannelTransport::Local {
            return Err(AllocatorError::Other(anyhow::anyhow!(
                "ProcessAllocator does not support local transport"
            )));
        }

        let name = ShortUuid::generate();
        let world_id = WorldId(name.to_string());
        tracing::info!(
            name = "ProcessAllocStatus",
            alloc_name = %world_id,
            addr = %bootstrap_addr,
            status = "Allocated",
        );
        Ok(ProcessAlloc {
            name: name.clone(),
            world_id,
            spec: spec.clone(),
            bootstrap_addr,
            rx,
            active: HashMap::new(),
            ranks: Ranks::new(spec.extent.num_ranks()),
            created: Vec::new(),
            cmd: Arc::clone(&self.cmd),
            children: JoinSet::new(),
            running: true,
            failed: false,
            client_context: ClientContext {
                trace_id: spec
                    .constraints
                    .match_labels
                    .get(CLIENT_TRACE_ID_LABEL)
                    .cloned()
                    .unwrap_or_else(|| "".to_string()),
            },
        })
    }
}

// Client Context is saved in ProcessAlloc, and is also passed in
// the RemoteProcessAllocator's Allocate method
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientContext {
    /// Trace ID for correlating logs across client and worker processes
    pub trace_id: String,
}

/// An allocation produced by [`ProcessAllocator`].
pub struct ProcessAlloc {
    name: ShortUuid,
    world_id: WorldId, // to provide storage
    spec: AllocSpec,
    bootstrap_addr: ChannelAddr,
    rx: channel::ChannelRx<Process2Allocator>,
    active: HashMap<usize, Child>,
    // Maps process index to its rank.
    ranks: Ranks<usize>,
    // Created processes by index.
    created: Vec<ShortUuid>,
    cmd: Arc<Mutex<Command>>,
    children: JoinSet<(usize, ProcStopReason)>,
    running: bool,
    failed: bool,
    client_context: ClientContext,
}

#[derive(EnumAsInner)]
enum ChannelState {
    NotConnected,
    Connected(ChannelTx<Allocator2Process>),
    Failed(ChannelError),
}

struct Child {
    local_rank: usize,
    channel: ChannelState,
    group: monitor::Group,
    exit_flag: Option<flag::Flag>,
    stdout_fwder: Arc<std::sync::Mutex<Option<StreamFwder>>>,
    stderr_fwder: Arc<std::sync::Mutex<Option<StreamFwder>>>,
    stop_reason: Arc<OnceLock<ProcStopReason>>,
    process_pid: Arc<std::sync::Mutex<Option<i32>>>,
}

impl Child {
    fn monitored(
        local_rank: usize,
        mut process: tokio::process::Child,
        log_channel: Option<ChannelAddr>,
        tail_size: usize,
    ) -> (Self, impl Future<Output = ProcStopReason>) {
        let (group, handle) = monitor::group();
        let (exit_flag, exit_guard) = flag::guarded();
        let stop_reason = Arc::new(OnceLock::new());
        let process_pid = Arc::new(std::sync::Mutex::new(process.id().map(|id| id as i32)));

        // Take ownership of the child's stdio pipes.
        //
        // NOTE:
        // - These Options are `Some(...)` **only if** the parent
        //   spawned the child with
        //   `stdout(Stdio::piped())/stderr(Stdio::piped())`, which
        //   the caller decides via its `need_stdio` calculation:
        //     need_stdio = enable_forwarding || tail_size > 0
        // - If `need_stdio == false` the parent used
        //   `Stdio::inherit()` and both will be `None`. In that case
        //   we intentionally *skip* installing `StreamFwder`s and
        //   the child writes directly to the parent's console with
        //   no interception, no tail.
        // - Even when we do install `StreamFwder`s, if `log_channel
        //   == None` (forwarding disabled) we still mirror to the
        //   parent console and keep an in-memory tail, but we don't
        //   send anything over the mesh log channel. (In the v0 path
        //   there's also no `FileAppender`.)
        let stdout_pipe = process.stdout.take();
        let stderr_pipe = process.stderr.take();

        let child = Self {
            local_rank,
            channel: ChannelState::NotConnected,
            group,
            exit_flag: Some(exit_flag),
            stdout_fwder: Arc::new(std::sync::Mutex::new(None)),
            stderr_fwder: Arc::new(std::sync::Mutex::new(None)),
            stop_reason: Arc::clone(&stop_reason),
            process_pid: process_pid.clone(),
        };

        // Set up logging monitors asynchronously without blocking process creation
        let child_stdout_fwder = child.stdout_fwder.clone();
        let child_stderr_fwder = child.stderr_fwder.clone();

        if let Some(stdout) = stdout_pipe {
            let pid = process.id().unwrap_or_default();
            let stdout_fwder = child_stdout_fwder.clone();
            let log_channel_clone = log_channel.clone();
            *stdout_fwder.lock().expect("stdout_fwder mutex poisoned") = Some(StreamFwder::start(
                stdout,
                None, // No file appender in v0.
                OutputTarget::Stdout,
                tail_size,
                log_channel_clone, // Optional channel address
                pid,
                local_rank,
            ));
        }

        if let Some(stderr) = stderr_pipe {
            let pid = process.id().unwrap_or_default();
            let stderr_fwder = child_stderr_fwder.clone();
            *stderr_fwder.lock().expect("stderr_fwder mutex poisoned") = Some(StreamFwder::start(
                stderr,
                None, // No file appender in v0.
                OutputTarget::Stderr,
                tail_size,
                log_channel, // Optional channel address
                pid,
                local_rank,
            ));
        }

        let monitor = async move {
            let reason = tokio::select! {
                _ = handle => {
                    Self::ensure_killed(process_pid);
                    Self::exit_status_to_reason(process.wait().await)
                }
                result = process.wait() => {
                    Self::exit_status_to_reason(result)
                }
            };
            exit_guard.signal();

            stop_reason.get_or_init(|| reason).clone()
        };

        (child, monitor)
    }

    fn ensure_killed(pid: Arc<std::sync::Mutex<Option<i32>>>) {
        if let Some(pid) = pid.lock().unwrap().take() {
            if let Err(e) = signal::kill(Pid::from_raw(pid), signal::SIGTERM) {
                match e {
                    nix::errno::Errno::ESRCH => {
                        // Process already gone.
                        tracing::debug!("pid {} already exited", pid);
                    }
                    _ => {
                        tracing::error!("failed to kill {}: {}", pid, e);
                    }
                }
            }
        }
    }

    fn exit_status_to_reason(result: io::Result<ExitStatus>) -> ProcStopReason {
        match result {
            Ok(status) if status.success() => ProcStopReason::Stopped,
            Ok(status) => {
                if let Some(signal) = status.signal() {
                    ProcStopReason::Killed(signal, status.core_dumped())
                } else if let Some(code) = status.code() {
                    ProcStopReason::Exited(code, String::new())
                } else {
                    ProcStopReason::Unknown
                }
            }
            Err(e) => {
                tracing::error!("error waiting for process: {}", e);
                ProcStopReason::Unknown
            }
        }
    }

    #[hyperactor::instrument_infallible]
    fn stop(&self, reason: ProcStopReason) {
        let _ = self.stop_reason.set(reason); // first stop wins
        self.group.fail();
    }

    fn connect(&mut self, addr: ChannelAddr) -> bool {
        if !self.channel.is_not_connected() {
            return false;
        }

        match channel::dial(addr) {
            Ok(channel) => {
                let mut status = channel.status().clone();
                self.channel = ChannelState::Connected(channel);
                // Monitor the channel, killing the process if it becomes unavailable
                // (fails keepalive).
                self.group.spawn(async move {
                    let _ = status
                        .wait_for(|status| matches!(status, TxStatus::Closed))
                        .await;
                    Result::<(), ()>::Err(())
                });
            }
            Err(err) => {
                self.channel = ChannelState::Failed(err);
                self.stop(ProcStopReason::Watchdog);
            }
        };
        true
    }

    fn spawn_watchdog(&mut self) {
        let Some(exit_flag) = self.exit_flag.take() else {
            tracing::info!("exit flag set, not spawning watchdog");
            return;
        };
        let group = self.group.clone();
        let stop_reason = self.stop_reason.clone();
        tracing::info!("spawning watchdog");
        tokio::spawn(async move {
            let exit_timeout =
                hyperactor_config::global::get(hyperactor::config::PROCESS_EXIT_TIMEOUT);
            #[allow(clippy::disallowed_methods)]
            if tokio::time::timeout(exit_timeout, exit_flag).await.is_err() {
                tracing::info!("watchdog timeout, killing process");
                let _ = stop_reason.set(ProcStopReason::Watchdog);
                group.fail();
            }
            tracing::info!("Watchdog task exit");
        });
    }

    #[hyperactor::instrument_infallible]
    fn post(&mut self, message: Allocator2Process) {
        if let ChannelState::Connected(channel) = &mut self.channel {
            channel.post(message);
        } else {
            self.stop(ProcStopReason::Watchdog);
        }
    }

    #[cfg(test)]
    fn fail_group(&self) {
        self.group.fail();
    }

    fn take_stream_monitors(&self) -> (Option<StreamFwder>, Option<StreamFwder>) {
        let out = self
            .stdout_fwder
            .lock()
            .expect("stdout_tailer mutex poisoned")
            .take();
        let err = self
            .stderr_fwder
            .lock()
            .expect("stderr_tailer mutex poisoned")
            .take();
        (out, err)
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        Self::ensure_killed(self.process_pid.clone());
    }
}

impl ProcessAlloc {
    // Also implement exit (for graceful exit)

    // Currently procs and processes are 1:1, so this just fully exits
    // the process.

    #[hyperactor::instrument_infallible]
    fn stop(&mut self, proc_id: &ProcId, reason: ProcStopReason) -> Result<(), anyhow::Error> {
        self.get_mut(proc_id)?.stop(reason);
        Ok(())
    }

    fn get(&self, proc_id: &ProcId) -> Result<&Child, anyhow::Error> {
        self.active.get(&self.index(proc_id)?).ok_or_else(|| {
            anyhow::anyhow!(
                "proc {} not currently active in alloc {}",
                proc_id,
                self.name
            )
        })
    }

    fn get_mut(&mut self, proc_id: &ProcId) -> Result<&mut Child, anyhow::Error> {
        self.active.get_mut(&self.index(proc_id)?).ok_or_else(|| {
            anyhow::anyhow!(
                "proc {} not currently active in alloc {}",
                &proc_id,
                self.name
            )
        })
    }

    /// The "world name" assigned to this alloc.
    pub(crate) fn name(&self) -> &ShortUuid {
        &self.name
    }

    fn index(&self, proc_id: &ProcId) -> Result<usize, anyhow::Error> {
        anyhow::ensure!(
            proc_id
                .world_name()
                .expect("proc must be ranked for allocation index")
                .parse::<ShortUuid>()?
                == self.name,
            "proc {} does not belong to alloc {}",
            proc_id,
            self.name
        );
        Ok(proc_id
            .rank()
            .expect("proc must be ranked for allocation index"))
    }

    #[hyperactor::instrument_infallible]
    async fn maybe_spawn(&mut self) -> Option<ProcState> {
        if self.active.len() >= self.spec.extent.num_ranks() {
            return None;
        }
        let mut cmd = self.cmd.lock().await;

        // In the case `MESH_ENABLE_LOG_FORWARDING` is set it's
        // probable the client execution context is a notebook. In
        // that case, for output from this process's children to
        // reach the client, we **must** use pipes and copy output
        // from child to parent (**`Stdio::inherit`** does not work!).
        // So, this variable is being used as a proxy for "use pipes"
        // here.
        let enable_forwarding = hyperactor_config::global::get(MESH_ENABLE_LOG_FORWARDING);
        let tail_size = hyperactor_config::global::get(MESH_TAIL_LOG_LINES);
        if enable_forwarding || tail_size > 0 {
            cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        } else {
            cmd.stdout(Stdio::inherit()).stderr(Stdio::inherit());
            tracing::info!(
                "child stdio NOT captured (forwarding/file_capture/tail all disabled); \
                 inheriting parent console"
            );
        }
        // Regardless of the value of `MESH_ENABLE_LOG_FORWARDING`
        // (c.f. `enable_forwarding`), we do not do log forwarding on
        // these procs. This is because, now that we are on the v1
        // path, the only procs we spawn via this code path are those
        // to support `HostMeshAgent`s.
        let log_channel: Option<ChannelAddr> = None;

        let index = self.created.len();
        self.created.push(ShortUuid::generate());
        let create_key = &self.created[index];

        cmd.env(
            bootstrap::BOOTSTRAP_ADDR_ENV,
            self.bootstrap_addr.to_string(),
        );
        cmd.env(
            bootstrap::CLIENT_TRACE_ID_ENV,
            self.client_context.trace_id.as_str(),
        );
        cmd.env(bootstrap::BOOTSTRAP_INDEX_ENV, index.to_string());

        tracing::debug!("spawning process {:?}", cmd);
        match cmd.spawn() {
            Err(err) => {
                // Likely retry won't help here so fail permanently.
                let message = format!(
                    "spawn {} index: {}, command: {:?}: {}",
                    create_key, index, cmd, err
                );
                tracing::error!(message);
                self.failed = true;
                Some(ProcState::Failed {
                    world_id: self.world_id.clone(),
                    description: message,
                })
            }
            Ok(mut process) => {
                let pid = process.id().unwrap_or(0);
                match self.ranks.assign(index) {
                    Err(_index) => {
                        tracing::info!("could not assign rank to {}", create_key);
                        let _ = process.kill().await;
                        None
                    }
                    Ok(rank) => {
                        let (handle, monitor) =
                            Child::monitored(rank, process, log_channel, tail_size);

                        // Insert into active map BEFORE spawning the monitor task
                        // This prevents a race where the monitor completes before insertion
                        self.active.insert(index, handle);

                        // Now spawn the monitor task
                        self.children.spawn(async move { (index, monitor.await) });

                        // Adjust for shape slice offset for non-zero shapes (sub-shapes).
                        let point = self.spec.extent.point_of_rank(rank).unwrap();
                        Some(ProcState::Created {
                            create_key: create_key.clone(),
                            point,
                            pid,
                        })
                    }
                }
            }
        }
    }

    fn remove(&mut self, index: usize) -> Option<Child> {
        self.ranks.unassign(index);
        self.active.remove(&index)
    }
}

#[async_trait]
impl Alloc for ProcessAlloc {
    #[hyperactor::instrument_infallible]
    async fn next(&mut self) -> Option<ProcState> {
        if !self.running && self.active.is_empty() {
            return None;
        }

        loop {
            // Do no allocate new processes if we are in failed state.
            if self.running
                && !self.failed
                && let state @ Some(_) = self.maybe_spawn().await
            {
                return state;
            }

            let transport = self.transport().clone();

            tokio::select! {
                Ok(Process2Allocator(index, message)) = self.rx.recv() => {
                    let child = match self.active.get_mut(&index) {
                        None => {
                            tracing::info!("message {:?} from zombie {}", message, index);
                            continue;
                        }
                        Some(child) => child,
                    };

                    match message {
                        Process2AllocatorMessage::Hello(addr) => {
                            if !child.connect(addr.clone()) {
                                tracing::error!("received multiple hellos from {}", index);
                                continue;
                            }

                            child.post(Allocator2Process::StartProc(
                                self.spec.proc_name.clone().map_or(
                                    ProcId::Ranked(WorldId(self.name.to_string()), index),
                                    |name| ProcId::Direct(addr.clone(), name)),
                                transport,
                            ));
                        }

                        Process2AllocatorMessage::StartedProc(proc_id, mesh_agent, addr) => {
                            break Some(ProcState::Running {
                                create_key: self.created[index].clone(),
                                proc_id,
                                mesh_agent,
                                addr,
                            });
                        }
                        Process2AllocatorMessage::Heartbeat => {
                            tracing::trace!("recv heartbeat from {index}");
                        }
                    }
                },

                Some(Ok((index, mut reason))) = self.children.join_next() => {
                    let stderr_content = if let Some(child) = self.remove(index) {
                        let mut stderr_lines = Vec::new();

                        let (stdout_mon, stderr_mon) = child.take_stream_monitors();

                        // Clean up stdout monitor
                        if let Some(stdout_monitor) = stdout_mon {
                            let (_lines, _result) = stdout_monitor.abort().await;
                            if let Err(e) = _result {
                                tracing::warn!("stdout monitor abort error: {}", e);
                            }
                        }

                        // Clean up stderr monitor and get stderr content for logging
                        if let Some(stderr_monitor) = stderr_mon {
                            let (lines, result) = stderr_monitor.abort().await;
                            stderr_lines = lines;
                            if let Err(e) = result {
                                tracing::warn!("stderr monitor abort error: {}", e);
                            }
                        }

                        stderr_lines.join("\n")
                    } else {
                        String::new()
                    };

                    if let ProcStopReason::Exited(code, _) = &mut reason {
                        reason = ProcStopReason::Exited(*code, stderr_content);
                    }

                    tracing::info!("child stopped with ProcStopReason::{:?}", reason);

                    break Some(ProcState::Stopped {
                        create_key: self.created[index].clone(),
                        reason,
                    });
                },
            }
        }
    }

    fn spec(&self) -> &AllocSpec {
        &self.spec
    }

    fn extent(&self) -> &Extent {
        &self.spec.extent
    }

    fn world_id(&self) -> &WorldId {
        &self.world_id
    }

    async fn stop(&mut self) -> Result<(), AllocatorError> {
        tracing::info!(
            name = "ProcessAllocStatus",
            alloc_name = %self.world_id(),
            status = "Stopping",
        );
        // We rely on the teardown here, and that the process should
        // exit on its own. We should have a hard timeout here as well,
        // so that we never rely on the system functioning correctly
        // for liveness.
        for (_index, child) in self.active.iter_mut() {
            child.post(Allocator2Process::StopAndExit(0));
            child.spawn_watchdog();
        }

        self.running = false;
        tracing::info!(
            name = "ProcessAllocStatus",
            alloc_name = %self.world_id(),
            status = "Stop::Sent",
            "StopAndExit was sent to allocators; check their logs for the stop progress."
        );
        Ok(())
    }
}

impl Drop for ProcessAlloc {
    fn drop(&mut self) {
        tracing::info!(
            name = "ProcessAllocStatus",
            alloc_name = %self.world_id(),
            status = "Dropped",
            "dropping ProcessAlloc of name: {}, world id: {}",
            self.name,
            self.world_id
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(fbcode_build)] // we use an external binary, produced by buck
    crate::alloc_test_suite!(ProcessAllocator::new(Command::new(
        crate::testresource::get("monarch/hyperactor_mesh/bootstrap")
    )));

    #[cfg(fbcode_build)]
    #[tokio::test]
    async fn test_sigterm_on_group_fail() {
        let bootstrap_binary = crate::testresource::get("monarch/hyperactor_mesh/bootstrap");
        let mut allocator = ProcessAllocator::new(Command::new(bootstrap_binary));

        let mut alloc = allocator
            .allocate(AllocSpec {
                extent: ndslice::extent!(replica = 1),
                constraints: Default::default(),
                proc_name: None,
                transport: ChannelTransport::Unix,
                proc_allocation_mode: Default::default(),
            })
            .await
            .unwrap();

        let proc_id = {
            loop {
                match alloc.next().await {
                    Some(ProcState::Running { proc_id, .. }) => {
                        break proc_id;
                    }
                    Some(ProcState::Failed { description, .. }) => {
                        panic!("Process allocation failed: {}", description);
                    }
                    Some(_other) => {}
                    None => {
                        panic!("Allocation ended unexpectedly");
                    }
                }
            }
        };

        if let Some(child) = alloc.active.get(
            &proc_id
                .rank()
                .expect("proc must be ranked for allocation lookup"),
        ) {
            child.fail_group();
        }

        assert!(matches!(
            alloc.next().await,
            Some(ProcState::Stopped {
                reason: ProcStopReason::Killed(15, false),
                ..
            })
        ));
    }
}
