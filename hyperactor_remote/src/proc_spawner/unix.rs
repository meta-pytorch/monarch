/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Unix process backed proc spawning.
//!
//! [`UnixProcSpawner`] launches one OS child process per spawned proc and
//! supervises it through a per-proc `UnixProcWorker`. Inside the child, a
//! proc-local [`SpawnActor`] endpoint hosts the spawned actors. The child joins
//! its spawner over a one-shot token; the worker then links the proc and relays
//! its supervision to the caller.
//!
//! The actors (boxes) and the messages they exchange, grouped by the proc each
//! runs on. `UnixProcSpawner` and its per-proc `UnixProcWorker`s share the
//! spawner proc; the caller's `Supervisor` lives on the caller proc (possibly
//! remote); each spawned `ActorSpawner` runs in its own OS process, a child
//! proc of the spawner proc. Once linked, the caller spawns actors directly on
//! the child through its `ActorSpawner` endpoint (the `SpawnActor` arrow) — the
//! spawner's job is only to create and supervise the proc.
//!
//! ```text
//!   ┌─────────────┐   SpawnProc        ┌─────────────────┐
//!   │             │ ─────────────────▶ │ UnixProcSpawner │ ┐
//!   │             │                    └─────────────────┘ │
//!   │             │   WorkerCommand             │ spawns    │ spawner proc
//!   │  Supervisor │ ─────────────────▶ ┌────────▼────────┐ │
//!   │             │ ◀───────────────── │  UnixProcWorker │ ┘ ◀─ UnixProcExited (reaper)
//!   │             │   SupervisorEvent  └─────────────────┘   ◀─ KillProc (self)
//!   │             │                       │          ▲
//!   │             │              StopProc │          │ token::Joined<UnixProcJoin>
//!   │             │                       ▼          │ ActorSupervisionEvent
//!   │             │   SpawnActor       ┌─────────────────┐ ┐ child proc
//!   │             │ ─────────────────▶ │   ActorSpawner  │ ┘ (child of the
//!   └─────────────┘   (future)         └─────────────────┘   spawner proc)
//!     caller proc
//! ```
//!
//! - `SpawnProc`: the caller asks the spawner to create a proc.
//! - `WorkerCommand`: the caller drives a linked proc (stop, unlink).
//! - `SupervisorEvent`: the worker reports lifecycle to the caller's supervisor.
//! - `token::Joined<UnixProcJoin>`: the child joins, handing back its gateway
//!   address, [`SpawnActor`] endpoint, and control port.
//! - `StopProc`: the worker asks the child to drain and stop gracefully.
//! - `ActorSupervisionEvent`: the child forwards a top-level failure to the worker.
//! - `UnixProcExited`: the OS-process reaper task reports the child's exit status.
//! - `KillProc`: a worker self-message that hard-kills the child when a graceful
//!   stop misses the grace period.
//! - `SpawnActor`: after linking, the caller spawns actors directly on the child
//!   proc through its `ActorSpawner` endpoint.

use std::collections::HashMap;
use std::ffi::OsString;
use std::fmt;
use std::path::PathBuf;
use std::process::ExitStatus;
use std::str::FromStr;
use std::time::Duration;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::ActorRef;
use hyperactor::AnyActorHandle;
use hyperactor::Context;
use hyperactor::Endpoint;
use hyperactor::Gateway;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::Location;
use hyperactor::PortRef;
use hyperactor::Proc;
use hyperactor::ProcAddr;
use hyperactor::Uid;
use hyperactor::actor::ActorStatus;
use hyperactor::actor::StopMode;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::ChannelTransport;
use hyperactor::context;
use hyperactor::context::Actor as _;
use hyperactor::gateway::GatewayServeHandle;
use hyperactor::gateway::PeerAttachGuard;
use hyperactor::mailbox::IntoBoxedMailboxSender as _;
use hyperactor::mailbox::MailboxClient;
use hyperactor::supervision::ActorSupervisionEvent;
use nix::errno::Errno;
use nix::sys::signal;
use nix::sys::signal::Signal;
use nix::unistd::Pid;
use serde::Deserialize;
use serde::Serialize;
use tokio::process::Child;
use tokio::process::Command;
use tokio::sync::oneshot;
use typeuri::Named;

use super::ProcSpawner;
use super::actor_spawner_ref;
use super::actor_spawner_uid;
use super::spawned_proc_addr_for_spawner_addr;
use crate::ActorSpawner;
use crate::OrphanPolicy;
use crate::RemoteActorDisposition;
use crate::SpawnProc;
use crate::Supervise;
use crate::SupervisionOptions;
use crate::SupervisorEvent;
use crate::TokenOptions;
use crate::TokenPolicy;
use crate::WorkerCommand;
use crate::actor_spawner::SpawnActor;
use crate::token;

/// Environment variable containing the one-shot token a child proc uses to boot and join its spawner.
pub const UNIX_PROC_TOKEN_ENV: &str = "HYPERACTOR_REMOTE_PROC_TOKEN";

/// Token used by a Unix child proc to boot and report readiness to its spawner.
#[derive(Clone, Debug, Serialize, Deserialize, Named, PartialEq, Eq)]
pub struct UnixProcToken {
    proc_addr: ProcAddr,
    /// Port on the spawner-side worker that coordinates this proc's
    /// top-level supervision events. The child installs it as the proc's
    /// supervision coordinator so failures surface to the worker at once.
    supervisor_events: PortRef<ActorSupervisionEvent>,
    rendezvous: token::Token<(), UnixProcJoin>,
}

impl UnixProcToken {
    /// Create a Unix proc boot token.
    pub fn new(
        proc_addr: ProcAddr,
        supervisor_events: PortRef<ActorSupervisionEvent>,
        rendezvous: token::Token<(), UnixProcJoin>,
    ) -> Self {
        Self {
            proc_addr,
            supervisor_events,
            rendezvous,
        }
    }

    /// The proc address that the child should use.
    pub fn proc_addr(&self) -> &ProcAddr {
        &self.proc_addr
    }

    /// The worker port that coordinates this proc's supervision events.
    pub fn supervisor_events(&self) -> &PortRef<ActorSupervisionEvent> {
        &self.supervisor_events
    }

    fn join(
        &self,
        cx: &impl context::Actor,
        joiner: UnixProcJoin,
        result: PortRef<token::JoinResult<()>>,
    ) -> anyhow::Result<()> {
        self.rendezvous.join(cx, joiner, result)
    }
}

impl fmt::Display for UnixProcToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let token = serde_json::to_string(self).map_err(|_| fmt::Error)?;
        f.write_str(&token)
    }
}

impl FromStr for UnixProcToken {
    type Err = anyhow::Error;

    fn from_str(token: &str) -> Result<Self, Self::Err> {
        Ok(serde_json::from_str(token)?)
    }
}

/// Graceful stop request sent by the spawner to a child proc's control
/// endpoint. The child drains and stops its actor-spawn endpoint, which
/// tears the proc down cleanly so the OS process exits with a normal status.
#[derive(Clone, Debug, Serialize, Deserialize, Named, PartialEq, Eq)]
pub struct StopProc {
    mode: StopMode,
    reason: String,
}
wirevalue::register_type!(StopProc);

/// Information exchanged with a Unix child proc when it joins its spawner.
#[derive(Clone, Debug, Serialize, Deserialize, Named, PartialEq, Eq)]
pub struct UnixProcJoin {
    /// Address where the child proc accepts gateway traffic.
    pub gateway_addr: Location,
    /// The child proc's [`SpawnActor`] endpoint.
    pub actor_spawner: ActorRef<SpawnActor>,
    /// Endpoint the spawner posts a graceful [`StopProc`] to.
    pub control: PortRef<StopProc>,
}
wirevalue::register_type!(UnixProcJoin);

/// Command used by [`UnixProcSpawner`] to launch child procs.
#[derive(Clone, Debug)]
pub struct UnixProcCommand {
    program: PathBuf,
    args: Vec<OsString>,
    env: Vec<(OsString, OsString)>,
}

impl UnixProcCommand {
    /// Create a command from an executable path.
    pub fn new(program: impl Into<PathBuf>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            env: Vec::new(),
        }
    }

    /// Add one command-line argument.
    pub fn arg(mut self, arg: impl Into<OsString>) -> Self {
        self.args.push(arg.into());
        self
    }

    /// Add command-line arguments.
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<OsString>,
    {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }

    /// Add one environment variable.
    pub fn env(mut self, key: impl Into<OsString>, value: impl Into<OsString>) -> Self {
        self.env.push((key.into(), value.into()));
        self
    }

    /// Spawn the child OS process, passing `token` to it through the
    /// environment. The child runs in its own process group so a later stop can
    /// signal the whole proc tree (see [`signal_proc_group`]), and is killed if
    /// the returned handle is dropped.
    fn spawn(&self, token: &UnixProcToken) -> std::io::Result<Child> {
        let mut command = Command::new(&self.program);
        command.args(&self.args);
        for (key, value) in &self.env {
            command.env(key, value);
        }
        command.env(UNIX_PROC_TOKEN_ENV, token.to_string());
        command.kill_on_drop(true);
        // Put the child in its own process group so a stop signals the whole
        // proc tree via the negative pid, mirroring the host bootstrap launcher.
        // SAFETY: runs in the forked child before exec; only calls the
        // async-signal-safe `setpgid` with constant arguments.
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        command.spawn()
    }
}

/// Unix process backed implementation of [`ProcSpawner`].
///
/// `UnixProcSpawner` starts one OS child process per spawned proc. The child
/// process calls [`UnixProc::boot_from_env`], starts a proc-local [`SpawnActor`]
/// endpoint, serves its gateway on a Unix domain socket, and joins the one-shot
/// token supplied by the spawner. The spawner links the proc only after that
/// join, and the child process exits when the actor-spawn endpoint exits.
#[derive(Debug)]
#[hyperactor::export(SpawnProc)]
pub struct UnixProcSpawner {
    command: UnixProcCommand,
    procs: HashMap<Uid, ActorHandle<UnixProcWorker>>,
}

impl UnixProcSpawner {
    /// Create a Unix proc spawner that launches `program` for every child proc.
    pub fn new(program: impl Into<PathBuf>) -> Self {
        Self::new_with_command(UnixProcCommand::new(program))
    }

    /// Create a Unix proc spawner with a pre-built child command.
    pub fn new_with_command(command: UnixProcCommand) -> Self {
        Self {
            command,
            procs: HashMap::new(),
        }
    }
}

#[async_trait]
impl Actor for UnixProcSpawner {
    async fn handle_supervision_event(
        &mut self,
        _this: &Instance<Self>,
        event: &ActorSupervisionEvent,
    ) -> anyhow::Result<bool> {
        let exited_uid = self.procs.iter().find_map(|(uid, worker)| {
            (worker.actor_addr() == &event.actor_id).then(|| uid.clone())
        });
        if let Some(uid) = exited_uid {
            self.procs.remove(&uid);
        }
        Ok(!event.is_error())
    }

    async fn handle_stop(
        &mut self,
        this: &Instance<Self>,
        mode: StopMode,
        reason: &str,
    ) -> anyhow::Result<()> {
        // The caller-side supervision tree owns each spawned actor-spawn endpoint.
        // `UnixProcSpawner` also owns the OS child processes, so spawner shutdown must
        // stop every worker even if no individual caller has requested it.
        for worker in self.procs.values() {
            let _ = match mode {
                StopMode::Stop => worker.stop(reason),
                StopMode::DrainAndStop => worker.drain_and_stop(reason),
            };
        }
        this.close();
        match mode {
            StopMode::Stop => this.exit(reason)?,
            StopMode::DrainAndStop => this.exit_after_drain(reason)?,
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<SpawnProc> for UnixProcSpawner {
    async fn handle(&mut self, cx: &Context<Self>, message: SpawnProc) -> anyhow::Result<()> {
        if !message.uid.is_instance() {
            reject_supervise(cx, &message.supervise, "spawned procs cannot be singletons");
            return Ok(());
        }
        let existing_is_active = self.procs.get(&message.uid).is_some_and(|worker| {
            let status = worker.status();
            !status.borrow().is_terminal()
        });
        if existing_is_active {
            reject_supervise(
                cx,
                &message.supervise,
                format!("proc {} already exists", message.uid),
            );
            return Ok(());
        }
        self.procs.remove(&message.uid);

        let spawner_addr = cx.self_addr().proc_addr();
        let proc_addr = spawned_proc_addr_for_spawner_addr(&spawner_addr, &message.uid);
        let actor_spawner = actor_spawner_ref(&proc_addr);
        let worker = cx.spawn(UnixProcWorker::new(
            message.uid.clone(),
            proc_addr,
            actor_spawner,
            self.command.clone(),
            message.supervise,
        ));
        self.procs.insert(message.uid, worker);
        Ok(())
    }
}

hyperactor::assert_behaves!(UnixProcSpawner as ProcSpawner);

/// Helper for Unix child processes launched by [`UnixProcSpawner`].
pub struct UnixProc;

impl UnixProc {
    /// Boot a Unix child proc from the environment set by [`UnixProcSpawner`].
    pub async fn boot_from_env() -> anyhow::Result<ActorStatus> {
        let token = parse_env::<UnixProcToken>(UNIX_PROC_TOKEN_ENV)?;
        Self::boot(token).await
    }

    /// Boot a Unix child proc and join its spawner.
    pub async fn boot(token: UnixProcToken) -> anyhow::Result<ActorStatus> {
        // If the spawner dies, the kernel should take this proc down with it.
        let _ = install_pdeathsig_kill();
        let proc_addr = token.proc_addr().clone();
        let advertised_location = proc_addr.location().clone();
        Gateway::global().set_default_location(advertised_location.clone());
        let proc = Proc::builder().proc_id(proc_addr.id().clone()).build()?;
        // Route this proc's top-level supervision events to the spawner-side
        // worker so a failure here surfaces immediately with full detail,
        // rather than only as a process exit observed by `waitpid`.
        proc.set_supervision_coordinator(token.supervisor_events().clone())?;
        let actor_spawner_handle = proc.spawn_with_uid(actor_spawner_uid(), ActorSpawner)?;
        let actor_spawner = actor_spawner_handle.bind::<SpawnActor>();

        let (mut serve, gateway_addr) = Self::serve_own_socket(&proc)?;
        let client = proc.client("unix-proc-bootstrap");
        let (result, mut result_rx) = client.open_port::<token::JoinResult<()>>();
        let (control, mut control_rx) = client.open_port::<StopProc>();
        // `serve_own_socket` left the gateway's default location at the
        // bound socket; restore the advertised location.
        proc.set_default_location(advertised_location);
        token.join(
            &client,
            UnixProcJoin {
                gateway_addr,
                actor_spawner,
                control: control.bind(),
            },
            result.bind(),
        )?;
        match result_rx.recv().await? {
            token::JoinResult::Joined { peer: _ } => {}
            token::JoinResult::Rejected { reason } => {
                anyhow::bail!("unix proc token join rejected: {}", reason);
            }
        }

        // Run until the actor-spawn endpoint stops on its own, or the spawner
        // asks us to drain and stop it. A graceful stop drains the endpoint,
        // tearing the proc down cleanly so the process exits with a normal
        // status the spawner observes via `waitpid`.
        let mut status = actor_spawner_handle.status();
        let mut control_open = true;
        let final_status = loop {
            let current = status.borrow().clone();
            if current.is_terminal() {
                break current;
            }
            tokio::select! {
                changed = status.changed() => {
                    if changed.is_err() {
                        break status.borrow().clone();
                    }
                }
                stop = control_rx.recv(), if control_open => {
                    match stop {
                        Ok(StopProc { mode, reason }) => {
                            let _ = match mode {
                                StopMode::Stop => actor_spawner_handle.stop(&reason),
                                StopMode::DrainAndStop => actor_spawner_handle.drain_and_stop(&reason),
                            };
                        }
                        Err(_) => control_open = false,
                    }
                }
            }
        };

        let mut proc = proc;
        let _ = proc
            .destroy_and_wait(Duration::from_secs(5), "actor-spawn endpoint exited")
            .await;
        serve.stop("actor-spawn endpoint exited");
        let _ = serve.join().await;
        Ok(final_status)
    }

    /// Serve `proc`'s gateway on a fresh Unix socket of its own, returning
    /// the serve handle and the bound location to advertise back to the
    /// spawner. The child serves on its own socket — not the
    /// spawner-derived proc address — so the spawner can dial it and
    /// install a via-peer route, mirroring how `Host` dials a spawned
    /// proc's own socket. Serving sets the gateway's default location to
    /// the bound socket; the caller restores the advertised location.
    fn serve_own_socket(proc: &Proc) -> anyhow::Result<(GatewayServeHandle, Location)> {
        let serve = proc
            .gateway()
            .serve(ChannelAddr::any(ChannelTransport::Unix))?;
        // `serve` sets the gateway's default location to the freshly bound
        // socket; read it back rather than threading the address out of serve.
        let gateway_addr = proc.gateway().default_location();
        Ok((serve, gateway_addr))
    }
}

#[derive(Debug)]
struct UnixProcSession {
    session_id: Uid,
    supervisor: PortRef<SupervisorEvent>,
    options: SupervisionOptions,
    liveness_handle: AnyActorHandle,
}

/// Outcome of reaping the child OS process. A process either exits with a
/// status code or is terminated by a signal, never both, so these are
/// distinct variants rather than a pair of `Option`s.
#[derive(Clone, Debug, Serialize, Deserialize, Named)]
enum UnixProcExited {
    Exited { code: i32 },
    Signalled { signal: i32 },
    WaitFailed { error: String },
}
wirevalue::register_type!(UnixProcExited);

impl UnixProcExited {
    fn from_wait_result(result: std::io::Result<ExitStatus>) -> Self {
        let status = match result {
            Ok(status) => status,
            Err(error) => {
                return Self::WaitFailed {
                    error: error.to_string(),
                };
            }
        };
        match (status.code(), exit_signal(&status)) {
            (Some(code), None) => Self::Exited { code },
            (None, Some(signal)) => Self::Signalled { signal },
            (code, signal) => panic!(
                "unix proc wait returned an impossible status (code={code:?}, signal={signal:?})"
            ),
        }
    }

    fn actor_status(&self, stop_reason: Option<&str>) -> ActorStatus {
        match (self, stop_reason) {
            (Self::WaitFailed { error }, _) => {
                ActorStatus::generic_failure(format!("unix proc wait failed: {error}"))
            }
            // A requested stop makes any process exit a clean actor stop.
            (Self::Exited { .. } | Self::Signalled { .. }, Some(reason)) => {
                ActorStatus::Stopped(reason.to_string())
            }
            (Self::Exited { code: 0 }, None) => {
                ActorStatus::Stopped("unix proc exited".to_string())
            }
            (Self::Exited { .. } | Self::Signalled { .. }, None) => {
                ActorStatus::generic_failure(self.describe())
            }
        }
    }

    fn describe(&self) -> String {
        match self {
            Self::WaitFailed { error } => format!("wait failed: {error}"),
            Self::Exited { code } => format!("exited with code {code}"),
            Self::Signalled { signal } => match Signal::try_from(*signal) {
                Ok(sig) => format!("terminated by signal {signal} ({})", sig.as_str()),
                Err(_) => format!("terminated by signal {signal}"),
            },
        }
    }
}

#[derive(Debug)]
#[hyperactor::export(WorkerCommand, UnixProcExited, ActorSupervisionEvent)]
struct UnixProcWorker {
    uid: Uid,
    proc_addr: ProcAddr,
    actor_spawner: ActorRef<SpawnActor>,
    command: UnixProcCommand,
    supervise: Option<Supervise>,
    session: Option<UnixProcSession>,
    peer_guard: Option<PeerAttachGuard>,
    control: Option<PortRef<StopProc>>,
    pid: Option<u32>,
    stop_reason: Option<String>,
    /// Resolves when the OS-process reaper observes the child exit. Lets
    /// `handle_stop` wait for a graceful drain before force-signaling.
    child_exit: Option<oneshot::Receiver<()>>,
}

impl UnixProcWorker {
    fn new(
        uid: Uid,
        proc_addr: ProcAddr,
        actor_spawner: ActorRef<SpawnActor>,
        command: UnixProcCommand,
        supervise: Supervise,
    ) -> Self {
        Self {
            uid,
            proc_addr,
            actor_spawner,
            command,
            supervise: Some(supervise),
            session: None,
            peer_guard: None,
            control: None,
            pid: None,
            stop_reason: None,
            child_exit: None,
        }
    }

    fn launch(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
        let rendezvous = token::create(
            this,
            (),
            this.port::<token::Joined<UnixProcJoin>>().bind(),
            TokenOptions {
                policy: TokenPolicy::Once,
            },
        )?;
        let token = UnixProcToken::new(
            self.proc_addr.clone(),
            this.port::<ActorSupervisionEvent>().bind(),
            rendezvous,
        );

        let mut child = self.command.spawn(&token)?;
        self.pid = child.id();
        let notify = this.port::<UnixProcExited>();
        let client = Instance::<()>::self_client();
        let (exit_tx, exit_rx) = oneshot::channel();
        self.child_exit = Some(exit_rx);
        tokio::spawn(async move {
            let exited = UnixProcExited::from_wait_result(child.wait().await);
            // Unblock a graceful `handle_stop` waiting on the exit, then report
            // the terminal status to the worker's own port.
            let _ = exit_tx.send(());
            let _ = notify.post(client, exited);
        });
        Ok(())
    }

    /// Wait up to `grace` for the OS-process reaper to observe the child exit.
    /// Returns `true` if the child exited in time (a clean drain); `false` if
    /// the wait timed out or the reaper vanished without reporting, in which
    /// case the caller should force the process down.
    async fn await_child_exit(&mut self, grace: Duration) -> bool {
        let Some(exit) = self.child_exit.take() else {
            return true;
        };
        matches!(tokio::time::timeout(grace, exit).await, Ok(Ok(())))
    }

    fn reject_and_stop(
        &mut self,
        cx: &Context<Self>,
        supervise: &Supervise,
        reason: impl Into<String>,
    ) {
        reject_supervise(cx, supervise, reason);
        self.signal_child(Signal::SIGKILL, "rejected proc supervision");
    }

    fn signal_child(&mut self, sig: Signal, reason: &str) {
        let Some(pid) = self.pid else {
            return;
        };
        if let Err(error) = signal_proc_group(pid, sig) {
            let _ = (error, reason);
        }
    }

    fn ensure_session(&self, session_id: Uid) -> anyhow::Result<Uid> {
        let Some(session) = &self.session else {
            anyhow::bail!("unix proc worker is not supervised");
        };
        if session.session_id != session_id {
            anyhow::bail!("remote supervision session id mismatch");
        }
        Ok(session_id)
    }

    async fn link_proc(
        &mut self,
        cx: &Context<'_, Self>,
        joined: UnixProcJoin,
    ) -> anyhow::Result<()> {
        let Some(supervise) = self.supervise.take() else {
            self.signal_child(Signal::SIGKILL, "duplicate unix proc join");
            return Ok(());
        };
        if joined.actor_spawner != self.actor_spawner {
            self.reject_and_stop(
                cx,
                &supervise,
                format!(
                    "proc joined with unexpected actor-spawn endpoint {}; expected {}",
                    joined.actor_spawner.actor_addr(),
                    self.actor_spawner.actor_addr()
                ),
            );
            return Ok(());
        }

        let gateway_addr = joined.gateway_addr.addr().clone();
        let child_sender = match MailboxClient::dial(gateway_addr.clone()) {
            Ok(sender) => sender,
            Err(error) => {
                self.reject_and_stop(
                    cx,
                    &supervise,
                    format!("failed to dial spawned proc at {}: {}", gateway_addr, error),
                );
                return Ok(());
            }
        };
        let peer_guard = match cx
            .instance()
            .proc()
            .gateway()
            .attach_peer(self.uid.clone(), child_sender.into_boxed())
        {
            Ok(guard) => guard,
            Err(error) => {
                self.reject_and_stop(
                    cx,
                    &supervise,
                    format!("failed to attach spawned proc route: {error}"),
                );
                return Ok(());
            }
        };

        let liveness_handle = match supervise.liveness.clone().spawn_worker(cx).await {
            Ok(liveness_handle) => liveness_handle,
            Err(error) => {
                self.reject_and_stop(
                    cx,
                    &supervise,
                    format!("failed to spawn proc liveness actor: {error}"),
                );
                return Ok(());
            }
        };
        let supervisor = supervise.supervisor.clone();
        self.peer_guard = Some(peer_guard);
        self.control = Some(joined.control);
        self.session = Some(UnixProcSession {
            session_id: supervise.session_id.clone(),
            supervisor: supervise.supervisor,
            options: supervise.options,
            liveness_handle,
        });
        supervisor.post(
            cx,
            SupervisorEvent::Linked {
                session_id: supervise.session_id,
                worker: cx.port::<WorkerCommand>().bind(),
                child: self.actor_spawner.actor_addr().clone(),
                display_name: Some(self.uid.to_string()),
            },
        );
        Ok(())
    }

    fn handle_liveness_event(
        &mut self,
        cx: &impl context::Actor,
        event: &ActorSupervisionEvent,
    ) -> anyhow::Result<bool> {
        let Some(session) = &self.session else {
            return Ok(true);
        };
        if session.liveness_handle.actor_id() != &event.actor_id {
            return Ok(!event.is_error());
        }

        let session = self
            .session
            .take()
            .expect("liveness event must have an active session");
        let _ = session
            .liveness_handle
            .stop("proc supervision liveness failed");
        session.supervisor.post(
            cx,
            SupervisorEvent::SupervisionEvent {
                session_id: session.session_id.clone(),
                event: ActorSupervisionEvent::new(
                    self.actor_spawner.actor_addr().clone(),
                    Some(self.uid.to_string()),
                    ActorStatus::generic_failure("proc supervision liveness failed"),
                    None,
                ),
                disposition: RemoteActorDisposition::Unreachable,
            },
        );
        if session.options.orphan_policy == OrphanPolicy::Stop {
            self.signal_child(Signal::SIGTERM, "proc supervision liveness failed");
        }
        Ok(true)
    }

    fn unlink_session(&mut self, reason: &str) -> Option<UnixProcSession> {
        let session = self.session.take()?;
        let _ = session.liveness_handle.stop(reason);
        Some(session)
    }

    /// Begin stopping the child proc. When linked, posts a graceful
    /// [`StopProc`] to the child's control endpoint and schedules a hard
    /// kill after the configured grace period ([`crate::config::STOP_GRACE_PERIOD`])
    /// in case the drain stalls; otherwise signals the OS process directly. Does
    /// not exit the worker — it exits once the process is reaped (see
    /// [`Handler<UnixProcExited>`]).
    fn request_child_stop(&mut self, cx: &Context<Self>, mode: StopMode, reason: &str) {
        self.stop_reason = Some(reason.to_string());
        let Some(control) = &self.control else {
            self.signal_child(Signal::SIGTERM, reason);
            return;
        };
        control.post(
            cx,
            StopProc {
                mode,
                reason: reason.to_string(),
            },
        );
        // After the grace period, hard-kill the proc with a `KillProc`
        // self-message — a no-op if it already drained and exited, and
        // discarded outright if this worker has stopped by then.
        cx.post_after(
            cx,
            KillProc {
                reason: reason.to_string(),
            },
            hyperactor_config::global::get(crate::config::STOP_GRACE_PERIOD),
        );
    }
}

/// Self-message: hard-kill the child OS process. The worker posts this to
/// itself when a graceful stop misses the grace period
/// ([`crate::config::STOP_GRACE_PERIOD`]).
#[derive(Debug)]
struct KillProc {
    reason: String,
}

#[async_trait]
impl Actor for UnixProcWorker {
    async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
        if let Err(error) = self.launch(this)
            && let Some(supervise) = self.supervise.take()
        {
            reject_supervise(
                this,
                &supervise,
                format!("failed to launch unix proc: {error}"),
            );
            this.exit("failed to launch unix proc")?;
        }
        Ok(())
    }

    async fn handle_supervision_event(
        &mut self,
        this: &Instance<Self>,
        event: &ActorSupervisionEvent,
    ) -> anyhow::Result<bool> {
        self.handle_liveness_event(this, event)
    }

    async fn handle_stop(
        &mut self,
        this: &Instance<Self>,
        mode: StopMode,
        reason: &str,
    ) -> anyhow::Result<()> {
        self.stop_reason = Some(reason.to_string());
        let _ = self.unlink_session(reason);
        // Ask the proc to drain gracefully over the control channel and wait for
        // the OS process to exit on its own; signal it down only if the drain
        // overruns the grace period. Unlike `request_child_stop`, this path must
        // exit the worker itself, so it waits inline rather than escalating
        // through a `KillProc` self-message.
        if let Some(control) = self.control.take() {
            control.post(
                this,
                StopProc {
                    mode,
                    reason: reason.to_string(),
                },
            );
            let grace = hyperactor_config::global::get(crate::config::STOP_GRACE_PERIOD);
            if !self.await_child_exit(grace).await {
                self.signal_child(Signal::SIGTERM, reason);
            }
        } else {
            // Not linked, so there is no control channel to drain over; signal
            // the OS process directly.
            self.signal_child(Signal::SIGTERM, reason);
        }
        this.close();
        match mode {
            StopMode::Stop => this.exit(reason)?,
            StopMode::DrainAndStop => this.exit_after_drain(reason)?,
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<token::Joined<UnixProcJoin>> for UnixProcWorker {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: token::Joined<UnixProcJoin>,
    ) -> anyhow::Result<()> {
        self.link_proc(cx, message.peer).await
    }
}

#[async_trait]
impl Handler<WorkerCommand> for UnixProcWorker {
    async fn handle(&mut self, cx: &Context<Self>, message: WorkerCommand) -> anyhow::Result<()> {
        match message {
            WorkerCommand::Stop {
                session_id,
                mode,
                reason,
            } => {
                self.ensure_session(session_id)?;
                // Drain the proc gracefully over the actor channel, hard-
                // killing the OS process only if the drain misses the grace
                // period. The worker exits once the process is reaped (see
                // `Handler<UnixProcExited>`).
                self.request_child_stop(cx, mode, &reason);
            }
            WorkerCommand::Unlink { session_id, reason } => {
                self.ensure_session(session_id.clone())?;
                if let Some(session) = self.unlink_session(&reason) {
                    if session.options.orphan_policy == OrphanPolicy::Stop {
                        self.stop_reason = Some(reason.clone());
                        self.signal_child(Signal::SIGTERM, &reason);
                    }
                    let _ = session
                        .supervisor
                        .post(cx, SupervisorEvent::Unlinked { session_id, reason });
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<UnixProcExited> for UnixProcWorker {
    async fn handle(&mut self, cx: &Context<Self>, message: UnixProcExited) -> anyhow::Result<()> {
        self.pid = None;
        self.peer_guard.take();
        self.control = None;
        let actor_status = message.actor_status(self.stop_reason.as_deref());
        if let Some(session) = self.unlink_session("unix proc exited") {
            session.supervisor.post(
                cx,
                SupervisorEvent::SupervisionEvent {
                    session_id: session.session_id,
                    event: ActorSupervisionEvent::new(
                        self.actor_spawner.actor_addr().clone(),
                        Some(self.uid.to_string()),
                        actor_status,
                        None,
                    ),
                    disposition: RemoteActorDisposition::Terminal,
                },
            );
        } else if let Some(supervise) = self.supervise.take() {
            reject_supervise(
                cx,
                &supervise,
                format!("unix proc exited before joining: {}", message.describe()),
            );
        }
        cx.exit("unix proc exited")?;
        Ok(())
    }
}

#[async_trait]
impl Handler<ActorSupervisionEvent> for UnixProcWorker {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        event: ActorSupervisionEvent,
    ) -> anyhow::Result<()> {
        // A top-level supervision event forwarded by the child proc's
        // coordinator. Clean lifecycle events need no action — the proc-exit
        // path reports a clean stop. A failure is surfaced to the caller at
        // once, with full detail, then the proc is drained and stopped.
        if !event.is_error() {
            return Ok(());
        }
        if let Some(session) = self.unlink_session("child proc reported a failure") {
            session.supervisor.post(
                cx,
                SupervisorEvent::SupervisionEvent {
                    session_id: session.session_id,
                    event,
                    disposition: RemoteActorDisposition::Terminal,
                },
            );
        }
        self.request_child_stop(cx, StopMode::DrainAndStop, "child proc reported a failure");
        Ok(())
    }
}

#[async_trait]
impl Handler<KillProc> for UnixProcWorker {
    async fn handle(&mut self, _cx: &Context<Self>, message: KillProc) -> anyhow::Result<()> {
        // Hard kill: a graceful drain missed the grace period. A no-op if the
        // process already exited.
        self.signal_child(Signal::SIGKILL, &message.reason);
        Ok(())
    }
}

fn reject_supervise(cx: &impl context::Actor, supervise: &Supervise, reason: impl Into<String>) {
    let _ = supervise.supervisor.post(
        cx,
        SupervisorEvent::SuperviseRejected {
            session_id: supervise.session_id.clone(),
            reason: reason.into(),
        },
    );
}

fn parse_env<T>(key: &str) -> anyhow::Result<T>
where
    T: std::str::FromStr,
    T::Err: Into<anyhow::Error>,
{
    std::env::var(key)?
        .parse()
        .map_err(|error: T::Err| error.into())
}

/// Send `sig` to the child's process group, mirroring how the host bootstrap
/// launcher signals procs. The child is its own group leader (see `launch`), so
/// the negative pid targets the whole proc tree rather than just the leader. A
/// vanished group (`ESRCH`) is treated as success, since the goal — that group
/// being gone — is already met.
fn signal_proc_group(pid: u32, sig: Signal) -> nix::Result<()> {
    let pgid = Pid::from_raw(-(pid as i32));
    match signal::kill(pgid, sig) {
        Ok(()) | Err(Errno::ESRCH) => Ok(()),
        Err(err) => Err(err),
    }
}

/// Ask the kernel to SIGKILL this process if its parent (the spawner) dies,
/// mirroring the host bootstrap safety net. A normal stop arrives over the
/// control channel; this only guards against the spawner vanishing without one.
/// Best-effort, but if the parent already died before `prctl` ran no signal
/// will arrive, so the reparent is detected and this process exits at once.
fn install_pdeathsig_kill() -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: `getppid` takes no arguments and only reads the parent pid.
        let ppid_before = unsafe { libc::getppid() };
        // SAFETY: `prctl(PR_SET_PDEATHSIG, ...)` reads only scalar arguments.
        let rc = unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL as libc::c_int) };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: as above; re-read the parent pid to detect a reparent.
        if unsafe { libc::getppid() } != ppid_before {
            std::process::exit(0);
        }
    }
    Ok(())
}

fn exit_signal(status: &ExitStatus) -> Option<i32> {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt as _;
        status.signal()
    }
    #[cfg(not(unix))]
    {
        let _ = status;
        None
    }
}

#[cfg(test)]
mod tests {
    use hyperactor::Label;
    use hyperactor::ProcId;

    use super::*;

    #[tokio::test]
    async fn serves_on_its_own_socket_not_the_spawner_address() {
        // A child proc address derived from a spawner: it is advertised
        // through the spawner's terminal address, which the child must not
        // serve on.
        let spawner = ProcAddr::new(
            ProcId::new(Uid::instance(Label::strip("spawner")), None),
            Location::from(ChannelAddr::any(ChannelTransport::Unix)),
        );
        let proc_addr =
            spawned_proc_addr_for_spawner_addr(&spawner, &Uid::instance(Label::strip("child")));

        let proc = Proc::isolated();
        let (mut serve, gateway_addr) = UnixProc::serve_own_socket(&proc).unwrap();

        let served = gateway_addr
            .as_addr()
            .expect("child reports a terminal address, not a via route");
        assert!(
            matches!(served, ChannelAddr::Unix(_)),
            "child serves on its own unix socket, got {served}"
        );
        assert_ne!(
            served,
            proc_addr.addr(),
            "child must serve on its own socket, not the spawner-derived address"
        );

        serve.stop("test complete");
        let _ = serve.join().await;
    }

    #[test]
    fn requested_stop_reports_stopped_even_when_process_exits_by_signal() {
        let status = UnixProcExited::Signalled {
            signal: libc::SIGTERM,
        }
        .actor_status(Some("caller stopped proc"));

        assert_eq!(
            status,
            ActorStatus::Stopped("caller stopped proc".to_string())
        );
    }

    #[test]
    fn unexpected_signal_exit_reports_failure() {
        let status = UnixProcExited::Signalled {
            signal: libc::SIGTERM,
        }
        .actor_status(None);

        assert!(status.is_failed());
    }

    #[test]
    fn describe_names_the_terminating_signal() {
        let exited = UnixProcExited::Signalled {
            signal: libc::SIGTERM,
        };
        // SIGTERM is 15; `describe` looks the name up from the number.
        assert_eq!(
            exited.describe(),
            format!("terminated by signal {} (SIGTERM)", libc::SIGTERM)
        );
    }
}
