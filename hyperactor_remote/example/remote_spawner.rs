/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Remote proc and actor spawn demo.
//!
//! Run `spawner` in one process and `driver` in another. The spawner process
//! starts a [`UnixProcSpawner`], publishes a rendezvous token for its
//! [`ProcSpawner`] interface, and waits. The driver joins that token, asks the
//! spawner to spawn a Unix child proc, receives the proc's `SpawnActor`
//! interface, spawns a remote [`Calculator`] actor through that proc-local spawner, issues one
//! calculation, and exits through [`Instance::exit`]. The spawned child process
//! runs the `proc` role and exits when its actor-spawn endpoint exits. Run the
//! driver as `overflow` to show a remote calculator failure propagating back to
//! the driver.

use std::path::Path;
use std::path::PathBuf;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorAddr;
use hyperactor::ActorHandle;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::Endpoint as _;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::Label;
use hyperactor::RemoteSpawn;
use hyperactor::Uid;
use hyperactor::actor::ActorStatus;
use hyperactor::actor::StopMode;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::ChannelTransport;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_config::Flattrs;
use hyperactor_remote::ActorSpawnerEndpoint;
use hyperactor_remote::JoinResult;
use hyperactor_remote::ProcSpawner;
use hyperactor_remote::ProcSpawnerEndpoint;
use hyperactor_remote::Token;
use hyperactor_remote::TokenOptions;
use hyperactor_remote::proc_spawner::unix::UnixProc;
use hyperactor_remote::proc_spawner::unix::UnixProcCommand;
use hyperactor_remote::proc_spawner::unix::UnixProcSpawner;
use hyperactor_remote::token;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

type DemoToken = Token<ActorRef<ProcSpawner>, ActorAddr>;

#[derive(Debug)]
struct SpawnerArgs {
    token_file: PathBuf,
    /// If set, passed to each spawned child proc as `--status-file`, so the
    /// child records its final lifecycle status on a clean exit.
    proc_status_file: Option<PathBuf>,
}

#[derive(Debug)]
struct ProcArgs {
    /// If set, the child records its final lifecycle status here on a clean exit.
    status_file: Option<PathBuf>,
}

#[derive(Debug)]
struct DriverArgs {
    token_file: PathBuf,
    mode: DriverMode,
}

#[derive(Clone, Debug, Serialize, Deserialize, Named)]
struct Calculate {
    lhs: i64,
    rhs: i64,
    result: hyperactor::PortRef<CalculationResult>,
}
wirevalue::register_type!(Calculate);

#[derive(Clone, Debug, Serialize, Deserialize, Named)]
struct CalculationResult {
    lhs: i64,
    rhs: i64,
    sum: i64,
}
wirevalue::register_type!(CalculationResult);

/// Tells the calculator to crash its entire child proc (OS process exit),
/// exercising proc-death supervision rather than a recoverable actor failure.
#[derive(Clone, Debug, Serialize, Deserialize, Named)]
struct Crash;
wirevalue::register_type!(Crash);

#[derive(Clone, Copy, Debug)]
enum DriverMode {
    Add,
    Overflow,
    Crash,
}

#[derive(Debug)]
#[hyperactor::export(Calculate, Crash)]
struct Calculator;

#[async_trait]
impl Actor for Calculator {
    async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
        println!("calculator running at {}", this.self_addr());
        Ok(())
    }

    async fn handle_stop(
        &mut self,
        this: &Instance<Self>,
        mode: StopMode,
        reason: &str,
    ) -> anyhow::Result<()> {
        println!("calculator stopping before driver exits: {reason}");
        this.close();
        match mode {
            StopMode::Stop => this.exit(reason)?,
            StopMode::DrainAndStop => this.exit_after_drain(reason)?,
        }
        Ok(())
    }
}

#[async_trait]
impl RemoteSpawn for Calculator {
    type Params = ();

    async fn new(_params: (), _environment: Flattrs) -> anyhow::Result<Self> {
        Ok(Self)
    }
}

#[async_trait]
impl Handler<Calculate> for Calculator {
    async fn handle(&mut self, cx: &Context<Self>, message: Calculate) -> anyhow::Result<()> {
        let Some(sum) = message.lhs.checked_add(message.rhs) else {
            println!(
                "calculator failed: integer overflow while adding {} + {}",
                message.lhs, message.rhs
            );
            anyhow::bail!("integer overflow while adding");
        };
        println!(
            "calculator computed {} + {} = {}",
            message.lhs, message.rhs, sum
        );
        message.result.post(
            cx,
            CalculationResult {
                lhs: message.lhs,
                rhs: message.rhs,
                sum,
            },
        );
        Ok(())
    }
}

#[async_trait]
impl Handler<Crash> for Calculator {
    async fn handle(&mut self, _cx: &Context<Self>, _message: Crash) -> anyhow::Result<()> {
        println!("calculator crashing its proc via process::exit(1)");
        std::process::exit(1);
    }
}

hyperactor::register_spawnable!(Calculator);

#[derive(Debug)]
struct SpawnerBootstrap {
    token_file: PathBuf,
    proc_program: PathBuf,
    proc_status_file: Option<PathBuf>,
    proc_spawner: Option<ActorHandle<UnixProcSpawner>>,
}

#[async_trait]
impl Actor for SpawnerBootstrap {
    async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
        let mut command = UnixProcCommand::new(self.proc_program.clone())
            .arg("proc")
            .env("RUST_BACKTRACE", "1");
        if let Some(path) = &self.proc_status_file {
            command = command.arg("--status-file").arg(path.clone());
        }
        let proc_spawner = this.spawn(UnixProcSpawner::new_with_command(command));
        let token = token::create(
            this,
            proc_spawner.bind::<ProcSpawner>(),
            this.port::<token::Joined<ActorAddr>>().bind(),
            TokenOptions::default(),
        )?;
        // Pass the token out of band. In principle, hyperactor_remote does not care
        // about the specific token-passing mechanism used.
        write_text(&self.token_file, &token.to_string()).await?;
        println!("proc spawner token {}", token);
        self.proc_spawner = Some(proc_spawner);
        Ok(())
    }

    async fn handle_stop(
        &mut self,
        this: &Instance<Self>,
        mode: StopMode,
        reason: &str,
    ) -> anyhow::Result<()> {
        if let Some(proc_spawner) = &self.proc_spawner {
            let _ = match mode {
                StopMode::Stop => proc_spawner.stop(reason),
                StopMode::DrainAndStop => proc_spawner.drain_and_stop(reason),
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
impl Handler<token::Joined<ActorAddr>> for SpawnerBootstrap {
    async fn handle(
        &mut self,
        _cx: &Context<Self>,
        message: token::Joined<ActorAddr>,
    ) -> anyhow::Result<()> {
        println!("driver joined from {}", message.peer);
        Ok(())
    }
}

#[derive(Debug)]
struct Driver {
    token: DemoToken,
    mode: DriverMode,
}

#[async_trait]
impl Actor for Driver {
    async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
        println!("driver joining proc spawner token");
        self.token.join(
            this,
            this.self_addr().clone(),
            this.port::<JoinResult<ActorRef<ProcSpawner>>>().bind(),
        )?;
        Ok(())
    }

    async fn handle_supervision_event(
        &mut self,
        _this: &Instance<Self>,
        event: &ActorSupervisionEvent,
    ) -> anyhow::Result<bool> {
        println!("driver observed supervision event and will propagate it: {event}");
        Ok(false)
    }
}

#[async_trait]
impl Handler<JoinResult<ActorRef<ProcSpawner>>> for Driver {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: JoinResult<ActorRef<ProcSpawner>>,
    ) -> anyhow::Result<()> {
        let proc_spawner = match message {
            JoinResult::Joined { peer } => peer,
            JoinResult::Rejected { reason } => {
                anyhow::bail!("token join rejected: {}", reason);
            }
        };
        println!("driver joined proc spawner {}", proc_spawner.actor_addr());
        // Spawn the proc and wait for its actor-spawn endpoint to become
        // reachable before spawning on it. The readiness port carries no
        // payload — we already hold the endpoint ref returned synchronously.
        let (proc_ready, proc_ready_rx) = cx.open_once_port::<()>();
        let actor_spawner = proc_spawner.spawn_proc_uid_with_ready(
            cx,
            Uid::instance(Label::new("calculator-proc").unwrap()),
            proc_ready,
        )?;
        println!("driver requested proc {}", actor_spawner.actor_addr());
        proc_ready_rx.recv().await?;

        // The proc booted and joined: its actor-spawn endpoint now routes, so
        // spawn the calculator and wait for its own readiness signal.
        let (calc_ready, calc_ready_rx) = cx.open_once_port::<()>();
        let calculator = actor_spawner.spawn_uid_with_ready::<Calculator>(
            cx,
            Uid::instance(Label::new("calculator").unwrap()),
            (),
            calc_ready,
        )?;
        println!("driver spawned calculator {}", calculator.actor_addr());
        calc_ready_rx.recv().await?;

        // The calculator linked and is now reachable: drive the scenario.
        match self.mode {
            DriverMode::Add => {
                println!("driver issuing calculation request: 40 + 2");
                calculator.post(
                    cx,
                    Calculate {
                        lhs: 40,
                        rhs: 2,
                        result: cx.port::<CalculationResult>().bind(),
                    },
                );
            }
            DriverMode::Overflow => {
                println!("driver issuing calculation request: {} + 1", i64::MAX);
                calculator.post(
                    cx,
                    Calculate {
                        lhs: i64::MAX,
                        rhs: 1,
                        result: cx.port::<CalculationResult>().bind(),
                    },
                );
            }
            DriverMode::Crash => {
                println!("driver telling calculator to crash its proc");
                calculator.post(cx, Crash);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<CalculationResult> for Driver {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        result: CalculationResult,
    ) -> anyhow::Result<()> {
        println!(
            "driver received result: {} + {} = {}",
            result.lhs, result.rhs, result.sum
        );
        println!("driver exiting through this.exit(); calculator should stop first");
        cx.exit("demo complete")?;
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let role = parse_args()?;
    match role {
        Role::Spawner(args) => run_spawner(args).await,
        Role::Driver(args) => run_driver(args).await,
        Role::Proc(args) => run_proc(args).await,
    }
}

async fn run_spawner(args: SpawnerArgs) -> anyhow::Result<()> {
    let _serve = hyperactor::serve(ChannelAddr::any(ChannelTransport::Unix))?;
    let spawner = hyperactor::spawn(SpawnerBootstrap {
        token_file: args.token_file,
        proc_program: std::env::current_exe()?,
        proc_status_file: args.proc_status_file,
        proc_spawner: None,
    });
    spawner.await;
    println!("demo proc spawner exited");
    Ok(())
}

async fn run_proc(args: ProcArgs) -> anyhow::Result<()> {
    let status = UnixProc::boot_from_env().await?;
    println!("demo proc exited after proc-like actor status: {status}");
    if let Some(path) = args.status_file {
        // Record whether the proc came down via a clean drain (graceful stop)
        // vs. some other terminal state, so tests can assert on it.
        let marker = match &status {
            ActorStatus::Stopped(_) => format!("STOPPED_CLEANLY: {status}"),
            other => format!("NOT_CLEAN: {other}"),
        };
        tokio::fs::write(&path, marker).await?;
    }
    Ok(())
}

async fn run_driver(args: DriverArgs) -> anyhow::Result<()> {
    let token = read_token(&args.token_file).await?;
    let _serve = hyperactor::serve(ChannelAddr::any(ChannelTransport::Unix))?;
    let mode = args.mode;
    let driver = hyperactor::spawn(Driver { token, mode });
    let status = driver.await;
    match (mode, status) {
        (DriverMode::Add, ActorStatus::Stopped(_)) => {
            println!("driver actor exited after remote calculator teardown");
            Ok(())
        }
        (DriverMode::Overflow, ActorStatus::Failed(error)) => {
            println!("driver failed after propagated calculator failure: {error}");
            anyhow::bail!("overflow demo failed as expected")
        }
        (DriverMode::Crash, ActorStatus::Failed(error)) => {
            println!("driver failed after observing proc death: {error}");
            anyhow::bail!("crash demo failed as expected")
        }
        (mode, status) => {
            anyhow::bail!("driver exited unexpectedly in {:?} mode: {}", mode, status)
        }
    }
}

async fn read_token(path: &Path) -> anyhow::Result<DemoToken> {
    let token = tokio::fs::read_to_string(path).await?;
    token.trim().parse()
}

async fn write_text(path: &Path, text: &str) -> anyhow::Result<()> {
    tokio::fs::write(path, format!("{text}\n")).await?;
    Ok(())
}

#[derive(Debug)]
enum Role {
    Spawner(SpawnerArgs),
    Driver(DriverArgs),
    Proc(ProcArgs),
}

fn parse_args() -> anyhow::Result<Role> {
    let mut args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() {
        anyhow::bail!(
            "usage: remote_spawner <spawner|driver|overflow|crash|proc> --token-file PATH"
        );
    }
    let role = args.remove(0);
    let token_file = take_value(&mut args, "--token-file")?
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/hyperactor_remote_remote_spawner_demo.token"));
    let proc_status_file = take_value(&mut args, "--proc-status-file")?.map(PathBuf::from);
    let status_file = take_value(&mut args, "--status-file")?.map(PathBuf::from);
    if !args.is_empty() {
        anyhow::bail!("unknown arguments: {:?}", args);
    }
    match role.as_str() {
        "spawner" => Ok(Role::Spawner(SpawnerArgs {
            token_file,
            proc_status_file,
        })),
        "proc" => Ok(Role::Proc(ProcArgs { status_file })),
        "driver" => Ok(Role::Driver(DriverArgs {
            token_file,
            mode: DriverMode::Add,
        })),
        "overflow" => Ok(Role::Driver(DriverArgs {
            token_file,
            mode: DriverMode::Overflow,
        })),
        "crash" => Ok(Role::Driver(DriverArgs {
            token_file,
            mode: DriverMode::Crash,
        })),
        _ => anyhow::bail!("unknown role: {}", role),
    }
}

fn take_value(args: &mut Vec<String>, flag: &str) -> anyhow::Result<Option<String>> {
    if let Some(index) = args.iter().position(|arg| arg == flag) {
        args.remove(index);
        if index >= args.len() {
            anyhow::bail!("{} requires a value", flag);
        }
        Ok(Some(args.remove(index)))
    } else {
        Ok(None)
    }
}
