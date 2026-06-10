/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! RemoteSpawner remote-spawn demo.
//!
//! Run `proc` in one process and `driver` in another. The proc process starts
//! a [`RemoteSpawner`], publishes a rendezvous token, and waits. The driver joins
//! that token to obtain an [`ActorRef<RemoteSpawner>`], spawns a remote
//! [`Calculator`] actor, issues one calculation, and exits through
//! [`Instance::exit`]. Exiting the driver tears down its local supervisor tree,
//! which stops the remote calculator before the driver actor finishes. Run the
//! driver as `overflow` to show a remote calculator failure propagating back to
//! the driver.

use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorAddr;
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
use hyperactor_remote::JoinResult;
use hyperactor_remote::RemoteSpawner;
use hyperactor_remote::RemoteSpawnerEndpoint;
use hyperactor_remote::Token;
use hyperactor_remote::TokenOptions;
use hyperactor_remote::token;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

type DemoToken = Token<ActorRef<RemoteSpawner>, ActorAddr>;

#[derive(Debug)]
struct ProcArgs {
    token_file: PathBuf,
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

#[derive(Clone, Copy, Debug)]
enum DriverMode {
    Add,
    Overflow,
}

#[derive(Debug)]
struct SendCalculation {
    calculator: ActorRef<Calculator>,
    lhs: i64,
    rhs: i64,
}

#[derive(Debug)]
#[hyperactor::export(Calculate)]
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

hyperactor::register_spawnable!(Calculator);

#[derive(Debug)]
struct ProcHost {
    remote_spawner: ActorRef<RemoteSpawner>,
    token_file: PathBuf,
}

#[async_trait]
impl Actor for ProcHost {
    async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
        let token = token::create(
            this,
            self.remote_spawner.clone(),
            this.port::<token::Joined<ActorAddr>>().bind(),
            TokenOptions::default(),
        )?;
        // Pass the token out of band. In principle, hyperactor_remote does not care
        // about the specific token-passing mechanism used.
        write_text(&self.token_file, &token.to_string()).await?;
        println!("remote spawner token {}", token);
        Ok(())
    }
}

#[async_trait]
impl Handler<token::Joined<ActorAddr>> for ProcHost {
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
        println!("driver joining remote spawner token");
        self.token.join(
            this,
            this.self_addr().clone(),
            this.port::<JoinResult<ActorRef<RemoteSpawner>>>().bind(),
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
impl Handler<JoinResult<ActorRef<RemoteSpawner>>> for Driver {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: JoinResult<ActorRef<RemoteSpawner>>,
    ) -> anyhow::Result<()> {
        let remote_spawner = match message {
            JoinResult::Joined { peer } => peer,
            JoinResult::Rejected { reason } => {
                anyhow::bail!("token join rejected: {}", reason);
            }
        };
        println!(
            "driver joined remote spawner {}",
            remote_spawner.actor_addr()
        );
        let calculator = remote_spawner.spawn_uid::<Calculator>(
            cx,
            Uid::instance(Label::new("calculator").unwrap()),
            (),
        )?;
        println!("driver spawned calculator {}", calculator.actor_addr());
        let (lhs, rhs) = match self.mode {
            DriverMode::Add => (40, 2),
            DriverMode::Overflow => (i64::MAX, 1),
        };
        cx.post_after(
            cx,
            SendCalculation {
                calculator,
                lhs,
                rhs,
            },
            Duration::from_millis(250),
        );
        Ok(())
    }
}

#[async_trait]
impl Handler<SendCalculation> for Driver {
    async fn handle(&mut self, cx: &Context<Self>, message: SendCalculation) -> anyhow::Result<()> {
        println!(
            "driver issuing calculation request: {} + {}",
            message.lhs, message.rhs
        );
        message.calculator.post(
            cx,
            Calculate {
                lhs: message.lhs,
                rhs: message.rhs,
                result: cx.port::<CalculationResult>().bind(),
            },
        );
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
        Role::Proc(args) => run_proc(args).await,
        Role::Driver(args) => run_driver(args).await,
    }
}

async fn run_proc(args: ProcArgs) -> anyhow::Result<()> {
    let _serve = hyperactor::serve(ChannelAddr::any(ChannelTransport::Unix))?;
    let remote_spawner = hyperactor::spawn(RemoteSpawner);
    let host = hyperactor::spawn(ProcHost {
        remote_spawner: remote_spawner.bind::<RemoteSpawner>(),
        token_file: args.token_file,
    });
    host.await;
    println!("proc host exited");
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
    Proc(ProcArgs),
    Driver(DriverArgs),
}

fn parse_args() -> anyhow::Result<Role> {
    let mut args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() {
        anyhow::bail!("usage: remote_spawner <proc|driver|overflow> --token-file PATH");
    }
    let role = args.remove(0);
    let token_file = take_value(&mut args, "--token-file")?
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/hyperactor_remote_remote_spawner_demo.token"));
    if !args.is_empty() {
        anyhow::bail!("unknown arguments: {:?}", args);
    }
    match role.as_str() {
        "proc" => Ok(Role::Proc(ProcArgs { token_file })),
        "driver" => Ok(Role::Driver(DriverArgs {
            token_file,
            mode: DriverMode::Add,
        })),
        "overflow" => Ok(Role::Driver(DriverArgs {
            token_file,
            mode: DriverMode::Overflow,
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
