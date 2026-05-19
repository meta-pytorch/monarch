/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Token-based remote supervision demo.
//!
//! Run `parent` in one process and `joiner` in another. The parent creates a
//! rendezvous token and writes it to a file. The joiner reads that token,
//! exchanges its `WorkerLike` ref with the parent, and the parent links the
//! worker through the remote supervision actors.

use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorAddr;
use hyperactor::ActorHandle;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::Proc;
use hyperactor::actor::StopMode;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::ChannelTransport;
use hyperactor::channel::TcpMode;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_remote::KeepaliveLink;
use hyperactor_remote::LinkOptions;
use hyperactor_remote::Supervisor;
use hyperactor_remote::Token;
use hyperactor_remote::TokenOptions;
use hyperactor_remote::Worker;
use hyperactor_remote::WorkerLike;
use hyperactor_remote::token;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

type DemoToken = Token<ActorAddr, ActorRef<WorkerLike>>;

#[derive(Debug)]
struct ParentArgs {
    token_file: PathBuf,
    ready_file: Option<PathBuf>,
    event_file: Option<PathBuf>,
    exit_after_link: Option<Duration>,
    keepalive_interval: Duration,
    keepalive_timeout: Duration,
}

#[derive(Debug)]
struct JoinerArgs {
    token_file: PathBuf,
    ready_file: Option<PathBuf>,
    stopped_file: Option<PathBuf>,
    exited_file: Option<PathBuf>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Named)]
enum ParentCommand {
    Exit,
}
wirevalue::register_type!(ParentCommand);

#[derive(Debug)]
struct Parent {
    token_file: PathBuf,
    ready_file: Option<PathBuf>,
    event_file: Option<PathBuf>,
    exit_after_link: Option<Duration>,
    keepalive_interval: Duration,
    keepalive_timeout: Duration,
    supervisor: Option<ActorHandle<Supervisor>>,
}

#[async_trait]
impl Actor for Parent {
    async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
        let token = token::create(
            this,
            this.self_addr().clone(),
            this.port::<token::Joined<ActorRef<WorkerLike>>>().bind(),
            TokenOptions::default(),
        )?;
        write_text(&self.token_file, &token.to_string()).await?;
        println!("parent token {}", token);
        Ok(())
    }

    async fn handle_supervision_event(
        &mut self,
        this: &Instance<Self>,
        event: &ActorSupervisionEvent,
    ) -> anyhow::Result<bool> {
        let text = format!("parent observed supervision event: {event}");
        println!("{text}");
        if let Some(path) = &self.event_file {
            write_text(path, &text).await?;
        }
        this.exit("remote supervision event observed")?;
        Ok(true)
    }
}

#[async_trait]
impl Handler<token::Joined<ActorRef<WorkerLike>>> for Parent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: token::Joined<ActorRef<WorkerLike>>,
    ) -> anyhow::Result<()> {
        println!("parent joined by worker {}", message.peer.actor_addr());
        let supervisor = cx.spawn(Supervisor::new(
            message.peer,
            KeepaliveLink::new(self.keepalive_interval, self.keepalive_timeout),
            LinkOptions::default(),
        ))?;
        self.supervisor = Some(supervisor);
        if let Some(path) = &self.ready_file {
            write_text(path, "linked").await?;
        }
        if let Some(delay) = self.exit_after_link {
            cx.self_message_with_delay(ParentCommand::Exit, delay)?;
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<ParentCommand> for Parent {
    async fn handle(&mut self, cx: &Context<Self>, message: ParentCommand) -> anyhow::Result<()> {
        match message {
            ParentCommand::Exit => {
                println!("parent exiting");
                cx.drain_and_stop("demo parent exit")?;
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct DemoChild {
    stopped_file: Option<PathBuf>,
}

#[async_trait]
impl Actor for DemoChild {
    async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
        println!("child running at {}", this.self_addr());
        Ok(())
    }

    async fn handle_stop(
        &mut self,
        this: &Instance<Self>,
        mode: StopMode,
        reason: &str,
    ) -> anyhow::Result<()> {
        println!("child stopping: {reason}");
        if let Some(path) = &self.stopped_file {
            write_text(path, reason).await?;
        }
        this.close();
        match mode {
            StopMode::Stop => this.exit(reason)?,
            StopMode::DrainAndStop => this.exit_after_drain(reason)?,
        }
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let role = parse_args()?;
    match role {
        Role::Parent(args) => run_parent(args).await,
        Role::Joiner(args) => run_joiner(args).await,
    }
}

async fn run_parent(args: ParentArgs) -> anyhow::Result<()> {
    let proc = Proc::direct(
        ChannelAddr::any(ChannelTransport::Tcp(TcpMode::Localhost)),
        "token_supervision_parent".to_string(),
    )?;
    let parent = proc.spawn(
        "parent",
        Parent {
            token_file: args.token_file,
            ready_file: args.ready_file,
            event_file: args.event_file,
            exit_after_link: args.exit_after_link,
            keepalive_interval: args.keepalive_interval,
            keepalive_timeout: args.keepalive_timeout,
            supervisor: None,
        },
    )?;
    parent.await;
    println!("parent process exiting");
    Ok(())
}

async fn run_joiner(args: JoinerArgs) -> anyhow::Result<()> {
    let token = read_token(&args.token_file).await?;
    let proc = Proc::direct(
        ChannelAddr::any(ChannelTransport::Tcp(TcpMode::Localhost)),
        "token_supervision_joiner".to_string(),
    )?;
    let joiner = proc.client("joiner");
    let worker = proc.spawn(
        "worker",
        Worker::new(DemoChild {
            stopped_file: args.stopped_file,
        }),
    )?;
    let (result_port, mut result_rx) = joiner.open_port::<token::JoinResult<ActorAddr>>();

    token.join(&joiner, worker.bind::<WorkerLike>(), result_port.bind())?;
    match result_rx.recv().await? {
        token::JoinResult::Joined { peer } => {
            println!("joiner linked to parent {peer}");
            if let Some(path) = &args.ready_file {
                write_text(path, "joined").await?;
            }
        }
        token::JoinResult::Rejected { reason } => {
            anyhow::bail!("token join rejected: {}", reason);
        }
    }

    worker.await;
    if let Some(path) = &args.exited_file {
        write_text(path, "exited").await?;
    }
    println!("joiner process exiting");
    Ok(())
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
    Parent(ParentArgs),
    Joiner(JoinerArgs),
}

fn parse_args() -> anyhow::Result<Role> {
    let mut args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() {
        anyhow::bail!("usage: token_supervision <parent|joiner> [options]");
    }
    let role = args.remove(0);
    match role.as_str() {
        "parent" => Ok(Role::Parent(parse_parent_args(args)?)),
        "joiner" => Ok(Role::Joiner(parse_joiner_args(args)?)),
        _ => anyhow::bail!("unknown role {}", role),
    }
}

fn parse_parent_args(args: Vec<String>) -> anyhow::Result<ParentArgs> {
    let mut parser = ArgParser::new(args);
    let token_file = parser.required_path("--token-file")?;
    let ready_file = parser.optional_path("--ready-file")?;
    let event_file = parser.optional_path("--event-file")?;
    let exit_after_link = parser
        .optional_u64("--exit-after-link-ms")?
        .map(Duration::from_millis);
    let keepalive_interval =
        Duration::from_millis(parser.optional_u64("--keepalive-ms")?.unwrap_or(100));
    let keepalive_timeout =
        Duration::from_millis(parser.optional_u64("--timeout-ms")?.unwrap_or(300));
    parser.finish()?;
    Ok(ParentArgs {
        token_file,
        ready_file,
        event_file,
        exit_after_link,
        keepalive_interval,
        keepalive_timeout,
    })
}

fn parse_joiner_args(args: Vec<String>) -> anyhow::Result<JoinerArgs> {
    let mut parser = ArgParser::new(args);
    let token_file = parser.required_path("--token-file")?;
    let ready_file = parser.optional_path("--ready-file")?;
    let stopped_file = parser.optional_path("--stopped-file")?;
    let exited_file = parser.optional_path("--exited-file")?;
    parser.finish()?;
    Ok(JoinerArgs {
        token_file,
        ready_file,
        stopped_file,
        exited_file,
    })
}

struct ArgParser {
    args: Vec<String>,
}

impl ArgParser {
    fn new(args: Vec<String>) -> Self {
        Self { args }
    }

    fn required_path(&mut self, flag: &str) -> anyhow::Result<PathBuf> {
        self.optional_path(flag)?
            .ok_or_else(|| anyhow::anyhow!("missing required argument {}", flag))
    }

    fn optional_path(&mut self, flag: &str) -> anyhow::Result<Option<PathBuf>> {
        Ok(self.optional_string(flag)?.map(PathBuf::from))
    }

    fn optional_u64(&mut self, flag: &str) -> anyhow::Result<Option<u64>> {
        self.optional_string(flag)?
            .map(|value| value.parse().map_err(Into::into))
            .transpose()
    }

    fn optional_string(&mut self, flag: &str) -> anyhow::Result<Option<String>> {
        let Some(index) = self.args.iter().position(|arg| arg == flag) else {
            return Ok(None);
        };
        self.args.remove(index);
        if index >= self.args.len() {
            anyhow::bail!("missing value for {}", flag);
        }
        Ok(Some(self.args.remove(index)))
    }

    fn finish(self) -> anyhow::Result<()> {
        if self.args.is_empty() {
            return Ok(());
        }
        anyhow::bail!("unknown arguments: {}", self.args.join(" "))
    }
}
