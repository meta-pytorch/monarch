/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use clap::Subcommand;
use hyperactor::Actor;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Named;
use hyperactor::RefClient;
use hyperactor::RemoteSpawn;
use hyperactor::channel::ChannelAddr;
use hyperactor::forward;
use hyperactor::id;
use hyperactor::reference::ActorId;
use hyperactor::reference::ActorRef;
use hyperactor::reference::OncePortRef;
use hyperactor::reference::ProcId;
use hyperactor::reference::WorldId;
use hyperactor_multiprocess::proc_actor::Environment;
use hyperactor_multiprocess::proc_actor::ProcActor;
use hyperactor_multiprocess::proc_actor::spawn;
use hyperactor_multiprocess::system_actor;
use hyperactor_multiprocess::system_actor::ProcLifecycleMode;
use hyperactor_multiprocess::system_actor::Shape;
use hyperactor_multiprocess::system_actor::SystemMessageClient;
use serde::Deserialize;
use serde::Serialize;

#[derive(Subcommand)]
pub enum DemoCommand {
    /// Spawn a new proc actor.
    Proc(ProcArgs),
    /// Spawn a new world on a given world_name on a system actor
    /// listening on the given addr.
    World(WorldArgs),
    /// Spawn a new DemoActor on the proc managed by the provided proc actor.
    Spawn(SpawnArgs),
    /// Send a message to the provided DemoActor.
    Send(SendArgs),
}

impl DemoCommand {
    pub async fn run(self) -> anyhow::Result<()> {
        match self {
            DemoCommand::Proc(ProcArgs {
                system_addr,
                proc_id,
                addr,
            }) => {
                let proc_id = proc_id.unwrap_or(id!(system).random_user_proc());
                let addr = addr.unwrap_or(ChannelAddr::any(system_addr.transport()));

                let proc_actor = ProcActor::bootstrap(
                    proc_id.clone(),
                    proc_id
                        .world_id()
                        .expect("unranked proc not supported")
                        .clone(),
                    addr,
                    system_addr,
                    Duration::from_secs(5),
                    HashMap::new(),
                    ProcLifecycleMode::ManagedBySystem,
                )
                .await?
                .proc_actor;
                eprintln!("{}: joined; proc actor: {}", proc_id, proc_actor.actor_id());
                proc_actor.await;
                Ok(())
            }
            DemoCommand::World(WorldArgs {
                system_addr,
                world_id,
                num_procs,
                num_procs_per_host,
                program,
            }) => {
                let mut system = hyperactor_multiprocess::System::new(system_addr);
                let client = system.attach().await.unwrap();
                eprintln!("new world {}", world_id.clone());
                let env = match program {
                    Some(program) => Environment::Exec { program },
                    None => Environment::Local,
                };
                let shape = Shape::Definite(vec![num_procs]);
                system_actor::SYSTEM_ACTOR_REF
                    .upsert_world(
                        &client,
                        world_id,
                        shape,
                        num_procs_per_host,
                        env,
                        HashMap::new(),
                    )
                    .await?;

                Ok(())
            }
            DemoCommand::Spawn(SpawnArgs {
                system_addr,
                proc_actor_id,
            }) => {
                let mut system = hyperactor_multiprocess::System::new(system_addr);
                let client = system.attach().await.unwrap();

                let demo_actor_ref =
                    spawn::<DemoActor>(&client, &ActorRef::attest(proc_actor_id), "demo", &())
                        .await?;

                eprintln!("{}: spawned", demo_actor_ref);
                Ok(())
            }
            DemoCommand::Send(SendArgs {
                system_addr,
                demo_actor_id,
                message,
                num_sends,
            }) => {
                let mut system = hyperactor_multiprocess::System::new(system_addr);
                let client = system.attach().await.unwrap();
                let demo_actor_ref: ActorRef<DemoActor> = ActorRef::attest(demo_actor_id);

                let num_sends = num_sends.unwrap_or(1);

                for _ in 0..num_sends {
                    match message.as_str() {
                        "echo" => {
                            demo_actor_ref.echo(&client, "hello".into()).await?;
                        }
                        "increment" => {
                            demo_actor_ref.increment(&client, 1).await?;
                        }
                        "panic" => {
                            demo_actor_ref.panic(&client).await?;
                        }
                        "spawn_child" => {
                            println!("{}", demo_actor_ref.spawn_child(&client).await?);
                        }
                        "error" => {
                            match demo_actor_ref.error(&client, "error message".into()).await {
                                Ok(()) => panic!("error handler returned ok!"),
                                Err(_) => {}
                            }
                        }
                        unrecognized => {
                            Err(anyhow::anyhow!("unrecognized command {}", unrecognized))?
                        }
                    }
                }
                Ok(())
            }
        }
    }
}

#[derive(clap::Args, Debug)]
pub struct ProcArgs {
    system_addr: ChannelAddr,
    proc_id: Option<ProcId>,
    /// The address the proc actor should use to serve its mailbox.
    #[arg(short, long)]
    addr: Option<ChannelAddr>,
}

#[derive(clap::Args, Debug)]
pub struct WorldArgs {
    system_addr: ChannelAddr,
    world_id: WorldId,
    // TODO: support flexible shape.
    num_procs: usize,
    num_procs_per_host: usize,
    program: Option<String>,
}

#[derive(clap::Args, Debug)]
pub struct SpawnArgs {
    system_addr: ChannelAddr,
    proc_actor_id: ActorId,
}

#[derive(clap::Args, Debug)]
pub struct SendArgs {
    system_addr: ChannelAddr,
    demo_actor_id: ActorId,
    message: String,
    /// The number of times to send the message.
    #[arg(short, long)]
    num_sends: Option<usize>,
}

#[derive(Handler, HandleClient, RefClient, Serialize, Deserialize, Debug, Named)]
enum DemoMessage {
    Echo(String, #[reply] OncePortRef<String>),

    Increment(u64, #[reply] OncePortRef<u64>),

    Panic(),

    SpawnChild(#[reply] OncePortRef<ActorRef<DemoActor>>),

    Error(String, #[reply] OncePortRef<()>),
}

#[derive(Debug, Default)]
#[hyperactor::export(
    spawn = true,
    handlers = [
        DemoMessage,
    ],
)]
struct DemoActor;

impl Actor for DemoActor {}

#[async_trait]
#[forward(DemoMessage)]
impl DemoMessageHandler for DemoActor {
    async fn echo(
        &mut self,
        _cx: &Context<Self>,
        message: String,
    ) -> Result<String, anyhow::Error> {
        tracing::info!("demo: message: {}", message);
        Ok(message)
    }

    async fn increment(&mut self, _cx: &Context<Self>, num: u64) -> Result<u64, anyhow::Error> {
        tracing::info!("demo: increment: {}", num);
        Ok(num + 1)
    }

    async fn panic(&mut self, _cx: &Context<Self>) -> Result<(), anyhow::Error> {
        tracing::info!("demo: panic!");
        panic!()
    }

    async fn spawn_child(&mut self, cx: &Context<Self>) -> Result<ActorRef<Self>, anyhow::Error> {
        tracing::info!("demo: spawn child");
        Ok(Self.spawn(cx).await?.bind())
    }

    async fn error(&mut self, _cx: &Context<Self>, message: String) -> Result<(), anyhow::Error> {
        tracing::info!("demo: message: {}", message);
        anyhow::bail!("{}", message)
    }
}
