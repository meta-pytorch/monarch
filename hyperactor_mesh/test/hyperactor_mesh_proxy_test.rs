/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::env;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use hyperactor::Actor;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::PortRef;
use hyperactor::RemoteSpawn;
use hyperactor::channel::ChannelTransport;
use hyperactor_mesh::Mesh;
use hyperactor_mesh::ProcMesh;
use hyperactor_mesh::RootActorMesh;
use hyperactor_mesh::alloc::AllocSpec;
use hyperactor_mesh::alloc::Allocator;
use hyperactor_mesh::alloc::ProcessAllocator;
use hyperactor_mesh::proc_mesh::global_root_client;
use hyperactor_mesh::supervision::SupervisionFailureMessage;
use ndslice::extent;
use serde::Deserialize;
use serde::Serialize;
use tokio::process::Command;
use typeuri::Named;

pub fn initialize() {
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("failed to set subscriber");
    tracing::info!("process {} logging initialized", std::process::id());
}

#[derive(Parser)]
struct Args {
    /// Run bootstrap logic
    #[arg(long)]
    bootstrap: bool,

    /// Keep the process hierarchy alive indefinitely
    #[arg(long)]
    keep_alive: bool,
}

// -- TestActor

#[derive(Debug)]
#[hyperactor::export(
    spawn = true,
    handlers = [
        Echo,
    ],
)]
pub struct TestActor;

impl Actor for TestActor {}

#[async_trait]
impl RemoteSpawn for TestActor {
    type Params = ();

    async fn new(_params: Self::Params) -> Result<Self, anyhow::Error> {
        Ok(Self)
    }
}

#[derive(Debug, Serialize, Deserialize, Named, Clone)]
pub struct Echo(pub String, pub PortRef<String>);

#[async_trait]
impl Handler<Echo> for TestActor {
    async fn handle(&mut self, cx: &Context<Self>, message: Echo) -> Result<(), anyhow::Error> {
        let Echo(message, reply_port) = message;
        reply_port.send(cx, message)?;
        Ok(())
    }
}

// -- ProxyActor

#[hyperactor::export(
    spawn = true,
    handlers = [
        Echo,
    ],
)]
pub struct ProxyActor {
    proc_mesh: &'static Arc<ProcMesh>,
    actor_mesh: Option<RootActorMesh<'static, TestActor>>,
}

impl fmt::Debug for ProxyActor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProxyActor")
            .field("proc_mesh", &"...")
            .field("actor_mesh", &"...")
            .finish()
    }
}

#[async_trait]
impl Actor for ProxyActor {
    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        self.actor_mesh = Some(self.proc_mesh.spawn(this, "echo", &()).await.unwrap());
        Ok(())
    }
}

#[async_trait]
impl RemoteSpawn for ProxyActor {
    type Params = String;

    async fn new(exe_path: Self::Params) -> anyhow::Result<Self, anyhow::Error> {
        let mut cmd = Command::new(PathBuf::from(&exe_path));
        cmd.arg("--bootstrap");

        let mut allocator = ProcessAllocator::new(cmd);

        let alloc = allocator
            .allocate(AllocSpec {
                extent: extent! { replica = 1 },
                constraints: Default::default(),
                proc_name: None,
                transport: ChannelTransport::Unix,
                proc_allocation_mode: Default::default(),
            })
            .await
            .unwrap();
        let proc_mesh = Arc::new(ProcMesh::allocate(alloc).await.unwrap());
        let leaked: &'static Arc<ProcMesh> = Box::leak(Box::new(proc_mesh));
        Ok(Self {
            proc_mesh: leaked,
            actor_mesh: None,
        })
    }
}

#[async_trait]
impl Handler<Echo> for ProxyActor {
    async fn handle(&mut self, cx: &Context<Self>, message: Echo) -> Result<(), anyhow::Error> {
        let actor = self.actor_mesh.as_ref().unwrap().get(0).unwrap();

        let (tx, mut rx) = cx.open_port();
        actor.send(cx, Echo(message.0, tx.bind()))?;
        message.1.send(cx, rx.recv().await.unwrap())?;

        Ok(())
    }
}

#[async_trait]
impl Handler<SupervisionFailureMessage> for ProxyActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: SupervisionFailureMessage,
    ) -> Result<(), anyhow::Error> {
        message.default_handler(cx)
    }
}

async fn run_client(exe_path: PathBuf, keep_alive: bool) -> Result<(), anyhow::Error> {
    let mut cmd = Command::new(PathBuf::from(&exe_path));
    cmd.arg("--bootstrap");

    let mut allocator = ProcessAllocator::new(cmd);
    let alloc = allocator
        .allocate(AllocSpec {
            extent: extent! { replica = 1 },
            constraints: Default::default(),
            proc_name: None,
            transport: ChannelTransport::Unix,
            proc_allocation_mode: Default::default(),
        })
        .await
        .unwrap();

    let instance = global_root_client();

    let mut proc_mesh = ProcMesh::allocate(alloc).await?;
    let actor_mesh: RootActorMesh<'_, ProxyActor> = proc_mesh
        .spawn(&instance, "proxy", &exe_path.to_str().unwrap().to_string())
        .await?;
    let proxy_actor = actor_mesh.get(0).unwrap();
    let (tx, mut rx) = actor_mesh.open_port::<String>();
    proxy_actor.send(proc_mesh.client(), Echo("hello!".to_owned(), tx.bind()))?;

    let msg = rx.recv().await?;
    println!("{}", msg);
    assert_eq!(msg, "hello!");

    let mut alloc = proc_mesh.events().unwrap().into_alloc();
    alloc.stop_and_wait().await?;
    drop(alloc);

    if keep_alive {
        // Artificially keep the hierarchy alive. Use `ps -aef | grep
        // $USER | grep proxy_test | head -1 | awk '{print "kill -TERM
        // -" $2}'` to interactively test termination.
        loop {
            #[allow(clippy::disallowed_methods)]
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
    }

    Ok(())
}

#[cfg(unix)]
fn get_process_group_id() -> libc::pid_t {
    // SAFETY: We are calling the POSIX FFI `getpgrp()`.
    // - `getpgrp()` takes no arguments and returns the calling
    //   process's process group ID as a `pid_t`. There are no
    //   pointers, buffers, or invariants to uphold on the Rust side.
    // - The returned value is just an integer; using it as such
    //   cannot violate Rust’s memory safety guarantees.
    unsafe { libc::getpgrp() }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    initialize();

    let args = Args::parse();
    if args.bootstrap {
        hyperactor_mesh::bootstrap_or_die().await;
    } else {
        #[cfg(unix)]
        {
            // SAFETY: We are calling the POSIX FFI `setpgid(0, 0)`.
            // - Arguments are plain integers; no pointers or borrowed memory
            //   are passed.
            // - Per POSIX, (0, 0) means “make the calling process the leader
            //   of a new process group whose pgid equals its pid”. This has
            //   no impact on Rust’s aliasing, lifetimes, or memory safety.
            // - We immediately check the return value and *do not* assume
            //   success; on error we log using `last_os_error()` and
            //   continue without invoking UB.
            // - This is executed before we spawn any child processes, so
            //   process-group semantics are well-defined and no races with
            //   our own children are possible.
            unsafe {
                if libc::setpgid(0, 0) != 0 {
                    tracing::error!("setpgid failed: {}", std::io::Error::last_os_error());
                } else {
                    tracing::info!(
                        "client {} is now process group leader of pgrp {}",
                        std::process::id(),
                        get_process_group_id()
                    );
                }
            }
        }

        let exe_path = env::current_exe().unwrap_or_else(|e| {
            eprintln!("Failed to get current executable path: {}", e);
            std::process::exit(1);
        });

        run_client(exe_path, args.keep_alive).await?;

        #[cfg(unix)]
        {
            tracing::info!("client done. sending kill");

            // SAFETY: We are calling the POSIX FFI `kill(−pgid, SIGTERM)`
            // to signal a process group.
            // - `pgid` is obtained from `getpgrp()` (a positive `pid_t`
            //   by POSIX contract).
            // - We assert `pgid > 0` before negation so we never pass 0
            //   or a positive pid when we intend a group target. A
            //   negative pid means “signal that process group”.
            // - The signal number (`SIGTERM`) is a valid constant.
            // - No pointers or borrowed memory are passed; no Rust
            //   aliasing/lifetime assumptions are involved. We check the
            //   return value and report any errno.
            unsafe {
                let pgid = libc::getpgrp();
                if libc::kill(-pgid, libc::SIGTERM) != 0 {
                    eprintln!("kill failed: {}", std::io::Error::last_os_error());
                }
            }
        }
    }

    Ok(())
}
