/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Based on "Parallel Sieve of Eratosthenes" from Xavier Leroy
//! and Didier Remy in ["UNIX SYSTEM PROGRAMMING IN OCAML"](https://ocaml.github.io/ocamlunix/ocamlunix.pdf).
//! This program illustrates a [Sieve of
//! Eratosthenes](https://en.wikipedia.org/wiki/Sieve_of_Eratosthenes)
//! using dynamically spawned actors to concurrently filter candidates.

use std::process::ExitCode;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor::PortRef;
use hyperactor::Proc;
use hyperactor::RemoteSpawn;
use hyperactor::channel::ChannelTransport;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor_config::Attrs;
use hyperactor_mesh::global_root_client;
use hyperactor_mesh::host_mesh::HostMesh;
use ndslice::View;
use ndslice::extent;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

/// Candidate number submitted to the sieve.
///
/// Sent into the actor chain to test `number` for primality. If found
/// to be prime, reported via `prime_collector`.
#[derive(Debug, Serialize, Deserialize, Named)]
pub struct NextNumber {
    /// Candidate number to test.
    pub number: u64,
    /// Port for reporting discovered primes.
    pub prime_collector: PortRef<u64>,
}

/// Parameters for spawning a `SieveActor`.
///
/// Carries the prime value this actor filters.
#[derive(Debug, Named, Serialize, Deserialize, Clone)]
pub struct SieveParams {
    /// Prime number assigned to this actor.
    pub prime: u64,
}

/// Actor representing one sieve filter.
///
/// Filters candidates divisible by `prime`. Forwards survivors to
/// `next`. Spawns a new child when a new prime is discovered.
#[derive(Debug)]
#[hyperactor::export(
        spawn = true,
        handlers = [
          NextNumber,
        ],
    )]
pub struct SieveActor {
    /// Prime used for filtering.
    prime: u64,
    /// Next actor in the sieve chain.
    next: Option<ActorHandle<SieveActor>>,
}

#[async_trait]
impl Handler<NextNumber> for SieveActor {
    async fn handle(&mut self, cx: &Context<Self>, msg: NextNumber) -> Result<()> {
        if msg.number.is_multiple_of(self.prime) {
            return Ok(());
        }
        match &self.next {
            Some(next) => {
                next.send(cx, msg)?;
            }
            None => {
                tracing::info!(
                    prime = self.prime,
                    discovered = msg.number,
                    "new prime discovered, spawning child"
                );
                msg.prime_collector.send(cx, msg.number)?;

                self.next = Some(
                    SieveActor::new(SieveParams { prime: msg.number }, Attrs::default())
                        .await?
                        .spawn(cx)?,
                );
            }
        }
        Ok(())
    }
}

impl Actor for SieveActor {}

#[async_trait]
impl RemoteSpawn for SieveActor {
    type Params = SieveParams;

    /// Creates a sieve actor for `prime`.
    async fn new(params: Self::Params, _environment: Attrs) -> Result<Self> {
        Ok(Self {
            prime: params.prime,
            next: None,
        })
    }
}

#[tokio::main]
async fn main() -> Result<ExitCode> {
    hyperactor::initialize_with_current_runtime();

    let mut host_mesh = HostMesh::local().await?;
    let instance = global_root_client();

    // Start the mesh admin agent.
    let admin_proc = Proc::direct(ChannelTransport::Unix.any(), "mesh_admin".to_string())?;
    let mesh_admin_addr = host_mesh.spawn_admin(instance, &admin_proc).await?;
    println!("Mesh admin server listening on http://{}", mesh_admin_addr);
    println!(
        "  - List hosts:    curl http://{}/v1/hosts",
        mesh_admin_addr
    );
    println!("  - Mesh tree:     curl http://{}/v1/tree", mesh_admin_addr);
    println!(
        "  - TUI:           buck2 run fbcode//monarch/hyperactor_mesh:hyperactor_mesh_admin_tui -- http://{}",
        mesh_admin_addr
    );
    println!();

    // TODO: put an indicatif spinner here
    println!("Starts in 5 seconds.");
    RealClock.sleep(Duration::from_secs(5)).await;
    println!("Starting...");

    let proc_mesh = host_mesh
        .spawn(instance, "sieve", extent!(replica = 1))
        .await?;

    let sieve_params = SieveParams { prime: 2 };
    let sieve_mesh: hyperactor_mesh::ActorMesh<SieveActor> =
        proc_mesh.spawn(&instance, "sieve", &sieve_params).await?;
    let sieve_head = sieve_mesh.get(0).unwrap();

    let (prime_collector_tx, mut prime_collector_rx) = instance.open_port();
    let prime_collector_ref = prime_collector_tx.bind();

    // Run the sieve computation in a spawned task so the admin HTTP
    // server remains responsive throughout.
    let compute = tokio::spawn(async move {
        // Keep the port handle alive so incoming primes can be delivered.
        let _prime_collector_tx = prime_collector_tx;
        let mut primes = vec![2u64];
        let mut candidate = 3u64;
        loop {
            while let Ok(Some(prime)) = prime_collector_rx.try_recv() {
                primes.push(prime);
            }
            if primes.len() >= 100 {
                break;
            }
            sieve_head
                .send(
                    instance,
                    NextNumber {
                        number: candidate,
                        prime_collector: prime_collector_ref.clone(),
                    },
                )
                .unwrap();
            candidate += 1;
            // Yield to the tokio runtime so it can service admin HTTP
            // requests and deliver incoming prime reports over the
            // Unix socket. `yield_now()` is insufficient — it
            // re-schedules immediately and starves the admin server.
            RealClock.sleep(Duration::from_millis(1)).await;
        }
        println!(
            "Sent {} candidates to find {} primes.",
            candidate - 3,
            primes.len()
        );
        primes.sort();
        primes
    });

    let primes = compute.await.expect("compute task panicked");

    println!("Found {} primes: {:?}", primes.len(), primes);
    println!("Press Ctrl+C to exit.");
    tokio::signal::ctrl_c().await?;

    // Clean shutdown: stop all hosts and child processes before
    // exiting so that Drop has nothing left to do and C++ static
    // destructors run in the right order.
    host_mesh.shutdown(instance).await?;

    Ok(ExitCode::SUCCESS)
}
