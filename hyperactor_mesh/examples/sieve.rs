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

use anyhow::Result;
use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor::PortRef;
use hyperactor::RemoteSpawn;
use hyperactor::channel::ChannelTransport;
use hyperactor_mesh::Mesh;
use hyperactor_mesh::ProcMesh;
use hyperactor_mesh::RootActorMesh;
use hyperactor_mesh::alloc::AllocSpec;
use hyperactor_mesh::alloc::Allocator;
use hyperactor_mesh::alloc::LocalAllocator;
use hyperactor_mesh::extent;
use hyperactor_mesh::proc_mesh::global_root_client;
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
        if !msg.number.is_multiple_of(self.prime) {
            match &self.next {
                Some(next) => {
                    next.send(cx, msg)?;
                }
                None => {
                    msg.prime_collector.send(cx, msg.number)?;

                    self.next = Some(
                        SieveActor::new(SieveParams { prime: msg.number })
                            .await?
                            .spawn(cx)?,
                    );
                }
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
    async fn new(params: Self::Params) -> Result<Self> {
        Ok(Self {
            prime: params.prime,
            next: None,
        })
    }
}

#[tokio::main]
async fn main() -> Result<ExitCode> {
    let alloc = LocalAllocator
        .allocate(AllocSpec {
            extent: extent! { replica = 1 },
            constraints: Default::default(),
            proc_name: None,
            transport: ChannelTransport::Local,
            proc_allocation_mode: Default::default(),
        })
        .await?;

    let mesh = ProcMesh::allocate(alloc).await?;

    let instance = global_root_client();

    let sieve_params = SieveParams { prime: 2 };
    let sieve_mesh: RootActorMesh<SieveActor> =
        mesh.spawn(&instance, "sieve", &sieve_params).await?;
    let sieve_head = sieve_mesh.get(0).unwrap();

    let mut primes = vec![2];
    let mut candidate = 3;

    let (prime_collector_tx, mut prime_collector_rx) = mesh.client().open_port();
    let prime_collector_ref = prime_collector_tx.bind();

    while primes.len() < 100 {
        sieve_head.send(
            mesh.client(),
            NextNumber {
                number: candidate,
                prime_collector: prime_collector_ref.clone(),
            },
        )?;
        while let Ok(Some(prime)) = prime_collector_rx.try_recv() {
            primes.push(prime);
        }
        candidate += 1;
    }

    while let Ok(Some(_)) = prime_collector_rx.try_recv() {}

    primes.sort();
    println!("Primes : {:?}", primes);
    Ok(ExitCode::SUCCESS)
}
