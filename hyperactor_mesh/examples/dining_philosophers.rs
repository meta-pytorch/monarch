/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/// A naive implementation of the Dining Philosophers problem using Hyperactor.
/// https://en.wikipedia.org/wiki/Dining_philosophers_problem
use std::collections::HashMap;
use std::process::ExitCode;

use anyhow::Result;
use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::Bind;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::Named;
use hyperactor::PortRef;
use hyperactor::Unbind;
use hyperactor_mesh::ProcMesh;
use hyperactor_mesh::actor_mesh::ActorMesh;
use hyperactor_mesh::alloc::AllocSpec;
use hyperactor_mesh::alloc::Allocator;
use hyperactor_mesh::alloc::LocalAllocator;
use hyperactor_mesh::comm::multicast::CastInfo;
use hyperactor_mesh::extent;
use hyperactor_mesh::selection::dsl::all;
use hyperactor_mesh::selection::dsl::true_;
use ndslice::selection::selection_from;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::OnceCell;

#[derive(Debug, Clone, PartialEq)]
enum ChopstickStatus {
    /// The chopstick is not held by anyone
    None,
    /// The chopstick is requested by someone, but not granted yet
    Requested,
    /// The chopstick is held by someone
    Granted,
}

#[derive(Debug)]
#[hyperactor::export(
    spawn = true,
    handlers = [
        PhilosopherMessage { cast = true },
    ],
)]
struct PhilosopherActor {
    /// Status of left and right chopsticks
    chopsticks: (ChopstickStatus, ChopstickStatus),
    /// Rank of the philosopher
    rank: usize,
    /// Total size of the group.
    size: usize,
    /// The waiter's port
    waiter: OnceCell<PortRef<WaiterMessage>>,
}

/// Message from the waiter to a philosopher
#[derive(Debug, Serialize, Deserialize, Named, Clone, Bind, Unbind)]
enum PhilosopherMessage {
    Start(#[binding(include)] PortRef<WaiterMessage>),
    GrantChopstick(usize),
}

/// Message from a philosopher to the waiter
#[derive(Debug, Serialize, Deserialize, Named, Clone)]
enum WaiterMessage {
    /// Request the chopsticks (rank, left, right)
    RequestChopsticks((usize, usize, usize)),
    /// Release the chopsticks (left, right)
    ReleaseChopsticks((usize, usize)),
}

#[derive(Debug, Serialize, Deserialize, Named, Clone)]
struct PhilosopherActorParams {
    /// Total size of the group.
    size: usize,
}

#[async_trait]
impl Actor for PhilosopherActor {
    type Params = PhilosopherActorParams;

    async fn new(params: Self::Params) -> Result<Self, anyhow::Error> {
        Ok(Self {
            chopsticks: (ChopstickStatus::None, ChopstickStatus::None),
            rank: 0, // will be set upon dining start
            size: params.size,
            waiter: OnceCell::new(),
        })
    }
}

impl PhilosopherActor {
    /// The indices of the left and right chopsticks.
    fn chopstick_indices(&self) -> (usize, usize) {
        let left = self.rank % self.size;
        let right = (self.rank + 1) % self.size;
        (left, right)
    }

    async fn request_chopsticks(&mut self, cx: &Instance<Self>) -> Result<()> {
        let (left, right) = self.chopstick_indices();
        self.waiter
            .get()
            .ok_or(anyhow::anyhow!("uninitialized waiter port"))?
            .send(
                cx,
                WaiterMessage::RequestChopsticks((self.rank, left, right)),
            )?;
        self.chopsticks = (ChopstickStatus::Requested, ChopstickStatus::Requested);
        Ok(())
    }

    async fn release_chopsticks(&mut self, cx: &Instance<Self>) -> Result<()> {
        let (left, right) = self.chopstick_indices();
        eprintln!(
            "philosopher {} releasing chopsticks, {} and {}",
            self.rank, left, right
        );
        self.waiter
            .get()
            .ok_or(anyhow::anyhow!("uninitialized waiter port"))?
            .send(cx, WaiterMessage::ReleaseChopsticks((left, right)))?;
        self.chopsticks = (ChopstickStatus::None, ChopstickStatus::None);
        Ok(())
    }
}

#[async_trait]
impl Handler<PhilosopherMessage> for PhilosopherActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: PhilosopherMessage,
    ) -> Result<(), anyhow::Error> {
        let point = cx.cast_point();
        self.rank = point.rank();
        match message {
            PhilosopherMessage::Start(waiter) => {
                self.waiter.set(waiter)?;
                self.request_chopsticks(cx).await?;
            }
            PhilosopherMessage::GrantChopstick(chopstick) => {
                eprintln!("philosopher {} granted chopstick {}", self.rank, chopstick);
                let (left, right) = self.chopstick_indices();
                if left == chopstick {
                    self.chopsticks = (ChopstickStatus::Granted, self.chopsticks.1.clone());
                } else if right == chopstick {
                    self.chopsticks = (self.chopsticks.0.clone(), ChopstickStatus::Granted);
                } else {
                    unreachable!("shouldn't be granted a chopstick that is not left or right");
                }
                if self.chopsticks == (ChopstickStatus::Granted, ChopstickStatus::Granted) {
                    eprintln!("philosopher {} starts dining", self.rank);
                    self.release_chopsticks(cx).await?;
                    self.request_chopsticks(cx).await?;
                }
            }
        }
        Ok(())
    }
}

struct Waiter<A> {
    /// A map from chopstick to the rank of the philosopher who holds it.
    chopstick_assignments: HashMap<usize, usize>,
    /// A map from chopstick to the rank of the philosopher who requested it.
    chopstick_requests: HashMap<usize, usize>,
    /// ActorMesh of the philosophers.
    philosophers: A,
}

impl<A> Waiter<A>
where
    A: ActorMesh<Actor = PhilosopherActor>,
{
    fn new(philosophers: A) -> Self {
        Self {
            chopstick_assignments: Default::default(),
            chopstick_requests: Default::default(),
            philosophers,
        }
    }

    fn is_chopstick_available(&self, chopstick: usize) -> bool {
        !self.chopstick_assignments.contains_key(&chopstick)
    }

    /// Grant the chopstick to the requesting philosopher.
    fn grant_chopstick(&mut self, chopstick: usize, rank: usize) {
        self.chopstick_assignments.insert(chopstick, rank);
    }

    fn handle_request_chopstick(&mut self, rank: usize, chopstick: usize) -> Result<()> {
        if self.is_chopstick_available(chopstick) {
            self.grant_chopstick(chopstick, rank);
            self.philosophers.cast(
                self.philosophers.proc_mesh().client(),
                selection_from(self.philosophers.shape(), &[("replica", rank..rank + 1)])?,
                PhilosopherMessage::GrantChopstick(chopstick),
            )?
        } else {
            self.chopstick_requests.insert(chopstick, rank);
        }
        Ok(())
    }

    fn handle_release_chopstick(&mut self, chopstick: usize) -> Result<()> {
        self.chopstick_assignments.remove(&chopstick);
        if let Some(rank) = self.chopstick_requests.remove(&chopstick) {
            // now just handle the request again to grant the chopstick
            self.handle_request_chopstick(rank, chopstick)?;
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<ExitCode> {
    let group_size = 5;
    let alloc = LocalAllocator
        .allocate(AllocSpec {
            extent: extent! {replica = group_size},
            constraints: Default::default(),
            proc_name: None,
        })
        .await?;

    let proc_mesh = ProcMesh::allocate(alloc).await?;
    let params = PhilosopherActorParams { size: group_size };
    let actor_mesh = proc_mesh
        .spawn::<PhilosopherActor>("philosopher", &params)
        .await?;
    let (dining_message_handle, mut dining_message_rx) = proc_mesh.client().open_port();
    actor_mesh
        .cast(
            proc_mesh.client(),
            all(true_()),
            PhilosopherMessage::Start(dining_message_handle.bind()),
        )
        .unwrap();
    let mut waiter = Waiter::new(actor_mesh);
    while let Ok(message) = dining_message_rx.recv().await {
        eprintln!("waiter received message: {:?}", &message);
        match message {
            WaiterMessage::RequestChopsticks((rank, left, right)) => {
                waiter.handle_request_chopstick(rank, left)?;
                waiter.handle_request_chopstick(rank, right)?;
            }
            WaiterMessage::ReleaseChopsticks((left, right)) => {
                waiter.handle_release_chopstick(left)?;
                waiter.handle_release_chopstick(right)?;
            }
        }
        let mut sorted_chopstick_assignments = waiter
            .chopstick_assignments
            .iter()
            .map(|(k, v)| (*k, *v))
            .collect::<Vec<_>>();
        sorted_chopstick_assignments.sort();
        eprintln!(
            "assignments [(CHO, PHI)]: {:?}",
            sorted_chopstick_assignments
        );
        let mut sorted_chopstick_requests = waiter
            .chopstick_requests
            .iter()
            .map(|(k, v)| (*k, *v))
            .collect::<Vec<_>>();
        sorted_chopstick_requests.sort();
        eprintln!(
            "pending requests [(CHO, PHI)]:: {:?}",
            sorted_chopstick_requests
        );
    }
    Ok(ExitCode::SUCCESS)
}
