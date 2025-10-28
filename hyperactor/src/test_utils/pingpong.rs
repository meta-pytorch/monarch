/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use serde::Serialize;

use crate as hyperactor; // for macros
use crate::Actor;
use crate::ActorRef;
use crate::Context;
use crate::Handler;
use crate::Instance;
use crate::Named;
use crate::OncePortRef;
use crate::PortRef;
use crate::clock::Clock;
use crate::clock::RealClock;
use crate::mailbox::MessageEnvelope;
use crate::mailbox::Undeliverable;
use crate::mailbox::UndeliverableMessageError;

/// A message that can be passed around. It contains
/// 0. the TTL of this PingPong game
/// 1. the next actor to send the message to
/// 2. a port to send a true value to when TTL = 0.
#[derive(Serialize, Deserialize, Debug, Named)]
pub struct PingPongMessage(pub u64, pub ActorRef<PingPongActor>, pub OncePortRef<bool>);

/// Initialization parameters for `PingPongActor`s.
#[derive(Debug, Named, Serialize, Deserialize, Clone)]
pub struct PingPongActorParams {
    /// A port to send undeliverable messages to.
    undeliverable_port_ref: Option<PortRef<Undeliverable<MessageEnvelope>>>,
    /// The TTL at which the actor will exit with error.
    error_ttl: Option<u64>,
    /// Manual delay before sending handling the message.
    delay: Option<Duration>,
}

impl PingPongActorParams {
    /// Create a new set of initialization parameters.
    pub fn new(
        undeliverable_port_ref: Option<PortRef<Undeliverable<MessageEnvelope>>>,
        error_ttl: Option<u64>,
    ) -> Self {
        Self {
            undeliverable_port_ref,
            error_ttl,
            delay: None,
        }
    }

    /// Set the delay
    pub fn set_delay(&mut self, delay: Duration) {
        self.delay = Some(delay);
    }
}

/// A PingPong actor that can play the PingPong game by sending messages around.
#[derive(Debug)]
#[hyperactor::export(handlers = [PingPongMessage])]
pub struct PingPongActor {
    params: PingPongActorParams,
}

#[async_trait]
impl Actor for PingPongActor {
    type Params = PingPongActorParams;

    async fn new(params: Self::Params) -> Result<Self, anyhow::Error> {
        Ok(Self { params })
    }

    // This is an override of the default actor behavior. It is used
    // for testing the mechanism for returning undeliverable messages to
    // their senders.
    async fn handle_undeliverable_message(
        &mut self,
        cx: &Instance<Self>,
        undelivered: crate::mailbox::Undeliverable<crate::mailbox::MessageEnvelope>,
    ) -> Result<(), anyhow::Error> {
        match &self.params.undeliverable_port_ref {
            Some(port) => port.send(cx, undelivered).unwrap(),
            None => {
                let Undeliverable(envelope) = undelivered;
                anyhow::bail!(UndeliverableMessageError::DeliveryFailure { envelope });
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Handler<PingPongMessage> for PingPongActor {
    /// Handles the PingPong Message. It will send the message to th actor specified in the
    /// PingPongMessage if TTL > 0. And deliver a true to the done port if TTL = 0.
    /// It also panics if TTL == 66 for testing purpose.
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        PingPongMessage(ttl, pong_actor, done_port): PingPongMessage,
    ) -> anyhow::Result<()> {
        // PingPongActor sends the messages back and forth. When it's ttl = 0, it will stop.
        // User can set a preconfigured TTL that can cause mocked problem: such as an error.
        if Some(ttl) == self.params.error_ttl {
            anyhow::bail!("PingPong handler encountered an Error");
        }
        if ttl == 0 {
            done_port.send(cx, true)?;
        } else {
            if let Some(delay) = self.params.delay {
                RealClock.sleep(delay).await;
            }
            let next_message = PingPongMessage(ttl - 1, cx.bind(), done_port);
            pong_actor.send(cx, next_message)?;
        }
        Ok(())
    }
}

hyperactor::remote!(PingPongActor);
