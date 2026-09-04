/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Follows Alice Ryhl's task-and-handle actor recipe. See
//! <https://ryhl.io/blog/actors-with-tokio/>.

use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

// Bound Echo's mailbox so producers must await capacity instead of
// allowing queued messages to grow without limit.
const MAILBOX_CAPACITY: usize = 8;

// The task-owned half of the actor. Its spawned task exclusively owns
// this receiver and, later, any private actor state.
struct EchoActor {
    receiver: mpsc::Receiver<EchoMessage>,
}

// Define the actor's mailbox protocol. Each variant names an
// operation and carries its arguments plus a one-shot channel for
// that operation's reply.
enum EchoMessage {
    Echo {
        payload: Vec<u8>,
        respond_to: oneshot::Sender<Vec<u8>>,
    },
}

impl EchoActor {
    fn new(receiver: mpsc::Receiver<EchoMessage>) -> Self {
        Self { receiver }
    }

    // Handle one mailbox message inside the actor task. Echo returns
    // the owned payload and ignores failure when a caller has stopped
    // waiting for its reply.
    fn handle_message(&mut self, message: EchoMessage) {
        match message {
            EchoMessage::Echo {
                payload,
                respond_to,
            } => {
                let _ = respond_to.send(payload);
            }
        }
    }
}

// Run the actor's receive loop until every mailbox sender is dropped;
// recv() then returns None and the actor task exits naturally.
async fn run_echo_actor(mut actor: EchoActor) {
    while let Some(message) = actor.receiver.recv().await {
        actor.handle_message(message);
    }
}

// The cloneable client-facing half of the actor. Handles can only
// send messages; they cannot access the task-owned receiver or actor
// state.
#[derive(Clone)]
pub(crate) struct EchoActorHandle {
    sender: mpsc::Sender<EchoMessage>,
}

impl EchoActorHandle {
    // Create the bounded mailbox, move the actor into its independent
    // Tokio task, and return both the sender-only handle and a task
    // handle for clean shutdown.
    pub(crate) fn new() -> (Self, JoinHandle<()>) {
        let (sender, receiver) = mpsc::channel(MAILBOX_CAPACITY);
        let actor = EchoActor::new(receiver);
        let task = tokio::spawn(run_echo_actor(actor));
        (Self { sender }, task)
    }

    // Turn a handle call into a mailbox message: create a one-shot
    // reply channel, await bounded-mailbox capacity, then await the
    // actor's single response. A failed mailbox send is observed
    // again when the response channel closes.
    pub(crate) async fn echo(&self, payload: &[u8]) -> Vec<u8> {
        let (respond_to, response) = oneshot::channel();
        let message = EchoMessage::Echo {
            payload: payload.to_vec(),
            respond_to,
        };
        let _ = self.sender.send(message).await;
        response.await.expect("Echo actor task should remain alive")
    }
}
