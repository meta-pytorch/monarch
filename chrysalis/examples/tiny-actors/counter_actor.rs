// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const MAILBOX_CAPACITY: usize = 8;

// The actor task exclusively owns the mutable count; handles can
// access it only by sending messages through this receiver.
struct CounterActor {
    receiver: mpsc::Receiver<CounterMessage>,
    count: u32,
}

enum CounterMessage {
    Increment { respond_to: oneshot::Sender<u32> },
}

impl CounterActor {
    fn new(receiver: mpsc::Receiver<CounterMessage>) -> Self {
        Self { receiver, count: 0 }
    }

    // Mutate the same task-owned count for every message, preserving
    // state across separate requests.
    fn handle_message(&mut self, message: CounterMessage) {
        match message {
            CounterMessage::Increment { respond_to } => {
                self.count += 1;
                let _ = respond_to.send(self.count);
            }
        }
    }
}

async fn run_counter_actor(mut actor: CounterActor) {
    while let Some(message) = actor.receiver.recv().await {
        actor.handle_message(message);
    }
}

#[derive(Clone)]
pub(crate) struct CounterActorHandle {
    sender: mpsc::Sender<CounterMessage>,
}

impl CounterActorHandle {
    pub(crate) fn new() -> (Self, JoinHandle<()>) {
        let (sender, receiver) = mpsc::channel(MAILBOX_CAPACITY);
        let actor = CounterActor::new(receiver);
        let task = tokio::spawn(run_counter_actor(actor));
        (Self { sender }, task)
    }

    pub(crate) async fn increment(&self) -> u32 {
        let (respond_to, response) = oneshot::channel();
        let message = CounterMessage::Increment { respond_to };
        let _ = self.sender.send(message).await;
        response
            .await
            .expect("Counter actor task should remain alive")
    }
}
