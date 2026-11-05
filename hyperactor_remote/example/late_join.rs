/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Connect a new client to a trainer that is already running.
//!
//! This program has two roles, run in separate processes:
//!
//! - `trainer` runs a simulated training loop as an ordinary Tokio task, not
//!   an actor. Monarch does not start or supervise that loop. Each step updates
//!   a step count and loss using the current learning rate.
//! - `client` connects later, reads the trainer's state, changes its learning
//!   rate, reads the state again, and exits. The trainer keeps running.
//!
//! The trainer embeds a Monarch proc containing three actors:
//!
//! - [`ProbeHost`] is the root actor. It creates the other two actors, writes
//!   the token file, and handles their supervision events.
//! - [`Probe`] is a child of `ProbeHost`. It shares the loop's live
//!   [`TrainerState`] behind a mutex and answers clients' state queries and
//!   learning-rate changes. It is called a probe because clients can use it
//!   to peer into the running trainer.
//! - The rendezvous actor is another child of `ProbeHost`, created by
//!   `token::create()`. It handles join requests and gives clients a reference
//!   to `Probe`; it does not answer the training-state requests itself.
//!
//! The training loop is not a fourth actor. It remains an ordinary Tokio task,
//! separate from this supervision tree. Clients do not own the loop or become
//! its controller.
//!
//! The client also embeds a Monarch proc. `proc.client("client")` creates a
//! lightweight actor instance with its own actor address and mailbox, but no
//! actor message loop like `Probe` has. Ordinary Rust code sends requests and
//! awaits replies through ports belonging to this client instance.
//!
//! To let a client find the probe, the trainer writes a token to a file.
//! The token string contains a reference to the rendezvous actor. The client
//! reads and parses the token, then uses that reference to send a join request
//! to the rendezvous actor. The request carries the client's actor address and
//! a reply port; the client does not send the token itself.
//!
//! The rendezvous actor handles the request by sending the client's actor
//! address to `ProbeHost` as a join notification, then sending a typed reference
//! to `Probe` to the client's reply port. `ProbeHost` logs the notification.
//! The client then sends [`Snapshot`] and [`SetLearningRate`] requests directly
//! to `Probe`, not through the rendezvous actor.
//!
//! This example chooses to give the trainer the client's actor address so
//! `ProbeHost` can log who connected. The token mechanism does not require an
//! actor address: other tokens can exchange other serializable values. Giving
//! the trainer this address does not make the trainer send requests to the
//! client or start supervising it.
//!
//! `ProbeHost` handles child supervision events rather than letting them fail the
//! root actor and terminate the trainer process. Joining establishes contact,
//! not remote supervision: clients can come and go independently. Multiple
//! clients can use the token, but it stops working when its creator stops.
//!
//! The roles can run on the same machine or on separate hosts. This example
//! uses plaintext TCP, which is fine on one machine or within one datacenter;
//! between hosts in different regions or datacenters, use an encrypted
//! transport such as `ChannelTransport::MetaTls(TlsMode::Hostname)` instead.
//! Each proc listens on a non-loopback TCP address resolved from its machine's
//! hostname. Each hostname must resolve to such an address, and the two hosts
//! must be able to reach each other's listeners: requests travel to the
//! trainer, and replies travel back to the client. Copy the trainer's token
//! file to the client host, or place it on a shared filesystem, before
//! starting the client.
//!
//! It demonstrates late attachment, not authentication or a Python API.

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorAddr;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::Endpoint as _;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::OncePortRef;
use hyperactor::Proc;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::ChannelTransport;
use hyperactor::channel::TcpMode;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_remote::Token;
use hyperactor_remote::TokenOptions;
use hyperactor_remote::token;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

/// Read `Token<Gives, Gets>` from the trainer's point of view:
/// the trainer gives each joining client an `ActorRef<ProbeLike>` and gets
/// that client's `ActorAddr`. The rendezvous actor delivers both values.
///
/// The `Probe` actor accepts `Snapshot` and `SetLearningRate` messages.
/// The trainer only logs the client's address.
type ProbeToken = Token<ActorRef<ProbeLike>, ActorAddr>;

/// Delay between simulated training steps.
const STEP_INTERVAL: Duration = Duration::from_millis(200);
/// Maximum time the client waits for a join response or a probe reply.
const REPLY_TIMEOUT: Duration = Duration::from_secs(10);

/// State of the simulated training loop, shared with the probe.
#[derive(Debug)]
struct TrainerState {
    /// Number of simulated training steps completed.
    step: u64,
    /// Simulated loss, multiplied by `1.0 - learning_rate` at each step.
    loss: f64,
    /// Rate used by the training loop; clients can change it through `Probe`.
    learning_rate: f64,
}

/// The trainer's state as reported to a client.
#[derive(Clone, Debug, Serialize, Deserialize, Named)]
struct TrainerStatus {
    /// Number of completed training steps when this snapshot was taken.
    step: u64,
    /// Simulated loss when this snapshot was taken.
    loss: f64,
    /// Learning rate in use when this snapshot was taken.
    learning_rate: f64,
}
// Register its type name, hash and JSON formatter for message diagnostics.
// The Serde derives above handle serialization; registration is for inspection.
wirevalue::register_type!(TrainerStatus);

/// Report the trainer's current state.
#[derive(Clone, Debug, Serialize, Deserialize, Named)]
struct Snapshot {
    /// Client's reply port, where the probe sends one `TrainerStatus`.
    reply: OncePortRef<TrainerStatus>,
}
wirevalue::register_type!(Snapshot);

/// Change the learning rate; the reply carries the previous one.
#[derive(Clone, Debug, Serialize, Deserialize, Named)]
struct SetLearningRate {
    /// New learning rate for the trainer to use on subsequent steps.
    learning_rate: f64,
    /// Client's reply port, where the probe sends the previous learning rate.
    reply: OncePortRef<f64>,
}
wirevalue::register_type!(SetLearningRate);

// Define the probe's client-facing interface as a named set of messages.
// The macro generates `ProbeLike`, a marker type, not another actor or message.
//
// `ActorRef<ProbeLike>` is a reference to an actor that accepts `Snapshot` and
// `SetLearningRate`. Clients can send those messages without knowing the
// actor's concrete type, its fields or how its handlers work. This is the
// reference the trainer gives clients through `ProbeToken`.
//
// When `probe.bind::<ProbeLike>()` creates that reference, the generated code
// requires the actor to implement `Handler<Snapshot>` and
// `Handler<SetLearningRate>`, and binds a message port for each of them.
// The `Probe` handlers below do the work; this macro neither implements them
// nor spawns an actor.
hyperactor::behavior!(ProbeLike, Snapshot, SetLearningRate);

/// The actor clients contact to inspect and adjust the running trainer.
///
/// `Snapshot` returns a copy of the shared training state. `SetLearningRate`
/// changes the rate used by later steps and replies with the previous rate.
/// The loop runs as a separate Tokio task; this actor does not drive or
/// supervise it.
#[derive(Debug)]
#[hyperactor::export(Snapshot, SetLearningRate)]
struct Probe {
    /// Live state shared with the training loop, not a copy.
    /// `Arc` lets the loop and this actor own it together; `Mutex` keeps
    /// their reads and updates from overlapping.
    state: Arc<Mutex<TrainerState>>,
}

/// Use Hyperactor's default actor lifecycle; no custom startup or shutdown.
/// The `Handler` implementations below define how client requests are answered.
impl Actor for Probe {}

#[async_trait]
impl Handler<Snapshot> for Probe {
    /// Reply with the trainer's state at the moment this request is handled.
    /// Copy all three fields under one lock so they describe the same instant,
    /// then release the lock before sending the reply to the client.
    async fn handle(&mut self, cx: &Context<Self>, message: Snapshot) -> anyhow::Result<()> {
        let status = {
            let state = self.state.lock().unwrap();
            TrainerStatus {
                step: state.step,
                loss: state.loss,
                learning_rate: state.learning_rate,
            }
        };
        message.reply.post(cx, status);
        Ok(())
    }
}

#[async_trait]
impl Handler<SetLearningRate> for Probe {
    /// Replace the live learning rate under the same lock used by the loop.
    /// Subsequent training steps use the new rate. Release the lock, log the
    /// change, and reply with the previous rate so the client knows what changed.
    async fn handle(&mut self, cx: &Context<Self>, message: SetLearningRate) -> anyhow::Result<()> {
        let previous = std::mem::replace(
            &mut self.state.lock().unwrap().learning_rate,
            message.learning_rate,
        );
        println!(
            "trainer: learning rate changed from {previous} to {}",
            message.learning_rate
        );
        message.reply.post(cx, previous);
        Ok(())
    }
}

/// Root actor in the trainer process that creates the probe and join token.
///
/// The `Probe` and token's rendezvous actor are its supervised children.
/// It publishes the token and logs their supervision events without letting
/// those events fail this root actor and terminate the trainer process.
/// The training loop is a separate Tokio task, not a child actor.
#[derive(Debug)]
struct ProbeHost {
    /// The loop's live state, passed to the `Probe` actor when it is spawned.
    state: Arc<Mutex<TrainerState>>,
    /// File where the trainer writes the token string for later clients to read.
    token_file: PathBuf,
}

#[async_trait]
impl Actor for ProbeHost {
    /// Spawn the probe and publish its join token for later clients.
    /// `token::create()` spawns the rendezvous actor and returns its token.
    /// Both actors become children of this `ProbeHost`; the training loop
    /// remains a separate task sharing the same state.
    async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
        // Spawn a supervised child that accesses the loop's existing state.
        // Cloning the `Arc` shares ownership; it does not copy the state.
        let probe = this.spawn(Probe {
            state: self.state.clone(),
        });
        // This call spawns the rendezvous actor; it does not just format a token.
        // The returned token contains a reference to that new actor.
        let token: ProbeToken = token::create(
            // Parent the rendezvous actor under this `ProbeHost`, not `Probe`.
            this,
            // Give each client the probe's two-message interface.
            probe.bind::<ProbeLike>(),
            // Route join notifications to this actor's
            // `Handler<token::Joined<ActorAddr>>` implementation.
            this.port::<token::Joined<ActorAddr>>().bind(),
            // `Multi`: accept clients while the rendezvous actor is alive.
            TokenOptions::default(),
        )?;
        // Share the token through a file, outside Monarch's messaging system.
        // A later client reads it to learn how to reach the rendezvous actor.
        write_text(&self.token_file, &token.to_string()).await?;
        println!("trainer: token written to {}", self.token_file.display());
        Ok(())
    }

    /// Log a supervision event from `Probe` or the token's rendezvous actor.
    ///
    /// `Ok(true)` tells Hyperactor the event is handled: do not fail this
    /// root actor and terminate the trainer process because of it.
    /// Handling the event does not restart a failed child.
    async fn handle_supervision_event(
        &mut self,
        _this: &Instance<Self>,
        event: &ActorSupervisionEvent,
    ) -> anyhow::Result<bool> {
        println!("trainer: probe event: {event}");
        Ok(true)
    }
}

#[async_trait]
impl Handler<token::Joined<ActorAddr>> for ProbeHost {
    /// Log the address supplied by a client joining through the token.
    /// The rendezvous actor sends this notification with the client's
    /// `ActorAddr`, not the probe reference it gives to the client.
    /// This handler only logs the join; it does not start remote supervision.
    async fn handle(
        &mut self,
        _cx: &Context<Self>,
        message: token::Joined<ActorAddr>,
    ) -> anyhow::Result<()> {
        println!("trainer: client {} connected", message.peer);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Each invocation runs one role. `?` returns argument errors before
    // starting either role; awaiting the selected role keeps main alive.
    match parse_args()? {
        // Run the training loop and actor tree until `ProbeHost` stops.
        Role::Trainer { token_file } => run_trainer(token_file).await,
        // Join the trainer found through the token, read and change its state,
        // then exit. `learning_rate` is the client's requested new rate.
        Role::Client {
            token_file,
            learning_rate,
        } => run_client(token_file, learning_rate).await,
    }
}

/// Start the simulated training loop and the actors clients use to reach it.
/// The loop and `Probe` share live state, but run independently: the loop is
/// a Tokio task, while `ProbeHost` creates and supervises the probe and
/// rendezvous actors. Wait for `ProbeHost` to stop before returning.
async fn run_trainer(token_file: PathBuf) -> anyhow::Result<()> {
    // Allocate one shared state for the loop and the probe. The mutex keeps
    // training steps and client reads or learning-rate changes from overlapping.
    let state = Arc::new(Mutex::new(TrainerState {
        step: 0,
        loss: 1.0,
        learning_rate: 0.01,
    }));

    // Give the task another reference to the same state, not a copy of it.
    // Tokio schedules this loop independently of the Monarch actors.
    let loop_state = state.clone();
    tokio::spawn(async move {
        loop {
            // Sleep without holding the mutex, so the probe can serve clients
            // between steps. Each step uses the latest learning rate.
            tokio::time::sleep(STEP_INTERVAL).await;
            let mut state = loop_state.lock().unwrap();
            state.step += 1;
            state.loss *= 1.0 - state.learning_rate;
            // Report progress every 25 steps rather than on every update.
            if state.step.is_multiple_of(25) {
                println!(
                    "trainer: step {} loss {:.4} learning rate {}",
                    state.step, state.loss, state.learning_rate
                );
            }
        }
    });

    // Embed a Monarch actor runtime in this trainer process. It will host
    // `ProbeHost`, `Probe`, and the token's rendezvous actor; creating a `Proc`
    // does not launch another OS process. Its TCP listener uses a non-loopback
    // address so clients on this machine or other hosts can reach these actors.
    let proc = Proc::direct(
        ChannelAddr::any(ChannelTransport::Tcp(TcpMode::Hostname)),
        "late_join_trainer".to_string(),
    )?;
    // Start `ProbeHost`. Its init() spawns `Probe` with the shared state, then
    // calls `token::create()` to spawn the rendezvous actor, and writes the
    // token file.
    let host = proc.spawn(ProbeHost { state, token_file });
    // Wait for the root actor to stop, not for the training loop to finish.
    // This keeps the trainer role alive while clients join and use the probe.
    host.await;
    Ok(())
}

/// Join the running trainer, read its state, change its learning rate, and
/// read its state again. The token locates the rendezvous actor; joining
/// returns a `Probe` reference for the subsequent requests. Each reply wait
/// is bounded by `REPLY_TIMEOUT`. Returning does not stop the trainer.
async fn run_client(token_file: PathBuf, learning_rate: f64) -> anyhow::Result<()> {
    // Read the token published by the trainer, possibly copied to this host.
    // Parsing recovers the rendezvous reference and checks the exchanged types.
    let token: ProbeToken = tokio::fs::read_to_string(&token_file)
        .await?
        .trim()
        .parse()?;
    // The client also needs a non-loopback listener: the trainer sends replies
    // to this proc's ports, which must be reachable from the trainer's host.
    let proc = Proc::direct(
        ChannelAddr::any(ChannelTransport::Tcp(TcpMode::Hostname)),
        "late_join_client".to_string(),
    )?;
    // Create our actor identity and mailbox without an actor message loop.
    // This function sends requests and reads replies through the client's ports.
    let client = proc.client("client");

    // Open a reply port for the rendezvous actor's acceptance or rejection.
    let (result_port, mut result_rx) = client.open_port::<token::JoinResult<ActorRef<ProbeLike>>>();
    // Use the token's reference to send a join request to the rendezvous actor.
    // Give our actor address for `ProbeHost` to log, and our reply port for the
    // rendezvous actor's answer. The token itself is not sent.
    token.join(&client, client.self_addr().clone(), result_port.bind())?;
    // Wait for an answer without waiting indefinitely. Acceptance supplies the
    // `Probe` reference; rejection returns an error instead of querying the probe.
    let probe = match tokio::time::timeout(REPLY_TIMEOUT, result_rx.recv()).await {
        Err(_) => anyhow::bail!("no answer from the trainer; is it running?"),
        Ok(result) => match result? {
            token::JoinResult::Joined { peer } => peer,
            token::JoinResult::Rejected { reason } => {
                anyhow::bail!("the trainer rejected the connection: {reason}")
            }
        },
    };
    println!("client: connected; probe is {}", probe.actor_addr());

    // Contact `Probe` directly for the initial state; rendezvous is no longer
    // involved. `snapshot()` sends a request with its own one-shot reply port.
    let status = snapshot(&client, &probe).await?;
    println!(
        "client: step {} loss {:.4} learning rate {}",
        status.step, status.loss, status.learning_rate
    );

    // Ask `Probe` to replace the shared learning rate. Give it a one-shot port
    // for returning the previous rate, not another trainer-state snapshot.
    let (reply, reply_rx) = client.open_once_port::<f64>();
    probe.post(
        &client,
        SetLearningRate {
            learning_rate,
            reply: reply.bind(),
        },
    );
    // The first `?` propagates a timeout; the second propagates a receive error.
    // A reply means the handler has replaced the rate used by training steps.
    let previous = tokio::time::timeout(REPLY_TIMEOUT, reply_rx.recv()).await??;
    println!("client: learning rate changed from {previous} to {learning_rate}");

    // Read again after the change. Training continued during these requests,
    // so the step count and loss may also differ from the initial snapshot.
    let status = snapshot(&client, &probe).await?;
    println!(
        "client: step {} loss {:.4} learning rate {}",
        status.step, status.loss, status.learning_rate
    );
    Ok(())
}

/// Ask `Probe` for the trainer's current state and await its one-shot reply.
/// The reply port belongs to `client`; the state is copied when `Probe`
/// handles the request. Return an error on timeout or receive failure.
async fn snapshot(
    client: &hyperactor::Client,
    probe: &ActorRef<ProbeLike>,
) -> anyhow::Result<TrainerStatus> {
    let (reply, reply_rx) = client.open_once_port::<TrainerStatus>();
    probe.post(
        client,
        Snapshot {
            reply: reply.bind(),
        },
    );
    Ok(tokio::time::timeout(REPLY_TIMEOUT, reply_rx.recv()).await??)
}

/// Write `text` followed by a newline, creating the file or replacing its
/// contents. Used to publish the token; return an error if writing fails.
async fn write_text(path: &Path, text: &str) -> anyhow::Result<()> {
    tokio::fs::write(path, format!("{text}\n")).await?;
    Ok(())
}

/// Which role this invocation runs, with its command-line settings.
#[derive(Debug)]
enum Role {
    /// Run the training loop and host the probe and rendezvous actors.
    Trainer {
        /// File where the trainer writes the token for later clients to read.
        token_file: PathBuf,
    },
    /// Join an existing trainer, inspect its state, and change its learning rate.
    Client {
        /// File containing the trainer's token, shared or copied to this host.
        token_file: PathBuf,
        /// New learning rate the client asks the probe to set.
        learning_rate: f64,
    },
}

fn parse_args() -> anyhow::Result<Role> {
    const USAGE: &str = "usage: late_join trainer --token-file PATH | late_join client --token-file PATH [--learning-rate RATE]";
    let mut args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() {
        anyhow::bail!(USAGE);
    }
    let role = args.remove(0);
    let token_file = PathBuf::from(
        take_flag(&mut args, "--token-file")?
            .ok_or_else(|| anyhow::anyhow!("missing --token-file; {USAGE}"))?,
    );
    let role = match role.as_str() {
        "trainer" => Role::Trainer { token_file },
        "client" => Role::Client {
            token_file,
            learning_rate: take_flag(&mut args, "--learning-rate")?
                .map(|rate| rate.parse())
                .transpose()?
                .unwrap_or(0.05),
        },
        _ => anyhow::bail!("unknown role {role}; {USAGE}"),
    };
    if !args.is_empty() {
        anyhow::bail!("unknown arguments: {}", args.join(" "));
    }
    Ok(role)
}

/// Remove `flag` and its value from `args`, returning the value.
fn take_flag(args: &mut Vec<String>, flag: &str) -> anyhow::Result<Option<String>> {
    let Some(index) = args.iter().position(|arg| arg == flag) else {
        return Ok(None);
    };
    args.remove(index);
    if index >= args.len() {
        anyhow::bail!("missing value for {flag}");
    }
    Ok(Some(args.remove(index)))
}
