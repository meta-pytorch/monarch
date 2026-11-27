/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A rendezvous is a broker. The server starts it and gives it a value to
//! offer. A connector asks for that value and brings a value of its own.
//! The broker matches the offer with the request and swaps the values.
//!
//! The address names the broker, the [protocol](RendezvousProtocol) says
//! what each side can send, and each `serve` call chooses the actual value.
//! Either side can send a string, an actor address, an actor reference, or
//! another type that meets the [`RendezvousValue`] bounds.
//!
//! ## Generator Example
//!
//! A generator actor is already running. A connector in another process
//! wants to probe it.
//!
//! - The **server** opens the rendezvous and offers the generator reference.
//!   In this example the server is a separate actor. The generator can also
//!   be the server and offer a reference to itself.
//! - The **connector** (the joiner in token-join) wants that reference. It
//!   supplies its own actor address so the server can log who connected.
//! - The **rendezvous actor** brokers that exchange.
//!
//! 1. The server calls [`open`]. This spawns one rendezvous actor as a child
//!    of the server actor and returns a [`Server`] handle. The handle's
//!    [`RendezvousAddr`] names the rendezvous actor and looks like `id@location`.
//!    The server makes that address available to the process running the
//!    connector. For example, the server can write it to a file that process
//!    can read. A command-line flag or scheduler metadata can carry it too.
//! 2. The server calls [`Server::serve`] to send an **offer** to the rendezvous
//!    actor. The offer contains the generator reference and the **result
//!    port** where the server wants to receive the connector's actor address.
//! 3. The connector passes the rendezvous address and its own actor address
//!    to [`connect`]. When the connector awaits the future, `connect` sends a
//!    **request** to the rendezvous actor and waits for a reply. The request
//!    carries the connector's actor address as the value for the server.
//!    `connect` also puts that same address in the [`ConnectRequestId`],
//!    which the rendezvous uses to track the request.
//! 4. The rendezvous actor matches the offer with the request. The connector
//!    gets the generator reference back from `connect` and can use it to
//!    probe the generator. The server gets the connector's actor address on
//!    its result port and can log it.
//!
//! Time runs downward. This exchange shows the offer arriving first.
//!
//! ```text
//!  Server actor                   Rendezvous actor                     Connector
//!        │                                                                 │
//! open   ├───────── spawn child ─────────▶●                                │
//!        │                                │                                │
//!        ├┄┄┄┄┄┄┄┄┄┄┄ share rendezvous address (e.g. in a file) ┄┄┄┄┄┄┄┄┄┄▶│
//!        │                                │                                │
//! serve  ├── offer: generator ref, port ─▶│                                │
//!        │                                │                                │
//!        │                                │◀─── request: connector addr ───┤ connect
//!        │                                │                                │
//!        │                                ◆ match                          │
//!        │                                │                                │
//!        │◀──────── connector addr ───────┼──────── generator ref ────────▶│
//!        │    to the result port          │    in the reply to connect     │
//!        ▼                                ▼                                ▼
//! ```
//!
//! The connector can await the future from `connect` before the server calls
//! `serve`. When no offer is ready, the rendezvous actor queues a new, valid
//! request if the queue has room. The rendezvous holds it up to the time
//! limit the connector passed to `connect`, and the future waits
//! [`RENDEZVOUS_REPLY_MARGIN`] longer for the rendezvous's answer.
//!
//! A successful call to `serve` posts one offer. That offer stays available
//! until the rendezvous matches it with a request or stops. Another call to
//! `serve` on the same `Server` handle posts a new offer at the same address.
//!
//! Each `serve` call supplies the server's value, and each `connect` call
//! supplies the connector's value. At the same address, the server can offer
//! the same generator reference again or a different one of the same type.
//! The connector can also supply a different value on each call. The
//! protocol fixes the types and checks each pair before the rendezvous
//! swaps the values.
//!
//! ## Protocols
//!
//! The server calls `open::<P>` and the connector calls `connect::<P>`.
//! They use the same protocol type `P`. Its [`RendezvousProtocol`]
//! implementation says what type of value each side sends and how to check
//! a pair.
//!
//! ```rust
//! # use hyperactor_remote::rendezvous::PairingRejection;
//! # use serde::Serialize;
//! # use serde::de::DeserializeOwned;
//! # use std::fmt;
//! # use typeuri::Named;
//! pub trait RendezvousValue:
//!     Named + Serialize + DeserializeOwned + Send + Sync + fmt::Debug + 'static
//! {
//! }
//!
//! pub trait RendezvousProtocol: Named + Send + Sync + 'static {
//!     type Provides: RendezvousValue;
//!     type InExchangeFor: RendezvousValue;
//!
//!     fn check(
//!         _offered: &Self::Provides,
//!         _connector_value: &Self::InExchangeFor,
//!     ) -> Result<(), PairingRejection> {
//!         Ok(())
//!     }
//! }
//! ```
//!
//! Any type that meets [`RendezvousValue`]'s requirements automatically
//! implements it and can be used for either side of the exchange. In this
//! example, `Provides` is an [`ActorRef`] to the generator. `InExchangeFor`
//! is the connector's [`ActorAddr`]. The rendezvous calls `check` with both
//! values. The default check accepts the pair.
//!
//! [`connect`] encodes the connector's value as a `wirevalue::Any` and sends
//! a [`RendezvousRequest::Connect`] carrying that value and the protocol
//! name from `P::typename()`. The message type is the same for every protocol.
//! The rendezvous checks the name against the protocol it was opened with
//! and rejects a mismatch before decoding the value.
//!
//! [`connect`] sends requests to the actor at the address it receives.
//! [`open`] starts the built-in rendezvous actor. To use a custom rendezvous
//! actor, bind it as [`RendezvousLike`] and pass its address to `connect`.
//! The actor must handle the `Connect` and `CancelConnect` variants of
//! [`RendezvousRequest`], answer connection attempts with [`ConnectReply`],
//! and follow RDV-2 through RDV-4.
//!
//! The built-in actor also handles two private messages. `serve` sends an
//! `Offer` to make a value available. The actor schedules an `Expire`
//! message to itself when it queues a request. A custom rendezvous chooses
//! how it supplies offers and expires waiting requests.
//!
//! ## Request IDs
//!
//! A [`ConnectRequestId`] combines the sender's actor address with a request
//! number. Each process has one counter shared by all its connectors.
//! Suppose actors A and B share a process and post in the order A, B, A.
//! Their IDs might be `(A, 1)`, `(B, 2)`, and `(A, 3)`. `connect` takes a
//! number and posts the request under one lock, so each actor's requests go
//! out in number order.
//!
//! The rendezvous keeps separate records for each actor address. It remembers
//! the numbers still waiting for an offer and the highest number it has
//! finished by matching, rejecting, expiring, or cancelling a request. It
//! ignores another copy of a queued request. Otherwise, it rejects a number
//! at or below the highest finished number as stale. `CancelConnect` carries
//! the same ID to identify the request to withdraw.
//!
//! ## Rendezvous invariants (RDV-*)
//!
//! - **RDV-1 (one pairing per offer).** A match uses one offer and answers one
//!   `connect` request. The rendezvous posts the server's value to the
//!   connector's reply port and the connector's value to the offer's result port.
//! - **RDV-2 (check before commit).** A request commits only after the
//!   interface check (its protocol's type URI), decoding its value, the
//!   protocol's check, and encoding the server's result all succeed. A
//!   failure leaves the offer available for another request.
//! - **RDV-3 (spent at most once).** A request id commits at most once. A
//!   duplicate of a queued request is ignored. A request whose number is at or
//!   below its connector's highest finished number, and isn't queued, is
//!   refused as stale, with no time window.
//! - **RDV-4 (expired or cancelled requests never commit).** The wait starts
//!   when the rendezvous begins handling the request. A request whose wait
//!   ends in the queue or during matching gets [`ConnectReply::Expired`].
//!   A cancel removes a queued request, and a cancel for a number not yet
//!   seen marks it finished. A cancel after the commit undoes nothing.
//! - **RDV-5 (bounded).** The rendezvous reads its wait and queue limits at
//!   [`open`] from [`RENDEZVOUS_MAX_WAIT`] and [`RENDEZVOUS_MAX_QUEUED`]. It
//!   rejects requests that exceed either limit or whose deadline can't be
//!   represented by the runtime clock.
//! - **RDV-6 (numbers follow posting order).** A connector takes its request's
//!   number and posts the request in one step, under one lock, so its requests
//!   arrive in number order.
//! - **RDV-7 (stopping fails queued offers and requests).** During orderly
//!   shutdown, the rendezvous posts [`Served::Failed`] to the result port of
//!   each offer in its offer queue, and [`ConnectReply::Rejected`] to the
//!   reply port of each request in its waiting queue.
//! - **RDV-8 (failures stay local).** Requests and cancels are posted without
//!   returning undeliverable messages, so a stopped rendezvous never faults
//!   the connector; it fails at its own deadline. Replies and serve results
//!   are posted the same way, so a departed connector or a closed result port
//!   never faults the rendezvous.
//! - **RDV-9 (addresses are dialable).** A [`RendezvousAddr`] names an actor
//!   at a dialable TCP, MetaTLS, QUIC, local or Unix location, possibly
//!   behind `Via` hops. Bind-only forms, other transports, and reserved `?` suffixes
//!   are refused. TCP aliases and listener-adopting `fd` ports are refused
//!   before location parsing. A Chrysalis `?join=` suffix produces
//!   [`AddressError::JoinUnsupported`].

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::fmt;
use std::str::FromStr;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorAddr;
use hyperactor::ActorHandle;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::Endpoint as _;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::Location;
use hyperactor::OncePortRef;
use hyperactor::PortRef;
use hyperactor::actor::ActorError;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::DialAddrParseError;
use hyperactor::context;
use hyperactor_config::global;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::runtime::Handle;
use tokio::time::Instant;
use typeuri::Named;

use crate::config::RENDEZVOUS_MAX_QUEUED;
use crate::config::RENDEZVOUS_MAX_WAIT;
use crate::config::RENDEZVOUS_REPLY_MARGIN;

/// A value exchanged through a rendezvous.
pub trait RendezvousValue:
    Named + Serialize + DeserializeOwned + Send + Sync + fmt::Debug + 'static
{
}

impl<T> RendezvousValue for T where
    T: Named + Serialize + DeserializeOwned + Send + Sync + fmt::Debug + 'static
{
}

/// What the server provides in exchange for a value from the connector.
///
/// `check` decides whether a particular pair can be exchanged.
pub trait RendezvousProtocol: Named + Send + Sync + 'static {
    /// The type of value the server provides to the connector.
    type Provides: RendezvousValue;
    /// The type of value the server expects in exchange. The rendezvous
    /// delivers it to the result port passed to `serve`.
    type InExchangeFor: RendezvousValue;

    /// Check a request against the offer it would take. An error rejects the
    /// request and keeps the offer (RDV-2).
    fn check(
        _offered: &Self::Provides,
        _connector_value: &Self::InExchangeFor,
    ) -> Result<(), PairingRejection> {
        Ok(())
    }
}

/// Why [`RendezvousProtocol::check`] rejected a proposed pairing.
///
/// When `check` returns `Err(PairingRejection { reason })`, the built-in
/// rendezvous sends `ConnectReply::Rejected { reason }` to the request's
/// reply port.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{reason}")]
pub struct PairingRejection {
    /// Concise lowercase reason.
    pub reason: String,
}

impl PairingRejection {
    /// A rejection with this reason.
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

/// Identifies a connection request (RDV-3, RDV-6).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Named)]
pub struct ConnectRequestId {
    /// The address of the actor instance that posted the request.
    pub connector: ActorAddr,
    /// The sequence number assigned when the request was posted.
    pub number: u64,
}
wirevalue::register_type!(ConnectRequestId);

/// A request for an offer.
#[derive(Clone, Debug, Serialize, Deserialize, Named)]
pub struct ConnectRequest {
    /// This request's id.
    pub id: ConnectRequestId,
    /// The type URI of `P` in `connect::<P>`. The built-in rendezvous checks
    /// it against the protocol chosen at `open`.
    pub protocol: String,
    /// The connector's value, erased.
    pub value: wirevalue::Any,
    /// How long the request may wait for an offer.
    pub wait: Duration,
    /// Where the rendezvous sends its one reply.
    pub reply: OncePortRef<ConnectReply>,
}
wirevalue::register_type!(ConnectRequest);

/// Messages a rendezvous actor handles for connection attempts.
#[derive(Clone, Debug, Serialize, Deserialize, Named)]
pub enum RendezvousRequest {
    /// Submit the connector's value and ask for an offer in return. The
    /// rendezvous sends its answer to the reply port in the request.
    Connect(Box<ConnectRequest>),
    /// Withdraw a connection request by ID. If the cancellation arrives
    /// first, the rendezvous rejects a later request with that ID as stale.
    /// A completed match stands.
    CancelConnect {
        /// The original request's ID, including the connector address and
        /// request number.
        id: ConnectRequestId,
    },
}
wirevalue::register_type!(RendezvousRequest);

/// A rendezvous's reply to a connection attempt.
#[derive(Clone, Debug, Serialize, Deserialize, Named)]
pub enum ConnectReply {
    /// The request took an offer.
    Paired {
        /// The offered value, erased.
        value: wirevalue::Any,
    },
    /// The rendezvous refused the request and left any pending offer available.
    /// If the protocol check failed, `reason` comes from its
    /// [`PairingRejection`]. The rendezvous can also supply a reason itself,
    /// for example when its queue is full.
    Rejected {
        /// Concise lowercase reason.
        reason: String,
    },
    /// The request's wait ended before an offer it could take was posted.
    Expired,
}
wirevalue::register_type!(ConnectReply);

// `handle.bind::<RendezvousLike>()` registers a mailbox port on that actor
// for `RendezvousRequest` and returns its `ActorRef<RendezvousLike>`.
// Requests sent to that port go to the actor's `Handler<RendezvousRequest>`.
hyperactor::behavior!(RendezvousLike, RendezvousRequest);

/// The outcome of an offer, sent to the result port the server passed to
/// [`Server::serve`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Named)]
#[serde(bound(serialize = "R: RendezvousValue"))]
#[serde(bound(deserialize = "R: RendezvousValue"))]
pub enum Served<R: RendezvousValue> {
    /// A connector took the offer.
    Paired {
        /// The connector's value.
        peer: R,
    },
    /// The offer was not taken.
    Failed {
        /// Concise lowercase reason.
        reason: String,
    },
}

/// An error constructing a [`RendezvousAddr`] from a string or an existing
/// [`ActorAddr`].
#[derive(Debug, thiserror::Error)]
pub enum AddressError {
    /// The address carries a `?join=` locator.
    #[error("chrysalis join locators are not supported yet")]
    JoinUnsupported,
    /// The address doesn't parse.
    #[error("malformed rendezvous address {address}: {reason}")]
    Malformed {
        /// The address as given.
        address: String,
        /// What is wrong with it.
        reason: String,
    },
    /// The location tells the server how to listen. For example,
    /// `tcp://0.0.0.0:9000` means all local interfaces, and
    /// `tcp://127.0.0.1:0` asks the OS to choose a port. A connector needs
    /// a specific host address and the actual port.
    #[error("rendezvous address {address} is bind-only: {reason}")]
    BindOnly {
        /// The address as given.
        address: String,
        /// Why it can't be dialed.
        reason: String,
    },
    /// A transport scheme outside the rendezvous address allowlist, such as
    /// `vsock://`.
    #[error("rendezvous address {address} uses unsupported transport {transport}")]
    Unsupported {
        /// The address as given.
        address: String,
        /// The transport's scheme.
        transport: String,
    },
}

impl AddressError {
    /// Report a parse failure with the original input and its cause.
    fn malformed(address: &str, reason: impl Into<String>) -> Self {
        Self::Malformed {
            address: address.to_string(),
            reason: reason.into(),
        }
    }

    /// Report a listener setting in an address meant for dialing.
    fn bind_only(address: &str, reason: &str) -> Self {
        Self::BindOnly {
            address: address.to_string(),
            reason: reason.to_string(),
        }
    }

    /// Report a transport scheme outside the rendezvous allowlist.
    fn unsupported(address: &str, transport: impl Into<String>) -> Self {
        Self::Unsupported {
            address: address.to_string(),
            transport: transport.into(),
        }
    }

    /// Translate a Hyperactor dial parse error and attach the original input.
    fn from_dial_error(address: &str, error: DialAddrParseError) -> Self {
        match error {
            DialAddrParseError::BindOnly(reason) => Self::bind_only(address, reason),
            DialAddrParseError::UnsupportedScheme(transport) => {
                Self::unsupported(address, transport)
            }
            DialAddrParseError::Malformed(error) => Self::malformed(address, error.to_string()),
        }
    }
}

/// A rendezvous's address: its actor address, `id@location`.
///
/// It names an actor at a dialable location (RDV-9). `?` is reserved for a
/// suffix: `?join=<locator>` will carry a Chrysalis join locator.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RendezvousAddr(ActorAddr);

impl RendezvousAddr {
    /// The rendezvous's actor address.
    pub fn actor_addr(&self) -> &ActorAddr {
        &self.0
    }

    /// Get the typed reference `connect` uses to find the request handler port.
    fn rendezvous(&self) -> ActorRef<RendezvousLike> {
        // SAFETY: This is an unchecked assertion that the actor at this
        // address has `RendezvousLike` bound. `open` binds the behavior before
        // returning the address. For an externally supplied address, the
        // publisher must provide that guarantee. Parsing checks syntax and
        // the dial location.
        ActorRef::attest(self.0.clone())
    }
}

impl TryFrom<ActorAddr> for RendezvousAddr {
    type Error = AddressError;

    /// Accept an actor address whose printed form is a valid address, so the
    /// result round-trips through [`Display`](fmt::Display) and [`FromStr`].
    fn try_from(actor_addr: ActorAddr) -> Result<Self, Self::Error> {
        actor_addr.to_string().parse()
    }
}

impl fmt::Display for RendezvousAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl FromStr for RendezvousAddr {
    type Err = AddressError;

    /// Parse a published rendezvous address in `id@location` form.
    ///
    /// `?join=` is reserved for Chrysalis and returns
    /// [`AddressError::JoinUnsupported`] today. Any other `?` suffix is
    /// malformed. An address with no `?` goes to Hyperactor's dial parser.
    fn from_str(address: &str) -> Result<Self, Self::Err> {
        if let Some((_, suffix)) = address.split_once('?') {
            if suffix.starts_with("join=") {
                return Err(AddressError::JoinUnsupported);
            }
            return Err(AddressError::malformed(address, "unknown suffix after '?'"));
        }

        // RDV-9: parse as a dial address before applying the rendezvous allowlist.
        let actor_addr = ActorAddr::from_dial_str(address)
            .map_err(|error| AddressError::from_dial_error(address, error))?;
        match actor_addr.location().addr() {
            ChannelAddr::Tcp(_)
            | ChannelAddr::MetaTls(_)
            | ChannelAddr::Quic(_)
            | ChannelAddr::Local(_)
            | ChannelAddr::Unix(_) => Ok(Self(actor_addr)),
            other => {
                let url = other.to_zmq_url();
                let (scheme, _) = url.split_once("://").expect("channel URLs have a scheme");
                Err(AddressError::unsupported(address, scheme))
            }
        }
    }
}

/// Start a rendezvous as a child of the calling actor.
///
/// The returned [`Server`] lets the caller share the child's
/// [`Server::address`] before posting the first offer with [`Server::serve`].
/// Shutting down the calling actor stops the child. To stop a rendezvous
/// opened from a standalone [`hyperactor::Client`], await
/// [`Instance::stop_children`] before dropping the client.
///
/// Returns [`AddressError`] before spawning if the proc's advertised location
/// fails the rendezvous address checks (RDV-9). If the location changes during
/// spawn and the child's address fails those checks, `open` asks the child
/// to stop before returning the address error.
pub fn open<P: RendezvousProtocol>(this: &impl context::Actor) -> Result<Server<P>, AddressError> {
    let instance = this.instance();

    // A proc can have a bind-only default location, such as `tcp://[::]:0`.
    // Check it with the caller's actor ID so an invalid address fails before
    // the rendezvous child is created.
    check_location(instance.self_addr(), instance.proc().default_location())?;

    // `Rendezvous::new` reads the maximum wait per request and the maximum
    // number of queued requests from config. The child stores both limits.
    let handle = instance.spawn(Rendezvous::<P>::new());

    // Open the rendezvous child's mailbox port so it can receive
    // `RendezvousRequest` messages.
    let rendezvous: ActorRef<RendezvousLike> = handle.bind();

    // The proc's default location can change after the first check. Check
    // the address of the bound actor before sharing it.
    let address = match RendezvousAddr::try_from(rendezvous.actor_addr().clone()) {
        Ok(address) => address,
        Err(error) => {
            let _ = handle.stop("unusable rendezvous address");
            return Err(error);
        }
    };

    Ok(Server { handle, address })
}

/// Check that an actor on `owner`'s proc, at `location`, would have a valid
/// address, the way [`RendezvousAddr::from_str`] checks one.
fn check_location(owner: &ActorAddr, location: Location) -> Result<(), AddressError> {
    RendezvousAddr::try_from(ActorAddr::new(owner.id().clone(), location)).map(|_| ())
}

/// Handle for the rendezvous actor started by [`open`].
///
/// Use [`Server::address`] to get the address a connector passes to
/// [`connect`]. Use [`Server::serve`] to post one offer at that address.
///
/// The rendezvous actor can keep running after this handle is dropped.
/// See [`open`] for the shutdown rules for actors and standalone clients.
pub struct Server<P: RendezvousProtocol> {
    /// Local handle `serve` uses to post offers to the rendezvous actor.
    handle: ActorHandle<Rendezvous<P>>,
    /// Validated address to share with callers of `connect`, cached at
    /// `open` so [`Server::address`] can return it by reference.
    address: RendezvousAddr,
}

impl<P: RendezvousProtocol> Server<P> {
    /// The rendezvous's address, to hand to connectors.
    pub fn address(&self) -> &RendezvousAddr {
        &self.address
    }

    /// Post one offer. On a match, the rendezvous sends the connector's value
    /// to `result`. During orderly shutdown, it posts [`Served::Failed`] for
    /// each offer in its offer queue. If encoding fails, `serve` posts nothing.
    pub fn serve(
        &self,
        cx: &impl context::Actor,
        offered: P::Provides,
        result: PortRef<Served<P::InExchangeFor>>,
    ) -> Result<(), wirevalue::Error> {
        let value = wirevalue::Any::serialize(&offered)?;
        self.handle.post(
            cx,
            Offer {
                offered,
                value,
                result,
            },
        );
        Ok(())
    }
}

/// Ask the rendezvous at `address` for an offer and supply `connector_value`
/// for the server. On a match, `connect` returns the offered value and the
/// rendezvous sends `connector_value` to the result port passed to `serve`.
///
/// The rendezvous has `wait` to match the request, starting when it begins
/// handling the request. It rejects waits above its configured maximum
/// (RDV-5). If the request is still pending when its wait expires, the
/// rendezvous replies with [`ConnectReply::Expired`].
///
/// The connector waits up to `wait` plus [`RENDEZVOUS_REPLY_MARGIN`] for
/// the reply, measured from when `connect` starts running. The margin
/// gives time to send the request, begin handling it at the rendezvous,
/// and deliver the reply. When `connect` receives an expiry reply, it
/// returns [`ConnectError::Expired`].
///
/// If the connector's timer wins, `connect` posts
/// [`RendezvousRequest::CancelConnect`] for the request (RDV-4) and returns
/// [`ConnectError::TimedOut`]. It posts the same cancellation if the reply
/// port fails, or if the future is dropped after sending the request. A
/// request that committed before the cancel arrived has still spent its
/// offer.
pub async fn connect<P: RendezvousProtocol>(
    cx: &impl context::Actor,
    address: &RendezvousAddr,
    connector_value: P::InExchangeFor,
    wait: Duration,
) -> Result<P::Provides, ConnectError> {
    // The rendezvous starts `wait` when the request arrives, so its deadline
    // falls after one measured here. Waiting a margin longer lets its answer,
    // including `Expired`, decide the outcome. The margin has to cover
    // sending the request, the rendezvous getting to it, and the reply. When
    // `connect` times out on its own, a match reaches it after it has given
    // up only if those take longer than the margin or the reply is lost.
    let margin = global::get(RENDEZVOUS_REPLY_MARGIN);
    let deadline = match Instant::now()
        .checked_add(wait)
        .and_then(|end| end.checked_add(margin))
    {
        Some(deadline) if !wait.is_zero() => deadline,
        _ => return Err(ConnectError::InvalidWait { wait }),
    };
    let value = wirevalue::Any::serialize(&connector_value)?;
    // Open a one-shot reply port on the calling actor for this request.
    // The request carries the port's address, and `connect` waits on its receiver.
    let (reply, reply_rx) = cx.instance().open_once_port::<ConnectReply>();

    let rendezvous_ref = address.rendezvous();
    let mut request_port = rendezvous_ref.port::<RendezvousRequest>();
    // RDV-8: Suppress undeliverable returns on this local `PortRef`. If the
    // rendezvous has stopped, `connect` reaches its own deadline.
    request_port.return_undeliverable(false);

    // RDV-6: Assign the request ID and post under the same lock.
    let id = post_numbered(cx, &request_port, |id| {
        RendezvousRequest::Connect(Box::new(ConnectRequest {
            id,
            protocol: P::typename().to_string(),
            value,
            wait,
            reply: reply.bind(),
        }))
    });
    // RDV-4: A queued request left behind by a caller that stopped waiting
    // would spend the next offer on a connector that is gone. Until the
    // rendezvous answers, dropping this guard withdraws the request. That
    // covers a local timeout, a failed reply port, and a caller dropping
    // this future, for example from an outer `select!`.
    let pending = Withdraw {
        cx,
        port: &request_port,
        id: Some(id),
        // `connect` is being polled, so this is the caller's runtime.
        runtime: Handle::try_current().ok(),
    };

    // The rendezvous can report expiry in a reply. A local timeout sends
    // `CancelConnect` with the ID of the posted request.
    let reply = match tokio::time::timeout_at(deadline, reply_rx.recv()).await {
        Ok(Ok(reply)) => reply,
        Ok(Err(err)) => {
            // No reply can arrive now, so withdraw the request.
            drop(pending);
            return Err(ConnectError::Receive(err.into()));
        }
        Err(_) => {
            // The request may still be queued at the rendezvous. Ask it to
            // withdraw the request. A match made before the cancel arrives
            // stands.
            drop(pending);
            return Err(ConnectError::TimedOut { wait });
        }
    };
    pending.answered();
    match reply {
        ConnectReply::Paired { value } => Ok(value.deserialized()?),
        ConnectReply::Rejected { reason } => Err(ConnectError::Rejected { reason }),
        ConnectReply::Expired => Err(ConnectError::Expired),
    }
}

/// Withdraws a posted request when dropped, unless the rendezvous answered
/// it first (RDV-4).
struct Withdraw<'a, C: context::Actor> {
    /// The connector that posted the request.
    cx: &'a C,
    /// The rendezvous request port, with undeliverable returns suppressed
    /// (RDV-8).
    port: &'a PortRef<RendezvousRequest>,
    /// The posted request's ID, or `None` once the rendezvous has answered.
    id: Option<ConnectRequestId>,
    /// The Tokio runtime that polled `connect`, entered while posting the
    /// cancel.
    ///
    /// `post` looks like a plain send, but when no usable connection to the
    /// rendezvous is cached, it dials a new one, and dialing spawns a task.
    /// Hyperactor spawns it on the runtime installed by
    /// `hyperactor::initialize`, or else on the ambient Tokio runtime, and
    /// panics if there is neither. Monarch's Python extension and bootstrap
    /// binaries install one. A bare Rust caller may not, and it can drop a
    /// pending `connect` future outside any runtime, for example after
    /// leaving `block_on`. A panic in `drop` would lose the cancel, and it
    /// would abort the process if the caller were already unwinding.
    runtime: Option<Handle>,
}

impl<C: context::Actor> Withdraw<'_, C> {
    /// Record that the rendezvous answered, so nothing is left to withdraw.
    fn answered(mut self) {
        self.id = None;
    }
}

impl<C: context::Actor> Drop for Withdraw<'_, C> {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            // See `runtime`: posting can dial, and this drop may run outside
            // any runtime.
            let _entered = self.runtime.as_ref().map(Handle::enter);
            self.port
                .post(self.cx, RendezvousRequest::CancelConnect { id });
        }
    }
}

/// Take the next request number and post the request built from it, in one
/// step (RDV-6).
///
/// One counter serves every connector in the process, so each connector's
/// numbers go up, and holding the lock across the post keeps them in posting
/// order. Hyperactor delivers messages in order between a sender and a
/// receiver.
fn post_numbered(
    cx: &impl context::Actor,
    port: &PortRef<RendezvousRequest>,
    request: impl FnOnce(ConnectRequestId) -> RendezvousRequest,
) -> ConnectRequestId {
    static LAST_NUMBER: Mutex<u64> = Mutex::new(0);
    // A panic while building or posting a request under this lock, for
    // example when `post` dials outside any runtime (see
    // `Withdraw::runtime`), poisons it. The lock guards only a counter,
    // which stays valid: the number is taken before the post, so later
    // numbers still go up. Use the lock anyway, so that one panic doesn't
    // make every later `connect` in the process panic here.
    let mut last = LAST_NUMBER.lock().unwrap_or_else(PoisonError::into_inner);
    *last += 1;
    let id = ConnectRequestId {
        connector: cx.instance().self_addr().clone(),
        number: *last,
    };
    port.post(cx, request(id.clone()));
    id
}

/// Why a connection failed.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    /// The wait is zero or too long to schedule, so `connect` sends nothing.
    #[error("invalid wait {wait:?}: must be positive and fit in a timer")]
    InvalidWait {
        /// The wait given.
        wait: Duration,
    },
    /// The rendezvous refused the request; no offer was spent.
    #[error("connection rejected: {reason}")]
    Rejected {
        /// Concise lowercase reason.
        reason: String,
    },
    /// The request's wait ended at the rendezvous before an offer it could take.
    #[error("connection expired before an offer was available")]
    Expired,
    /// No reply arrived within the wait plus [`RENDEZVOUS_REPLY_MARGIN`].
    /// `connect` posted a cancellation.
    #[error("no reply from the rendezvous within {wait:?} plus the reply margin")]
    TimedOut {
        /// The wait given.
        wait: Duration,
    },
    /// A value could not be encoded or decoded.
    #[error(transparent)]
    Value(#[from] wirevalue::Error),
    /// The reply port failed. `connect` posted a cancellation.
    #[error("reply port failed: {0}")]
    Receive(anyhow::Error),
}

/// One offer posted by `serve` and kept by the rendezvous until a request
/// takes it or the rendezvous stops.
struct Offer<P: RendezvousProtocol> {
    /// The server's value in typed form, for `P::check` to inspect.
    offered: P::Provides,
    /// The same value, encoded when `serve` posts the offer. The rendezvous
    /// sends it in the reply to the connector that takes the offer.
    value: wirevalue::Any,
    /// The result port supplied to `serve`. The rendezvous sends
    /// `Served::Paired` with the connector's value on a match, or
    /// `Served::Failed` during orderly shutdown if the offer is still queued.
    result: PortRef<Served<P::InExchangeFor>>,
}

/// A decoded connection request to check against an offer. The rendezvous
/// queues it if an offer is unavailable.
struct Queued<P: RendezvousProtocol> {
    /// Identifies the request for cancellation and duplicate detection.
    id: ConnectRequestId,
    /// The connector's decoded value. `P::check` inspects it, and a match
    /// sends it to the result port supplied to `serve`.
    connector_value: P::InExchangeFor,
    /// The rendezvous's deadline for this request while it waits in the queue.
    deadline: Instant,
    /// The connector's one-shot port for the rendezvous's reply.
    reply: OncePortRef<ConnectReply>,
}

/// When the rendezvous queues a request, it schedules this message to itself
/// at the request's deadline with `post_at`. The handler removes the request
/// if it is still queued and replies with [`ConnectReply::Expired`].
struct Expire(
    /// The ID of the request to expire if it is still queued.
    ConnectRequestId,
);

/// What a rendezvous remembers about one connector (RDV-3).
#[derive(Default)]
struct Connector {
    /// Numbers of this connector's queued requests.
    queued: BTreeSet<u64>,
    /// The highest number answered, cancelled or refused.
    finished: u64,
}

/// The rendezvous actor started by [`open`]. It matches offers posted by
/// [`Server::serve`] with requests from [`connect`] and delivers each side's
/// value to the other when the pair passes `P::check`.
struct Rendezvous<P: RendezvousProtocol> {
    /// The largest wait a connection request can ask for, read at `open`.
    max_wait: Duration,
    /// The most requests that can wait for an offer, read at `open`.
    max_queued: usize,
    /// Pending offers in arrival order. The front offer is the next one
    /// considered for a match.
    offers: VecDeque<Offer<P>>,
    /// Decoded connection requests waiting for an offer, in arrival order.
    queue: VecDeque<Queued<P>>,
    /// Queued numbers and the highest finished number for each connector
    /// address, used to recognize duplicate and stale requests.
    ///
    /// Records are never removed. RDV-3 refuses a stale repeat however late
    /// it arrives, so a connector's highest finished number must outlive its
    /// requests. Forgetting it after a retention period would let a delayed
    /// repeat look new and spend a second offer. The cost is one small
    /// record per distinct connector address for the life of the rendezvous.
    connectors: BTreeMap<ActorAddr, Connector>,
}

impl<P: RendezvousProtocol> Rendezvous<P> {
    /// Create an empty rendezvous using the current wait and queue limits.
    fn new() -> Self {
        Self {
            max_wait: global::get(RENDEZVOUS_MAX_WAIT),
            max_queued: global::get(RENDEZVOUS_MAX_QUEUED),
            offers: VecDeque::new(),
            queue: VecDeque::new(),
            connectors: BTreeMap::new(),
        }
    }

    /// Remove the request number from the connector's set of queued numbers
    /// and update its highest finished number (RDV-3).
    ///
    /// The rendezvous handles one message at a time and has exclusive
    /// access to these records through `&mut self`.
    fn finish(&mut self, id: &ConnectRequestId) {
        let connector = self.connectors.entry(id.connector.clone()).or_default();
        connector.queued.remove(&id.number);
        connector.finished = connector.finished.max(id.number);
    }

    fn reply(cx: &impl context::Actor, mut port: OncePortRef<ConnectReply>, reply: ConnectReply) {
        // RDV-8: a departed connector must not fault the rendezvous.
        port.return_undeliverable(false);
        port.post(cx, reply);
    }

    /// Send an encoded offer outcome to the result port passed to `serve`.
    fn report(
        cx: &impl context::Actor,
        mut result: PortRef<Served<P::InExchangeFor>>,
        served: wirevalue::Any,
    ) {
        // RDV-8: a closed result port must not fault the rendezvous.
        result.return_undeliverable(false);
        result.post_serialized(cx, Default::default(), served);
    }

    /// Finish the request and reply with its rejection reason. Any pending
    /// offer stays available for another request.
    fn reject(
        &mut self,
        cx: &impl context::Actor,
        id: &ConnectRequestId,
        reply: OncePortRef<ConnectReply>,
        reason: String,
    ) {
        self.finish(id);
        Self::reply(cx, reply, ConnectReply::Rejected { reason });
    }

    /// Spend the front offer and post the prepared replies (RDV-1).
    fn commit(
        &mut self,
        cx: &impl context::Actor,
        id: &ConnectRequestId,
        reply: OncePortRef<ConnectReply>,
        served: wirevalue::Any,
    ) {
        let Offer { value, result, .. } = self
            .offers
            .pop_front()
            .expect("commit requires a pending offer");
        self.finish(id);
        Self::reply(cx, reply, ConnectReply::Paired { value });
        Self::report(cx, result, served);
    }

    /// Finish a request whose wait ended and notify the connector (RDV-4).
    fn expire(
        &mut self,
        cx: &impl context::Actor,
        id: &ConnectRequestId,
        reply: OncePortRef<ConnectReply>,
    ) {
        self.finish(id);
        Self::reply(cx, reply, ConnectReply::Expired);
    }

    /// Check the oldest offer against a request. Return whether it was spent.
    fn try_pair(&mut self, cx: &impl context::Actor, request: Queued<P>) -> bool {
        if Instant::now() >= request.deadline {
            self.expire(cx, &request.id, request.reply);
            return false;
        }
        let offer = self
            .offers
            .front()
            .expect("pairing requires a pending offer");
        // RDV-2: Prepare the server's result before spending the offer.
        let outcome = match P::check(&offer.offered, &request.connector_value) {
            Ok(()) => wirevalue::Any::serialize(&Served::Paired {
                peer: request.connector_value,
            })
            .map_err(|error| {
                format!("connector value could not be encoded for the server: {error}")
            }),
            Err(rejection) => Err(rejection.reason),
        };
        // RDV-4: The wait can end during the check or result encoding.
        if Instant::now() >= request.deadline {
            self.expire(cx, &request.id, request.reply);
            return false;
        }
        match outcome {
            Ok(served) => {
                self.commit(cx, &request.id, request.reply, served);
                true
            }
            Err(reason) => {
                self.reject(cx, &request.id, request.reply, reason);
                false
            }
        }
    }

    /// Pair queued requests, oldest first, with the front offer until one
    /// commits or none is left. A request past its deadline expires instead
    /// (RDV-4); one that fails the check is rejected (RDV-2).
    fn pair(&mut self, cx: &impl context::Actor) {
        while !self.offers.is_empty()
            && let Some(request) = self.queue.pop_front()
        {
            if self.try_pair(cx, request) {
                return;
            }
        }
    }

    /// Process the `Connect` case of `Handler<RendezvousRequest>::handle`.
    /// Check the request, then try the oldest offer or queue the request to
    /// wait for an offer.
    fn handle_connect(&mut self, cx: &Context<'_, Self>, connect: ConnectRequest) {
        let arrived = Instant::now();
        let ConnectRequest {
            id,
            protocol,
            value,
            wait,
            reply: connector_reply_port,
        } = connect;
        // RDV-3: duplicates of a queued request are ignored, and anything at
        // or below the connector's finished number is stale.
        let connector = self.connectors.entry(id.connector.clone()).or_default();
        if connector.queued.contains(&id.number) {
            return;
        }
        if id.number <= connector.finished {
            Self::reply(
                cx,
                connector_reply_port,
                ConnectReply::Rejected {
                    reason: "stale request".to_string(),
                },
            );
            return;
        }

        // RDV-5.
        if wait > self.max_wait {
            let reason = format!("wait {wait:?} exceeds the maximum {:?}", self.max_wait);
            return self.reject(cx, &id, connector_reply_port, reason);
        }
        let Some(deadline) = arrived.checked_add(wait) else {
            let reason = format!("wait {wait:?} is too long to schedule");
            return self.reject(cx, &id, connector_reply_port, reason);
        };

        // RDV-2: The server chooses its protocol at `open`. A connector in
        // another process chooses its own protocol at `connect`. `connect`
        // uses the same request and reply types for every protocol, so the
        // rendezvous can reject a mismatch before decoding the connector's
        // value as `P::InExchangeFor`.
        let expected_protocol = P::typename();
        if protocol != expected_protocol {
            let reason = format!("protocol mismatch: expected {expected_protocol}, got {protocol}");
            return self.reject(cx, &id, connector_reply_port, reason);
        }
        let connector_value: P::InExchangeFor = match value.deserialized() {
            Ok(connector_value) => connector_value,
            Err(err) => {
                let reason = format!("request value could not be decoded: {err}");
                return self.reject(cx, &id, connector_reply_port, reason);
            }
        };

        let request = Queued {
            id,
            connector_value,
            deadline,
            reply: connector_reply_port,
        };

        // With an offer ready, skip the waiting queue and check the oldest
        // offer now. The `Queued` value carries the request fields straight
        // to `try_pair`, which records the request as finished.
        if !self.offers.is_empty() {
            self.try_pair(cx, request);
            return;
        }

        // RDV-5: A request waiting for an offer needs room in the queue.
        if self.queue.len() >= self.max_queued {
            return self.reject(
                cx,
                &request.id,
                request.reply,
                "request queue is full".to_string(),
            );
        }

        // Track the queued number in this connector's record so a second
        // copy of the same request can be ignored.
        connector.queued.insert(request.id.number);

        // Schedule expiry and put the full request in the queue for matching.
        cx.post_at(cx, Expire(request.id.clone()), request.deadline);
        self.queue.push_back(request);
    }

    /// Process the `CancelConnect` case of `Handler<RendezvousRequest>::handle`.
    ///
    /// RDV-4: withdraw a queued request, or mark an unseen number finished so
    /// its request is refused if it arrives. After the commit this does nothing.
    fn handle_cancel(&mut self, id: ConnectRequestId) {
        if let Some(index) = self.queue.iter().position(|queued| queued.id == id) {
            self.queue.remove(index);
        }
        self.finish(&id);
    }
}

#[async_trait]
impl<P: RendezvousProtocol> Actor for Rendezvous<P> {
    /// RDV-7: Post failure to the result port of each queued offer, and
    /// reject each queued request so its connector doesn't wait out its
    /// deadline.
    async fn cleanup(
        &mut self,
        this: &Instance<Self>,
        _err: Option<&ActorError>,
    ) -> anyhow::Result<()> {
        let failure = wirevalue::Any::serialize(&Served::<P::InExchangeFor>::Failed {
            reason: "rendezvous stopped".to_string(),
        })?;
        for offer in self.offers.drain(..) {
            Self::report(this, offer.result, failure.clone());
        }
        for request in self.queue.drain(..) {
            let reason = "rendezvous stopped".to_string();
            Self::reply(this, request.reply, ConnectReply::Rejected { reason });
        }
        Ok(())
    }
}

#[async_trait]
impl<P: RendezvousProtocol> Handler<RendezvousRequest> for Rendezvous<P> {
    /// Dispatch connection requests and cancellations received on the
    /// rendezvous's request port.
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        request: RendezvousRequest,
    ) -> anyhow::Result<()> {
        match request {
            RendezvousRequest::Connect(connect) => self.handle_connect(cx, *connect),
            RendezvousRequest::CancelConnect { id } => self.handle_cancel(id),
        }
        Ok(())
    }
}

#[async_trait]
impl<P: RendezvousProtocol> Handler<Offer<P>> for Rendezvous<P> {
    /// Add the offer posted by `serve`, then try to match the oldest offer
    /// with a waiting request.
    async fn handle(&mut self, cx: &Context<Self>, offer: Offer<P>) -> anyhow::Result<()> {
        self.offers.push_back(offer);
        self.pair(cx);
        Ok(())
    }
}

#[async_trait]
impl<P: RendezvousProtocol> Handler<Expire> for Rendezvous<P> {
    /// RDV-4: expire the request if it is still queued.
    async fn handle(&mut self, cx: &Context<Self>, expire: Expire) -> anyhow::Result<()> {
        let Expire(id) = expire;
        let Some(index) = self.queue.iter().position(|queued| queued.id == id) else {
            return Ok(());
        };
        let queued = self.queue.remove(index).expect("index is in range");
        self.expire(cx, &queued.id, queued.reply);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::panic::AssertUnwindSafe;

    use hyperactor::Client;
    use hyperactor::PortHandle;
    use hyperactor::Proc;
    use hyperactor::channel::ChannelTransport;
    use hyperactor::channel::TcpMode;
    use hyperactor::mailbox::OncePortReceiver;
    use hyperactor::mailbox::PortReceiver;

    use super::*;

    // Rendezvous invariant coverage:
    // - RDV-1: test_connect_takes_a_posted_offer,
    //   test_connect_waits_for_a_later_offer, test_each_serve_admits_one_connector.
    // - RDV-2: test_a_failed_check_keeps_the_offer,
    //   test_another_protocol_is_rejected_and_the_offer_kept,
    //   test_an_undecodable_value_is_rejected,
    //   test_a_value_that_cannot_be_encoded_for_the_server_keeps_the_offer.
    // - RDV-3: test_a_duplicate_request_spends_one_offer,
    //   test_a_repeat_of_a_committed_request_is_stale.
    // - RDV-4: test_a_request_expires_without_an_offer,
    //   test_an_expired_request_leaves_a_ready_offer,
    //   test_connect_reports_the_rendezvous_expiry,
    //   test_a_cancelled_request_spends_nothing,
    //   test_a_dropped_connect_spends_nothing,
    //   test_a_connect_dropped_outside_its_runtime_still_withdraws,
    //   test_a_cancel_before_its_request_refuses_it,
    //   test_a_cancel_after_the_commit_is_not_rolled_back.
    // - RDV-5: test_a_full_queue_rejects_the_next_request,
    //   test_connect_refuses_an_invalid_wait,
    //   test_the_rendezvous_enforces_its_configured_wait,
    //   test_a_wait_too_long_to_schedule_is_rejected.
    // - RDV-6: test_concurrent_requests_from_one_instance_both_pair,
    //   test_a_panic_under_the_number_lock_does_not_break_connect.
    // - RDV-7: test_an_owner_that_stops_fails_its_offers,
    //   test_stopping_rejects_queued_requests.
    // - RDV-8: test_an_owner_that_stops_fails_its_offers (the connector side),
    //   test_a_departed_connector_does_not_fault_the_rendezvous.
    // - RDV-9: test_addresses_round_trip, test_unusable_addresses_are_refused,
    //   test_open_refuses_a_bind_only_location.
    // - The protocol is open to caller-written rendezvous:
    //   test_a_caller_written_rendezvous_answers_connect.

    const WAIT: Duration = Duration::from_secs(10);
    /// How long a test watches for a message that must not arrive.
    const QUIET: Duration = Duration::from_millis(300);

    /// Offers a `String`; a connector sends a `String`, and "wrong" fails the
    /// check.
    #[derive(Named)]
    struct TestProtocol;

    impl RendezvousProtocol for TestProtocol {
        type Provides = String;
        type InExchangeFor = String;

        fn check(_offered: &String, connector_value: &String) -> Result<(), PairingRejection> {
            if connector_value == "wrong" {
                return Err(PairingRejection::new("wrong request"));
            }
            Ok(())
        }
    }

    struct Fixture {
        proc: Proc,
        owner: Client,
        server: Server<TestProtocol>,
        served: PortHandle<Served<String>>,
        served_rx: PortReceiver<Served<String>>,
    }

    impl Fixture {
        fn new() -> Self {
            let _config = global::lock();
            Self::open()
        }

        fn open() -> Self {
            let proc = Proc::isolated();
            let owner = proc.client("owner");
            let server = open::<TestProtocol>(&owner).unwrap();
            let (served, served_rx) = owner.open_port::<Served<String>>();
            Self {
                proc,
                owner,
                server,
                served,
                served_rx,
            }
        }

        fn serve(&self, offered: &str) {
            self.server
                .serve(&self.owner, offered.to_string(), self.served.bind())
                .unwrap();
        }

        async fn connect(
            &self,
            cx: &Client,
            connector_value: &str,
        ) -> Result<String, ConnectError> {
            connect::<TestProtocol>(cx, self.server.address(), connector_value.to_string(), WAIT)
                .await
        }

        /// The rendezvous port for tests that post messages directly.
        fn request_port(&self) -> PortRef<RendezvousRequest> {
            let rendezvous_ref = self.server.address().rendezvous();
            rendezvous_ref.port::<RendezvousRequest>()
        }

        /// Post a request with a chosen id, bypassing [`connect`].
        fn post(
            &self,
            cx: &Client,
            number: u64,
            protocol: &str,
            wait: Duration,
        ) -> OncePortReceiver<ConnectReply> {
            let (reply, reply_rx) = cx.open_once_port::<ConnectReply>();
            self.request_port().post(
                cx,
                RendezvousRequest::Connect(Box::new(ConnectRequest {
                    id: id(cx, number),
                    protocol: protocol.to_string(),
                    value: wirevalue::Any::serialize(&"peer".to_string()).unwrap(),
                    wait,
                    reply: reply.bind(),
                })),
            );
            reply_rx
        }

        fn cancel(&self, cx: &Client, number: u64) {
            self.request_port()
                .post(cx, RendezvousRequest::CancelConnect { id: id(cx, number) });
        }

        async fn next_served(&mut self) -> Served<String> {
            tokio::time::timeout(WAIT, self.served_rx.recv())
                .await
                .unwrap()
                .unwrap()
        }

        async fn assert_no_pairing(&mut self) {
            assert!(
                tokio::time::timeout(QUIET, self.served_rx.recv())
                    .await
                    .is_err(),
                "no offer should have been taken"
            );
        }
    }

    fn id(cx: &Client, number: u64) -> ConnectRequestId {
        ConnectRequestId {
            connector: cx.self_addr().clone(),
            number,
        }
    }

    fn protocol() -> &'static str {
        TestProtocol::typename()
    }

    async fn recv(reply_rx: OncePortReceiver<ConnectReply>) -> ConnectReply {
        tokio::time::timeout(WAIT, reply_rx.recv())
            .await
            .unwrap()
            .unwrap()
    }

    fn assert_rejected(reply: ConnectReply, expected: &str) {
        match reply {
            ConnectReply::Rejected { reason } => {
                assert!(reason.contains(expected), "unexpected reason: {reason}")
            }
            other => panic!("expected a rejection containing {expected:?}, got {other:?}"),
        }
    }

    fn paired(peer: &str) -> Served<String> {
        Served::Paired {
            peer: peer.to_string(),
        }
    }

    /// Upholds RDV-1.
    ///
    /// An offer posted first pairs with the next connector.
    #[tokio::test]
    async fn test_connect_takes_a_posted_offer() {
        let mut f = Fixture::new();
        let connector = f.proc.client("connector");
        f.serve("offer");

        assert_eq!(f.connect(&connector, "peer").await.unwrap(), "offer");
        assert_eq!(f.next_served().await, paired("peer"));
    }

    /// Upholds RDV-1.
    ///
    /// A connector that arrives first waits for an offer.
    #[tokio::test]
    async fn test_connect_waits_for_a_later_offer() {
        let mut f = Fixture::new();
        let connector = f.proc.client("connector");

        let (result, ()) = tokio::join!(f.connect(&connector, "peer"), async {
            tokio::time::sleep(QUIET).await;
            f.serve("offer");
        });

        assert_eq!(result.unwrap(), "offer");
        assert_eq!(f.next_served().await, paired("peer"));
    }

    /// Upholds RDV-1.
    ///
    /// Each serve admits one connector; serving again admits another.
    #[tokio::test]
    async fn test_each_serve_admits_one_connector() {
        let mut f = Fixture::new();
        let first = f.proc.client("first");
        let second = f.proc.client("second");
        let raw = f.proc.client("raw");
        f.serve("one");

        assert_eq!(f.connect(&first, "first").await.unwrap(), "one");
        assert_eq!(f.next_served().await, paired("first"));
        let reply_rx = f.post(&raw, 1, protocol(), QUIET);
        assert!(matches!(recv(reply_rx).await, ConnectReply::Expired));

        f.serve("two");
        assert_eq!(f.connect(&second, "second").await.unwrap(), "two");
        assert_eq!(f.next_served().await, paired("second"));
    }

    /// Upholds RDV-2.
    ///
    /// A request failing the protocol's check is rejected, and the same
    /// offer pairs with the next request.
    #[tokio::test]
    async fn test_a_failed_check_keeps_the_offer() {
        let mut f = Fixture::new();
        let connector = f.proc.client("connector");
        f.serve("offer");

        match f.connect(&connector, "wrong").await {
            Err(ConnectError::Rejected { reason }) => assert_eq!(reason, "wrong request"),
            other => panic!("expected a rejection, got {other:?}"),
        }
        f.assert_no_pairing().await;
        assert_eq!(f.connect(&connector, "right").await.unwrap(), "offer");
        assert_eq!(f.next_served().await, paired("right"));
    }

    /// Upholds RDV-2.
    ///
    /// A request for another protocol is rejected, and a later matching
    /// request takes the same offer.
    #[tokio::test]
    async fn test_another_protocol_is_rejected_and_the_offer_kept() {
        let mut f = Fixture::new();
        let connector = f.proc.client("connector");
        let other = f.proc.client("other");
        f.serve("offer");

        let reply_rx = f.post(&other, 1, "some::other::Protocol", WAIT);
        assert_rejected(recv(reply_rx).await, "protocol mismatch");
        f.assert_no_pairing().await;

        assert_eq!(f.connect(&connector, "peer").await.unwrap(), "offer");
        assert_eq!(f.next_served().await, paired("peer"));
    }

    /// Upholds RDV-2.
    ///
    /// A value that doesn't decode as the protocol's request is rejected.
    #[tokio::test]
    async fn test_an_undecodable_value_is_rejected() {
        let mut f = Fixture::new();
        let connector = f.proc.client("connector");
        f.serve("offer");

        let (reply, reply_rx) = connector.open_once_port::<ConnectReply>();
        f.request_port().post(
            &connector,
            RendezvousRequest::Connect(Box::new(ConnectRequest {
                id: id(&connector, 1),
                protocol: protocol().to_string(),
                value: wirevalue::Any::serialize(&7u64).unwrap(),
                wait: WAIT,
                reply: reply.bind(),
            })),
        );
        assert_rejected(recv(reply_rx).await, "could not be decoded");
        f.assert_no_pairing().await;
    }

    /// Upholds RDV-2.
    ///
    /// A value that decodes but fails to encode for the server leaves the
    /// offer available for a later request.
    #[tokio::test]
    async fn test_a_value_that_cannot_be_encoded_for_the_server_keeps_the_offer() {
        use serde::Deserializer;
        use serde::Serializer;

        #[derive(Debug, Named)]
        struct ReencodeValue {
            byte: u8,
            can_encode: bool,
        }

        impl Serialize for ReencodeValue {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                if !self.can_encode {
                    return Err(serde::ser::Error::custom("test encoding failure"));
                }
                serializer.serialize_u8(self.byte)
            }
        }

        impl<'de> Deserialize<'de> for ReencodeValue {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let byte = u8::deserialize(deserializer)?;
                Ok(Self {
                    byte,
                    can_encode: byte != 0,
                })
            }
        }

        #[derive(Named)]
        struct ReencodeProtocol;

        impl RendezvousProtocol for ReencodeProtocol {
            type Provides = String;
            type InExchangeFor = ReencodeValue;
        }

        let proc = Proc::isolated();
        let owner = proc.client("owner");
        let (result, mut result_rx) = owner.open_port::<Served<ReencodeValue>>();
        let server = open::<ReencodeProtocol>(&owner).unwrap();
        server
            .serve(&owner, "offer".to_string(), result.bind())
            .unwrap();

        let connect_value = |byte| {
            connect::<ReencodeProtocol>(
                &owner,
                server.address(),
                ReencodeValue {
                    byte,
                    can_encode: true,
                },
                WAIT,
            )
        };
        assert!(matches!(
            connect_value(0).await,
            Err(ConnectError::Rejected { reason }) if reason.contains("test encoding failure")
        ));
        assert_eq!(connect_value(1).await.unwrap(), "offer");
        let served = tokio::time::timeout(WAIT, result_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(served, Served::Paired { peer } if peer.byte == 1));
    }

    /// Upholds RDV-3.
    ///
    /// A duplicate of a queued request is ignored, so it spends no
    /// second offer.
    #[tokio::test]
    async fn test_a_duplicate_request_spends_one_offer() {
        let mut f = Fixture::new();
        let raw = f.proc.client("raw");
        let connector = f.proc.client("connector");
        let first_rx = f.post(&raw, 1, protocol(), WAIT);
        let duplicate_rx = f.post(&raw, 1, protocol(), WAIT);

        f.serve("one");
        f.serve("two");
        assert!(matches!(recv(first_rx).await, ConnectReply::Paired { .. }));
        assert_eq!(f.next_served().await, paired("peer"));
        f.assert_no_pairing().await;
        assert!(
            tokio::time::timeout(QUIET, duplicate_rx.recv())
                .await
                .is_err(),
            "a duplicate gets no reply"
        );

        assert_eq!(f.connect(&connector, "peer").await.unwrap(), "two");
    }

    /// Upholds RDV-3.
    ///
    /// A committed request posted again, long after its wait, is
    /// refused as stale and spends nothing.
    #[tokio::test]
    async fn test_a_repeat_of_a_committed_request_is_stale() {
        let mut f = Fixture::new();
        let raw = f.proc.client("raw");
        let connector = f.proc.client("connector");
        let first_rx = f.post(&raw, 1, protocol(), QUIET);
        f.serve("one");
        assert!(matches!(recv(first_rx).await, ConnectReply::Paired { .. }));
        assert_eq!(f.next_served().await, paired("peer"));

        tokio::time::sleep(QUIET * 2).await;
        f.serve("two");
        let repeat_rx = f.post(&raw, 1, protocol(), QUIET);
        assert_rejected(recv(repeat_rx).await, "stale");
        f.assert_no_pairing().await;

        assert_eq!(f.connect(&connector, "peer").await.unwrap(), "two");
    }

    /// Upholds RDV-4.
    ///
    /// A request whose wait ends with no offer gets `Expired` from the
    /// rendezvous's timer and spends nothing.
    #[tokio::test]
    async fn test_a_request_expires_without_an_offer() {
        let mut f = Fixture::new();
        let raw = f.proc.client("raw");
        let connector = f.proc.client("connector");

        let reply_rx = f.post(&raw, 1, protocol(), Duration::from_millis(100));
        assert!(matches!(recv(reply_rx).await, ConnectReply::Expired));

        f.serve("offer");
        f.assert_no_pairing().await;
        assert_eq!(f.connect(&connector, "peer").await.unwrap(), "offer");
    }

    /// Upholds RDV-4.
    ///
    /// A request whose wait already ended leaves a ready offer available.
    #[tokio::test]
    async fn test_an_expired_request_leaves_a_ready_offer() {
        let mut f = Fixture::new();
        let connector = f.proc.client("connector");
        f.serve("offer");

        // The protocol rejects "wrong" only after checking a ready offer.
        assert!(matches!(
            f.connect(&connector, "wrong").await,
            Err(ConnectError::Rejected { reason }) if reason == "wrong request"
        ));
        let raw = f.proc.client("raw");
        let expired = f.post(&raw, 1, protocol(), Duration::ZERO);
        assert!(matches!(recv(expired).await, ConnectReply::Expired));
        assert_eq!(f.connect(&connector, "peer").await.unwrap(), "offer");
        assert_eq!(f.next_served().await, paired("peer"));
    }

    /// Upholds RDV-4.
    ///
    /// With no offer, `connect` returns the rendezvous's `Expired` answer
    /// instead of timing out on its own.
    #[tokio::test]
    async fn test_connect_reports_the_rendezvous_expiry() {
        let mut f = Fixture::new();
        let connector = f.proc.client("connector");
        let wait = Duration::from_millis(100);

        let result =
            connect::<TestProtocol>(&connector, f.server.address(), "peer".to_string(), wait).await;
        assert!(
            matches!(result, Err(ConnectError::Expired)),
            "got {result:?}"
        );
        f.assert_no_pairing().await;
    }

    /// Upholds RDV-4.
    ///
    /// A cancelled queued request takes no later offer.
    #[tokio::test]
    async fn test_a_cancelled_request_spends_nothing() {
        let mut f = Fixture::new();
        let raw = f.proc.client("raw");
        let connector = f.proc.client("connector");
        let reply_rx = f.post(&raw, 1, protocol(), WAIT);
        f.cancel(&raw, 1);

        f.serve("offer");
        f.assert_no_pairing().await;
        assert!(
            tokio::time::timeout(QUIET, reply_rx.recv()).await.is_err(),
            "a cancelled request gets no pairing"
        );
        assert_eq!(f.connect(&connector, "peer").await.unwrap(), "offer");
    }

    /// Upholds RDV-4.
    ///
    /// Dropping a `connect` future after it sends its request withdraws the
    /// request, so the next offer goes to a connector that is still waiting.
    #[tokio::test]
    async fn test_a_dropped_connect_spends_nothing() {
        let mut f = Fixture::new();
        let dropped = f.proc.client("dropped");
        let connector = f.proc.client("connector");

        // The first poll sends the request. The outer timeout then drops the
        // future while the request waits for an offer.
        let abandoned = tokio::time::timeout(QUIET, f.connect(&dropped, "dropped")).await;
        assert!(abandoned.is_err(), "no offer was posted yet");

        f.serve("offer");
        assert_eq!(f.connect(&connector, "peer").await.unwrap(), "offer");
        assert_eq!(f.next_served().await, paired("peer"));
    }

    /// Upholds RDV-4.
    ///
    /// A pending `connect` dropped outside any runtime still posts its
    /// cancel, even when posting has to open a new connection.
    #[test]
    fn test_a_connect_dropped_outside_its_runtime_still_withdraws() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        // Nothing accepts on this listener until the end. It holds the port,
        // so the cancel goes to an address this proc has never dialed.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let location = format!("tcp://{}", listener.local_addr().unwrap());
        let (proc, connector, port) = {
            let _entered = runtime.enter();
            let proc = Proc::direct(
                ChannelAddr::any(ChannelTransport::Tcp(TcpMode::Localhost)),
                "connector".to_string(),
            )
            .unwrap();
            let connector = proc.client("connector");
            let address = RendezvousAddr::try_from(actor_at(&location)).unwrap();
            let mut port = address.rendezvous().port::<RendezvousRequest>();
            port.return_undeliverable(false);
            (proc, connector, port)
        };

        assert!(
            Handle::try_current().is_err(),
            "the guard drops outside any runtime"
        );
        drop(Withdraw {
            cx: &connector,
            port: &port,
            id: Some(id(&connector, 1)),
            runtime: Some(runtime.handle().clone()),
        });

        // The cancel dialed the rendezvous's address on the captured runtime.
        listener.set_nonblocking(true).unwrap();
        let dialing = std::time::Instant::now();
        while listener.accept().is_err() {
            assert!(dialing.elapsed() < WAIT, "the cancel never dialed");
            std::thread::sleep(Duration::from_millis(10));
        }

        let _entered = runtime.enter();
        drop((port, connector, proc));
    }

    /// Upholds RDV-4.
    ///
    /// A cancel seen before its request makes the request stale.
    #[tokio::test]
    async fn test_a_cancel_before_its_request_refuses_it() {
        let mut f = Fixture::new();
        let raw = f.proc.client("raw");
        let connector = f.proc.client("connector");
        f.serve("offer");

        f.cancel(&raw, 5);
        let reply_rx = f.post(&raw, 5, protocol(), WAIT);
        assert_rejected(recv(reply_rx).await, "stale");
        f.assert_no_pairing().await;

        assert_eq!(f.connect(&connector, "peer").await.unwrap(), "offer");
    }

    /// Upholds RDV-4.
    ///
    /// A cancel after the commit undoes nothing: the offer stays spent
    /// and the server keeps its result.
    #[tokio::test]
    async fn test_a_cancel_after_the_commit_is_not_rolled_back() {
        let mut f = Fixture::new();
        let raw = f.proc.client("raw");
        let reply_rx = f.post(&raw, 1, protocol(), WAIT);
        f.serve("one");
        assert!(matches!(recv(reply_rx).await, ConnectReply::Paired { .. }));
        assert_eq!(f.next_served().await, paired("peer"));

        f.cancel(&raw, 1);
        let later_rx = f.post(&raw, 2, protocol(), QUIET);
        assert!(
            matches!(recv(later_rx).await, ConnectReply::Expired),
            "the spent offer was not restored"
        );
    }

    /// Upholds RDV-5.
    ///
    /// Requests fill the configured queue, and the next is rejected.
    #[tokio::test]
    async fn test_a_full_queue_rejects_the_next_request() {
        let f = {
            let config = global::lock();
            let _queue_limit = config.override_key(RENDEZVOUS_MAX_QUEUED, 2);
            Fixture::open()
        };
        let raw = f.proc.client("raw");
        let first = f.post(&raw, 1, protocol(), WAIT);
        let second = f.post(&raw, 2, protocol(), WAIT);

        let reply_rx = f.post(&raw, 3, protocol(), WAIT);
        assert_rejected(recv(reply_rx).await, "queue is full");

        for queued in [first, second] {
            f.serve("offer");
            assert!(matches!(recv(queued).await, ConnectReply::Paired { .. }));
        }
    }

    /// Upholds RDV-5.
    ///
    /// `connect` refuses zero and unrepresentable waits before sending.
    #[tokio::test]
    async fn test_connect_refuses_an_invalid_wait() {
        let mut f = Fixture::new();
        let connector = f.proc.client("connector");
        f.serve("offer");

        for wait in [Duration::ZERO, Duration::MAX] {
            let result =
                connect::<TestProtocol>(&connector, f.server.address(), "peer".to_string(), wait)
                    .await;
            assert!(matches!(result, Err(ConnectError::InvalidWait { .. })));
        }
        f.assert_no_pairing().await;
    }

    /// Upholds RDV-5.
    ///
    /// The broker rejects a wait above its own limit and accepts a wait
    /// at that limit, even when the broker allows a wait longer than the default.
    #[tokio::test]
    async fn test_the_rendezvous_enforces_its_configured_wait() {
        let limit = Duration::from_secs(601);
        let f = {
            let config = global::lock();
            let _wait_limit = config.override_key(RENDEZVOUS_MAX_WAIT, limit);
            Fixture::open()
        };
        let connector = f.proc.client("connector");
        f.serve("offer");

        let connect = |wait| {
            connect::<TestProtocol>(&connector, f.server.address(), "peer".to_string(), wait)
        };
        let result = tokio::time::timeout(WAIT, connect(limit + Duration::from_secs(1)))
            .await
            .unwrap();
        assert!(matches!(
            result,
            Err(ConnectError::Rejected { reason }) if reason.contains("exceeds the maximum 601s")
        ));
        let offered = tokio::time::timeout(WAIT, connect(limit))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(offered, "offer");
    }

    /// Upholds RDV-5.
    ///
    /// Even with a higher configured limit, a wait that cannot be
    /// represented as a deadline is rejected.
    #[tokio::test]
    async fn test_a_wait_too_long_to_schedule_is_rejected() {
        let mut f = {
            let config = global::lock();
            let _wait_limit = config.override_key(RENDEZVOUS_MAX_WAIT, Duration::MAX);
            Fixture::open()
        };
        let raw = f.proc.client("raw");
        let reply_rx = f.post(&raw, 1, protocol(), Duration::MAX);
        assert_rejected(recv(reply_rx).await, "too long to schedule");
        f.assert_no_pairing().await;
    }

    /// Upholds RDV-6.
    ///
    /// Two requests from one instance, started at once, both pair.
    /// The offers are posted first, so each request commits as it arrives: a
    /// lower number arriving after a higher one committed would be stale.
    /// This test drives both connection futures on one task.
    #[tokio::test]
    async fn test_concurrent_requests_from_one_instance_both_pair() {
        let mut f = Fixture::new();
        let connector = f.proc.client("connector");
        f.serve("one");
        f.serve("two");

        let (first, second) = tokio::join!(
            f.connect(&connector, "first"),
            f.connect(&connector, "second"),
        );

        let mut offered = vec![first.unwrap(), second.unwrap()];
        offered.sort();
        assert_eq!(offered, ["one", "two"]);
        let mut peers = Vec::new();
        for _ in 0..2 {
            match f.next_served().await {
                Served::Paired { peer } => peers.push(peer),
                other => panic!("expected a pairing, got {other:?}"),
            }
        }
        peers.sort();
        assert_eq!(peers, ["first", "second"]);
    }

    /// Upholds RDV-6.
    ///
    /// A panic while building a request under the request-number lock
    /// poisons the lock. Later requests still get numbers and pair.
    #[tokio::test]
    async fn test_a_panic_under_the_number_lock_does_not_break_connect() {
        let mut f = Fixture::new();
        let connector = f.proc.client("connector");
        let port = f.request_port();

        let panicked = std::panic::catch_unwind(AssertUnwindSafe(|| {
            post_numbered(&connector, &port, |_| -> RendezvousRequest {
                panic!("test panic while posting")
            })
        }));
        assert!(
            panicked.is_err(),
            "building the request panicked under the lock"
        );

        f.serve("offer");
        assert_eq!(f.connect(&connector, "peer").await.unwrap(), "offer");
        assert_eq!(f.next_served().await, paired("peer"));
    }

    /// Opens a rendezvous in `init`, serves one offer to `served`, and reports
    /// the address.
    struct Owner {
        served: PortRef<Served<String>>,
        address: PortHandle<RendezvousAddr>,
        server: Option<Server<TestProtocol>>,
    }

    #[async_trait]
    impl Actor for Owner {
        async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
            let server = open::<TestProtocol>(this)?;
            server.serve(this, "offer".to_string(), self.served.clone())?;
            // A protocol rejection proves the offer reached the actor and
            // leaves the offer queued for the shutdown check.
            assert!(matches!(
                connect::<TestProtocol>(this, server.address(), "wrong".to_string(), WAIT).await,
                Err(ConnectError::Rejected { reason }) if reason == "wrong request"
            ));
            self.address.post(this, server.address().clone());
            self.server = Some(server);
            Ok(())
        }
    }

    /// Upholds RDV-7 and RDV-8.
    ///
    /// An owner that stops fails its queued offers. A connector that gets
    /// there afterwards fails at its own deadline.
    #[tokio::test]
    async fn test_an_owner_that_stops_fails_its_offers() {
        let proc = Proc::isolated();
        let observer = proc.client("observer");
        let connector = proc.client("connector");
        let (served, mut served_rx) = observer.open_port::<Served<String>>();
        let (address, mut address_rx) = observer.open_port::<RendezvousAddr>();
        let owner = proc.spawn(Owner {
            served: served.bind(),
            address,
            server: None,
        });
        let address = tokio::time::timeout(WAIT, address_rx.recv())
            .await
            .unwrap()
            .unwrap();

        owner.stop("test").unwrap();
        tokio::time::timeout(WAIT, owner).await.unwrap();
        match tokio::time::timeout(WAIT, served_rx.recv())
            .await
            .unwrap()
            .unwrap()
        {
            Served::Failed { reason } => assert_eq!(reason, "rendezvous stopped"),
            other => panic!("expected a failed offer, got {other:?}"),
        }

        let wait = Duration::from_millis(500);
        let result = connect::<TestProtocol>(&connector, &address, "peer".to_string(), wait).await;
        assert!(matches!(result, Err(ConnectError::TimedOut { .. })));
    }

    /// Upholds RDV-7.
    ///
    /// A request waiting for an offer is rejected when the rendezvous stops,
    /// instead of waiting out its deadline.
    #[tokio::test]
    async fn test_stopping_rejects_queued_requests() {
        let f = Fixture::new();
        let raw = f.proc.client("raw");
        let queued = f.post(&raw, 1, protocol(), WAIT);
        // Requests from one sender arrive in order, so this rejection
        // proves the first request is already queued.
        let mismatch = f.post(&raw, 2, "some::other::Protocol", WAIT);
        assert_rejected(recv(mismatch).await, "protocol mismatch");

        f.server.handle.stop("test").unwrap();
        assert_rejected(recv(queued).await, "rendezvous stopped");
    }

    /// Upholds RDV-8.
    ///
    /// A reply to a connector that stopped listening doesn't fault the
    /// rendezvous.
    #[tokio::test]
    async fn test_a_departed_connector_does_not_fault_the_rendezvous() {
        let mut f = Fixture::new();
        let raw = f.proc.client("raw");
        let connector = f.proc.client("connector");
        drop(f.post(&raw, 1, protocol(), Duration::from_millis(100)));
        tokio::time::sleep(QUIET).await;

        f.serve("offer");
        assert_eq!(f.connect(&connector, "peer").await.unwrap(), "offer");
        assert_eq!(f.next_served().await, paired("peer"));
    }

    /// A caller-written rendezvous holding one offer: it refuses the first
    /// request and pairs the second.
    #[derive(Debug, Default)]
    struct Picky {
        refused: bool,
    }

    impl Actor for Picky {}

    #[async_trait]
    impl Handler<RendezvousRequest> for Picky {
        async fn handle(
            &mut self,
            cx: &Context<Self>,
            request: RendezvousRequest,
        ) -> anyhow::Result<()> {
            let RendezvousRequest::Connect(connect) = request else {
                return Ok(());
            };
            let answer = if self.refused {
                ConnectReply::Paired {
                    value: wirevalue::Any::serialize(&"custom".to_string())?,
                }
            } else {
                self.refused = true;
                ConnectReply::Rejected {
                    reason: "not yet".to_string(),
                }
            };
            connect.reply.post(cx, answer);
            Ok(())
        }
    }

    /// Exercises the reply part of the module's custom rendezvous contract.
    /// A caller-written actor answers `connect` by address, first with a
    /// rejection and then with an offered value.
    #[tokio::test]
    async fn test_a_caller_written_rendezvous_answers_connect() {
        let proc = Proc::isolated();
        let connector = proc.client("connector");
        let picky = proc.spawn(Picky::default());
        let address =
            RendezvousAddr::try_from(picky.bind::<RendezvousLike>().actor_addr().clone()).unwrap();

        let first = connect::<TestProtocol>(&connector, &address, "peer".to_string(), WAIT).await;
        assert!(matches!(first, Err(ConnectError::Rejected { reason }) if reason == "not yet"));
        let second = connect::<TestProtocol>(&connector, &address, "peer".to_string(), WAIT).await;
        assert_eq!(second.unwrap(), "custom");
    }

    fn actor_at(location: &str) -> ActorAddr {
        let proc = Proc::isolated();
        let id = proc.client("probe").self_addr().id().clone();
        ActorAddr::new(id, location.parse().unwrap())
    }

    /// Upholds RDV-9.
    ///
    /// Every accepted form round-trips through its printed form,
    /// including the examples' transports.
    #[tokio::test]
    async fn test_addresses_round_trip() {
        for location in [
            "tcp://127.0.0.1:2345",
            "tcp://[::1]:2345",
            "metatls://devhost.example.com:2345",
            "quic://devhost.example.com:2345",
            "inproc://7",
            "ipc:///tmp/rendezvous.sock",
            "gateway.tcp://127.0.0.1:2345",
            "gateway.metatls://devhost.example.com:2345",
        ] {
            let address = RendezvousAddr::try_from(actor_at(location)).unwrap();
            let text = address.to_string();
            let parsed: RendezvousAddr = text.parse().unwrap();
            assert_eq!(parsed, address, "{location} round-trips");
            assert!(text.ends_with(location));
        }
    }

    /// Upholds RDV-9.
    ///
    /// Bind-only, listener-adopting, unsupported and suffixed forms are
    /// refused.
    #[tokio::test]
    async fn test_unusable_addresses_are_refused() {
        let good = RendezvousAddr::try_from(actor_at("tcp://127.0.0.1:2345")).unwrap();
        let id = good.actor_addr().id();
        let with = |location: &str| format!("{id}@{location}");

        assert!(matches!(
            format!("{good}?join=parent").parse::<RendezvousAddr>(),
            Err(AddressError::JoinUnsupported)
        ));
        assert!(matches!(
            format!("{good}?other").parse::<RendezvousAddr>(),
            Err(AddressError::Malformed { .. })
        ));
        for location in [
            "tcp://*:2345",
            "tcp://0.0.0.0:2345",
            "tcp://[::]:2345",
            "tcp://127.0.0.1:0",
            // Parsing this would adopt descriptor 5 as a listener.
            "tcp://127.0.0.1:fd5",
            "tcp://127.0.0.1:2345@tcp://0.0.0.0:2345",
            "metatls://*:2345",
            "metatls://[::]:2345",
            "metatls://0.0.0.0:2345",
            "metatls://devhost.example.com:0",
            "quic://*:2345",
            "quic://[::]:2345",
            "quic://0.0.0.0:2345",
            "quic://devhost.example.com:0",
            "quic://devhost.example.com:fd",
        ] {
            assert!(
                matches!(
                    with(location).parse::<RendezvousAddr>(),
                    Err(AddressError::BindOnly { .. })
                ),
                "{location} is bind-only"
            );
        }
        assert!(matches!(
            with("vsock://3:2345").parse::<RendezvousAddr>(),
            Err(AddressError::Unsupported { .. })
        ));
        for address in [
            "no-location".to_string(),
            with(""),
            with("tcp://127.0.0.1"),
            with("tcp://127.0.0.1:port"),
            with("quic://:2345"),
            "not an id@tcp://127.0.0.1:2345".to_string(),
        ] {
            assert!(
                matches!(
                    address.parse::<RendezvousAddr>(),
                    Err(AddressError::Malformed { .. })
                ),
                "{address} is malformed"
            );
        }
    }

    /// Upholds RDV-9.
    ///
    /// `open` refuses a proc whose location can't be dialed.
    #[tokio::test]
    async fn test_open_refuses_a_bind_only_location() {
        let proc = Proc::isolated();
        let owner = proc.client("owner");
        proc.set_default_location("tcp://[::]:0".parse().unwrap());
        assert!(matches!(
            open::<TestProtocol>(&owner),
            Err(AddressError::BindOnly { .. })
        ));
    }
}
