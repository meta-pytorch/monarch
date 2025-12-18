/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Hyperactor is an actor system intended for managing large scale compute.
//!
//! # Actor data model
//!
//! Hyperactor is designed to support large scale (millions of nodes)
//! machine learning workloads where actor topologies communicate through
//! high fanout multicast messaging.
//!
//! Supporting this scale requires us to impose additional structure
//! at the level of the framework, so that we can efficiently refer to
//! _gangs_ of actors that implement the same worker runtimes.
//!
//! Similarly, Hyperactor must gang-schedule actors in order to support
//! collective communicaton between actors.
//!
//! Hyperactor is organized into a hierarchy, wherein parents manage
//! the lifecycle of their children:
//!
//! * Each _world_ represents a fixed number of _procs_, scheduled as
//!   a gang.
//! * Each _proc_ represents a single actor runtime instance, and hosts
//!   zero or more actors.
//! * Actors are _spawned_ into worlds, and assigned a global name.
//!   Actors spawned in this way are assigned a local PID (pid) of 0.
//!   Actors in turn can spawn local actors. These inherit the global pid
//!   of their parent, but receive a unique pid.
//!
//! Actors that share a name within a world are called a _gang_.
//!
//! This scheme confers several benefits:
//!
//! * Routing of messages can be performed by prefix. For example, we
//!   can route a message to an actor based on the world the actor belongs
//!   to; from there, we can identify the _proc_ of the actor and send
//!   the message to it, which can then in turn be routed locally.
//!
//! * We can represent gangs of actors in a uniform and compact way.
//!   This is the basis on which we implement efficient multicasting
//!   within the system.
//!
//!
//! | Entity    | Identifier                |
//! |-----------|---------------------------|
//! | World     | `worldid`                 |
//! | Proc      | `worldid[rank]`           |
//! | Actor     | `worldid[rank].name[pid]` |
//! | Gang      | `worldid.name`            |

#![feature(anonymous_lifetime_in_impl_trait)]
#![feature(assert_matches)]
#![feature(associated_type_defaults)]
#![feature(box_patterns)]
#![feature(btree_cursors)]
#![feature(error_reporter)]
#![feature(impl_trait_in_assoc_type)]
#![feature(never_type)]
#![feature(panic_update_hook)]
#![feature(type_alias_impl_trait)]
#![feature(trait_alias)]
#![deny(missing_docs)]

pub mod accum;
pub mod actor;
pub mod channel;
pub mod checkpoint;
pub mod clock;
pub mod config;
pub mod context;
pub mod data;
pub mod host;
mod init;
pub mod mailbox;
pub mod message;
pub mod metrics;
mod ordering;
pub mod panic_handler;
pub mod proc;
pub mod reference;
mod signal_handler;
pub mod simnet;
mod stdio_redirect;
pub mod supervision;
pub mod sync;
/// Test utilities
pub mod test_utils;
pub mod time;

pub use actor::Actor;
pub use actor::ActorHandle;
pub use actor::Handler;
pub use actor::RemoteHandles;
pub use actor::RemoteSpawn;
// Re-export public dependencies of hyperactor_macros codegen.
#[doc(hidden)]
pub use anyhow;
#[doc(hidden)]
pub use async_trait;
// Re-exported to use in Named derive macro.
#[doc(hidden)]
pub use cityhasher;
#[doc(hidden)]
pub use dashmap; // For intern_typename!
pub use data::Named;
#[doc(inline)]
pub use hyperactor_macros::Bind;
#[doc(inline)]
pub use hyperactor_macros::HandleClient;
#[doc(inline)]
pub use hyperactor_macros::Handler;
#[doc(inline)]
pub use hyperactor_macros::Named;
#[doc(inline)]
pub use hyperactor_macros::RefClient;
#[doc(inline)]
pub use hyperactor_macros::Unbind;
#[doc(inline)]
pub use hyperactor_macros::behavior;
#[doc(inline)]
pub use hyperactor_macros::export;
#[doc(inline)]
pub use hyperactor_macros::forward;
#[doc(inline)]
pub use hyperactor_macros::instrument;
#[doc(inline)]
pub use hyperactor_macros::instrument_infallible;
pub use hyperactor_macros::observe_async;
pub use hyperactor_macros::observe_result;
#[doc(hidden)]
pub use hyperactor_named; // For declare_attrs! macro
pub use hyperactor_telemetry::declare_static_counter;
pub use hyperactor_telemetry::declare_static_gauge;
pub use hyperactor_telemetry::declare_static_histogram;
pub use hyperactor_telemetry::declare_static_timer;
pub use hyperactor_telemetry::key_value;
pub use hyperactor_telemetry::kv_pairs;
#[doc(inline)]
pub use init::get_runtime;
#[doc(inline)]
pub use init::initialize;
#[doc(inline)]
pub use init::initialize_with_current_runtime;
#[doc(inline)]
pub use init::initialize_with_log_prefix;
// Re-exported to make this available to callers of the `register!` macro.
#[doc(hidden)]
pub use inventory::submit;
pub use mailbox::Data;
pub use mailbox::Mailbox;
pub use mailbox::Message;
pub use mailbox::OncePortHandle;
pub use mailbox::PortHandle;
pub use mailbox::RemoteMessage;
// Re-exported to support opentelemetry in hyperactor_macros codegen.
#[doc(hidden)]
pub use opentelemetry;
#[doc(hidden)]
pub use paste::paste;
pub use proc::Context;
pub use proc::Instance;
pub use proc::Proc;
pub use reference::ActorId;
pub use reference::ActorRef;
pub use reference::GangId;
pub use reference::GangRef;
pub use reference::OncePortRef;
pub use reference::PortId;
pub use reference::PortRef;
pub use reference::ProcId;
pub use reference::WorldId;
// Re-exported to support tracing in hyperactor_macros codegen.
#[doc(hidden)]
pub use serde_json;
#[doc(inline)]
pub use signal_handler::SignalCleanupGuard;
#[doc(inline)]
pub use signal_handler::SignalDisposition;
#[doc(inline)]
pub use signal_handler::query_signal_disposition;
#[doc(inline)]
pub use signal_handler::register_signal_cleanup;
#[doc(inline)]
pub use signal_handler::register_signal_cleanup_scoped;
#[doc(inline)]
pub use signal_handler::sigpipe_disposition;
#[doc(inline)]
pub use signal_handler::unregister_signal_cleanup;
// Re-exported to support tracing in hyperactor_macros codegen.
#[doc(hidden)]
pub use tracing;

mod private {
    /// Public trait in a private module for sealing traits within this crate:
    /// [Sealed trait pattern](https://rust-lang.github.io/api-guidelines/future-proofing.html#sealed-traits-protect-against-downstream-implementations-c-sealed).
    pub trait Sealed {}

    // These two implement context capabilities:
    impl<A: crate::Actor> Sealed for crate::proc::Instance<A> {}
    impl<A: crate::Actor> Sealed for &crate::proc::Instance<A> {}
    impl<A: crate::Actor> Sealed for crate::proc::Context<'_, A> {}
    impl<A: crate::Actor> Sealed for &crate::proc::Context<'_, A> {}
    impl Sealed for crate::mailbox::Mailbox {}
    impl Sealed for &crate::mailbox::Mailbox {}
}
