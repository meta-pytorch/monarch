/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This module defines traits that are used as context arguments to various
//! hyperactor APIs; usually [`crate::context::Actor`], implemented by
//! [`crate::proc::Context`] (provided to actor handlers) and [`crate::proc::Instance`],
//! representing a running actor instance.
//!
//! Context traits are sealed, and thus can only be implemented by data types in the
//! core hyperactor crate.

use std::sync::Mutex;
use std::sync::OnceLock;

use async_trait::async_trait;
use dashmap::DashSet;

use crate::ActorId;
use crate::Instance;
use crate::PortId;
use crate::accum;
use crate::accum::ReducerSpec;
use crate::attrs::Attrs;
use crate::data::Serialized;
use crate::mailbox;
use crate::mailbox::MailboxSender;
use crate::mailbox::MessageEnvelope;

/// A mailbox context provides a mailbox.
pub trait Mailbox: crate::private::Sealed + Send + Sync {
    /// The mailbox associated with this context
    fn mailbox(&self) -> &crate::Mailbox;
}

/// A typed actor context, providing both a [`Mailbox`] and an [`Instance`].
///
/// Note: Send and Sync markers are here only temporarily in order to bridge
/// the transition to the context types, away from the [`crate::cap`] module.
#[async_trait]
pub trait Actor: Mailbox {
    /// The type of actor associated with this context.
    type A: crate::Actor;

    /// The instance associated with this context.
    fn instance(&self) -> &Instance<Self::A>;
}

/// An internal extension trait for Mailbox contexts.
/// TODO: consider moving this to another module.
pub(crate) trait MailboxExt: Mailbox {
    /// Post a message to the provided destination with the provided headers, and data.
    /// All messages posted from actors should use this implementation.
    fn post(&self, dest: PortId, headers: Attrs, data: Serialized);

    /// Split a port, using a provided reducer spec, if provided.
    fn split(&self, port_id: PortId, reducer_spec: Option<ReducerSpec>) -> anyhow::Result<PortId>;
}

// Tracks mailboxes that have emitted a `CanSend::post` warning due to
// missing an `Undeliverable<MessageEnvelope>` binding. In this
// context, mailboxes are few and long-lived; unbounded growth is not
// a realistic concern.
static CAN_SEND_WARNED_MAILBOXES: OnceLock<DashSet<ActorId>> = OnceLock::new();

/// Only actors CanSend because they need a return port.
impl<T: Mailbox + Send + Sync> MailboxExt for T {
    fn post(&self, dest: PortId, headers: Attrs, data: Serialized) {
        let return_handle = self.mailbox().bound_return_handle().unwrap_or_else(|| {
            let actor_id = self.mailbox().actor_id();
            if CAN_SEND_WARNED_MAILBOXES
                .get_or_init(DashSet::new)
                .insert(actor_id.clone())
            {
                let bt = std::backtrace::Backtrace::force_capture();
                tracing::warn!(
                    actor_id = ?actor_id,
                    backtrace = ?bt,
                    "mailbox attempted to post a message without binding Undeliverable<MessageEnvelope>"
                );
            }
            mailbox::monitored_return_handle()
        });

        let envelope = MessageEnvelope::new(self.mailbox().actor_id().clone(), dest, data, headers);
        MailboxSender::post(self.mailbox(), envelope, return_handle);
    }

    fn split(&self, port_id: PortId, reducer_spec: Option<ReducerSpec>) -> anyhow::Result<PortId> {
        fn post(mailbox: &mailbox::Mailbox, port_id: PortId, msg: Serialized) {
            mailbox::MailboxSender::post(
                mailbox,
                MessageEnvelope::new(mailbox.actor_id().clone(), port_id, msg, Attrs::new()),
                // TODO(pzhang) figure out how to use upstream's return handle,
                // instead of getting a new one like this.
                // This is okay for now because upstream is currently also using
                // the same handle singleton, but that could change in the future.
                mailbox::monitored_return_handle(),
            );
        }

        let port_index = self.mailbox().allocate_port();
        let split_port = self.mailbox().actor_id().port_id(port_index);
        let mailbox = self.mailbox().clone();
        let reducer = reducer_spec
            .map(
                |ReducerSpec {
                     typehash,
                     builder_params,
                 }| { accum::resolve_reducer(typehash, builder_params) },
            )
            .transpose()?
            .flatten();
        let enqueue: Box<
            dyn Fn(Serialized) -> Result<(), (Serialized, anyhow::Error)> + Send + Sync,
        > = match reducer {
            None => Box::new(move |serialized: Serialized| {
                post(&mailbox, port_id.clone(), serialized);
                Ok(())
            }),
            Some(r) => {
                let buffer = Mutex::new(mailbox::SplitPortBuffer::default());
                Box::new(move |serialized: Serialized| {
                    // Hold the lock until messages are sent. This is to avoid another
                    // invocation of this method trying to send message concurrently and
                    // cause messages delivered out of order.
                    let mut buf = buffer.lock().unwrap();
                    if let Some(buffered) = buf.push(serialized) {
                        let reduced = r.reduce_updates(buffered).map_err(|(e, mut b)| {
                            (
                                b.pop()
                                    .expect("there should be at least one update from buffer"),
                                e,
                            )
                        })?;
                        post(&mailbox, port_id.clone(), reduced);
                    }
                    Ok(())
                })
            }
        };
        self.mailbox().bind_untyped(
            &split_port,
            mailbox::UntypedUnboundedSender {
                sender: enqueue,
                port_id: split_port.clone(),
            },
        );
        Ok(split_port)
    }
}
