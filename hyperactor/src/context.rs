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

use std::mem::take;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;

use async_trait::async_trait;
use backoff::ExponentialBackoffBuilder;
use backoff::backoff::Backoff;
use dashmap::DashSet;
use hyperactor_config::attrs::Attrs;

use crate::ActorId;
use crate::Instance;
use crate::PortId;
use crate::accum;
use crate::accum::ErasedCommReducer;
use crate::accum::ReducerMode;
use crate::accum::ReducerSpec;
use crate::config;
use crate::mailbox;
use crate::mailbox::MailboxSender;
use crate::mailbox::MessageEnvelope;
use crate::ordering::SEQ_INFO;
use crate::time::Alarm;

/// Policy for handling SEQ_INFO in message headers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SeqInfoPolicy {
    /// Assign a new sequence number. Panics if SEQ_INFO is already set.
    AssignNew,
    /// Allow externally-set SEQ_INFO. Used only by CommActor for mesh routing.
    AllowExternal,
}

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
    fn post(
        &self,
        dest: PortId,
        headers: Attrs,
        data: wirevalue::Any,
        return_undeliverable: bool,
        seq_info_policy: SeqInfoPolicy,
    );

    /// Split a port, using a provided reducer spec, if provided.
    fn split(
        &self,
        port_id: PortId,
        reducer_spec: Option<ReducerSpec>,
        reducer_mode: ReducerMode,
        return_undeliverable: bool,
    ) -> anyhow::Result<PortId>;
}

// Tracks mailboxes that have emitted a `CanSend::post` warning due to
// missing an `Undeliverable<MessageEnvelope>` binding. In this
// context, mailboxes are few and long-lived; unbounded growth is not
// a realistic concern.
static CAN_SEND_WARNED_MAILBOXES: OnceLock<DashSet<ActorId>> = OnceLock::new();

/// Only actors CanSend because they need a return port.
impl<T: Actor + Send + Sync> MailboxExt for T {
    fn post(
        &self,
        dest: PortId,
        mut headers: Attrs,
        data: wirevalue::Any,
        return_undeliverable: bool,
        seq_info_policy: SeqInfoPolicy,
    ) {
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

        assert!(
            !headers.contains_key(SEQ_INFO) || seq_info_policy == SeqInfoPolicy::AllowExternal,
            "SEQ_INFO must not be set on headers outside of fn post unless explicitly allowed"
        );

        if !headers.contains_key(SEQ_INFO) {
            // This method is infallible so is okay to assign the sequence number
            // without worrying about rollback.
            let sequencer = self.instance().sequencer();
            let seq_info = sequencer.assign_seq(&dest);
            headers.set(SEQ_INFO, seq_info);
        }

        let mut envelope =
            MessageEnvelope::new(self.mailbox().actor_id().clone(), dest, data, headers);
        envelope.set_return_undeliverable(return_undeliverable);
        MailboxSender::post(self.mailbox(), envelope, return_handle);
    }

    fn split(
        &self,
        port_id: PortId,
        reducer_spec: Option<ReducerSpec>,
        reducer_mode: ReducerMode,
        return_undeliverable: bool,
    ) -> anyhow::Result<PortId> {
        fn post(
            mailbox: &mailbox::Mailbox,
            port_id: PortId,
            msg: wirevalue::Any,
            return_undeliverable: bool,
        ) {
            let mut envelope =
                MessageEnvelope::new(mailbox.actor_id().clone(), port_id, msg, Attrs::new());
            envelope.set_return_undeliverable(return_undeliverable);
            mailbox::MailboxSender::post(
                mailbox,
                envelope,
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
            dyn Fn(wirevalue::Any) -> Result<bool, (wirevalue::Any, anyhow::Error)> + Send + Sync,
        > = match reducer {
            None => Box::new(move |serialized: wirevalue::Any| {
                post(&mailbox, port_id.clone(), serialized, return_undeliverable);
                Ok(true)
            }),
            Some(reducer) => match reducer_mode {
                ReducerMode::Streaming(_) => {
                    let buffer: Arc<Mutex<UpdateBuffer>> =
                        Arc::new(Mutex::new(UpdateBuffer::new(reducer)));

                    let alarm = Alarm::new();

                    {
                        let mut sleeper = alarm.sleeper();
                        let buffer = Arc::clone(&buffer);
                        let port_id = port_id.clone();
                        let mailbox = mailbox.clone();
                        tokio::spawn(async move {
                            while sleeper.sleep().await {
                                let mut buf = buffer.lock().unwrap();
                                match buf.reduce() {
                                    None => (),
                                    Some(Ok(reduced)) => post(
                                        &mailbox,
                                        port_id.clone(),
                                        reduced,
                                        return_undeliverable,
                                    ),
                                    // We simply ignore errors here, and let them be propagated
                                    // later in the enqueueing function.
                                    //
                                    // If this is the last update, then this strategy will cause a hang.
                                    // We should obtain a supervisor here from our send context and notify
                                    // it.
                                    Some(Err(e)) => tracing::error!(
                                        "error while reducing update: {}; waiting until the next send to propagate",
                                        e
                                    ),
                                }
                            }
                        });
                    }

                    // Note: alarm is held in the closure while the port is active;
                    // when it is dropped, the alarm terminates, and so does the sleeper
                    // task.
                    let alarm = Mutex::new(alarm);

                    let max_interval = reducer_mode.max_update_interval();
                    let initial_interval = reducer_mode.initial_update_interval();

                    // Create exponential backoff for buffer flush interval, starting at
                    // initial_interval and growing to max_interval
                    let backoff = Mutex::new(
                        ExponentialBackoffBuilder::new()
                            .with_initial_interval(initial_interval)
                            .with_multiplier(2.0)
                            .with_max_interval(max_interval)
                            .with_max_elapsed_time(None)
                            .build(),
                    );

                    Box::new(move |update: wirevalue::Any| {
                        // Hold the lock until messages are sent. This is to avoid another
                        // invocation of this method trying to send message concurrently and
                        // cause messages delivered out of order.
                        //
                        // We also always acquire alarm *after* the buffer, to avoid deadlocks.
                        let mut buf = buffer.lock().unwrap();
                        match buf.push(update) {
                            None => {
                                let interval = backoff.lock().unwrap().next_backoff().unwrap();
                                alarm.lock().unwrap().rearm(interval);
                                Ok(true)
                            }
                            Some(Ok(reduced)) => {
                                alarm.lock().unwrap().disarm();
                                post(&mailbox, port_id.clone(), reduced, return_undeliverable);
                                Ok(true)
                            }
                            Some(Err(e)) => Err((buf.pop().unwrap(), e)),
                        }
                    })
                }
                ReducerMode::Once(0) => Box::new(move |update: wirevalue::Any| {
                    Err((
                        update,
                        anyhow::anyhow!(
                            "invalid ReducerMode: Once must specify at least one update"
                        ),
                    ))
                }),
                ReducerMode::Once(expected) => {
                    let buffer: Arc<Mutex<OnceBuffer>> =
                        Arc::new(Mutex::new(OnceBuffer::new(reducer, expected)));

                    Box::new(move |update: wirevalue::Any| {
                        let mut buf = buffer.lock().unwrap();
                        if buf.done {
                            return Err((
                                update,
                                anyhow::anyhow!("OnceReducer has already emitted"),
                            ));
                        }
                        match buf.push(update) {
                            Ok(Some(reduced)) => {
                                post(&mailbox, port_id.clone(), reduced, return_undeliverable);
                                Ok(false) // Done, tear down the port
                            }
                            Ok(None) => Ok(true),
                            Err(e) => Err(e),
                        }
                    })
                }
            },
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

struct UpdateBuffer {
    buffered: Vec<wirevalue::Any>,
    reducer: Box<dyn ErasedCommReducer + Send + Sync + 'static>,
}

impl UpdateBuffer {
    fn new(reducer: Box<dyn ErasedCommReducer + Send + Sync + 'static>) -> Self {
        Self {
            buffered: Vec::new(),
            reducer,
        }
    }

    fn pop(&mut self) -> Option<wirevalue::Any> {
        self.buffered.pop()
    }

    /// Push a new item to the buffer, and optionally return any items that should
    /// be flushed.
    fn push(&mut self, serialized: wirevalue::Any) -> Option<anyhow::Result<wirevalue::Any>> {
        let limit = hyperactor_config::global::get(config::SPLIT_MAX_BUFFER_SIZE);

        self.buffered.push(serialized);
        if self.buffered.len() >= limit {
            self.reduce()
        } else {
            None
        }
    }

    fn reduce(&mut self) -> Option<anyhow::Result<wirevalue::Any>> {
        if self.buffered.is_empty() {
            None
        } else {
            match self.reducer.reduce_updates(take(&mut self.buffered)) {
                Ok(reduced) => Some(Ok(reduced)),
                Err((e, b)) => {
                    self.buffered = b;
                    Some(Err(e))
                }
            }
        }
    }
}

struct OnceBuffer {
    accumulated: Option<wirevalue::Any>,
    reducer: Box<dyn ErasedCommReducer + Send + Sync + 'static>,
    expected: usize,
    count: usize,
    done: bool,
}

impl OnceBuffer {
    fn new(reducer: Box<dyn ErasedCommReducer + Send + Sync + 'static>, expected: usize) -> Self {
        Self {
            accumulated: None,
            reducer,
            expected,
            count: 0,
            done: false,
        }
    }

    /// Push a new value and reduce incrementally. Returns Ok(Some(reduced)) when
    /// the expected count is reached, Ok(None) while still accumulating. On error,
    /// the buffer is broken and returns the rejected value.
    fn push(
        &mut self,
        value: wirevalue::Any,
    ) -> Result<Option<wirevalue::Any>, (wirevalue::Any, anyhow::Error)> {
        self.count += 1;
        self.accumulated = match self.accumulated.take() {
            None => Some(value),
            Some(acc) => match self.reducer.reduce_updates(vec![acc, value]) {
                Ok(reduced) => Some(reduced),
                Err((e, mut rejected)) => {
                    return Err((
                        rejected
                            .pop()
                            .unwrap_or_else(|| wirevalue::Any::serialize(&()).unwrap()),
                        e,
                    ));
                }
            },
        };
        if self.count >= self.expected {
            self.done = true;
            Ok(self.accumulated.take())
        } else {
            Ok(None)
        }
    }
}
