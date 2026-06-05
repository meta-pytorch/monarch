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
use hyperactor_config::Flattrs;
use hyperactor_config::attrs::OPERATION_CONTEXT_HEADER;
use hyperactor_config::attrs::copy_marked_flattrs;

use crate::ActorAddr;
use crate::Instance;
use crate::PortAddr;
use crate::Proc;
use crate::accum;
use crate::accum::ErasedCommReducer;
use crate::accum::ReducerMode;
use crate::accum::ReducerSpec;
use crate::config;
use crate::id::Uid;
use crate::mailbox;
use crate::mailbox::MailboxSender;
use crate::mailbox::MessageEnvelope;
use crate::ordering::SEQ_INFO;
use crate::port::Port;
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

    /// Spawn a child actor under this actor context.
    fn spawn<C: crate::Actor>(&self, actor: C) -> crate::ActorHandle<C>
    where
        Self: Sized,
    {
        self.instance().spawn(actor)
    }

    /// Spawn a child actor with a fresh uid carrying a display label.
    fn spawn_with_label<C: crate::Actor>(&self, label: &str, actor: C) -> crate::ActorHandle<C>
    where
        Self: Sized,
    {
        self.instance().spawn_with_label(label, actor)
    }

    /// Spawn a child actor using an explicit uid.
    fn spawn_with_uid<C: crate::Actor>(
        &self,
        uid: Uid,
        actor: C,
    ) -> anyhow::Result<crate::ActorHandle<C>>
    where
        Self: Sized,
    {
        self.instance().spawn_with_uid(uid, actor)
    }

    /// The inbound message headers associated with this context, if any.
    ///
    /// Plain [`Instance`] send contexts are not handling an inbound message, so
    /// they use the default empty header set.
    fn headers(&self) -> &Flattrs {
        static EMPTY_HEADERS: OnceLock<Flattrs> = OnceLock::new();
        EMPTY_HEADERS.get_or_init(Flattrs::new)
    }
}

/// An internal extension trait for Mailbox contexts.
/// TODO: consider moving this to another module.
pub(crate) trait MailboxExt: Mailbox {
    /// Post a message to the provided destination with the provided headers, and data.
    /// All messages posted from actors should use this implementation.
    fn post(
        &self,
        dest: PortAddr,
        headers: Flattrs,
        data: wirevalue::Any,
        return_undeliverable: bool,
        seq_info_policy: SeqInfoPolicy,
    );

    /// Split a port, using a provided reducer spec, if provided.
    fn split(
        &self,
        port_id: PortAddr,
        reducer_spec: Option<ReducerSpec>,
        reducer_mode: ReducerMode,
        return_undeliverable: bool,
    ) -> anyhow::Result<PortAddr>;
}

// Tracks mailboxes that have emitted a `CanSend::post` warning due to
// missing an `Undeliverable<MessageEnvelope>` binding. In this
// context, mailboxes are few and long-lived; unbounded growth is not
// a realistic concern.
static CAN_SEND_WARNED_MAILBOXES: OnceLock<DashSet<ActorAddr>> = OnceLock::new();

fn operation_context_headers(headers: &Flattrs) -> Flattrs {
    let mut operation_headers = Flattrs::new();
    copy_marked_flattrs(&mut operation_headers, headers, OPERATION_CONTEXT_HEADER);
    operation_headers
}

/// Only actors CanSend because they need a return port.
impl<T: Actor + Send + Sync> MailboxExt for T {
    fn post(
        &self,
        dest: PortAddr,
        mut headers: Flattrs,
        data: wirevalue::Any,
        return_undeliverable: bool,
        seq_info_policy: SeqInfoPolicy,
    ) {
        let return_handle = self.mailbox().bound_return_handle().unwrap_or_else(|| {
            let actor_id = self.mailbox().actor_addr();
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
            // Pair the SENDER_ACTOR_ID stamp with the seq we just assigned.
            // Helper applies the (seq<=4 || stale) gate, the handler-port +
            // non-bypass guard, and the framework-owned overwrite semantics.
            crate::mailbox::headers::stamp_sender_actor_id(
                &mut headers,
                &seq_info,
                &dest,
                self.mailbox().actor_addr(),
            );
            headers.set(SEQ_INFO, seq_info);
        }

        let mut envelope =
            MessageEnvelope::new(self.mailbox().actor_addr().clone(), dest, data, headers);
        envelope.set_return_undeliverable(return_undeliverable);
        MailboxSender::post(self.instance().proc(), envelope, return_handle);
    }

    fn split(
        &self,
        port_id: PortAddr,
        reducer_spec: Option<ReducerSpec>,
        reducer_mode: ReducerMode,
        return_undeliverable: bool,
    ) -> anyhow::Result<PortAddr> {
        fn post(
            proc: &Proc,
            sender: &ActorAddr,
            sequencer: &crate::ordering::Sequencer,
            port_id: PortAddr,
            mut headers: Flattrs,
            msg: wirevalue::Any,
            return_undeliverable: bool,
        ) {
            assert!(
                !headers.contains_key(SEQ_INFO),
                "SEQ_INFO must not be set on split-port forwarded headers"
            );
            let seq_info = sequencer.assign_seq(&port_id);
            crate::mailbox::headers::stamp_sender_actor_id(
                &mut headers,
                &seq_info,
                &port_id,
                sender,
            );
            headers.set(SEQ_INFO, seq_info);

            let mut envelope = MessageEnvelope::new(sender.clone(), port_id, msg, headers);
            envelope.set_return_undeliverable(return_undeliverable);
            mailbox::MailboxSender::post(
                proc,
                envelope,
                // TODO(pzhang) figure out how to use upstream's return handle,
                // instead of getting a new one like this.
                // This is okay for now because upstream is currently also using
                // the same handle singleton, but that could change in the future.
                mailbox::monitored_return_handle(),
            );
        }

        let port_index = self.mailbox().allocate_port();
        let split_port = self
            .mailbox()
            .actor_addr()
            .port_addr(Port::from(port_index));
        let proc = self.instance().proc().clone();
        let sender = self.mailbox().actor_addr().clone();
        let sequencer = self.instance().sequencer().clone();
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
            dyn Fn(
                    Flattrs,
                    wirevalue::Any,
                )
                    -> Result<mailbox::SerializedSendDisposition, mailbox::SerializedSendFailure>
                + Send
                + Sync,
        > = match reducer {
            None => {
                let proc = proc.clone();
                let sender = sender.clone();
                let sequencer = sequencer.clone();
                Box::new(move |headers: Flattrs, serialized: wirevalue::Any| {
                    post(
                        &proc,
                        &sender,
                        &sequencer,
                        port_id.clone(),
                        operation_context_headers(&headers),
                        serialized,
                        return_undeliverable,
                    );
                    Ok(mailbox::SerializedSendDisposition::Delivered)
                })
            }
            Some(reducer) => match reducer_mode {
                ReducerMode::Streaming(_) => {
                    let buffer: Arc<Mutex<UpdateBuffer>> =
                        Arc::new(Mutex::new(UpdateBuffer::new(reducer)));

                    let alarm = Alarm::new();

                    {
                        let mut sleeper = alarm.sleeper();
                        let buffer = Arc::clone(&buffer);
                        let port_id = port_id.clone();
                        let proc = proc.clone();
                        let sender = sender.clone();
                        let sequencer = sequencer.clone();
                        tokio::spawn(async move {
                            while sleeper.sleep().await {
                                let mut buf = buffer.lock().unwrap();
                                match buf.reduce() {
                                    None => (),
                                    Some(Ok((headers, reduced))) => post(
                                        &proc,
                                        &sender,
                                        &sequencer,
                                        port_id.clone(),
                                        headers,
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

                    let error_port_id = split_port.clone();
                    let sequencer = sequencer.clone();
                    Box::new(move |headers: Flattrs, update: wirevalue::Any| {
                        // Hold the lock until messages are sent. This is to avoid another
                        // invocation of this method trying to send message concurrently and
                        // cause messages delivered out of order.
                        //
                        // We also always acquire alarm *after* the buffer, to avoid deadlocks.
                        let mut buf = buffer.lock().unwrap();
                        match buf.push(headers.clone(), update) {
                            None => {
                                let interval = backoff.lock().unwrap().next_backoff().unwrap();
                                alarm.lock().unwrap().rearm(interval);
                                Ok(mailbox::SerializedSendDisposition::Delivered)
                            }
                            Some(Ok((headers, reduced))) => {
                                alarm.lock().unwrap().disarm();
                                post(
                                    &proc,
                                    &sender,
                                    &sequencer,
                                    port_id.clone(),
                                    headers,
                                    reduced,
                                    return_undeliverable,
                                );
                                Ok(mailbox::SerializedSendDisposition::Delivered)
                            }
                            Some(Err(error)) => Err(mailbox::SerializedSendFailure::Error(
                                mailbox::SerializedSendError {
                                    data: buf
                                        .pop()
                                        .expect("reducer error should leave update buffered"),
                                    error: crate::mailbox::MailboxSenderError::new_bound(
                                        error_port_id.clone(),
                                        crate::mailbox::MailboxSenderErrorKind::Other(error),
                                    ),
                                    headers,
                                },
                            )),
                        }
                    })
                }
                ReducerMode::Once(0) => {
                    let error_port_id = split_port.clone();
                    Box::new(move |headers: Flattrs, update: wirevalue::Any| {
                        Err(mailbox::SerializedSendFailure::Error(
                            mailbox::SerializedSendError {
                                data: update,
                                error: crate::mailbox::MailboxSenderError::new_bound(
                                    error_port_id.clone(),
                                    crate::mailbox::MailboxSenderErrorKind::Other(anyhow::anyhow!(
                                        "invalid ReducerMode: Once must specify at least one update"
                                    )),
                                ),
                                headers,
                            },
                        ))
                    })
                }
                ReducerMode::Once(expected) => {
                    let buffer: Arc<Mutex<OnceBuffer>> =
                        Arc::new(Mutex::new(OnceBuffer::new(reducer, expected)));
                    let error_port_id = split_port.clone();
                    let proc = proc.clone();
                    let sender = sender.clone();
                    let sequencer = sequencer.clone();

                    Box::new(move |headers: Flattrs, update: wirevalue::Any| {
                        let mut buf = buffer.lock().unwrap();
                        if buf.done {
                            return Err(mailbox::SerializedSendFailure::Dead {
                                data: update,
                                headers,
                            });
                        }
                        match buf.push(headers.clone(), update) {
                            Ok(Some((headers, reduced))) => {
                                post(
                                    &proc,
                                    &sender,
                                    &sequencer,
                                    port_id.clone(),
                                    headers,
                                    reduced,
                                    return_undeliverable,
                                );
                                Ok(mailbox::SerializedSendDisposition::DeliveredAndExhausted)
                            }
                            Ok(None) => Ok(mailbox::SerializedSendDisposition::Delivered),
                            Err((data, error)) => Err(mailbox::SerializedSendFailure::Error(
                                mailbox::SerializedSendError {
                                    data,
                                    error: crate::mailbox::MailboxSenderError::new_bound(
                                        error_port_id.clone(),
                                        crate::mailbox::MailboxSenderErrorKind::Other(error),
                                    ),
                                    headers,
                                },
                            )),
                        }
                    })
                }
            },
        };
        self.mailbox().bind_untyped(
            &split_port,
            mailbox::UntypedUnboundedSender { sender: enqueue },
        );
        Ok(split_port)
    }
}

struct UpdateBuffer {
    buffered: Vec<wirevalue::Any>,
    headers: Option<Flattrs>,
    reducer: Box<dyn ErasedCommReducer + Send + Sync + 'static>,
}

impl UpdateBuffer {
    fn new(reducer: Box<dyn ErasedCommReducer + Send + Sync + 'static>) -> Self {
        Self {
            buffered: Vec::new(),
            headers: None,
            reducer,
        }
    }

    fn pop(&mut self) -> Option<wirevalue::Any> {
        let value = self.buffered.pop();
        if self.buffered.is_empty() {
            self.headers = None;
        }
        value
    }

    /// Push a new item to the buffer, and optionally return any items that should
    /// be flushed.
    fn push(
        &mut self,
        headers: Flattrs,
        serialized: wirevalue::Any,
    ) -> Option<anyhow::Result<(Flattrs, wirevalue::Any)>> {
        let limit = hyperactor_config::global::get(config::SPLIT_MAX_BUFFER_SIZE);

        if self.headers.is_none() {
            self.headers = Some(operation_context_headers(&headers));
        }
        self.buffered.push(serialized);
        if self.buffered.len() >= limit {
            self.reduce()
        } else {
            None
        }
    }

    fn reduce(&mut self) -> Option<anyhow::Result<(Flattrs, wirevalue::Any)>> {
        if self.buffered.is_empty() {
            None
        } else {
            let headers = self.headers.take().unwrap_or_else(Flattrs::new);
            match self.reducer.reduce_updates(take(&mut self.buffered)) {
                Ok(reduced) => Some(Ok((headers, reduced))),
                Err((e, b)) => {
                    self.buffered = b;
                    self.headers = Some(headers);
                    Some(Err(e))
                }
            }
        }
    }
}

struct OnceBuffer {
    accumulated: Option<wirevalue::Any>,
    headers: Option<Flattrs>,
    reducer: Box<dyn ErasedCommReducer + Send + Sync + 'static>,
    expected: usize,
    count: usize,
    done: bool,
}

impl OnceBuffer {
    fn new(reducer: Box<dyn ErasedCommReducer + Send + Sync + 'static>, expected: usize) -> Self {
        Self {
            accumulated: None,
            headers: None,
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
        headers: Flattrs,
        value: wirevalue::Any,
    ) -> Result<Option<(Flattrs, wirevalue::Any)>, (wirevalue::Any, anyhow::Error)> {
        self.count += 1;
        if self.headers.is_none() {
            self.headers = Some(operation_context_headers(&headers));
        }
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
            Ok(self
                .accumulated
                .take()
                .map(|reduced| (self.headers.take().unwrap_or_else(Flattrs::new), reduced)))
        } else {
            Ok(None)
        }
    }
}
