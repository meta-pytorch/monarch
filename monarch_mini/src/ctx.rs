/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::os::fd::OwnedFd;
use std::rc::Rc;
use std::thread;
use std::thread::JoinHandle;

use slotmap::SlotMap;
use slotmap::new_key_type;
use tokio::runtime;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::msg::MsgPart;

new_key_type! {
    pub(crate) struct Key;
    pub(crate) struct PollerKey;
}

pub(crate) struct Delivered {
    pub(crate) index: usize,
    pub(crate) msg: Vec<MsgPart>,
}

struct ActorEntry {
    ident: Option<Vec<u8>>,
    delivery: Delivery,
}

struct PollerEntry {
    tx: mpsc::UnboundedSender<Delivered>,
    subscriptions: Vec<PollerSubscription>,
    event_fd: OwnedFd,
    delivered_count: u64,
    wake_after: Option<u64>,
}

struct PollerSubscription {
    index: usize,
    actor: Key,
}

impl PollerEntry {
    fn new(tx: mpsc::UnboundedSender<Delivered>, event_fd: OwnedFd) -> Self {
        Self {
            tx,
            subscriptions: Vec::new(),
            event_fd,
            delivered_count: 0,
            wake_after: Some(0),
        }
    }

    fn has_index(&self, index: usize) -> bool {
        self.subscriptions
            .iter()
            .any(|subscription| subscription.index == index)
    }

    fn insert(&mut self, index: usize, actor: Key) {
        self.subscriptions.push(PollerSubscription { index, actor });
    }

    fn remove(&mut self, index: usize) -> Option<Key> {
        let position = self
            .subscriptions
            .iter()
            .position(|subscription| subscription.index == index)?;
        Some(self.subscriptions.swap_remove(position).actor)
    }

    fn deliver(&mut self, index: usize, msg: Vec<MsgPart>) {
        if self.tx.send(Delivered { index, msg }).is_err() {
            return;
        }

        self.delivered_count += 1;
        let Some(wake_after) = self.wake_after else {
            return;
        };

        if self.delivered_count > wake_after {
            write_eventfd(&self.event_fd);
            self.wake_after = None;
        }
    }

    fn arm(&mut self, wake_after: u64) {
        if self.delivered_count > wake_after {
            write_eventfd(&self.event_fd);
            self.wake_after = None;
        } else {
            self.wake_after = Some(wake_after);
        }
    }
}

struct PollerDelivery {
    poller_key: PollerKey,
    index: usize,
    msg: Vec<MsgPart>,
}

enum Delivery {
    NoPoller { buffered: VecDeque<Vec<MsgPart>> },
    Poller { poller_key: PollerKey, index: usize },
}

impl ActorEntry {
    fn new(ident: Option<Vec<u8>>) -> Self {
        Self {
            ident,
            delivery: Delivery::NoPoller {
                buffered: VecDeque::new(),
            },
        }
    }

    fn deliver(&mut self, msg: Vec<MsgPart>) -> Option<PollerDelivery> {
        match &mut self.delivery {
            Delivery::NoPoller { buffered } => {
                buffered.push_back(msg);
                None
            }
            Delivery::Poller { poller_key, index } => Some(PollerDelivery {
                poller_key: *poller_key,
                index: *index,
                msg,
            }),
        }
    }

    fn subscribe(
        &mut self,
        poller_key: PollerKey,
        index: usize,
    ) -> anyhow::Result<VecDeque<Vec<MsgPart>>> {
        let old = std::mem::replace(&mut self.delivery, Delivery::Poller { poller_key, index });

        let Delivery::NoPoller { buffered } = old else {
            self.delivery = old;
            anyhow::bail!("actor is already subscribed to a poller");
        };

        Ok(buffered)
    }

    fn unsubscribe(&mut self) {
        self.delivery = Delivery::NoPoller {
            buffered: VecDeque::new(),
        };
    }

    fn poller_subscription(&self) -> Option<(PollerKey, usize)> {
        match &self.delivery {
            Delivery::NoPoller { .. } => None,
            Delivery::Poller {
                poller_key, index, ..
            } => Some((*poller_key, *index)),
        }
    }
}

pub(crate) enum Command {
    Init {
        thread: JoinHandle<()>,
    },
    CreateActor {
        ident: Option<MsgPart>,
        done: oneshot::Sender<Key>,
    },
    DestroyActor {
        key: Key,
    },
    CreatePoller {
        tx: mpsc::UnboundedSender<Delivered>,
        event_fd: OwnedFd,
        done: oneshot::Sender<PollerKey>,
    },
    DestroyPoller {
        poller: PollerKey,
    },
    Subscribe {
        poller: PollerKey,
        index: usize,
        actor: Key,
        done: oneshot::Sender<anyhow::Result<()>>,
    },
    Unsubscribe {
        poller: PollerKey,
        index: usize,
    },
    ArmPoller {
        poller: PollerKey,
        wake_after: u64,
    },
    Send {
        sender: Key,
        receiver_ident: MsgPart,
        parts: Vec<MsgPart>,
    },
    Shutdown {
        done: oneshot::Sender<JoinHandle<()>>,
    },
}

struct Ctx {
    actors: SlotMap<Key, ActorEntry>,
    pollers: SlotMap<PollerKey, PollerEntry>,
    thread: Option<JoinHandle<()>>,
    _not_send: PhantomData<Rc<()>>,
}

impl Ctx {
    fn new() -> Self {
        Self {
            actors: SlotMap::with_key(),
            pollers: SlotMap::with_key(),
            thread: None,
            _not_send: PhantomData,
        }
    }

    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<Command>) {
        while let Some(command) = rx.recv().await {
            match command {
                Command::Init { thread } => {
                    self.thread = Some(thread);
                }
                Command::CreateActor { ident, done } => {
                    let key = self.actors.insert(ActorEntry::new(
                        ident.as_ref().map(|part| part.as_bytes().to_vec()),
                    ));
                    drop(ident);
                    let _ = done.send(key);
                }
                Command::DestroyActor { key } => {
                    if let Some(actor) = self.actors.remove(key) {
                        if let Some((poller, index)) = actor.poller_subscription() {
                            if let Some(poller) = self.pollers.get_mut(poller) {
                                let _ = poller.remove(index);
                            }
                        }
                    }
                }
                Command::CreatePoller { tx, event_fd, done } => {
                    let key = self.pollers.insert(PollerEntry::new(tx, event_fd));
                    let _ = done.send(key);
                }
                Command::DestroyPoller { poller } => {
                    if let Some(poller) = self.pollers.remove(poller) {
                        for subscription in poller.subscriptions {
                            if let Some(actor) = self.actors.get_mut(subscription.actor) {
                                actor.unsubscribe();
                            }
                        }
                    }
                }
                Command::Subscribe {
                    poller,
                    index,
                    actor,
                    done,
                } => {
                    let result = self.subscribe(poller, index, actor);
                    let _ = done.send(result);
                }
                Command::Unsubscribe { poller, index } => {
                    if let Some(poller) = self.pollers.get_mut(poller) {
                        if let Some(actor) = poller.remove(index) {
                            if let Some(actor) = self.actors.get_mut(actor) {
                                actor.unsubscribe();
                            }
                        }
                    }
                }
                Command::ArmPoller { poller, wake_after } => {
                    if let Some(poller) = self.pollers.get_mut(poller) {
                        poller.arm(wake_after);
                    }
                }
                Command::Send {
                    sender,
                    receiver_ident,
                    parts,
                } => {
                    self.send(sender, receiver_ident, parts);
                }
                Command::Shutdown { done } => {
                    let thread = self
                        .thread
                        .take()
                        .expect("context should have thread handle");
                    let _ = done.send(thread);
                    break;
                }
            }
        }
    }

    fn subscribe(&mut self, poller: PollerKey, index: usize, actor: Key) -> anyhow::Result<()> {
        let Some(poller_entry) = self.pollers.get_mut(poller) else {
            anyhow::bail!("poller does not exist");
        };
        if poller_entry.has_index(index) {
            anyhow::bail!("poller index is already subscribed");
        }

        let Some(actor_entry) = self.actors.get_mut(actor) else {
            anyhow::bail!("actor does not exist");
        };

        let buffered = actor_entry.subscribe(poller, index)?;
        poller_entry.insert(index, actor);
        for msg in buffered {
            poller_entry.deliver(index, msg);
        }
        Ok(())
    }

    fn send(&mut self, sender: Key, receiver_ident: MsgPart, parts: Vec<MsgPart>) {
        let Some(actor) = self.actors.get_mut(sender) else {
            return;
        };

        if actor.ident.as_deref() == Some(receiver_ident.as_bytes()) {
            let delivery = actor.deliver(parts);
            if let Some(delivery) = delivery {
                if let Some(poller) = self.pollers.get_mut(delivery.poller_key) {
                    poller.deliver(delivery.index, delivery.msg);
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct CtxHandle {
    tx: mpsc::UnboundedSender<Command>,
}

fn write_eventfd(event_fd: &OwnedFd) {
    use std::os::fd::AsRawFd;

    let value = 1_u64.to_ne_bytes();
    unsafe {
        let _ = crate::write(event_fd.as_raw_fd(), value.as_ptr().cast(), value.len());
    }
}

impl CtxHandle {
    pub fn new() -> anyhow::Result<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        let thread = thread::Builder::new()
            .name("monarch-mini".to_owned())
            .spawn(move || {
                let rt = runtime::Builder::new_current_thread()
                    .build()
                    .expect("tokio runtime should build");
                let local = tokio::task::LocalSet::new();
                local.block_on(&rt, Ctx::new().run(rx));
            })?;

        let handle = Self { tx };
        handle.send_command(Command::Init { thread })?;
        Ok(handle)
    }

    pub(crate) fn send_command(&self, command: Command) -> anyhow::Result<()> {
        self.tx
            .send(command)
            .map_err(|_| anyhow::anyhow!("context runtime stopped"))
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot;

    use super::Command;
    use super::CtxHandle;

    #[test]
    fn context_starts_and_stops_runtime_thread() {
        let ctx = CtxHandle::new().expect("context should start");
        let (done_tx, done_rx) = oneshot::channel();
        ctx.send_command(Command::Shutdown { done: done_tx })
            .expect("shutdown should send");
        done_rx
            .blocking_recv()
            .expect("shutdown should return thread")
            .join()
            .expect("thread should join");
        drop(ctx);
    }
}
