/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::collections::VecDeque;

use slotmap::SlotMap;

use crate::connection::Connection;
use crate::ctx::ChildConnectionKey;
use crate::ctx::PollerKey;
use crate::msg::MsgPart;

pub(crate) struct ActorEntry {
    pub(crate) ident: Option<Vec<u8>>,
    pub(crate) delivery: Delivery,
    pub(crate) parent: Option<Connection>,
    pub(crate) children: SlotMap<ChildConnectionKey, Connection>,
    pub(crate) routes: HashMap<Vec<u8>, Route>,
    pub(crate) gateway: bool,
    pub(crate) alive: bool,
}

impl ActorEntry {
    pub(crate) fn new(ident: Option<Vec<u8>>) -> Self {
        Self {
            ident,
            delivery: Delivery::NoPoller {
                buffered: VecDeque::new(),
            },
            parent: None,
            children: SlotMap::with_key(),
            routes: HashMap::new(),
            gateway: true,
            alive: true,
        }
    }

    pub(crate) fn subscribe(
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

    pub(crate) fn unsubscribe(&mut self) {
        self.delivery = Delivery::NoPoller {
            buffered: VecDeque::new(),
        };
    }

    pub(crate) fn poller_subscription(&self) -> Option<(PollerKey, usize)> {
        match &self.delivery {
            Delivery::NoPoller { .. } => None,
            Delivery::Poller {
                poller_key, index, ..
            } => Some((*poller_key, *index)),
        }
    }

    /// Buffer a message addressed to a destination whose route is not yet known
    /// here, so it can be flushed once the route becomes known or this actor gains
    /// a parent. This is the only place messages enter a [`Route::Unknown`] buffer.
    pub(crate) fn buffer_unrouted(&mut self, destination: Vec<u8>, parts: Vec<MsgPart>) {
        let Route::Unknown(buffered) = self
            .routes
            .entry(destination)
            .or_insert_with(|| Route::Unknown(Vec::new()))
        else {
            unreachable!("a known route would have been used to send instead of buffering");
        };
        buffered.push(parts);
    }

    /// Remove every buffered (unrouted) destination, returning each with its pending
    /// messages and leaving only concrete routes behind. Used when the actor gains a
    /// parent: the messages are re-routed up to the parent and no longer buffered here.
    pub(crate) fn drain_buffered_routes(&mut self) -> Vec<(Vec<u8>, Vec<Vec<MsgPart>>)> {
        let mut buffered = Vec::new();
        let mut kept = HashMap::new();
        for (ident, route) in std::mem::take(&mut self.routes) {
            match route {
                Route::Unknown(messages) => buffered.push((ident, messages)),
                Route::Connection(_) => {
                    kept.insert(ident, route);
                }
            }
        }
        self.routes = kept;
        buffered
    }
}

/// A destination ident's entry in an actor's routing table.
pub(crate) enum Route {
    /// The destination is not (yet) known here — it may not have been created.
    /// Messages for it are buffered until a route is published, then flushed.
    Unknown(Vec<Vec<MsgPart>>),
    /// The destination is reachable through this child connection.
    Connection(ChildConnectionKey),
}

pub(crate) enum Delivery {
    NoPoller { buffered: VecDeque<Vec<MsgPart>> },
    Poller { poller_key: PollerKey, index: usize },
}
