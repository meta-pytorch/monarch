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
    pub(crate) name: ActorName,
    pub(crate) delivery: Delivery,
    pub(crate) parent: Option<Connection>,
    pub(crate) children: SlotMap<ChildConnectionKey, Connection>,
    /// Routing table: per destination ident, where it lives (or that it is dead)
    /// *and* the monitor subscriptions this actor is responsible for on it. Monitors
    /// live on the route because they share its key and travel the same paths: a
    /// monitor registered against an `Unknown` route is forwarded up exactly like a
    /// buffered message when this actor gains a parent, and one on a `Connection`
    /// route fires when that route transitions to `Dead`.
    pub(crate) routes: HashMap<Vec<u8>, Route>,
    /// Monitors this actor *created* (it is the monitoring/`dest` side). Keyed by
    /// monitor id so they can be fired locally and cancelled. The failure_prefix
    /// stays here and never travels — the responsible ancestor only sends back
    /// `id` and we reconstruct the full failure message from this.
    pub(crate) monitors: HashMap<u64, LocalMonitor>,
    pub(crate) gateway: bool,
    pub(crate) alive: bool,
}

impl ActorEntry {
    pub(crate) fn new(ident: Option<Vec<u8>>) -> Self {
        Self {
            name: ActorName::new(ident),
            delivery: Delivery::NoPoller {
                buffered: VecDeque::new(),
            },
            parent: None,
            children: SlotMap::with_key(),
            routes: HashMap::new(),
            monitors: HashMap::new(),
            gateway: true,
            alive: true,
        }
    }

    /// Record that `ident` is live through child connection `child`, so that if that
    /// connection later fails we can mark exactly those idents dead in
    /// O(idents-handled) instead of scanning the whole routing table. Only live
    /// idents are tracked; ones already dead need no re-killing on failure.
    pub(crate) fn record_routed_via_child(&mut self, child: ChildConnectionKey, ident: &[u8]) {
        if let Some(Connection::Established { routed_idents, .. }) = self.children.get_mut(child) {
            routed_idents.insert(ident.to_vec());
        }
    }

    /// This actor's ident, or `None` if it has not been named yet.
    pub(crate) fn name(&self) -> Option<&[u8]> {
        match &self.name {
            ActorName::Named(name) => Some(name),
            ActorName::Unknown { .. } => None,
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
        match self
            .routes
            .entry(destination)
            .or_insert_with(Route::new_unknown)
        {
            Route::Unknown { messages, .. } => messages.push(parts),
            _ => unreachable!("a known route would have been used to send instead of buffering"),
        }
    }

    /// Register a monitor subscription on `to_monitor`'s route. Held on the route
    /// (creating an `Unknown` buffer if there is none yet) until the route turns
    /// `Dead` (fired) or this actor gains a parent (forwarded up, for `Unknown`).
    /// The caller guarantees the route is not `Dead` (a dead route fires at once).
    pub(crate) fn buffer_monitor(&mut self, to_monitor: Vec<u8>, sub: MonitorSub) {
        match self
            .routes
            .entry(to_monitor)
            .or_insert_with(Route::new_unknown)
        {
            Route::Unknown { monitors, .. } | Route::Connection { monitors, .. } => {
                monitors.push(sub)
            }
            Route::Dead => unreachable!("a dead route fires immediately and never registers"),
        }
    }
}

/// A destination ident's entry in an actor's routing table. Carries the monitor
/// subscriptions this actor is responsible for on that destination alongside its
/// reachability, so the two stay in lock-step (same key, same propagation path).
pub(crate) enum Route {
    /// The destination is not (yet) known here — it may not have been created.
    /// Messages and monitor subscriptions for it are buffered until a route is
    /// published (then carried into [`Route::Connection`]) or this actor gains a
    /// parent (then forwarded up).
    Unknown {
        messages: Vec<Vec<MsgPart>>,
        monitors: Vec<MonitorSub>,
    },
    /// The destination is reachable through this child connection. Monitors here
    /// fire when the route transitions to [`Route::Dead`].
    Connection {
        child: ChildConnectionKey,
        monitors: Vec<MonitorSub>,
    },
    /// The destination was reachable but has since died (directly, or because a
    /// connection on the path to it failed). Messages for it are dropped; its
    /// monitors were fired and removed at the moment of transition.
    Dead,
}

impl Route {
    pub(crate) fn new_unknown() -> Self {
        Route::Unknown {
            messages: Vec::new(),
            monitors: Vec::new(),
        }
    }

    /// The monitor subscriptions held on this route, if it can hold any (i.e. it is
    /// not `Dead`).
    pub(crate) fn monitors_mut(&mut self) -> Option<&mut Vec<MonitorSub>> {
        match self {
            Route::Unknown { monitors, .. } | Route::Connection { monitors, .. } => Some(monitors),
            Route::Dead => None,
        }
    }
}

/// A monitor created by this actor (the monitoring/`dest` side). Held until the
/// monitored actor dies or the monitor is cancelled.
pub(crate) struct LocalMonitor {
    pub(crate) to_monitor: Vec<u8>,
    pub(crate) failure_prefix: Vec<MsgPart>,
}

/// A subscription held at the responsible common ancestor `R`: when the watched
/// ident dies, route a fire to `dest` carrying `id`.
pub(crate) struct MonitorSub {
    pub(crate) dest: Vec<u8>,
    pub(crate) id: u64,
}

/// An actor's ident, which may not be known at creation (a child can be named by
/// its parent when it joins).
pub(crate) enum ActorName {
    /// Not named yet. Monitors created in the meantime cannot address their
    /// fire-backs (which route to this actor's own ident), so they wait here until
    /// a name is assigned, then subscribe for real.
    Unknown {
        pending_monitors: Vec<PendingMonitor>,
    },
    Named(Vec<u8>),
}

impl ActorName {
    pub(crate) fn new(ident: Option<Vec<u8>>) -> Self {
        match ident {
            Some(name) => ActorName::Named(name),
            None => ActorName::Unknown {
                pending_monitors: Vec::new(),
            },
        }
    }
}

/// A monitor whose subscription was deferred until its creating actor is named.
pub(crate) struct PendingMonitor {
    pub(crate) id: u64,
    pub(crate) to_monitor: Vec<u8>,
}

pub(crate) enum Delivery {
    NoPoller { buffered: VecDeque<Vec<MsgPart>> },
    Poller { poller_key: PollerKey, index: usize },
}
