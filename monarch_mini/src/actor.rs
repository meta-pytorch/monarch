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
use crate::connection::SendPayload;
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
    /// Monitors this actor *created* (it is the monitoring/`dest` side), grouped by
    /// the *monitored target* ident. All of an actor's monitors on one target share
    /// a single upstream subscription: the first creates it, the rest just join,
    /// and they all fire together when that target dies (a fire carries only the
    /// dead ident — no per-monitor id is needed on the wire). The matching
    /// `Unsubscribe` is debounced via [`TargetMonitors::unsub_timer`], so rapid
    /// monitor/cancel churn on one target sends no per-cycle traffic.
    pub(crate) monitored: HashMap<Vec<u8>, TargetMonitors>,
    pub(crate) gateway: bool,
    pub(crate) alive: bool,
}

/// State of this actor's monitoring of one target ident. The variant *is* the
/// state — no generation bookkeeping — so a debounced unsubscribe is correct by
/// construction: re-monitoring aborts the pending task and flips back to
/// `Active`, and the timer's command only acts if the entry is still
/// `PendingUnsubscribe` when it is processed.
pub(crate) enum TargetMonitors {
    /// Live local monitors: monitor id → its failure-message prefix. The prefix
    /// never travels; the full message is reconstructed here when the target
    /// dies. (For an unnamed actor these are recorded but the upstream `Subscribe`
    /// is held until naming.)
    Active(HashMap<u64, Vec<MsgPart>>),
    /// Every monitor on this target was cancelled; the upstream subscription is
    /// still live, but a debounced unsubscribe task is scheduled. Holds that
    /// task's abort handle so re-monitoring (or a fire) cancels it.
    PendingUnsubscribe(tokio::task::AbortHandle),
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
            monitored: HashMap::new(),
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

    /// Buffer a payload addressed to a destination whose route is not yet known
    /// here, so it can be flushed once the route becomes known or this actor gains
    /// a parent. This is the only place payloads enter a [`Route::Unknown`] buffer.
    pub(crate) fn buffer_unrouted(&mut self, destination: Vec<u8>, payload: SendPayload) {
        match self
            .routes
            .entry(destination)
            .or_insert_with(Route::new_unknown)
        {
            Route::Unknown { messages, .. } => messages.push(payload),
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
        messages: Vec<SendPayload>,
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

/// A subscription held at the responsible common ancestor `R`: when the watched
/// ident dies, route a fire down to `dest` (the monitoring actor). One per
/// monitoring actor per target — the dead ident itself identifies the fire, so
/// no per-monitor id is carried.
pub(crate) struct MonitorSub {
    pub(crate) dest: Vec<u8>,
}

/// An actor's ident, which may not be known at creation (a child can be named by
/// its parent when it joins).
pub(crate) enum ActorName {
    /// Not named yet. Monitors created in the meantime cannot address their
    /// fire-backs (which route to this actor's own ident), so their upstream
    /// `Subscribe` is held (in `monitored`) until a name is assigned, then sent.
    Unknown {},
    Named(Vec<u8>),
}

impl ActorName {
    pub(crate) fn new(ident: Option<Vec<u8>>) -> Self {
        match ident {
            Some(name) => ActorName::Named(name),
            None => ActorName::Unknown {},
        }
    }
}

pub(crate) enum Delivery {
    NoPoller { buffered: VecDeque<Vec<MsgPart>> },
    Poller { poller_key: PollerKey, index: usize },
}
