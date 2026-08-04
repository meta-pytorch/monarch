/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::os::fd::OwnedFd;

use tokio::sync::mpsc;

use crate::ctx::Key;
use crate::msg::MsgPart;

pub(crate) struct Delivered {
    pub(crate) index: usize,
    pub(crate) msg: Vec<MsgPart>,
}

pub(crate) struct PollerEntry {
    tx: mpsc::UnboundedSender<Delivered>,
    pub(crate) subscriptions: Vec<PollerSubscription>,
    event_fd: OwnedFd,
    delivered_count: u64,
    wake_after: Option<u64>,
}

pub(crate) struct PollerSubscription {
    index: usize,
    pub(crate) actor: Key,
}

impl PollerEntry {
    pub(crate) fn new(tx: mpsc::UnboundedSender<Delivered>, event_fd: OwnedFd) -> Self {
        Self {
            tx,
            subscriptions: Vec::new(),
            event_fd,
            delivered_count: 0,
            wake_after: Some(0),
        }
    }

    pub(crate) fn has_index(&self, index: usize) -> bool {
        self.subscriptions
            .iter()
            .any(|subscription| subscription.index == index)
    }

    pub(crate) fn insert(&mut self, index: usize, actor: Key) {
        self.subscriptions.push(PollerSubscription { index, actor });
    }

    pub(crate) fn remove(&mut self, index: usize) -> Option<Key> {
        let position = self
            .subscriptions
            .iter()
            .position(|subscription| subscription.index == index)?;
        Some(self.subscriptions.swap_remove(position).actor)
    }

    pub(crate) fn deliver(&mut self, index: usize, msg: Vec<MsgPart>) {
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

    pub(crate) fn arm(&mut self, wake_after: u64) {
        if self.delivered_count > wake_after {
            write_eventfd(&self.event_fd);
            self.wake_after = None;
        } else {
            self.wake_after = Some(wake_after);
        }
    }
}

fn write_eventfd(event_fd: &OwnedFd) {
    use std::os::fd::AsRawFd;

    let value = 1_u64.to_ne_bytes();
    unsafe {
        let _ = crate::write(event_fd.as_raw_fd(), value.as_ptr().cast(), value.len());
    }
}
