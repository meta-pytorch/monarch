/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use async_trait::async_trait;

use crate::actor::Actor;
use crate::actor::ActorHandle;
use crate::mailbox::BoxedMailboxSender;
use crate::reference::ActorId;
#[derive(Debug)]
struct LocalSpawnerState {
    root: ActorId,
    sender: BoxedMailboxSender,
    next_pid: AtomicU64,
}

#[derive(Clone, Debug)]
pub(crate) struct LocalSpawner(Option<Arc<LocalSpawnerState>>);

impl LocalSpawner {
    pub(crate) fn new(root: ActorId, sender: BoxedMailboxSender) -> Self {
        Self(Some(Arc::new(LocalSpawnerState {
            root,
            sender,
            next_pid: AtomicU64::new(1),
        })))
    }

    pub(crate) fn new_panicking() -> Self {
        Self(None)
    }
}

#[async_trait]
impl CanSpawn for LocalSpawner {
    async fn spawn<A: Actor>(&self, params: A::Params) -> ActorHandle<A::Message> {
        let state = self.0.as_ref().expect("invalid spawner");
        let pid = state.next_pid.fetch_add(1, Ordering::Relaxed);
        let actor_id = state.root.child_id(pid);
        A::do_spawn(state.sender.clone(), actor_id, params, self.clone())
            .await
            .unwrap()
    }
}
