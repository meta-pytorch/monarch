/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::sync::Arc;

use async_trait::async_trait;

use crate::actor::Actor;
use crate::actor::ActorHandle;
use crate::mailbox::BoxedMailboxSender;
use crate::ref_;

#[derive(Debug)]
struct LocalSpawnerState {
    root: ref_::ActorRef,
    sender: BoxedMailboxSender,
}

#[derive(Clone, Debug)]
pub(crate) struct LocalSpawner(Option<Arc<LocalSpawnerState>>);

impl LocalSpawner {
    pub(crate) fn new(root: ref_::ActorRef, sender: BoxedMailboxSender) -> Self {
        Self(Some(Arc::new(LocalSpawnerState { root, sender })))
    }

    pub(crate) fn new_panicking() -> Self {
        Self(None)
    }
}

#[async_trait]
impl CanSpawn for LocalSpawner {
    async fn spawn<A: Actor>(&self, params: A::Params) -> ActorHandle<A::Message> {
        let state = self.0.as_ref().expect("invalid spawner");
        let actor_id = state.root.unique_child();
        A::do_spawn(state.sender.clone(), actor_id.into(), params, self.clone())
            .await
            .unwrap()
    }
}

/// The trait for local spawning of actors.
#[async_trait]
pub(crate) trait CanSpawn: Clone + Send + Sync + 'static {
    /// Spawn an actor with the given params.
    async fn spawn<A: Actor>(&self, params: A::Params) -> ActorHandle<A::Message>;
}
