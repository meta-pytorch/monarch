/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use async_trait::async_trait;
use tokio::sync::watch;

use crate::Actor;
use crate::ActorHandle;
use crate::Context;
use crate::Handler;
use crate::proc::Proc;
use crate::supervision::ActorSupervisionEvent;

/// Used to create a proc supervison coordinator for testing purposes. Normally you
/// should not use this struct. It is only required in the following cases:
///   1. The tests' logic involves actor failures;
///   2. A supervison coordinator is not already set for the proc (e.g. the
///      ProcActor scenario which will be explained later.)
///
///   This is because hyperactor's supervision logic requires actor failures in
///   a proc to be bubbled up to through the supervision chain:
///
///   grandchild actor -> child actor -> root actor -> proc supervison coordinator
///
///   If the the proc supervison coordinator is not set, supervision will crash the
///   process because it cannot find the coordinator during the "bubbling up".
#[derive(Debug)]
pub struct ProcSupervisionCoordinator(watch::Sender<Option<ActorSupervisionEvent>>);

impl ProcSupervisionCoordinator {
    /// Spawn a coordinator actor and set it as the coordinator for the given
    /// proc. Returns the reported event state and the coordinator actor handle.
    /// Callers should drain and stop the coordinator handle when the test is
    /// done to avoid ASAN thread-leak errors.
    pub async fn set(
        proc: &Proc,
    ) -> Result<(ReportedEvent, ActorHandle<ProcSupervisionCoordinator>), anyhow::Error> {
        let (tx, rx) = watch::channel(None);
        let actor = ProcSupervisionCoordinator(tx);
        let coordinator = proc.spawn::<ProcSupervisionCoordinator>("coordinator", actor)?;
        proc.set_supervision_coordinator(coordinator.port())?;
        Ok((ReportedEvent(rx), coordinator))
    }
}

/// Used to store the last event reported to [ProcSupervisionCoordinator].
#[derive(Clone, Debug)]
pub struct ReportedEvent(watch::Receiver<Option<ActorSupervisionEvent>>);

impl ReportedEvent {
    /// The last event reported to the coordinator.
    pub fn event(&self) -> Option<ActorSupervisionEvent> {
        self.0.borrow().clone()
    }

    /// Wait until the coordinator receives an event.
    pub async fn recv(&mut self) -> ActorSupervisionEvent {
        self.0
            .wait_for(|v| v.is_some())
            .await
            .expect("coordinator sender dropped without sending an event")
            .clone()
            .unwrap()
    }
}

#[async_trait]
impl Actor for ProcSupervisionCoordinator {}

#[async_trait]
impl Handler<ActorSupervisionEvent> for ProcSupervisionCoordinator {
    async fn handle(
        &mut self,
        _cx: &Context<Self>,
        msg: ActorSupervisionEvent,
    ) -> anyhow::Result<()> {
        tracing::debug!("in handler, handling supervision event");
        self.0.send_replace(Some(msg));
        Ok(())
    }
}
