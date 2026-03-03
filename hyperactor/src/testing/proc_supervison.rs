/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::Actor;
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
pub struct ProcSupervisionCoordinator(ReportedEvent);

impl ProcSupervisionCoordinator {
    /// Spawn a coordinator actor and set it as the coordinator for the given
    /// proc.
    pub async fn set(proc: &Proc) -> Result<ReportedEvent, anyhow::Error> {
        let state = ReportedEvent::new();
        let actor = ProcSupervisionCoordinator(state.clone());
        let coordinator = proc.spawn::<ProcSupervisionCoordinator>("coordinator", actor)?;
        proc.set_supervision_coordinator(coordinator.port())?;
        Ok(state)
    }
}

/// Used to store the last event reported to [ProcSupervisionCoordinator].
#[derive(Clone, Debug)]
pub struct ReportedEvent(Arc<Mutex<Option<ActorSupervisionEvent>>>);
impl ReportedEvent {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(None)))
    }

    /// The last event reported to the coordinator.
    pub fn event(&self) -> Option<ActorSupervisionEvent> {
        self.0.lock().unwrap().clone()
    }

    fn set(&self, event: ActorSupervisionEvent) {
        *self.0.lock().unwrap() = Some(event);
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
        self.0.set(msg);
        Ok(())
    }
}
