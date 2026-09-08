/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;

use chrysalis_core::Pid;
use chrysalis_transport::Route;
use chrysalis_transport::RouteGate;
use chrysalis_transport::Router;

use crate::DirectoryChange;
use crate::LinkId;
use crate::ProcEntry;

#[derive(Clone, Debug)]
pub(crate) struct RouteProjector {
    inner: Arc<RouteProjectorInner>,
}

#[derive(Debug)]
struct RouteProjectorInner {
    router: Arc<Router>,
    links: Mutex<BTreeMap<LinkId, LinkRoutes>>,
}

#[derive(Debug)]
struct LinkRoutes {
    gate: RouteGate,
    targets: BTreeSet<Pid>,
}

impl RouteProjector {
    pub(crate) fn new(router: Arc<Router>) -> Self {
        Self {
            inner: Arc::new(RouteProjectorInner {
                router,
                links: Mutex::new(BTreeMap::new()),
            }),
        }
    }

    pub(crate) fn open(&self, link: LinkId) -> RouteSession {
        let state = LinkRoutes {
            gate: RouteGate::new(),
            targets: BTreeSet::new(),
        };
        let replaced = self
            .inner
            .links
            .lock()
            .expect("route projector lock poisoned")
            .insert(link, state);
        assert!(replaced.is_none(), "link route gate must be one-shot");
        RouteSession {
            projector: self.clone(),
            link,
            finished: false,
        }
    }

    pub(crate) fn apply(&self, change: &DirectoryChange) {
        let mut links = self
            .inner
            .links
            .lock()
            .expect("route projector lock poisoned");
        let Some(state) = links.get_mut(&change.link) else {
            return;
        };
        for target in &change.removals {
            if state.targets.remove(target) {
                self.inner.router.remove(*target);
            }
        }
        for entry in &change.upserts {
            match preferred_locator(entry) {
                Some(locator) => {
                    self.inner.router.insert(
                        entry.pid,
                        Route::gated(locator.address.clone(), state.gate.clone()),
                    );
                    state.targets.insert(entry.pid);
                }
                None => {
                    if state.targets.remove(&entry.pid) {
                        self.inner.router.remove(entry.pid);
                    }
                }
            }
        }
    }

    fn fence(&self, link: LinkId) {
        let gate = self
            .inner
            .links
            .lock()
            .expect("route projector lock poisoned")
            .get(&link)
            .expect("route link must be open")
            .gate
            .clone();
        gate.close();
    }

    fn finish(&self, link: LinkId) {
        let state = self
            .inner
            .links
            .lock()
            .expect("route projector lock poisoned")
            .remove(&link)
            .expect("route link must be open");
        for target in state.targets {
            self.inner.router.remove(target);
        }
    }
}

#[derive(Debug)]
pub(crate) struct RouteSession {
    projector: RouteProjector,
    link: LinkId,
    finished: bool,
}

impl RouteSession {
    pub(crate) fn fence(&self) {
        self.projector.fence(self.link);
    }

    pub(crate) fn finish(mut self) {
        self.projector.finish(self.link);
        self.finished = true;
    }
}

impl Drop for RouteSession {
    fn drop(&mut self) {
        if !self.finished {
            self.projector.fence(self.link);
            self.projector.finish(self.link);
        }
    }
}

fn preferred_locator(entry: &ProcEntry) -> Option<&crate::Locator> {
    entry.locators.iter().min_by_key(|locator| locator.priority)
}

#[cfg(test)]
mod tests {
    use chrysalis_transport::DatagramAddr;

    use super::*;
    use crate::Locator;
    use crate::Revision;

    const AUTHORITY: Pid = Pid::from_bytes([1; 16]);
    const TARGET: Pid = Pid::from_bytes([2; 16]);
    const LINK: LinkId = LinkId::from_bytes([3; 16]);

    fn entry(locators: &[(u8, u32)]) -> ProcEntry {
        ProcEntry {
            pid: TARGET,
            tls_server_name: "target.test".into(),
            labels: crate::protocol::Labels::new(),
            locators: locators
                .iter()
                .map(|(address, priority)| Locator {
                    address: DatagramAddr::new("test", [*address]),
                    priority: *priority,
                })
                .collect(),
        }
    }

    fn change(upserts: Vec<ProcEntry>, removals: Vec<Pid>) -> DirectoryChange {
        DirectoryChange {
            link: LINK,
            revision: Revision {
                authority: AUTHORITY,
                value: 1,
            },
            upserts,
            removals,
        }
    }

    #[test]
    fn projects_preferred_locator_and_removes_withdrawal() {
        let router = Arc::new(Router::new());
        let projector = RouteProjector::new(router.clone());
        let session = projector.open(LINK);
        projector.apply(&change(vec![entry(&[(1, 10), (2, 0)])], Vec::new()));
        assert!(router.get(TARGET).is_some());
        projector.apply(&change(Vec::new(), vec![TARGET]));
        assert!(router.get(TARGET).is_none());
        session.finish();
    }

    #[test]
    fn fencing_precedes_route_cleanup() {
        let router = Arc::new(Router::new());
        let projector = RouteProjector::new(router.clone());
        let session = projector.open(LINK);
        projector.apply(&change(vec![entry(&[(1, 0)])], Vec::new()));
        session.fence();

        let links = projector
            .inner
            .links
            .lock()
            .expect("route projector lock poisoned");
        assert!(!links.get(&LINK).unwrap().gate.is_active());
        assert!(router.get(TARGET).is_some());
        drop(links);

        session.finish();
        assert!(router.get(TARGET).is_none());
    }

    #[test]
    fn dropping_session_fences_and_cleans_routes() {
        let router = Arc::new(Router::new());
        let projector = RouteProjector::new(router.clone());
        let session = projector.open(LINK);
        projector.apply(&change(vec![entry(&[(1, 0)])], Vec::new()));
        drop(session);
        assert!(router.get(TARGET).is_none());
    }

    #[test]
    fn entry_without_locator_withdraws_existing_route() {
        let router = Arc::new(Router::new());
        let projector = RouteProjector::new(router.clone());
        let session = projector.open(LINK);
        projector.apply(&change(vec![entry(&[(1, 0)])], Vec::new()));
        projector.apply(&change(vec![entry(&[])], Vec::new()));
        assert!(router.get(TARGET).is_none());
        session.finish();
    }

    #[test]
    fn change_for_closed_link_is_ignored() {
        let router = Arc::new(Router::new());
        let projector = RouteProjector::new(router.clone());

        projector.apply(&change(vec![entry(&[(1, 0)])], Vec::new()));

        assert!(router.get(TARGET).is_none());
    }
}
