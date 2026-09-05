/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;

use chrysalis::LinkContext;
use chrysalis::LinkId;
use chrysalis::LinkSide;
use chrysalis::NodeConfig;
use chrysalis::Pid;
use chrysalis::Stream;

use crate::Replica;
use crate::SQLITE_LINK_PROTOCOL;
use crate::SitePublisher;
use crate::SiteScope;
use crate::SiteSet;

/// Composes one SQLite replica with Node-managed parent and child links.
#[derive(Clone)]
pub struct ReplicationTopology {
    inner: Arc<Inner>,
}

struct Inner {
    replica: Replica,
    upstream: SitePublisher,
    children: Mutex<BTreeMap<LinkId, ChildScope>>,
}

struct ChildScope {
    peer: Pid,
    sites: Option<SiteSet>,
}

impl ReplicationTopology {
    /// Constructs an empty rooted-tree replication topology.
    pub fn new(replica: Replica) -> Self {
        let upstream = replica.local_publisher();
        Self {
            inner: Arc::new(Inner {
                replica,
                upstream,
                children: Mutex::new(BTreeMap::new()),
            }),
        }
    }

    /// Registers SQLite replication on every link managed by the node.
    pub fn configure(&self, config: NodeConfig) -> NodeConfig {
        let topology = self.clone();
        config.with_link_protocol(SQLITE_LINK_PROTOCOL, move |context, stream| {
            let topology = topology.clone();
            async move {
                topology.run(context, stream).await;
            }
        })
    }

    /// Returns the explicit subtree scope currently advertised to the parent.
    pub fn upstream_scope(&self) -> SiteScope {
        self.inner.upstream.scope()
    }

    async fn run(&self, context: LinkContext, stream: Stream) {
        match context.side() {
            LinkSide::Parent => {
                if let Err(error) = self
                    .inner
                    .replica
                    .replicate(context.peer(), stream, self.inner.upstream.clone())
                    .await
                {
                    tracing::error!(peer = ?context.peer(), %error, "sqlite replication failed");
                }
            }
            LinkSide::Child => self.run_child(context, stream).await,
        }
    }

    async fn run_child(&self, context: LinkContext, stream: Stream) {
        let _child = ChildGuard::enter(self.inner.clone(), context);
        let mut changes = self.inner.replica.subscribe_peer_scopes();
        let replication = self.inner.replica.replicate(
            context.peer(),
            stream,
            self.inner.replica.complement_publisher(),
        );
        tokio::pin!(replication);
        loop {
            self.inner.refresh_child(context);
            tokio::select! {
                result = &mut replication => {
                    if let Err(error) = result {
                        tracing::error!(peer = ?context.peer(), %error, "sqlite replication failed");
                    }
                    return;
                }
                changed = changes.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
            }
        }
    }
}

impl Inner {
    fn refresh_child(&self, context: LinkContext) {
        let sites = match self.replica.peer_scopes().get(&context.peer()) {
            Some(SiteScope::Explicit(sites)) => Some(sites.clone()),
            Some(SiteScope::ComplementOfPeer) | None => None,
        };
        let mut children = self.children.lock().expect("child scope lock poisoned");
        let child = children
            .get_mut(&context.link())
            .expect("active child session must have a topology entry");
        assert_eq!(child.peer, context.peer(), "child link peer changed");
        if child.sites != sites {
            child.sites = sites;
            self.publish(&children);
        }
    }

    fn remove_child(&self, context: LinkContext) {
        let mut children = self.children.lock().expect("child scope lock poisoned");
        let child = children
            .remove(&context.link())
            .expect("active child session disappeared");
        assert_eq!(child.peer, context.peer(), "child link peer changed");
        self.publish(&children);
    }

    fn publish(&self, children: &BTreeMap<LinkId, ChildScope>) {
        let mut sites = vec![self.replica.site_id().to_vec()];
        for child in children.values() {
            if let Some(child_sites) = &child.sites {
                sites.extend(child_sites.site_ids().iter().cloned());
            }
        }
        sites.sort();
        sites.dedup();
        let current = self
            .upstream
            .scope()
            .as_explicit()
            .expect("upstream scope must be explicit")
            .site_ids()
            .to_vec();
        if sites != current {
            self.upstream
                .set(sites)
                .expect("aggregated child sites must form a valid explicit scope");
        }
    }
}

struct ChildGuard {
    inner: Arc<Inner>,
    context: LinkContext,
}

impl ChildGuard {
    fn enter(inner: Arc<Inner>, context: LinkContext) -> Self {
        let replaced = inner
            .children
            .lock()
            .expect("child scope lock poisoned")
            .insert(
                context.link(),
                ChildScope {
                    peer: context.peer(),
                    sites: None,
                },
            );
        assert!(
            replaced.is_none(),
            "child link already has a SQLite session"
        );
        Self { inner, context }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.inner.remove_child(self.context);
    }
}
