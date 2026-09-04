/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::sync::Arc;

use chrysalis_core::Pid;
use chrysalis_transport::DatagramSocket;
use chrysalis_transport::LinkLocalProtocol;
use tokio::task::JoinSet;

use crate::LinkId;
use crate::NameserverService;
use crate::UpstreamNameserver;
use crate::link::Completion;
use crate::link::CompletionGuard;

/// Accepts and supervises child links selected by the nameserver link-local protocol.
pub struct ChildLinkServer<T: DatagramSocket> {
    _protocol: LinkLocalProtocol<T>,
    completion: Arc<Completion>,
}

impl<T: DatagramSocket + 'static> ChildLinkServer<T> {
    /// Spawns a root child-link server with no upstream resolver.
    pub fn spawn(protocol: LinkLocalProtocol<T>, service: Arc<NameserverService>) -> Self {
        Self::spawn_inner(protocol, service, None)
    }

    /// Spawns a delegated child-link server that forwards misses through `upstream`.
    pub fn spawn_with_upstream(
        protocol: LinkLocalProtocol<T>,
        service: Arc<NameserverService>,
        upstream: Arc<dyn UpstreamNameserver>,
    ) -> Self {
        Self::spawn_inner(protocol, service, Some(upstream))
    }

    fn spawn_inner(
        protocol: LinkLocalProtocol<T>,
        service: Arc<NameserverService>,
        upstream: Option<Arc<dyn UpstreamNameserver>>,
    ) -> Self {
        let completion = Arc::new(Completion::default());
        let task_completion = completion.clone();
        let task_protocol = protocol.clone();
        tokio::spawn(async move {
            let _guard = CompletionGuard(&task_completion);
            serve_links(task_protocol, service, upstream, task_completion.clone()).await;
        });
        Self {
            _protocol: protocol,
            completion,
        }
    }

    /// Idempotently requests child-link server shutdown.
    pub fn shutdown(&self) {
        self.completion.shutdown();
    }

    /// Waits for the accept loop and every child-link task to terminate.
    pub async fn join(&self) {
        self.completion.join().await;
    }
}

impl<T: DatagramSocket> Drop for ChildLinkServer<T> {
    fn drop(&mut self) {
        self.completion.shutdown();
    }
}

async fn serve_links<T: DatagramSocket + 'static>(
    protocol: LinkLocalProtocol<T>,
    service: Arc<NameserverService>,
    upstream: Option<Arc<dyn UpstreamNameserver>>,
    completion: Arc<Completion>,
) {
    let mut links = JoinSet::new();
    let mut next_link = 1u64;
    loop {
        tokio::select! {
            biased;
            () = completion.cancelled() => break,
            incoming = protocol.accept() => {
                let Ok(incoming) = incoming else {
                    break;
                };
                let link = allocate_link_id(service.authority(), next_link);
                next_link = next_link.checked_add(1).expect("link ID counter exhausted");
                let child_service = service.clone();
                let child_upstream = upstream.clone();
                let child_completion = completion.clone();
                links.spawn(async move {
                    child_service
                        .serve_until_shutdown(
                            link,
                            incoming,
                            child_upstream.as_deref(),
                            child_completion.cancelled(),
                        )
                        .await
                });
            }
            _ = links.join_next(), if !links.is_empty() => {}
        }
    }
    while links.join_next().await.is_some() {}
}

fn allocate_link_id(authority: Pid, sequence: u64) -> LinkId {
    let mut bytes = *authority.as_bytes();
    bytes[8..].copy_from_slice(&sequence.to_be_bytes());
    LinkId::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    const AUTHORITY: Pid = Pid::from_bytes([0x11; 16]);

    #[test]
    fn allocated_link_ids_are_parent_scoped_and_monotonic() {
        let first = allocate_link_id(AUTHORITY, 1);
        let second = allocate_link_id(AUTHORITY, 2);
        assert_ne!(first, second);
        assert_eq!(&first.as_bytes()[..8], &AUTHORITY.as_bytes()[..8]);
        assert_eq!(&first.as_bytes()[8..], &1u64.to_be_bytes());
    }
}
