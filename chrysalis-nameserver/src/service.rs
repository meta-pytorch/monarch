/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::RwLock;

use chrysalis_core::Pid;
use chrysalis_transport::IncomingStream;
use chrysalis_transport::Router;
use thiserror::Error;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::sync::Mutex;
use tokio::sync::watch;

use crate::ApplyEffects;
use crate::Command;
use crate::DEFAULT_ENUMERATION_PAGE_SIZE;
use crate::EnumerateRequest;
use crate::EnumerationCursor;
use crate::EnumerationPage;
use crate::EnumerationResult;
use crate::LinkId;
use crate::MAX_ENUMERATION_PAGE_SIZE;
use crate::MessageStreamError;
use crate::Nameserver;
use crate::ParentAction;
use crate::ParentSession;
use crate::ProcEntry;
use crate::Resolution;
use crate::Revision;
use crate::SessionError;
use crate::UpstreamNameserver;
use crate::VersionRange;
use crate::routes::RouteProjector;
use crate::stream::receive_message;
use crate::stream::send_message;

/// A parent-link protocol, framing, or cleanup failure.
#[derive(Debug, Error)]
pub enum LinkError {
    /// Message framing or stream I/O failed.
    #[error(transparent)]
    Stream(#[from] MessageStreamError),

    /// The parent-link protocol or deterministic state transition failed.
    #[error(transparent)]
    Session(#[from] SessionError),

    /// The upstream parent link closed while resolving a child request.
    #[error(transparent)]
    Upstream(#[from] crate::ResolveError),

    /// The QUIC send stream could not be finished cleanly.
    #[error("failed to finish nameserver stream")]
    Finish,
}

/// A singleton authoritative nameserver that serves authenticated child links.
///
/// This is the minimal non-replicated execution layer. It serializes deterministic commands under
/// one mutex, which makes [`Nameserver::apply`] the commit point. A replicated implementation can
/// replace this executor without changing the wire protocol or [`ParentSession`].
#[derive(Debug)]
pub struct NameserverService {
    authority: Pid,
    versions: VersionRange,
    negative_ttl_millis: u64,
    state: Mutex<Nameserver>,
    changes: watch::Sender<Revision>,
    child_links: watch::Sender<BTreeMap<Pid, LinkId>>,
    routes: Option<RouteProjector>,
    local_entry: RwLock<Option<ProcEntry>>,
}

impl NameserverService {
    /// Constructs an empty singleton nameserver.
    pub fn try_new(
        authority: Pid,
        versions: VersionRange,
        negative_ttl_millis: u64,
    ) -> Result<Self, crate::ApplyError> {
        let state = Nameserver::try_new(authority)?;
        let (changes, _) = watch::channel(state.revision());
        let (child_links, _) = watch::channel(state.child_links().collect());
        Ok(Self {
            authority,
            versions,
            negative_ttl_millis,
            state: Mutex::new(state),
            changes,
            child_links,
            routes: None,
            local_entry: RwLock::new(None),
        })
    }

    /// Constructs an empty singleton nameserver that projects child publications into `router`.
    pub fn try_new_with_router(
        authority: Pid,
        versions: VersionRange,
        negative_ttl_millis: u64,
        router: std::sync::Arc<Router>,
    ) -> Result<Self, crate::ApplyError> {
        let mut service = Self::try_new(authority, versions, negative_ttl_millis)?;
        service.routes = Some(RouteProjector::new(router));
        Ok(service)
    }

    /// Returns this nameserver incarnation's authority PID.
    pub const fn authority(&self) -> Pid {
        self.authority
    }

    /// Installs the immutable process entry that represents this nameserver itself.
    pub fn set_local_entry(&self, entry: ProcEntry) -> Result<(), LocalEntryError> {
        if entry.pid != self.authority {
            return Err(LocalEntryError::PidMismatch {
                expected: self.authority,
                actual: entry.pid,
            });
        }
        let mut local_entry = self.local_entry.write().expect("local entry lock poisoned");
        if local_entry.is_some() {
            return Err(LocalEntryError::AlreadySet);
        }
        *local_entry = Some(entry);
        Ok(())
    }

    /// Serves one transport-authenticated child stream until it closes or fails.
    ///
    /// Every terminal path commits `RemoveLink` before returning. The caller supplies a fresh,
    /// one-shot link ID for this stream.
    pub async fn serve(&self, link: LinkId, incoming: IncomingStream) -> Result<(), LinkError> {
        self.serve_incoming(link, incoming, None, std::future::pending())
            .await
    }

    /// Serves one child stream and forwards local resolution misses through `parent`.
    pub async fn serve_with_upstream(
        &self,
        link: LinkId,
        incoming: IncomingStream,
        upstream: &dyn UpstreamNameserver,
    ) -> Result<(), LinkError> {
        self.serve_incoming(link, incoming, Some(upstream), std::future::pending())
            .await
    }

    pub(crate) async fn serve_until_shutdown<F>(
        &self,
        link: LinkId,
        incoming: IncomingStream,
        upstream: Option<&dyn UpstreamNameserver>,
        shutdown: F,
    ) -> Result<(), LinkError>
    where
        F: Future<Output = ()>,
    {
        self.serve_incoming(link, incoming, upstream, shutdown)
            .await
    }

    async fn serve_incoming<F>(
        &self,
        link: LinkId,
        incoming: IncomingStream,
        upstream: Option<&dyn UpstreamNameserver>,
        shutdown: F,
    ) -> Result<(), LinkError>
    where
        F: Future<Output = ()>,
    {
        let (source, stream) = incoming.into_parts();
        let (mut send, recv) = stream.into_parts();
        let result = self
            .serve_parts_until(source, link, &mut send, recv, upstream, shutdown)
            .await;
        if result.is_ok() {
            send.finish().await.map_err(|_| LinkError::Finish)?;
        }
        result
    }

    /// Resolves a PID from the local authoritative directory.
    pub async fn resolve(&self, pid: Pid) -> Resolution {
        let state = self.state.lock().await;
        if pid == self.authority
            && let Some(entry) = self
                .local_entry
                .read()
                .expect("local entry lock poisoned")
                .clone()
        {
            return Resolution::Found {
                entry,
                revision: state.revision(),
            };
        }
        local_resolution(&state, pid, self.negative_ttl_millis)
    }

    /// Returns one locally authoritative process entry.
    pub async fn get(&self, pid: Pid) -> Option<ProcEntry> {
        if pid == self.authority
            && let Some(entry) = self
                .local_entry
                .read()
                .expect("local entry lock poisoned")
                .clone()
        {
            return Some(entry);
        }
        self.state.lock().await.get(pid).cloned()
    }

    /// Returns this nameserver's current local revision.
    pub async fn revision(&self) -> Revision {
        self.state.lock().await.revision()
    }

    /// Returns a deterministic complete snapshot of locally visible child publications.
    pub async fn snapshot(&self) -> Vec<ProcEntry> {
        self.state.lock().await.snapshot()
    }

    /// Enumerates one revision-stable page of the locally visible directory.
    pub async fn enumerate(
        &self,
        cursor: Option<EnumerationCursor>,
        limit: u32,
    ) -> EnumerationResult {
        let state = self.state.lock().await;
        let revision = state.revision();
        if cursor.is_some_and(|cursor| cursor.revision != revision) {
            return EnumerationResult::Stale { current: revision };
        }

        let after = cursor.map(|cursor| cursor.after);
        let limit = effective_enumeration_limit(limit);
        let mut entries = state
            .entries_after(after)
            .take(limit + 1)
            .cloned()
            .collect::<Vec<_>>();
        if let Some(entry) = self
            .local_entry
            .read()
            .expect("local entry lock poisoned")
            .clone()
            .filter(|entry| after.is_none_or(|after| entry.pid > after))
        {
            let insertion = entries
                .binary_search_by_key(&entry.pid, |candidate| candidate.pid)
                .expect_err("the authoritative PID cannot be delegated by a child");
            entries.insert(insertion, entry);
            entries.truncate(limit + 1);
        }
        let has_more = entries.len() > limit;
        if has_more {
            entries.pop();
        }
        let next = has_more.then(|| EnumerationCursor {
            revision,
            after: entries
                .last()
                .expect("a nonempty limit must yield a last entry")
                .pid,
        });
        EnumerationResult::Page(EnumerationPage {
            entries,
            revision,
            next,
        })
    }

    /// Subscribes to coalescing visible-directory revision notifications.
    ///
    /// A notification is only a hint to fetch another [`Self::snapshot`]. Intermediate revisions
    /// may coalesce when a receiver is slow.
    pub fn subscribe(&self) -> watch::Receiver<Revision> {
        self.changes.subscribe()
    }

    /// Subscribes to complete snapshots of directly admitted child links.
    ///
    /// The receiver's initial value is the current snapshot. Intermediate snapshots may coalesce.
    pub fn subscribe_child_links(&self) -> watch::Receiver<BTreeMap<Pid, LinkId>> {
        self.child_links.subscribe()
    }

    #[cfg(test)]
    pub(crate) async fn serve_parts<W, R>(
        &self,
        source: Pid,
        link: LinkId,
        writer: W,
        reader: R,
        upstream: Option<&dyn UpstreamNameserver>,
    ) -> Result<(), LinkError>
    where
        W: AsyncWrite + Unpin,
        R: AsyncRead + Unpin,
    {
        self.serve_parts_until(
            source,
            link,
            writer,
            reader,
            upstream,
            std::future::pending(),
        )
        .await
    }

    async fn serve_parts_until<W, R, F>(
        &self,
        source: Pid,
        link: LinkId,
        mut writer: W,
        mut reader: R,
        upstream: Option<&dyn UpstreamNameserver>,
        shutdown: F,
    ) -> Result<(), LinkError>
    where
        W: AsyncWrite + Unpin,
        R: AsyncRead + Unpin,
        F: Future<Output = ()>,
    {
        let mut session = ParentSession::try_new(self.authority, source, link, self.versions)?;
        let routes = self.routes.as_ref().map(|routes| routes.open(link));
        tokio::pin!(shutdown);
        let result = tokio::select! {
            result = self.run_session(&mut session, &mut writer, &mut reader, upstream) => result,
            () = &mut shutdown => Ok(()),
        };
        if let Some(routes) = &routes {
            routes.fence();
        }
        let cleanup = self.remove_session(&mut session).await;
        if cleanup.is_ok()
            && let Some(routes) = routes
        {
            routes.finish();
        }
        cleanup?;
        result
    }

    async fn run_session<W, R>(
        &self,
        session: &mut ParentSession,
        writer: &mut W,
        reader: &mut R,
        upstream: Option<&dyn UpstreamNameserver>,
    ) -> Result<(), LinkError>
    where
        W: AsyncWrite + Unpin,
        R: AsyncRead + Unpin,
    {
        while let Some(message) = receive_message(reader).await? {
            let action = session.receive(message)?;
            if self
                .execute_action(session, writer, action, upstream)
                .await?
            {
                return Ok(());
            }
        }
        Ok(())
    }

    async fn execute_action<W>(
        &self,
        session: &mut ParentSession,
        writer: &mut W,
        mut action: ParentAction,
        upstream: Option<&dyn UpstreamNameserver>,
    ) -> Result<bool, LinkError>
    where
        W: AsyncWrite + Unpin,
    {
        loop {
            match action {
                ParentAction::Commit(command) => {
                    let result = self.apply(command).await;
                    let Some(next) = session.complete_commit(result)? else {
                        return Ok(false);
                    };
                    action = next;
                }
                ParentAction::Resolve(request) => {
                    let result = self.resolve_for_child(request, upstream).await?;
                    action = session.resolved(request.request, result)?;
                }
                ParentAction::Enumerate(request) => {
                    let result = self.enumerate_for_child(request, upstream).await?;
                    action = session.enumerated(request.request, result)?;
                }
                ParentAction::Send(message) => {
                    send_message(writer, &message).await?;
                    return Ok(false);
                }
                ParentAction::SendAndClose(message) => {
                    send_message(writer, &message).await?;
                    return Ok(true);
                }
            }
        }
    }

    async fn remove_session(&self, session: &mut ParentSession) -> Result<(), LinkError> {
        let Some(action) = session.close()? else {
            return Ok(());
        };
        let ParentAction::Commit(command) = action else {
            panic!("closing a parent session must only commit link removal");
        };
        let result = self.apply(command).await;
        let next = session.complete_commit(result)?;
        assert!(next.is_none(), "link removal must complete the session");
        Ok(())
    }

    async fn apply(&self, command: Command) -> Result<ApplyEffects, crate::ApplyError> {
        let mut state = self.state.lock().await;
        let updates_child_links = matches!(
            &command,
            Command::AdmitLink { .. } | Command::RemoveLink { .. }
        );
        let previous_links =
            updates_child_links.then(|| state.child_links().collect::<BTreeMap<_, _>>());
        let effects = state.apply(command)?;
        if let Some(previous_links) = previous_links {
            let child_links = state.child_links().collect::<BTreeMap<_, _>>();
            if child_links != previous_links {
                self.child_links.send_replace(child_links);
            }
        }
        if let Some(change) = &effects.directory_change {
            if let Some(routes) = &self.routes {
                routes.apply(change);
            }
            self.changes.send_replace(change.revision);
        }
        Ok(effects)
    }

    async fn resolve_for_child(
        &self,
        request: crate::ResolveRequest,
        upstream: Option<&dyn UpstreamNameserver>,
    ) -> Result<Resolution, crate::ResolveError> {
        {
            let state = self.state.lock().await;
            if request.pid == self.authority
                && let Some(entry) = self
                    .local_entry
                    .read()
                    .expect("local entry lock poisoned")
                    .clone()
            {
                return Ok(Resolution::Found {
                    entry,
                    revision: state.revision(),
                });
            }
            if let Some(result) = state.resolve(request.pid) {
                return Ok(self.rewrite_for_child(result));
            }
        }
        let result = match upstream {
            Some(upstream) => upstream.resolve(request.pid, request.consistency).await,
            None => Ok(self.resolve(request.pid).await),
        }?;
        Ok(self.rewrite_for_child(result))
    }

    async fn enumerate_for_child(
        &self,
        request: EnumerateRequest,
        upstream: Option<&dyn UpstreamNameserver>,
    ) -> Result<EnumerationResult, crate::ResolveError> {
        let result = if request.consistency == crate::ResolveConsistency::Refresh
            && let Some(upstream) = upstream
        {
            upstream
                .enumerate(request.cursor, request.limit, request.consistency)
                .await?
        } else {
            self.enumerate(request.cursor, request.limit).await
        };
        Ok(self.rewrite_enumeration_for_child(result))
    }

    fn rewrite_for_child(&self, mut result: Resolution) -> Resolution {
        let local_entry = self.local_entry.read().expect("local entry lock poisoned");
        if let (Some(local_entry), Resolution::Found { entry, .. }) = (&*local_entry, &mut result) {
            entry.locators.clone_from(&local_entry.locators);
        }
        result
    }

    fn rewrite_enumeration_for_child(&self, mut result: EnumerationResult) -> EnumerationResult {
        let local_entry = self.local_entry.read().expect("local entry lock poisoned");
        if let (Some(local_entry), EnumerationResult::Page(page)) = (&*local_entry, &mut result) {
            for entry in &mut page.entries {
                entry.locators.clone_from(&local_entry.locators);
            }
        }
        result
    }
}

/// A local process entry that does not identify its nameserver.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum LocalEntryError {
    /// The entry PID differs from the nameserver authority.
    #[error("local entry PID mismatch: expected {expected:?}, got {actual:?}")]
    PidMismatch { expected: Pid, actual: Pid },

    /// Version one does not support changing the local entry after installation.
    #[error("local nameserver entry is already set")]
    AlreadySet,
}

fn local_resolution(state: &Nameserver, pid: Pid, negative_ttl_millis: u64) -> Resolution {
    state.resolve(pid).unwrap_or_else(|| Resolution::NotFound {
        pid,
        revision: state.revision(),
        valid_for_millis: negative_ttl_millis,
    })
}

fn effective_enumeration_limit(limit: u32) -> usize {
    let limit = if limit == 0 {
        DEFAULT_ENUMERATION_PAGE_SIZE
    } else {
        limit.min(MAX_ENUMERATION_PAGE_SIZE)
    };
    limit as usize
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::Context;
    use std::task::Poll;

    use chrysalis_transport::DatagramAddr;
    use tokio::io::AsyncWrite;
    use tokio::io::AsyncWriteExt;
    use tokio::io::DuplexStream;
    use tokio::io::ReadHalf;
    use tokio::io::WriteHalf;

    use super::*;
    use crate::ApplyError;
    use crate::Locator;
    use crate::Message;
    use crate::ProtocolVersion;
    use crate::PublicationSequence;
    use crate::RejectCode;
    use crate::RequestId;
    use crate::ResolveConsistency;
    use crate::SnapshotId;
    use crate::VERSION_1;

    const PARENT: Pid = Pid::from_bytes([1; 16]);
    const CHILD: Pid = Pid::from_bytes([2; 16]);
    const TARGET: Pid = Pid::from_bytes([3; 16]);
    const OTHER: Pid = Pid::from_bytes([4; 16]);
    const LINK: LinkId = LinkId::from_bytes([5; 16]);
    const NEGATIVE_TTL_MILLIS: u64 = 1_000;

    struct Harness {
        service: Arc<NameserverService>,
        writer: WriteHalf<DuplexStream>,
        reader: ReadHalf<DuplexStream>,
        task: tokio::task::JoinHandle<Result<(), LinkError>>,
    }

    struct FailAfterWrites {
        remaining: usize,
    }

    impl AsyncWrite for FailAfterWrites {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            if self.remaining == 0 {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "test writer failed",
                )));
            }
            self.remaining -= 1;
            Poll::Ready(Ok(bytes.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn versions() -> VersionRange {
        VersionRange::try_new(VERSION_1, ProtocolVersion::new(2)).unwrap()
    }

    fn harness() -> Harness {
        let service =
            Arc::new(NameserverService::try_new(PARENT, versions(), NEGATIVE_TTL_MILLIS).unwrap());
        harness_with_service(service)
    }

    fn harness_with_service(service: Arc<NameserverService>) -> Harness {
        let (client, server) = tokio::io::duplex(64 * 1024);
        let (client_reader, client_writer) = tokio::io::split(client);
        let (server_reader, server_writer) = tokio::io::split(server);
        let server = service.clone();
        let task = tokio::spawn(async move {
            server
                .serve_parts(CHILD, LINK, server_writer, server_reader, None)
                .await
        });
        Harness {
            service,
            writer: client_writer,
            reader: client_reader,
            task,
        }
    }

    fn hello(child: Pid) -> Message {
        Message::Hello {
            versions: versions(),
            child,
        }
    }

    fn entry(pid: Pid) -> ProcEntry {
        ProcEntry {
            pid,
            tls_server_name: "target.test".into(),
            labels: crate::protocol::Labels::new(),
            locators: Vec::new(),
        }
    }

    fn routed_entry(pid: Pid) -> ProcEntry {
        ProcEntry {
            pid,
            tls_server_name: "target.test".into(),
            labels: crate::protocol::Labels::new(),
            locators: vec![Locator {
                address: DatagramAddr::new("test", [9]),
                priority: 0,
            }],
        }
    }

    async fn send(writer: &mut WriteHalf<DuplexStream>, message: &Message) {
        send_message(writer, message).await.unwrap();
    }

    async fn receive(reader: &mut ReadHalf<DuplexStream>) -> Message {
        receive_message(reader).await.unwrap().expect("message")
    }

    async fn admit(harness: &mut Harness) {
        send(&mut harness.writer, &hello(CHILD)).await;
        assert_eq!(
            receive(&mut harness.reader).await,
            Message::Welcome {
                version: ProtocolVersion::new(2),
                parent: PARENT,
                link: LINK,
            }
        );
    }

    async fn publish(harness: &mut Harness) {
        send(
            &mut harness.writer,
            &Message::SnapshotBegin {
                snapshot: SnapshotId::from_u64(10),
                base_sequence: PublicationSequence::from_u64(1),
            },
        )
        .await;
        send(
            &mut harness.writer,
            &Message::SnapshotChunk {
                snapshot: SnapshotId::from_u64(10),
                chunk: 0,
                entries: vec![entry(TARGET)],
            },
        )
        .await;
        send(
            &mut harness.writer,
            &Message::SnapshotEnd {
                snapshot: SnapshotId::from_u64(10),
                chunks: 1,
            },
        )
        .await;
        assert_eq!(
            receive(&mut harness.reader).await,
            Message::PublicationAck {
                sequence: PublicationSequence::from_u64(1),
            }
        );
    }

    #[tokio::test]
    async fn serves_publication_resolution_and_eof_cleanup() {
        let mut harness = harness();
        let mut changes = harness.service.subscribe();
        admit(&mut harness).await;

        send(
            &mut harness.writer,
            &Message::SnapshotBegin {
                snapshot: SnapshotId::from_u64(10),
                base_sequence: PublicationSequence::from_u64(1),
            },
        )
        .await;
        tokio::task::yield_now().await;
        assert_eq!(harness.service.get(TARGET).await, None);
        send(
            &mut harness.writer,
            &Message::SnapshotChunk {
                snapshot: SnapshotId::from_u64(10),
                chunk: 0,
                entries: vec![entry(TARGET)],
            },
        )
        .await;
        tokio::task::yield_now().await;
        assert_eq!(harness.service.get(TARGET).await, None);
        send(
            &mut harness.writer,
            &Message::SnapshotEnd {
                snapshot: SnapshotId::from_u64(10),
                chunks: 1,
            },
        )
        .await;
        assert_eq!(
            receive(&mut harness.reader).await,
            Message::PublicationAck {
                sequence: PublicationSequence::from_u64(1),
            }
        );
        assert_eq!(harness.service.get(TARGET).await, Some(entry(TARGET)));
        changes.changed().await.unwrap();
        assert_eq!(
            *changes.borrow_and_update(),
            harness.service.revision().await
        );
        assert_eq!(harness.service.snapshot().await, vec![entry(TARGET)]);

        let request = RequestId::from_u64(20);
        send(
            &mut harness.writer,
            &Message::Resolve {
                request,
                pid: TARGET,
                consistency: ResolveConsistency::Refresh,
            },
        )
        .await;
        assert_eq!(
            receive(&mut harness.reader).await,
            Message::ResolveResult {
                request,
                result: Resolution::Found {
                    entry: entry(TARGET),
                    revision: harness.service.revision().await,
                },
            }
        );

        harness.writer.shutdown().await.unwrap();
        harness.task.await.unwrap().unwrap();
        assert_eq!(harness.service.get(TARGET).await, None);
        changes.changed().await.unwrap();
        assert_eq!(harness.service.snapshot().await, Vec::new());
    }

    #[tokio::test]
    async fn rewrites_child_resolution_to_the_local_gateway() {
        let mut harness = harness();
        harness
            .service
            .set_local_entry(routed_entry(PARENT))
            .unwrap();
        admit(&mut harness).await;
        publish(&mut harness).await;
        let request = RequestId::from_u64(20);

        send(
            &mut harness.writer,
            &Message::Resolve {
                request,
                pid: TARGET,
                consistency: ResolveConsistency::Refresh,
            },
        )
        .await;

        let mut expected = routed_entry(PARENT);
        expected.pid = TARGET;
        assert_eq!(
            receive(&mut harness.reader).await,
            Message::ResolveResult {
                request,
                result: Resolution::Found {
                    entry: expected,
                    revision: harness.service.revision().await,
                },
            }
        );
        harness.writer.shutdown().await.unwrap();
        harness.task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn enumerates_stable_pages_rewrites_locators_and_rejects_stale_cursors() {
        let mut harness = harness();
        harness
            .service
            .set_local_entry(routed_entry(PARENT))
            .unwrap();
        admit(&mut harness).await;
        publish(&mut harness).await;

        let first_request = RequestId::from_u64(30);
        send(
            &mut harness.writer,
            &Message::Enumerate {
                request: first_request,
                consistency: ResolveConsistency::Cached,
                cursor: None,
                limit: 1,
            },
        )
        .await;
        let Message::EnumerateResult {
            request,
            result: EnumerationResult::Page(first),
        } = receive(&mut harness.reader).await
        else {
            panic!("expected first enumeration page");
        };
        assert_eq!(request, first_request);
        assert_eq!(first.entries, vec![routed_entry(PARENT)]);
        let cursor = first.next.expect("target must remain on the next page");

        let second_request = RequestId::from_u64(31);
        send(
            &mut harness.writer,
            &Message::Enumerate {
                request: second_request,
                consistency: ResolveConsistency::Cached,
                cursor: Some(cursor),
                limit: 1,
            },
        )
        .await;
        let mut routed_target = routed_entry(PARENT);
        routed_target.pid = TARGET;
        assert_eq!(
            receive(&mut harness.reader).await,
            Message::EnumerateResult {
                request: second_request,
                result: EnumerationResult::Page(EnumerationPage {
                    entries: vec![routed_target],
                    revision: cursor.revision,
                    next: None,
                }),
            }
        );

        send(
            &mut harness.writer,
            &Message::Delta {
                sequence: PublicationSequence::from_u64(2),
                upserts: vec![entry(OTHER)],
                removals: Vec::new(),
            },
        )
        .await;
        assert_eq!(
            receive(&mut harness.reader).await,
            Message::PublicationAck {
                sequence: PublicationSequence::from_u64(2),
            }
        );

        let stale_request = RequestId::from_u64(32);
        send(
            &mut harness.writer,
            &Message::Enumerate {
                request: stale_request,
                consistency: ResolveConsistency::Cached,
                cursor: Some(cursor),
                limit: 1,
            },
        )
        .await;
        assert_eq!(
            receive(&mut harness.reader).await,
            Message::EnumerateResult {
                request: stale_request,
                result: EnumerationResult::Stale {
                    current: harness.service.revision().await,
                },
            }
        );

        harness.writer.shutdown().await.unwrap();
        harness.task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn projects_route_before_ack_and_removes_it_on_close() {
        let router = Arc::new(Router::new());
        let service = Arc::new(
            NameserverService::try_new_with_router(
                PARENT,
                versions(),
                NEGATIVE_TTL_MILLIS,
                router.clone(),
            )
            .unwrap(),
        );
        let mut harness = harness_with_service(service);
        admit(&mut harness).await;
        send(
            &mut harness.writer,
            &Message::SnapshotBegin {
                snapshot: SnapshotId::from_u64(1),
                base_sequence: PublicationSequence::from_u64(1),
            },
        )
        .await;
        send(
            &mut harness.writer,
            &Message::SnapshotChunk {
                snapshot: SnapshotId::from_u64(1),
                chunk: 0,
                entries: vec![routed_entry(TARGET)],
            },
        )
        .await;
        send(
            &mut harness.writer,
            &Message::SnapshotEnd {
                snapshot: SnapshotId::from_u64(1),
                chunks: 1,
            },
        )
        .await;
        assert_eq!(
            receive(&mut harness.reader).await,
            Message::PublicationAck {
                sequence: PublicationSequence::from_u64(1),
            }
        );
        assert!(router.get(TARGET).is_some());

        harness.writer.shutdown().await.unwrap();
        harness.task.await.unwrap().unwrap();
        assert!(router.get(TARGET).is_none());
    }

    #[tokio::test]
    async fn absent_resolution_uses_local_revision_and_ttl() {
        let mut harness = harness();
        admit(&mut harness).await;
        let request = RequestId::from_u64(21);
        send(
            &mut harness.writer,
            &Message::Resolve {
                request,
                pid: TARGET,
                consistency: ResolveConsistency::Cached,
            },
        )
        .await;
        assert_eq!(
            receive(&mut harness.reader).await,
            Message::ResolveResult {
                request,
                result: Resolution::NotFound {
                    pid: TARGET,
                    revision: Revision {
                        authority: PARENT,
                        value: 0,
                    },
                    valid_for_millis: NEGATIVE_TTL_MILLIS,
                },
            }
        );
        harness.writer.shutdown().await.unwrap();
        harness.task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn identity_mismatch_sends_reject_and_installs_no_link() {
        let mut harness = harness();
        send(&mut harness.writer, &hello(OTHER)).await;
        assert_eq!(
            receive(&mut harness.reader).await,
            Message::Reject {
                code: RejectCode::IdentityMismatch,
            }
        );
        harness.task.await.unwrap().unwrap();
        assert_eq!(harness.service.revision().await.value, 0);
    }

    #[tokio::test]
    async fn protocol_error_revokes_committed_publications() {
        let mut harness = harness();
        admit(&mut harness).await;
        publish(&mut harness).await;
        assert_eq!(harness.service.get(TARGET).await, Some(entry(TARGET)));

        send(
            &mut harness.writer,
            &Message::Welcome {
                version: VERSION_1,
                parent: PARENT,
                link: LINK,
            },
        )
        .await;
        assert!(matches!(
            harness.task.await.unwrap(),
            Err(LinkError::Session(SessionError::UnexpectedMessage { .. }))
        ));
        assert_eq!(harness.service.get(TARGET).await, None);
    }

    #[tokio::test]
    async fn truncated_frame_revokes_committed_publications() {
        let mut harness = harness();
        admit(&mut harness).await;
        publish(&mut harness).await;
        harness.writer.write_all(&[0, 0]).await.unwrap();
        harness.writer.shutdown().await.unwrap();
        assert!(matches!(
            harness.task.await.unwrap(),
            Err(LinkError::Stream(MessageStreamError::TruncatedFrame))
        ));
        assert_eq!(harness.service.get(TARGET).await, None);
    }

    #[tokio::test]
    async fn response_write_failure_revokes_committed_publications() {
        let service = NameserverService::try_new(PARENT, versions(), NEGATIVE_TTL_MILLIS).unwrap();
        let (mut input, reader) = tokio::io::duplex(64 * 1024);
        send_message(&mut input, &hello(CHILD)).await.unwrap();
        send_message(
            &mut input,
            &Message::SnapshotBegin {
                snapshot: SnapshotId::from_u64(10),
                base_sequence: PublicationSequence::from_u64(1),
            },
        )
        .await
        .unwrap();
        send_message(
            &mut input,
            &Message::SnapshotChunk {
                snapshot: SnapshotId::from_u64(10),
                chunk: 0,
                entries: vec![entry(TARGET)],
            },
        )
        .await
        .unwrap();
        send_message(
            &mut input,
            &Message::SnapshotEnd {
                snapshot: SnapshotId::from_u64(10),
                chunks: 1,
            },
        )
        .await
        .unwrap();
        send_message(
            &mut input,
            &Message::Resolve {
                request: RequestId::from_u64(30),
                pid: TARGET,
                consistency: ResolveConsistency::Cached,
            },
        )
        .await
        .unwrap();
        input.shutdown().await.unwrap();
        assert!(matches!(
            service
                .serve_parts(CHILD, LINK, FailAfterWrites { remaining: 2 }, reader, None,)
                .await,
            Err(LinkError::Stream(MessageStreamError::Io(_)))
        ));
        assert_eq!(service.get(TARGET).await, None);
    }

    #[tokio::test]
    async fn rejected_publication_sends_no_ack_and_removes_link() {
        let mut harness = harness();
        admit(&mut harness).await;
        send(
            &mut harness.writer,
            &Message::SnapshotChunk {
                snapshot: SnapshotId::from_u64(1),
                chunk: 0,
                entries: vec![entry(TARGET)],
            },
        )
        .await;
        assert!(matches!(
            harness.task.await.unwrap(),
            Err(LinkError::Session(SessionError::Apply(
                ApplyError::WrongSnapshot { .. }
            )))
        ));
        assert_eq!(receive_message(&mut harness.reader).await.unwrap(), None);
        assert_eq!(harness.service.get(TARGET).await, None);
    }
}
