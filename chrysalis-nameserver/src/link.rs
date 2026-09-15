/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::time::Instant;

use chrysalis_core::Pid;
use chrysalis_transport::DatagramAddr;
use chrysalis_transport::Stream;
use thiserror::Error;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;

use crate::CacheError;
use crate::CacheTime;
use crate::ChildEvent;
use crate::ChildSession;
use crate::ChildSessionError;
use crate::EnumerationCursor;
use crate::EnumerationResult;
use crate::Locator;
use crate::MessageStreamError;
use crate::NameserverService;
use crate::ProcEntry;
use crate::PublicationSequence;
use crate::RequestId;
use crate::Resolution;
use crate::ResolveConsistency;
use crate::ResolverCache;
use crate::SnapshotId;
use crate::VersionRange;
use crate::stream::receive_message;
use crate::stream::send_message;

const REQUEST_CAPACITY: usize = 1024;
const SNAPSHOT_CHUNK_ENTRIES: usize = 1024;
const RUNNING: u8 = 0;
const SHUTTING_DOWN: u8 = 1;
const TERMINATED: u8 = 2;

/// A parent-link configuration, protocol, or I/O failure.
#[derive(Debug, Error)]
pub enum ParentLinkError {
    /// The local entry does not identify the local nameserver.
    #[error("local publication PID does not match nameserver authority")]
    LocalPidMismatch,

    /// The parent and child have the same PID.
    #[error("nameserver cannot link to itself")]
    SelfLink,

    /// The child-side protocol failed.
    #[error(transparent)]
    Session(#[from] ChildSessionError),

    /// Message framing or stream I/O failed.
    #[error(transparent)]
    Stream(#[from] MessageStreamError),

    /// A parent cache update violated revision ordering.
    #[error(transparent)]
    Cache(#[from] CacheError),

    /// The parent rejected the link.
    #[error("parent rejected nameserver link: {code}")]
    Rejected {
        /// The protocol-level rejection reason.
        code: crate::RejectCode,
    },

    /// The parent closed the control stream.
    #[error("parent closed nameserver link")]
    ParentClosed,

    /// A monotonic link-local counter was exhausted.
    #[error("{0} exhausted")]
    CounterExhausted(&'static str),

    /// The parent acknowledged publication state that was not in flight.
    #[error("unexpected publication acknowledgment")]
    UnexpectedAck,
}

/// A parent-link resolution failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ResolveError {
    /// The parent link terminated before answering.
    #[error("parent nameserver link is closed")]
    Closed,
}

/// A supervised child-side link to one parent nameserver.
///
/// The link republishes the local nameserver's complete delegated subtree. Descendant entries are
/// rewritten to use the local process's locators, making this process their next hop from the
/// parent's perspective.
#[derive(Clone, Debug)]
pub struct ParentLink {
    requests: mpsc::Sender<LinkRequest>,
    cache: Arc<Mutex<ResolverCache>>,
    next_hop: DatagramAddr,
    started: Instant,
    completion: Arc<Completion>,
    state: watch::Receiver<ParentLinkState>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ParentLinkState {
    Pending,
    Active(crate::LinkId),
    Rejected(crate::RejectCode),
    Closed,
}

impl ParentLink {
    /// Spawns one parent-link task over an authenticated QUIC stream.
    pub fn spawn(
        parent: Pid,
        next_hop: DatagramAddr,
        stream: Stream,
        service: Arc<NameserverService>,
        local_entry: ProcEntry,
        versions: VersionRange,
    ) -> Result<Self, ParentLinkError> {
        if local_entry.pid != service.authority() {
            return Err(ParentLinkError::LocalPidMismatch);
        }
        if parent == service.authority() {
            return Err(ParentLinkError::SelfLink);
        }
        let cache = Arc::new(Mutex::new(ResolverCache::try_new(parent)?));
        let (requests, request_rx) = mpsc::channel(REQUEST_CAPACITY);
        let completion = Arc::new(Completion::default());
        let (state_tx, state) = watch::channel(ParentLinkState::Pending);
        let task_completion = completion.clone();
        let task_cache = cache.clone();
        let started = Instant::now();
        let (mut writer, reader) = stream.into_parts();
        tokio::spawn(async move {
            let _guard = CompletionGuard(&task_completion);
            let result = run_parent_link(
                parent,
                service,
                local_entry,
                versions,
                task_cache.clone(),
                started,
                request_rx,
                &mut writer,
                reader,
                task_completion.clone(),
                Some(state_tx.clone()),
            )
            .await;
            let state = match &result {
                Err(ParentLinkError::Rejected { code }) => ParentLinkState::Rejected(*code),
                _ => ParentLinkState::Closed,
            };
            state_tx.send_replace(state);
            task_cache.lock().await.clear();
            if result.is_ok() {
                let _ = writer.shutdown().await;
            }
        });
        Ok(Self {
            requests,
            cache,
            next_hop,
            started,
            completion,
            state,
        })
    }

    /// Resolves a PID through the parent, using a live local cache entry when permitted.
    pub async fn resolve(
        &self,
        pid: Pid,
        consistency: ResolveConsistency,
    ) -> Result<Resolution, ResolveError> {
        if consistency == ResolveConsistency::Cached
            && let Some(result) = self.cache.lock().await.resolve(pid, self.now())
        {
            return Ok(self.rewrite_resolution(result));
        }
        let (reply, result) = oneshot::channel();
        let request = LinkRequest::Resolve {
            pid,
            consistency,
            reply,
        };
        tokio::select! {
            send = self.requests.send(request) => send.map_err(|_| ResolveError::Closed)?,
            () = self.completion.cancelled() => return Err(ResolveError::Closed),
        }
        let result = tokio::select! {
            result = result => result.unwrap_or(Err(ResolveError::Closed)),
            () = self.completion.cancelled() => Err(ResolveError::Closed),
        }?;
        Ok(self.rewrite_resolution(result))
    }

    /// Enumerates one page through the parent nameserver.
    pub async fn enumerate(
        &self,
        cursor: Option<EnumerationCursor>,
        limit: u32,
        consistency: ResolveConsistency,
    ) -> Result<EnumerationResult, ResolveError> {
        let (reply, result) = oneshot::channel();
        let request = LinkRequest::Enumerate {
            cursor,
            limit,
            consistency,
            reply,
        };
        tokio::select! {
            send = self.requests.send(request) => send.map_err(|_| ResolveError::Closed)?,
            () = self.completion.cancelled() => return Err(ResolveError::Closed),
        }
        let result = tokio::select! {
            result = result => result.unwrap_or(Err(ResolveError::Closed)),
            () = self.completion.cancelled() => Err(ResolveError::Closed),
        }?;
        Ok(self.rewrite_enumeration(result))
    }

    /// Idempotently requests link shutdown.
    pub fn shutdown(&self) {
        self.completion.shutdown();
    }

    /// Waits for the parent-link task to terminate.
    pub async fn join(&self) {
        self.completion.join().await;
    }

    /// Waits until the parent accepts the handshake and acknowledges initial publication.
    pub async fn wait_active(&self) -> Result<crate::LinkId, ParentLinkError> {
        let mut state = self.state.clone();
        loop {
            match *state.borrow() {
                ParentLinkState::Pending => {}
                ParentLinkState::Active(link) => return Ok(link),
                ParentLinkState::Rejected(code) => {
                    return Err(ParentLinkError::Rejected { code });
                }
                ParentLinkState::Closed => return Err(ParentLinkError::ParentClosed),
            }
            if state.changed().await.is_err() {
                return Err(ParentLinkError::ParentClosed);
            }
        }
    }

    fn now(&self) -> CacheTime {
        cache_time(self.started)
    }

    fn rewrite_resolution(&self, mut result: Resolution) -> Resolution {
        if let Resolution::Found { entry, .. } = &mut result {
            entry.locators = vec![Locator {
                address: self.next_hop.clone(),
                priority: 0,
            }];
        }
        result
    }

    fn rewrite_enumeration(&self, mut result: EnumerationResult) -> EnumerationResult {
        if let EnumerationResult::Page(page) = &mut result {
            for entry in &mut page.entries {
                entry.locators = vec![Locator {
                    address: self.next_hop.clone(),
                    priority: 0,
                }];
            }
        }
        result
    }
}

enum LinkRequest {
    Resolve {
        pid: Pid,
        consistency: ResolveConsistency,
        reply: oneshot::Sender<Result<Resolution, ResolveError>>,
    },
    Enumerate {
        cursor: Option<EnumerationCursor>,
        limit: u32,
        consistency: ResolveConsistency,
        reply: oneshot::Sender<Result<EnumerationResult, ResolveError>>,
    },
}

struct PendingPublication {
    sequence: PublicationSequence,
    desired: BTreeMap<Pid, ProcEntry>,
}

struct PublicationState {
    sequence: PublicationSequence,
    next_snapshot: u64,
    published: BTreeMap<Pid, ProcEntry>,
    pending: Option<PendingPublication>,
    dirty: bool,
}

impl PublicationState {
    fn new() -> Self {
        Self {
            sequence: PublicationSequence::from_u64(0),
            next_snapshot: 1,
            published: BTreeMap::new(),
            pending: None,
            dirty: true,
        }
    }
}

async fn run_parent_link<W, R>(
    parent: Pid,
    service: Arc<NameserverService>,
    local_entry: ProcEntry,
    versions: VersionRange,
    cache: Arc<Mutex<ResolverCache>>,
    started: Instant,
    mut requests: mpsc::Receiver<LinkRequest>,
    writer: &mut W,
    mut reader: R,
    completion: Arc<Completion>,
    state: Option<watch::Sender<ParentLinkState>>,
) -> Result<(), ParentLinkError>
where
    W: AsyncWrite + Unpin,
    R: AsyncRead + Unpin,
{
    let mut session = ChildSession::try_new(service.authority(), parent, versions)?;
    send_message(writer, &session.hello()?).await?;
    let message = tokio::select! {
        message = receive_message(&mut reader) => message?,
        () = completion.cancelled() => return Ok(()),
    }
    .ok_or(ParentLinkError::ParentClosed)?;
    let link = match session.receive(message)? {
        ChildEvent::Established { link, .. } => link,
        ChildEvent::Rejected { code } => return Err(ParentLinkError::Rejected { code }),
        _ => return Err(ParentLinkError::UnexpectedAck),
    };

    let mut changes = service.subscribe();
    let mut publication = PublicationState::new();
    send_current_snapshot(
        &mut session,
        &service,
        &local_entry,
        &mut publication,
        writer,
    )
    .await?;
    let mut next_request = 1u64;
    let mut pending_resolutions = BTreeMap::new();
    let mut pending_enumerations = BTreeMap::new();

    loop {
        tokio::select! {
            biased;
            () = completion.cancelled() => return Ok(()),
            message = receive_message(&mut reader) => {
                let message = message?.ok_or(ParentLinkError::ParentClosed)?;
                let event = session.receive(message)?;
                handle_parent_event(
                    event,
                    &mut session,
                    &service,
                    &local_entry,
                    &cache,
                    started,
                    &mut publication,
                    &mut pending_resolutions,
                    &mut pending_enumerations,
                    writer,
                    link,
                    state.as_ref(),
                ).await?;
            }
            changed = changes.changed() => {
                if changed.is_err() {
                    return Ok(());
                }
                changes.borrow_and_update();
                publication.dirty = true;
            }
            request = requests.recv() => {
                let Some(request) = request else {
                    return Ok(());
                };
                let id = RequestId::from_u64(next_request);
                next_request = next_request
                    .checked_add(1)
                    .ok_or(ParentLinkError::CounterExhausted("request ID"))?;
                let message = match request {
                    LinkRequest::Resolve { pid, consistency, reply } => {
                        let message = session.resolve(id, pid, consistency)?;
                        pending_resolutions.insert(id, reply);
                        message
                    }
                    LinkRequest::Enumerate { cursor, limit, consistency, reply } => {
                        let message = session.enumerate(id, consistency, cursor, limit)?;
                        pending_enumerations.insert(id, reply);
                        message
                    }
                };
                send_message(writer, &message).await?;
            }
        }
        maybe_send_delta(&session, &service, &local_entry, &mut publication, writer).await?;
    }
}

async fn handle_parent_event<W>(
    event: ChildEvent,
    session: &mut ChildSession,
    service: &NameserverService,
    local_entry: &ProcEntry,
    cache: &Mutex<ResolverCache>,
    started: Instant,
    publication: &mut PublicationState,
    pending_resolutions: &mut BTreeMap<
        RequestId,
        oneshot::Sender<Result<Resolution, ResolveError>>,
    >,
    pending_enumerations: &mut BTreeMap<
        RequestId,
        oneshot::Sender<Result<EnumerationResult, ResolveError>>,
    >,
    writer: &mut W,
    link: crate::LinkId,
    state: Option<&watch::Sender<ParentLinkState>>,
) -> Result<(), ParentLinkError>
where
    W: AsyncWrite + Unpin,
{
    match event {
        ChildEvent::PublicationAck { sequence } => {
            let Some(pending) = publication.pending.take() else {
                return Err(ParentLinkError::UnexpectedAck);
            };
            if sequence != pending.sequence {
                publication.pending = Some(pending);
                return Err(ParentLinkError::UnexpectedAck);
            }
            publication.published = pending.desired;
            if let Some(state) = state {
                state.send_replace(ParentLinkState::Active(link));
            }
        }
        ChildEvent::ResnapshotRequired { .. } => {
            if let Some(state) = state {
                state.send_replace(ParentLinkState::Pending);
            }
            send_current_snapshot(session, service, local_entry, publication, writer).await?;
        }
        ChildEvent::Resolved { request, result } => {
            update_cache(cache, result.clone(), cache_time(started)).await?;
            let reply = pending_resolutions
                .remove(&request)
                .ok_or(ParentLinkError::UnexpectedAck)?;
            let _ = reply.send(Ok(result));
        }
        ChildEvent::Enumerated { request, result } => {
            let reply = pending_enumerations
                .remove(&request)
                .ok_or(ParentLinkError::UnexpectedAck)?;
            let _ = reply.send(Ok(result));
        }
        ChildEvent::CacheUpdate { result } => {
            update_cache(cache, result, cache_time(started)).await?;
        }
        ChildEvent::Established { .. } | ChildEvent::Rejected { .. } => {
            return Err(ParentLinkError::UnexpectedAck);
        }
    }
    Ok(())
}

async fn update_cache(
    cache: &Mutex<ResolverCache>,
    result: Resolution,
    now: CacheTime,
) -> Result<(), ParentLinkError> {
    match cache.lock().await.update(result, now) {
        Ok(_) | Err(CacheError::AuthorityMismatch { .. }) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

async fn maybe_send_delta<W>(
    session: &ChildSession,
    service: &NameserverService,
    local_entry: &ProcEntry,
    publication: &mut PublicationState,
    writer: &mut W,
) -> Result<(), ParentLinkError>
where
    W: AsyncWrite + Unpin,
{
    if !publication.dirty || publication.pending.is_some() {
        return Ok(());
    }
    let desired = export_snapshot(service, local_entry).await;
    publication.dirty = false;
    let (upserts, removals) = diff(&publication.published, &desired);
    if upserts.is_empty() && removals.is_empty() {
        return Ok(());
    }
    let next = publication
        .sequence
        .as_u64()
        .checked_add(1)
        .ok_or(ParentLinkError::CounterExhausted("publication sequence"))?;
    publication.sequence = PublicationSequence::from_u64(next);
    let message = session.delta(publication.sequence, upserts, removals)?;
    send_message(writer, &message).await?;
    publication.pending = Some(PendingPublication {
        sequence: publication.sequence,
        desired,
    });
    Ok(())
}

async fn send_current_snapshot<W>(
    session: &mut ChildSession,
    service: &NameserverService,
    local_entry: &ProcEntry,
    publication: &mut PublicationState,
    writer: &mut W,
) -> Result<(), ParentLinkError>
where
    W: AsyncWrite + Unpin,
{
    let desired = export_snapshot(service, local_entry).await;
    let snapshot = SnapshotId::from_u64(publication.next_snapshot);
    publication.next_snapshot = publication
        .next_snapshot
        .checked_add(1)
        .ok_or(ParentLinkError::CounterExhausted("snapshot ID"))?;
    send_message(
        writer,
        &session.snapshot_begin(snapshot, publication.sequence)?,
    )
    .await?;
    let mut chunks = 0u32;
    let entries = desired.values().cloned().collect::<Vec<_>>();
    for entries in entries.chunks(SNAPSHOT_CHUNK_ENTRIES) {
        send_message(
            writer,
            &session.snapshot_chunk(snapshot, chunks, entries.to_vec())?,
        )
        .await?;
        chunks = chunks
            .checked_add(1)
            .ok_or(ParentLinkError::CounterExhausted("snapshot chunk count"))?;
    }
    send_message(writer, &session.snapshot_end(snapshot, chunks)?).await?;
    publication.pending = Some(PendingPublication {
        sequence: publication.sequence,
        desired,
    });
    publication.dirty = false;
    Ok(())
}

async fn export_snapshot(
    service: &NameserverService,
    local_entry: &ProcEntry,
) -> BTreeMap<Pid, ProcEntry> {
    let mut exported = BTreeMap::new();
    exported.insert(local_entry.pid, local_entry.clone());
    for entry in service.snapshot().await {
        exported.insert(
            entry.pid,
            ProcEntry {
                pid: entry.pid,
                tls_server_name: entry.tls_server_name,
                labels: entry.labels,
                locators: local_entry.locators.clone(),
            },
        );
    }
    exported
}

fn diff(
    published: &BTreeMap<Pid, ProcEntry>,
    desired: &BTreeMap<Pid, ProcEntry>,
) -> (Vec<ProcEntry>, Vec<Pid>) {
    let upserts = desired
        .iter()
        .filter(|(pid, entry)| published.get(pid) != Some(*entry))
        .map(|(_, entry)| entry.clone())
        .collect();
    let removals = published
        .keys()
        .filter(|pid| !desired.contains_key(pid))
        .copied()
        .collect();
    (upserts, removals)
}

fn cache_time(started: Instant) -> CacheTime {
    CacheTime::from_millis(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX))
}

#[derive(Debug, Default)]
pub(crate) struct Completion {
    state: AtomicU8,
    shutdown: Notify,
    terminated: Notify,
}

impl Completion {
    pub(crate) fn shutdown(&self) {
        if self
            .state
            .compare_exchange(RUNNING, SHUTTING_DOWN, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.shutdown.notify_waiters();
        }
    }

    pub(crate) async fn cancelled(&self) {
        loop {
            let notified = self.shutdown.notified();
            if self.state.load(Ordering::Acquire) != RUNNING {
                return;
            }
            notified.await;
        }
    }

    pub(crate) fn terminate(&self) {
        if self.state.swap(TERMINATED, Ordering::AcqRel) != TERMINATED {
            self.shutdown.notify_waiters();
            self.terminated.notify_waiters();
        }
    }

    pub(crate) async fn join(&self) {
        loop {
            let notified = self.terminated.notified();
            if self.state.load(Ordering::Acquire) == TERMINATED {
                return;
            }
            notified.await;
        }
    }
}

pub(crate) struct CompletionGuard<'a>(pub(crate) &'a Completion);

impl Drop for CompletionGuard<'_> {
    fn drop(&mut self) {
        self.0.terminate();
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::time::Duration;

    use chrysalis_transport::DatagramAddr;
    use tokio::io::AsyncWriteExt;
    use tokio::time::timeout;

    use super::*;
    use crate::LinkId;
    use crate::Locator;
    use crate::Message;
    use crate::ProtocolVersion;
    use crate::RejectCode;
    use crate::VERSION_1;

    const ROOT: Pid = Pid::from_bytes([1; 16]);
    const MIDDLE: Pid = Pid::from_bytes([2; 16]);
    const LEAF: Pid = Pid::from_bytes([3; 16]);
    const ROOT_LINK: LinkId = LinkId::from_bytes([4; 16]);
    const LEAF_LINK: LinkId = LinkId::from_bytes([5; 16]);

    fn versions() -> VersionRange {
        VersionRange::try_new(VERSION_1, ProtocolVersion::new(2)).unwrap()
    }

    fn entry(pid: Pid, address: u8) -> ProcEntry {
        ProcEntry {
            pid,
            tls_server_name: "target.test".into(),
            labels: crate::protocol::Labels::new(),
            locators: vec![Locator {
                address: DatagramAddr::new("test", [address]),
                priority: 0,
            }],
        }
    }

    async fn wait_for_entry(service: &NameserverService, pid: Pid, expected: Option<ProcEntry>) {
        timeout(Duration::from_secs(2), async {
            loop {
                if service.get(pid).await == expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("directory update timed out");
    }

    #[tokio::test]
    async fn recursively_replicates_resolves_and_withdraws_subtree() {
        let root = Arc::new(NameserverService::try_new(ROOT, versions(), 1_000).unwrap());
        let middle = Arc::new(NameserverService::try_new(MIDDLE, versions(), 1_000).unwrap());
        let middle_entry = entry(MIDDLE, 20);

        let (middle_stream, root_stream) = tokio::io::duplex(64 * 1024);
        let (middle_reader, mut middle_writer) = tokio::io::split(middle_stream);
        let (root_reader, root_writer) = tokio::io::split(root_stream);
        let root_server = root.clone();
        let root_task = tokio::spawn(async move {
            root_server
                .serve_parts(MIDDLE, ROOT_LINK, root_writer, root_reader, None)
                .await
        });

        let cache = Arc::new(Mutex::new(ResolverCache::try_new(ROOT).unwrap()));
        let completion = Arc::new(Completion::default());
        let (requests, request_rx) = mpsc::channel(NonZeroUsize::new(8).unwrap().get());
        let link_middle = middle.clone();
        let link_cache = cache.clone();
        let link_completion = completion.clone();
        let started = Instant::now();
        let upstream = ParentLink {
            requests: requests.clone(),
            cache: cache.clone(),
            next_hop: DatagramAddr::new("test", [10]),
            started,
            completion: completion.clone(),
            state: watch::channel(ParentLinkState::Active(ROOT_LINK)).1,
        };
        let link_task = tokio::spawn(async move {
            let result = run_parent_link(
                ROOT,
                link_middle,
                middle_entry.clone(),
                versions(),
                link_cache,
                started,
                request_rx,
                &mut middle_writer,
                middle_reader,
                link_completion,
                None,
            )
            .await;
            middle_writer.shutdown().await.unwrap();
            result
        });

        wait_for_entry(&root, MIDDLE, Some(entry(MIDDLE, 20))).await;

        let (leaf_stream, middle_server_stream) = tokio::io::duplex(64 * 1024);
        let (mut leaf_reader, mut leaf_writer) = tokio::io::split(leaf_stream);
        let (middle_server_reader, middle_server_writer) = tokio::io::split(middle_server_stream);
        let middle_server = middle.clone();
        let middle_upstream = upstream.clone();
        let leaf_task = tokio::spawn(async move {
            middle_server
                .serve_parts(
                    LEAF,
                    LEAF_LINK,
                    middle_server_writer,
                    middle_server_reader,
                    Some(&middle_upstream),
                )
                .await
        });
        send_message(
            &mut leaf_writer,
            &Message::Hello {
                versions: versions(),
                child: LEAF,
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            receive_message(&mut leaf_reader).await.unwrap(),
            Some(Message::Welcome { .. })
        ));
        send_message(
            &mut leaf_writer,
            &Message::SnapshotBegin {
                snapshot: SnapshotId::from_u64(1),
                base_sequence: PublicationSequence::from_u64(0),
            },
        )
        .await
        .unwrap();
        send_message(
            &mut leaf_writer,
            &Message::SnapshotChunk {
                snapshot: SnapshotId::from_u64(1),
                chunk: 0,
                entries: vec![entry(LEAF, 30)],
            },
        )
        .await
        .unwrap();
        send_message(
            &mut leaf_writer,
            &Message::SnapshotEnd {
                snapshot: SnapshotId::from_u64(1),
                chunks: 1,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            receive_message(&mut leaf_reader).await.unwrap(),
            Some(Message::PublicationAck {
                sequence: PublicationSequence::from_u64(0),
            })
        );

        wait_for_entry(&middle, LEAF, Some(entry(LEAF, 30))).await;
        wait_for_entry(&root, LEAF, Some(entry(LEAF, 20))).await;

        let (reply, response) = oneshot::channel();
        requests
            .send(LinkRequest::Resolve {
                pid: LEAF,
                consistency: ResolveConsistency::Refresh,
                reply,
            })
            .await
            .unwrap();
        let resolved = response.await.unwrap().unwrap();
        assert_eq!(resolved.pid(), LEAF);
        assert_eq!(resolved.revision().authority, ROOT);
        assert!(matches!(
            resolved,
            Resolution::Found { entry: actual, .. } if actual == entry(LEAF, 20)
        ));
        let rewritten = upstream
            .resolve(LEAF, ResolveConsistency::Cached)
            .await
            .unwrap();
        assert!(matches!(
            rewritten,
            Resolution::Found { entry: actual, .. } if actual == entry(LEAF, 10)
        ));

        let request = RequestId::from_u64(70);
        send_message(
            &mut leaf_writer,
            &Message::Resolve {
                request,
                pid: Pid::from_bytes([9; 16]),
                consistency: ResolveConsistency::Refresh,
            },
        )
        .await
        .unwrap();
        let Some(Message::ResolveResult { result, .. }) =
            receive_message(&mut leaf_reader).await.unwrap()
        else {
            panic!("expected forwarded resolution result");
        };
        assert!(matches!(
            result,
            Resolution::NotFound {
                revision: crate::Revision {
                    authority: ROOT,
                    ..
                },
                ..
            }
        ));

        leaf_writer.shutdown().await.unwrap();
        leaf_task.await.unwrap().unwrap();
        wait_for_entry(&middle, LEAF, None).await;
        wait_for_entry(&root, LEAF, None).await;

        completion.shutdown();
        link_task.await.unwrap().unwrap();
        root_task.await.unwrap().unwrap();
        wait_for_entry(&root, MIDDLE, None).await;
    }

    #[tokio::test]
    async fn resnapshot_request_restarts_complete_publication() {
        let service = Arc::new(NameserverService::try_new(MIDDLE, versions(), 1_000).unwrap());
        let (child_stream, parent_stream) = tokio::io::duplex(64 * 1024);
        let (child_reader, mut child_writer) = tokio::io::split(child_stream);
        let (mut parent_reader, mut parent_writer) = tokio::io::split(parent_stream);
        let cache = Arc::new(Mutex::new(ResolverCache::try_new(ROOT).unwrap()));
        let completion = Arc::new(Completion::default());
        let (_requests, request_rx) = mpsc::channel(8);
        let child_completion = completion.clone();
        let child_task = tokio::spawn(async move {
            run_parent_link(
                ROOT,
                service,
                entry(MIDDLE, 20),
                versions(),
                cache,
                Instant::now(),
                request_rx,
                &mut child_writer,
                child_reader,
                child_completion,
                None,
            )
            .await
        });

        assert!(matches!(
            receive_message(&mut parent_reader).await.unwrap(),
            Some(Message::Hello { .. })
        ));
        send_message(
            &mut parent_writer,
            &Message::Welcome {
                version: ProtocolVersion::new(2),
                parent: ROOT,
                link: ROOT_LINK,
            },
        )
        .await
        .unwrap();

        let Some(Message::SnapshotBegin {
            snapshot: first, ..
        }) = receive_message(&mut parent_reader).await.unwrap()
        else {
            panic!("expected initial snapshot");
        };
        assert!(matches!(
            receive_message(&mut parent_reader).await.unwrap(),
            Some(Message::SnapshotChunk { .. })
        ));
        assert!(matches!(
            receive_message(&mut parent_reader).await.unwrap(),
            Some(Message::SnapshotEnd { .. })
        ));
        send_message(
            &mut parent_writer,
            &Message::ResnapshotRequired {
                expected_sequence: PublicationSequence::from_u64(0),
            },
        )
        .await
        .unwrap();

        let Some(Message::SnapshotBegin {
            snapshot: second,
            base_sequence,
        }) = receive_message(&mut parent_reader).await.unwrap()
        else {
            panic!("expected replacement snapshot");
        };
        assert_ne!(first, second);
        assert_eq!(base_sequence, PublicationSequence::from_u64(0));
        assert!(matches!(
            receive_message(&mut parent_reader).await.unwrap(),
            Some(Message::SnapshotChunk { .. })
        ));
        assert!(matches!(
            receive_message(&mut parent_reader).await.unwrap(),
            Some(Message::SnapshotEnd { .. })
        ));
        send_message(
            &mut parent_writer,
            &Message::PublicationAck {
                sequence: PublicationSequence::from_u64(0),
            },
        )
        .await
        .unwrap();
        completion.shutdown();
        child_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn foreign_authority_resolution_is_passed_through_without_caching() {
        let cache = Mutex::new(ResolverCache::try_new(ROOT).unwrap());
        let result = Resolution::NotFound {
            pid: LEAF,
            revision: crate::Revision {
                authority: MIDDLE,
                value: 1,
            },
            valid_for_millis: 100,
        };
        update_cache(&cache, result, CacheTime::from_millis(0))
            .await
            .unwrap();
        assert_eq!(
            cache.lock().await.resolve(LEAF, CacheTime::from_millis(0)),
            None
        );
    }

    #[tokio::test]
    async fn active_waiter_tracks_handshake_and_terminal_state() {
        let (requests, _) = mpsc::channel(1);
        let cache = Arc::new(Mutex::new(ResolverCache::try_new(ROOT).unwrap()));
        let completion = Arc::new(Completion::default());
        let (state_tx, state) = watch::channel(ParentLinkState::Pending);
        let link = ParentLink {
            requests,
            cache,
            next_hop: DatagramAddr::new("test", [10]),
            started: Instant::now(),
            completion: completion.clone(),
            state,
        };
        let waiter = tokio::spawn({
            let link = link.clone();
            async move { link.wait_active().await }
        });
        state_tx.send_replace(ParentLinkState::Active(ROOT_LINK));
        assert!(matches!(waiter.await.unwrap(), Ok(ROOT_LINK)));

        state_tx.send_replace(ParentLinkState::Rejected(RejectCode::AlreadyLinked));
        assert!(matches!(
            link.wait_active().await,
            Err(ParentLinkError::Rejected {
                code: RejectCode::AlreadyLinked
            })
        ));
        state_tx.send_replace(ParentLinkState::Closed);
        completion.terminate();
        assert!(matches!(
            link.wait_active().await,
            Err(ParentLinkError::ParentClosed)
        ));
    }

    #[test]
    fn export_rewrites_descendants_and_diff_is_deterministic() {
        let local = entry(MIDDLE, 20);
        let published = BTreeMap::from([(MIDDLE, local.clone()), (LEAF, entry(LEAF, 20))]);
        let desired = BTreeMap::from([(MIDDLE, local), (ROOT, entry(ROOT, 20))]);
        let (upserts, removals) = diff(&published, &desired);
        assert_eq!(upserts, vec![entry(ROOT, 20)]);
        assert_eq!(removals, vec![LEAF]);
    }
}
