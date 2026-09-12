/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use chrysalis_core::Pid;
use chrysalis_transport::LinkLocalProtocolId;
use chrysalis_transport::Stream;
use libsql::Connection;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::watch;

use crate::Change;
use crate::EligibilityMode;
use crate::Error;
use crate::OriginEntry;
use crate::SITE_ID_LEN;
use crate::SiteScope;
use crate::SiteSet;
use crate::VersionFrontier;
use crate::apply_change_chunk;
use crate::apply_table_schema;
use crate::bootstrap_origins;
use crate::changes_for_peer;
use crate::db_version;
use crate::initialize_frontiers;
use crate::load_explicit_frontier_sites;
use crate::load_peer_frontier_state;
use crate::origin_entries_after;
use crate::protocol::MAX_FRAME_LEN;
use crate::protocol::Message;
use crate::protocol::MessageReader;
use crate::protocol::encoded_change_len;
use crate::protocol::send_message;
use crate::reconcile_frontier;
use crate::record_frontier;
use crate::register_origin_ids;
use crate::site_id;
use crate::table_schemas;

/// The version-four CRR replication protocol on the link-local stream mux.
pub const SQLITE_LINK_PROTOCOL: LinkLocalProtocolId =
    LinkLocalProtocolId::from_bytes(*b"chrysalis.crr.v4");

const CHANGE_CHUNK_TARGET_LEN: usize = 1024 * 1024;
const CHANGE_CHUNK_OVERHEAD: usize = 5;
const CHANGE_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// One local CRR replica that can synchronize with adjacent peers.
#[derive(Clone)]
pub struct Replica {
    inner: Arc<Inner>,
}

struct Inner {
    connection: Connection,
    site_id: Vec<u8>,
    changes: watch::Sender<u64>,
    active_peers: Mutex<HashSet<Pid>>,
    peer_scopes: Mutex<BTreeMap<Pid, SiteScope>>,
    peer_scope_changes: watch::Sender<u64>,
    synchronized_peers: Mutex<HashSet<Pid>>,
    peer_synchronization_changes: watch::Sender<u64>,
    origins: Mutex<OriginRegistry>,
    apply_lock: Arc<AsyncMutex<()>>,
    remote_applies: Arc<AtomicUsize>,
}

struct InFlight {
    batch_id: u64,
    advances: VersionFrontier,
    observed_db_version: Option<i64>,
}

struct Outbound {
    origin_position: i64,
    scope_mode: Option<EligibilityMode>,
    explicit_sites: BTreeSet<Vec<u8>>,
    frontier_dirty: bool,
    observed_db_version: i64,
    peer_scope: SiteScope,
    local_scope: SiteScope,
    next_batch_id: u64,
    in_flight: Option<InFlight>,
    schemas: BTreeMap<String, [u8; 32]>,
    synchronized: bool,
}

struct IncomingBatch {
    batch_id: u64,
}

struct OriginRegistry {
    entries: Vec<OriginEntry>,
    known: HashSet<Vec<u8>>,
    position: i64,
}

impl OriginRegistry {
    fn from_entries(entries: Vec<OriginEntry>) -> Result<Self, Error> {
        let mut known = HashSet::new();
        let mut position = 0;
        for entry in &entries {
            if entry.position <= position || !known.insert(entry.site_id.clone()) {
                return Err(Error::InvalidMetadata);
            }
            position = entry.position;
        }
        Ok(Self {
            entries,
            known,
            position,
        })
    }
}

/// The dynamically advertised origin scope for one replication link.
#[derive(Clone)]
pub struct SitePublisher {
    own_site_id: Vec<u8>,
    state: Arc<Mutex<SiteScope>>,
    updates: watch::Sender<SiteScope>,
}

impl SitePublisher {
    /// Returns the currently advertised origin scope.
    pub fn scope(&self) -> SiteScope {
        self.state
            .lock()
            .expect("site publisher lock poisoned")
            .clone()
    }

    /// Replaces an explicit scope's set with a new monotonic generation.
    pub fn set(&self, site_ids: Vec<Vec<u8>>) -> Result<SiteScope, Error> {
        let mut current = self.state.lock().expect("site publisher lock poisoned");
        let SiteScope::Explicit(current_sites) = &*current else {
            return Err(Error::ComplementIsDerived);
        };
        let generation = current_sites
            .generation()
            .checked_add(1)
            .expect("site-set generation exhausted");
        let sites = SiteSet::try_new(generation, site_ids)?;
        if !sites.contains(&self.own_site_id) {
            return Err(Error::OwnSiteMissing);
        }
        let scope = SiteScope::Explicit(sites);
        *current = scope.clone();
        self.updates.send_replace(scope.clone());
        Ok(scope)
    }

    fn subscribe(&self) -> watch::Receiver<SiteScope> {
        self.updates.subscribe()
    }
}

impl Replica {
    /// Initializes replication metadata and installs a write-notification hook.
    ///
    /// SQLite supports one update hook per connection, so this replaces a hook previously installed
    /// by the caller.
    pub async fn new(connection: Connection) -> Result<Self, Error> {
        initialize_frontiers(&connection).await?;
        let site_id = site_id(&connection).await?;
        if site_id.len() != SITE_ID_LEN {
            return Err(crate::SiteSetError::InvalidSiteId.into());
        }
        bootstrap_origins(&connection, &site_id).await?;
        let origins = OriginRegistry::from_entries(origin_entries_after(&connection, 0).await?)?;
        let (changes, _) = watch::channel(0);
        let (peer_scope_changes, _) = watch::channel(0);
        let (peer_synchronization_changes, _) = watch::channel(0);
        let remote_applies = Arc::new(AtomicUsize::new(0));
        let hook_changes = changes.clone();
        let hook_remote_applies = remote_applies.clone();
        connection.add_update_hook(Box::new(move |_, _, _, _| {
            if hook_remote_applies.load(Ordering::Acquire) == 0 {
                notify(&hook_changes);
            }
        }))?;
        Ok(Self {
            inner: Arc::new(Inner {
                connection,
                site_id,
                changes,
                active_peers: Mutex::new(HashSet::new()),
                peer_scopes: Mutex::new(BTreeMap::new()),
                peer_scope_changes,
                synchronized_peers: Mutex::new(HashSet::new()),
                peer_synchronization_changes,
                origins: Mutex::new(origins),
                apply_lock: Arc::new(AsyncMutex::new(())),
                remote_applies,
            }),
        })
    }

    /// Returns this database's stable CRR site ID.
    pub fn site_id(&self) -> &[u8] {
        &self.inner.site_id
    }

    /// Creates the site-set publisher for one adjacent replication link.
    ///
    /// The set describes the local side of that edge and must include this
    /// replica's own site ID. A topology coordinator updates it when processes
    /// join or leave. Separate links require separate publishers so a gateway
    /// can apply split-horizon filtering.
    pub fn publisher(&self, site_ids: Vec<Vec<u8>>) -> Result<SitePublisher, Error> {
        let scope = SiteScope::explicit(0, site_ids)?;
        let sites = scope
            .as_explicit()
            .expect("explicit scope constructor returned a complement");
        if !sites.contains(&self.inner.site_id) {
            return Err(Error::OwnSiteMissing);
        }
        let (updates, _) = watch::channel(scope.clone());
        Ok(SitePublisher {
            own_site_id: self.inner.site_id.clone(),
            state: Arc::new(Mutex::new(scope)),
            updates,
        })
    }

    /// Creates a publisher that owns every site outside the peer's explicit scope.
    pub fn complement_publisher(&self) -> SitePublisher {
        let scope = SiteScope::ComplementOfPeer;
        let (updates, _) = watch::channel(scope.clone());
        SitePublisher {
            own_site_id: self.inner.site_id.clone(),
            state: Arc::new(Mutex::new(scope)),
            updates,
        }
    }

    /// Creates an advertisement containing only this replica's site ID.
    pub fn local_publisher(&self) -> SitePublisher {
        self.publisher(vec![self.inner.site_id.clone()])
            .expect("local site ID was validated during replica construction")
    }

    /// Explicitly notifies all sessions that application writes may be available.
    ///
    /// Writes through this replica's connection are detected automatically. This remains useful for
    /// integrations that can provide a lower-latency signal for writes through another connection.
    pub fn notify_changed(&self) {
        notify(&self.inner.changes);
    }

    /// Returns the latest origin scope advertised by every active peer.
    pub fn peer_scopes(&self) -> BTreeMap<Pid, SiteScope> {
        self.inner
            .peer_scopes
            .lock()
            .expect("peer scope lock poisoned")
            .clone()
    }

    /// Subscribes to coalescing peer scope changes.
    ///
    /// On notification, call [`Self::peer_scopes`] to obtain a complete snapshot.
    pub fn subscribe_peer_scopes(&self) -> watch::Receiver<u64> {
        self.inner.peer_scope_changes.subscribe()
    }

    /// Returns whether a peer has sent a synchronization marker after its latest updates.
    pub fn is_peer_synchronized(&self, peer: Pid) -> bool {
        self.inner
            .synchronized_peers
            .lock()
            .expect("synchronized peer lock poisoned")
            .contains(&peer)
    }

    /// Subscribes to coalescing peer synchronization changes.
    pub fn subscribe_peer_synchronization(&self) -> watch::Receiver<u64> {
        self.inner.peer_synchronization_changes.subscribe()
    }

    /// Replicates bidirectionally over one authenticated Chrysalis stream.
    pub async fn replicate(
        &self,
        peer: Pid,
        stream: Stream,
        sites: SitePublisher,
    ) -> Result<(), Error> {
        let (writer, reader) = stream.into_parts();
        self.replicate_io(peer, writer, reader, sites).await
    }

    /// Replicates over generic ordered byte streams.
    ///
    /// This is useful for in-process composition and protocol testing. Network callers should use
    /// [`crate::ReplicationTopology`] with a Chrysalis node.
    pub async fn replicate_io<W, R>(
        &self,
        peer: Pid,
        writer: W,
        reader: R,
        sites: SitePublisher,
    ) -> Result<(), Error>
    where
        W: AsyncWrite + Unpin,
        R: AsyncRead + Unpin,
    {
        let _peer = PeerGuard::enter(self.inner.clone(), peer)?;
        run(self.clone(), peer, writer, reader, sites).await
    }
}

struct PeerGuard {
    inner: Arc<Inner>,
    peer: Pid,
}

impl PeerGuard {
    fn enter(inner: Arc<Inner>, peer: Pid) -> Result<Self, Error> {
        let inserted = inner
            .active_peers
            .lock()
            .expect("active peer lock poisoned")
            .insert(peer);
        if !inserted {
            return Err(Error::DuplicatePeer);
        }
        Ok(Self { inner, peer })
    }
}

impl Drop for PeerGuard {
    fn drop(&mut self) {
        assert!(
            self.inner
                .active_peers
                .lock()
                .expect("active peer lock poisoned")
                .remove(&self.peer),
            "active peer disappeared before session completion"
        );
        if self
            .inner
            .peer_scopes
            .lock()
            .expect("peer scope lock poisoned")
            .remove(&self.peer)
            .is_some()
        {
            notify(&self.inner.peer_scope_changes);
        }
        set_peer_synchronized(&self.inner, self.peer, false);
    }
}

async fn run<W, R>(
    replica: Replica,
    peer: Pid,
    mut writer: W,
    reader: R,
    local_site_publisher: SitePublisher,
) -> Result<(), Error>
where
    W: AsyncWrite + Unpin,
    R: AsyncRead + Unpin,
{
    let mut local_scopes = local_site_publisher.subscribe();
    let mut local_changes = replica.inner.changes.subscribe();
    let mut poll = tokio::time::interval(CHANGE_POLL_INTERVAL);
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    poll.tick().await;
    let hello_scope = local_scopes.borrow().clone();
    send_message(
        &mut writer,
        &Message::Hello {
            site_id: replica.inner.site_id.clone(),
            scope: hello_scope.clone(),
        },
    )
    .await?;

    let mut messages = MessageReader::new(reader);
    let Some(Message::Hello {
        site_id: peer_site_id,
        scope: mut peer_scope,
    }) = messages.receive().await?
    else {
        return Err(Error::MissingHello);
    };
    validate_scope_pair(
        &replica.inner.site_id,
        &hello_scope,
        &peer_site_id,
        &peer_scope,
    )?;
    set_peer_scope(&replica.inner, peer, peer_scope.clone());
    let (peer_state, explicit_sites) = {
        let _apply_lock = replica.inner.apply_lock.lock().await;
        let state =
            load_peer_frontier_state(&replica.inner.connection, peer, &peer_site_id).await?;
        let sites = match state {
            Some(state) => {
                load_explicit_frontier_sites(
                    &replica.inner.connection,
                    peer,
                    &peer_site_id,
                    state.mode,
                )
                .await?
            }
            None => BTreeSet::new(),
        };
        (state, sites)
    };
    let mut outbound = Outbound {
        origin_position: peer_state.map_or(0, |state| state.origin_position),
        scope_mode: peer_state.map(|state| state.mode),
        explicit_sites,
        frontier_dirty: true,
        observed_db_version: -1,
        peer_scope: peer_scope.clone(),
        local_scope: hello_scope.clone(),
        next_batch_id: 1,
        in_flight: None,
        schemas: BTreeMap::new(),
        synchronized: false,
    };
    let mut incoming = None;
    send_schema_updates(&replica, &mut writer, &mut outbound).await?;
    send_next(
        &replica,
        &mut writer,
        peer,
        &peer_site_id,
        &peer_scope,
        &hello_scope,
        &mut outbound,
    )
    .await?;

    loop {
        tokio::select! {
            message = messages.receive() => {
                let Some(message) = message? else {
                    return Ok(());
                };
                match message {
                    Message::Hello { .. } => return Err(Error::UnexpectedHello),
                    Message::Schema(schema) => {
                        if incoming.is_some() {
                            return Err(Error::UnexpectedBatch);
                        }
                        set_peer_synchronized(&replica.inner, peer, false);
                        let table = schema.table.clone();
                        let hash = schema.hash;
                        let apply_lock = replica.inner.apply_lock.lock().await;
                        apply_table_schema(&replica.inner.connection, &schema).await?;
                        drop(apply_lock);
                        outbound.schemas.insert(table, hash);
                        replica.notify_changed();
                    }
                    Message::Scope(scope) => {
                        set_peer_synchronized(&replica.inner, peer, false);
                        update_peer_scope(&mut peer_scope, scope)?;
                        let local_scope = local_scopes.borrow().clone();
                        validate_scope_pair(
                            &replica.inner.site_id,
                            &local_scope,
                            &peer_site_id,
                            &peer_scope,
                        )?;
                        set_peer_scope(&replica.inner, peer, peer_scope.clone());
                    }
                    Message::BeginBatch { batch_id } => {
                        if incoming.is_some() {
                            return Err(Error::UnexpectedBatch);
                        }
                        set_peer_synchronized(&replica.inner, peer, false);
                        incoming = Some(IncomingBatch { batch_id });
                    }
                    Message::BatchChunk(changes) => {
                        if incoming.is_none() {
                            return Err(Error::UnexpectedBatch);
                        }
                        validate_changes(&changes)?;
                        apply_remote_chunk(&replica, &changes).await?;
                    }
                    Message::CommitBatch => {
                        let Some(batch) = incoming.take() else {
                            return Err(Error::UnexpectedBatch);
                        };
                        send_message(
                            &mut writer,
                            &Message::Ack {
                                batch_id: batch.batch_id,
                            },
                        )
                        .await?;
                    }
                    Message::Ack { batch_id } => {
                        let Some(sent) = outbound.in_flight.take() else {
                            return Err(Error::UnexpectedAck);
                        };
                        if sent.batch_id != batch_id {
                            return Err(Error::UnexpectedAck);
                        }
                        {
                            let _apply_lock = replica.inner.apply_lock.lock().await;
                            record_frontier(
                                &replica.inner.connection,
                                peer,
                                &peer_site_id,
                                &sent.advances,
                            )
                            .await?;
                        }
                        if let Some(version) = sent.observed_db_version {
                            outbound.observed_db_version = version;
                        }
                    }
                    Message::Synchronized => {
                        if incoming.is_some() {
                            return Err(Error::UnexpectedBatch);
                        }
                        set_peer_synchronized(&replica.inner, peer, true);
                    }
                }
            }
            changed = local_scopes.changed() => {
                changed.map_err(|_| Error::Closed)?;
                let scope = local_scopes.borrow().clone();
                validate_scope_pair(
                    &replica.inner.site_id,
                    &scope,
                    &peer_site_id,
                    &peer_scope,
                )?;
                send_message(&mut writer, &Message::Scope(scope)).await?;
            }
            changed = local_changes.changed() => {
                changed.map_err(|_| Error::Closed)?;
            }
            _ = poll.tick() => {}
        }
        send_schema_updates(&replica, &mut writer, &mut outbound).await?;
        let local_scope = local_scopes.borrow().clone();
        refresh_scope(&peer_scope, &local_scope, &mut outbound);
        send_next(
            &replica,
            &mut writer,
            peer,
            &peer_site_id,
            &peer_scope,
            &local_scope,
            &mut outbound,
        )
        .await?;
    }
}

async fn apply_remote_chunk(replica: &Replica, changes: &[Change]) -> Result<(), Error> {
    let apply_lock = replica.inner.apply_lock.lock().await;
    let remote_apply = RemoteApplyGuard::enter(replica.inner.remote_applies.clone());
    validate_change_schemas(&replica.inner.connection, changes).await?;
    let transaction = replica.inner.connection.transaction().await?;
    apply_change_chunk(&transaction, changes).await?;
    transaction.commit().await?;
    drop(remote_apply);
    drop(apply_lock);
    replica.notify_changed();
    Ok(())
}

async fn send_schema_updates<W>(
    replica: &Replica,
    writer: &mut W,
    outbound: &mut Outbound,
) -> Result<(), Error>
where
    W: AsyncWrite + Unpin,
{
    let schemas = {
        let _apply_lock = replica.inner.apply_lock.lock().await;
        table_schemas(&replica.inner.connection).await?
    };
    for schema in schemas.into_values() {
        match outbound.schemas.get(&schema.table) {
            Some(hash) if hash == &schema.hash => continue,
            Some(_) => return Err(Error::SchemaChanged(schema.table)),
            None => {}
        }
        send_message(writer, &Message::Schema(schema.clone())).await?;
        outbound.schemas.insert(schema.table, schema.hash);
        outbound.synchronized = false;
    }
    Ok(())
}

async fn validate_change_schemas(connection: &Connection, changes: &[Change]) -> Result<(), Error> {
    let schemas = table_schemas(connection).await?;
    if let Some(change) = changes
        .iter()
        .find(|change| !schemas.contains_key(&change.table))
    {
        return Err(Error::MissingSchema(change.table.clone()));
    }
    Ok(())
}

fn set_peer_scope(inner: &Inner, peer: Pid, scope: SiteScope) {
    let changed = inner
        .peer_scopes
        .lock()
        .expect("peer scope lock poisoned")
        .insert(peer, scope.clone())
        .as_ref()
        != Some(&scope);
    if changed {
        notify(&inner.peer_scope_changes);
    }
}

fn set_peer_synchronized(inner: &Inner, peer: Pid, synchronized: bool) {
    let changed = if synchronized {
        inner
            .synchronized_peers
            .lock()
            .expect("synchronized peer lock poisoned")
            .insert(peer)
    } else {
        inner
            .synchronized_peers
            .lock()
            .expect("synchronized peer lock poisoned")
            .remove(&peer)
    };
    if changed {
        notify(&inner.peer_synchronization_changes);
    }
}

fn notify(sender: &watch::Sender<u64>) {
    sender.send_modify(|generation| {
        *generation = generation
            .checked_add(1)
            .expect("notification generation exhausted");
    });
}

fn validate_scope_pair(
    local_site_id: &[u8],
    local_scope: &SiteScope,
    peer_site_id: &[u8],
    peer_scope: &SiteScope,
) -> Result<(), Error> {
    if local_site_id.len() != SITE_ID_LEN || peer_site_id.len() != SITE_ID_LEN {
        return Err(crate::SiteSetError::InvalidSiteId.into());
    }
    if !scope_contains(local_scope, local_site_id, peer_scope)? {
        return Err(Error::OwnSiteMissing);
    }
    if !scope_contains(peer_scope, peer_site_id, local_scope)? {
        return Err(Error::PeerSiteMissing);
    }
    Ok(())
}

fn scope_contains(
    scope: &SiteScope,
    site_id: &[u8],
    peer_scope: &SiteScope,
) -> Result<bool, Error> {
    match scope {
        SiteScope::Explicit(sites) => Ok(sites.contains(site_id)),
        SiteScope::ComplementOfPeer => {
            let SiteScope::Explicit(peer_sites) = peer_scope else {
                return Err(Error::UnanchoredComplement);
            };
            Ok(!peer_sites.contains(site_id))
        }
    }
}

fn update_peer_scope(current: &mut SiteScope, update: SiteScope) -> Result<(), Error> {
    match (&*current, &update) {
        (SiteScope::ComplementOfPeer, SiteScope::ComplementOfPeer) => Ok(()),
        (SiteScope::Explicit(current_sites), SiteScope::Explicit(update_sites)) => {
            if update_sites.generation() < current_sites.generation() {
                return Err(Error::SiteGenerationRegressed);
            }
            if update_sites.generation() == current_sites.generation() {
                if update_sites != current_sites {
                    return Err(Error::ConflictingSiteGeneration);
                }
                return Ok(());
            }
            *current = update;
            Ok(())
        }
        _ => Err(Error::SiteScopeChanged),
    }
}

fn validate_changes(changes: &[Change]) -> Result<(), Error> {
    if changes
        .iter()
        .any(|change| change.db_version < 0 || change.site_id.len() != SITE_ID_LEN)
    {
        return Err(Error::InvalidBatch);
    }
    Ok(())
}

fn refresh_scope(peer_scope: &SiteScope, local_scope: &SiteScope, outbound: &mut Outbound) {
    if outbound.in_flight.is_some() {
        return;
    }
    if &outbound.peer_scope != peer_scope || &outbound.local_scope != local_scope {
        outbound.peer_scope = peer_scope.clone();
        outbound.local_scope = local_scope.clone();
        outbound.frontier_dirty = true;
        outbound.observed_db_version = -1;
        outbound.synchronized = false;
    }
}

fn effective_scope(
    peer_scope: &SiteScope,
    local_scope: &SiteScope,
) -> Result<(EligibilityMode, BTreeSet<Vec<u8>>), Error> {
    match local_scope {
        SiteScope::Explicit(sites) => Ok((
            EligibilityMode::Allow,
            sites.site_ids().iter().cloned().collect(),
        )),
        SiteScope::ComplementOfPeer => {
            let SiteScope::Explicit(sites) = peer_scope else {
                return Err(Error::UnanchoredComplement);
            };
            Ok((
                EligibilityMode::Block,
                sites.site_ids().iter().cloned().collect(),
            ))
        }
    }
}

fn origin_eligible(
    mode: EligibilityMode,
    explicit_sites: &BTreeSet<Vec<u8>>,
    site_id: &[u8],
) -> bool {
    match mode {
        EligibilityMode::Allow => explicit_sites.contains(site_id),
        EligibilityMode::Block => !explicit_sites.contains(site_id),
    }
}

async fn refresh_origins(replica: &Replica) -> Result<(), Error> {
    let position = replica
        .inner
        .origins
        .lock()
        .expect("origin registry lock poisoned")
        .position;
    let entries = origin_entries_after(&replica.inner.connection, position).await?;
    if entries.is_empty() {
        return Ok(());
    }
    let mut origins = replica
        .inner
        .origins
        .lock()
        .expect("origin registry lock poisoned");
    for entry in entries {
        if entry.position <= origins.position || !origins.known.insert(entry.site_id.clone()) {
            return Err(Error::InvalidMetadata);
        }
        origins.position = entry.position;
        origins.entries.push(entry);
    }
    Ok(())
}

async fn send_next<W>(
    replica: &Replica,
    writer: &mut W,
    peer: Pid,
    peer_site_id: &[u8],
    peer_scope: &SiteScope,
    local_scope: &SiteScope,
    outbound: &mut Outbound,
) -> Result<(), Error>
where
    W: AsyncWrite + Unpin,
{
    if outbound.in_flight.is_some() {
        return Ok(());
    }
    let apply_lock = replica.inner.apply_lock.lock().await;
    let current = db_version(&replica.inner.connection).await?;
    if current <= outbound.observed_db_version && !outbound.frontier_dirty {
        drop(apply_lock);
        if !outbound.synchronized {
            send_message(writer, &Message::Synchronized).await?;
            outbound.synchronized = true;
        }
        return Ok(());
    }
    outbound.synchronized = false;
    let (mode, explicit_sites) = effective_scope(peer_scope, local_scope)?;
    if outbound.frontier_dirty {
        let missing: Vec<_> = {
            let origins = replica
                .inner
                .origins
                .lock()
                .expect("origin registry lock poisoned");
            explicit_sites
                .iter()
                .filter(|site_id| !origins.known.contains(*site_id))
                .cloned()
                .collect()
        };
        if !missing.is_empty() {
            register_origin_ids(&replica.inner.connection, &missing).await?;
        }
    }
    refresh_origins(replica).await?;
    let (origin_position, new_origins, all_origins) = {
        let origins = replica
            .inner
            .origins
            .lock()
            .expect("origin registry lock poisoned");
        if outbound.origin_position > origins.position {
            return Err(Error::InvalidMetadata);
        }
        let new_origins: Vec<_> = origins
            .entries
            .iter()
            .filter(|entry| entry.position > outbound.origin_position)
            .cloned()
            .collect();
        let all_origins = (outbound.scope_mode != Some(mode)).then(|| origins.entries.to_vec());
        (origins.position, new_origins, all_origins)
    };
    if outbound.frontier_dirty || !new_origins.is_empty() {
        let mut eligibility = BTreeMap::new();
        if let Some(origins) = all_origins {
            for origin in origins {
                eligibility.insert(
                    origin.site_id.clone(),
                    origin_eligible(mode, &explicit_sites, &origin.site_id),
                );
            }
        } else {
            for site_id in explicit_sites.difference(&outbound.explicit_sites) {
                eligibility.insert(site_id.clone(), mode == EligibilityMode::Allow);
            }
            for site_id in outbound.explicit_sites.difference(&explicit_sites) {
                eligibility.insert(site_id.clone(), mode == EligibilityMode::Block);
            }
        }
        for origin in new_origins {
            eligibility.insert(
                origin.site_id.clone(),
                origin_eligible(mode, &explicit_sites, &origin.site_id),
            );
        }
        reconcile_frontier(
            &replica.inner.connection,
            peer,
            peer_site_id,
            origin_position,
            mode,
            &eligibility,
        )
        .await?;
        outbound.origin_position = origin_position;
        outbound.scope_mode = Some(mode);
        outbound.explicit_sites = explicit_sites;
        outbound.frontier_dirty = false;
    }
    let selected = changes_for_peer(&replica.inner.connection, peer, peer_site_id).await?;
    drop(apply_lock);
    if selected
        .batch
        .changes
        .iter()
        .any(|change| !outbound.schemas.contains_key(&change.table))
    {
        return Ok(());
    }
    if selected.batch.changes.is_empty() {
        outbound.observed_db_version = current;
        send_message(writer, &Message::Synchronized).await?;
        outbound.synchronized = true;
        return Ok(());
    }
    let batch_id = outbound.next_batch_id;
    outbound.next_batch_id = outbound
        .next_batch_id
        .checked_add(1)
        .expect("replication batch ID exhausted");
    send_message(writer, &Message::BeginBatch { batch_id }).await?;
    for chunk in change_chunks(selected.batch.changes)? {
        send_message(writer, &Message::BatchChunk(chunk)).await?;
    }
    send_message(writer, &Message::CommitBatch).await?;
    outbound.in_flight = Some(InFlight {
        batch_id,
        advances: selected.batch.advances,
        observed_db_version: selected.complete.then_some(current),
    });
    Ok(())
}

fn change_chunks(changes: Vec<Change>) -> Result<Vec<Vec<Change>>, Error> {
    let mut chunks = Vec::new();
    let mut chunk = Vec::new();
    let mut chunk_len = CHANGE_CHUNK_OVERHEAD;
    for change in changes {
        let change_len = encoded_change_len(&change)?;
        let framed_change_len = change_len
            .checked_add(CHANGE_CHUNK_OVERHEAD)
            .ok_or(crate::ProtocolError::FrameTooLarge { length: usize::MAX })?;
        if framed_change_len > MAX_FRAME_LEN {
            return Err(crate::ProtocolError::FrameTooLarge {
                length: framed_change_len,
            }
            .into());
        }
        if !chunk.is_empty()
            && chunk_len
                .checked_add(change_len)
                .is_none_or(|length| length > CHANGE_CHUNK_TARGET_LEN)
        {
            chunks.push(std::mem::take(&mut chunk));
            chunk_len = CHANGE_CHUNK_OVERHEAD;
        }
        chunk_len = chunk_len
            .checked_add(change_len)
            .ok_or(crate::ProtocolError::FrameTooLarge { length: usize::MAX })?;
        chunk.push(change);
    }
    if !chunk.is_empty() {
        chunks.push(chunk);
    }
    Ok(chunks)
}

struct RemoteApplyGuard(Arc<AtomicUsize>);

impl RemoteApplyGuard {
    fn enter(active: Arc<AtomicUsize>) -> Self {
        active.fetch_add(1, Ordering::AcqRel);
        Self(active)
    }
}

impl Drop for RemoteApplyGuard {
    fn drop(&mut self) {
        let previous = self.0.fetch_sub(1, Ordering::AcqRel);
        assert!(previous > 0, "remote apply count underflow");
    }
}

#[cfg(test)]
mod tests {
    use libsql::Value;

    use super::*;

    fn change_with_blob(length: usize, sequence: i64) -> Change {
        Change {
            table: "items".into(),
            pk: vec![sequence as u8],
            cid: "value".into(),
            value: Value::Blob(vec![0x42; length]),
            col_version: 1,
            db_version: 1,
            site_id: vec![1; 16],
            cl: 1,
            seq: sequence,
        }
    }

    #[test]
    fn change_chunks_are_bounded_and_ordered() {
        let chunks = change_chunks(vec![
            change_with_blob(700 * 1024, 1),
            change_with_blob(700 * 1024, 2),
        ])
        .unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0][0].seq, 1);
        assert_eq!(chunks[1][0].seq, 2);
        assert!(change_chunks(Vec::new()).unwrap().is_empty());
    }

    #[test]
    fn change_chunk_rejects_one_oversized_change() {
        assert!(matches!(
            change_chunks(vec![change_with_blob(MAX_FRAME_LEN, 1)]),
            Err(Error::Protocol(crate::ProtocolError::FrameTooLarge { .. }))
        ));
    }

    #[test]
    fn explicit_scope_updates_are_monotonic_and_replayable() {
        let mut current = SiteScope::explicit(2, vec![vec![1; 16]]).unwrap();
        update_peer_scope(
            &mut current,
            SiteScope::explicit(3, vec![vec![1; 16], vec![2; 16]]).unwrap(),
        )
        .unwrap();
        let replay = current.clone();
        update_peer_scope(&mut current, replay).unwrap();
        assert!(matches!(
            update_peer_scope(
                &mut current,
                SiteScope::explicit(1, vec![vec![1; 16]]).unwrap()
            ),
            Err(Error::SiteGenerationRegressed)
        ));
        assert!(matches!(
            update_peer_scope(
                &mut current,
                SiteScope::explicit(3, vec![vec![3; 16]]).unwrap()
            ),
            Err(Error::ConflictingSiteGeneration)
        ));
        assert!(matches!(
            update_peer_scope(&mut current, SiteScope::ComplementOfPeer),
            Err(Error::SiteScopeChanged)
        ));
    }

    #[test]
    fn complement_scope_is_anchored_by_the_opposite_explicit_scope() {
        let child_site = vec![1; 16];
        let parent_site = vec![2; 16];
        let child = SiteScope::explicit(0, vec![child_site.clone()]).unwrap();
        let parent = SiteScope::ComplementOfPeer;
        validate_scope_pair(&child_site, &child, &parent_site, &parent).unwrap();
        assert!(scope_contains(&child, &child_site, &parent).unwrap());
        assert!(scope_contains(&parent, &parent_site, &child).unwrap());
        assert!(!scope_contains(&parent, &child_site, &child).unwrap());
        assert!(matches!(
            validate_scope_pair(&child_site, &parent, &parent_site, &parent),
            Err(Error::UnanchoredComplement)
        ));
    }
}
