/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! One database replica and its peer-session state machines.
//!
//! [`Replica`] owns the durable SQLite connection and trusted schemas. An
//! application opens a [`ReplicaTransaction`], changes application tables,
//! captures each final row or deleted key, and consumes the transaction with
//! [`ReplicaTransaction::commit`]. The commit stores the application changes,
//! logical clock, change log, and resolved-row metadata atomically.
//!
//! Each call to [`Replica::replicate`] runs one bidirectional peer session.
//! Its inbound half validates schemas and commits received chunks; its outbound
//! half derives the origins eligible for that link, selects complete logical
//! versions beyond the durable peer frontier, and waits for an acknowledgement
//! before advancing that frontier. SQLite holds all restart-critical state.
//! The structs in this module hold only live sessions, notifications, cached
//! origins, and batches awaiting acknowledgement.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use chrysalis_core::Pid;
use chrysalis_transport::LinkLocalProtocolId;
use chrysalis_transport::Stream;
use libsql::Connection;
use libsql::Transaction;
use libsql::TransactionBehavior;
use libsql::Value;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::OwnedMutexGuard;
use tokio::sync::watch;

use crate::Change;
use crate::EligibilityMode;
use crate::Error;
use crate::Mutation;
use crate::OriginEntry;
use crate::SITE_ID_LEN;
use crate::SiteScope;
use crate::SiteSet;
use crate::TableSchema;
use crate::VersionFrontier;
use crate::bootstrap_origins;
use crate::changes_for_peer;
use crate::initialize_sync_metadata;
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
use crate::storage;

/// The version-five typed SQLite replication protocol on the link-local stream mux.
pub const SQLITE_LINK_PROTOCOL: LinkLocalProtocolId =
    LinkLocalProtocolId::from_bytes(*b"chrysalis.sql.v5");

const CHANGE_CHUNK_TARGET_LEN: usize = 1024 * 1024;
const CHANGE_CHUNK_OVERHEAD: usize = 5;
const CHANGE_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// One local SQLite replica that can synchronize with adjacent peers.
#[derive(Clone)]
pub struct Replica {
    inner: Arc<Inner>,
}

struct Inner {
    connection: Connection,
    site_id: Vec<u8>,
    schemas: BTreeMap<String, TableSchema>,
    changes: watch::Sender<u64>,
    active_peers: Mutex<HashSet<Pid>>,
    peer_scopes: Mutex<BTreeMap<Pid, SiteScope>>,
    peer_scope_changes: watch::Sender<u64>,
    synchronized_peers: Mutex<HashSet<Pid>>,
    peer_synchronization_changes: watch::Sender<u64>,
    origins: Mutex<OriginRegistry>,
    apply_lock: Arc<AsyncMutex<()>>,
}

/// One sent batch whose durable frontier cannot advance until its ack arrives.
struct InFlight {
    batch_id: u64,
    advances: VersionFrontier,
    observed_db_version: Option<i64>,
}

/// Outbound state that is derived from durable frontiers for one live session.
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

/// The batch whose chunks the peer is currently sending.
struct IncomingBatch {
    batch_id: u64,
}

/// An append-only in-memory cache of the durable origin registry.
struct OriginRegistry {
    entries: Vec<OriginEntry>,
    known: HashSet<Vec<u8>>,
    position: i64,
}

impl OriginRegistry {
    /// Reconstructs the cache while checking its monotonic position invariant.
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

/// An application transaction with its captured replication mutations.
///
/// Use the dereferenced [`Transaction`] to modify application tables, then call
/// [`Self::capture_upsert`] or [`Self::capture_delete`] for each affected row.
/// [`Self::commit`] consumes the transaction so application data and the
/// replication log cannot be committed separately.
pub struct ReplicaTransaction {
    inner: Arc<Inner>,
    transaction: Transaction,
    mutations: Vec<Mutation>,
    apply_lock: OwnedMutexGuard<()>,
}

impl Deref for ReplicaTransaction {
    type Target = Transaction;

    fn deref(&self) -> &Self::Target {
        &self.transaction
    }
}

impl ReplicaTransaction {
    /// Captures the current row for `key` as an upsert.
    pub async fn capture_upsert(&mut self, table: &str, key: Vec<Value>) -> Result<(), Error> {
        let schema = self
            .inner
            .schemas
            .get(table)
            .ok_or_else(|| Error::MissingSchema(table.to_owned()))?;
        self.mutations
            .push(storage::capture_upsert(&self.transaction, schema, key).await?);
        Ok(())
    }

    /// Captures a deletion for `key` after deleting the application row.
    pub fn capture_delete(&mut self, table: &str, key: Vec<Value>) -> Result<(), Error> {
        let schema = self
            .inner
            .schemas
            .get(table)
            .ok_or_else(|| Error::MissingSchema(table.to_owned()))?;
        self.mutations.push(storage::capture_delete(schema, key)?);
        Ok(())
    }

    /// Atomically commits the application writes and captured mutations.
    pub async fn commit(self) -> Result<(), Error> {
        let Self {
            inner,
            transaction,
            mutations,
            apply_lock,
        } = self;
        storage::commit_local(transaction, &inner.schemas, &inner.site_id, &mutations).await?;
        drop(apply_lock);
        notify(&inner.changes);
        Ok(())
    }
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
    /// Initializes the trusted schemas and local replication metadata.
    pub async fn new(
        connection: Connection,
        schemas: impl IntoIterator<Item = TableSchema>,
    ) -> Result<Self, Error> {
        let schemas = storage::schema_map(schemas)?;
        storage::initialize(&connection, &schemas).await?;
        initialize_sync_metadata(&connection).await?;
        let site_id = storage::site_id(&connection).await?;
        if site_id.len() != SITE_ID_LEN {
            return Err(crate::SiteSetError::InvalidSiteId.into());
        }
        bootstrap_origins(&connection, &site_id).await?;
        let origins = OriginRegistry::from_entries(origin_entries_after(&connection, 0).await?)?;
        let (changes, _) = watch::channel(0);
        let (peer_scope_changes, _) = watch::channel(0);
        let (peer_synchronization_changes, _) = watch::channel(0);
        Ok(Self {
            inner: Arc::new(Inner {
                connection,
                site_id,
                schemas,
                changes,
                active_peers: Mutex::new(HashSet::new()),
                peer_scopes: Mutex::new(BTreeMap::new()),
                peer_scope_changes,
                synchronized_peers: Mutex::new(HashSet::new()),
                peer_synchronization_changes,
                origins: Mutex::new(origins),
                apply_lock: Arc::new(AsyncMutex::new(())),
            }),
        })
    }

    /// Begins an application transaction that buffers captured mutations.
    pub async fn transaction(&self) -> Result<ReplicaTransaction, Error> {
        let apply_lock = self.inner.apply_lock.clone().lock_owned().await;
        let transaction = self.inner.connection.transaction().await?;
        Ok(ReplicaTransaction {
            inner: self.inner.clone(),
            transaction,
            mutations: Vec::new(),
            apply_lock,
        })
    }

    /// Begins an application transaction with an explicit SQLite locking behavior.
    pub async fn transaction_with_behavior(
        &self,
        behavior: TransactionBehavior,
    ) -> Result<ReplicaTransaction, Error> {
        let apply_lock = self.inner.apply_lock.clone().lock_owned().await;
        let transaction = self
            .inner
            .connection
            .transaction_with_behavior(behavior)
            .await?;
        Ok(ReplicaTransaction {
            inner: self.inner.clone(),
            transaction,
            mutations: Vec::new(),
            apply_lock,
        })
    }

    /// Returns this database's stable replication site ID.
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

/// Registers one active peer and removes all of its live state on every exit path.
///
/// Holding this guard makes duplicate sessions for the same PID impossible.
/// Dropping it after success, error, or cancellation releases the active-peer
/// slot, removes the advertised scope, and clears the synchronized marker.
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

/// Drives one bidirectional session.
///
/// The state machine:
///
/// 1. exchanges hello messages and validates the two origin scopes;
/// 2. restores the peer's durable frontier and advertises local schemas;
/// 3. applies incoming batch chunks before acknowledging their commit;
/// 4. records acknowledgements before selecting another outbound batch; and
/// 5. reacts to messages, scope changes, local writes, and a fallback poll.
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
                    Message::Schema { table, hash } => {
                        if incoming.is_some() {
                            return Err(Error::UnexpectedBatch);
                        }
                        set_peer_synchronized(&replica.inner, peer, false);
                        let local = replica
                            .inner
                            .schemas
                            .get(&table)
                            .ok_or_else(|| Error::MissingSchema(table.clone()))?;
                        if local.hash() != hash {
                            return Err(Error::SchemaConflict(table));
                        }
                        outbound.schemas.insert(table, hash);
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

/// Applies one received chunk atomically and registers every newly seen origin.
async fn apply_remote_chunk(replica: &Replica, changes: &[Change]) -> Result<(), Error> {
    let apply_lock = replica.inner.apply_lock.lock().await;
    let transaction = replica.inner.connection.transaction().await?;
    let origins: Vec<_> = changes
        .iter()
        .map(|change| change.site_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    crate::register_origin_ids_in_transaction(&transaction, &origins).await?;
    storage::apply_change_chunk(&transaction, &replica.inner.schemas, changes).await?;
    transaction.commit().await?;
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
    for schema in replica.inner.schemas.values() {
        match outbound.schemas.get(schema.table()) {
            Some(hash) if hash == &schema.hash() => continue,
            Some(_) => return Err(Error::SchemaChanged(schema.table().to_owned())),
            None => {}
        }
        send_message(
            writer,
            &Message::Schema {
                table: schema.table().to_owned(),
                hash: schema.hash(),
            },
        )
        .await?;
        outbound
            .schemas
            .insert(schema.table().to_owned(), schema.hash());
        outbound.synchronized = false;
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

/// Applies an idempotent, monotonic peer-scope update without changing its mode.
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

/// Extends the in-memory origin cache from its last durable sequence position.
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

/// Reconciles link eligibility and sends at most one complete logical batch.
///
/// The sender:
///
/// 1. waits while a previous batch is in flight;
/// 2. snapshots the database clock and rebuilds durable eligibility if a scope
///    changed or a new origin appeared;
/// 3. selects only complete origin versions beyond this peer's frontier;
/// 4. waits for missing schema acknowledgements instead of sending undecodable
///    rows; and
/// 5. emits one framed batch and retains its frontier advance until its ack.
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
    let current = storage::db_version(&replica.inner.connection).await?;
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

/// Packs ordered changes into bounded frames without splitting a change.
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

#[cfg(test)]
mod tests {
    use libsql::Builder;
    use libsql::Value;
    use tokio::io::duplex;

    use super::*;

    fn item_schema() -> TableSchema {
        TableSchema::try_new(
            "items",
            "CREATE TABLE IF NOT EXISTS items (\
                id INTEGER PRIMARY KEY NOT NULL, value TEXT NOT NULL\
            )",
            &["id", "value"],
            &["id"],
        )
        .expect("item schema is valid")
    }

    fn change_with_blob(length: usize, sequence: i64) -> Change {
        Change {
            table: "items".into(),
            key: vec![Value::Integer(sequence)],
            row: Some(vec![
                Value::Integer(sequence),
                Value::Blob(vec![0x42; length]),
            ]),
            db_version: 1,
            site_id: vec![1; 16],
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

    #[tokio::test]
    async fn replicas_exchange_explicitly_captured_rows() {
        let first_connection = Builder::new_local(":memory:")
            .build()
            .await
            .expect("create first database")
            .connect()
            .expect("connect first database");
        let second_connection = Builder::new_local(":memory:")
            .build()
            .await
            .expect("create second database")
            .connect()
            .expect("connect second database");
        let first = Replica::new(first_connection.clone(), [item_schema()])
            .await
            .expect("create first replica");
        let second = Replica::new(second_connection.clone(), [item_schema()])
            .await
            .expect("create second replica");

        let mut transaction = first.transaction().await.expect("begin application write");
        transaction
            .execute(
                "INSERT INTO items (id, value) VALUES (?1, ?2)",
                vec![Value::Integer(1), Value::Text("replicated".into())],
            )
            .await
            .expect("insert application row");
        transaction
            .capture_upsert("items", vec![Value::Integer(1)])
            .await
            .expect("capture application row");
        transaction.commit().await.expect("commit application row");

        let first_pid = Pid::from_bytes([1; 16]);
        let second_pid = Pid::from_bytes([2; 16]);
        let (first_io, second_io) = duplex(1024 * 1024);
        let (first_reader, first_writer) = tokio::io::split(first_io);
        let (second_reader, second_writer) = tokio::io::split(second_io);
        let first_task = tokio::spawn({
            let first = first.clone();
            let sites = first.local_publisher();
            async move {
                first
                    .replicate_io(second_pid, first_writer, first_reader, sites)
                    .await
            }
        });
        let second_task = tokio::spawn({
            let second = second.clone();
            let sites = second.local_publisher();
            async move {
                second
                    .replicate_io(first_pid, second_writer, second_reader, sites)
                    .await
            }
        });

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut rows = second_connection
                    .query("SELECT value FROM items WHERE id = 1", ())
                    .await
                    .expect("query replicated row");
                if let Some(row) = rows.next().await.expect("read replicated row") {
                    assert_eq!(
                        row.get::<String>(0).expect("read replicated value"),
                        "replicated"
                    );
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("replication should complete");
        first_task.abort();
        second_task.abort();
    }
}
