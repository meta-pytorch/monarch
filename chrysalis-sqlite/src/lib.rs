/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Experimental `cr-sqlite` replication primitives.

mod protocol;
mod replica;
mod topology;

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::Path;

use chrysalis_core::Pid;
use libsql::Connection;
use libsql::Transaction;
use libsql::Value;
pub use protocol::ProtocolError;
pub use replica::Replica;
pub use replica::SQLITE_LINK_PROTOCOL;
pub use replica::SitePublisher;
use sha2::Digest as _;
use sha2::Sha256;
use thiserror::Error;
pub use topology::ReplicationTopology;

/// The fixed width of a `cr-sqlite` site identifier.
pub const SITE_ID_LEN: usize = 16;

/// One immutable CRR table definition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableSchema {
    pub table: String,
    pub create_sql: String,
    pub hash: [u8; 32],
}

impl TableSchema {
    /// Constructs a table definition and its deterministic content hash.
    pub fn new(table: String, create_sql: String) -> Self {
        let hash = schema_hash(&table, &create_sql);
        Self {
            table,
            create_sql,
            hash,
        }
    }

    fn from_wire(table: String, create_sql: String, hash: [u8; 32]) -> Result<Self, Error> {
        if table.is_empty() || create_sql.is_empty() || schema_hash(&table, &create_sql) != hash {
            return Err(Error::InvalidSchema);
        }
        Ok(Self {
            table,
            create_sql,
            hash,
        })
    }
}

/// One owned row from the `crsql_changes` virtual table.
#[derive(Clone, Debug, PartialEq)]
pub struct Change {
    pub table: String,
    pub pk: Vec<u8>,
    pub cid: String,
    pub value: Value,
    pub col_version: i64,
    pub db_version: i64,
    pub site_id: Vec<u8>,
    pub cl: i64,
    pub seq: i64,
}

/// A destination-specific replication payload.
#[derive(Clone, Debug, PartialEq)]
pub struct ChangeBatch {
    pub changes: Vec<Change>,
    pub advances: VersionFrontier,
}

pub(crate) struct PeerChangeBatch {
    pub batch: ChangeBatch,
    pub complete: bool,
}

/// The greatest source-local database version acknowledged for each origin site.
pub type VersionFrontier = BTreeMap<Vec<u8>, i64>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OriginEntry {
    pub position: i64,
    pub site_id: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EligibilityMode {
    Allow = 0,
    Block = 1,
}

impl EligibilityMode {
    fn from_i64(value: i64) -> Result<Self, Error> {
        match value {
            0 => Ok(Self::Allow),
            1 => Ok(Self::Block),
            _ => Err(Error::InvalidMetadata),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PeerFrontierState {
    pub origin_position: i64,
    pub mode: EligibilityMode,
}

/// A versioned, deterministic set of site IDs reachable through one side of a link.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SiteSet {
    generation: u64,
    site_ids: Vec<Vec<u8>>,
}

/// The origin sites owned by one side of a replication link.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SiteScope {
    /// A finite, versioned set of origin sites.
    Explicit(SiteSet),

    /// Every origin site except those explicitly advertised by the peer.
    ComplementOfPeer,
}

impl SiteScope {
    /// Constructs an explicit scope.
    pub fn explicit(generation: u64, site_ids: Vec<Vec<u8>>) -> Result<Self, SiteSetError> {
        Ok(Self::Explicit(SiteSet::try_new(generation, site_ids)?))
    }

    /// Returns the explicit site set, if this scope is finite.
    pub const fn as_explicit(&self) -> Option<&SiteSet> {
        match self {
            Self::Explicit(sites) => Some(sites),
            Self::ComplementOfPeer => None,
        }
    }
}

impl SiteSet {
    /// Constructs a site set, sorting and deduplicating its IDs.
    pub fn try_new(generation: u64, mut site_ids: Vec<Vec<u8>>) -> Result<Self, SiteSetError> {
        if site_ids.iter().any(|site_id| site_id.len() != SITE_ID_LEN) {
            return Err(SiteSetError::InvalidSiteId);
        }
        site_ids.sort_unstable();
        site_ids.dedup();
        Ok(Self {
            generation,
            site_ids,
        })
    }

    /// Returns this set's monotonic generation.
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the sorted site IDs.
    pub fn site_ids(&self) -> &[Vec<u8>] {
        &self.site_ids
    }

    fn contains(&self, site_id: &[u8]) -> bool {
        self.site_ids
            .binary_search_by(|candidate| candidate.as_slice().cmp(site_id))
            .is_ok()
    }
}

/// An invalid replication site set.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum SiteSetError {
    /// A `cr-sqlite` site ID was not exactly 16 bytes.
    #[error("site ID must be exactly 16 bytes")]
    InvalidSiteId,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("query returned no scalar row")]
    MissingScalar,

    #[error(transparent)]
    Sqlite(#[from] libsql::Error),

    #[error(transparent)]
    Protocol(#[from] ProtocolError),

    #[error(transparent)]
    SiteSet(#[from] SiteSetError),

    #[error("database version is negative")]
    NegativeDbVersion,

    #[error("replication session for peer is already active")]
    DuplicatePeer,

    #[error("local site set does not include this replica")]
    OwnSiteMissing,

    #[error("peer site set does not include its primary site ID")]
    PeerSiteMissing,

    #[error("complement scope requires an explicit peer scope")]
    UnanchoredComplement,

    #[error("site scope representation changed during a session")]
    SiteScopeChanged,

    #[error("complement scope cannot publish an explicit site set")]
    ComplementIsDerived,

    #[error("peer sent a message before hello")]
    MissingHello,

    #[error("peer sent a second hello")]
    UnexpectedHello,

    #[error("peer site-set generation regressed")]
    SiteGenerationRegressed,

    #[error("peer replayed a site-set generation with different contents")]
    ConflictingSiteGeneration,

    #[error("peer acknowledged an unexpected database version")]
    UnexpectedAck,

    #[error("peer sent an invalid change batch")]
    InvalidBatch,

    #[error("one database version exceeds the replication batch limit")]
    ChangeVersionTooLarge,

    #[error("peer sent an unexpected batch message")]
    UnexpectedBatch,

    #[error("replication state is closed")]
    Closed,

    #[error("replication metadata is invalid")]
    InvalidMetadata,

    #[error("invalid replicated table schema")]
    InvalidSchema,

    #[error("conflicting schema for table {0}")]
    SchemaConflict(String),

    #[error("schema changed after propagation for table {0}")]
    SchemaChanged(String),

    #[error("change batch references unknown schema for table {0}")]
    MissingSchema(String),
}

/// Loads a trusted `cr-sqlite` shared library into `connection`.
pub fn load_crsqlite(connection: &Connection, path: &Path) -> Result<(), Error> {
    connection.load_extension_enable()?;
    let load_result = connection.load_extension(path, Some("sqlite3_crsqlite_init"));
    let disable_result = connection.load_extension_disable();
    load_result?;
    disable_result?;
    Ok(())
}

/// Returns this replica's stable `cr-sqlite` site identifier.
pub async fn site_id(connection: &Connection) -> Result<Vec<u8>, Error> {
    let mut rows = connection.query("SELECT crsql_site_id()", ()).await?;
    rows.next()
        .await?
        .ok_or(Error::MissingScalar)?
        .get(0)
        .map_err(Error::from)
}

/// Returns the current local CRR database version.
pub async fn db_version(connection: &Connection) -> Result<i64, Error> {
    let mut rows = connection.query("SELECT crsql_db_version()", ()).await?;
    let version = rows.next().await?.ok_or(Error::MissingScalar)?.get(0)?;
    if version < 0 {
        return Err(Error::NegativeDbVersion);
    }
    Ok(version)
}

/// Returns all locally initialized CRR table definitions.
pub async fn table_schemas(
    connection: &Connection,
) -> Result<BTreeMap<String, TableSchema>, Error> {
    let mut rows = connection
        .query(
            "SELECT tables.name, tables.sql \
             FROM sqlite_schema AS tables \
             WHERE tables.type = 'table' \
               AND tables.sql IS NOT NULL \
               AND EXISTS (\
                   SELECT 1 FROM sqlite_schema AS triggers \
                   WHERE triggers.type = 'trigger' \
                     AND triggers.name = tables.name || '__crsql_itrig'\
               ) \
             ORDER BY tables.name",
            (),
        )
        .await?;
    let mut schemas = BTreeMap::new();
    while let Some(row) = rows.next().await? {
        let table: String = row.get(0)?;
        let create_sql: String = row.get(1)?;
        schemas.insert(table.clone(), TableSchema::new(table, create_sql));
    }
    Ok(schemas)
}

pub(crate) async fn apply_table_schema(
    connection: &Connection,
    schema: &TableSchema,
) -> Result<(), Error> {
    let mut rows = connection
        .query(
            "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?1",
            vec![Value::Text(schema.table.clone())],
        )
        .await?;
    if let Some(row) = rows.next().await? {
        let local_sql: String = row.get(0)?;
        if schema_hash(&schema.table, &local_sql) != schema.hash {
            return Err(Error::SchemaConflict(schema.table.clone()));
        }
    } else {
        connection.execute(&schema.create_sql, ()).await?;
    }

    let mut rows = connection
        .query(
            "SELECT crsql_as_crr(?1)",
            vec![Value::Text(schema.table.clone())],
        )
        .await?;
    rows.next().await?.ok_or(Error::MissingScalar)?;
    let local = table_schemas(connection).await?;
    if local.get(&schema.table) != Some(schema) {
        return Err(Error::SchemaConflict(schema.table.clone()));
    }
    Ok(())
}

fn schema_hash(table: &str, create_sql: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update((table.len() as u64).to_be_bytes());
    hasher.update(table.as_bytes());
    hasher.update((create_sql.len() as u64).to_be_bytes());
    hasher.update(create_sql.as_bytes());
    hasher.finalize().into()
}

/// Reads changes not yet acknowledged by one destination replica.
pub async fn changes_for(
    source: &Connection,
    destination_site_id: &[u8],
    frontier: &VersionFrontier,
) -> Result<ChangeBatch, Error> {
    let sites = SiteSet::try_new(0, vec![destination_site_id.to_vec()])?;
    changes_excluding(source, &sites, frontier).await
}

/// Reads a change batch while excluding every site reachable through the peer.
pub async fn changes_excluding(
    source: &Connection,
    excluded_sites: &SiteSet,
    frontier: &VersionFrontier,
) -> Result<ChangeBatch, Error> {
    let origins = origin_site_ids(source).await?;
    changes_filtered(source, origins, frontier, |site_id| {
        !excluded_sites.contains(site_id)
    })
    .await
}

/// Reads changes selected by the peer's abstract origin scope.
pub async fn changes_for_scope(
    source: &Connection,
    peer_scope: &SiteScope,
    local_scope: &SiteScope,
    frontier: &VersionFrontier,
) -> Result<ChangeBatch, Error> {
    let mut origins = origin_site_ids(source).await?;
    for scope in [peer_scope, local_scope] {
        if let SiteScope::Explicit(sites) = scope {
            origins.extend(sites.site_ids().iter().cloned());
        }
    }
    match peer_scope {
        SiteScope::Explicit(peer_sites) => {
            changes_filtered(source, origins, frontier, |site_id| {
                !peer_sites.contains(site_id)
            })
            .await
        }
        SiteScope::ComplementOfPeer => {
            let SiteScope::Explicit(local_sites) = local_scope else {
                return Err(Error::UnanchoredComplement);
            };
            changes_filtered(source, origins, frontier, |site_id| {
                local_sites.contains(site_id)
            })
            .await
        }
    }
}

async fn changes_filtered<F>(
    source: &Connection,
    origins: BTreeSet<Vec<u8>>,
    frontier: &VersionFrontier,
    include: F,
) -> Result<ChangeBatch, Error>
where
    F: Fn(&[u8]) -> bool,
{
    if frontier
        .iter()
        .any(|(site_id, version)| site_id.len() != SITE_ID_LEN || *version < 0)
    {
        return Err(Error::InvalidBatch);
    }

    let mut changes = Vec::new();
    let mut advances = VersionFrontier::new();
    for origin in origins {
        if !include(&origin) {
            continue;
        }
        let after = frontier.get(&origin).copied().unwrap_or(0);
        let mut rows = source
            .query(
                "SELECT [table], [pk], [cid], [val], [col_version], [db_version], \
                        [site_id], [cl], [seq] \
                 FROM crsql_changes \
                 WHERE site_id IS ?1 AND db_version > ?2 \
                 ORDER BY db_version, seq",
                vec![Value::Blob(origin), Value::Integer(after)],
            )
            .await?;
        while let Some(row) = rows.next().await? {
            let change = change_from_row(&row)?;
            advances
                .entry(change.site_id.clone())
                .and_modify(|version| *version = (*version).max(change.db_version))
                .or_insert(change.db_version);
            changes.push(change);
        }
    }

    Ok(ChangeBatch { changes, advances })
}

pub(crate) async fn origin_site_ids(source: &Connection) -> Result<BTreeSet<Vec<u8>>, Error> {
    let mut rows = source
        .query(
            &format!("SELECT site_id FROM {ORIGIN_TABLE} ORDER BY position"),
            (),
        )
        .await?;
    let mut origins = BTreeSet::new();
    while let Some(row) = rows.next().await? {
        let site_id: Vec<u8> = row.get(0)?;
        if site_id.len() != SITE_ID_LEN {
            return Err(Error::InvalidBatch);
        }
        origins.insert(site_id);
    }
    Ok(origins)
}

fn change_from_row(row: &libsql::Row) -> Result<Change, Error> {
    let change = Change {
        table: row.get(0)?,
        pk: row.get(1)?,
        cid: row.get(2)?,
        value: row.get_value(3)?,
        col_version: row.get(4)?,
        db_version: row.get(5)?,
        site_id: row.get(6)?,
        cl: row.get(7)?,
        seq: row.get(8)?,
    };
    if change.site_id.len() != SITE_ID_LEN || change.db_version < 0 {
        return Err(Error::InvalidBatch);
    }
    Ok(change)
}

const ORIGIN_TABLE: &str = "__chrysalis_crr_origins_v1";
const PEER_STATE_TABLE: &str = "__chrysalis_crr_peer_state_v1";
const FRONTIER_TABLE: &str = "__chrysalis_crr_frontiers_v4";
const PEER_CHANGE_BATCH_MAX_ROWS: usize = 4_096;
const PEER_CHANGE_BATCH_MAX_BYTES: usize = 4 * 1024 * 1024;

pub(crate) async fn initialize_frontiers(connection: &Connection) -> Result<(), Error> {
    connection
        .execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS {ORIGIN_TABLE} (\
                position INTEGER PRIMARY KEY, \
                site_id BLOB NOT NULL UNIQUE\
            ); \
            CREATE TABLE IF NOT EXISTS {PEER_STATE_TABLE} (\
                peer_pid BLOB NOT NULL, \
                peer_site_id BLOB NOT NULL, \
                origin_position INTEGER NOT NULL CHECK (origin_position >= 0), \
                eligibility_mode INTEGER NOT NULL CHECK (eligibility_mode IN (0, 1)), \
                PRIMARY KEY (peer_pid, peer_site_id)\
            ) WITHOUT ROWID; \
            CREATE TABLE IF NOT EXISTS {FRONTIER_TABLE} (\
                peer_pid BLOB NOT NULL, \
                peer_site_id BLOB NOT NULL, \
                origin_site_id BLOB NOT NULL, \
                synced_at INTEGER NOT NULL CHECK (synced_at >= 0), \
                eligible INTEGER NOT NULL CHECK (eligible IN (0, 1)), \
                PRIMARY KEY (peer_pid, peer_site_id, origin_site_id)\
            ) WITHOUT ROWID"
        ))
        .await?;
    Ok(())
}

pub(crate) async fn bootstrap_origins(
    connection: &Connection,
    local_site_id: &[u8],
) -> Result<(), Error> {
    let mut site_ids = BTreeSet::from([local_site_id.to_vec()]);
    let mut rows = connection
        .query("SELECT site_id FROM crsql_site_id", ())
        .await?;
    while let Some(row) = rows.next().await? {
        let site_id: Vec<u8> = row.get(0)?;
        if site_id.len() != SITE_ID_LEN {
            return Err(Error::InvalidMetadata);
        }
        site_ids.insert(site_id);
    }
    register_origin_ids(connection, &site_ids.into_iter().collect::<Vec<_>>()).await
}

pub(crate) async fn register_origin_ids(
    connection: &Connection,
    site_ids: &[Vec<u8>],
) -> Result<(), Error> {
    let transaction = connection.transaction().await?;
    register_origin_ids_in_transaction(&transaction, site_ids).await?;
    transaction.commit().await?;
    Ok(())
}

async fn register_origin_ids_in_transaction(
    transaction: &Transaction,
    site_ids: &[Vec<u8>],
) -> Result<(), Error> {
    if site_ids.iter().any(|site_id| site_id.len() != SITE_ID_LEN) {
        return Err(Error::InvalidMetadata);
    }
    for site_id in site_ids {
        transaction
            .execute(
                &format!("INSERT OR IGNORE INTO {ORIGIN_TABLE} (site_id) VALUES (?1)"),
                vec![Value::Blob(site_id.clone())],
            )
            .await?;
    }
    Ok(())
}

pub(crate) async fn origin_entries_after(
    connection: &Connection,
    position: i64,
) -> Result<Vec<OriginEntry>, Error> {
    if position < 0 {
        return Err(Error::InvalidMetadata);
    }
    let mut rows = connection
        .query(
            &format!(
                "SELECT position, site_id FROM {ORIGIN_TABLE} \
                 WHERE position > ?1 ORDER BY position"
            ),
            vec![Value::Integer(position)],
        )
        .await?;
    let mut entries = Vec::new();
    while let Some(row) = rows.next().await? {
        let entry = OriginEntry {
            position: row.get(0)?,
            site_id: row.get(1)?,
        };
        if entry.position <= position || entry.site_id.len() != SITE_ID_LEN {
            return Err(Error::InvalidMetadata);
        }
        entries.push(entry);
    }
    Ok(entries)
}

pub(crate) async fn load_peer_frontier_state(
    connection: &Connection,
    peer: Pid,
    peer_site_id: &[u8],
) -> Result<Option<PeerFrontierState>, Error> {
    let mut rows = connection
        .query(
            &format!(
                "SELECT origin_position, eligibility_mode FROM {PEER_STATE_TABLE} \
                 WHERE peer_pid = ?1 AND peer_site_id = ?2"
            ),
            vec![
                Value::Blob(peer.as_bytes().to_vec()),
                Value::Blob(peer_site_id.to_vec()),
            ],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    let origin_position = row.get(0)?;
    if origin_position < 0 {
        return Err(Error::InvalidMetadata);
    }
    Ok(Some(PeerFrontierState {
        origin_position,
        mode: EligibilityMode::from_i64(row.get(1)?)?,
    }))
}

pub(crate) async fn load_explicit_frontier_sites(
    connection: &Connection,
    peer: Pid,
    peer_site_id: &[u8],
    mode: EligibilityMode,
) -> Result<BTreeSet<Vec<u8>>, Error> {
    let eligible = match mode {
        EligibilityMode::Allow => 1,
        EligibilityMode::Block => 0,
    };
    let mut rows = connection
        .query(
            &format!(
                "SELECT origin_site_id FROM {FRONTIER_TABLE} \
                 WHERE peer_pid = ?1 AND peer_site_id = ?2 AND eligible = ?3"
            ),
            vec![
                Value::Blob(peer.as_bytes().to_vec()),
                Value::Blob(peer_site_id.to_vec()),
                Value::Integer(eligible),
            ],
        )
        .await?;
    let mut sites = BTreeSet::new();
    while let Some(row) = rows.next().await? {
        let site_id: Vec<u8> = row.get(0)?;
        if site_id.len() != SITE_ID_LEN {
            return Err(Error::InvalidMetadata);
        }
        sites.insert(site_id);
    }
    Ok(sites)
}

pub(crate) async fn reconcile_frontier(
    connection: &Connection,
    peer: Pid,
    peer_site_id: &[u8],
    origin_position: i64,
    mode: EligibilityMode,
    eligibility: &BTreeMap<Vec<u8>, bool>,
) -> Result<(), Error> {
    if origin_position < 0
        || eligibility
            .keys()
            .any(|site_id| site_id.len() != SITE_ID_LEN)
    {
        return Err(Error::InvalidMetadata);
    }
    let transaction = connection.transaction().await?;
    for (origin_site_id, eligible) in eligibility {
        transaction
            .execute(
                &format!(
                    "INSERT INTO {FRONTIER_TABLE} \
                        (peer_pid, peer_site_id, origin_site_id, synced_at, eligible) \
                     VALUES (?1, ?2, ?3, 0, ?4) \
                     ON CONFLICT (peer_pid, peer_site_id, origin_site_id) DO UPDATE \
                     SET eligible = excluded.eligible"
                ),
                vec![
                    Value::Blob(peer.as_bytes().to_vec()),
                    Value::Blob(peer_site_id.to_vec()),
                    Value::Blob(origin_site_id.clone()),
                    Value::Integer(i64::from(*eligible)),
                ],
            )
            .await?;
    }
    transaction
        .execute(
            &format!(
                "INSERT INTO {PEER_STATE_TABLE} \
                    (peer_pid, peer_site_id, origin_position, eligibility_mode) \
                 VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT (peer_pid, peer_site_id) DO UPDATE SET \
                    origin_position = excluded.origin_position, \
                    eligibility_mode = excluded.eligibility_mode"
            ),
            vec![
                Value::Blob(peer.as_bytes().to_vec()),
                Value::Blob(peer_site_id.to_vec()),
                Value::Integer(origin_position),
                Value::Integer(mode as i64),
            ],
        )
        .await?;
    transaction.commit().await?;
    Ok(())
}

pub(crate) async fn changes_for_peer(
    source: &Connection,
    peer: Pid,
    peer_site_id: &[u8],
) -> Result<PeerChangeBatch, Error> {
    let query_rows = PEER_CHANGE_BATCH_MAX_ROWS
        .checked_add(1)
        .expect("change batch query limit should not overflow");
    let mut rows = source
        .query(
            &format!(
                "SELECT changes.[table], changes.[pk], changes.[cid], changes.[val], \
                        changes.[col_version], changes.[db_version], changes.[site_id], \
                        changes.[cl], changes.[seq] \
                 FROM {FRONTIER_TABLE} AS frontier \
                 JOIN crsql_changes AS changes \
                   ON changes.site_id IS frontier.origin_site_id \
                  AND changes.db_version > frontier.synced_at \
                 WHERE frontier.peer_pid = ?1 \
                   AND frontier.peer_site_id = ?2 \
                   AND frontier.eligible = 1 \
                 ORDER BY changes.site_id, changes.db_version, changes.seq \
                 LIMIT ?3"
            ),
            vec![
                Value::Blob(peer.as_bytes().to_vec()),
                Value::Blob(peer_site_id.to_vec()),
                Value::Integer(query_rows as i64),
            ],
        )
        .await?;
    let mut changes = Vec::new();
    let mut advances = VersionFrontier::new();
    let mut batch_bytes = 0;
    let mut group = Vec::new();
    let mut group_bytes = 0;
    let mut group_key = None;
    let mut row_count = 0;
    while let Some(row) = rows.next().await? {
        let change = change_from_row(&row)?;
        row_count += 1;
        let key = (change.site_id.clone(), change.db_version);
        if group_key.as_ref().is_some_and(|current| current != &key) {
            if !append_change_group(
                &mut changes,
                &mut advances,
                &mut batch_bytes,
                &mut group,
                group_bytes,
            )? {
                return Ok(PeerChangeBatch {
                    batch: ChangeBatch { changes, advances },
                    complete: false,
                });
            }
            group_bytes = 0;
        }
        group_key = Some(key);
        group_bytes = group_bytes
            .checked_add(protocol::encoded_change_len(&change)?)
            .ok_or(Error::ChangeVersionTooLarge)?;
        if group_bytes > PEER_CHANGE_BATCH_MAX_BYTES {
            return Err(Error::ChangeVersionTooLarge);
        }
        group.push(change);
    }
    if row_count > PEER_CHANGE_BATCH_MAX_ROWS {
        if changes.is_empty() {
            return Err(Error::ChangeVersionTooLarge);
        }
        return Ok(PeerChangeBatch {
            batch: ChangeBatch { changes, advances },
            complete: false,
        });
    }
    let complete = append_change_group(
        &mut changes,
        &mut advances,
        &mut batch_bytes,
        &mut group,
        group_bytes,
    )?;
    Ok(PeerChangeBatch {
        batch: ChangeBatch { changes, advances },
        complete,
    })
}

fn append_change_group(
    changes: &mut Vec<Change>,
    advances: &mut VersionFrontier,
    batch_bytes: &mut usize,
    group: &mut Vec<Change>,
    group_bytes: usize,
) -> Result<bool, Error> {
    if group.is_empty() {
        return Ok(true);
    }
    let next_bytes = batch_bytes
        .checked_add(group_bytes)
        .ok_or(Error::ChangeVersionTooLarge)?;
    if !changes.is_empty() && next_bytes > PEER_CHANGE_BATCH_MAX_BYTES {
        return Ok(false);
    }
    if next_bytes > PEER_CHANGE_BATCH_MAX_BYTES {
        return Err(Error::ChangeVersionTooLarge);
    }
    let site_id = group[0].site_id.clone();
    let db_version = group[0].db_version;
    assert!(
        group
            .iter()
            .all(|change| change.site_id == site_id && change.db_version == db_version),
        "change group should contain one origin database version"
    );
    advances.insert(site_id, db_version);
    changes.append(group);
    *batch_bytes = next_bytes;
    Ok(true)
}

#[cfg(test)]
pub(crate) async fn load_frontier(
    connection: &Connection,
    peer: Pid,
    peer_site_id: &[u8],
) -> Result<VersionFrontier, Error> {
    let mut rows = connection
        .query(
            &format!(
                "SELECT origin_site_id, synced_at FROM {FRONTIER_TABLE} \
                 WHERE peer_pid = ?1 AND peer_site_id = ?2"
            ),
            vec![
                Value::Blob(peer.as_bytes().to_vec()),
                Value::Blob(peer_site_id.to_vec()),
            ],
        )
        .await?;
    let mut frontier = VersionFrontier::new();
    while let Some(row) = rows.next().await? {
        let site_id: Vec<u8> = row.get(0)?;
        let version: i64 = row.get(1)?;
        if site_id.len() != SITE_ID_LEN || version < 0 {
            return Err(Error::InvalidBatch);
        }
        frontier.insert(site_id, version);
    }
    Ok(frontier)
}

pub(crate) async fn record_frontier(
    connection: &Connection,
    peer: Pid,
    peer_site_id: &[u8],
    advances: &VersionFrontier,
) -> Result<(), Error> {
    if advances
        .iter()
        .any(|(site_id, version)| site_id.len() != SITE_ID_LEN || *version < 0)
    {
        return Err(Error::InvalidBatch);
    }
    let transaction = connection.transaction().await?;
    for (origin_site_id, version) in advances {
        transaction
            .execute(
                &format!(
                    "INSERT INTO {FRONTIER_TABLE} \
                        (peer_pid, peer_site_id, origin_site_id, synced_at, eligible) \
                     VALUES (?1, ?2, ?3, ?4, 1) \
                     ON CONFLICT (peer_pid, peer_site_id, origin_site_id) DO UPDATE \
                     SET synced_at = excluded.synced_at \
                     WHERE excluded.synced_at >= synced_at"
                ),
                vec![
                    Value::Blob(peer.as_bytes().to_vec()),
                    Value::Blob(peer_site_id.to_vec()),
                    Value::Blob(origin_site_id.clone()),
                    Value::Integer(*version),
                ],
            )
            .await?;
    }
    transaction.commit().await?;
    Ok(())
}

/// Applies one batch atomically at the destination replica.
pub async fn apply_changes(destination: &Connection, batch: &ChangeBatch) -> Result<usize, Error> {
    initialize_frontiers(destination).await?;
    bootstrap_origins(destination, &site_id(destination).await?).await?;
    let transaction = destination.transaction().await?;
    let applied = apply_change_chunk(&transaction, &batch.changes).await?;
    transaction.commit().await?;
    Ok(applied)
}

pub(crate) async fn apply_change_chunk(
    destination: &Transaction,
    changes: &[Change],
) -> Result<usize, Error> {
    let origins: Vec<_> = changes
        .iter()
        .map(|change| change.site_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    register_origin_ids_in_transaction(destination, &origins).await?;
    let mut applied = 0;
    for change in changes {
        applied += destination
            .execute(
                "INSERT INTO crsql_changes \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                vec![
                    Value::Text(change.table.clone()),
                    Value::Blob(change.pk.clone()),
                    Value::Text(change.cid.clone()),
                    change.value.clone(),
                    Value::Integer(change.col_version),
                    Value::Integer(change.db_version),
                    Value::Blob(change.site_id.clone()),
                    Value::Integer(change.cl),
                    Value::Integer(change.seq),
                ],
            )
            .await? as usize;
    }
    Ok(applied)
}

#[cfg(test)]
mod tests {
    use libsql::Builder;

    use super::*;

    const PEER: Pid = Pid::from_bytes([7; 16]);

    fn change(site_id: u8, db_version: i64, bytes: usize) -> Change {
        Change {
            table: "items".into(),
            pk: vec![db_version as u8],
            cid: "value".into(),
            value: Value::Blob(vec![0x42; bytes]),
            col_version: 1,
            db_version,
            site_id: vec![site_id; SITE_ID_LEN],
            cl: 1,
            seq: 0,
        }
    }

    #[test]
    fn peer_batches_stop_before_exceeding_the_byte_budget() {
        let mut changes = Vec::new();
        let mut advances = VersionFrontier::new();
        let mut bytes = 0;
        let mut first = vec![change(1, 1, 3 * 1024 * 1024)];
        let first_bytes = protocol::encoded_change_len(&first[0]).unwrap();
        assert!(
            append_change_group(
                &mut changes,
                &mut advances,
                &mut bytes,
                &mut first,
                first_bytes,
            )
            .unwrap()
        );
        let mut second = vec![change(1, 2, 3 * 1024 * 1024)];
        let second_bytes = protocol::encoded_change_len(&second[0]).unwrap();
        assert!(
            !append_change_group(
                &mut changes,
                &mut advances,
                &mut bytes,
                &mut second,
                second_bytes,
            )
            .unwrap()
        );
        assert_eq!(changes.len(), 1);
        assert_eq!(advances, VersionFrontier::from([(vec![1; SITE_ID_LEN], 1)]));
        assert_eq!(second.len(), 1);
    }

    #[test]
    fn site_sets_are_sorted_deduplicated_and_validated() {
        let sites = SiteSet::try_new(3, vec![vec![2; 16], vec![1; 16], vec![2; 16]]).unwrap();
        assert_eq!(sites.generation(), 3);
        assert_eq!(sites.site_ids(), &[vec![1; 16], vec![2; 16]]);
        assert_eq!(
            SiteSet::try_new(0, vec![vec![1; 15]]),
            Err(SiteSetError::InvalidSiteId)
        );
    }

    #[test]
    fn site_scope_distinguishes_finite_and_complement_views() {
        let explicit = SiteScope::explicit(4, vec![vec![1; 16]]).unwrap();
        assert_eq!(explicit.as_explicit().unwrap().generation(), 4);
        assert_eq!(SiteScope::ComplementOfPeer.as_explicit(), None);
    }

    #[tokio::test]
    async fn frontiers_are_peer_incarnation_scoped_and_monotonic() {
        let database = Builder::new_local(":memory:").build().await.unwrap();
        let connection = database.connect().unwrap();
        initialize_frontiers(&connection).await.unwrap();
        let first_peer_site = vec![1; 16];
        let second_peer_site = vec![2; 16];
        let first_origin = vec![3; 16];
        let second_origin = vec![4; 16];
        let third_origin = vec![5; 16];

        register_origin_ids(&connection, &[first_origin.clone(), second_origin.clone()])
            .await
            .unwrap();
        assert_eq!(
            origin_entries_after(&connection, 0).await.unwrap(),
            [
                OriginEntry {
                    position: 1,
                    site_id: first_origin.clone(),
                },
                OriginEntry {
                    position: 2,
                    site_id: second_origin.clone(),
                },
            ]
        );

        assert_eq!(
            load_frontier(&connection, PEER, &first_peer_site)
                .await
                .unwrap(),
            VersionFrontier::new()
        );
        record_frontier(
            &connection,
            PEER,
            &first_peer_site,
            &VersionFrontier::from([(first_origin.clone(), 8), (second_origin.clone(), 3)]),
        )
        .await
        .unwrap();
        record_frontier(
            &connection,
            PEER,
            &first_peer_site,
            &VersionFrontier::from([(first_origin.clone(), 7)]),
        )
        .await
        .unwrap();
        reconcile_frontier(
            &connection,
            PEER,
            &first_peer_site,
            2,
            EligibilityMode::Allow,
            &BTreeMap::from([(first_origin.clone(), false), (second_origin.clone(), true)]),
        )
        .await
        .unwrap();
        assert_eq!(
            load_peer_frontier_state(&connection, PEER, &first_peer_site)
                .await
                .unwrap(),
            Some(PeerFrontierState {
                origin_position: 2,
                mode: EligibilityMode::Allow,
            })
        );
        register_origin_ids(&connection, &[third_origin.clone(), third_origin.clone()])
            .await
            .unwrap();
        assert_eq!(
            origin_entries_after(&connection, 2).await.unwrap(),
            [OriginEntry {
                position: 3,
                site_id: third_origin.clone(),
            }]
        );
        reconcile_frontier(
            &connection,
            PEER,
            &first_peer_site,
            3,
            EligibilityMode::Allow,
            &BTreeMap::from([(third_origin.clone(), true)]),
        )
        .await
        .unwrap();
        assert_eq!(
            load_frontier(&connection, PEER, &first_peer_site)
                .await
                .unwrap(),
            VersionFrontier::from([
                (first_origin.clone(), 8),
                (second_origin.clone(), 3),
                (third_origin.clone(), 0),
            ])
        );
        let mut rows = connection
            .query(
                &format!(
                    "SELECT origin_site_id, eligible FROM {FRONTIER_TABLE} \
                     WHERE peer_pid = ?1 AND peer_site_id = ?2 \
                     ORDER BY origin_site_id"
                ),
                vec![
                    Value::Blob(PEER.as_bytes().to_vec()),
                    Value::Blob(first_peer_site.clone()),
                ],
            )
            .await
            .unwrap();
        let mut eligibility = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            eligibility.push((row.get::<Vec<u8>>(0).unwrap(), row.get::<i64>(1).unwrap()));
        }
        assert_eq!(
            eligibility,
            [
                (first_origin, 0),
                (second_origin.clone(), 1),
                (third_origin.clone(), 1)
            ]
        );
        assert_eq!(
            load_explicit_frontier_sites(
                &connection,
                PEER,
                &first_peer_site,
                EligibilityMode::Allow,
            )
            .await
            .unwrap(),
            BTreeSet::from([second_origin, third_origin])
        );
        assert_eq!(
            load_frontier(&connection, PEER, &second_peer_site)
                .await
                .unwrap(),
            VersionFrontier::new()
        );
        assert_eq!(
            load_peer_frontier_state(&connection, PEER, &second_peer_site)
                .await
                .unwrap(),
            None
        );
    }
}
