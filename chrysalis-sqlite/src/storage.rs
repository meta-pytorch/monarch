/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::BTreeMap;

use libsql::Connection;
use libsql::Transaction;
use libsql::Value;
use sha2::Digest as _;
use sha2::Sha256;

use crate::Error;
use crate::SITE_ID_LEN;
use crate::protocol::decode_values;
use crate::protocol::encode_values;

pub(crate) const CHANGE_TABLE: &str = "__chrysalis_changes_v1";
const META_TABLE: &str = "__chrysalis_replica_v1";
const RESOLVED_TABLE: &str = "__chrysalis_resolved_rows_v2";

/// A trusted local description of one replicated table.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableSchema {
    table: String,
    create_sql: String,
    columns: Vec<String>,
    primary_key: Vec<String>,
    hash: [u8; 32],
}

impl TableSchema {
    /// Constructs and validates a table descriptor.
    pub fn try_new(
        table: &str,
        create_sql: &str,
        columns: &[&str],
        primary_key: &[&str],
    ) -> Result<Self, Error> {
        if !is_identifier(table)
            || create_sql.is_empty()
            || columns.is_empty()
            || primary_key.is_empty()
            || columns.iter().any(|column| !is_identifier(column))
            || primary_key.iter().any(|column| !columns.contains(column))
            || has_duplicates(columns)
            || has_duplicates(primary_key)
        {
            return Err(Error::InvalidSchema);
        }
        let columns: Vec<String> = columns.iter().map(|column| (*column).to_owned()).collect();
        let primary_key: Vec<String> = primary_key
            .iter()
            .map(|column| (*column).to_owned())
            .collect();
        let hash = schema_hash(table, create_sql, &columns, &primary_key);
        Ok(Self {
            table: table.to_owned(),
            create_sql: create_sql.to_owned(),
            columns,
            primary_key,
            hash,
        })
    }

    /// Returns the table name.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Returns the descriptor's deterministic compatibility hash.
    pub const fn hash(&self) -> [u8; 32] {
        self.hash
    }
}

/// One row-level mutation captured inside the application's transaction.
#[derive(Clone, Debug, PartialEq)]
pub struct Mutation {
    table: String,
    key: Vec<Value>,
    row: Option<Vec<Value>>,
}

/// One durable row mutation exchanged between replicas.
#[derive(Clone, Debug, PartialEq)]
pub struct Change {
    pub table: String,
    pub key: Vec<Value>,
    pub row: Option<Vec<Value>>,
    pub db_version: i64,
    pub site_id: Vec<u8>,
    pub seq: i64,
}

pub(crate) fn schema_map(
    schemas: impl IntoIterator<Item = TableSchema>,
) -> Result<BTreeMap<String, TableSchema>, Error> {
    let mut result = BTreeMap::new();
    for schema in schemas {
        if result.insert(schema.table.clone(), schema).is_some() {
            return Err(Error::InvalidSchema);
        }
    }
    if result.is_empty() {
        return Err(Error::InvalidSchema);
    }
    Ok(result)
}

pub(crate) async fn initialize(
    connection: &Connection,
    schemas: &BTreeMap<String, TableSchema>,
) -> Result<(), Error> {
    connection
        .execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS {META_TABLE} (\
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1), \
                site_id BLOB NOT NULL CHECK (length(site_id) = {SITE_ID_LEN}), \
                db_version INTEGER NOT NULL CHECK (db_version >= 0)\
            ); \
            INSERT OR IGNORE INTO {META_TABLE} (singleton, site_id, db_version) \
                VALUES (1, randomblob({SITE_ID_LEN}), 0); \
            CREATE TABLE IF NOT EXISTS {CHANGE_TABLE} (\
                site_id BLOB NOT NULL, \
                db_version INTEGER NOT NULL CHECK (db_version > 0), \
                seq INTEGER NOT NULL CHECK (seq >= 0), \
                table_name TEXT NOT NULL, \
                key_values BLOB NOT NULL, \
                row_values BLOB, \
                PRIMARY KEY (site_id, db_version, seq)\
            ) WITHOUT ROWID; \
            CREATE TABLE IF NOT EXISTS {RESOLVED_TABLE} (\
                table_name TEXT NOT NULL, \
                key_values BLOB NOT NULL, \
                db_version INTEGER NOT NULL CHECK (db_version > 0), \
                site_id BLOB NOT NULL, \
                seq INTEGER NOT NULL CHECK (seq >= 0), \
                deleted INTEGER NOT NULL CHECK (deleted IN (0, 1)), \
                PRIMARY KEY (table_name, key_values)\
            ) WITHOUT ROWID"
        ))
        .await?;
    for schema in schemas.values() {
        connection.execute_batch(&schema.create_sql).await?;
    }
    Ok(())
}

pub(crate) async fn site_id(connection: &Connection) -> Result<Vec<u8>, Error> {
    let mut rows = connection
        .query(
            &format!("SELECT site_id FROM {META_TABLE} WHERE singleton = 1"),
            (),
        )
        .await?;
    let site_id: Vec<u8> = rows.next().await?.ok_or(Error::MissingScalar)?.get(0)?;
    if site_id.len() != SITE_ID_LEN {
        return Err(Error::InvalidMetadata);
    }
    Ok(site_id)
}

pub(crate) async fn db_version(connection: &Connection) -> Result<i64, Error> {
    let mut rows = connection
        .query(
            &format!("SELECT db_version FROM {META_TABLE} WHERE singleton = 1"),
            (),
        )
        .await?;
    let version = rows.next().await?.ok_or(Error::MissingScalar)?.get(0)?;
    if version < 0 {
        return Err(Error::NegativeDbVersion);
    }
    Ok(version)
}

pub(crate) async fn capture_upsert(
    transaction: &Transaction,
    schema: &TableSchema,
    key: Vec<Value>,
) -> Result<Mutation, Error> {
    validate_key(schema, &key)?;
    let select = format!(
        "SELECT {} FROM {} WHERE {}",
        quoted_list(&schema.columns),
        quote(&schema.table),
        predicates(&schema.primary_key, 1),
    );
    let mut rows = transaction.query(&select, key.clone()).await?;
    let row = rows.next().await?.ok_or_else(|| Error::MissingRow {
        table: schema.table.clone(),
    })?;
    let values = (0..schema.columns.len())
        .map(|index| row.get_value(index as i32).map_err(Error::from))
        .collect::<Result<Vec<_>, _>>()?;
    if rows.next().await?.is_some() {
        return Err(Error::NonUniqueKey {
            table: schema.table.clone(),
        });
    }
    Ok(Mutation {
        table: schema.table.clone(),
        key,
        row: Some(values),
    })
}

pub(crate) fn capture_delete(schema: &TableSchema, key: Vec<Value>) -> Result<Mutation, Error> {
    validate_key(schema, &key)?;
    Ok(Mutation {
        table: schema.table.clone(),
        key,
        row: None,
    })
}

pub(crate) async fn commit_local(
    transaction: Transaction,
    schemas: &BTreeMap<String, TableSchema>,
    local_site_id: &[u8],
    mutations: &[Mutation],
) -> Result<(), Error> {
    if mutations.is_empty() {
        transaction.commit().await?;
        return Ok(());
    }
    let mut rows = transaction
        .query(
            &format!(
                "UPDATE {META_TABLE} SET db_version = db_version + 1 \
                 WHERE singleton = 1 RETURNING db_version"
            ),
            (),
        )
        .await?;
    let version: i64 = rows.next().await?.ok_or(Error::MissingScalar)?.get(0)?;
    drop(rows);
    for (sequence, mutation) in mutations.iter().enumerate() {
        let schema = schemas
            .get(&mutation.table)
            .ok_or_else(|| Error::MissingSchema(mutation.table.clone()))?;
        validate_mutation(schema, mutation)?;
        let sequence = i64::try_from(sequence).map_err(|_| Error::InvalidBatch)?;
        let change = Change {
            table: mutation.table.clone(),
            key: mutation.key.clone(),
            row: mutation.row.clone(),
            db_version: version,
            site_id: local_site_id.to_vec(),
            seq: sequence,
        };
        insert_change(&transaction, &change).await?;
        record_resolved(&transaction, &change).await?;
    }
    transaction.commit().await?;
    Ok(())
}

pub(crate) async fn apply_change_chunk(
    transaction: &Transaction,
    schemas: &BTreeMap<String, TableSchema>,
    changes: &[Change],
) -> Result<usize, Error> {
    let mut applied = 0;
    for change in changes {
        let schema = schemas
            .get(&change.table)
            .ok_or_else(|| Error::MissingSchema(change.table.clone()))?;
        validate_change(schema, change)?;
        if insert_change(transaction, change).await? == 0 {
            continue;
        }
        advance_clock(transaction, change.db_version).await?;
        if supersedes_resolved(transaction, change).await? {
            apply_row(transaction, schema, change).await?;
            record_resolved(transaction, change).await?;
            applied += 1;
        }
    }
    Ok(applied)
}

pub(crate) fn change_from_row(row: &libsql::Row) -> Result<Change, Error> {
    let row_values = match row.get_value(2)? {
        Value::Null => None,
        Value::Blob(bytes) => Some(decode_values(&bytes)?),
        _ => return Err(Error::InvalidMetadata),
    };
    let change = Change {
        table: row.get(0)?,
        key: decode_values(&row.get::<Vec<u8>>(1)?)?,
        row: row_values,
        db_version: row.get(3)?,
        site_id: row.get(4)?,
        seq: row.get(5)?,
    };
    if change.site_id.len() != SITE_ID_LEN || change.db_version <= 0 || change.seq < 0 {
        return Err(Error::InvalidBatch);
    }
    Ok(change)
}

async fn insert_change(transaction: &Transaction, change: &Change) -> Result<u64, Error> {
    let row = match &change.row {
        Some(values) => Value::Blob(encode_values(values)?),
        None => Value::Null,
    };
    Ok(transaction
        .execute(
            &format!(
                "INSERT OR IGNORE INTO {CHANGE_TABLE} \
                    (site_id, db_version, seq, table_name, key_values, row_values) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
            ),
            vec![
                Value::Blob(change.site_id.clone()),
                Value::Integer(change.db_version),
                Value::Integer(change.seq),
                Value::Text(change.table.clone()),
                Value::Blob(encode_values(&change.key)?),
                row,
            ],
        )
        .await?)
}

async fn advance_clock(transaction: &Transaction, version: i64) -> Result<(), Error> {
    transaction
        .execute(
            &format!(
                "UPDATE {META_TABLE} SET db_version = MAX(db_version, ?1) WHERE singleton = 1"
            ),
            vec![Value::Integer(version)],
        )
        .await?;
    Ok(())
}

async fn supersedes_resolved(transaction: &Transaction, change: &Change) -> Result<bool, Error> {
    let key = encode_values(&change.key)?;
    let mut rows = transaction
        .query(
            &format!(
                "SELECT db_version, site_id, seq FROM {RESOLVED_TABLE} \
                 WHERE table_name = ?1 AND key_values = ?2"
            ),
            vec![Value::Text(change.table.clone()), Value::Blob(key)],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(true);
    };
    let version: i64 = row.get(0)?;
    let site_id: Vec<u8> = row.get(1)?;
    let sequence: i64 = row.get(2)?;
    Ok((change.db_version, change.site_id.as_slice(), change.seq)
        > (version, site_id.as_slice(), sequence))
}

async fn record_resolved(transaction: &Transaction, change: &Change) -> Result<(), Error> {
    transaction
        .execute(
            &format!(
                "INSERT INTO {RESOLVED_TABLE} \
                    (table_name, key_values, db_version, site_id, seq, deleted) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
                 ON CONFLICT (table_name, key_values) DO UPDATE SET \
                    db_version = excluded.db_version, \
                    site_id = excluded.site_id, \
                    seq = excluded.seq, \
                    deleted = excluded.deleted"
            ),
            vec![
                Value::Text(change.table.clone()),
                Value::Blob(encode_values(&change.key)?),
                Value::Integer(change.db_version),
                Value::Blob(change.site_id.clone()),
                Value::Integer(change.seq),
                Value::Integer(i64::from(change.row.is_none())),
            ],
        )
        .await?;
    Ok(())
}

async fn apply_row(
    transaction: &Transaction,
    schema: &TableSchema,
    change: &Change,
) -> Result<(), Error> {
    match &change.row {
        Some(row) => {
            let update_columns: Vec<_> = schema
                .columns
                .iter()
                .filter(|column| !schema.primary_key.contains(column))
                .collect();
            let conflict = if update_columns.is_empty() {
                "DO NOTHING".to_owned()
            } else {
                format!(
                    "DO UPDATE SET {}",
                    update_columns
                        .iter()
                        .map(|column| format!("{} = excluded.{}", quote(column), quote(column)))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            };
            let sql = format!(
                "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) {conflict}",
                quote(&schema.table),
                quoted_list(&schema.columns),
                placeholders(schema.columns.len(), 1),
                quoted_list(&schema.primary_key),
            );
            transaction.execute(&sql, row.clone()).await?;
        }
        None => {
            let sql = format!(
                "DELETE FROM {} WHERE {}",
                quote(&schema.table),
                predicates(&schema.primary_key, 1),
            );
            transaction.execute(&sql, change.key.clone()).await?;
        }
    }
    Ok(())
}

fn validate_change(schema: &TableSchema, change: &Change) -> Result<(), Error> {
    if change.site_id.len() != SITE_ID_LEN || change.db_version <= 0 || change.seq < 0 {
        return Err(Error::InvalidBatch);
    }
    validate_mutation(
        schema,
        &Mutation {
            table: change.table.clone(),
            key: change.key.clone(),
            row: change.row.clone(),
        },
    )
}

fn validate_mutation(schema: &TableSchema, mutation: &Mutation) -> Result<(), Error> {
    validate_key(schema, &mutation.key)?;
    if mutation
        .row
        .as_ref()
        .is_some_and(|row| row.len() != schema.columns.len())
    {
        return Err(Error::InvalidMutation {
            table: schema.table.clone(),
        });
    }
    Ok(())
}

fn validate_key(schema: &TableSchema, key: &[Value]) -> Result<(), Error> {
    if key.len() != schema.primary_key.len() || key.iter().any(|value| value == &Value::Null) {
        return Err(Error::InvalidMutation {
            table: schema.table.clone(),
        });
    }
    Ok(())
}

fn schema_hash(
    table: &str,
    create_sql: &str,
    columns: &[String],
    primary_key: &[String],
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    [
        std::slice::from_ref(&table.to_owned()),
        columns,
        primary_key,
    ]
    .into_iter()
    .flatten()
    .for_each(|part| {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part.as_bytes());
    });
    hasher.update((create_sql.len() as u64).to_be_bytes());
    hasher.update(create_sql.as_bytes());
    hasher.finalize().into()
}

fn is_identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphabetic)
}

fn has_duplicates(values: &[&str]) -> bool {
    values
        .iter()
        .enumerate()
        .any(|(index, value)| values[..index].contains(value))
}

fn quote(identifier: &str) -> String {
    format!("\"{identifier}\"")
}

fn quoted_list(identifiers: &[String]) -> String {
    identifiers
        .iter()
        .map(|identifier| quote(identifier))
        .collect::<Vec<_>>()
        .join(", ")
}

fn placeholders(count: usize, first: usize) -> String {
    (first..first + count)
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn predicates(columns: &[String], first: usize) -> String {
    columns
        .iter()
        .enumerate()
        .map(|(index, column)| format!("{} = ?{}", quote(column), first + index))
        .collect::<Vec<_>>()
        .join(" AND ")
}

#[cfg(test)]
mod tests {
    use libsql::Builder;

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

    async fn connection() -> Connection {
        Builder::new_local(":memory:")
            .build()
            .await
            .expect("create in-memory database")
            .connect()
            .expect("connect to in-memory database")
    }

    #[tokio::test]
    async fn captures_application_write_and_log_atomically() {
        let connection = connection().await;
        let schemas = schema_map([item_schema()]).expect("build schemas");
        initialize(&connection, &schemas)
            .await
            .expect("initialize replica");
        let local_site_id = site_id(&connection).await.expect("load site ID");
        let transaction = connection.transaction().await.expect("begin transaction");
        transaction
            .execute(
                "INSERT INTO items (id, value) VALUES (?1, ?2)",
                vec![Value::Integer(7), Value::Text("seven".into())],
            )
            .await
            .expect("insert item");
        let mutation = capture_upsert(
            &transaction,
            schemas.get("items").expect("item schema exists"),
            vec![Value::Integer(7)],
        )
        .await
        .expect("capture item");
        commit_local(transaction, &schemas, &local_site_id, &[mutation])
            .await
            .expect("commit mutation");

        assert_eq!(db_version(&connection).await.expect("load version"), 1);
        let mut rows = connection
            .query(
                &format!(
                    "SELECT table_name, key_values, row_values, db_version, site_id, seq \
                     FROM {CHANGE_TABLE}"
                ),
                (),
            )
            .await
            .expect("query mutation log");
        assert_eq!(
            change_from_row(
                &rows
                    .next()
                    .await
                    .expect("read mutation")
                    .expect("mutation exists")
            )
            .expect("decode mutation"),
            Change {
                table: "items".into(),
                key: vec![Value::Integer(7)],
                row: Some(vec![Value::Integer(7), Value::Text("seven".into())]),
                db_version: 1,
                site_id: local_site_id,
                seq: 0,
            }
        );
    }

    #[tokio::test]
    async fn row_resolution_is_deterministic_and_duplicate_safe() {
        let connection = connection().await;
        let schemas = schema_map([item_schema()]).expect("build schemas");
        initialize(&connection, &schemas)
            .await
            .expect("initialize replica");
        let lower = Change {
            table: "items".into(),
            key: vec![Value::Integer(1)],
            row: Some(vec![Value::Integer(1), Value::Text("lower".into())]),
            db_version: 3,
            site_id: vec![1; SITE_ID_LEN],
            seq: 0,
        };
        let higher = Change {
            row: Some(vec![Value::Integer(1), Value::Text("higher".into())]),
            site_id: vec![2; SITE_ID_LEN],
            ..lower.clone()
        };
        let transaction = connection.transaction().await.expect("begin apply");
        assert_eq!(
            apply_change_chunk(&transaction, &schemas, &[higher.clone(), lower.clone()])
                .await
                .expect("apply changes"),
            1
        );
        transaction.commit().await.expect("commit apply");

        let transaction = connection.transaction().await.expect("begin replay");
        assert_eq!(
            apply_change_chunk(&transaction, &schemas, &[higher])
                .await
                .expect("replay change"),
            0
        );
        transaction.commit().await.expect("commit replay");
        let mut rows = connection
            .query("SELECT value FROM items WHERE id = 1", ())
            .await
            .expect("query winner");
        let value: String = rows
            .next()
            .await
            .expect("read winner")
            .expect("winner exists")
            .get(0)
            .expect("read winner value");
        assert_eq!(value, "higher");
        assert_eq!(db_version(&connection).await.expect("load clock"), 3);

        let deletion = Change {
            row: None,
            db_version: 4,
            site_id: vec![1; SITE_ID_LEN],
            ..lower
        };
        let transaction = connection.transaction().await.expect("begin deletion");
        assert_eq!(
            apply_change_chunk(&transaction, &schemas, &[deletion])
                .await
                .expect("apply deletion"),
            1
        );
        transaction.commit().await.expect("commit deletion");
        let mut rows = connection
            .query("SELECT 1 FROM items WHERE id = 1", ())
            .await
            .expect("query deleted row");
        assert!(
            rows.next().await.expect("read deleted row").is_none(),
            "newer deletion should remove the row"
        );
    }

    #[test]
    fn schema_rejects_untrusted_identifiers_and_keys() {
        assert!(matches!(
            TableSchema::try_new(
                "items; DROP TABLE items",
                "CREATE TABLE items (id INTEGER PRIMARY KEY)",
                &["id"],
                &["id"],
            ),
            Err(Error::InvalidSchema)
        ));
        assert!(matches!(
            TableSchema::try_new(
                "items",
                "CREATE TABLE items (id INTEGER PRIMARY KEY)",
                &["id"],
                &["missing"],
            ),
            Err(Error::InvalidSchema)
        ));
    }
}
