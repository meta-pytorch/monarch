/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! SQLite exporter with batched writes and transactions.
//! Runs on background thread to avoid blocking application threads.
//!
//! Reuses table definitions and insertion logic from the old SqliteLayer
//! to ensure 100% identical behavior.

use std::path::Path;

use anyhow::Result;
use anyhow::anyhow;
use rusqlite::Connection;
use rusqlite::functions::FunctionFlags;
use serde_json::Value as JValue;
use tracing_subscriber::filter::Targets;

use crate::sqlite;
use crate::unified::FieldValue;
use crate::unified::TraceEvent;
use crate::unified::TraceExporter;

/// SQLite exporter that batches events and writes them in transactions.
/// Reuses the exact same table schema and insertion logic from SqliteLayer.
pub struct SqliteExporter {
    conn: Connection,
    batch: Vec<TraceEvent>,
    batch_size: usize,
    target_filter: Option<Targets>,
}

impl SqliteExporter {
    /// Create a new SQLite exporter with an in-memory database.
    /// Matches the API of SqliteLayer::new()
    ///
    /// # Arguments
    /// * `batch_size` - Number of events to batch before flushing to disk
    pub fn new(batch_size: usize) -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        Self::setup_connection(conn, batch_size)
    }

    /// Create a new SQLite exporter with a file-based database.
    /// Matches the API of SqliteLayer::new_with_file()
    ///
    /// # Arguments
    /// * `db_path` - Path to SQLite database file
    /// * `batch_size` - Number of events to batch before flushing to disk
    pub fn new_with_file(db_path: impl AsRef<Path>, batch_size: usize) -> Result<Self> {
        let conn = Connection::open(db_path)?;
        Self::setup_connection(conn, batch_size)
    }

    fn setup_connection(conn: Connection, batch_size: usize) -> Result<Self> {
        for table in sqlite::ALL_TABLES.iter() {
            conn.execute(&table.create_table_stmt, [])?;
        }

        conn.create_scalar_function(
            "assert",
            2,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
            move |ctx| {
                let condition: bool = ctx.get(0)?;
                let message: String = ctx.get(1)?;

                if !condition {
                    return Err(rusqlite::Error::UserFunctionError(
                        anyhow!("assertion failed:{condition} {message}",).into(),
                    ));
                }

                Ok(condition)
            },
        )?;

        Ok(Self {
            conn,
            batch: Vec::with_capacity(batch_size),
            batch_size,
            target_filter: None,
        })
    }

    /// Configure target filtering for this exporter (same pattern as ScubaExporter).
    /// This allows excluding internal telemetry events that weren't logged in the old
    /// implementation due to timing (SQLite layer being None during initialization).
    ///
    /// # Example
    /// ```ignore
    /// // Exclude internal events
    /// exporter.with_target_filter(
    ///     Targets::new()
    ///         .with_target("execution", LevelFilter::OFF)
    ///         .with_target("opentelemetry", LevelFilter::OFF)
    ///         .with_target("hyperactor_telemetry", LevelFilter::OFF)
    ///         .with_default(LevelFilter::DEBUG)
    /// )
    /// ```
    pub fn with_target_filter(mut self, filter: Targets) -> Self {
        self.target_filter = Some(filter);
        self
    }

    fn check_target_filter(&self, target: &str, level: tracing::Level) -> bool {
        match &self.target_filter {
            None => true,
            Some(targets) => targets.would_enable(target, &level),
        }
    }

    fn flush_batch(&mut self) -> Result<()> {
        if self.batch.is_empty() {
            return Ok(());
        }

        let tx = self.conn.transaction()?;

        for event in &self.batch {
            if let TraceEvent::Event {
                name,
                target,
                level,
                fields,
                timestamp,
                parent_span,
                thread_id: _,
                module_path,
                file,
                line,
            } = event
            {
                let timestamp_us = timestamp
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_micros()
                    .to_string();

                let mut visitor = sqlite::SqlVisitor::default();

                visitor
                    .0
                    .insert("time_us".to_string(), JValue::String(timestamp_us));

                if let Some(mp) = module_path {
                    visitor
                        .0
                        .insert("module_path".to_string(), JValue::String(mp.to_string()));
                }
                if let Some(l) = line {
                    visitor
                        .0
                        .insert("line".to_string(), JValue::String(l.to_string()));
                }
                if let Some(f) = file {
                    visitor
                        .0
                        .insert("file".to_string(), JValue::String(f.to_string()));
                }

                for (key, value) in fields {
                    let json_value = match value {
                        FieldValue::Bool(b) => JValue::Bool(*b),
                        FieldValue::I64(i) => JValue::Number((*i).into()),
                        FieldValue::U64(u) => JValue::Number((*u).into()),
                        FieldValue::F64(f) => serde_json::Number::from_f64(*f)
                            .map(JValue::Number)
                            .unwrap_or(JValue::Null),
                        FieldValue::Str(s) => JValue::String(s.clone()),
                        FieldValue::Debug(d) => JValue::String(d.clone()),
                    };
                    visitor.0.insert(key.clone(), json_value);
                }

                let table = if &**target == sqlite::TableName::ACTOR_LIFECYCLE_STR {
                    sqlite::TableName::ActorLifecycle.get_table()
                } else if &**target == sqlite::TableName::MESSAGES_STR {
                    sqlite::TableName::Messages.get_table()
                } else {
                    sqlite::TableName::LogEvents.get_table()
                };

                sqlite::insert_event_fields(&tx, table, visitor)?;
            }
        }

        tx.commit()?;
        self.batch.clear();

        Ok(())
    }
}

impl TraceExporter for SqliteExporter {
    fn export(&mut self, event: &TraceEvent) -> Result<(), anyhow::Error> {
        self.batch.push(event.clone());

        if self.batch.len() >= self.batch_size {
            self.flush_batch()?;
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<(), anyhow::Error> {
        self.flush_batch()
    }

    fn name(&self) -> &str {
        "SqliteExporter"
    }

    fn should_export(&self, event: &TraceEvent) -> bool {
        match event {
            TraceEvent::SpanEnter { target, level, .. }
            | TraceEvent::Event { target, level, .. } => self.check_target_filter(target, *level),
            _ => true,
        }
    }
}
