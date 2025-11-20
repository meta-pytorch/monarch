/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Correctness test harness comparing old vs unified telemetry implementations.
//!
//! This test harness runs identical workloads through both implementations and
//! verifies that the outputs are equivalent across all exporters:
//! - Glog: Read log files and compare lines
//! - SQLite: Query database and compare rows
//! - Scuba: Mock client and compare logged samples
//!
//! Usage:
//!   buck2 run //monarch/hyperactor_telemetry:correctness_test

use std::path::PathBuf;

use anyhow::Result;
use hyperactor_telemetry::*;

struct TestResults {
    glog_path: Option<PathBuf>,
    sqlite_path: Option<PathBuf>,
    #[allow(dead_code)]
    _sqlite_tracing: Option<hyperactor_telemetry::sqlite::SqliteTracing>,
    scuba_tracing_samples: Vec<TelemetrySample>,
    scuba_executions_samples: Vec<TelemetrySample>,
}

/// Record from log_events table (timestamps excluded for comparison)
#[derive(Debug, Clone, PartialEq)]
struct LogEventRecord {
    seq: i64,
    name: Option<String>,
    level: Option<String>,
    span_id: Option<String>,
    message: Option<String>,
    actor_id: Option<String>,
}

/// Record from messages table (timestamps excluded for comparison)
#[derive(Debug, Clone, PartialEq)]
struct MessageRecord {
    seq: i64,
    span_id: Option<String>,
    src: Option<String>,
    dest: Option<String>,
    payload: Option<String>,
}

/// Record from actor_lifecycle table
#[derive(Debug, Clone, PartialEq)]
struct ActorLifecycleRecord {
    seq: i64,
    actor_id: Option<String>,
    actor: Option<String>,
    name: Option<String>,
    actor_status: Option<String>,
}

struct CorrectnessTestHarness {}

impl CorrectnessTestHarness {
    fn run<F>(&self, workload: F, unified: bool) -> Result<TestResults>
    where
        F: Fn(),
    {
        let test_handle = initialize_logging_with_log_prefix_mock_scuba(
            DefaultTelemetryClock {},
            Some("TEST_LOG_PREFIX".to_string()),
        );

        let sqlite_tracing = if unified {
            None
        } else {
            let sqlite_tracing = hyperactor_telemetry::sqlite::SqliteTracing::new()
                .expect("Failed to create SqliteTracing");
            let db_path = sqlite_tracing.db_path().expect("No db_path");
            println!("SqliteTracing created successfully, db_path: {:?}", db_path);
            println!("Database exists: {}", db_path.exists());
            Some(sqlite_tracing)
        };

        workload();

        std::thread::sleep(std::time::Duration::from_millis(300));

        let username = whoami::username();
        let possible_paths = vec![
            format!(
                "/tmp/{}/hyperactor_trace_{}.db",
                username,
                std::process::id()
            ),
            format!("/tmp/hyperactor_trace_{}.db", std::process::id()),
            format!("/tmp/traces/hyperactor_trace_{}.db", std::process::id()),
            format!("./hyperactor_trace_{}.db", std::process::id()),
        ];

        let mut sqlite_path = None;
        for path in possible_paths {
            if std::path::Path::new(&path).exists() {
                sqlite_path = Some(PathBuf::from(path));
                break;
            }
        }

        let scuba_tracing_samples = test_handle.get_tracing_samples();
        let scuba_executions_samples = test_handle.get_execution_samples();

        Ok(TestResults {
            sqlite_path,
            glog_path: Self::find_glog_path(),
            scuba_tracing_samples,
            scuba_executions_samples,
            _sqlite_tracing: sqlite_tracing,
        })
    }
    fn compare_scuba_samples(
        &self,
        old_samples: &[hyperactor_telemetry::TelemetrySample],
        unified_samples: &[hyperactor_telemetry::TelemetrySample],
        table_name: &str,
    ) -> Result<()> {
        use std::collections::BTreeMap;

        println!("\n[Comparing {} Scuba Samples]", table_name);
        println!("  Old samples: {}", old_samples.len());
        println!("  Unified samples: {}", unified_samples.len());

        if old_samples.is_empty() && unified_samples.is_empty() {
            println!("  SKIP: No samples in either implementation");
            return Ok(());
        }

        if !old_samples.is_empty() {
            let mut by_type: BTreeMap<String, usize> = BTreeMap::new();
            for sample in old_samples {
                if let Some(event_type) = sample.get_string("event_type") {
                    *by_type.entry(event_type.to_string()).or_insert(0) += 1;
                }
            }
            println!("  Old samples by event_type:");
            for (event_type, count) in by_type {
                println!("    {}: {}", event_type, count);
            }
        }

        if old_samples.len() != unified_samples.len() {
            return Err(anyhow::anyhow!(
                "Sample count mismatch: old={} unified={}",
                old_samples.len(),
                unified_samples.len()
            ));
        }

        for (i, (old, unified)) in old_samples.iter().zip(unified_samples.iter()).enumerate() {
            let old_event_type = old.get_string("event_type");
            let unified_event_type = unified.get_string("event_type");
            if old_event_type != unified_event_type {
                return Err(anyhow::anyhow!(
                    "Sample #{} event_type mismatch: old={:?} unified={:?}",
                    i,
                    old_event_type,
                    unified_event_type
                ));
            }

            let old_name = old.get_string("name");
            let unified_name = unified.get_string("name");

            let skip_name_comparison = old_name
                .map(|s| s.starts_with("event fbcode/"))
                .unwrap_or(false)
                && unified_name
                    .map(|s| s.starts_with("event fbcode/"))
                    .unwrap_or(false);

            if !skip_name_comparison && old_name != unified_name {
                return Err(anyhow::anyhow!(
                    "Sample #{} name mismatch: old={:?} unified={:?}",
                    i,
                    old_name,
                    unified_name
                ));
            }

            let old_level = old.get_string("level");
            let unified_level = unified.get_string("level");
            if old_level != unified_level {
                return Err(anyhow::anyhow!(
                    "Sample #{} level mismatch: old={:?} unified={:?}",
                    i,
                    old_level,
                    unified_level
                ));
            }

            let old_target = old.get_string("target");
            let unified_target = unified.get_string("target");
            if old_target != unified_target {
                return Err(anyhow::anyhow!(
                    "Sample #{} target mismatch: old={:?} unified={:?}",
                    i,
                    old_target,
                    unified_target
                ));
            }
        }

        println!("  ✓ All {} samples match!", old_samples.len());
        Ok(())
    }

    fn find_glog_path() -> Option<PathBuf> {
        let username = whoami::username();
        let suffix = std::env::var(hyperactor_telemetry::MONARCH_LOG_SUFFIX_ENV)
            .map(|s| format!("_{}", s))
            .unwrap_or_default();
        let possible_paths = vec![
            format!("/tmp/{}/monarch_log{}.log", username, suffix),
            format!("/tmp/monarch_log{}.log", suffix),
            format!("/logs/dedicated_log_monarch{}.log", suffix),
        ];

        for path in possible_paths {
            if std::path::Path::new(&path).exists() {
                return Some(PathBuf::from(path));
            }
        }
        None
    }

    fn query_log_events(&self, conn: &rusqlite::Connection) -> Result<Vec<LogEventRecord>> {
        let mut stmt = conn.prepare(
            "SELECT seq, name, level, span_id, message, actor_id FROM log_events ORDER BY seq",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(LogEventRecord {
                seq: row.get(0)?,
                name: row.get(1)?,
                level: row.get(2)?,
                span_id: row.get(3)?,
                message: row.get(4)?,
                actor_id: row.get(5)?,
            })
        })?;

        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| anyhow::anyhow!("Failed to query log_events: {}", e))
    }

    fn query_messages(&self, conn: &rusqlite::Connection) -> Result<Vec<MessageRecord>> {
        let mut stmt =
            conn.prepare("SELECT seq, span_id, src, dest, payload FROM messages ORDER BY seq")?;
        let rows = stmt.query_map([], |row| {
            Ok(MessageRecord {
                seq: row.get(0)?,
                span_id: row.get(1)?,
                src: row.get(2)?,
                dest: row.get(3)?,
                payload: row.get(4)?,
            })
        })?;

        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| anyhow::anyhow!("Failed to query messages: {}", e))
    }

    fn query_actor_lifecycle(
        &self,
        conn: &rusqlite::Connection,
    ) -> Result<Vec<ActorLifecycleRecord>> {
        let mut stmt = conn.prepare(
            "SELECT seq, actor_id, actor, name, actor_status FROM actor_lifecycle ORDER BY seq",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(ActorLifecycleRecord {
                seq: row.get(0)?,
                actor_id: row.get(1)?,
                actor: row.get(2)?,
                name: row.get(3)?,
                actor_status: row.get(4)?,
            })
        })?;

        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| anyhow::anyhow!("Failed to query actor_lifecycle: {}", e))
    }

    /// Normalize a glog line by removing timestamp, thread ID, file:line, and prefix for comparison.
    /// Both old and unified implementations should now use the same format:
    /// "[prefix]Lmmdd HH:MM:SS.ffffff thread_id file:line] message, fields"
    ///
    /// Normalized to: "L] message, fields" (prefix removed)
    fn normalize_glog_line(line: &str) -> String {
        // Find the level character position
        if let Some(level_pos) = line
            .chars()
            .position(|c| matches!(c, 'I' | 'D' | 'E' | 'W' | 'T'))
        {
            // Find the closing bracket that comes AFTER the level character (not the one in the prefix)
            if let Some(close_bracket) = line[level_pos..].find(']') {
                let actual_bracket_pos = level_pos + close_bracket;
                let level = &line[level_pos..=level_pos]; // e.g., "I"
                let rest = &line[actual_bracket_pos + 1..].trim_start(); // Everything after the real "]"
                // Don't include prefix - just level + content
                return format!("{}] {}", level, rest);
            }
        }

        line.to_string()
    }

    fn compare_glog_files(&self, old_file: &PathBuf, unified_file: &PathBuf) -> Result<()> {
        println!("\n[Comparing Glog Files]");
        println!("  Old: {}", old_file.display());
        println!("  Unified: {}", unified_file.display());

        let old_content = std::fs::read_to_string(old_file)?;
        let unified_content = std::fs::read_to_string(unified_file)?;

        println!("  Old lines: {}", old_content.lines().count());
        println!("  Unified lines: {}", unified_content.lines().count());

        let old_lines: Vec<String> = old_content.lines().map(Self::normalize_glog_line).collect();

        let unified_lines: Vec<String> = unified_content
            .lines()
            .map(Self::normalize_glog_line)
            .collect();

        if old_lines.len() != unified_lines.len() {
            return Err(anyhow::anyhow!(
                "Line count mismatch: old={} unified={}",
                old_lines.len(),
                unified_lines.len()
            ));
        }

        let skip_lines = 1;

        for (i, (old_line, unified_line)) in old_lines
            .iter()
            .zip(unified_lines.iter())
            .enumerate()
            .skip(skip_lines)
        {
            if old_line != unified_line {
                return Err(anyhow::anyhow!(
                    "Line #{} mismatch:\n  old:     {}\n  unified: {}",
                    i,
                    old_line,
                    unified_line
                ));
            }
        }

        println!(
            "  ✓ All {} lines match (skipped {} initialization lines)!",
            old_lines.len() - skip_lines,
            skip_lines
        );
        Ok(())
    }

    fn compare_sqlite_databases(&self, old_db: &PathBuf, unified_db: &PathBuf) -> Result<()> {
        println!("\n[Comparing SQLite Databases]");
        println!("  Old: {}", old_db.display());
        println!("  Unified: {}", unified_db.display());

        let old_conn = rusqlite::Connection::open(old_db)?;
        let unified_conn = rusqlite::Connection::open(unified_db)?;

        // Compare log_events
        let old_log_events = self.query_log_events(&old_conn)?;
        let unified_log_events = self.query_log_events(&unified_conn)?;

        println!("  Old log_events: {}", old_log_events.len());
        println!("  Unified log_events: {}", unified_log_events.len());

        if old_log_events.len() != unified_log_events.len() {
            return Err(anyhow::anyhow!(
                "log_events count mismatch: old={} unified={}",
                old_log_events.len(),
                unified_log_events.len()
            ));
        }

        for (i, (old, unified)) in old_log_events
            .iter()
            .zip(unified_log_events.iter())
            .enumerate()
        {
            if old.name != unified.name {
                return Err(anyhow::anyhow!(
                    "log_event #{} name mismatch: old={:?} unified={:?}",
                    i,
                    old.name,
                    unified.name
                ));
            }
            if old.level != unified.level {
                return Err(anyhow::anyhow!(
                    "log_event #{} level mismatch: old={:?} unified={:?}",
                    i,
                    old.level,
                    unified.level
                ));
            }
            if old.span_id != unified.span_id {
                return Err(anyhow::anyhow!(
                    "log_event #{} span_id mismatch: old={:?} unified={:?}",
                    i,
                    old.span_id,
                    unified.span_id
                ));
            }
        }

        // Compare messages
        let old_messages = self.query_messages(&old_conn)?;
        let unified_messages = self.query_messages(&unified_conn)?;

        println!("  Old messages: {}", old_messages.len());
        println!("  Unified messages: {}", unified_messages.len());

        if old_messages.len() != unified_messages.len() {
            return Err(anyhow::anyhow!(
                "messages count mismatch: old={} unified={}",
                old_messages.len(),
                unified_messages.len()
            ));
        }

        for (i, (old, unified)) in old_messages.iter().zip(unified_messages.iter()).enumerate() {
            if old.span_id != unified.span_id {
                return Err(anyhow::anyhow!(
                    "message #{} span_id mismatch: old={:?} unified={:?}",
                    i,
                    old.span_id,
                    unified.span_id
                ));
            }
            if old.src != unified.src {
                return Err(anyhow::anyhow!(
                    "message #{} src mismatch: old={:?} unified={:?}",
                    i,
                    old.src,
                    unified.src
                ));
            }
            if old.dest != unified.dest {
                return Err(anyhow::anyhow!(
                    "message #{} dest mismatch: old={:?} unified={:?}",
                    i,
                    old.dest,
                    unified.dest
                ));
            }
            if old.payload != unified.payload {
                return Err(anyhow::anyhow!(
                    "message #{} payload mismatch: old={:?} unified={:?}",
                    i,
                    old.payload,
                    unified.payload
                ));
            }
        }

        // Compare actor_lifecycle
        let old_lifecycle = self.query_actor_lifecycle(&old_conn)?;
        let unified_lifecycle = self.query_actor_lifecycle(&unified_conn)?;

        println!("  Old actor_lifecycle: {}", old_lifecycle.len());
        println!("  Unified actor_lifecycle: {}", unified_lifecycle.len());

        if old_lifecycle.len() != unified_lifecycle.len() {
            return Err(anyhow::anyhow!(
                "actor_lifecycle count mismatch: old={} unified={}",
                old_lifecycle.len(),
                unified_lifecycle.len()
            ));
        }

        for (i, (old, unified)) in old_lifecycle
            .iter()
            .zip(unified_lifecycle.iter())
            .enumerate()
        {
            if old.actor_id != unified.actor_id {
                return Err(anyhow::anyhow!(
                    "actor_lifecycle #{} actor_id mismatch: old={:?} unified={:?}",
                    i,
                    old.actor_id,
                    unified.actor_id
                ));
            }
            if old.actor != unified.actor {
                return Err(anyhow::anyhow!(
                    "actor_lifecycle #{} actor mismatch: old={:?} unified={:?}",
                    i,
                    old.actor,
                    unified.actor
                ));
            }
            if old.name != unified.name {
                return Err(anyhow::anyhow!(
                    "actor_lifecycle #{} name mismatch: old={:?} unified={:?}",
                    i,
                    old.name,
                    unified.name
                ));
            }
            if old.actor_status != unified.actor_status {
                return Err(anyhow::anyhow!(
                    "actor_lifecycle #{} actor_status mismatch: old={:?} unified={:?}",
                    i,
                    old.actor_status,
                    unified.actor_status
                ));
            }
        }

        println!("  ✓ All tables match!");
        Ok(())
    }
}

// ============================================================================
// Test Workloads
// ============================================================================

fn workload_simple_info_events() {
    for i in 0..100 {
        tracing::info!(iteration = i, "simple info event");
    }
}

fn workload_spans_with_fields() {
    for i in 0..50 {
        let _span = tracing::info_span!(
            "test_span",
            iteration = i,
            foo = 42,
            message_type = "Request"
        )
        .entered();
    }
}

fn workload_nested_spans() {
    for i in 0..20 {
        let _outer = tracing::info_span!("outer", iteration = i).entered();
        {
            let _middle = tracing::info_span!("middle", level = 2).entered();
            {
                let _inner = tracing::info_span!("inner", level = 3).entered();
                tracing::info!("inside nested span");
            }
        }
    }
}

fn workload_events_with_fields() {
    for i in 0..100 {
        tracing::info!(
            iteration = i,
            foo = 42,
            message_type = "Request",
            status = "ok",
            count = 100,
            "event with many fields"
        );
    }
}

fn workload_mixed_log_levels() {
    for _ in 0..25 {
        tracing::trace!("trace event");
        tracing::debug!(value = 1, "debug event");
        tracing::info!(value = 2, "info event");
        tracing::warn!(value = 3, "warn event");
        tracing::error!(value = 4, "error event");
    }
}

fn workload_events_in_spans() {
    for i in 0..30 {
        let _span = tracing::info_span!("outer_span", iteration = i).entered();
        tracing::info!(step = "start", "starting work");
        tracing::debug!(step = "middle", "doing work");
        tracing::info!(step = "end", "finished work");
    }
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    // This script will spawn itself into this branch
    if args.len() > 2 {
        let test_name = &args[1];
        let impl_type = &args[2];
        return run_single_test(test_name, impl_type);
    }

    println!("\n\nHyperactor Telemetry Correctness Test Suite");
    println!("Comparing OLD vs UNIFIED implementations\n");

    let tests = vec![
        "simple_info_events",
        "spans_with_fields",
        "nested_spans",
        "events_with_fields",
        "mixed_log_levels",
        "events_in_spans",
    ];

    let mut all_passed = true;

    for test_name in tests {
        println!("\n{}", "=".repeat(80));
        println!("Running test: {}", test_name_to_display(test_name));
        println!("{}", "=".repeat(80));

        let mut test_passed = true;

        println!("\n[Running OLD implementation...]");
        let old_log_suffix = format!("{}_old", test_name);
        let old_status = std::process::Command::new(&args[0])
            .arg(test_name)
            .arg("--old")
            .env("TEST_LOG_PREFIX", "test")
            .env(
                hyperactor_telemetry::MONARCH_LOG_SUFFIX_ENV,
                &old_log_suffix,
            )
            .env("ENABLE_SQLITE_TRACING", "1")
            .status()?;

        if !old_status.success() {
            println!("\n✗ OLD implementation FAILED");
            all_passed = false;
            test_passed = false;
            continue;
        }

        println!("\n[Running UNIFIED implementation...]");
        let unified_log_suffix = format!("{}_unified", test_name);
        let unified_status = std::process::Command::new(&args[0])
            .arg(test_name)
            .arg("--unified")
            .env("TEST_LOG_PREFIX", "test")
            .env(
                hyperactor_telemetry::MONARCH_LOG_SUFFIX_ENV,
                &unified_log_suffix,
            )
            .env("ENABLE_SQLITE_TRACING", "1")
            .status()?;

        if !unified_status.success() {
            println!("\n✗ UNIFIED implementation FAILED");
            all_passed = false;
            test_passed = false;
            continue;
        }

        let username = whoami::username();
        let harness = CorrectnessTestHarness {};

        // Compare glog files
        let old_log = PathBuf::from(format!("/tmp/{}/test_{}_old.log", username, test_name));
        let unified_log =
            PathBuf::from(format!("/tmp/{}/test_{}_unified.log", username, test_name));

        if !old_log.exists() || !unified_log.exists() {
            println!("\n⚠ Glog files not found, skipping comparison");
            if !old_log.exists() {
                println!("  Missing: {}", old_log.display());
            }
            if !unified_log.exists() {
                println!("  Missing: {}", unified_log.display());
            }
            all_passed = false;
            test_passed = false;
        } else {
            match harness.compare_glog_files(&old_log, &unified_log) {
                Ok(()) => {
                    println!("\n✓ Glog files match");
                }
                Err(e) => {
                    println!("\n✗ Glog comparison FAILED: {}", e);
                    all_passed = false;
                    test_passed = false;
                }
            }
        }

        // Compare SQLite databases
        let old_db = PathBuf::from(format!("/tmp/{}/test_{}_old.db", username, test_name));
        let unified_db = PathBuf::from(format!("/tmp/{}/test_{}_unified.db", username, test_name));

        // SQLite databases are now required - both implementations should create them
        if !old_db.exists() {
            println!("\n✗ OLD database not found: {}", old_db.display());
            all_passed = false;
            test_passed = false;
        } else if !unified_db.exists() {
            println!("\n✗ UNIFIED database not found: {}", unified_db.display());
            all_passed = false;
            test_passed = false;
        } else {
            match harness.compare_sqlite_databases(&old_db, &unified_db) {
                Ok(()) => {
                    println!("\n✓ SQLite databases match");
                }
                Err(e) => {
                    println!("\n✗ SQLite comparison FAILED: {}", e);
                    all_passed = false;
                    test_passed = false;
                }
            }
        }

        let old_tracing = PathBuf::from(format!(
            "/tmp/{}/test_{}_old_scuba_tracing.json",
            username, test_name
        ));
        let unified_tracing = PathBuf::from(format!(
            "/tmp/{}/test_{}_unified_scuba_tracing.json",
            username, test_name
        ));

        if !old_tracing.exists() || !unified_tracing.exists() {
            println!("\n⚠ Scuba tracing sample files not found, skipping comparison");
            if !old_tracing.exists() {
                println!("  Missing: {}", old_tracing.display());
            }
            if !unified_tracing.exists() {
                println!("  Missing: {}", unified_tracing.display());
            }
            all_passed = false;
            test_passed = false;
        } else {
            let old_samples_json = std::fs::read_to_string(&old_tracing)?;
            let unified_samples_json = std::fs::read_to_string(&unified_tracing)?;

            let old_samples: Vec<TelemetrySample> = serde_json::from_str(&old_samples_json)?;
            let unified_samples: Vec<TelemetrySample> =
                serde_json::from_str(&unified_samples_json)?;

            match harness.compare_scuba_samples(&old_samples, &unified_samples, "Tracing") {
                Ok(()) => {
                    println!("\n✓ Scuba tracing samples match");
                }
                Err(e) => {
                    println!("\n✗ Scuba tracing comparison FAILED: {}", e);
                    all_passed = false;
                    test_passed = false;
                }
            }

            let _ = std::fs::remove_file(&old_tracing);
            let _ = std::fs::remove_file(&unified_tracing);
        }

        let old_executions = PathBuf::from(format!(
            "/tmp/{}/test_{}_old_scuba_executions.json",
            username, test_name
        ));
        let unified_executions = PathBuf::from(format!(
            "/tmp/{}/test_{}_unified_scuba_executions.json",
            username, test_name
        ));

        if !old_executions.exists() || !unified_executions.exists() {
            println!("\n⚠ Scuba executions sample files not found, skipping comparison");
            if !old_executions.exists() {
                println!("  Missing: {}", old_executions.display());
            }
            if !unified_executions.exists() {
                println!("  Missing: {}", unified_executions.display());
            }
            all_passed = false;
            test_passed = false;
        } else {
            let old_samples_json = std::fs::read_to_string(&old_executions)?;
            let unified_samples_json = std::fs::read_to_string(&unified_executions)?;

            let old_samples: Vec<TelemetrySample> = serde_json::from_str(&old_samples_json)?;
            let unified_samples: Vec<TelemetrySample> =
                serde_json::from_str(&unified_samples_json)?;

            match harness.compare_scuba_samples(&old_samples, &unified_samples, "Executions") {
                Ok(()) => {
                    println!("\n✓ Scuba executions samples match");
                }
                Err(e) => {
                    println!("\n✗ Scuba executions comparison FAILED: {}", e);
                    all_passed = false;
                    test_passed = false;
                }
            }

            let _ = std::fs::remove_file(&old_executions);
            let _ = std::fs::remove_file(&unified_executions);
        }

        if test_passed {
            println!("\n✓ Test PASSED: {}", test_name_to_display(test_name));
        } else {
            println!("\n✗ Test FAILED: {}", test_name_to_display(test_name));
        }

        // Clean up test files
        let _ = std::fs::remove_file(&old_db);
        let _ = std::fs::remove_file(&unified_db);
        let _ = std::fs::remove_file(&old_log);
        let _ = std::fs::remove_file(&unified_log);
    }

    println!("\n\n{}", "=".repeat(80));
    if all_passed {
        println!("All tests completed successfully!");
    } else {
        println!("Some tests FAILED!");
        return Err(anyhow::anyhow!("Test failures detected"));
    }
    println!("{}", "=".repeat(80));

    Ok(())
}

/// Called in child process
fn run_single_test(test_name: &str, impl_type: &str) -> Result<()> {
    let impl_suffix = if impl_type == "--old" {
        "old"
    } else {
        "unified"
    };
    let log_suffix = format!("{}_{}", test_name, impl_suffix);
    let username = whoami::username();
    let possible_log_paths = vec![
        format!("/tmp/{}/monarch_log_{}.log", username, log_suffix),
        format!("/tmp/monarch_log_{}.log", log_suffix),
        format!("/logs/dedicated_log_monarch_{}.log", log_suffix),
    ];

    for path in &possible_log_paths {
        if std::path::Path::new(path).exists() {
            let _ = std::fs::remove_file(path);
            println!("Cleaned up existing log file: {}", path);
        }
    }

    let target_log_copy = format!("/tmp/{}/test_{}_{}.log", username, test_name, impl_suffix);
    if std::path::Path::new(&target_log_copy).exists() {
        let _ = std::fs::remove_file(&target_log_copy);
        println!("Cleaned up existing copy file: {}", target_log_copy);
    }

    let harness = CorrectnessTestHarness {};

    let workload: fn() = match test_name {
        "simple_info_events" => workload_simple_info_events,
        "spans_with_fields" => workload_spans_with_fields,
        "nested_spans" => workload_nested_spans,
        "events_with_fields" => workload_events_with_fields,
        "mixed_log_levels" => workload_mixed_log_levels,
        "events_in_spans" => workload_events_in_spans,
        _ => {
            return Err(anyhow::anyhow!("Unknown test: {}", test_name));
        }
    };

    let results = match impl_type {
        "--old" => {
            println!("Running with OLD implementation...");
            harness.run(workload, false)?
        }
        "--unified" => {
            println!("Running with UNIFIED implementation...");
            // Set USE_UNIFIED_LAYER to use unified implementation
            // SAFETY: Setting before any telemetry initialization
            unsafe {
                std::env::set_var("USE_UNIFIED_LAYER", "1");
            }
            harness.run(workload, true)?
        }
        _ => {
            return Err(anyhow::anyhow!(
                "Unknown implementation type: {}",
                impl_type
            ));
        }
    };

    if let Some(glog_path) = results.glog_path {
        let target_path = format!("/tmp/{}/test_{}_{}.log", username, test_name, impl_suffix);

        std::fs::copy(&glog_path, &target_path)?;
        println!("Glog file copied to: {}", target_path);
    }

    if let Some(db_path) = results.sqlite_path {
        let target_path = format!("/tmp/{}/test_{}_{}.db", username, test_name, impl_suffix);

        println!(
            "Attempting to copy database from {} to {}",
            db_path.display(),
            target_path
        );
        std::fs::copy(&db_path, &target_path).map_err(|e| {
            anyhow::anyhow!(
                "Failed to copy database from {} to {}: {}",
                db_path.display(),
                target_path,
                e
            )
        })?;

        // Also copy WAL files if they exist (SQLite WAL mode)
        let wal_path = format!("{}-wal", db_path.display());
        let shm_path = format!("{}-shm", db_path.display());
        let target_wal = format!("{}-wal", target_path);
        let target_shm = format!("{}-shm", target_path);

        if std::path::Path::new(&wal_path).exists() {
            let _ = std::fs::copy(&wal_path, &target_wal);
        }
        if std::path::Path::new(&shm_path).exists() {
            let _ = std::fs::copy(&shm_path, &target_shm);
        }

        println!("Database copied to: {}", target_path);
    } else {
        println!("Warning: No SQLite database path found");
    }

    let tracing_path = format!(
        "/tmp/{}/test_{}_{}_scuba_tracing.json",
        username, test_name, impl_suffix
    );
    let tracing_json = serde_json::to_string_pretty(&results.scuba_tracing_samples)?;
    std::fs::write(&tracing_path, tracing_json)?;
    println!("Scuba tracing samples saved to: {}", tracing_path);

    let executions_path = format!(
        "/tmp/{}/test_{}_{}_scuba_executions.json",
        username, test_name, impl_suffix
    );
    let executions_json = serde_json::to_string_pretty(&results.scuba_executions_samples)?;
    std::fs::write(&executions_path, executions_json)?;
    println!("Scuba executions samples saved to: {}", executions_path);

    Ok(())
}

fn test_name_to_display(test_name: &str) -> &str {
    match test_name {
        "simple_info_events" => "Simple info events",
        "spans_with_fields" => "Spans with fields",
        "nested_spans" => "Nested spans",
        "events_with_fields" => "Events with many fields",
        "mixed_log_levels" => "Mixed log levels",
        "events_in_spans" => "Events in spans",
        _ => test_name,
    }
}
