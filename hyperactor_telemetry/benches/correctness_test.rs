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
//!
//! Usage:
//!   buck2 run //monarch/hyperactor_telemetry:correctness_test

#![allow(clippy::disallowed_methods)] // don't want to take a dependency on `hyperactor`` just for `hyperactor::clock::Clock`

use std::path::PathBuf;

use anyhow::Result;
use hyperactor_telemetry::*;

struct TestResults {
    glog_path: Option<PathBuf>,
}

struct CorrectnessTestHarness {}

impl CorrectnessTestHarness {
    fn run<F>(&self, workload: F) -> Result<TestResults>
    where
        F: Fn(),
    {
        initialize_logging_with_log_prefix(
            DefaultTelemetryClock {},
            Some("TEST_LOG_PREFIX".to_string()),
        );

        workload();

        std::thread::sleep(std::time::Duration::from_millis(300));

        Ok(TestResults {
            glog_path: Self::find_glog_path(),
        })
    }

    fn find_glog_path() -> Option<PathBuf> {
        let username = whoami::username();
        let suffix = std::env::var("MONARCH_LOG_SUFFIX")
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
            .env("MONARCH_LOG_SUFFIX", &old_log_suffix)
            .status()?;

        if !old_status.success() {
            println!("\n✗ OLD implementation FAILED");
            all_passed = false;
            test_passed = false;
        }

        println!("\n[Running UNIFIED implementation...]");
        let unified_log_suffix = format!("{}_unified", test_name);
        let unified_status = std::process::Command::new(&args[0])
            .arg(test_name)
            .arg("--unified")
            .env("TEST_LOG_PREFIX", "test")
            .env("MONARCH_LOG_SUFFIX", &unified_log_suffix)
            .status()?;

        if !unified_status.success() {
            println!("\n✗ UNIFIED implementation FAILED");
            all_passed = false;
            test_passed = false;
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

        if test_passed {
            println!("\n✓ Test PASSED: {}", test_name_to_display(test_name));
        } else {
            println!("\n✗ Test FAILED: {}", test_name_to_display(test_name));
        }

        // Clean up test files
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
            harness.run(workload)?
        }
        "--unified" => {
            println!("Running with UNIFIED implementation...");
            // Set USE_UNIFIED_LAYER to use unified implementation
            // SAFETY: Setting before any telemetry initialization
            unsafe {
                std::env::set_var("USE_UNIFIED_LAYER", "1");
            }
            harness.run(workload)?
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
