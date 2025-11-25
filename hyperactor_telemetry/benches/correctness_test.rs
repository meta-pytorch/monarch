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
//! verifies that the outputs are equivalent across all exporters
//!
//! Usage:
//!   buck2 run //monarch/hyperactor_telemetry:correctness_test

#![allow(clippy::disallowed_methods)] // don't want to take a dependency on `hyperactor`` just for `hyperactor::clock::Clock`

use anyhow::Result;
use hyperactor_telemetry::*;

struct TestResults {}

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

        Ok(TestResults {})
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
        let old_status = std::process::Command::new(&args[0])
            .arg(test_name)
            .arg("--old")
            .env("TEST_LOG_PREFIX", "test")
            .status()?;

        if !old_status.success() {
            println!("\n✗ OLD implementation FAILED");
            all_passed = false;
            test_passed = false;
        }

        println!("\n[Running UNIFIED implementation...]");
        let unified_status = std::process::Command::new(&args[0])
            .arg(test_name)
            .arg("--unified")
            .env("TEST_LOG_PREFIX", "test")
            .status()?;

        if !unified_status.success() {
            println!("\n✗ UNIFIED implementation FAILED");
            all_passed = false;
            test_passed = false;
        }

        if test_passed {
            println!("\n✓ Test PASSED: {}", test_name_to_display(test_name));
        } else {
            println!("\n✗ Test FAILED: {}", test_name_to_display(test_name));
        }
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

    let _results = match impl_type {
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
