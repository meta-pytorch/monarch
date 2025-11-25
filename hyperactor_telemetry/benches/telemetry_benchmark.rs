/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Benchmark comparing old vs unified telemetry implementations.
//!
//! This benchmark simulates a realistic workload with:
//! - Nested spans (simulating actor message processing)
//! - Events at different log levels
//! - Field recording
//! - Multiple iterations
//!
//! Usage:
//!   buck2 run //monarch/hyperactor_telemetry:telemetry_benchmark -- --old
//!   buck2 run //monarch/hyperactor_telemetry:telemetry_benchmark -- --unified
//!   buck2 run //monarch/hyperactor_telemetry:telemetry_benchmark -- --compare

#![allow(clippy::disallowed_methods)] // don't want to take a dependency on `hyperactor` just for `hyperactor::clock::Clock`

use std::time::Instant;

use hyperactor_telemetry::*;

fn stage_debug_events_only(iterations: usize) {
    for i in 0..iterations {
        tracing::debug!(iteration = i, "debug event");
    }
}

fn stage_info_events_only(iterations: usize) {
    for i in 0..iterations {
        tracing::info!(iteration = i, "info event");
    }
}

fn stage_trace_events_only(iterations: usize) {
    for i in 0..iterations {
        tracing::trace!(iteration = i, "trace event");
    }
}

fn stage_error_events_only(iterations: usize) {
    for i in 0..iterations {
        tracing::error!(iteration = i, "error event");
    }
}

fn stage_simple_spans_only(iterations: usize) {
    for _ in 0..iterations {
        let _span = tracing::info_span!("simple_span").entered();
    }
}

fn stage_spans_with_fields(iterations: usize) {
    for i in 0..iterations {
        let _span = tracing::info_span!(
            "span_with_fields",
            iteration = i,
            foo = 42,
            message_type = "Request"
        )
        .entered();
    }
}

fn stage_nested_spans(iterations: usize) {
    for _ in 0..iterations {
        let _outer = tracing::info_span!("outer", level = 1).entered();
        {
            let _middle = tracing::info_span!("middle", level = 2).entered();
            {
                let _inner = tracing::info_span!("inner", level = 3).entered();
            }
        }
    }
}

fn stage_events_with_fields(iterations: usize) {
    for i in 0..iterations {
        tracing::info!(
            iteration = i,
            foo = 42,
            message_type = "Request",
            status = "ok",
            count = 100,
            "event with fields"
        );
    }
}

fn run_benchmark_stages(iterations: usize) -> Vec<(&'static str, std::time::Duration)> {
    let stages: Vec<(&'static str, fn(usize))> = vec![
        ("Debug events only", stage_debug_events_only),
        ("Info events only", stage_info_events_only),
        ("Trace events only", stage_trace_events_only),
        ("Error events only", stage_error_events_only),
        ("Simple spans only", stage_simple_spans_only),
        ("Spans with fields", stage_spans_with_fields),
        ("Nested spans (3 levels)", stage_nested_spans),
        ("Events with fields", stage_events_with_fields),
    ];

    let mut results = Vec::new();

    for (name, stage_fn) in stages {
        // Warm up
        stage_fn(10);

        // Benchmark
        let start = Instant::now();
        stage_fn(iterations);
        let elapsed = start.elapsed();

        println!(
            "  {:30} {} iterations in {:>12?} ({:>10?}/iter)",
            format!("{}:", name),
            iterations,
            elapsed,
            elapsed / iterations as u32
        );

        results.push((name, elapsed));
    }

    results
}

fn benchmark(iterations: usize) -> Vec<(&'static str, std::time::Duration)> {
    println!("{}", "=".repeat(100));

    initialize_logging_with_log_prefix(DefaultTelemetryClock {}, None);

    let results = run_benchmark_stages(iterations);

    std::thread::sleep(std::time::Duration::from_millis(500));

    results
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let iterations = 1000;

    if args.len() < 2 {
        println!("Usage: {} [--old | --unified | --compare]", args[0]);
        println!("  --old: Benchmark old implementation only");
        println!("  --unified: Benchmark unified implementation only");
        println!("  --compare: Run both in separate processes and compare");
        return;
    }

    match args[1].as_str() {
        "--old" => {
            println!("Benchmarking OLD implementation...");
            // Don't set USE_UNIFIED_LAYER - uses old implementation
            let _results = benchmark(iterations);
            println!("\n{}", "=".repeat(100));
        }
        "--unified" => {
            println!("Benchmarking UNIFIED implementation...");
            // Set USE_UNIFIED_LAYER to use unified implementation
            // SAFETY: Setting before any telemetry initialization
            unsafe {
                std::env::set_var("USE_UNIFIED_LAYER", "1");
            }
            let _results = benchmark(iterations);
            println!("\n{}", "=".repeat(100));
        }
        "--compare" => {
            println!(
                "Running comparison benchmark with {} iterations...\n",
                iterations
            );

            let old_status = std::process::Command::new(&args[0])
                .arg("--old")
                .status()
                .expect("Failed to spawn old implementation");

            if !old_status.success() {
                eprintln!("\n✗ OLD implementation benchmark FAILED");
                return;
            }

            let unified_status = std::process::Command::new(&args[0])
                .arg("--unified")
                .status()
                .expect("Failed to spawn unified implementation");

            if !unified_status.success() {
                eprintln!("\n✗ UNIFIED implementation benchmark FAILED");
            }
        }
        _ => {
            println!("Unknown option: {}", args[1]);
            println!("Use --old, --unified, or --compare");
        }
    }
}
