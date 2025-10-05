/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Example showing how to use the Prometheus OTLP backend for telemetry.
//!
//! This example demonstrates the modern approach to Prometheus integration using
//! OpenTelemetry's OTLP HTTP protocol to send metrics directly to Prometheus.
//!
//! ## Setup
//!
//! 1. Start Prometheus with OTLP receiver:
//! ```bash
//! prometheus --web.enable-otlp-receiver
//! ```
//!
//! 2. Run this example:
//! ```bash
//! cd hyperactor_telemetry
//! HYPERACTOR_OTEL_BACKEND=prometheus \
//! OTEL_SERVICE_NAME=prometheus-example \
//! OTEL_RESOURCE_ATTRIBUTES=environment=demo,version=1.0.0 \
//! cargo run --example prometheus_example
//! ```
//!
//! ## Query Examples
//!
//! After running, you can query Prometheus:
//! - Rate of requests: `rate(http_requests_total[2m])`
//! - With resource attributes: `rate(http_requests_total[2m]) * on (job, instance) group_left (environment) target_info`
//! - P95 latency: `histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[2m]))`

use std::time::Duration;

use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor_telemetry::declare_static_counter;
use hyperactor_telemetry::declare_static_gauge;
use hyperactor_telemetry::declare_static_histogram;
use hyperactor_telemetry::initialize_logging_for_test;
use hyperactor_telemetry::kv_pairs;

// Declare some example metrics
declare_static_counter!(REQUESTS_TOTAL, "http_requests_total");
declare_static_histogram!(REQUEST_DURATION, "http_request_duration_seconds");
declare_static_gauge!(ACTIVE_CONNECTIONS, "active_connections");

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure environment if not already set
    // Safety: Setting environment variables at program startup before any threads are created
    unsafe {
        if std::env::var("HYPERACTOR_OTEL_BACKEND").is_err() {
            std::env::set_var("HYPERACTOR_OTEL_BACKEND", "prometheus");
        }

        // Set default OpenTelemetry configuration for OTLP mode
        if std::env::var("OTEL_SERVICE_NAME").is_err() {
            std::env::set_var("OTEL_SERVICE_NAME", "prometheus-example");
        }
        if std::env::var("OTEL_RESOURCE_ATTRIBUTES").is_err() {
            std::env::set_var("OTEL_RESOURCE_ATTRIBUTES", "environment=demo,version=1.0.0");
        }
        if std::env::var("OTEL_METRIC_EXPORT_INTERVAL").is_err() {
            std::env::set_var("OTEL_METRIC_EXPORT_INTERVAL", "5000"); // 5 seconds for demo
        }
    }

    // Initialize telemetry
    initialize_logging_for_test();

    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:9090/api/v1/otlp/v1/metrics".to_string());

    println!("🚀 Starting Prometheus OTLP example...");
    println!("📡 Sending metrics directly to Prometheus via OTLP");
    println!("🔗 Endpoint: {}", endpoint);
    println!("ℹ️  Make sure Prometheus is running with --web.enable-otlp-receiver");
    println!("🎯 Query Prometheus at: http://localhost:9090");

    println!("✅ OTLP exporter configured - metrics will be sent automatically!");

    println!("Generating some sample metrics...");

    // Generate some sample metrics using hyperactor telemetry macros
    for i in 0..100 {
        // Simulate HTTP requests
        let status = if i % 10 == 0 { "500" } else { "200" };
        let method = if i % 3 == 0 { "POST" } else { "GET" };

        REQUESTS_TOTAL.add(
            1,
            kv_pairs!(
                "method" => method,
                "status" => status,
                "endpoint" => "/api/data"
            ),
        );

        // Simulate request durations
        let duration = 0.001 + (i as f64) * 0.001; // 1ms to 100ms
        REQUEST_DURATION.record(
            duration,
            kv_pairs!(
                "method" => method,
                "endpoint" => "/api/data"
            ),
        );

        // Simulate active connections
        let connections = 10.0 + (i as f64 % 20.0);
        ACTIVE_CONNECTIONS.record(
            connections,
            kv_pairs!(
                "server" => "primary"
            ),
        );

        // Small delay to spread metrics over time
        RealClock.sleep(Duration::from_millis(50)).await;

        if i % 20 == 0 {
            println!("Generated {} metrics so far...", i + 1);
        }
    }

    println!("✨ Finished generating metrics!");
    println!("🎯 Check Prometheus for your metrics:");
    println!("   - Prometheus UI: http://localhost:9090");
    println!("   - Example query: rate(http_requests_total[2m])");
    println!(
        "   - Resource attributes query: rate(http_requests_total[2m]) * on (job, instance) group_left (environment) target_info"
    );
    println!("📡 Metrics are being sent to Prometheus via OTLP every 5 seconds");

    // Keep generating metrics to show real-time updates
    println!("🔄 Continuing to generate metrics every 10 seconds...");
    for _ in 0..5 {
        // Generate 5 more batches then exit
        RealClock.sleep(Duration::from_secs(10)).await;

        // Generate a few more metrics
        REQUESTS_TOTAL.add(
            1,
            kv_pairs!("method" => "GET", "status" => "200", "endpoint" => "/health"),
        );
        REQUEST_DURATION.record(0.001, kv_pairs!("method" => "GET", "endpoint" => "/health"));
        ACTIVE_CONNECTIONS.record(15.0, kv_pairs!("server" => "primary"));

        println!("📊 Sent batch of metrics to Prometheus");
    }

    println!("🎉 Example completed successfully!");
    Ok(())
}
