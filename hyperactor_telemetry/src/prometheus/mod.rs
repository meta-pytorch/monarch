/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

mod otlp;

use opentelemetry::KeyValue;
use opentelemetry_sdk::Resource;

use super::env::Env;

/// Initialize the Prometheus OTLP backend.
/// This sets up the OpenTelemetry meter provider to send metrics via OTLP HTTP to Prometheus.
pub fn initialize_prometheus_backend() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match otlp::prometheus_meter_provider() {
        Ok(provider) => {
            opentelemetry::global::set_meter_provider(provider);
            Ok(())
        }
        Err(e) => {
            tracing::error!("Failed to create OTLP meter provider: {}", e);
            tracing::error!(
                "Check that Prometheus is running with --web.enable-otlp-receiver and accessible at the configured endpoint"
            );
            Err(e)
        }
    }
}

/// Creates a no-op tracing layer for Prometheus builds since Prometheus
/// primarily handles metrics, not distributed tracing.
/// For distributed tracing with Prometheus, you would typically use Jaeger or similar.
/// Todo: Add support for distributed tracing
pub fn tracing_layer<
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
>() -> impl tracing_subscriber::Layer<S> {
    // For now, we provide a no-op layer since Prometheus is primarily for metrics
    tracing_subscriber::filter::FilterFn::new(|_| false)
}

/// Creates the base OpenTelemetry resource configuration for Prometheus,
/// configuring common attributes including:
/// - Service name as "monarch/monarch"
/// - Environment variables (job owner, oncall, user, hostname)
/// - Environment-based labels (local/test/prod)
/// - Execution ID for tracking related events
fn base_resource() -> Resource {
    let builder = opentelemetry_sdk::Resource::builder()
        .with_service_name("monarch/monarch")
        .with_attributes(
            vec![
                pairs_from_env(["MONARCH_CLIENT_TRACE_ID"]),
                if whoami::fallible::hostname().is_ok() {
                    vec![crate::key_value!(
                        "hostname",
                        whoami::fallible::hostname().unwrap()
                    )]
                } else {
                    vec![]
                },
                vec![
                    crate::key_value!("user", whoami::username()),
                    crate::key_value!("execution_id", super::env::execution_id()),
                ],
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>(),
        );

    builder.build()
}

fn pairs_from_env<'a, I: IntoIterator<Item = &'a str>>(var_names: I) -> Vec<KeyValue> {
    var_names
        .into_iter()
        .filter_map(|name| match std::env::var(name) {
            Ok(val) => Some((name, val)),
            Err(_) => None,
        })
        .map(|(key, val)| KeyValue::new(key.to_ascii_lowercase(), val))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initialize_prometheus_backend() {
        // Test that initialize_prometheus_backend behaves correctly when environment variables are set
        // SAFETY: Setting environment variables in tests is generally safe as long as tests are run serially.
        unsafe {
            std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:9090");
        }

        // This is an integration test that verifies the backend initialization
        match initialize_prometheus_backend() {
            Ok(()) => {
                // Successfully initialized - this is expected when environment is properly configured
                tracing::info!("Prometheus backend initialized successfully in test");
            }
            Err(e) => {
                // Failed to initialize - this might happen if there are other issues beyond configuration
                panic!("Failed to initialize Prometheus backend in test: {}", e)
            }
        }

        // SAFETY: Clean up environment variables after test
        unsafe {
            std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
        }
    }
}
