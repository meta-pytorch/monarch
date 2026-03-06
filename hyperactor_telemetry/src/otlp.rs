/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! OTLP export for OSS observability.
//!
//! When `OTEL_EXPORTER_OTLP_ENDPOINT` is set, this module provides:
//! - A `SdkMeterProvider` that exports metrics via OTLP/HTTP+protobuf
//! - An `OtlpTraceSink` that exports traces via OTLP/HTTP+protobuf
//!
//! When the env var is unset, both functions return `None`, preserving
//! the current no-op behavior for OSS builds.

use opentelemetry_sdk::metrics::SdkMeterProvider;

use crate::config::OTEL_METRIC_EXPORT_INTERVAL;

#[allow(dead_code)]
const OTLP_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";

/// Build an OTLP-backed `SdkMeterProvider` if `OTEL_EXPORTER_OTLP_ENDPOINT` is set.
///
/// The `opentelemetry-otlp` crate automatically reads standard OTel env vars
/// (`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_HEADERS`,
/// `OTEL_EXPORTER_OTLP_TIMEOUT`, etc.), so callers only need to set those.
///
/// Returns `None` if the endpoint is not configured, leaving the global
/// meter provider as the default no-op.
#[allow(dead_code)]
pub fn otlp_meter_provider() -> Option<SdkMeterProvider> {
    if std::env::var(OTLP_ENDPOINT_ENV).is_err() {
        return None;
    }

    let exporter = match opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .build()
    {
        Ok(e) => e,
        Err(e) => {
            eprintln!("[telemetry] failed to build OTLP metric exporter: {}", e);
            return None;
        }
    };

    let interval = hyperactor_config::global::get(OTEL_METRIC_EXPORT_INTERVAL);

    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter)
        .with_interval(interval)
        .build();

    let provider = SdkMeterProvider::builder().with_reader(reader).build();

    Some(provider)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_otlp_meter_provider_returns_none_without_endpoint() {
        // Safety: test-only; no other threads read this env var concurrently.
        unsafe { std::env::remove_var(OTLP_ENDPOINT_ENV) };
        assert!(otlp_meter_provider().is_none());
    }
}
