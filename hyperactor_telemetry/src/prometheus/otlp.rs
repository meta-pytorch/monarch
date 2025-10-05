/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Modern OTLP HTTP implementation for Prometheus backend.
//!
//! This module implements the recommended approach from the Prometheus OpenTelemetry guide:
//! https://prometheus.io/docs/guides/opentelemetry/
//!
//! Key features:
//! - Uses OTLP HTTP protocol to send metrics directly to Prometheus
//! - Supports standard OpenTelemetry environment variables
//! - Configurable resource attribute promotion
//! - UTF-8 translation strategies
//! - Delta temporality support (experimental)

use std::collections::HashMap;
use std::env;

use opentelemetry::KeyValue;
use opentelemetry_otlp::MetricExporter;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::SdkMeterProvider;

/// Standard OpenTelemetry environment variables for OTLP configuration
const OTEL_EXPORTER_OTLP_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
const OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT";
const OTEL_EXPORTER_OTLP_PROTOCOL: &str = "OTEL_EXPORTER_OTLP_PROTOCOL";
const OTEL_METRIC_EXPORT_INTERVAL: &str = "OTEL_METRIC_EXPORT_INTERVAL";
const OTEL_SERVICE_NAME: &str = "OTEL_SERVICE_NAME";
const OTEL_SERVICE_INSTANCE_ID: &str = "OTEL_SERVICE_INSTANCE_ID";
const OTEL_RESOURCE_ATTRIBUTES: &str = "OTEL_RESOURCE_ATTRIBUTES";
const OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE: &str =
    "OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE";

/// Configuration for Prometheus OTLP backend
#[derive(Debug, Clone)]
pub struct PrometheusOtlpConfig {
    /// Prometheus server endpoint (default: http://localhost:9090/api/v1/otlp/v1/metrics)
    pub endpoint: String,
    /// Export protocol (default: http/protobuf)
    pub protocol: String,
    /// Metrics export interval in milliseconds (default: 15000ms)
    pub export_interval_ms: u64,
    /// Service name for resource attributes
    pub service_name: String,
    /// Service instance ID for resource attributes
    pub service_instance_id: String,
    /// Additional resource attributes
    pub resource_attributes: HashMap<String, String>,
    /// Enable delta temporality (experimental)
    pub enable_delta_temporality: bool,
}

impl Default for PrometheusOtlpConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:9090/api/v1/otlp/v1/metrics".to_string(),
            protocol: "http/protobuf".to_string(),
            export_interval_ms: 15000, // 15 seconds as recommended by Prometheus
            service_name: "monarch".to_string(),
            service_instance_id: format!("instance_{}", std::process::id()),
            resource_attributes: HashMap::new(),
            enable_delta_temporality: false,
        }
    }
}

impl PrometheusOtlpConfig {
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(endpoint) = env::var(OTEL_EXPORTER_OTLP_METRICS_ENDPOINT) {
            config.endpoint = endpoint;
        } else if let Ok(base_endpoint) = env::var(OTEL_EXPORTER_OTLP_ENDPOINT) {
            if base_endpoint.ends_with("/api/v1/otlp/v1/metrics") {
                config.endpoint = base_endpoint;
            } else {
                config.endpoint = format!(
                    "{}/api/v1/otlp/v1/metrics",
                    base_endpoint.trim_end_matches('/')
                );
            }
        } else {
            panic!(
                "Selected Prometheus but OTEL_EXPORTER_OTLP_METRICS_ENDPOINT or OTEL_EXPORTER_OTLP_ENDPOINT not set"
            );
        }

        if let Ok(protocol) = env::var(OTEL_EXPORTER_OTLP_PROTOCOL) {
            config.protocol = protocol;
        }

        if let Ok(interval_str) = env::var(OTEL_METRIC_EXPORT_INTERVAL) {
            if let Ok(interval_ms) = interval_str.parse::<u64>() {
                config.export_interval_ms = interval_ms;
            }
        }

        if let Ok(service_name) = env::var(OTEL_SERVICE_NAME) {
            config.service_name = service_name;
        }

        if let Ok(instance_id) = env::var(OTEL_SERVICE_INSTANCE_ID) {
            config.service_instance_id = instance_id;
        }

        if let Ok(attrs_str) = env::var(OTEL_RESOURCE_ATTRIBUTES) {
            config.resource_attributes = parse_resource_attributes(&attrs_str);
        }

        config.enable_delta_temporality =
            env::var(OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE)
                .map(|v| v.to_lowercase() == "delta")
                .unwrap_or(false);

        config
    }

    pub fn build_meter_provider(
        self,
    ) -> Result<SdkMeterProvider, Box<dyn std::error::Error + Send + Sync>> {
        let resource = self.build_resource();

        let exporter = MetricExporter::builder()
            .with_http()
            .with_endpoint(&self.endpoint)
            .build()?;

        let meter_provider = SdkMeterProvider::builder()
            .with_resource(resource)
            .with_periodic_exporter(exporter)
            .build();

        Ok(meter_provider)
    }

    fn build_resource(&self) -> Resource {
        let mut attributes = vec![
            KeyValue::new("service.name", self.service_name.clone()),
            KeyValue::new("service.instance.id", self.service_instance_id.clone()),
        ];

        // Add custom resource attributes
        for (key, value) in &self.resource_attributes {
            attributes.push(KeyValue::new(key.clone(), value.clone()));
        }

        // Add environment-specific attributes from base resource
        attributes.extend(
            super::base_resource()
                .iter()
                .map(|(key, value)| KeyValue::new(key.clone(), value.clone())),
        );

        Resource::builder().with_attributes(attributes).build()
    }
}

/// Parse resource attributes from OTEL_RESOURCE_ATTRIBUTES format
/// Format: "key1=value1,key2=value2,key3=value3"
fn parse_resource_attributes(attrs_str: &str) -> HashMap<String, String> {
    attrs_str
        .split(',')
        .filter_map(|pair| {
            let mut parts = pair.splitn(2, '=');
            match (parts.next(), parts.next()) {
                (Some(key), Some(value)) => {
                    Some((key.trim().to_string(), value.trim().to_string()))
                }
                _ => None,
            }
        })
        .collect()
}

pub fn prometheus_meter_provider()
-> Result<SdkMeterProvider, Box<dyn std::error::Error + Send + Sync>> {
    let config = PrometheusOtlpConfig::from_env();
    tracing::info!(
        "Initializing Prometheus OTLP backend with endpoint: {}",
        config.endpoint
    );
    config.build_meter_provider()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = PrometheusOtlpConfig::default();
        assert_eq!(
            config.endpoint,
            "http://localhost:9090/api/v1/otlp/v1/metrics"
        );
        assert_eq!(config.protocol, "http/protobuf");
        assert_eq!(config.export_interval_ms, 15000);
    }

    #[test]
    fn test_parse_resource_attributes() {
        let attrs =
            parse_resource_attributes("service.version=1.0.0,environment=prod,region=us-west");
        assert_eq!(attrs.get("service.version"), Some(&"1.0.0".to_string()));
        assert_eq!(attrs.get("environment"), Some(&"prod".to_string()));
        assert_eq!(attrs.get("region"), Some(&"us-west".to_string()));
    }

    #[test]
    fn test_config_from_env() {
        // SAFETY: Setting environment variables in tests is generally safe as long as tests are run serially.
        unsafe {
            std::env::set_var(OTEL_EXPORTER_OTLP_ENDPOINT, "http://localhost:9091");
            std::env::set_var(OTEL_SERVICE_NAME, "test-service");
            std::env::set_var(OTEL_METRIC_EXPORT_INTERVAL, "30000");
            std::env::set_var(OTEL_RESOURCE_ATTRIBUTES, "env=test,version=2.0");
        }

        let config = PrometheusOtlpConfig::from_env();
        assert_eq!(config.service_name, "test-service");
        assert_eq!(config.export_interval_ms, 30000);
        assert_eq!(
            config.resource_attributes.get("env"),
            Some(&"test".to_string())
        );
        assert_eq!(
            config.endpoint,
            "http://localhost:9091/api/v1/otlp/v1/metrics"
        );

        // SAFETY: Setting environment variables in tests is generally safe as long as tests are run serially.
        unsafe {
            std::env::remove_var(OTEL_EXPORTER_OTLP_ENDPOINT);
            std::env::remove_var(OTEL_SERVICE_NAME);
            std::env::remove_var(OTEL_METRIC_EXPORT_INTERVAL);
            std::env::remove_var(OTEL_RESOURCE_ATTRIBUTES);
        }
    }
}
