/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Weak;

use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::metrics::InstrumentKind;
use opentelemetry_sdk::metrics::ManualReader;
use opentelemetry_sdk::metrics::MetricResult;
use opentelemetry_sdk::metrics::Pipeline;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::metrics::Temporality;
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::data::Sum;
use opentelemetry_sdk::metrics::reader::MetricReader;

// InMemoryReader that uses a shared ManualReader and implements MetricReader
#[derive(Debug, Clone)]
pub struct InMemoryReader {
    manual_reader: Arc<ManualReader>,
}

impl InMemoryReader {
    // Create a new InMemoryReader with a specific ManualReader
    pub fn new(manual_reader: Arc<ManualReader>) -> Self {
        Self { manual_reader }
    }

    // Get all counters from the shared ManualReader
    pub fn get_all_counters(&self) -> HashMap<String, i64> {
        let mut rm = ResourceMetrics {
            resource: Resource::builder_empty().build(),
            scope_metrics: Vec::new(),
        };
        let _ = self.manual_reader.collect(&mut rm);

        // Extract counters directly from the collected metrics
        let mut counters = HashMap::new();
        for scope in &rm.scope_metrics {
            for metric in &scope.metrics {
                let data = metric.data.as_any();

                if let Some(sum_u64) = data.downcast_ref::<Sum<u64>>() {
                    for data_point in &sum_u64.data_points {
                        let metric_name = metric.name.to_string();
                        counters.insert(metric_name, data_point.value as i64);
                    }
                } else if let Some(sum_i64) = data.downcast_ref::<Sum<i64>>() {
                    for data_point in &sum_i64.data_points {
                        let metric_name = metric.name.to_string();
                        counters.insert(metric_name, data_point.value);
                    }
                }
            }
        }
        counters
    }
}

impl MetricReader for InMemoryReader {
    fn register_pipeline(&self, pipeline: Weak<Pipeline>) {
        self.manual_reader.register_pipeline(pipeline);
    }

    fn collect(&self, rm: &mut ResourceMetrics) -> MetricResult<()> {
        self.manual_reader.collect(rm)
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.manual_reader.force_flush()
    }

    fn shutdown(&self) -> OTelSdkResult {
        self.manual_reader.shutdown()
    }

    fn temporality(&self, kind: InstrumentKind) -> Temporality {
        self.manual_reader.temporality(kind)
    }
}

// RAII guard for in-memory metrics collection during testing
//
// Usage:
//     let _guard = InMemoryMetrics::new();
//
//     // Your code that emits metrics
//     my_counter.add(42, &[]);
//
//     // Check accumulated metrics
//     let counters = _guard.get_counters();
//     assert_eq!(counters.get("my_counter"), Some(&42));
pub struct InMemoryMetrics {
    in_memory_reader: InMemoryReader,
    _provider: SdkMeterProvider,
}

impl InMemoryMetrics {
    // Create a new InMemoryMetrics
    //
    // This will:
    // 1. Create a ManualReader as shared state
    // 2. Create an InMemoryReader that uses the shared ManualReader
    // 3. Create a new SdkMeterProvider with the InMemoryReader
    // 4. Set it as the global meter provider
    //
    // When the guard is dropped, the provider will be shut down.
    pub fn new() -> Self {
        // Create the manual reader with cumulative temporality - this state
        // will only exists for the lifetime of the guard
        let manual_reader = Arc::new(
            ManualReader::builder()
                .with_temporality(Temporality::Cumulative)
                .build(),
        );

        // Create the in-memory reader using the shared manual reader
        let in_memory_reader = InMemoryReader::new(Arc::clone(&manual_reader));

        // Create a new provider with the in-memory reader
        let provider = SdkMeterProvider::builder()
            .with_reader(in_memory_reader)
            .build();

        // Set as global provider
        opentelemetry::global::set_meter_provider(provider.clone());

        Self {
            in_memory_reader: InMemoryReader::new(Arc::clone(&manual_reader)),
            _provider: provider,
        }
    }

    // Get all counters accumulated since this guard was created
    pub fn get_counters(&self) -> HashMap<String, i64> {
        self.in_memory_reader.get_all_counters()
    }

    // Get the value of a specific counter by name
    pub fn get_counter(&self, name: &str) -> Option<i64> {
        self.get_counters().get(name).copied()
    }

    // Get a reference to the InMemoryReader for advanced usage
    pub fn reader(&self) -> &InMemoryReader {
        &self.in_memory_reader
    }
}

impl Drop for InMemoryMetrics {
    fn drop(&mut self) {
        // Shutdown our provider
        let _ = self._provider.shutdown();

        // Reset to a no-op provider to prevent metrics from continuing
        // to be collected by our in-memory reader after the guard is dropped
        let noop_provider = SdkMeterProvider::builder().build();
        opentelemetry::global::set_meter_provider(noop_provider);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_memory_metrics_guard() {
        // Use the RAII guard
        let guard = InMemoryMetrics::new();

        // Create and use counters
        crate::declare_static_counter!(GUARD_TEST_COUNTER, "guard_test_counter");
        GUARD_TEST_COUNTER.add(42, &[]);

        // Check that we can read the counter value
        let counters = guard.get_counters();
        assert_eq!(counters.get("guard_test_counter"), Some(&42));

        // Test the convenience method
        assert_eq!(guard.get_counter("guard_test_counter"), Some(42));
        assert_eq!(guard.get_counter("nonexistent_counter"), None);

        // Guard will be dropped here, cleaning up automatically
    }

    #[test]
    fn test_multiple_guards_sequential() {
        // Test that multiple guards work correctly when used sequentially
        {
            let guard1 = InMemoryMetrics::new();
            crate::declare_static_counter!(COUNTER_1, "counter_1");
            COUNTER_1.add(10, &[]);
            assert_eq!(guard1.get_counter("counter_1"), Some(10));
        } // guard1 dropped here

        {
            let guard2 = InMemoryMetrics::new();
            crate::declare_static_counter!(COUNTER_2, "counter_2");
            COUNTER_2.add(20, &[]);
            assert_eq!(guard2.get_counter("counter_2"), Some(20));
            // counter_1 should not be visible in guard2 since it's a new provider
            assert_eq!(guard2.get_counter("counter_1"), None);
        } // guard2 dropped here
    }

    #[test]
    fn test_counter_accumulation() {
        let guard = InMemoryMetrics::new();

        crate::declare_static_counter!(ACCUMULATING_COUNTER, "accumulating_counter");

        // Add values multiple times
        ACCUMULATING_COUNTER.add(1, &[]);
        assert_eq!(guard.get_counter("accumulating_counter"), Some(1));

        ACCUMULATING_COUNTER.add(2, &[]);
        assert_eq!(guard.get_counter("accumulating_counter"), Some(3));

        ACCUMULATING_COUNTER.add(7, &[]);
        assert_eq!(guard.get_counter("accumulating_counter"), Some(10));
    }

    #[test]
    fn test_guard_isolation() {
        // Test that each guard creates its own isolated ManualReader
        let _guard1 = InMemoryMetrics::new();
        let _guard2 = InMemoryMetrics::new();

        // Create counters in each guard's context
        {
            // Switch to guard1's provider
            let _temp_guard1 = InMemoryMetrics::new(); // This sets guard1's provider as global
            crate::declare_static_counter!(ISOLATED_COUNTER_1, "isolated_counter_1");
            ISOLATED_COUNTER_1.add(100, &[]);
        }

        {
            // Switch to guard2's provider
            let _temp_guard2 = InMemoryMetrics::new(); // This sets guard2's provider as global
            crate::declare_static_counter!(ISOLATED_COUNTER_2, "isolated_counter_2");
            ISOLATED_COUNTER_2.add(200, &[]);
        }
    }
}
