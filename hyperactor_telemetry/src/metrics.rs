/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Shared OpenTelemetry metric representations and dispatch.

use opentelemetry_sdk::metrics::data::AggregatedMetrics;
use opentelemetry_sdk::metrics::data::MetricData;
use opentelemetry_sdk::metrics::data::ResourceMetrics;

pub(crate) trait MetricNumber: Copy + PartialOrd {
    fn into_f64(self) -> f64;
}

impl MetricNumber for f64 {
    fn into_f64(self) -> f64 {
        self
    }
}

impl MetricNumber for i64 {
    fn into_f64(self) -> f64 {
        self as f64
    }
}

impl MetricNumber for u64 {
    fn into_f64(self) -> f64 {
        self as f64
    }
}

#[derive(Clone, Copy)]
pub(crate) struct MetricMetadata<'a> {
    pub(crate) name: &'a str,
    pub(crate) scope_name: &'a str,
    pub(crate) unit: &'a str,
}

pub(crate) trait MetricDataVisitor {
    fn metric<T: MetricNumber>(&mut self, metadata: MetricMetadata<'_>, data: &MetricData<T>);
}

pub(crate) fn visit_metrics<V: MetricDataVisitor>(metrics: &ResourceMetrics, visitor: &mut V) {
    for scope in metrics.scope_metrics() {
        for metric in scope.metrics() {
            let metadata = MetricMetadata {
                name: metric.name(),
                scope_name: scope.scope().name(),
                unit: metric.unit(),
            };
            match metric.data() {
                AggregatedMetrics::F64(data) => visitor.metric(metadata, data),
                AggregatedMetrics::I64(data) => visitor.metric(metadata, data),
                AggregatedMetrics::U64(data) => visitor.metric(metadata, data),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use opentelemetry::KeyValue;
    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::metrics::ManualReader;
    use opentelemetry_sdk::metrics::SdkMeterProvider;
    use opentelemetry_sdk::metrics::Temporality;
    use opentelemetry_sdk::metrics::data::MetricData;
    use opentelemetry_sdk::metrics::data::ResourceMetrics;
    use opentelemetry_sdk::metrics::reader::MetricReader;

    use super::*;
    use crate::in_memory_reader::InMemoryReader;

    #[derive(Default)]
    struct RecordingVisitor(Vec<(String, String, String, &'static str, usize)>);

    impl MetricDataVisitor for RecordingVisitor {
        fn metric<T: MetricNumber>(&mut self, metadata: MetricMetadata<'_>, data: &MetricData<T>) {
            let (kind, points) = match data {
                MetricData::Gauge(gauge) => ("gauge", gauge.data_points().count()),
                MetricData::Sum(sum) => ("sum", sum.data_points().count()),
                MetricData::Histogram(histogram) => ("histogram", histogram.data_points().count()),
                MetricData::ExponentialHistogram(_) => ("exponential_histogram", 0),
            };
            self.0.push((
                metadata.name.to_string(),
                metadata.scope_name.to_string(),
                metadata.unit.to_string(),
                kind,
                points,
            ));
        }
    }

    #[test]
    fn visit_metrics_dispatches_family_and_metadata() {
        let reader = Arc::new(
            ManualReader::builder()
                .with_temporality(Temporality::Delta)
                .build(),
        );
        let provider = SdkMeterProvider::builder()
            .with_reader(InMemoryReader::new(Arc::clone(&reader)))
            .build();
        let meter = provider.meter("test.scope");
        meter
            .u64_counter("requests")
            .with_unit("items")
            .build()
            .add(3, &[KeyValue::new("route", "a")]);

        let mut metrics = ResourceMetrics::default();
        reader
            .collect(&mut metrics)
            .expect("metrics should collect");
        let mut visitor = RecordingVisitor::default();
        visit_metrics(&metrics, &mut visitor);

        assert_eq!(
            visitor.0,
            vec![(
                "requests".to_string(),
                "test.scope".to_string(),
                "items".to_string(),
                "sum",
                1,
            )]
        );
    }
}
