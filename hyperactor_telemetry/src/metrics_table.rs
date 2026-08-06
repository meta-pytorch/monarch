/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! OpenTelemetry metric export to the distributed telemetry table.

use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use monarch_telemetry_schema::metric_tables::MetricGauge;
use monarch_telemetry_schema::metric_tables::MetricGaugeBuffer;
use monarch_telemetry_schema::metric_tables::MetricHistogram;
use monarch_telemetry_schema::metric_tables::MetricHistogramBuffer;
use monarch_telemetry_schema::metric_tables::MetricSum;
use monarch_telemetry_schema::metric_tables::MetricSumBuffer;
use opentelemetry::Array;
use opentelemetry::Value;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::metrics::Temporality;
use opentelemetry_sdk::metrics::data::AggregatedMetrics;
use opentelemetry_sdk::metrics::data::Gauge;
use opentelemetry_sdk::metrics::data::Histogram;
use opentelemetry_sdk::metrics::data::MetricData;
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::data::Sum;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;

#[derive(Debug)]
pub(crate) struct UdsMetricExporter;

pub(crate) fn uds_metric_exporter() -> UdsMetricExporter {
    UdsMetricExporter
}

impl PushMetricExporter for UdsMetricExporter {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        if crate::unix_sink::unix_socket_sink_is_active() {
            let buffers = encode_metrics(metrics);
            crate::unix_sink::send_metric_buffers(buffers.gauges, buffers.sums, buffers.histograms);
        }
        Ok(())
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        Ok(())
    }

    fn temporality(&self) -> Temporality {
        Temporality::Delta
    }
}

#[derive(Default)]
struct MetricBuffers {
    gauges: MetricGaugeBuffer,
    sums: MetricSumBuffer,
    histograms: MetricHistogramBuffer,
}

#[derive(Clone, Copy)]
struct InstrumentMetadata<'a> {
    name: &'a str,
    scope_name: &'a str,
    unit: &'a str,
}

trait TableMetricNumber: Copy {
    fn into_columns(self) -> (Option<f64>, Option<i64>, Option<u64>);
}

impl TableMetricNumber for f64 {
    fn into_columns(self) -> (Option<f64>, Option<i64>, Option<u64>) {
        (Some(self), None, None)
    }
}

impl TableMetricNumber for i64 {
    fn into_columns(self) -> (Option<f64>, Option<i64>, Option<u64>) {
        (None, Some(self), None)
    }
}

impl TableMetricNumber for u64 {
    fn into_columns(self) -> (Option<f64>, Option<i64>, Option<u64>) {
        (None, None, Some(self))
    }
}

fn encode_metrics(metrics: &ResourceMetrics) -> MetricBuffers {
    let mut buffers = MetricBuffers::default();
    let resource_attributes_json = resource_attributes_to_json(metrics.resource());
    for scope in metrics.scope_metrics() {
        for metric in scope.metrics() {
            let metadata = InstrumentMetadata {
                name: metric.name(),
                scope_name: scope.scope().name(),
                unit: metric.unit(),
            };
            match metric.data() {
                AggregatedMetrics::F64(data) => {
                    buffer_metric(&mut buffers, &resource_attributes_json, metadata, data)
                }
                AggregatedMetrics::I64(data) => {
                    buffer_metric(&mut buffers, &resource_attributes_json, metadata, data)
                }
                AggregatedMetrics::U64(data) => {
                    buffer_metric(&mut buffers, &resource_attributes_json, metadata, data)
                }
            }
        }
    }
    buffers
}

fn buffer_metric<T: TableMetricNumber>(
    buffers: &mut MetricBuffers,
    resource_attributes_json: &str,
    metadata: InstrumentMetadata<'_>,
    data: &MetricData<T>,
) {
    match data {
        MetricData::Gauge(gauge) => buffer_gauge(
            &mut buffers.gauges,
            resource_attributes_json,
            metadata,
            gauge,
        ),
        MetricData::Sum(sum) => {
            buffer_sum(&mut buffers.sums, resource_attributes_json, metadata, sum)
        }
        MetricData::Histogram(histogram) => buffer_histogram(
            &mut buffers.histograms,
            resource_attributes_json,
            metadata,
            histogram,
        ),
        MetricData::ExponentialHistogram(_) => tracing::warn!(
            metric = metadata.name,
            "distributed metrics table does not support exponential histograms"
        ),
    }
}

fn buffer_gauge<T: TableMetricNumber>(
    buffer: &mut MetricGaugeBuffer,
    resource_attributes_json: &str,
    metadata: InstrumentMetadata<'_>,
    gauge: &Gauge<T>,
) {
    for point in gauge.data_points() {
        let (value_f64, value_i64, value_u64) = point.value().into_columns();
        buffer.insert(MetricGauge {
            name: metadata.name.to_string(),
            timestamp_us: timestamp_to_micros(gauge.time()),
            start_timestamp_us: gauge.start_time().map(timestamp_to_micros),
            scope_name: metadata.scope_name.to_string(),
            unit: metadata.unit.to_string(),
            attributes_json: key_values_to_json(point.attributes()),
            resource_attributes_json: resource_attributes_json.to_string(),
            value_f64,
            value_i64,
            value_u64,
        });
    }
}

fn buffer_sum<T: TableMetricNumber>(
    buffer: &mut MetricSumBuffer,
    resource_attributes_json: &str,
    metadata: InstrumentMetadata<'_>,
    sum: &Sum<T>,
) {
    for point in sum.data_points() {
        let (sum_f64, sum_i64, sum_u64) = point.value().into_columns();
        buffer.insert(MetricSum {
            name: metadata.name.to_string(),
            timestamp_us: timestamp_to_micros(sum.time()),
            start_timestamp_us: timestamp_to_micros(sum.start_time()),
            scope_name: metadata.scope_name.to_string(),
            unit: metadata.unit.to_string(),
            temporality: temporality_name(sum.temporality()).to_string(),
            is_monotonic: sum.is_monotonic(),
            attributes_json: key_values_to_json(point.attributes()),
            resource_attributes_json: resource_attributes_json.to_string(),
            sum_f64,
            sum_i64,
            sum_u64,
        });
    }
}

fn buffer_histogram<T: TableMetricNumber>(
    buffer: &mut MetricHistogramBuffer,
    resource_attributes_json: &str,
    metadata: InstrumentMetadata<'_>,
    histogram: &Histogram<T>,
) {
    for point in histogram.data_points() {
        let (sum_f64, sum_i64, sum_u64) = point.sum().into_columns();
        let (min_f64, min_i64, min_u64) = point
            .min()
            .map(TableMetricNumber::into_columns)
            .unwrap_or_default();
        let (max_f64, max_i64, max_u64) = point
            .max()
            .map(TableMetricNumber::into_columns)
            .unwrap_or_default();
        buffer.insert(MetricHistogram {
            name: metadata.name.to_string(),
            timestamp_us: timestamp_to_micros(histogram.time()),
            start_timestamp_us: timestamp_to_micros(histogram.start_time()),
            scope_name: metadata.scope_name.to_string(),
            unit: metadata.unit.to_string(),
            temporality: temporality_name(histogram.temporality()).to_string(),
            attributes_json: key_values_to_json(point.attributes()),
            resource_attributes_json: resource_attributes_json.to_string(),
            count: point.count(),
            sum_f64,
            sum_i64,
            sum_u64,
            min_f64,
            min_i64,
            min_u64,
            max_f64,
            max_i64,
            max_u64,
            bounds_json: serde_json::to_string(&point.bounds().collect::<Vec<_>>())
                .expect("metric bounds should serialize"),
            bucket_counts_json: serde_json::to_string(&point.bucket_counts().collect::<Vec<_>>())
                .expect("metric bucket counts should serialize"),
        });
    }
}

fn temporality_name(temporality: Temporality) -> &'static str {
    match temporality {
        Temporality::Cumulative => "cumulative",
        Temporality::Delta => "delta",
        _ => "unknown",
    }
}

fn key_values_to_json<'a>(
    attributes: impl IntoIterator<Item = &'a opentelemetry::KeyValue>,
) -> String {
    monarch_telemetry_schema::fields_to_json(
        attributes
            .into_iter()
            .map(|kv| (kv.key.as_str(), otel_value_to_json(&kv.value))),
    )
}

fn resource_attributes_to_json(resource: &opentelemetry_sdk::Resource) -> String {
    monarch_telemetry_schema::fields_to_json(
        resource
            .iter()
            .map(|(key, value)| (key.as_str(), otel_value_to_json(value))),
    )
}

fn otel_value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Bool(value) => serde_json::Value::Bool(*value),
        Value::I64(value) => serde_json::Value::Number((*value).into()),
        Value::F64(value) => serde_json::Number::from_f64(*value)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::String(value) => serde_json::Value::String(value.as_str().to_string()),
        Value::Array(array) => match array {
            Array::Bool(values) => serde_json::Value::Array(
                values
                    .iter()
                    .copied()
                    .map(serde_json::Value::Bool)
                    .collect(),
            ),
            Array::I64(values) => serde_json::Value::Array(
                values
                    .iter()
                    .copied()
                    .map(|value| serde_json::Value::Number(value.into()))
                    .collect(),
            ),
            Array::F64(values) => serde_json::Value::Array(
                values
                    .iter()
                    .map(|value| {
                        serde_json::Number::from_f64(*value)
                            .map(serde_json::Value::Number)
                            .unwrap_or(serde_json::Value::Null)
                    })
                    .collect(),
            ),
            Array::String(values) => serde_json::Value::Array(
                values
                    .iter()
                    .map(|value| serde_json::Value::String(value.as_str().to_string()))
                    .collect(),
            ),
            _ => serde_json::Value::String(value.as_str().into_owned()),
        },
        _ => serde_json::Value::String(value.as_str().into_owned()),
    }
}

fn timestamp_to_micros(timestamp: SystemTime) -> i64 {
    timestamp
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::Array as _;
    use datafusion::arrow::array::BooleanArray;
    use datafusion::arrow::array::Float64Array;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::array::UInt64Array;
    use datafusion::arrow::record_batch::RecordBatch;
    use monarch_record_batch::RecordBatchBuffer;
    use opentelemetry::KeyValue;
    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::metrics::ManualReader;
    use opentelemetry_sdk::metrics::SdkMeterProvider;
    use opentelemetry_sdk::metrics::reader::MetricReader;
    use serde_json::json;

    use super::*;
    use crate::in_memory_reader::InMemoryReader;

    fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> &'a T {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("missing {name} column"))
            .as_any()
            .downcast_ref::<T>()
            .unwrap_or_else(|| panic!("unexpected {name} column type"))
    }

    #[test]
    fn encodes_typed_points_and_sum_semantics() {
        const EXACT_I64: i64 = -9_007_199_254_740_993;

        let reader = Arc::new(
            ManualReader::builder()
                .with_temporality(Temporality::Delta)
                .build(),
        );
        let resource = Resource::builder_empty()
            .with_attribute(KeyValue::new("service.name", "test-service"))
            .build();
        let provider = SdkMeterProvider::builder()
            .with_resource(resource)
            .with_reader(InMemoryReader::new(Arc::clone(&reader)))
            .build();
        let meter = provider.meter("test.scope");
        meter
            .u64_counter("requests")
            .build()
            .add(u64::MAX, &[KeyValue::new("route", "a")]);
        meter
            .i64_up_down_counter("queue.depth")
            .build()
            .add(EXACT_I64, &[]);
        meter.f64_gauge("load").build().record(1.5, &[]);
        meter
            .u64_histogram("batch.size")
            .with_boundaries(vec![1.0, 10.0])
            .build()
            .record(9_007_199_254_740_993, &[]);

        let mut metrics = ResourceMetrics::default();
        reader.collect(&mut metrics).unwrap();
        let MetricBuffers {
            mut gauges,
            mut sums,
            mut histograms,
        } = encode_metrics(&metrics);
        let gauge_batch = gauges.drain_to_record_batch().unwrap();
        let sum_batch = sums.drain_to_record_batch().unwrap();
        let histogram_batch = histograms.drain_to_record_batch().unwrap();

        assert_eq!(gauge_batch.num_rows(), 1);
        assert_eq!(sum_batch.num_rows(), 2);
        assert_eq!(histogram_batch.num_rows(), 1);
        let names = column::<StringArray>(&sum_batch, "name");
        let row_for = |name: &str| {
            (0..sum_batch.num_rows())
                .find(|row| names.value(*row) == name)
                .unwrap_or_else(|| panic!("missing {name} row"))
        };

        let requests = row_for("requests");
        assert_eq!(
            column::<UInt64Array>(&sum_batch, "sum_u64").value(requests),
            u64::MAX
        );
        assert!(column::<Float64Array>(&sum_batch, "sum_f64").is_null(requests));
        assert!(column::<Int64Array>(&sum_batch, "sum_i64").is_null(requests));
        assert_eq!(
            column::<StringArray>(&sum_batch, "temporality").value(requests),
            "delta"
        );
        assert!(column::<BooleanArray>(&sum_batch, "is_monotonic").value(requests));

        let queue = row_for("queue.depth");
        assert_eq!(
            column::<Int64Array>(&sum_batch, "sum_i64").value(queue),
            EXACT_I64
        );
        assert!(!column::<BooleanArray>(&sum_batch, "is_monotonic").value(queue));

        assert_eq!(column::<StringArray>(&gauge_batch, "name").value(0), "load");
        assert_eq!(
            column::<Float64Array>(&gauge_batch, "value_f64").value(0),
            1.5
        );

        assert_eq!(
            column::<StringArray>(&histogram_batch, "name").value(0),
            "batch.size"
        );
        assert_eq!(
            column::<UInt64Array>(&histogram_batch, "sum_u64").value(0),
            9_007_199_254_740_993
        );
        assert_eq!(
            column::<UInt64Array>(&histogram_batch, "min_u64").value(0),
            9_007_199_254_740_993
        );
        assert_eq!(
            column::<UInt64Array>(&histogram_batch, "max_u64").value(0),
            9_007_199_254_740_993
        );
        assert_eq!(column::<UInt64Array>(&histogram_batch, "count").value(0), 1);
        let bounds: Vec<f64> =
            serde_json::from_str(column::<StringArray>(&histogram_batch, "bounds_json").value(0))
                .unwrap();
        let bucket_counts: Vec<u64> = serde_json::from_str(
            column::<StringArray>(&histogram_batch, "bucket_counts_json").value(0),
        )
        .unwrap();
        assert_eq!(bounds, vec![1.0, 10.0]);
        assert_eq!(bucket_counts.len(), bounds.len() + 1);
        assert_eq!(bucket_counts.iter().sum::<u64>(), 1);

        let attributes: serde_json::Value = serde_json::from_str(
            column::<StringArray>(&sum_batch, "attributes_json").value(requests),
        )
        .unwrap();
        assert_eq!(attributes["route"], json!("a"));
        let resources: serde_json::Value = serde_json::from_str(
            column::<StringArray>(&sum_batch, "resource_attributes_json").value(requests),
        )
        .unwrap();
        assert_eq!(resources["service.name"], json!("test-service"));
    }

    #[test]
    fn attributes_json_preserves_value_types() {
        let attributes = [
            KeyValue::new("name", "worker"),
            KeyValue::new("pid", 42_i64),
            KeyValue::new("enabled", true),
            KeyValue::new("ratio", 1.5_f64),
            KeyValue::new("flags", Value::Array(Array::Bool(vec![true, false]))),
            KeyValue::new("ranks", Value::Array(Array::I64(vec![1, 2]))),
            KeyValue::new("weights", Value::Array(Array::F64(vec![1.5, 2.5]))),
            KeyValue::new(
                "names",
                Value::Array(Array::String(vec!["first".into(), "second".into()])),
            ),
        ];

        let attributes: serde_json::Value =
            serde_json::from_str(&key_values_to_json(attributes.iter())).unwrap();

        assert_eq!(attributes["name"], json!("worker"));
        assert_eq!(attributes["pid"], json!(42));
        assert_eq!(attributes["enabled"], json!(true));
        assert_eq!(attributes["ratio"], json!(1.5));
        assert_eq!(attributes["flags"], json!([true, false]));
        assert_eq!(attributes["ranks"], json!([1, 2]));
        assert_eq!(attributes["weights"], json!([1.5, 2.5]));
        assert_eq!(attributes["names"], json!(["first", "second"]));
    }
}
