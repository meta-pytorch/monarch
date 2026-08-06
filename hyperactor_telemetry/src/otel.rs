/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::sync::OnceLock;

#[cfg(not(all(fbcode_build, target_os = "linux")))]
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::PeriodicReader;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;

static METER_PROVIDER: OnceLock<SdkMeterProvider> = OnceLock::new();

fn periodic_reader<E>(exporter: E) -> PeriodicReader<E>
where
    E: PushMetricExporter,
{
    let interval = hyperactor_config::global::get(crate::config::OTEL_METRIC_EXPORT_INTERVAL);
    PeriodicReader::builder(exporter)
        .with_interval(interval)
        .build()
}

fn install_metric_provider<B>(build_provider: B)
where
    B: FnOnce() -> SdkMeterProvider,
{
    install_metric_provider_with(
        &METER_PROVIDER,
        build_provider,
        opentelemetry::global::set_meter_provider,
    );
}

fn install_metric_provider_with<B, F>(
    provider_cell: &OnceLock<SdkMeterProvider>,
    build_provider: B,
    set_global_provider: F,
) where
    B: FnOnce() -> SdkMeterProvider,
    F: FnOnce(SdkMeterProvider),
{
    provider_cell.get_or_init(|| {
        let provider = build_provider();
        set_global_provider(provider.clone());
        provider
    });
}

#[allow(dead_code)]
pub fn tracing_layer<
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
>() -> Option<impl tracing_subscriber::Layer<S>> {
    #[cfg(all(fbcode_build, target_os = "linux"))]
    {
        Some(crate::meta::tracing_layer())
    }
    #[cfg(not(all(fbcode_build, target_os = "linux")))]
    {
        None::<Box<dyn tracing_subscriber::Layer<S> + Send + Sync>>
    }
}

#[allow(dead_code)]
pub fn init_metrics() {
    if METER_PROVIDER.get().is_some() {
        return;
    }

    #[cfg(all(fbcode_build, target_os = "linux"))]
    {
        let resource = crate::meta::metrics_resource();
        let exporter = crate::meta::scuba_metric_exporter(&resource);
        let uds = crate::metrics_table::uds_metric_exporter();
        install_metric_provider(|| {
            SdkMeterProvider::builder()
                .with_reader(periodic_reader(exporter))
                .with_reader(periodic_reader(uds))
                .with_resource(resource)
                .build()
        });
    }
    #[cfg(not(all(fbcode_build, target_os = "linux")))]
    {
        let otlp = crate::otlp::otlp_metric_exporter();
        let uds = crate::metrics_table::uds_metric_exporter();
        let resource = Resource::builder().build();
        install_metric_provider(|| {
            let mut builder = SdkMeterProvider::builder()
                .with_reader(periodic_reader(uds))
                .with_resource(resource);
            if let Some(exporter) = otlp {
                builder = builder.with_reader(periodic_reader(exporter));
            }
            builder.build()
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;

    #[test]
    fn installs_metric_provider_once() {
        let provider_cell = OnceLock::new();
        let installations = AtomicUsize::new(0);

        for _ in 0..2 {
            install_metric_provider_with(
                &provider_cell,
                || SdkMeterProvider::builder().build(),
                |_| {
                    installations.fetch_add(1, Ordering::Relaxed);
                },
            );
        }

        assert_eq!(installations.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn installs_metric_provider_once_concurrently() {
        let provider_cell = Arc::new(OnceLock::new());
        let constructions = Arc::new(AtomicUsize::new(0));
        let installations = Arc::new(AtomicUsize::new(0));
        let handles = (0..8)
            .map(|_| {
                let provider_cell = Arc::clone(&provider_cell);
                let constructions = Arc::clone(&constructions);
                let installations = Arc::clone(&installations);
                std::thread::spawn(move || {
                    install_metric_provider_with(
                        &provider_cell,
                        || {
                            constructions.fetch_add(1, Ordering::Relaxed);
                            SdkMeterProvider::builder().build()
                        },
                        |_| {
                            installations.fetch_add(1, Ordering::Relaxed);
                        },
                    );
                })
            })
            .collect::<Vec<_>>();

        for handle in handles {
            handle
                .join()
                .expect("provider installation should not panic");
        }

        assert_eq!(constructions.load(Ordering::Relaxed), 1);
        assert_eq!(installations.load(Ordering::Relaxed), 1);
    }
}
