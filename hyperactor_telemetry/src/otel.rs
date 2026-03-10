/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[allow(dead_code)]
pub fn tracing_layer<
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
>() -> Option<impl tracing_subscriber::Layer<S>> {
    #[cfg(fbcode_build)]
    {
        Some(crate::meta::tracing_layer())
    }
    #[cfg(not(fbcode_build))]
    {
        None::<Box<dyn tracing_subscriber::Layer<S> + Send + Sync>>
    }
}

#[allow(dead_code)]
pub fn init_metrics() {
    #[cfg(fbcode_build)]
    {
        opentelemetry::global::set_meter_provider(crate::meta::meter_provider());
    }
    #[cfg(not(fbcode_build))]
    {
        if let Some(provider) = crate::otlp::otlp_meter_provider() {
            opentelemetry::global::set_meter_provider(provider);
        }
    }
}
