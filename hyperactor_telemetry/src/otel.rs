/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Environment variable to select the OpenTelemetry backend
const OTEL_BACKEND_ENV: &str = "HYPERACTOR_OTEL_BACKEND";

#[derive(Debug, Clone, PartialEq)]
pub enum Backend {
    Scuba,
    Prometheus,
    None,
}

impl Backend {
    fn from_env() -> Self {
        match std::env::var(OTEL_BACKEND_ENV).as_deref() {
            Ok("prometheus") => Backend::Prometheus,
            Ok("scuba") => Backend::Scuba,
            Ok("none") | Ok("") => Backend::None,
            _ => {
                // Default behavior: use scuba if fbcode_build is enabled, otherwise none
                #[cfg(fbcode_build)]
                return Backend::Scuba;
                #[cfg(not(fbcode_build))]
                return Backend::None;
            }
        }
    }
}

#[allow(dead_code)]
pub fn tracing_layer<
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
>() -> Option<Box<dyn tracing_subscriber::Layer<S> + Send + Sync>> {
    match Backend::from_env() {
        Backend::Scuba => {
            #[cfg(fbcode_build)]
            return Some(Box::new(crate::meta::tracing_layer()));
            #[cfg(not(fbcode_build))]
            None
        }
        Backend::Prometheus => {
            #[cfg(prometheus_build)]
            return Some(Box::new(crate::prometheus::tracing_layer()));
            #[cfg(not(prometheus_build))]
            None
        }
        Backend::None => None,
    }
}

#[allow(dead_code)]
pub fn init_metrics() {
    match Backend::from_env() {
        Backend::Scuba => {
            #[cfg(fbcode_build)]
            opentelemetry::global::set_meter_provider(crate::meta::meter_provider());
        }
        Backend::Prometheus => {
            if let Err(e) = crate::prometheus::initialize_prometheus_backend() {
                tracing::error!("Failed to initialize Prometheus backend: {}", e);
            }
        }
        Backend::None => {
            tracing::warn!("Metrics backend is set to None, no metrics will be collected");
            // Do nothing for None backend
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backend_from_env_defaults() {
        // Test default behavior when environment variable is not set
        // SAFETY: Setting environment variables in tests is generally safe as long as tests are run serially.
        unsafe {
            std::env::remove_var(OTEL_BACKEND_ENV);
        }

        let backend = Backend::from_env();

        #[cfg(fbcode_build)]
        assert_eq!(backend, Backend::Scuba);

        #[cfg(not(fbcode_build))]
        assert_eq!(backend, Backend::None);
    }

    #[test]
    fn test_backend_from_env_prometheus() {
        // Test that prometheus backend is selected when env var is set to "prometheus"
        // SAFETY: Setting environment variables in tests is generally safe as long as tests are run serially.
        unsafe {
            std::env::set_var(OTEL_BACKEND_ENV, "prometheus");
        }

        let backend = Backend::from_env();
        assert_eq!(backend, Backend::Prometheus);

        // SAFETY: Setting environment variables in tests is generally safe as long as tests are run serially.
        unsafe {
            std::env::remove_var(OTEL_BACKEND_ENV);
        }
    }

    #[test]
    fn test_backend_from_env_scuba() {
        // Test that scuba backend is selected when env var is set to "scuba"
        // SAFETY: Setting environment variables in tests is generally safe as long as tests are run serially.
        unsafe {
            std::env::set_var(OTEL_BACKEND_ENV, "scuba");
        }

        let backend = Backend::from_env();
        assert_eq!(backend, Backend::Scuba);

        // SAFETY: Setting environment variables in tests is generally safe as long as tests are run serially.
        unsafe {
            std::env::remove_var(OTEL_BACKEND_ENV);
        }
    }

    #[test]
    fn test_backend_from_env_empty_string() {
        // Test that none backend is selected when env var is set to empty string
        // SAFETY: Setting environment variables in tests is generally safe as long as tests are run serially.
        unsafe {
            std::env::set_var(OTEL_BACKEND_ENV, "");
        }

        let backend = Backend::from_env();
        assert_eq!(backend, Backend::None);

        // SAFETY: Setting environment variables in tests is generally safe as long as tests are run serially.
        unsafe {
            std::env::remove_var(OTEL_BACKEND_ENV);
        }
    }
}
