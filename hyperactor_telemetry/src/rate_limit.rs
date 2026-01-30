/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Rate-limited logging macros.
//!
//! Provides macros for time-based (`*_every_ms!`) and count-based (`*_every_n!`)
//! rate limiting of log messages per call site.

/// Internal helper macro for rate-limited logging. Not intended for direct use.
#[doc(hidden)]
#[macro_export]
macro_rules! log_every_ms_impl {
    ($interval_ms:expr, $level:ident, $($args:tt)+) => {{
        const { assert!($interval_ms > 0, "interval_ms must be greater than 0"); }
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::time::{SystemTime, UNIX_EPOCH};

        // Store last log time as millis since UNIX_EPOCH, 0 means never logged
        static LAST_LOG_MS: AtomicU64 = AtomicU64::new(0);

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let last = LAST_LOG_MS.load(Ordering::Relaxed);
        let should_log = last == 0 || now_ms.saturating_sub(last) >= $interval_ms as u64;

        if should_log {
            LAST_LOG_MS.store(now_ms, Ordering::Relaxed);
            tracing::$level!($($args)+)
        }
    }};
}

/// Rate-limited trace logging. See [`info_every_ms!`] for details.
#[macro_export]
macro_rules! trace_every_ms {
    ($interval_ms:expr, $($args:tt)+) => {
        $crate::log_every_ms_impl!($interval_ms, trace, $($args)+)
    };
}

/// Rate-limited debug logging. See [`info_every_ms!`] for details.
#[macro_export]
macro_rules! debug_every_ms {
    ($interval_ms:expr, $($args:tt)+) => {
        $crate::log_every_ms_impl!($interval_ms, debug, $($args)+)
    };
}

/// Rate-limited logging that emits at most once per N milliseconds per call site.
///
/// Uses atomic operations with relaxed ordering for minimal overhead.
///
/// # Example
/// ```ignore
/// // Basic message
/// info_every_ms!(1000, "periodic status update");
///
/// // With format args
/// info_every_ms!(500, "processed {} items", count);
///
/// // With key-value pairs
/// info_every_ms!(1000, actor_id = id, "actor started");
/// ```
#[macro_export]
macro_rules! info_every_ms {
    ($interval_ms:expr, $($args:tt)+) => {
        $crate::log_every_ms_impl!($interval_ms, info, $($args)+)
    };
}

/// Rate-limited warn logging. See [`info_every_ms!`] for details.
#[macro_export]
macro_rules! warn_every_ms {
    ($interval_ms:expr, $($args:tt)+) => {
        $crate::log_every_ms_impl!($interval_ms, warn, $($args)+)
    };
}

/// Rate-limited error logging. See [`info_every_ms!`] for details.
#[macro_export]
macro_rules! error_every_ms {
    ($interval_ms:expr, $($args:tt)+) => {
        $crate::log_every_ms_impl!($interval_ms, error, $($args)+)
    };
}

/// Internal helper macro for count-based rate-limited logging. Not intended for direct use.
#[doc(hidden)]
#[macro_export]
macro_rules! log_every_n_impl {
    ($n:expr, $level:ident, $($args:tt)+) => {{
        const { assert!($n > 0, "n must be greater than 0"); }
        use std::sync::atomic::{AtomicU64, Ordering};

        static COUNTER: AtomicU64 = AtomicU64::new(0);

        // fetch_add returns the previous value, so first call returns 0
        // This gives us glog behavior: log on 1st, n+1, 2n+1, ... invocations
        let count = COUNTER.fetch_add(1, Ordering::Relaxed);
        if count % $n as u64 == 0 {
            tracing::$level!($($args)+)
        }
    }};
}

/// Rate-limited trace logging. See [`info_every_n!`] for details.
#[macro_export]
macro_rules! trace_every_n {
    ($n:expr, $($args:tt)+) => {
        $crate::log_every_n_impl!($n, trace, $($args)+)
    };
}

/// Rate-limited debug logging. See [`info_every_n!`] for details.
#[macro_export]
macro_rules! debug_every_n {
    ($n:expr, $($args:tt)+) => {
        $crate::log_every_n_impl!($n, debug, $($args)+)
    };
}

/// Count-based rate-limited logging that emits on the first invocation,
/// then once every N invocations per call site (matching glog behavior).
///
/// Uses atomic operations with relaxed ordering for minimal overhead.
///
/// # Example
/// ```ignore
/// // Basic message
/// info_every_n!(100, "periodic status");
///
/// // With format args
/// info_every_n!(50, "processed {} items", count);
///
/// // With key-value pairs
/// info_every_n!(100, actor_id = id, "actor started");
/// ```
#[macro_export]
macro_rules! info_every_n {
    ($n:expr, $($args:tt)+) => {
        $crate::log_every_n_impl!($n, info, $($args)+)
    };
}

/// Rate-limited warn logging. See [`info_every_n!`] for details.
#[macro_export]
macro_rules! warn_every_n {
    ($n:expr, $($args:tt)+) => {
        $crate::log_every_n_impl!($n, warn, $($args)+)
    };
}

/// Rate-limited error logging. See [`info_every_n!`] for details.
#[macro_export]
macro_rules! error_every_n {
    ($n:expr, $($args:tt)+) => {
        $crate::log_every_n_impl!($n, error, $($args)+)
    };
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU32;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use tracing_subscriber::prelude::*;

    #[test]
    fn test_rate_limited_logging_macros() {
        static CALL_COUNT: AtomicU32 = AtomicU32::new(0);

        // Test that the macros compile with various argument patterns
        // Using a test subscriber to capture events
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .with_writer(move || {
                    CALL_COUNT.fetch_add(1, Ordering::SeqCst);
                    std::io::stdout()
                })
                .with_filter(tracing_subscriber::filter::LevelFilter::TRACE),
        );

        tracing::subscriber::with_default(subscriber, || {
            let initial = CALL_COUNT.load(Ordering::SeqCst);

            // Call the same call site multiple times in a loop.
            // First iteration should log, subsequent ones should be suppressed.
            for i in 0..5 {
                info_every_ms!(50, "test message iteration {}", i);
            }
            let after_loop = CALL_COUNT.load(Ordering::SeqCst);
            assert_eq!(
                after_loop - initial,
                1,
                "only first iteration should log: {} -> {}",
                initial,
                after_loop
            );

            // After interval, should log again
            std::thread::sleep(Duration::from_millis(60));
            for i in 5..8 {
                info_every_ms!(50, "test message iteration {}", i);
            }
            let after_interval = CALL_COUNT.load(Ordering::SeqCst);
            assert_eq!(
                after_interval - after_loop,
                1,
                "only first iteration after sleep should log: {} -> {}",
                after_loop,
                after_interval
            );
        });

        // Verify other macro levels compile with various argument patterns
        let name = "test";
        trace_every_ms!(100, "trace message");
        debug_every_ms!(100, "debug message");
        warn_every_ms!(100, key = "value", %name, "warn with fields");
        error_every_ms!(100, count = 42, ?name, "error with format: {}", "test");
    }

    #[test]
    fn test_count_based_rate_limited_logging() {
        use std::io::Write;
        use std::sync::Arc;
        use std::sync::Mutex;

        #[derive(Clone)]
        struct SharedWriter(Arc<Mutex<Vec<u8>>>);

        impl Write for SharedWriter {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let buffer = Arc::new(Mutex::new(Vec::new()));
        let writer = SharedWriter(buffer.clone());

        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .with_writer(move || writer.clone())
                .with_ansi(false)
                .without_time()
                .with_filter(tracing_subscriber::filter::LevelFilter::TRACE),
        );

        tracing::subscriber::with_default(subscriber, || {
            // With n=3, should log on 1st, 4th invocations (glog behavior)
            for i in 1..=6 {
                info_every_n!(3, "message {}", i);
            }
        });

        let output = String::from_utf8(buffer.lock().unwrap().clone()).unwrap();

        // Should contain 1st and 4th messages only
        assert!(
            output.contains("message 1"),
            "should log message 1: {output}"
        );
        assert!(
            !output.contains("message 2"),
            "should NOT log message 2: {output}"
        );
        assert!(
            !output.contains("message 3"),
            "should NOT log message 3: {output}"
        );
        assert!(
            output.contains("message 4"),
            "should log message 4: {output}"
        );
        assert!(
            !output.contains("message 5"),
            "should NOT log message 5: {output}"
        );
        assert!(
            !output.contains("message 6"),
            "should NOT log message 6: {output}"
        );

        // Verify other macro levels compile with various argument patterns
        let name = "test";
        trace_every_n!(5, "trace message");
        debug_every_n!(5, "debug message");
        warn_every_n!(5, key = "value", %name, "warn with fields");
        error_every_n!(5, count = 42, ?name, "error with format: {}", "test");
    }
}
