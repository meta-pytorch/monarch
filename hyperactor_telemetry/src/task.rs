/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[inline]
pub fn current_task_id() -> u64 {
    tokio::task::try_id().map_or(0, |x| x.to_string().parse::<u64>().unwrap_or(0))
}

#[macro_export]
macro_rules! spawn {
    ($f:expr) => {{
        $crate::spawn!(
            concat!(file!(), ":", line!()),
            tokio::runtime::Handle::current(),
            $f
        )
    }};
    ($name:expr, $f:expr) => {{ $crate::spawn!($name, tokio::runtime::Handle::current(), $f) }};
    ($name:expr, $rt:expr, $f:expr) => {{
        let current = tracing::span::Span::current().id();
        let parent_task = $crate::task::current_task_id();
        let ft = $f;
        $rt.spawn(async move {
            let span = tracing::debug_span!($name, parent_tokio_task_id = parent_task);
            span.follows_from(current);
            span.in_scope(|| tracing::debug!("spawned_tokio_task"));
            $crate::tracing::Instrument::instrument(ft, span).await
        })
    }};
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use tokio::time::timeout;
    use tracing_test::traced_test;

    use super::*;

    /// Like the `logs_assert` injected by `#[traced_test]`, but without scope
    /// filtering. Use when asserting on events emitted outside the test's span
    /// (e.g. from spawned tasks or panic hooks).
    fn logs_assert_unscoped(f: impl Fn(&[&str]) -> Result<(), String>) {
        let buf = tracing_test::internal::global_buf().lock().unwrap();
        let logs_str = std::str::from_utf8(&buf).expect("Logs contain invalid UTF8");
        let lines: Vec<&str> = logs_str.lines().collect();
        match f(&lines) {
            Ok(()) => {}
            Err(msg) => panic!("{}", msg),
        }
    }

    #[traced_test]
    #[tokio::test]
    async fn test_current_task_id_returns_valid_id() {
        let handle = spawn!("test", async move {
            let task_id = current_task_id();
            // Task ID should be non-zero when called from within a tokio task
            assert!(task_id > 0, "Task ID should be greater than 0");
        });
        handle.await.unwrap();
    }

    #[traced_test]
    #[tokio::test]
    async fn test_current_task_id_different_tasks() {
        let task1_id = Arc::new(std::sync::Mutex::new(0u64));
        let task2_id = Arc::new(std::sync::Mutex::new(0u64));

        let task1_id_clone = task1_id.clone();
        let task2_id_clone = task2_id.clone();

        let handle1 = crate::spawn!(async move {
            *task1_id_clone.lock().unwrap() = current_task_id();
        });

        let handle2 = crate::spawn!(async move {
            *task2_id_clone.lock().unwrap() = current_task_id();
        });

        handle1.await.unwrap();
        handle2.await.unwrap();

        let id1 = *task1_id.lock().unwrap();
        let id2 = *task2_id.lock().unwrap();

        assert!(id1 > 0, "Task 1 ID should be greater than 0");
        assert!(id2 > 0, "Task 2 ID should be greater than 0");
        assert_ne!(id1, id2, "Different tasks should have different IDs");
    }

    #[traced_test]
    #[tokio::test]
    async fn test_spawn_macro_basic_functionality() {
        let completed = Arc::new(AtomicBool::new(false));
        let completed_clone = completed.clone();

        let handle = spawn!("test_task", async move {
            completed_clone.store(true, Ordering::SeqCst);
            42
        });

        let result = handle.await.unwrap();
        assert_eq!(result, 42);
        assert!(completed.load(Ordering::SeqCst));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_spawn_macro_with_runtime_handle() {
        let rt = tokio::runtime::Handle::current();
        let completed = Arc::new(AtomicBool::new(false));
        let completed_clone = completed.clone();

        let handle = spawn!("test_task_with_rt", rt, async move {
            completed_clone.store(true, Ordering::SeqCst);
            "success"
        });

        let result = handle.await.unwrap();
        assert_eq!(result, "success");
        assert!(completed.load(Ordering::SeqCst));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_spawn_macro_with_async_operation() {
        let handle = spawn!("async_operation", async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            "async_result"
        });

        let result = timeout(Duration::from_secs(1), handle)
            .await
            .expect("Task should complete within timeout")
            .expect("Task should not panic");

        assert_eq!(result, "async_result");
    }

    #[traced_test]
    #[tokio::test]
    async fn test_spawn_macro_error_handling() {
        let handle = spawn!("error_task", async {
            panic!("intentional panic");
        });

        let result = handle.await;
        assert!(result.is_err(), "Task should panic and return an error");
    }

    #[traced_test]
    #[tokio::test]
    async fn test_spawn_macro_multiple_tasks() {
        let num_tasks = 5;
        let completed_count = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for i in 0..num_tasks {
            let count_clone = completed_count.clone();
            let handle = spawn!("parallel_task", async move {
                count_clone.fetch_add(1, Ordering::SeqCst);
                i
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        let mut results = Vec::new();
        for handle in handles {
            let result = handle.await.expect("Task should complete successfully");
            results.push(result);
        }

        assert_eq!(results.len(), num_tasks);
        assert_eq!(completed_count.load(Ordering::SeqCst), num_tasks);

        // Results should contain values 0 through num_tasks-1
        let mut sorted_results = results;
        sorted_results.sort();
        let expected: Vec<usize> = (0..num_tasks).collect();
        assert_eq!(sorted_results, expected);
    }

    macro_rules! logs_match {
        ($expr:expr) => {
            logs_match!($expr, format!("{} not in logs", stringify!($expr)));
        };
        ($expr:expr, $msg:expr) => {
            logs_assert_unscoped(|lines| {
                if lines.iter().any($expr) {
                    Ok(())
                } else {
                    Err($msg.into())
                }
            })
        };
    }

    #[traced_test]
    #[tokio::test]
    async fn test_spawn_macro_creates_proper_span() {
        let completed = Arc::new(AtomicBool::new(false));
        let completed_clone = completed.clone();

        let parent_span = tracing::debug_span!("parent_span");
        let _guard = parent_span.enter();

        let handle = spawn!("child_task", async move {
            tracing::debug!(task_data = "test_value", "task_execution");
            completed_clone.store(true, Ordering::SeqCst);
            "completed"
        });

        let result = handle.await.unwrap();
        assert_eq!(result, "completed");
        assert!(completed.load(Ordering::SeqCst));

        // Check that spawn event was logged
        logs_match!(
            |line| line.contains("spawned_tokio_task"),
            "task logging never occured"
        );

        // Check that task execution event was logged
        logs_match!(|line| line.contains("task_execution"));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_spawn_macro_preserves_parent_context() {
        let parent_span = tracing::debug_span!("parent", operation = "test_context");
        let _guard = parent_span.enter();

        let handle = spawn!("context_child", async {
            tracing::debug!(child_data = "value", "child_operation");
            "context_preserved"
        });

        let result = handle.await.unwrap();
        assert_eq!(result, "context_preserved");

        // Verify the spawn event was logged
        logs_match!(|line| line.contains("spawned_tokio_task"));

        // Verify child operation was logged
        logs_match!(|line| line.contains("child_operation"));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_spawn_macro_with_instrumentation() {
        let handle = spawn!("instrumented_task", async {
            tracing::info!("inside_spawned_task");
            42
        });

        let result = handle.await.unwrap();
        assert_eq!(result, 42);

        // Check for spawned task event
        logs_match!(|line| line.contains("spawned_tokio_task"));

        // Check for task execution event
        logs_match!(|line| line.contains("inside_spawned_task"));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_spawn_macro_span_hierarchy() {
        let outer_span = tracing::info_span!("outer_operation", test_id = 123);
        let _outer_guard = outer_span.enter();

        let handle = spawn!("nested_task", async {
            let inner_span = tracing::debug_span!("inner_operation", step = "processing");
            let _inner_guard = inner_span.enter();

            tracing::warn!(data_size = 1024, "processing_data");
            "nested_complete"
        });

        let result = handle.await.unwrap();
        assert_eq!(result, "nested_complete");

        // Verify spawn debug event was captured
        logs_match!(|line| line.contains("spawned_tokio_task"));

        // Verify the warning event was captured
        logs_match!(|line| line.contains("processing_data"));

        // Verify structured data is present
        logs_match!(|line| line.contains("data_size"));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_spawn_macro_error_tracing() {
        let handle = spawn!("error_prone_task", async {
            tracing::error!(reason = "intentional", "task_about_to_fail");
            panic!("deliberate failure");
        });

        let result = handle.await;
        assert!(result.is_err(), "Task should fail");

        // Verify spawn event was logged
        logs_match!(|line| line.contains("spawned_tokio_task"));

        // Verify error event was logged before panic
        logs_match!(|line| line.contains("task_about_to_fail"));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_spawn_macro_concurrent_tracing() {
        let barrier = Arc::new(tokio::sync::Barrier::new(3));

        let handles = (0..3)
            .map(|i| {
                let barrier = barrier.clone();
                spawn!("concurrent_task", async move {
                    barrier.wait().await;
                    tracing::info!(task_num = i, "concurrent_execution");
                    i * 10
                })
            })
            .collect::<Vec<_>>();

        // Await all handles manually
        let mut results = Vec::new();
        for handle in handles {
            let result = handle.await.expect("Task should complete");
            results.push(result);
        }

        results.sort();
        assert_eq!(results, vec![0, 10, 20]);

        // Verify spawn events were logged (at least 3)
        logs_assert_unscoped(|lines| {
            let spawn_count = lines
                .iter()
                .filter(|line| line.contains("spawned_tokio_task"))
                .count();
            match spawn_count {
                3.. => Ok(()),
                _ => Err("wrong count".into()),
            }
        });

        // Verify execution events were logged
        logs_assert_unscoped(|lines| {
            let exec_count = lines
                .iter()
                .filter(|line| line.contains("concurrent_execution"))
                .count();
            if exec_count >= 3 {
                Ok(())
            } else {
                Err(format!(
                    "Expected at least 3 concurrent execution events, found {}",
                    exec_count
                ))
            }
        });
    }

    #[traced_test]
    #[tokio::test]
    async fn test_spawn_macro_with_fields() {
        let handle = spawn!("field_task", async {
            tracing::info!(user_id = 42, session = "abc123", "user_action");
            "field_test_complete"
        });

        let result = handle.await.unwrap();
        assert_eq!(result, "field_test_complete");

        // Verify spawn event was logged
        logs_match!(|line| line.contains("spawned_tokio_task"));

        // Verify user action event with fields was logged
        logs_match!(|line| line.contains("user_action")
            && line.contains("user_id")
            && line.contains("session"));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_spawn_macro_nested_spans() {
        let outer_span = tracing::info_span!("request_handler", request_id = "req-123");
        let _outer_guard = outer_span.enter();

        let handle = spawn!("database_query", async {
            let db_span = tracing::debug_span!("db_operation", table = "users");
            let _db_guard = db_span.enter();

            tracing::debug!(query = "SELECT * FROM users", "executing_query");

            let cache_span = tracing::debug_span!("cache_check", cache_key = "user:42");
            let _cache_guard = cache_span.enter();

            tracing::debug!("cache_miss");

            "query_complete"
        });

        let result = handle.await.unwrap();
        assert_eq!(result, "query_complete");
        // Verify spawn event
        logs_match!(|line| line.contains("spawned_tokio_task"));

        // Verify query execution event
        logs_match!(|line| line.contains("executing_query"));

        // Verify cache miss event
        logs_match!(|line| line.contains("cache_miss"));

        // Verify structured fields are present
        logs_match!(|line| line.contains("table"));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_spawn_macro_performance_tracing() {
        let handle = spawn!("performance_task", async {
            let start = std::time::Instant::now();

            // Simulate some work
            tokio::time::sleep(Duration::from_millis(50)).await;

            let duration = start.elapsed();
            tracing::info!(duration_ms = duration.as_millis(), "task_completed");

            duration.as_millis()
        });

        let duration_ms = handle.await.unwrap();
        assert!(duration_ms >= 50, "Task should take at least 50ms");

        // Verify spawn event was logged
        logs_match!(|line| line.contains("spawned_tokio_task"));

        // Verify task completion event with duration
        logs_match!(|line| line.contains("task_completed") && line.contains("duration_ms"));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_spawn_macro_error_with_context() {
        let outer_span = tracing::error_span!("error_context", operation = "critical_task");
        let _guard = outer_span.enter();

        let handle = spawn!("failing_task", async {
            tracing::warn!(retry_count = 1, "attempting_risky_operation");
            tracing::error!(
                error_code = "E001",
                message = "Operation failed",
                "critical_error"
            );
            panic!("Critical failure occurred");
        });

        let result = handle.await;
        assert!(result.is_err(), "Task should fail");

        // Verify spawn event
        logs_match!(|line| line.contains("spawned_tokio_task"));

        logs_match!(|line| line.contains("attempting_risky_operation"));

        // Verify critical error event
        logs_assert_unscoped(|lines| {
            if lines
                .iter()
                .any(|line| line.contains("critical_error") && line.contains("error_code"))
            {
                Ok(())
            } else {
                Err("Critical error event with error_code not found in logs".into())
            }
        });
    }
}
