/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyValueError;

use super::*;
use crate::runtime::GilSite;
use crate::runtime::ensure_python;
use crate::runtime::get_tokio_runtime;
use crate::runtime::monarch_with_gil_blocking;

// The blocking future path reports producer loss even without poll()/get().
// Attests HDL-10.
#[test]
fn wait_future_dropped_producer_surfaces_runtime_error() {
    ensure_python();
    let (tx, handle) = pending_handle();
    drop(tx);

    let err = get_tokio_runtime()
        .block_on(handle.wait_future())
        .unwrap_err();
    monarch_with_gil_blocking(GilSite::Test, |py| {
        assert_producer_ended_error(py, &err);
    });
}

// The Rust-only completion observer reports the same producer-lifecycle
// failure as Python's synchronous and asyncio observers.
// Attests HDL-10.
#[test]
fn wait_completion_dropped_producer_surfaces_runtime_error() {
    ensure_python();
    let (tx, handle) = pending_handle();
    drop(tx);

    let err = get_tokio_runtime()
        .block_on(handle.wait_completion())
        .unwrap_err();
    monarch_with_gil_blocking(GilSite::Test, |py| {
        assert_producer_ended_error(py, &err);
    });
}

// spawn() eagerly runs its producer -- before any observer exists -- and
// resolves the converted value.
// Attests HDL-1, HDL-2, HDL-13.
#[test]
fn rust_spawn_eagerly_resolves_handle() {
    ensure_python();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let handle = PyHandle::spawn(async move {
        let _ = started_tx.send(());
        let _ = release_rx.await;
        Ok::<i64, PyErr>(42)
    });
    // Eager: the producer starts before we create any observer.
    get_tokio_runtime()
        .block_on(started_rx)
        .expect("spawn() must start its producer eagerly, before observation");
    let _ = release_tx.send(());
    let out = get_tokio_runtime().block_on(handle.wait_future());
    let got =
        monarch_with_gil_blocking(GilSite::Test, |py| out.unwrap().extract::<i64>(py).unwrap());
    assert_eq!(
        got, 42,
        "spawn() should resolve the converted producer value"
    );
}

// Two wait_completion() observers of one handle both see the same terminal
// outcome; observation is non-consuming, and success projects to Ok(()).
// Attests HDL-3, HDL-13.
#[test]
fn rust_wait_completion_is_non_consuming() {
    ensure_python();

    // Success: two observers of one pending handle both resolve to Ok(()).
    let (tx, handle) = pending_handle();
    let ok1 = handle.wait_completion();
    let ok2 = handle.wait_completion();
    let value = monarch_with_gil_blocking(GilSite::Test, |py| 1i64.into_py_any(py).unwrap());
    tx.publish(Ok(value));
    let (r1, r2) = get_tokio_runtime().block_on(async move { tokio::join!(ok1, ok2) });
    assert!(
        r1.is_ok() && r2.is_ok(),
        "both completion observers should project success to Ok(())"
    );

    // Error: two observers of one failed handle both surface the stored error.
    let (etx, ehandle) = pending_handle();
    let err1 = ehandle.wait_completion();
    let err2 = ehandle.wait_completion();
    etx.publish(Err(PyValueError::new_err("boom")));
    let (e1, e2) = get_tokio_runtime().block_on(async move { tokio::join!(err1, err2) });
    monarch_with_gil_blocking(GilSite::Test, |py| {
        assert!(
            e1.unwrap_err().is_instance_of::<PyValueError>(py),
            "the error projection should clone the stored PyErr"
        );
        assert!(e2.unwrap_err().is_instance_of::<PyValueError>(py));
    });
}

// Dropping the returned Handle before the producer completes does not cancel
// it (HDL-4: the core has no producer task handle to abort).
// Attests HDL-4, HDL-13.
#[test]
fn rust_spawn_survives_handle_drop() {
    ensure_python();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
    let handle = PyHandle::spawn(async move {
        let _ = release_rx.await;
        let _ = done_tx.send(());
        Ok::<i64, PyErr>(7)
    });
    // Drop the observer before releasing the producer.
    drop(handle);
    let _ = release_tx.send(());
    // The producer still runs to completion despite the dropped Handle.
    get_tokio_runtime()
        .block_on(done_rx)
        .expect("dropping the Handle must not cancel the spawn() producer");
}

// The real spawn() -> wait_completion() pair: a spawn()-produced future that
// returns Err surfaces the exact exception (type + message) through
// wait_completion, and observing twice sequentially returns the same cached
// error -- proving non-consuming error projection over a real producer.
// Attests HDL-1, HDL-2, HDL-3, HDL-13.
#[test]
fn rust_spawn_error_observed_by_wait_completion_twice() {
    ensure_python();
    let handle = PyHandle::spawn(async move { Err::<(), PyErr>(PyValueError::new_err("boom")) });
    // Observe the same terminal error twice, sequentially, over the real
    // spawn()-produced core.
    let first = get_tokio_runtime().block_on(handle.wait_completion());
    let second = get_tokio_runtime().block_on(handle.wait_completion());
    monarch_with_gil_blocking(GilSite::Test, |py| {
        for (label, res) in [("first", first), ("second", second)] {
            let err = res.expect_err("wait_completion should surface the producer error");
            assert!(
                err.is_instance_of::<PyValueError>(py),
                "{label}: expected PyValueError"
            );
            let msg = err.value(py).to_string();
            assert!(
                msg.contains("boom"),
                "{label}: cached error message should be preserved, got {msg}"
            );
        }
    });
}

#[derive(Debug, PartialEq)]
struct SentinelErr(&'static str);

/// After readiness resolves `Ok`, the operation closure runs exactly once
/// and its result is returned.
#[tokio::test]
async fn after_ready_runs_operation_once_on_success() {
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    let out: Result<u8, SentinelErr> =
        after_ready(async { Ok::<(), SentinelErr>(()) }, move || {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(7u8)
            }
        })
        .await;
    assert_eq!(out, Ok(7));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

/// A readiness error is returned unchanged and the operation closure is
/// never invoked.
#[tokio::test]
async fn after_ready_short_circuits_on_readiness_error() {
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    let out: Result<u8, SentinelErr> = after_ready(
        async { Err::<(), SentinelErr>(SentinelErr("boom")) },
        move || {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(7u8)
            }
        },
    )
    .await;
    assert_eq!(out, Err(SentinelErr("boom")));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

/// While readiness is pending the composed future cannot complete and the
/// operation closure is never invoked.
#[test]
fn after_ready_does_not_run_operation_while_pending() {
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    let pending_ready = std::future::pending::<Result<(), SentinelErr>>();
    let mut fut = Box::pin(after_ready(pending_ready, move || {
        let c = c.clone();
        async move {
            c.fetch_add(1, Ordering::SeqCst);
            Ok(7u8)
        }
    }));
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(fut.as_mut().poll(&mut cx), Poll::Pending);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
