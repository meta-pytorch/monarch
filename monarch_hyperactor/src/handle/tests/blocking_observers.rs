/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyTimeoutError;
use pyo3::exceptions::PyValueError;
use pyo3::types::PyTuple;

use super::*;
use crate::runtime::GilSite;
use crate::runtime::ensure_python;
use crate::runtime::get_tokio_runtime;
use crate::runtime::monarch_with_gil;
use crate::runtime::monarch_with_gil_blocking;

// A ready value is returned via the poll fast path.
// Attests HDL-2.
#[test]
fn get_returns_ready_value() {
    ensure_python();
    let got = monarch_with_gil_blocking(GilSite::Test, |py| -> i64 {
        let value = 42i64.into_py_any(py).unwrap();
        let handle = PyHandle::from_value(value);
        let r = Py::new(py, handle).unwrap();
        let out = PyHandle::get(r.borrow(py), py, None).unwrap();
        out.extract::<i64>(py).unwrap()
    });
    assert_eq!(got, 42, "get() should return the resolved value");
}

// A pending handle blocks get() on a sync thread until a producer completes.
// Attests HDL-1, HDL-2.
#[test]
fn get_blocks_then_returns() {
    ensure_python();
    let value = monarch_with_gil_blocking(GilSite::Test, |py| 7i64.into_py_any(py).unwrap());
    let (tx, handle) = pending_handle();
    get_tokio_runtime().spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        tx.publish(Ok(value));
    });
    let got = monarch_with_gil_blocking(GilSite::Test, |py| -> i64 {
        let r = Py::new(py, handle).unwrap();
        let out = PyHandle::get(r.borrow(py), py, None).unwrap();
        out.extract::<i64>(py).unwrap()
    });
    assert_eq!(got, 7, "get() should block then return the produced value");
}

// poll() transitions from None to the value once the producer completes.
// Attests HDL-1, HDL-2.
#[test]
fn poll_transitions_pending_to_ready() {
    ensure_python();
    let (tx, handle) = pending_handle();
    monarch_with_gil_blocking(GilSite::Test, |py| {
        assert!(
            handle.poll(py).unwrap().is_none(),
            "poll() should be None while pending"
        );
        tx.publish(Ok(1i64.into_py_any(py).unwrap()));
        let got = handle.poll(py).unwrap().unwrap();
        assert_eq!(
            got.extract::<i64>(py).unwrap(),
            1,
            "poll() should observe the produced value"
        );
    });
}

// poll() is non-consuming: after it returns ready, get() and poll() still
// observe the same value.
// Attests HDL-1, HDL-3.
#[test]
fn repeated_observation_after_poll() {
    ensure_python();
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let value = 5i64.into_py_any(py).unwrap();
        let handle = PyHandle::from_value(value);
        let r = Py::new(py, handle).unwrap();

        let first = r.borrow(py).poll(py).unwrap().unwrap();
        assert_eq!(first.extract::<i64>(py).unwrap(), 5);

        let via_get = PyHandle::get(r.borrow(py), py, None).unwrap();
        assert_eq!(via_get.extract::<i64>(py).unwrap(), 5);

        let again = r.borrow(py).poll(py).unwrap().unwrap();
        assert_eq!(again.extract::<i64>(py).unwrap(), 5);
    });
}

// Two independent observers of one still-pending core both resolve;
// waiting never consumes the outcome.
// Attests HDL-1, HDL-3.
#[test]
fn multiple_observers_resolve() {
    ensure_python();
    let (tx, handle) = pending_handle();
    let f1 = handle.wait_future();
    let f2 = handle.wait_future();
    let value = monarch_with_gil_blocking(GilSite::Test, |py| 3i64.into_py_any(py).unwrap());
    tx.publish(Ok(value));
    let (r1, r2) = get_tokio_runtime().block_on(async move { tokio::join!(f1, f2) });
    monarch_with_gil_blocking(GilSite::Test, |py| {
        assert_eq!(r1.unwrap().extract::<i64>(py).unwrap(), 3);
        assert_eq!(r2.unwrap().extract::<i64>(py).unwrap(), 3);
    });
}

// A still-pending get() in a Tokio runtime context raises WouldBlockRuntime.
// Attests HDL-6.
#[test]
fn get_pending_in_tokio_raises_would_block() {
    ensure_python();
    let (_tx, handle) = pending_handle();
    get_tokio_runtime().block_on(async {
        monarch_with_gil(GilSite::Test, |py| {
            let r = Py::new(py, handle).unwrap();
            let err = PyHandle::get(r.borrow(py), py, None).unwrap_err();
            assert!(
                err.is_instance_of::<WouldBlockRuntime>(py),
                "pending get() in a tokio context should raise WouldBlockRuntime"
            );
        })
        .await
    });
}

// A ready value in a Tokio runtime context still raises: get() is refused by
// context, not by whether the value happens to be available (HDL-6).
// Attests HDL-6.
#[test]
fn get_ready_in_tokio_also_raises() {
    ensure_python();
    get_tokio_runtime().block_on(async {
        monarch_with_gil(GilSite::Test, |py| {
            let value = 9i64.into_py_any(py).unwrap();
            let handle = PyHandle::from_value(value);
            let r = Py::new(py, handle).unwrap();
            let err = PyHandle::get(r.borrow(py), py, None).unwrap_err();
            assert!(
                err.is_instance_of::<WouldBlockRuntime>(py),
                "get() in a tokio context must raise even for a ready value"
            );
        })
        .await
    });
}

// get(timeout) raises TimeoutError and leaves the handle pending, so a later
// observation still sees completion.
// Attests HDL-12.
#[test]
fn get_timeout_raises_and_leaves_pending() {
    ensure_python();
    let (tx, handle) = pending_handle();
    let r = monarch_with_gil_blocking(GilSite::Test, |py| Py::new(py, handle).unwrap());

    let is_timeout = monarch_with_gil_blocking(GilSite::Test, |py| {
        let err = PyHandle::get(r.borrow(py), py, Some(0.05)).unwrap_err();
        err.is_instance_of::<PyTimeoutError>(py)
    });
    assert!(is_timeout, "get(timeout) should raise TimeoutError");

    let still_pending =
        monarch_with_gil_blocking(GilSite::Test, |py| r.borrow(py).poll(py).unwrap().is_none());
    assert!(
        still_pending,
        "a timed-out get() must not resolve the handle"
    );

    let value = monarch_with_gil_blocking(GilSite::Test, |py| 4i64.into_py_any(py).unwrap());
    tx.publish(Ok(value));
    let observed = monarch_with_gil_blocking(GilSite::Test, |py| {
        r.borrow(py)
            .poll(py)
            .unwrap()
            .unwrap()
            .extract::<i64>(py)
            .unwrap()
    });
    assert_eq!(
        observed, 4,
        "completion is still observable after a timeout"
    );
}

// A negative, NaN, or non-finite timeout is rejected with ValueError rather
// than panicking in `Duration::from_secs_f64`.
// Attests HDL-12.
#[test]
fn get_invalid_timeout_raises_value_error() {
    ensure_python();
    let (_tx, handle) = pending_handle();
    let r = monarch_with_gil_blocking(GilSite::Test, |py| Py::new(py, handle).unwrap());
    monarch_with_gil_blocking(GilSite::Test, |py| {
        for bad in [-1.0, f64::NAN, f64::INFINITY] {
            let err = PyHandle::get(r.borrow(py), py, Some(bad)).unwrap_err();
            assert!(
                err.is_instance_of::<PyValueError>(py),
                "get(timeout={bad}) should raise ValueError, not panic"
            );
        }
    });
}

// On a running asyncio loop, get() warns (UserWarning) regardless of whether
// the value is ready -- calling the blocking get() from a loop is the
// anti-pattern being flagged. A ready value still returns after warning.
// Attests HDL-8.
#[test]
fn get_on_asyncio_loop_warns() {
    ensure_python();
    let (value, warned) = monarch_with_gil_blocking(GilSite::Test, |py| -> (i64, bool) {
        let v = 5i64.into_py_any(py).unwrap();
        let handle = PyHandle::from_value(v);
        let h = Py::new(py, handle).unwrap();
        let helper = loop_helper(py);
        let res = helper
            .getattr("run_get_on_loop")
            .unwrap()
            .call1((h,))
            .unwrap();
        let tup = res.cast::<PyTuple>().unwrap();
        (
            tup.get_item(0).unwrap().extract::<i64>().unwrap(),
            tup.get_item(1).unwrap().extract::<bool>().unwrap(),
        )
    });
    assert_eq!(value, 5, "a ready value still returns after warning");
    assert!(
        warned,
        "get() on a running asyncio loop warns regardless of readiness"
    );
}

// A producer error surfaces through the direct read paths: poll() returns it
// and get() raises it (the primary sync API path, previously observed only
// through await/as_asyncio).
// Attests HDL-2.
#[test]
fn poll_and_get_surface_producer_error() {
    ensure_python();
    let (tx, handle) = pending_handle();
    tx.publish(Err(PyValueError::new_err("boom")));
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let r = Py::new(py, handle).unwrap();
        let poll_err = r.borrow(py).poll(py).unwrap_err();
        assert!(
            poll_err.is_instance_of::<PyValueError>(py),
            "poll() should surface the producer error"
        );
        let get_err = PyHandle::get(r.borrow(py), py, None).unwrap_err();
        assert!(
            get_err.is_instance_of::<PyValueError>(py),
            "get() should raise the producer error"
        );
    });
}

// A producer already gone when poll() or get() is called surfaces a Python
// error through the ready check rather than being mistaken for pending work.
// Attests HDL-10.
#[test]
fn dropped_producer_surfaces_error() {
    ensure_python();
    let (tx, handle) = pending_handle();
    drop(tx);
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let r = Py::new(py, handle).unwrap();
        let poll_err = r.borrow(py).poll(py).unwrap_err();
        assert_producer_ended_error(py, &poll_err);
        let err = PyHandle::get(r.borrow(py), py, None).unwrap_err();
        assert_producer_ended_error(py, &err);
    });
}

// HDL-10: losing the producer after get() enters its pending branch raises
// the producer-lifecycle error, not TimeoutError.
#[test]
fn get_pending_surfaces_dropped_producer() {
    ensure_python();
    let (producer, result) = monarch_with_gil_blocking(GilSite::Test, |py| {
        let (tx, handle) = pending_handle();
        let handle = Py::new(py, handle).unwrap();
        assert!(handle.borrow(py).poll(py).unwrap().is_none());
        // Load asyncio before starting the producer: importing it can
        // release the GIL, but the ready check must run while we hold it.
        assert!(pyo3_async_runtimes::get_running_loop(py).is_err());

        // This thread cannot drop the publisher until get() releases the GIL.
        // The producer therefore stays pending through get()'s ready check,
        // forcing the pending branch without a scheduling sleep.
        let producer = thread::spawn(move || {
            monarch_with_gil_blocking(GilSite::Test, |_py| drop(tx));
        });
        let result = PyHandle::get(handle.borrow(py), py, Some(60.0));
        (producer, result)
    });
    producer.join().expect("producer thread should finish");
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let error = result.expect_err("get() should report the dropped producer");
        assert_producer_ended_error(py, &error);
    });
}

// A producer that publishes and is then gone: poll() and get() return what
// was published, not producer loss.
// Attests HDL-1, HDL-2, HDL-10.
#[test]
fn poll_and_get_read_an_outcome_published_by_a_departed_producer() {
    ensure_python();
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let publish_then_close = |outcome: PyResult<Py<PyAny>>| {
            let (tx, handle) = pending_handle();
            tx.publish(outcome);
            Py::new(py, handle).unwrap()
        };

        let h = publish_then_close(Ok(42i64.into_py_any(py).unwrap()));
        let polled = h.borrow(py).poll(py).unwrap().expect("the published value");
        assert_eq!(polled.extract::<i64>(py).unwrap(), 42);

        let h = publish_then_close(Err(PyValueError::new_err("boom")));
        let err = h.borrow(py).poll(py).unwrap_err();
        assert!(
            err.is_instance_of::<PyValueError>(py),
            "poll() should surface the stored exception, not producer loss: {err}"
        );

        let h = publish_then_close(Ok(7i64.into_py_any(py).unwrap()));
        let got = PyHandle::get(h.borrow(py), py, None).unwrap();
        assert_eq!(got.extract::<i64>(py).unwrap(), 7);

        let h = publish_then_close(Err(PyValueError::new_err("boom")));
        let err = PyHandle::get(h.borrow(py), py, None).unwrap_err();
        assert!(
            err.is_instance_of::<PyValueError>(py),
            "get() should raise the stored exception, not producer loss: {err}"
        );
    });
}

// get(timeout) whose producer resolves before the deadline returns the value
// (the Ok branch of tokio::time::timeout, distinct from the timeout-fires and
// bad-float cases).
// Attests HDL-2, HDL-12.
#[test]
fn get_timeout_returns_value_before_deadline() {
    ensure_python();
    let value = monarch_with_gil_blocking(GilSite::Test, |py| 13i64.into_py_any(py).unwrap());
    let (tx, handle) = pending_handle();
    get_tokio_runtime().spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        tx.publish(Ok(value));
    });
    let got = monarch_with_gil_blocking(GilSite::Test, |py| -> i64 {
        let r = Py::new(py, handle).unwrap();
        let out = PyHandle::get(r.borrow(py), py, Some(5.0)).unwrap();
        out.extract::<i64>(py).unwrap()
    });
    assert_eq!(got, 13, "a generous timeout returns the produced value");
}

// Under a warnings-as-errors filter, a get() on a running loop escalates the
// anti-pattern warning to an error -- even for a ready value -- rather than
// silently returning. Warn-regardless means the strict caller sees it.
// Attests HDL-8.
#[test]
fn get_on_loop_under_warnings_as_errors_raises() {
    ensure_python();
    let raised = monarch_with_gil_blocking(GilSite::Test, |py| -> bool {
        let v = 5i64.into_py_any(py).unwrap();
        let handle = PyHandle::from_value(v);
        let h = Py::new(py, handle).unwrap();
        let helper = loop_helper(py);
        helper
            .getattr("run_get_on_loop_strict")
            .unwrap()
            .call1((h,))
            .is_err()
    });
    assert!(
        raised,
        "get() on a loop under warnings-as-errors should raise the escalated warning"
    );
}

// The timeout is validated before the ready fast path, so an invalid timeout
// raises ValueError even when the handle is already resolved (where get()
// would otherwise return without blocking).
// Attests HDL-12.
#[test]
fn get_invalid_timeout_on_ready_raises() {
    ensure_python();
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let value = 7i64.into_py_any(py).unwrap();
        let handle = PyHandle::from_value(value);
        let r = Py::new(py, handle).unwrap();
        let err = PyHandle::get(r.borrow(py), py, Some(f64::NAN)).unwrap_err();
        assert!(
            err.is_instance_of::<PyValueError>(py),
            "an invalid timeout must raise ValueError even for a ready handle"
        );
    });
}

// Reading a lazy error normalizes it, which releases and reacquires the
// GIL. A concurrent observer that holds the GIL and observes the same core
// must not deadlock.
// Attests HDL-9.
#[test]
fn lazy_error_read_does_not_deadlock_a_concurrent_observer() {
    ensure_python();
    for _ in 0..50 {
        let (tx, handle) = pending_handle();
        tx.publish(Err(PyValueError::new_err("lazy")));
        let (reader_handle, poller_handle) = monarch_with_gil_blocking(GilSite::Test, |py| {
            let handle = Py::new(py, handle).unwrap();
            (handle.clone_ref(py), handle)
        });
        let (done_tx, done_rx) = mpsc::channel();
        let reader_done = done_tx.clone();
        let reader = thread::spawn(move || {
            monarch_with_gil_blocking(GilSite::Test, |py| {
                let _ = PyHandle::get(reader_handle.borrow(py), py, None);
            });
            reader_done.send(()).unwrap();
        });
        let poller = thread::spawn(move || {
            monarch_with_gil_blocking(GilSite::Test, |py| {
                for _ in 0..100 {
                    let _ = poller_handle.borrow(py).poll(py);
                }
            });
            done_tx.send(()).unwrap();
        });
        for _ in 0..2 {
            done_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("neither observer may deadlock");
        }
        reader.join().unwrap();
        poller.join().unwrap();
    }
}
