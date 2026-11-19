/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyKeyboardInterrupt;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyTimeoutError;
use pyo3::exceptions::PyValueError;
use pyo3::types::PyList;
use pyo3::types::PyModule;

use super::*;
use crate::runtime::GilSite;
use crate::runtime::ensure_python;
use crate::runtime::get_tokio_runtime;
use crate::runtime::is_main_thread;
use crate::runtime::monarch_with_gil;
use crate::runtime::monarch_with_gil_blocking;

// HDL-4, HDL-24: a deferred producer that nobody observes is dropped with
// its last Handle, never polled.
#[tokio::test]
async fn unobserved_deferred_producer_is_dropped_unpolled() {
    ensure_python();
    struct AcknowledgeDrop(Option<tokio::sync::oneshot::Sender<()>>);
    impl Drop for AcknowledgeDrop {
        fn drop(&mut self) {
            let _ = self.0.take().expect("only Drop takes the sender").send(());
        }
    }
    let polled = Arc::new(AtomicBool::new(false));
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
    let guard = AcknowledgeDrop(Some(dropped_tx));
    let handle = monarch_with_gil_blocking(GilSite::Test, |py| {
        let polled = polled.clone();
        PyHandle::deferred(
            py,
            async move {
                let _guard = guard;
                polled.store(true, Ordering::SeqCst);
                std::future::pending::<ReceiveOutcome>().await
            },
            None,
            ObserverLoss::Stop,
        )
    });
    drop(handle);
    tokio::time::timeout(Duration::from_secs(30), dropped_rx)
        .await
        .expect("the unobserved producer should be dropped with its Handle")
        .expect("the producer's Drop should acknowledge destruction");
    assert!(
        !polled.load(Ordering::SeqCst),
        "an unobserved deferred producer must never be polled"
    );
}

// A clone observed first starts a deferred producer, once, and the
// original reads the same object.
// Attests HDL-23, HDL-24.
#[test]
fn clone_observed_first_starts_the_producer_once() {
    ensure_python();
    let polls = Arc::new(AtomicUsize::new(0));
    let handle = monarch_with_gil_blocking(GilSite::Test, |py| {
        let polls = polls.clone();
        let value = PyList::empty(py).into_any().unbind();
        PyHandle::deferred(
            py,
            async move {
                polls.fetch_add(1, Ordering::SeqCst);
                ReceiveOutcome::Outcome(Ok(value))
            },
            None,
            ObserverLoss::Continue,
        )
    });
    let clone = handle.clone_ref();
    let from_clone = get_tokio_runtime().block_on(clone.wait_future()).unwrap();
    let from_original = get_tokio_runtime().block_on(handle.wait_future()).unwrap();
    assert_eq!(
        polls.load(Ordering::SeqCst),
        1,
        "the producer should start once"
    );
    monarch_with_gil_blocking(GilSite::Test, |py| {
        assert!(from_clone.bind(py).is(from_original.bind(py)));
    });
}

// Each rejected observation leaves a deferred producer unstarted; a later
// valid observation starts it once.
// Attests HDL-6, HDL-8, HDL-12, HDL-24.
#[test]
fn rejected_observations_start_nothing() {
    ensure_python();
    let polls = Arc::new(AtomicUsize::new(0));
    let handle = monarch_with_gil_blocking(GilSite::Test, |py| {
        let polls = polls.clone();
        let deferred = PyHandle::deferred(
            py,
            async move {
                polls.fetch_add(1, Ordering::SeqCst);
                ReceiveOutcome::Outcome(
                    monarch_with_gil(GilSite::Test, |py| 5i64.into_py_any(py)).await,
                )
            },
            None,
            ObserverLoss::Continue,
        );
        Py::new(py, deferred).unwrap()
    });
    let in_tokio = monarch_with_gil_blocking(GilSite::Test, |py| handle.clone_ref(py));
    get_tokio_runtime().block_on(async move {
        monarch_with_gil(GilSite::Test, move |py| {
            let err = PyHandle::get(in_tokio.borrow(py), py, None).unwrap_err();
            assert!(err.is_instance_of::<WouldBlockRuntime>(py));
        })
        .await
    });
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let err = PyHandle::get(handle.borrow(py), py, Some(-1.0)).unwrap_err();
        assert!(err.is_instance_of::<PyValueError>(py));
        let err = PyHandle::as_asyncio(handle.borrow(py), py).unwrap_err();
        assert!(err.is_instance_of::<PyRuntimeError>(py));
        let strict = loop_helper(py).getattr("run_get_on_loop_strict").unwrap();
        assert!(
            strict.call1((handle.clone_ref(py),)).is_err(),
            "a warning raised as an error rejects the observation"
        );
    });
    thread::sleep(Duration::from_millis(50));
    assert_eq!(
        polls.load(Ordering::SeqCst),
        0,
        "no rejected observation may start the producer"
    );
    let wait = monarch_with_gil_blocking(GilSite::Test, |py| handle.borrow(py).wait_future());
    let value = get_tokio_runtime().block_on(wait).unwrap();
    monarch_with_gil_blocking(GilSite::Test, |py| {
        assert_eq!(value.extract::<i64>(py).unwrap(), 5);
    });
    assert_eq!(
        polls.load(Ordering::SeqCst),
        1,
        "a valid observation starts it once"
    );
}

// During an inline drive, a second get() on another thread waits for the
// shared result instead of failing.
// Attests HDL-3, HDL-24.
#[test]
fn second_reader_waits_for_an_inline_drive() {
    ensure_python();
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
    let driver: Arc<std::sync::Mutex<Option<thread::ThreadId>>> = Arc::default();
    let handle = monarch_with_gil_blocking(GilSite::Test, |py| {
        let driver = driver.clone();
        let value = PyList::empty(py).into_any().unbind();
        let deferred = PyHandle::deferred(
            py,
            async move {
                *driver.lock().unwrap() = Some(thread::current().id());
                gate_rx.await.expect("the test opens the gate");
                ReceiveOutcome::Outcome(Ok(value))
            },
            None,
            ObserverLoss::Continue,
        );
        Py::new(py, deferred).unwrap()
    });
    let first_handle = monarch_with_gil_blocking(GilSite::Test, |py| handle.clone_ref(py));
    let first = thread::spawn(move || {
        let value = monarch_with_gil_blocking(GilSite::Test, |py| {
            PyHandle::get(first_handle.borrow(py), py, None).unwrap()
        });
        (thread::current().id(), value)
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while driver.lock().unwrap().is_none() {
        assert!(
            Instant::now() < deadline,
            "the first get() should start driving"
        );
        thread::sleep(Duration::from_millis(1));
    }
    let second_handle = monarch_with_gil_blocking(GilSite::Test, |py| handle.clone_ref(py));
    let second = thread::spawn(move || {
        monarch_with_gil_blocking(GilSite::Test, |py| {
            PyHandle::get(second_handle.borrow(py), py, Some(10.0)).unwrap()
        })
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while monarch_with_gil_blocking(GilSite::Test, |py| {
        handle.borrow(py).observer_count_for_test()
    }) < 3
    {
        assert!(Instant::now() < deadline, "the second get() should wait");
        thread::sleep(Duration::from_millis(1));
    }
    gate_tx.send(()).unwrap();
    let (first_thread, first_value) = first.join().unwrap();
    let second_value = second.join().unwrap();
    assert_eq!(
        *driver.lock().unwrap(),
        Some(first_thread),
        "the first get() drives the producer on its own thread"
    );
    monarch_with_gil_blocking(GilSite::Test, |py| {
        assert!(first_value.bind(py).is(second_value.bind(py)));
    });
}

// A Python finalizer that observes the Handle while its producer runs
// neither deadlocks nor panics: no Python runs under the state lock.
// Attests HDL-9.
#[test]
fn finalizer_observing_the_handle_during_a_drive_does_not_deadlock() {
    ensure_python();
    let slot: Arc<std::sync::Mutex<Option<Py<PyAny>>>> = Arc::default();
    let (handle, seen) = monarch_with_gil_blocking(GilSite::Test, |py| {
        let module = PyModule::from_code(
            py,
            cr#"
class PollOnDel:
    def __init__(self, handle, seen):
        self.handle = handle
        self.seen = seen

    def __del__(self):
        self.seen.append(self.handle.poll())
"#,
            c"poll_on_del.py",
            c"poll_on_del",
        )
        .unwrap();
        let in_producer = slot.clone();
        let deferred = PyHandle::deferred(
            py,
            async move {
                ReceiveOutcome::Outcome(
                    monarch_with_gil(GilSite::Test, move |py| {
                        // Run the finalizer while the producer is still running.
                        drop(in_producer.lock().unwrap().take());
                        3i64.into_py_any(py)
                    })
                    .await,
                )
            },
            None,
            ObserverLoss::Continue,
        );
        let handle = Py::new(py, deferred).unwrap();
        let seen = PyList::empty(py).unbind();
        let observer = module
            .getattr("PollOnDel")
            .unwrap()
            .call1((handle.clone_ref(py), seen.clone_ref(py)))
            .unwrap()
            .unbind();
        *slot.lock().unwrap() = Some(observer);
        (handle, seen)
    });
    let reader = monarch_with_gil_blocking(GilSite::Test, |py| handle.clone_ref(py));
    let (done_tx, done_rx) = mpsc::channel();
    thread::spawn(move || {
        let value = monarch_with_gil_blocking(GilSite::Test, |py| {
            let value = PyHandle::get(reader.borrow(py), py, None).unwrap();
            value.extract::<i64>(py).unwrap()
        });
        done_tx.send(value).unwrap();
    });
    let value = done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the finalizer must not deadlock the drive");
    assert_eq!(value, 3);
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let seen = seen.bind(py);
        assert_eq!(seen.len(), 1, "the finalizer ran once");
        assert!(
            seen.get_item(0).unwrap().is_none(),
            "the finalizer observed the producer still running"
        );
    });
}

// A timed get() that times out while driving hands the producer to Tokio:
// the producer finishes there and a later get() reads its result.
// Attests HDL-12, HDL-24.
#[test]
fn timed_out_drive_hands_off_and_its_result_survives() {
    ensure_python();
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
    let handle = monarch_with_gil_blocking(GilSite::Test, |py| {
        let value = PyList::empty(py).into_any().unbind();
        let deferred = PyHandle::deferred(
            py,
            async move {
                gate_rx.await.expect("the test opens the gate");
                ReceiveOutcome::Outcome(Ok(value))
            },
            None,
            ObserverLoss::Continue,
        );
        Py::new(py, deferred).unwrap()
    });
    let driver = monarch_with_gil_blocking(GilSite::Test, |py| handle.clone_ref(py));
    let timed_out = thread::spawn(move || {
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let err = PyHandle::get(driver.borrow(py), py, Some(0.05)).unwrap_err();
            err.is_instance_of::<PyTimeoutError>(py)
        })
    })
    .join()
    .unwrap();
    assert!(timed_out, "the driving get() should time out");
    gate_tx.send(()).unwrap();
    let (first, second) = monarch_with_gil_blocking(GilSite::Test, |py| {
        let first = PyHandle::get(handle.borrow(py), py, Some(5.0)).unwrap();
        let second = PyHandle::get(handle.borrow(py), py, None).unwrap();
        (first, second)
    });
    monarch_with_gil_blocking(GilSite::Test, |py| {
        assert!(first.bind(py).is(second.bind(py)));
    });
}

// A stop-when-unobserved producer handed to Tokio by a timed-out get()
// still stops once its last observer drops.
// Attests HDL-23, HDL-24.
#[test]
fn handed_off_producer_stops_when_unobserved() {
    ensure_python();
    struct AcknowledgeDrop(Option<tokio::sync::oneshot::Sender<()>>);
    impl Drop for AcknowledgeDrop {
        fn drop(&mut self) {
            let _ = self.0.take().expect("only Drop takes the sender").send(());
        }
    }
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
    let guard = AcknowledgeDrop(Some(dropped_tx));
    let handle = monarch_with_gil_blocking(GilSite::Test, |py| {
        let deferred = PyHandle::deferred(
            py,
            async move {
                let _guard = guard;
                std::future::pending::<ReceiveOutcome>().await
            },
            None,
            ObserverLoss::Stop,
        );
        Py::new(py, deferred).unwrap()
    });
    let driver = monarch_with_gil_blocking(GilSite::Test, |py| handle.clone_ref(py));
    thread::spawn(move || {
        monarch_with_gil_blocking(GilSite::Test, |py| {
            PyHandle::get(driver.borrow(py), py, Some(0.05)).unwrap_err();
        })
    })
    .join()
    .unwrap();
    monarch_with_gil_blocking(GilSite::Test, |_py| drop(handle));
    get_tokio_runtime()
        .block_on(async { tokio::time::timeout(Duration::from_secs(30), dropped_rx).await })
        .expect("loss of the last observer should destroy the handed-off producer")
        .expect("the producer's Drop should acknowledge destruction");
}

/// A deferred producer whose decoding reports an interrupt, with a
/// finished decoding that yields `value`.
fn interrupted_producer(py: Python<'_>, value: Py<PyAny>) -> PyHandle {
    PyHandle::deferred(
        py,
        async move {
            ReceiveOutcome::Interrupted {
                interrupt: PyKeyboardInterrupt::new_err(()),
                finish: Box::pin(async move { Ok(value) }),
            }
        },
        None,
        ObserverLoss::Continue,
    )
}

// A driving get() whose decoding reports an interrupt raises it, and a
// later get() reads the result the finished decoding publishes.
// Attests HDL-26.
#[test]
fn interrupted_decode_raises_and_its_finished_result_survives() {
    ensure_python();
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let value = PyList::empty(py).into_any().unbind();
        let handle = Py::new(py, interrupted_producer(py, value)).unwrap();
        let err = PyHandle::get(handle.borrow(py), py, None).unwrap_err();
        assert!(
            err.is_instance_of::<PyKeyboardInterrupt>(py),
            "the driving get() should raise the interrupt"
        );
        let first = PyHandle::get(handle.borrow(py), py, Some(5.0)).unwrap();
        let second = PyHandle::get(handle.borrow(py), py, None).unwrap();
        assert!(first.bind(py).is_instance_of::<PyList>());
        assert!(first.bind(py).is(second.bind(py)));
    });
}

// A spawned producer whose decoding reports an interrupt publishes the
// finished decoding's result.
// Attests HDL-26.
#[test]
fn spawned_producer_finishes_an_interrupted_decode() {
    ensure_python();
    let handle = monarch_with_gil_blocking(GilSite::Test, |py| {
        interrupted_producer(py, 7i64.into_py_any(py).unwrap())
    });
    let value = get_tokio_runtime()
        .block_on(handle.wait_future())
        .expect("the finished decoding should publish its value");
    let value = monarch_with_gil_blocking(GilSite::Test, |py| value.extract::<i64>(py));
    assert_eq!(value.unwrap(), 7);
}

// Off Python's main thread no signal handler runs, so even a decoding
// failure that is not an `Exception` is the outcome, and it decodes once.
// Attests HDL-26.
#[test]
fn decode_off_the_main_thread_takes_every_error_as_the_outcome() {
    ensure_python();
    // Under Python 3.12 the first thread to import `threading` becomes
    // its main thread; settle that before starting the thread below.
    is_main_thread();
    let decodes = Arc::new(AtomicUsize::new(0));
    let counted = decodes.clone();
    let outcome_is_the_error = thread::spawn(move || {
        monarch_with_gil_blocking(GilSite::Test, move |py| {
            let produced = ReceiveOutcome::decode(py, GilSite::Test, (), move |_py, ()| {
                counted.fetch_add(1, Ordering::SeqCst);
                Err(PyKeyboardInterrupt::new_err(()))
            });
            matches!(
                produced,
                ReceiveOutcome::Outcome(Err(err)) if err.is_instance_of::<PyKeyboardInterrupt>(py)
            )
        })
    })
    .join()
    .unwrap();
    assert!(outcome_is_the_error, "the error should be the outcome");
    assert_eq!(
        decodes.load(Ordering::SeqCst),
        1,
        "the decoding should run once"
    );
}
