/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;

use super::*;
use crate::handle::core::AFTER_REGISTER;
use crate::handle::core::BEFORE_NOTIFY;
use crate::handle::core::Core;
use crate::handle::core::Observed;
use crate::handle::core::STATE_LOCK_COUNT;
use crate::handle::core::State;
use crate::handle::core::wait_terminal;
use crate::handle::produce::_new_handle_pair;
use crate::runtime::GilSite;
use crate::runtime::ensure_python;
use crate::runtime::get_tokio_runtime;
use crate::runtime::monarch_with_gil;
use crate::runtime::monarch_with_gil_blocking;

// HDL-25: the raw feature adds no lock probe to ready get/poll, a running
// get's check/register/recheck, or an inline deferred receive.
#[test]
fn non_raw_observations_keep_the_state_lock_budget() {
    ensure_python();
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let handle = Py::new(py, PyHandle::from_value(py.None())).expect("ready Handle");
        for operation in ["get", "poll"] {
            STATE_LOCK_COUNT.with(|count| count.set(0));
            handle
                .call_method0(py, operation)
                .expect("ready observation");
            assert_eq!(
                STATE_LOCK_COUNT.with(|count| count.get()),
                1,
                "{operation} has one check"
            );
        }
        let (publisher, handle) = pending_handle();
        let handle = Py::new(py, handle).expect("pending Handle");
        AFTER_REGISTER.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                let (tx, rx) = mpsc::channel();
                let thread = thread::spawn(move || {
                    publisher.publish(Err(PyValueError::new_err("budget failure")));
                    tx.send(()).expect("publisher acknowledges");
                });
                rx.recv_timeout(Duration::from_secs(5))
                    .expect("publication completes");
                thread.join().expect("publisher exits");
            }))
        });
        STATE_LOCK_COUNT.with(|count| count.set(0));
        handle.call_method0(py, "get").expect_err("stored failure");
        assert_eq!(
            STATE_LOCK_COUNT.with(|count| count.get()),
            3,
            "check, register, recheck"
        );
        let handle = Py::new(
            py,
            PyHandle::deferred(
                py,
                async { ReceiveOutcome::Outcome(Err(PyValueError::new_err("inline failure"))) },
                None,
                ObserverLoss::Continue,
            ),
        )
        .expect("deferred Handle");
        STATE_LOCK_COUNT.with(|count| count.set(0));
        handle.call_method0(py, "get").expect_err("inline failure");
        assert_eq!(
            STATE_LOCK_COUNT.with(|count| count.get()),
            2,
            "claim and publication, no wait"
        );
    });
}

// HDL-23: losing the last observer stops a started stop-when-unobserved
// producer. The drop acknowledgement proves destruction, not merely that a
// stop was requested.
#[tokio::test]
async fn stop_when_unobserved_ends_when_last_observer_drops() {
    ensure_python();
    struct AcknowledgeDrop(Option<tokio::sync::oneshot::Sender<()>>);
    impl Drop for AcknowledgeDrop {
        fn drop(&mut self) {
            let _ = self.0.take().expect("only Drop takes the sender").send(());
        }
    }
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
    let guard = AcknowledgeDrop(Some(dropped_tx));
    let handle = monarch_with_gil_blocking(GilSite::Test, |py| {
        let handle = PyHandle::deferred(
            py,
            async move {
                let _guard = guard;
                started_tx
                    .send(())
                    .expect("the test waits for the first poll");
                std::future::pending::<ReceiveOutcome>().await
            },
            None,
            ObserverLoss::Stop,
        );
        // HDL-24: an observation starts it.
        assert!(handle.poll(py).unwrap().is_none());
        handle
    });
    tokio::time::timeout(Duration::from_secs(30), started_rx)
        .await
        .expect("the started producer should be polled")
        .expect("the producer should acknowledge its first poll");
    drop(handle);
    tokio::time::timeout(Duration::from_secs(30), dropped_rx)
        .await
        .expect("loss of the last observer should destroy the producer")
        .expect("the producer's Drop should acknowledge destruction");
}

// HDL-23: a native waiter keeps a stop-when-unobserved producer running
// after its Handle drops. Stopping when the Handle drops would lose this
// waiter's result.
#[tokio::test]
async fn stop_when_unobserved_keeps_running_for_a_waiting_observer() {
    ensure_python();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let handle = monarch_with_gil_blocking(GilSite::Test, |py| {
        PyHandle::deferred(
            py,
            async move { ReceiveOutcome::Outcome(rx.await.expect("the test supplies the value")) },
            None,
            ObserverLoss::Stop,
        )
    });
    let waiter = handle.wait_future();
    drop(handle);
    let value = monarch_with_gil_blocking(GilSite::Test, |py| 37.into_py_any(py))
        .expect("the value should convert");
    tx.send(Ok(value))
        .expect("the producer should still be waiting");
    let ready = tokio::time::timeout(Duration::from_secs(30), waiter)
        .await
        .expect("the native waiter should complete")
        .expect("the producer should publish its value");
    monarch_with_gil_blocking(GilSite::Test, |py| {
        assert_eq!(ready.extract::<i64>(py).unwrap(), 37);
    });
}

// Neither side of the private completion pair can be constructed directly
// from Python -- both pyclasses have no #[new].
// Attests HDL-11.
#[test]
fn handle_and_completer_not_constructible_from_python() {
    ensure_python();
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let handle_type = py.get_type::<PyHandle>();
        let handle_error = handle_type.call0().unwrap_err();
        assert!(
            handle_error.is_instance_of::<pyo3::exceptions::PyTypeError>(py),
            "constructing Handle() from Python should raise TypeError (no __new__)"
        );
        let completer_type = py.get_type::<PyHandleCompleter>();
        let completer_error = completer_type.call0().unwrap_err();
        assert!(
            completer_error.is_instance_of::<pyo3::exceptions::PyTypeError>(py),
            "constructing _HandleCompleter() from Python should raise TypeError (no __new__)"
        );
    });
}

// A pending Handle releases the GIL while blocking, allowing the paired
// producer on another thread to acquire it, publish, and wake the observer.
// The zero-capacity channel ensures the worker has reached the gate
// immediately before its GIL acquisition when observation starts.
// Attests HDL-1, HDL-2, HDL-9, HDL-11.
#[test]
fn python_pair_resolves_pending_handle_across_threads() {
    use std::sync::mpsc::sync_channel;
    use std::time::Duration;

    ensure_python();
    let (worker, observed) = monarch_with_gil_blocking(GilSite::Test, |py| {
        let (handle, completer) = _new_handle_pair();
        let handle = Py::new(py, handle).unwrap();
        let completer = Py::new(py, completer).unwrap();
        let (ready_tx, ready_rx) = sync_channel(0);
        let worker = std::thread::spawn(move || {
            ready_tx.send(()).unwrap();
            monarch_with_gil_blocking(GilSite::Test, |py| {
                completer
                    .borrow_mut(py)
                    .set_result(42i64.into_py_any(py).unwrap())
                    .unwrap();
            });
        });

        ready_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("completion worker should reach its pre-GIL gate");
        assert!(
            PyHandle::poll(&handle.borrow(py), py).unwrap().is_none(),
            "the Handle should still be pending before the worker acquires the GIL"
        );
        let value = PyHandle::get(handle.borrow(py), py, Some(2.0))
            .unwrap()
            .extract::<i64>(py)
            .unwrap();
        (worker, value)
    });

    worker.join().expect("completion worker should exit");
    assert_eq!(
        observed, 42,
        "the pending observer should receive the value"
    );
}

// Failure publication follows the same pending cross-thread path and wakes
// the observer with the original Python error.
// Attests HDL-1, HDL-2, HDL-9, HDL-11.
#[test]
fn python_pair_publishes_exception_to_pending_handle_across_threads() {
    use std::sync::mpsc::sync_channel;
    use std::time::Duration;

    ensure_python();
    let worker = monarch_with_gil_blocking(GilSite::Test, |py| {
        let (handle, completer) = _new_handle_pair();
        let handle = Py::new(py, handle).unwrap();
        let completer = Py::new(py, completer).unwrap();
        let (ready_tx, ready_rx) = sync_channel(0);
        let worker = std::thread::spawn(move || {
            ready_tx.send(()).unwrap();
            monarch_with_gil_blocking(GilSite::Test, |py| {
                let error = PyRuntimeError::new_err("cross-thread failure")
                    .into_value(py)
                    .into_bound(py)
                    .into_any();
                completer.borrow_mut(py).set_exception(error).unwrap();
            });
        });

        ready_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("completion worker should reach its pre-GIL gate");
        assert!(
            PyHandle::poll(&handle.borrow(py), py).unwrap().is_none(),
            "the Handle should still be pending before the worker acquires the GIL"
        );
        let error = PyHandle::get(handle.borrow(py), py, Some(2.0)).unwrap_err();
        assert!(
            error.is_instance_of::<PyRuntimeError>(py),
            "the pending observer should receive the worker's exception"
        );
        assert_eq!(error.to_string(), "RuntimeError: cross-thread failure");
        worker
    });

    worker.join().expect("completion worker should exit");
}

// A ready read and an inline drive create no wake; waiters share one.
// Attests HDL-25.
#[test]
fn wake_exists_only_while_an_observer_waits() {
    ensure_python();
    let ready = monarch_with_gil_blocking(GilSite::Test, |py| PyHandle::from_value(py.None()));
    monarch_with_gil_blocking(GilSite::Test, |py| {
        assert!(
            ready.poll(py).unwrap().is_some(),
            "the ready Handle reads at once"
        );
    });
    assert!(matches!(*ready.core.lock(), State::Done(_)));

    let saw_wake = Arc::new(AtomicBool::new(true));
    let slot: Arc<std::sync::Mutex<Option<Weak<Core>>>> = Arc::default();
    let driven = monarch_with_gil_blocking(GilSite::Test, |py| {
        let (saw_wake, producer_slot) = (saw_wake.clone(), slot.clone());
        let handle = PyHandle::deferred(
            py,
            async move {
                let core = producer_slot
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .upgrade()
                    .unwrap();
                let has_wake = matches!(*core.lock(), State::Running { wake: Some(_), .. });
                saw_wake.store(has_wake, Ordering::SeqCst);
                drop(core);
                ReceiveOutcome::Outcome(
                    monarch_with_gil(GilSite::Test, |py| 1i64.into_py_any(py)).await,
                )
            },
            None,
            ObserverLoss::Continue,
        );
        *slot.lock().unwrap() = Some(Arc::downgrade(&handle.core));
        Py::new(py, handle).unwrap()
    });
    let reader = monarch_with_gil_blocking(GilSite::Test, |py| driven.clone_ref(py));
    thread::spawn(move || {
        monarch_with_gil_blocking(GilSite::Test, |py| {
            PyHandle::get(reader.borrow(py), py, None).unwrap();
        });
    })
    .join()
    .unwrap();
    assert!(
        !saw_wake.load(Ordering::SeqCst),
        "an inline drive with no waiter should create no wake"
    );

    let (tx, handle) = pending_handle();
    let waiters: Vec<_> = (0..2)
        .map(|_| get_tokio_runtime().spawn(handle.wait_completion()))
        .collect();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let registered = match &*handle.core.lock() {
            State::Running {
                wake: Some(wake), ..
            } => Arc::strong_count(wake) - 1,
            _ => 0,
        };
        if registered == 2 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "both waiters should register one shared wake"
        );
        thread::sleep(Duration::from_millis(1));
    }
    tx.publish(Ok(monarch_with_gil_blocking(GilSite::Test, |py| py.None())));
    get_tokio_runtime().block_on(async {
        for waiter in waiters {
            waiter.await.unwrap().unwrap();
        }
    });
}

// A publish between a waiter's registration and its first poll still wakes
// it: its `OwnedNotified` existed before the notify.
// Attests HDL-25.
#[test]
fn publish_between_registration_and_await_wakes_the_waiter() {
    ensure_python();
    let (tx, handle) = pending_handle();
    let value = monarch_with_gil_blocking(GilSite::Test, |py| 9i64.into_py_any(py).unwrap());
    AFTER_REGISTER.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || tx.publish(Ok(value))));
    });
    let outcome = get_tokio_runtime()
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(5), wait_terminal(handle.core.clone())).await
        })
        .expect("the registered waiter should wake")
        .expect("the producer published a value");
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let value = outcome.as_ref().as_ref().unwrap();
        assert_eq!(value.extract::<i64>(py).unwrap(), 9);
    });
}

// Between the publisher's transition and its notify, a new observer sees
// the outcome without waiting.
// Attests HDL-25.
#[test]
fn observer_between_transition_and_notify_sees_the_outcome() {
    ensure_python();
    let (tx, handle) = pending_handle();
    let core = handle.core.clone();
    let seen = Arc::new(AtomicBool::new(false));
    let seen_in_hook = seen.clone();
    BEFORE_NOTIFY.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            let done = matches!(core.observe(), Observed::Done(_));
            seen_in_hook.store(done, Ordering::SeqCst);
        }));
    });
    tx.publish(Ok(monarch_with_gil_blocking(GilSite::Test, |py| py.None())));
    assert!(
        seen.load(Ordering::SeqCst),
        "a new observer should see the outcome before the notify"
    );
    drop(handle);
}

// Many waiters racing one publisher all wake.
// Attests HDL-25.
#[test]
fn many_waiters_racing_a_publisher_all_wake() {
    ensure_python();
    for _ in 0..200 {
        let (tx, handle) = pending_handle();
        let waiters: Vec<_> = (0..8)
            .map(|_| get_tokio_runtime().spawn(handle.wait_completion()))
            .collect();
        let value = monarch_with_gil_blocking(GilSite::Test, |py| py.None());
        get_tokio_runtime().spawn(async move {
            tokio::task::yield_now().await;
            tx.publish(Ok(value));
        });
        get_tokio_runtime().block_on(async {
            for waiter in waiters {
                tokio::time::timeout(Duration::from_secs(5), waiter)
                    .await
                    .expect("every waiter should wake")
                    .expect("the waiter task should not panic")
                    .expect("the outcome is a value");
            }
        });
    }
}

// A producer task that panics publishes producer loss.
// Attests HDL-10.
#[test]
fn panicking_producer_surfaces_producer_loss() {
    ensure_python();
    let value = monarch_with_gil_blocking(GilSite::Test, |py| py.None());
    let panics = true;
    let handle = PyHandle::spawn_pyobject(async move {
        if panics {
            panic!("the producer panics on purpose");
        }
        Ok(value)
    });
    let err = get_tokio_runtime()
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(5), handle.wait_future()).await
        })
        .expect("the waiter should not hang")
        .unwrap_err();
    monarch_with_gil_blocking(GilSite::Test, |py| assert_producer_ended_error(py, &err));
}
