/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyKeyboardInterrupt;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;
use pyo3::types::PyModule;

use super::*;
use crate::handle::core::AFTER_REGISTER;
use crate::handle::core::BEFORE_RECONSTRUCTION_SETTLE;
use crate::handle::core::Observed;
use crate::handle::core::State;
use crate::handle::core::wait_arrival;
use crate::handle::core::wait_terminal;
use crate::handle::outcome::ReconstructionFailurePolicy;
use crate::handle::wake::LoopWake;
use crate::runtime::GilSite;
use crate::runtime::ensure_python;
use crate::runtime::get_tokio_runtime;
use crate::runtime::monarch_with_gil;
use crate::runtime::monarch_with_gil_blocking;

// HDL-1, HDL-3, HDL-27: pending native observers do not claim at construction;
// concurrent polled observers and a clone share exactly one Python object.
#[tokio::test]
async fn raw_reply_has_one_shared_reconstruction() {
    ensure_python();
    let (publisher, handle) = pending_handle();
    let (reply, calls, drops) = test_reply(|py, _| vec![1, 2, 3].into_py_any(py));
    publisher.publish_raw(reply);
    let clone = handle.clone_ref();
    let first = handle.wait_future();
    let second = clone.wait_future();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "unpolled Rust observers must not reconstruct"
    );
    let (first, second) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(first, second)
    })
    .await
    .expect("both raw observers must complete");
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let first = first.expect("first reconstruction succeeds");
        let second = second.expect("second observer succeeds");
        let third = clone
            .poll(py)
            .expect("clone polls")
            .expect("value is ready");
        assert!(
            first.is(&second) && first.is(&third),
            "observers must share one Python object"
        );
    });
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "Done retires the raw payload"
    );
}

// HDL-2, HDL-27: raw arrival is not readiness, including _after prerequisites.
#[tokio::test]
async fn raw_reconstruction_error_fails_readiness_and_after() {
    ensure_python();
    let (publisher, handle) = pending_handle();
    let (reply, calls, _) = test_reply(|_, _| Err(PyValueError::new_err("unpickle failed")));
    publisher.publish_raw(reply);
    let readiness = handle.wait_completion();
    let result = monarch_with_gil_blocking(GilSite::Test, |py| PyHandle::from_value(py.None()));
    let after = PyHandle::spawn_pyobject(after_ready(readiness, move || result.wait_future()));
    let error = tokio::time::timeout(Duration::from_secs(5), after.wait_future())
        .await
        .expect("_after must settle")
        .expect_err("raw is not successful readiness");
    let again = handle
        .wait_future()
        .await
        .expect_err("stored reconstruction error is shared");
    monarch_with_gil_blocking(GilSite::Test, |py| {
        assert!(
            error.value(py).is(again.value(py)),
            "_after and the original share the error"
        );
        let polled = handle.poll(py).expect_err("poll sees the same error");
        assert!(error.value(py).is(polled.value(py)));
    });
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

// HDL-27: direct interrupts/unwinds release the claim, not the original bytes.
#[test]
fn raw_observer_interrupt_and_unwind_leave_a_retry() {
    ensure_python();
    for unwind in [false, true] {
        let (publisher, handle) = pending_handle();
        let (reply, calls, _) = test_reply(move |py, attempt| {
            if attempt == 0 {
                if unwind {
                    panic!("observer unwind");
                }
                return Err(PyKeyboardInterrupt::new_err("observer interrupted"));
            }
            42.into_py_any(py)
        });
        publisher.publish_raw(reply);
        let attempt = catch_unwind(AssertUnwindSafe(|| {
            monarch_with_gil_blocking(GilSite::Test, |py| handle.poll(py))
        }));
        if unwind {
            assert!(attempt.is_err());
        } else {
            assert!(attempt.expect("not an unwind").is_err());
        }
        assert!(
            matches!(&*handle.core.lock(), State::Raw(raw) if !raw.claimed),
            "the claim is released and the reply retained"
        );
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let value = handle
                .poll(py)
                .expect("retry succeeds")
                .expect("reply is retained");
            assert_eq!(value.extract::<i64>(py).expect("integer reply"), 42);
        });
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}

// HDL-25, HDL-27: register before the first notification poll, then perform
// each transition. The hook acknowledges the required race, not a sleep.
// The fallback's terminal waiter rebuilds the reply; the pipe's arrival
// waiter returns without claiming it.
#[test]
fn raw_arrival_release_and_settlement_wake_registered_waiters() {
    ensure_python();
    for arrival_only in [false, true] {
        for transition in ["arrival", "release", "settle"] {
            let (publisher, handle) = pending_handle();
            let (reply, calls, _) = test_reply(|py, _| 9.into_py_any(py));
            let change: Box<dyn FnOnce()> = if transition == "arrival" {
                Box::new(move || publisher.publish_raw(reply))
            } else {
                publisher.publish_raw(reply);
                let claim = raw_claim(&handle);
                if transition == "release" {
                    Box::new(move || drop(claim))
                } else {
                    Box::new(move || {
                        monarch_with_gil_blocking(GilSite::Test, |py| {
                            claim
                                .reconstruct(
                                    py,
                                    ReconstructionFailurePolicy::KeepReplyAfterInterrupt,
                                )
                                .expect("settle succeeds");
                        });
                    })
                }
            };
            let acknowledged = Arc::new(AtomicBool::new(false));
            let acknowledgement = Arc::clone(&acknowledged);
            AFTER_REGISTER.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    acknowledgement.store(true, Ordering::SeqCst);
                    change();
                }))
            });
            let core = Arc::clone(&handle.core);
            let rebuilt = get_tokio_runtime()
                .block_on(async {
                    tokio::time::timeout(Duration::from_secs(5), async move {
                        if arrival_only {
                            wait_arrival(core).await;
                            None
                        } else {
                            Some(wait_terminal(core).await)
                        }
                    })
                    .await
                })
                .expect("registered raw waiter must wake");
            assert!(
                acknowledged.load(Ordering::SeqCst),
                "the transition must follow registration"
            );
            if let Some(outcome) = rebuilt {
                assert!(outcome.expect("the reply reconstructs").is_ok());
            }
            let expected = if arrival_only && transition != "settle" {
                assert!(
                    matches!(&*handle.core.lock(), State::Raw(raw) if !raw.claimed),
                    "{transition}: the arrival waiter leaves the bytes unclaimed"
                );
                0
            } else {
                1
            };
            assert_eq!(calls.load(Ordering::SeqCst), expected, "{transition}");
        }
    }
}

// HDL-27: even a caught worker unwind retains the claim until Done.
#[test]
fn worker_failure_keeps_claim_until_terminal_publication() {
    ensure_python();
    for unwind in [false, true] {
        let (publisher, handle) = pending_handle();
        let (reply, calls, _) = test_reply(move |_, _| {
            if unwind {
                panic!("worker unwind");
            }
            Err(PyKeyboardInterrupt::new_err("worker payload failure"))
        });
        publisher.publish_raw(reply);
        let competitor = Arc::clone(&handle.core);
        let acknowledged = Arc::new(AtomicBool::new(false));
        let acknowledgement = Arc::clone(&acknowledged);
        BEFORE_RECONSTRUCTION_SETTLE.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                let (tx, rx) = mpsc::channel();
                let thread = thread::spawn(move || {
                    let pending = matches!(competitor.observe(), Observed::Pending);
                    tx.send(pending).expect("competitor acknowledges");
                });
                assert!(
                    rx.recv_timeout(Duration::from_secs(5))
                        .expect("competitor runs"),
                    "no competitor may claim between worker failure and Done"
                );
                thread.join().expect("acknowledged competitor exits");
                acknowledgement.store(true, Ordering::SeqCst);
            }))
        });
        let outcome = monarch_with_gil_blocking(GilSite::Test, |py| {
            raw_claim(&handle)
                .reconstruct(py, ReconstructionFailurePolicy::StoreEveryFailureInHandle)
                .expect("worker failures must settle, not escape")
        });
        assert!(acknowledged.load(Ordering::SeqCst));
        assert!(outcome.is_err());
        assert!(matches!(&*handle.core.lock(), State::Done(_)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}

// HDL-27: cancelling an observer queued for the async GIL releases its claim.
#[test]
fn cancelled_raw_gil_wait_releases_claim() {
    use futures::FutureExt;

    ensure_python();
    let (publisher, handle) = pending_handle();
    let (reply, calls, _) = test_reply(|py, _| 7.into_py_any(py));
    publisher.publish_raw(reply);
    let claim = raw_claim(&handle);
    let (held_tx, held_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (exited_tx, exited_rx) = mpsc::channel();
    let holder = thread::spawn(move || {
        get_tokio_runtime().block_on(monarch_with_gil(GilSite::Test, move |_py| {
            held_tx.send(()).expect("GIL holder acknowledges");
            release_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("release GIL holder");
        }));
        exited_tx.send(()).expect("holder acknowledges exit");
    });
    held_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("async GIL lock is held");
    let mut waiting = Box::pin(monarch_with_gil(GilSite::ReplyConvert, move |py| {
        claim.reconstruct(py, ReconstructionFailurePolicy::KeepReplyAfterInterrupt)
    }));
    assert!(
        waiting.as_mut().now_or_never().is_none(),
        "observer must be queued for the GIL"
    );
    drop(waiting);
    let released = matches!(handle.core.observe(), Observed::Reconstruct(_));
    release_tx.send(()).expect("release holder");
    exited_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("GIL holder exits");
    holder.join().expect("acknowledged holder exits");
    assert!(released, "cancellation must release the raw claim");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    get_tokio_runtime()
        .block_on(handle.wait_future())
        .expect("another observer reconstructs");
}

// HDL-13, HDL-27: ignored raw success is discarded without running Python.
#[test]
fn ignored_raw_reply_drops_once_without_reconstruction() {
    let (publisher, handle) = pending_handle();
    let (reply, calls, drops) = test_reply(|_, _| panic!("ignored reply must not reconstruct"));
    publisher.publish_raw(reply);
    drop(handle);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

/// Test-only publication gate; it supplies bytes after asyncio registration.
#[pyclass]
struct RawPublication {
    publication: Mutex<Option<(Publisher, Arc<dyn Reconstruct>)>>,
}

#[pymethods]
impl RawPublication {
    fn publish(&self) {
        let (publisher, reply) = self
            .publication
            .lock()
            .expect("test publication lock")
            .take()
            .expect("publish once");
        publisher.publish_raw(reply);
    }

    fn entries(&self, py: Python<'_>, event_loop: &Bound<'_, PyAny>) -> PyResult<usize> {
        Ok(LoopWake::for_loop(py, event_loop)?.map_or(0, |channel| channel.get().entry_count()))
    }
}

// HDL-21, HDL-27: the fallback worker settles every failure before its
// callback, so the loop never faces Raw, and no wake-channel entry exists.
#[test]
fn raw_fallback_worker_failures_settle_asyncio() {
    ensure_python();
    for unwind in [false, true] {
        let (publisher, handle) = pending_handle();
        let (reply, calls, _) = test_reply(move |_, _| {
            if unwind {
                panic!("worker reconstruction unwind");
            }
            Err(PyKeyboardInterrupt::new_err(
                "worker reconstruction failure",
            ))
        });
        let gate = RawPublication {
            publication: Mutex::new(Some((publisher, reply))),
        };
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let helper = PyModule::from_code(
                py,
                cr#"
import asyncio

def run(handle, gate):
    loop = asyncio.new_event_loop()
    def no_reader(*args):
        raise NotImplementedError
    loop.add_reader = no_reader
    async def observe():
        pending = handle.as_asyncio()
        assert not pending.done(), 'the raw publication must follow registration'
        assert gate.entries(loop) == 0
        gate.publish()
        try:
            await asyncio.wait_for(pending, 5)
        except BaseException as error:
            assert pending.done(), 'worker error must settle the asyncio Future'
            return error
        raise AssertionError('worker reconstruction must fail')
    try:
        return loop.run_until_complete(observe())
    finally:
        loop.close()
"#,
                c"raw_worker_test.py",
                c"raw_worker_test",
            )
            .expect("worker test helper");
            let handle = Py::new(py, handle).expect("test Handle");
            let delivered = helper
                .call_method1("run", (&handle, gate))
                .expect("asyncio must return the reconstruction error");
            let error = handle.borrow(py).poll(py).expect_err("shared worker error");
            assert!(
                error.value(py).is(&delivered),
                "worker and later poll share one error"
            );
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            if unwind {
                assert!(error.is_instance_of::<PyRuntimeError>(py));
            } else {
                assert!(error.is_instance_of::<PyKeyboardInterrupt>(py));
            }
        });
    }
}
