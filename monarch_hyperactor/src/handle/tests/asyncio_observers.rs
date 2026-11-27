/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyKeyboardInterrupt;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyStopIteration;
use pyo3::exceptions::PySystemExit;
use pyo3::exceptions::PyTypeError;
use pyo3::exceptions::PyValueError;
use pyo3::types::PyList;
use pyo3::types::PyModule;
use pyo3::types::PyTuple;
use pyo3::types::PyWeakrefReference;

use super::*;
use crate::handle::asyncio::schedule_completion;
use crate::handle::core::State;
use crate::handle::outcome::ReconstructionFailurePolicy;
use crate::handle::outcome::clone_shared_outcome;
use crate::pywaker::Waker;
use crate::runtime::GilSite;
use crate::runtime::ensure_python;
use crate::runtime::get_tokio_runtime;
use crate::runtime::is_in_tokio_runtime;
use crate::runtime::monarch_with_gil;
use crate::runtime::monarch_with_gil_blocking;

/// Loop drivers for the wake reader's raw-reply witnesses.
fn reader_helper(py: Python<'_>) -> Bound<'_, PyModule> {
    PyModule::from_code(
        py,
        cr#"
import asyncio


class ReaderBaseError(BaseException):
    pass


def observe(loop, handles):
    async def start():
        return [handle.as_asyncio() for handle in handles]

    return loop.run_until_complete(start())


def run_once(loop):
    # One iteration: ready callbacks and, if the pipe is readable, the reader.
    loop.call_soon(loop.stop)
    try:
        loop.run_forever()
    except BaseException as error:
        return error
    return None


def run_until_done(loop, futures):
    loop.run_until_complete(asyncio.wait(futures, timeout=5))


def capture_errors(loop):
    errors = []
    loop.set_exception_handler(lambda _loop, context: errors.append(context))
    return errors


def try_observe(loop, handle):
    async def start():
        try:
            handle.as_asyncio()
        except Exception as error:
            return error
        return None

    return loop.run_until_complete(start())


class NoReaderLoop(asyncio.SelectorEventLoop):
    # The call_soon_threadsafe fallback (HDL-21).
    def add_reader(self, fd, callback, *args):
        raise NotImplementedError


class TypeRejectingFuture(asyncio.Future):
    def add_done_callback(self, fn, *, context=None):
        raise TypeError("add_done_callback rejected")


class NoReaderTypeRejectingLoop(NoReaderLoop):
    def create_future(self):
        return TypeRejectingFuture(loop=self)
"#,
        c"loop_reader_test.py",
        c"loop_reader_test",
    )
    .expect("loop reader test helper")
}

fn new_loop(py: Python<'_>) -> Bound<'_, PyAny> {
    py.import("asyncio")
        .expect("asyncio")
        .call_method0("new_event_loop")
        .expect("a new event loop")
}

fn run_once(helper: &Bound<'_, PyModule>, event_loop: &Bound<'_, PyAny>) -> Option<PyErr> {
    let raised = helper
        .call_method1("run_once", (event_loop,))
        .expect("run_once returns what the run raised");
    (!raised.is_none()).then(|| PyErr::from_value(raised))
}

fn is_done(fut: &Bound<'_, PyAny>) -> bool {
    fut.call_method0("done")
        .and_then(|done| done.is_truthy())
        .expect("done() answers")
}

fn int_result(fut: &Bound<'_, PyAny>) -> i64 {
    fut.call_method0("result")
        .and_then(|value| value.extract())
        .expect("an integer result")
}

fn raw_unclaimed(py: Python<'_>, handle: &Py<PyHandle>) -> bool {
    matches!(
        &*handle.borrow(py).core.lock(),
        State::Raw(raw) if !raw.claimed
    )
}

/// Wait until `holders` references to the pipe's write end remain: every
/// other notifier has sent its token, if its channel lives, and exited.
fn wait_for_waker_holders(waker: &Arc<Waker>, holders: usize) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Arc::strong_count(waker) > holders {
        assert!(
            Instant::now() < deadline,
            "the arrival waiters should notify without the GIL, which this thread holds"
        );
        thread::sleep(Duration::from_millis(1));
    }
}

/// Wait, at most five seconds, until `done` holds.
fn wait_until(mut done: impl FnMut() -> bool, message: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !done() {
        assert!(Instant::now() < deadline, "{message}");
        thread::sleep(Duration::from_millis(1));
    }
}

/// The only pending await on `handle`, started on `event_loop`.
fn observe_one<'py>(
    py: Python<'py>,
    helper: &Bound<'py, PyModule>,
    event_loop: &Bound<'py, PyAny>,
    handle: &Py<PyHandle>,
) -> Bound<'py, PyAny> {
    helper
        .call_method1("observe", (event_loop, PyList::new(py, [handle]).unwrap()))
        .and_then(|futures| futures.get_item(0))
        .expect("a pending observer")
}

/// Sends on drop, so a test proves that a producer was destroyed, not only
/// that a stop was requested.
struct AcknowledgeDrop(Option<mpsc::Sender<()>>);

impl Drop for AcknowledgeDrop {
    fn drop(&mut self) {
        let _ = self.0.take().expect("only Drop takes the sender").send(());
    }
}

/// A deferred producer that stops when nobody can observe it, as a port
/// receive does (HDL-23). It reports its first poll on the first receiver and
/// its destruction on the second, and publishes `value` only once the
/// returned sender sends.
fn signalled_receive(
    py: Python<'_>,
    value: i64,
) -> (
    PyHandle,
    mpsc::Receiver<()>,
    mpsc::Receiver<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    let value = value.into_py_any(py).unwrap();
    let (started_tx, started) = mpsc::channel();
    let (dropped_tx, dropped) = mpsc::channel();
    let (go, go_rx) = tokio::sync::oneshot::channel::<()>();
    let guard = AcknowledgeDrop(Some(dropped_tx));
    let handle = PyHandle::deferred(
        py,
        async move {
            let _guard = guard;
            let _ = started_tx.send(());
            match go_rx.await {
                Ok(()) => ReceiveOutcome::Outcome(Ok(value)),
                Err(_) => std::future::pending().await,
            }
        },
        None,
        ObserverLoss::Stop,
    );
    (handle, started, dropped, go)
}

/// Register one pending await per reply, then publish the replies in
/// order, each after the previous token is queued. One reader callback
/// then takes the tokens as one batch, in that order.
fn queue_raw_batch<'py>(
    py: Python<'py>,
    helper: &Bound<'py, PyModule>,
    event_loop: &Bound<'py, PyAny>,
    replies: Vec<Arc<dyn Reconstruct>>,
) -> (Vec<Py<PyHandle>>, Vec<Bound<'py, PyAny>>) {
    let (publishers, handles): (Vec<_>, Vec<_>) = replies
        .iter()
        .map(|_| {
            let (publisher, handle) = pending_handle();
            (publisher, Py::new(py, handle).expect("test Handle"))
        })
        .unzip();
    let futures: Vec<Bound<'py, PyAny>> = helper
        .call_method1("observe", (event_loop, PyList::new(py, &handles).unwrap()))
        .and_then(|futures| futures.extract())
        .expect("pending observers");
    let waker = channel(py, event_loop).get().waker_for_test();
    // The channel, this test and one notifier per await.
    let holders = Arc::strong_count(&waker);
    publishers
        .into_iter()
        .zip(replies)
        .enumerate()
        .for_each(|(published, (publisher, reply))| {
            publisher.publish_raw(reply);
            wait_for_waker_holders(&waker, holders - published - 1);
        });
    (handles, futures)
}

// HDL-17, HDL-27: the pipe waiter sends its token on raw arrival without
// the GIL or a claim, and the loop's reader rebuilds the reply on the loop
// thread, once, for every observer.
#[test]
fn raw_arrival_wakes_the_loop_which_rebuilds() {
    ensure_python();
    let threads = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&threads);
    let (reply, calls, _) = test_reply(move |py, _| {
        recorded
            .lock()
            .expect("thread record")
            .push(thread::current().id());
        vec![5].into_py_any(py)
    });
    let (publisher, handle) = pending_handle();
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let helper = reader_helper(py);
        let event_loop = new_loop(py);
        let handle = Py::new(py, handle).expect("test Handle");
        let fut = helper
            .call_method1(
                "observe",
                (&event_loop, PyList::new(py, [&handle]).unwrap()),
            )
            .and_then(|futures| futures.get_item(0))
            .expect("a pending observer");
        let channel = channel(py, &event_loop);

        publisher.publish_raw(reply);
        assert!(
            channel.get().readable_within(Duration::from_secs(5)),
            "the waiter should wake the loop without the GIL, which this thread holds"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "the waiter does not rebuild"
        );
        assert!(
            raw_unclaimed(py, &handle),
            "the waiter leaves the bytes unclaimed"
        );

        assert!(run_once(&helper, &event_loop).is_none());
        let value = fut
            .call_method0("result")
            .expect("the reader sets the value");
        assert_eq!(
            *threads.lock().expect("thread record"),
            vec![thread::current().id()],
            "the loop thread rebuilds"
        );
        let again = handle
            .borrow(py)
            .poll(py)
            .expect("poll reads the value")
            .expect("the value is stored");
        assert!(value.is(&again), "the await and poll share one object");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        event_loop.call_method0("close").unwrap();
    });
}

// HDL-17, HDL-23: an await re-armed behind another observer's claim is
// released by its cancel while the claim is still held. Its new notifier is
// dropped and its core reference goes, so ordinary claim settlement can't
// account for either.
#[test]
fn rearmed_await_is_released_by_its_cancel() {
    ensure_python();
    let (reply, calls, _) = test_reply(|py, _| 6.into_py_any(py));
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let helper = reader_helper(py);
        let event_loop = new_loop(py);
        let (handles, futures) = queue_raw_batch(py, &helper, &event_loop, vec![reply]);
        let channel = channel(py, &event_loop);
        let waker = channel.get().waker_for_test();
        let claim = raw_claim(&handles[0].borrow(py));

        assert!(run_once(&helper, &event_loop).is_none());
        assert_eq!(channel.get().entry_count(), 1, "the entry is re-armed");
        // The channel, this test and the re-armed waiter's notifier.
        assert_eq!(
            Arc::strong_count(&waker),
            3,
            "the re-armed waiter holds a notifier"
        );
        let core = Arc::clone(&handles[0].borrow(py).core);
        let holders = Arc::strong_count(&core);

        futures[0].call_method0("cancel").expect("cancel the await");
        assert!(run_once(&helper, &event_loop).is_none());
        assert_eq!(
            channel.get().entry_count(),
            0,
            "the reaper removes the entry"
        );
        wait_until(
            || Arc::strong_count(&waker) == 2,
            "the released waiter should drop its notifier while the claim is held",
        );
        wait_until(
            || Arc::strong_count(&core) == holders - 1,
            "the released waiter should drop its core while the claim is held",
        );

        drop(claim);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "nothing rebuilds the reply"
        );
        event_loop.call_method0("close").unwrap();
    });
}

// HDL-17, HDL-27: a token that finds Raw claimed re-arms its entry, and the
// loop runs on. The claim's settlement, or its release and the reader's
// own rebuild, then settles the await.
#[test]
fn claimed_raw_rearms_until_the_claim_settles_or_releases() {
    ensure_python();
    for ending in ["settle", "release"] {
        let (reply, calls, _) = test_reply(|py, _| vec![6].into_py_any(py));
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let helper = reader_helper(py);
            let event_loop = new_loop(py);
            let (handles, futures) = queue_raw_batch(py, &helper, &event_loop, vec![reply]);
            let channel = channel(py, &event_loop);
            let claim = raw_claim(&handles[0].borrow(py));

            assert!(
                run_once(&helper, &event_loop).is_none(),
                "{ending}: the loop runs on"
            );
            assert!(!is_done(&futures[0]), "{ending}: the await waits");
            assert_eq!(
                channel.get().entry_count(),
                1,
                "{ending}: the entry is re-armed"
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0);

            let claimed = if ending == "settle" {
                let outcome = claim
                    .reconstruct(py, ReconstructionFailurePolicy::KeepReplyAfterInterrupt)
                    .expect("the claim settles");
                Some(clone_shared_outcome(py, &outcome).expect("a value"))
            } else {
                drop(claim);
                None
            };
            assert!(
                channel.get().readable_within(Duration::from_secs(5)),
                "{ending}: the re-armed waiter wakes the loop"
            );
            assert!(run_once(&helper, &event_loop).is_none());
            let value = futures[0]
                .call_method0("result")
                .expect("the reader sets the value");
            if let Some(claimed) = claimed {
                assert!(value.is(&claimed), "the await shares the claim's outcome");
            }
            assert_eq!(calls.load(Ordering::SeqCst), 1, "{ending}: one rebuild");
            assert_eq!(channel.get().entry_count(), 0);
            event_loop.call_method0("close").unwrap();
        });
    }
}

// HDL-17, HDL-27: KeyboardInterrupt or SystemExit from the loop's rebuild
// escapes the loop run and leaves the reply Raw and unclaimed. The
// interrupted await and the batch's undelivered token settle next run.
#[test]
fn loop_rebuild_interrupt_escapes_the_run_and_retries() {
    ensure_python();
    for interrupt in ["KeyboardInterrupt", "SystemExit"] {
        let (first, first_calls, _) = test_reply(|py, _| 1.into_py_any(py));
        let (interrupted, interrupted_calls, _) = test_reply(move |py, attempt| {
            if attempt > 0 {
                return 2.into_py_any(py);
            }
            Err(match interrupt {
                "KeyboardInterrupt" => PyKeyboardInterrupt::new_err("loop rebuild interrupted"),
                _ => PySystemExit::new_err("loop rebuild exited"),
            })
        });
        let (last, last_calls, _) = test_reply(|py, _| 3.into_py_any(py));
        let calls = || {
            [&first_calls, &interrupted_calls, &last_calls]
                .map(|calls| calls.load(Ordering::SeqCst))
        };
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let helper = reader_helper(py);
            let event_loop = new_loop(py);
            let (handles, futures) =
                queue_raw_batch(py, &helper, &event_loop, vec![first, interrupted, last]);
            let channel = channel(py, &event_loop);

            let raised = run_once(&helper, &event_loop)
                .unwrap_or_else(|| panic!("{interrupt} should escape the loop run"));
            assert!(
                raised.is_instance_of::<PyKeyboardInterrupt>(py)
                    || raised.is_instance_of::<PySystemExit>(py),
                "{interrupt}: unexpected {raised}"
            );
            assert_eq!(int_result(&futures[0]), 1, "the earlier token is delivered");
            assert!(
                !is_done(&futures[1]) && !is_done(&futures[2]),
                "{interrupt}: the interrupted and later awaits wait"
            );
            assert!(
                raw_unclaimed(py, &handles[1]) && raw_unclaimed(py, &handles[2]),
                "{interrupt}: both replies stay Raw and unclaimed"
            );
            assert_eq!(calls(), [1, 1, 0], "the later token was not delivered");
            assert_eq!(channel.get().entry_count(), 2);
            assert!(
                channel.get().readable_within(Duration::ZERO),
                "{interrupt}: the re-queued tokens wake the next run"
            );

            assert!(run_once(&helper, &event_loop).is_none());
            assert_eq!(int_result(&futures[1]), 2, "the rebuild is retried");
            assert_eq!(int_result(&futures[2]), 3);
            assert_eq!(calls(), [1, 2, 1]);
            assert_eq!(channel.get().entry_count(), 0);
            event_loop.call_method0("close").unwrap();
        });
    }
}

// HDL-27: any other BaseException from the loop's rebuild is the shared
// outcome. It is not retried and not reported, and the loop goes idle.
#[test]
fn loop_rebuild_base_exception_is_shared_not_retried() {
    ensure_python();
    for kind in ["CancelledError", "ReaderBaseError"] {
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let helper = reader_helper(py);
            let event_loop = new_loop(py);
            let errors = helper
                .call_method1("capture_errors", (&event_loop,))
                .expect("an exception handler");
            let exception = match kind {
                "CancelledError" => py
                    .import("asyncio")
                    .and_then(|asyncio| asyncio.getattr(kind)),
                _ => helper.getattr(kind),
            }
            .and_then(|class| class.call1(("loop rebuild failed",)))
            .expect("the exception")
            .unbind();
            let raised = exception.clone_ref(py);
            let (reply, calls, _) =
                test_reply(move |py, _| Err(PyErr::from_value(raised.bind(py).clone())));
            let (handles, futures) = queue_raw_batch(py, &helper, &event_loop, vec![reply]);
            let channel = channel(py, &event_loop);

            assert!(
                run_once(&helper, &event_loop).is_none(),
                "{kind}: nothing escapes"
            );
            let delivered = futures[0]
                .call_method0("exception")
                .expect("the await settles with an exception");
            assert!(
                delivered.is(&exception),
                "{kind}: the await has the exception"
            );
            let stored = handles[0]
                .borrow(py)
                .poll(py)
                .expect_err("poll raises the stored exception");
            assert!(
                stored.value(py).is(&exception),
                "{kind}: one shared outcome"
            );
            assert_eq!(calls.load(Ordering::SeqCst), 1, "{kind}: no retry");
            assert_eq!(channel.get().entry_count(), 0, "{kind}: no re-arm");
            assert_eq!(errors.len().unwrap(), 0, "{kind}: nothing is reported");
            assert!(
                !channel.get().readable_within(Duration::ZERO),
                "{kind}: the loop goes idle"
            );
            event_loop.call_method0("close").unwrap();
        });
    }
}

// HDL-27: a Rust panic in the loop's rebuild settles that await with a
// shared RuntimeError. It unwinds through neither the reader nor the
// batch, and leaves no claim or entry.
#[test]
fn loop_rebuild_panic_settles_its_await_and_spares_the_batch() {
    ensure_python();
    let (first, _, _) = test_reply(|py, _| 1.into_py_any(py));
    let (panicking, panicking_calls, _) = test_reply(|_, _| panic!("loop rebuild panic"));
    let (last, _, _) = test_reply(|py, _| 3.into_py_any(py));
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let helper = reader_helper(py);
        let event_loop = new_loop(py);
        let errors = helper
            .call_method1("capture_errors", (&event_loop,))
            .expect("an exception handler");
        let (handles, futures) =
            queue_raw_batch(py, &helper, &event_loop, vec![first, panicking, last]);
        let channel = channel(py, &event_loop);

        assert!(
            run_once(&helper, &event_loop).is_none(),
            "nothing escapes the loop run"
        );
        assert_eq!(int_result(&futures[0]), 1);
        assert_eq!(int_result(&futures[2]), 3);
        let delivered = futures[1]
            .call_method0("exception")
            .expect("the await settles with an exception");
        assert!(delivered.is_instance_of::<PyRuntimeError>());
        assert!(
            delivered.to_string().contains("loop rebuild panic"),
            "unexpected {delivered}"
        );
        let stored = handles[1]
            .borrow(py)
            .poll(py)
            .expect_err("poll raises the stored error");
        assert!(stored.value(py).is(&delivered), "one shared outcome");
        assert!(
            matches!(&*handles[1].borrow(py).core.lock(), State::Done(_)),
            "no claim is left held"
        );
        assert_eq!(panicking_calls.load(Ordering::SeqCst), 1);
        assert_eq!(channel.get().entry_count(), 0);
        assert_eq!(errors.len().unwrap(), 0, "nothing is reported");
        event_loop.call_method0("close").unwrap();
    });
}

// HDL-27, behavior change 5: an await cancelled before its reply arrives
// does not rebuild it. Its released waiter drops its notifier, so the reply's
// arrival does not wake the loop (HDL-17). The channel stays usable, and a
// later get() rebuilds once.
#[test]
fn cancelled_await_does_not_rebuild_its_raw_reply() {
    ensure_python();
    let (reply, calls, _) = test_reply(|py, _| 8.into_py_any(py));
    let (publisher, handle) = pending_handle();
    let (other_publisher, other) = pending_handle();
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let helper = reader_helper(py);
        let event_loop = new_loop(py);
        let handle = Py::new(py, handle).expect("test Handle");
        let fut = helper
            .call_method1(
                "observe",
                (&event_loop, PyList::new(py, [&handle]).unwrap()),
            )
            .and_then(|futures| futures.get_item(0))
            .expect("a pending observer");
        let channel = channel(py, &event_loop);
        let waker = channel.get().waker_for_test();
        fut.call_method0("cancel").expect("cancel the await");
        assert!(run_once(&helper, &event_loop).is_none());
        assert_eq!(
            channel.get().entry_count(),
            0,
            "the reaper removes the entry"
        );
        // Only the channel and this test hold the write end once the
        // released waiter has dropped its notifier.
        wait_until(
            || Arc::strong_count(&waker) == 2,
            "the released waiter should drop its notifier",
        );

        publisher.publish_raw(reply);
        assert!(
            !channel.get().readable_within(Duration::from_millis(300)),
            "the released waiter does not wake the loop"
        );
        assert!(run_once(&helper, &event_loop).is_none());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "nothing rebuilds the reply"
        );

        let other = Py::new(py, other).expect("second Handle");
        let second = helper
            .call_method1("observe", (&event_loop, PyList::new(py, [&other]).unwrap()))
            .and_then(|futures| futures.get_item(0))
            .expect("a second pending observer");
        assert!(
            channel.is(self::channel(py, &event_loop)),
            "the second await uses the same channel"
        );
        other_publisher.publish(Ok(9.into_py_any(py).unwrap()));
        helper
            .call_method1("run_until_done", (&event_loop, [&second]))
            .expect("the second await completes");
        assert_eq!(int_result(&second), 9);

        let value = PyHandle::get(handle.borrow(py), py, None).expect("get rebuilds");
        assert_eq!(value.extract::<i64>(py).unwrap(), 8);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        event_loop.call_method0("close").unwrap();
    });
}

// HDL-19, HDL-20, HDL-27, behavior change 5: an await whose loop is closed
// and released before its reply arrives does not rebuild it. Freeing the
// channel releases the waiter, which drops the write end before the reply is
// published, and a later get() rebuilds once.
#[test]
fn closed_loop_does_not_rebuild_its_raw_reply() {
    ensure_python();
    let (reply, calls, _) = test_reply(|py, _| 4.into_py_any(py));
    let (publisher, handle) = pending_handle();
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let helper = reader_helper(py);
        let event_loop = new_loop(py);
        let handle = Py::new(py, handle).expect("test Handle");
        let fut = helper
            .call_method1(
                "observe",
                (&event_loop, PyList::new(py, [&handle]).unwrap()),
            )
            .and_then(|futures| futures.get_item(0))
            .expect("a pending observer");
        let channel = channel(py, &event_loop);
        assert_eq!(channel.get().entry_count(), 1);
        let waker = channel.get().waker_for_test();
        let channel_ref = PyWeakrefReference::new(channel.as_any()).expect("weakref");

        event_loop.call_method0("close").unwrap();
        drop((fut, channel, event_loop));
        py.import("gc").unwrap().call_method0("collect").unwrap();
        assert!(
            channel_ref.upgrade().is_none(),
            "closing the loop frees the channel and its entry"
        );
        wait_until(
            || Arc::strong_count(&waker) == 1,
            "the released waiter should drop the write end",
        );

        publisher.publish_raw(reply);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "nothing rebuilds the reply"
        );

        let value = PyHandle::get(handle.borrow(py), py, None).expect("get rebuilds");
        assert_eq!(value.extract::<i64>(py).unwrap(), 4);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    });
}

// __await__ in a Tokio runtime context finds no asyncio loop to bridge to,
// so it surfaces asyncio's native "no running event loop" RuntimeError --
// NOT WouldBlockRuntime, which among the observers stays specific to get()
// (HDL-6). The refusal consumes nothing: the same handle still observes its
// value afterwards. Lives here rather than in the Python suite so the Handle
// contract is tested directly through its Rust binding.
// Attests HDL-6, HDL-7.
#[test]
fn await_in_tokio_raises_native_and_leaves_handle_observable() {
    ensure_python();
    let handle = monarch_with_gil_blocking(GilSite::Test, |py| {
        let value = 11i64.into_py_any(py).unwrap();
        Py::new(py, PyHandle::from_value(value)).unwrap()
    });
    let in_tokio = monarch_with_gil_blocking(GilSite::Test, |py| handle.clone_ref(py));

    // `block_on` would poll the root future on *this* thread under an
    // entered runtime -- `is_in_tokio_runtime()` is true there, but it is not a
    // worker and nested blocking is tolerated. Spawn instead, so the
    // assertion genuinely runs on a runtime worker, and join from here.
    let joined = get_tokio_runtime().spawn(async move {
        assert!(
            is_in_tokio_runtime(),
            "the oracle must observe from a runtime worker"
        );
        monarch_with_gil(GilSite::Test, |py| {
            let err = PyHandle::__await__(in_tokio.borrow(py), py).unwrap_err();
            assert!(
                !err.is_instance_of::<WouldBlockRuntime>(py),
                "__await__ must not raise WouldBlockRuntime; among the observers that is get()'s alone"
            );
            assert!(
                err.is_instance_of::<PyRuntimeError>(py),
                "__await__ off an asyncio loop should raise the native RuntimeError"
            );
            assert!(
                err.to_string().contains("no running event loop"),
                "expected asyncio's native message, got {err}"
            );
        })
        .await
    });
    get_tokio_runtime()
        .block_on(joined)
        .expect("the spawned oracle panicked");

    monarch_with_gil_blocking(GilSite::Test, |py| {
        let value = PyHandle::get(handle.borrow(py), py, None).unwrap();
        assert_eq!(
            value.extract::<i64>(py).unwrap(),
            11,
            "a refused __await__ must not consume the handle"
        );
    });
}

// as_asyncio() off a loop raises the native RuntimeError, not
// WouldBlockRuntime.
// Attests HDL-6.
#[test]
fn as_asyncio_off_loop_raises_runtime_error() {
    ensure_python();
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let handle = PyHandle::from_value(py.None());
        let r = Py::new(py, handle).unwrap();
        let err = PyHandle::as_asyncio(r.borrow(py), py).unwrap_err();
        assert!(
            err.is_instance_of::<PyRuntimeError>(py),
            "off a loop as_asyncio() should raise RuntimeError"
        );
        assert!(
            !err.is_instance_of::<WouldBlockRuntime>(py),
            "the off-loop error must be the native RuntimeError, not WouldBlockRuntime"
        );
    });
}

// __await__ off a loop surfaces the native RuntimeError from as_asyncio().
// Attests HDL-6.
#[test]
fn await_off_loop_raises_runtime_error() {
    ensure_python();
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let handle = PyHandle::from_value(py.None());
        let r = Py::new(py, handle).unwrap();
        let err = PyHandle::__await__(r.borrow(py), py).unwrap_err();
        assert!(
            err.is_instance_of::<PyRuntimeError>(py),
            "off a loop __await__ should raise RuntimeError"
        );
        assert!(
            !err.is_instance_of::<WouldBlockRuntime>(py),
            "__await__ must not raise WouldBlockRuntime"
        );
    });
}

// await on a pre-resolved handle resolves on a real loop and yields the
// value.
// Attests HDL-2, HDL-7.
#[test]
fn await_resolves_ok_on_loop() {
    ensure_python();
    let got = monarch_with_gil_blocking(GilSite::Test, |py| -> i64 {
        let value = 42i64.into_py_any(py).unwrap();
        let handle = PyHandle::from_value(value);
        let h = Py::new(py, handle).unwrap();
        let helper = loop_helper(py);
        helper
            .getattr("run_await")
            .unwrap()
            .call1((h,))
            .unwrap()
            .extract::<i64>()
            .unwrap()
    });
    assert_eq!(got, 42, "await should resolve to the value on a real loop");
}

// await surfaces an error sent while it waits as a Python exception on a
// real loop.
// Attests HDL-2, HDL-7.
#[test]
fn await_resolves_err_on_loop() {
    ensure_python();
    let (tx, handle) = pending_handle();
    let waited = Arc::new(AtomicBool::new(false));
    let waited_in_producer = Arc::clone(&waited);
    get_tokio_runtime().spawn(async move {
        // The Handle is one observer and the pending observer adds a second,
        // so the error is sent only once the observer is on the pending path.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while tx.observer_count() < 2 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        waited_in_producer.store(tx.observer_count() >= 2, Ordering::SeqCst);
        tx.publish(Err(PyValueError::new_err("boom")));
    });
    let is_value_error = monarch_with_gil_blocking(GilSite::Test, |py| {
        let h = Py::new(py, handle).unwrap();
        let helper = loop_helper(py);
        let err = helper
            .getattr("run_await")
            .unwrap()
            .call1((h,))
            .unwrap_err();
        err.is_instance_of::<PyValueError>(py)
    });
    assert!(
        waited.load(Ordering::SeqCst),
        "the error should be sent only after the observer waited"
    );
    assert!(is_value_error, "await should raise the stored exception");
}

// await resolves a still-pending handle once a producer completes it while
// the loop runs.
// Attests HDL-1, HDL-7.
#[test]
fn await_resolves_pending_on_loop() {
    ensure_python();
    let value = monarch_with_gil_blocking(GilSite::Test, |py| 11i64.into_py_any(py).unwrap());
    let (tx, handle) = pending_handle();
    get_tokio_runtime().spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        tx.publish(Ok(value));
    });
    let got = monarch_with_gil_blocking(GilSite::Test, |py| -> i64 {
        let h = Py::new(py, handle).unwrap();
        let helper = loop_helper(py);
        helper
            .getattr("run_await")
            .unwrap()
            .call1((h,))
            .unwrap()
            .extract::<i64>()
            .unwrap()
    });
    assert_eq!(got, 11, "await should resolve once the producer completes");
}

// Cancelling an as_asyncio() future does not cancel the handle. Here the
// handle is pre-resolved, so its observer is already done: cancel() is
// refused and the value stays observable via poll(). The live-producer
// case -- one observer's cancel leaves the producer running and other
// observers still resolving -- is
// `cancel_asyncio_future_does_not_stop_producer_or_observers`.
// Attests HDL-3, HDL-22.
#[test]
fn cancel_asyncio_future_does_not_cancel_handle() {
    ensure_python();
    let (was_done, accepted, cancelled, handle_resolved) =
        monarch_with_gil_blocking(GilSite::Test, |py| -> (bool, bool, bool, bool) {
            let value = 42i64.into_py_any(py).unwrap();
            let handle = PyHandle::from_value(value);
            let h = Py::new(py, handle).unwrap();
            let helper = loop_helper(py);
            let res = helper.getattr("run_cancel").unwrap().call1((h,)).unwrap();
            let tup = res.cast::<PyTuple>().unwrap();
            let flag = |i| tup.get_item(i).unwrap().extract::<bool>().unwrap();
            (
                flag(0),
                flag(1),
                flag(2),
                !tup.get_item(3).unwrap().is_none(),
            )
        });
    assert!(was_done, "a ready handle's observer should already be done");
    assert!(!accepted, "cancel() on a done observer should return false");
    assert!(!cancelled, "a done observer should not become cancelled");
    assert!(
        handle_resolved,
        "cancelling the future must not cancel the handle"
    );
}

// Run `observe_ready` on `h`: whether the observer was done when returned,
// its outcome (a value or an exception object), and the add_reader calls.
fn observe_ready(py: Python<'_>, h: Py<PyHandle>) -> (bool, Bound<'_, PyAny>, i64) {
    let res = loop_helper(py)
        .getattr("observe_ready")
        .unwrap()
        .call1((h,))
        .unwrap();
    let tup = res.cast::<PyTuple>().unwrap();
    (
        tup.get_item(0).unwrap().extract().unwrap(),
        tup.get_item(1).unwrap(),
        tup.get_item(2).unwrap().extract().unwrap(),
    )
}

fn errored_handle(err: PyErr) -> PyHandle {
    let (tx, handle) = pending_handle();
    tx.publish(Err(err));
    handle
}

// A ready value comes back in an already-done observer, and no wake channel
// is installed.
// Attests HDL-22.
#[test]
fn as_asyncio_ready_value_is_done_without_wake() {
    ensure_python();
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let handle = PyHandle::from_value(42i64.into_py_any(py).unwrap());
        let (done, outcome, add_reader_calls) = observe_ready(py, Py::new(py, handle).unwrap());
        assert!(done, "a ready handle's observer should already be done");
        assert_eq!(outcome.extract::<i64>().unwrap(), 42);
        assert_eq!(add_reader_calls, 0, "no wake channel should be installed");
    });
}

// A ready error comes back in an already-done observer; a stored
// StopIteration is wrapped in RuntimeError as on the pending path.
// Attests HDL-7, HDL-22.
#[test]
fn as_asyncio_ready_error_is_done_without_wake() {
    ensure_python();
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let h = Py::new(py, errored_handle(PyValueError::new_err("boom"))).unwrap();
        let (done, outcome, add_reader_calls) = observe_ready(py, h);
        assert!(done, "a ready handle's observer should already be done");
        assert!(outcome.is_instance_of::<PyValueError>());
        assert_eq!(add_reader_calls, 0, "no wake channel should be installed");

        let h = Py::new(py, errored_handle(PyStopIteration::new_err(()))).unwrap();
        let (done, outcome, _) = observe_ready(py, h);
        assert!(done);
        assert!(
            outcome.is_instance_of::<PyRuntimeError>()
                && !outcome.is_instance_of::<PyStopIteration>(),
            "a stored StopIteration should surface as RuntimeError"
        );
    });
}

// A producer that ended without publishing is a ready outcome too.
// Attests HDL-10, HDL-22.
#[test]
fn as_asyncio_producer_ended_is_done_without_wake() {
    ensure_python();
    let (tx, handle) = pending_handle();
    drop(tx);
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let (done, outcome, add_reader_calls) = observe_ready(py, Py::new(py, handle).unwrap());
        assert!(done, "a ready handle's observer should already be done");
        assert_producer_ended_error(py, &PyErr::from_value(outcome));
        assert_eq!(add_reader_calls, 0, "no wake channel should be installed");
    });
}

// A producer that publishes and is then gone: the observer gets what was
// published, not producer loss.
// Attests HDL-1, HDL-10, HDL-22.
#[test]
fn as_asyncio_reads_an_outcome_published_by_a_departed_producer() {
    ensure_python();
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let outcomes = [
            Ok(42i64.into_py_any(py).unwrap()),
            Err(PyValueError::new_err("boom")),
        ];
        for outcome in outcomes {
            let expect_value = outcome.is_ok();
            let (tx, handle) = pending_handle();
            tx.publish(outcome);
            let (done, got, _) = observe_ready(py, Py::new(py, handle).unwrap());
            assert!(done, "the published outcome is ready");
            if expect_value {
                assert_eq!(got.extract::<i64>().unwrap(), 42);
            } else {
                assert!(
                    got.is_instance_of::<PyValueError>(),
                    "the stored exception should surface, not producer loss: {got}"
                );
            }
        }
    });
}

// Awaiting a ready handle does not give the loop a turn: the awaiting task
// runs on before a task scheduled after it, so the order is a1, a2, b.
// Attests HDL-22.
#[test]
fn await_ready_handle_takes_no_loop_turn() {
    ensure_python();
    let order = monarch_with_gil_blocking(GilSite::Test, |py| -> Vec<String> {
        let handle = PyHandle::from_value(1i64.into_py_any(py).unwrap());
        loop_helper(py)
            .getattr("ready_await_order")
            .unwrap()
            .call1((Py::new(py, handle).unwrap(),))
            .unwrap()
            .extract()
            .unwrap()
    });
    assert_eq!(order, ["a1", "a2", "b"]);
}

// The live-producer companion to `cancel_asyncio_future_does_not_cancel_handle`:
// with a still-pending handle and a real producer, cancelling one observer
// releases that observer's waiter (HDL-17) and leaves the producer running
// and other observers still resolving. A second as_asyncio() future resolves
// to the value and the handle stays pollable. Both awaits register while the
// producer is held, so the count of three is seen before the cancel, and the
// release is seen before the value is published.
// Attests HDL-3, HDL-7, HDL-17.
#[test]
fn cancel_asyncio_future_does_not_stop_producer_or_observers() {
    ensure_python();
    let (tx, handle) = pending_handle();
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let helper = reader_helper(py);
        let event_loop = new_loop(py);
        let handle = Py::new(py, handle).unwrap();
        let futures = helper
            .call_method1(
                "observe",
                (&event_loop, PyList::new(py, [&handle, &handle]).unwrap()),
            )
            .expect("two pending observers");
        let cancelled = futures.get_item(0).unwrap();
        let awaited = futures.get_item(1).unwrap();
        // The Handle is one observer and each pending `as_asyncio()` adds
        // another.
        assert_eq!(tx.observer_count(), 3, "both awaits observe");

        cancelled.call_method0("cancel").unwrap();
        assert!(run_once(&helper, &event_loop).is_none());
        wait_until(
            || tx.observer_count() == 2,
            "the cancelled await's waiter should release the core",
        );

        tx.publish(Ok(42i64.into_py_any(py).unwrap()));
        helper
            .call_method1("run_until_done", (&event_loop, [&awaited]))
            .expect("the second await completes");
        assert!(
            cancelled
                .call_method0("cancelled")
                .and_then(|cancelled| cancelled.is_truthy())
                .unwrap(),
            "the cancelled future should stay cancelled"
        );
        assert_eq!(
            int_result(&awaited),
            42,
            "a second observer resolves even after the first future is cancelled"
        );
        let polled = handle
            .borrow(py)
            .poll(py)
            .expect("poll reads the value")
            .expect("the value is stored");
        assert_eq!(
            polled.extract::<i64>(py).unwrap(),
            42,
            "the handle stays observable after one observer is cancelled"
        );
        event_loop.call_method0("close").unwrap();
    });
}

// HDL-17, HDL-23: cancelling the only await on a started stop-when-unobserved
// producer releases its waiter, and the producer is destroyed.
#[test]
fn cancelled_await_stops_an_unobserved_receive() {
    ensure_python();
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let helper = reader_helper(py);
        let event_loop = new_loop(py);
        let (handle, started, dropped, _go) = signalled_receive(py, 0);
        let handle = Py::new(py, handle).unwrap();
        let fut = observe_one(py, &helper, &event_loop, &handle);
        started
            .recv_timeout(Duration::from_secs(5))
            .expect("the await starts the producer (HDL-24)");
        drop(handle);

        fut.call_method0("cancel").expect("cancel the await");
        assert!(run_once(&helper, &event_loop).is_none());
        dropped
            .recv_timeout(Duration::from_secs(5))
            .expect("the released waiter should let the producer stop");
        event_loop.call_method0("close").unwrap();
    });
}

// HDL-23: an await that still waits keeps a stop-when-unobserved producer
// running after the test's Handle is dropped, and its value then resolves
// the await.
#[test]
fn waiting_await_keeps_an_unobserved_receive() {
    ensure_python();
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let helper = reader_helper(py);
        let event_loop = new_loop(py);
        let (handle, started, dropped, go) = signalled_receive(py, 5);
        let handle = Py::new(py, handle).unwrap();
        let fut = observe_one(py, &helper, &event_loop, &handle);
        started
            .recv_timeout(Duration::from_secs(5))
            .expect("the await starts the producer (HDL-24)");
        drop(handle);

        assert!(run_once(&helper, &event_loop).is_none());
        assert!(
            dropped.recv_timeout(Duration::from_millis(300)).is_err(),
            "the waiting await keeps the producer"
        );
        go.send(()).expect("the producer waits for the signal");
        helper
            .call_method1("run_until_done", (&event_loop, [&fut]))
            .expect("the await completes");
        assert_eq!(int_result(&fut), 5);
        event_loop.call_method0("close").unwrap();
    });
}

// HDL-21, HDL-23: on the fallback, the future's done callback releases the
// waiter, so cancelling the only await stops the producer.
#[test]
fn cancelled_fallback_await_stops_an_unobserved_receive() {
    ensure_python();
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let helper = reader_helper(py);
        let event_loop = helper
            .getattr("NoReaderLoop")
            .unwrap()
            .call0()
            .expect("a loop without add_reader");
        let (handle, started, dropped, _go) = signalled_receive(py, 0);
        let handle = Py::new(py, handle).unwrap();
        let fut = observe_one(py, &helper, &event_loop, &handle);
        started
            .recv_timeout(Duration::from_secs(5))
            .expect("the await starts the producer (HDL-24)");
        drop(handle);

        fut.call_method0("cancel").expect("cancel the await");
        // Runs the done callbacks, including the one that releases the
        // waiter.
        assert!(run_once(&helper, &event_loop).is_none());
        dropped
            .recv_timeout(Duration::from_secs(5))
            .expect("the released fallback waiter should let the producer stop");
        event_loop.call_method0("close").unwrap();
    });
}

// HDL-21: on the fallback, a future that rejects add_done_callback makes
// as_asyncio raise, as the pipe path does, and no waiter starts.
#[test]
fn fallback_rejected_done_callback_raises_and_starts_no_waiter() {
    ensure_python();
    let (tx, handle) = pending_handle();
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let helper = reader_helper(py);
        let event_loop = helper
            .getattr("NoReaderTypeRejectingLoop")
            .unwrap()
            .call0()
            .expect("a fallback loop whose futures reject done callbacks");
        let handle = Py::new(py, handle).unwrap();
        let err = helper
            .call_method1("try_observe", (&event_loop, &handle))
            .unwrap();
        assert!(
            err.is_instance_of::<PyTypeError>(),
            "as_asyncio should raise the add_done_callback TypeError: {err}"
        );
        assert_eq!(
            tx.observer_count(),
            1,
            "only the Handle holds the core; no waiter started"
        );
        event_loop.call_method0("close").unwrap();
    });
}

// Two as_asyncio() futures waiting on one handle both resolve on a real
// loop.
// Attests HDL-3, HDL-7.
#[test]
fn two_asyncio_observers_resolve_on_loop() {
    ensure_python();
    let value = monarch_with_gil_blocking(GilSite::Test, |py| 8i64.into_py_any(py).unwrap());
    let (tx, handle) = pending_handle();
    let both_waited = Arc::new(AtomicBool::new(false));
    let both_waited_in_producer = Arc::clone(&both_waited);
    get_tokio_runtime().spawn(async move {
        // The Handle is one observer and each pending `as_asyncio()` adds
        // another.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while tx.observer_count() < 3 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        both_waited_in_producer.store(tx.observer_count() >= 3, Ordering::SeqCst);
        tx.publish(Ok(value));
    });
    let (a, b) = monarch_with_gil_blocking(GilSite::Test, |py| -> (i64, i64) {
        let h = Py::new(py, handle).unwrap();
        let helper = loop_helper(py);
        let res = helper.getattr("run_two").unwrap().call1((h,)).unwrap();
        let tup = res.cast::<PyTuple>().unwrap();
        (
            tup.get_item(0).unwrap().extract::<i64>().unwrap(),
            tup.get_item(1).unwrap().extract::<i64>().unwrap(),
        )
    });
    assert!(
        both_waited.load(Ordering::SeqCst),
        "the value should be sent only after both observers waited"
    );
    assert_eq!((a, b), (8, 8), "both observers should resolve to the value");
}

// Scheduling completion onto a closed loop returns an error rather than
// panicking; the observer swallows this and logs.
// Attests HDL-7.
#[test]
fn schedule_on_closed_loop_errs_not_panics() {
    ensure_python();
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let asyncio = py.import("asyncio").unwrap();
        let event_loop = asyncio.call_method0("new_event_loop").unwrap();
        let fut = event_loop.call_method0("create_future").unwrap();
        event_loop.call_method0("close").unwrap();
        let result = schedule_completion(py, &event_loop, &fut, Ok(py.None()));
        assert!(
            result.is_err(),
            "call_soon_threadsafe on a closed loop should error, not panic"
        );
    });
}

// complete_asyncio_future swallows InvalidStateError from an already-settled
// (non-cancelled) future, and propagates any other setter error.
// Attests HDL-7.
#[test]
fn complete_asyncio_future_swallows_and_propagates() {
    ensure_python();
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let event_loop = py
            .import("asyncio")
            .unwrap()
            .call_method0("new_event_loop")
            .unwrap();
        let settled = event_loop.call_method0("create_future").unwrap();
        settled.call_method1("set_result", (1i64,)).unwrap();
        assert!(
            complete_asyncio_future(&settled, false, py.None()).is_ok(),
            "InvalidStateError from a settled future should be swallowed"
        );
        event_loop.call_method0("close").unwrap();

        // A fake future whose setter raises a non-InvalidStateError: it must
        // propagate rather than be swallowed.
        let helper = PyModule::from_code(
            py,
            cr#"
class RaisingFuture:
    def cancelled(self):
        return False

    def set_result(self, value):
        raise ValueError("nope")
"#,
            c"raising_future.py",
            c"raising_future",
        )
        .unwrap();
        let fake = helper.getattr("RaisingFuture").unwrap().call0().unwrap();
        let err = complete_asyncio_future(&fake, false, py.None()).unwrap_err();
        assert!(
            err.is_instance_of::<PyValueError>(py),
            "a non-InvalidStateError setter error should propagate"
        );
    });
}

// A producer that drops its publisher without publishing, once the observer
// is waiting, surfaces through as_asyncio/await (producer loss ->
// set_exception) as a raised exception, not a hang -- the async mirror of
// dropped_producer_surfaces_error. The already-ended case is
// `as_asyncio_producer_ended_is_done_without_wake`.
// Attests HDL-10.
#[test]
fn as_asyncio_dropped_producer_raises_on_loop() {
    ensure_python();
    let (tx, handle) = pending_handle();
    let observed = Arc::new(AtomicBool::new(false));
    let gate = observed.clone();
    get_tokio_runtime().spawn(async move {
        // The Handle is one observer and the pending observer adds a second,
        // so the producer drops only once the observer is on the pending path.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while tx.observer_count() < 2 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        gate.store(tx.observer_count() >= 2, Ordering::SeqCst);
        drop(tx);
    });
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let h = Py::new(py, handle).unwrap();
        let helper = loop_helper(py);
        let err = helper
            .getattr("run_await")
            .unwrap()
            .call1((h,))
            .unwrap_err();
        assert_producer_ended_error(py, &err);
    });
    assert!(
        observed.load(Ordering::SeqCst),
        "the producer should drop only after the observer is waiting"
    );
}

// A StopIteration producer error is wrapped in RuntimeError before
// set_exception, since asyncio.Future.set_exception rejects a StopIteration
// (PEP 479) with a TypeError that would hang the awaiter.
// Attests HDL-7.
#[test]
fn complete_asyncio_future_wraps_stop_iteration() {
    ensure_python();
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let event_loop = py
            .import("asyncio")
            .unwrap()
            .call_method0("new_event_loop")
            .unwrap();
        let fut = event_loop.call_method0("create_future").unwrap();
        let stop_iteration = py.get_type::<PyStopIteration>().call0().unwrap().unbind();
        // Must not raise (a raw set_exception(StopIteration) would TypeError).
        complete_asyncio_future(&fut, true, stop_iteration).unwrap();
        let exc = fut.call_method0("exception").unwrap();
        assert!(
            exc.is_instance_of::<PyRuntimeError>(),
            "a StopIteration producer error should be wrapped in RuntimeError"
        );
        assert!(
            !exc.is_instance_of::<PyStopIteration>(),
            "the wrapped error must not remain a StopIteration"
        );
        event_loop.call_method0("close").unwrap();
    });
}

// A StopIteration SUBCLASS is also wrapped in RuntimeError: we wrap any
// StopIteration, not just the exact type, so the awaiter never hits the
// set_exception TypeError hang.
// Attests HDL-7.
#[test]
fn complete_asyncio_future_wraps_stop_iteration_subclass() {
    ensure_python();
    monarch_with_gil_blocking(GilSite::Test, |py| {
        let event_loop = py
            .import("asyncio")
            .unwrap()
            .call_method0("new_event_loop")
            .unwrap();
        let fut = event_loop.call_method0("create_future").unwrap();
        let helper = PyModule::from_code(
            py,
            cr#"
class MyStop(StopIteration):
    pass
"#,
            c"my_stop.py",
            c"my_stop",
        )
        .unwrap();
        let subclass_exc = helper.getattr("MyStop").unwrap().call0().unwrap().unbind();
        complete_asyncio_future(&fut, true, subclass_exc).unwrap();
        let exc = fut.call_method0("exception").unwrap();
        assert!(
            exc.is_instance_of::<PyRuntimeError>(),
            "a StopIteration subclass should be wrapped in RuntimeError"
        );
        assert!(
            !exc.is_instance_of::<PyStopIteration>(),
            "the wrapped error must not remain a StopIteration"
        );
        event_loop.call_method0("close").unwrap();
    });
}
