/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

mod asyncio_observers;
mod blocking_observers;
mod core_transitions;
mod receive_driving;
mod reconstruction;
mod rust_observers;

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use pyo3::exceptions::PyRuntimeError;
use pyo3::types::PyModule;

use super::*;
use crate::handle::core::Observed;
use crate::handle::core::ReconstructionClaim;
use crate::handle_wake::LoopWake;

// Build a still-pending `Handle` and return its publisher so the test
// drives completion explicitly.
fn pending_handle() -> (Publisher, PyHandle) {
    PyHandle::pending()
}

/// A module-private reconstructor, not a Python producer or endpoint fixture.
struct TestReply {
    calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    rebuild: Box<dyn Fn(Python<'_>, usize) -> PyResult<Py<PyAny>> + Send + Sync>,
}

impl Reconstruct for TestReply {
    fn reconstruct(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let attempt = self.calls.fetch_add(1, Ordering::SeqCst);
        (self.rebuild)(py, attempt)
    }
}

impl Drop for TestReply {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn test_reply(
    rebuild: impl Fn(Python<'_>, usize) -> PyResult<Py<PyAny>> + Send + Sync + 'static,
) -> (Arc<dyn Reconstruct>, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    (
        Arc::new(TestReply {
            calls: Arc::clone(&calls),
            drops: Arc::clone(&drops),
            rebuild: Box::new(rebuild),
        }),
        calls,
        drops,
    )
}

fn raw_claim(handle: &PyHandle) -> ReconstructionClaim {
    match handle.core.observe() {
        Observed::Reconstruct(claim) => claim,
        _ => panic!("the raw reply should be available to claim"),
    }
}

fn channel<'py>(py: Python<'py>, event_loop: &Bound<'py, PyAny>) -> Bound<'py, LoopWake> {
    LoopWake::for_loop(py, event_loop)
        .expect("the loop's channel")
        .expect("a selector loop uses the pipe")
}

fn assert_producer_ended_error(py: Python<'_>, error: &PyErr) {
    assert!(
        error.is_instance_of::<PyRuntimeError>(py),
        "a dropped producer should surface a RuntimeError, got {error}"
    );
    assert_eq!(
        error.to_string(),
        "RuntimeError: Handle producer ended without publishing a result"
    );
}

// A Python helper module providing loop drivers for the `asyncio` tests.
fn loop_helper(py: Python<'_>) -> Bound<'_, PyModule> {
    let code = cr#"
import asyncio
import warnings

async def _await(h):
    return await h

def run_await(h):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_await(h))
    finally:
        loop.close()

async def _get_on_loop(h):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        value = h.get()
    return (value, any(issubclass(w.category, UserWarning) for w in caught))

def run_get_on_loop(h):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_get_on_loop(h))
    finally:
        loop.close()

async def _get_on_loop_strict(h):
    # get() under a warnings-as-errors filter: a ready value must still return
    # (no-warn fast path), not raise the UserWarning as an error.
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        return h.get()

def run_get_on_loop_strict(h):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_get_on_loop_strict(h))
    finally:
        loop.close()

async def _cancel(h):
    f = h.as_asyncio()
    was_done = f.done()
    accepted = f.cancel()
    await asyncio.sleep(0.05)
    return (was_done, accepted, f.cancelled(), h.poll())

def run_cancel(h):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_cancel(h))
    finally:
        loop.close()

async def _two(h):
    a = h.as_asyncio()
    b = h.as_asyncio()
    # gather returns a list; the Rust side downcasts a tuple
    return tuple(await asyncio.gather(a, b))

def run_two(h):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_two(h))
    finally:
        loop.close()

class CountingLoop(asyncio.SelectorEventLoop):
    # Counts only the public add_reader, which the wake channel uses; the
    # loop's own self-pipe goes through _add_reader.
    def __init__(self):
        super().__init__()
        self.add_reader_calls = 0

    def add_reader(self, fd, callback, *args):
        self.add_reader_calls += 1
        return super().add_reader(fd, callback, *args)

def observe_ready(h):
    # (done when returned, the outcome or the exception, add_reader calls)
    loop = CountingLoop()

    async def observe():
        fut = h.as_asyncio()
        done = fut.done()
        try:
            outcome = await fut
        except BaseException as e:
            outcome = e
        return (done, outcome, loop.add_reader_calls)

    try:
        return loop.run_until_complete(observe())
    finally:
        loop.close()

def ready_await_order(h):
    order = []

    async def a():
        order.append("a1")
        await h
        order.append("a2")

    async def b():
        order.append("b")

    async def main():
        await asyncio.gather(a(), b())

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    return order
"#;
    PyModule::from_code(py, code, c"handle_test_helper.py", c"handle_test_helper").unwrap()
}
