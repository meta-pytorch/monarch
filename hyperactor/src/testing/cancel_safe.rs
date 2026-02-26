/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Utilities for testing **cancel safety** of futures.
//!
//! # What does "cancel-safe" mean?
//!
//! A future is *cancel-safe* if, at **any** `Poll::Pending` boundary:
//!
//! 1. **State remains valid** – dropping the future there does not
//!    violate external invariants or leave shared state corrupted.
//! 2. **Restartability holds** – from that state, constructing a
//!    fresh future for the same logical operation can still run to
//!    completion and produce the expected result.
//! 3. **No partial side effects** – cancellation never leaves behind
//!    a visible "half-done" action; effects are either not started,
//!    or fully completed in an idempotent way.
//!
//! # Why cancel-safety matters
//!
//! Executors are free to drop futures after any `Poll::Pending`. This
//! means that cancellation is not an exceptional path – it is *part
//! of the normal contract*. A cancel-unsafe future can leak
//! resources, corrupt protocol state, or leave behind truncated I/O.
//!
//! # What this module offers
//!
//! This module provides helpers (`assert_cancel_safe`,
//! `assert_cancel_safe_async`) that:
//!
//! - drive a future to completion once, counting its yield points,
//! - then for every possible cancellation boundary `k`, poll a fresh
//!   future `k` times, drop it, and finally ensure a **new run**
//!   still produces the expected result.
//!
//! # Examples
//!
//! - ✓ Pure/logical futures: simple state machines with no I/O (e.g.
//!   yields twice, then return 42).
//! - ✓ Framed writers that stage bytes internally and only commit
//!   once the frame is fully written.
//! - ✗ Writers that flush a partial frame before returning `Pending`.
//! - ✗ Futures that consume from a shared queue before `Pending` and
//!   drop without rollback.

use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::task::RawWaker;
use std::task::RawWakerVTable;
use std::task::Waker;

/// A minimal no-op waker for manual polling.
fn noop_waker() -> Waker {
    fn clone(_: *const ()) -> RawWaker {
        RawWaker::new(std::ptr::null(), &VTABLE)
    }
    fn wake(_: *const ()) {}
    fn wake_by_ref(_: *const ()) {}
    fn drop(_: *const ()) {}
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);
    // SAFETY: The vtable doesn't use the data pointer.
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
}

/// Poll a future once.
fn poll_once<F: Future + Unpin>(fut: &mut F, cx: &mut Context<'_>) -> Poll<F::Output> {
    Pin::new(fut).poll(cx)
}

/// Drive a fresh future to completion, returning (`pending_count`,
/// `out`). `pending_count` is the number of times the future returned
/// `Poll::Pending` before it finally resolved to `Poll::Ready`.
fn run_to_completion_count_pending<F, T>(mut mk: impl FnMut() -> F) -> (usize, T)
where
    F: Future<Output = T>,
{
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    let mut fut = Box::pin(mk());
    let mut pending_count = 0usize;

    loop {
        match poll_once(&mut fut, &mut cx) {
            Poll::Ready(out) => return (pending_count, out),
            Poll::Pending => {
                pending_count += 1;
                // Nothing else to do: we are just counting yield
                // points.
            }
        }
    }
}

/// Runtime-independent version: on each `Poll::Pending`, we just poll
/// again. Suitable for pure/logical futures that don’t rely on
/// timers, IO, or other external progress driven by an async runtime.
pub fn assert_cancel_safe<F, T>(mut mk: impl FnMut() -> F, expected: &T)
where
    F: Future<Output = T>,
    T: Debug + PartialEq,
{
    // 1) Establish ground truth and number of yield points.
    let (pending_total, out) = run_to_completion_count_pending(&mut mk);
    assert_eq!(&out, expected, "baseline run output mismatch");

    // 2) Cancel at every poll boundary k, then ensure a fresh run
    // still matches.
    for k in 0..=pending_total {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Poll exactly k times (dropping afterwards).
        {
            let mut fut = Box::pin(mk());
            for _ in 0..k {
                if poll_once(&mut fut, &mut cx).is_ready() {
                    // Future completed earlier than k: no
                    // cancellation point here. Drop and move on to
                    // next k.
                    break;
                }
            }
            // Drop here = "cancellation".
            drop(fut);
        }

        // 3) Now ensure we can still complete cleanly and match
        // expected. This verifies cancelling at this boundary didn’t
        // corrupt global state or violate invariants needed for a
        // clean, subsequent run.
        let (_, out2) = run_to_completion_count_pending(&mut mk);
        assert_eq!(
            &out2, expected,
            "output mismatch after cancelling at poll #{k}"
        );
    }
}

/// Cancel-safety check for async futures. On every `Poll::Pending`,
/// runs `on_pending().await` to drive external progress (e.g.
/// advancing a paused clock or IO). Cancels at each yield boundary
/// and ensures a fresh run still produces `expected`.
pub async fn assert_cancel_safe_async<F, T, P, FutStep>(
    mut mk: impl FnMut() -> F,
    expected: &T,
    mut on_pending: P,
) where
    F: Future<Output = T>,
    T: Debug + PartialEq,
    P: FnMut() -> FutStep,
    FutStep: Future<Output = ()>,
{
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    // 1) First, establish expected + number of pendings with the
    // ability to drive progress.
    let mut pending_total = 0usize;
    {
        let mut fut = Box::pin(mk());
        loop {
            match poll_once(&mut fut, &mut cx) {
                Poll::Ready(out) => {
                    assert_eq!(&out, expected, "baseline run output mismatch");
                    break;
                }
                Poll::Pending => {
                    pending_total += 1;
                    on_pending().await;
                }
            }
        }
    }

    // 2) Cancel at each poll boundary.
    for k in 0..=pending_total {
        // Poll exactly k steps, advancing external progress each
        // time.
        {
            let mut fut = Box::pin(mk());
            for _ in 0..k {
                match poll_once(&mut fut, &mut cx) {
                    Poll::Ready(_) => break, // Completed earlier than k
                    Poll::Pending => on_pending().await,
                }
            }
            drop(fut); // cancellation
        }

        // 3) Then ensure a clean full completion still yields
        // expected.
        {
            let mut fut = Box::pin(mk());
            loop {
                match poll_once(&mut fut, &mut cx) {
                    Poll::Ready(out) => {
                        assert_eq!(
                            &out, expected,
                            "output mismatch after cancelling at poll #{k}"
                        );
                        break;
                    }
                    Poll::Pending => on_pending().await,
                }
            }
        }
    }
}

/// Convenience macro for `assert_cancel_safe`.
///
/// Example:
/// ```ignore
/// assert_cancel_safe!(CountToThree { step: 0 }, 42);
/// ```
///
/// - `my_future_expr` is any expression that produces a fresh future
///   when evaluated (e.g. `CountToThree { step: 0 }`).
/// - `expected_value` is the value you expect the future to resolve
///   to. **Pass a plain value, not a reference**. The macro will take a
///   reference internally.
#[macro_export]
macro_rules! assert_cancel_safe {
    ($make_future:expr, $expected:expr) => {{ $crate::testing::cancel_safe::assert_cancel_safe(|| $make_future, &$expected) }};
}

/// Async convenience macro for `assert_cancel_safe_async`.
///
/// Example:
/// ```ignore
/// assert_cancel_safe_async!(
///     two_sleeps(),
///     7,
///     || async { tokio::time::advance(std::time::Duration::from_millis(1)).await }
/// );
/// ```
///
/// - `my_future_expr` is any expression that produces a fresh future
///   when evaluated (e.g. `two_sleeps()`).
/// - `expected_value` is the value you expect the future to resolve
///   to. **Pass a plain value, not a reference**. The macro will take
///   a reference internally.
/// - `on_pending` is a closure that returns an async block, used to
///   drive external progress each time the future yields
///   `Poll::Pending`.
#[macro_export]
macro_rules! assert_cancel_safe_async {
    ($make_future:expr, $expected:expr, $on_pending:expr) => {{
        $crate::testing::cancel_safe::assert_cancel_safe_async(
            || $make_future,
            &$expected,
            $on_pending,
        )
        .await
    }};
}

#[cfg(test)]
mod tests {
    use tokio::time::Duration;
    use tokio::time::{self};

    use super::*;

    // A future that yields twice, then returns a number.
    struct CountToThree {
        step: u8,
    }

    impl Future for CountToThree {
        type Output = u8;

        fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.step += 1;
            match self.step {
                1 | 2 => Poll::Pending, // yield twice...
                3 => Poll::Ready(42),   // ... 3rd time's a charm
                _ => panic!("polled after completion"),
            }
        }
    }

    // Smoke test: verify that a simple state-machine future (yields
    // twice, then completes) passes the cancel-safety checks.
    #[test]
    fn test_count_to_three_cancel_safe() {
        assert_cancel_safe!(CountToThree { step: 0 }, 42u8);
    }

    // A future that waits for two sleeps (1ms each), then returns 7.
    #[allow(clippy::disallowed_methods)]
    async fn two_sleeps() -> u8 {
        time::sleep(Duration::from_millis(1)).await;
        time::sleep(Duration::from_millis(1)).await;
        7
    }

    // Smoke test: verify that a timer-based async future (with two
    // sleeps) passes the async cancel-safety checks under tokio's
    // mocked time.
    #[tokio::test(start_paused = true)]
    async fn test_two_sleeps_cancel_safe_async() {
        assert_cancel_safe_async!(two_sleeps(), 7, || async {
            time::advance(Duration::from_millis(1)).await
        });
    }
}
