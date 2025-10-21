/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This module implements a cancellation-safe zero-copy framer for network channels.

use std::fmt;
use std::io;
use std::io::IoSlice;
use std::mem::take;
use std::task::Poll;

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use futures::future::poll_fn;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::ReadBuf;

/// A FrameReader reads frames from an underlying [`AsyncRead`].
pub struct FrameReader<R> {
    reader: R,
    max_frame_length: usize,
    state: FrameReaderState,
}

enum FrameReaderState {
    /// Accumulating 8-byte length prefix.
    ReadLen { buf: [u8; 8], off: usize },
    /// Accumulating body of exactly `len` bytes.
    ReadBody { buf: Vec<u8>, len: usize }, // buf.len() <= len
}

impl<R: AsyncRead + Unpin> FrameReader<R> {
    /// Create a new framer for `reader`. Frames exceeding `max_frame_length`
    /// in length result in an irrecoverable reader error.
    pub fn new(reader: R, max_frame_length: usize) -> Self {
        Self {
            reader,
            max_frame_length,
            state: FrameReaderState::ReadLen {
                buf: [0; 8],
                off: 0,
            },
        }
    }

    /// Read the next frame from the underlying reader. If the frame
    /// exceeds the configured maximum length, `next` returns an
    /// `io::ErrorKind::InvalidData` error.
    ///
    /// The method is cancellation safe in the sense that, if it is
    /// used in a branch of a `tokio::select!` block, frames are never
    /// dropped.
    ///
    /// # Errors
    ///
    /// * Returns `io::ErrorKind::InvalidData` if a frame exceeds
    ///   `max_frame_length`. **This error is fatal:** once returned,
    ///   the `FrameReader` must be dropped; the underlying connection
    ///   is no longer valid.
    pub async fn next(&mut self) -> io::Result<Option<Bytes>> {
        loop {
            match &mut self.state {
                FrameReaderState::ReadLen { buf, off } if *off < 8 => {
                    let n = self.reader.read(&mut buf[*off..]).await?;
                    *off += n;
                    assert!(*off <= 8);

                    // https://docs.rs/tokio/latest/tokio/io/trait.AsyncReadExt.html#method.read
                    // "This reader has reached its "end of file" and will likely no longer
                    // be able to produce bytes. Note that this does not mean that the reader
                    // will always no longer be able to produce bytes."
                    //
                    // In practice, this means EOF.
                    if n == 0 {
                        if *off == 0 {
                            // We ended on a frame boundary. End of stream:
                            return Ok(None);
                        } else {
                            return Err(io::ErrorKind::UnexpectedEof.into());
                        }
                    }
                }

                FrameReaderState::ReadLen { buf, off } => {
                    assert_eq!(*off, 8);
                    let len = (&buf[..]).get_u64() as usize;
                    if len > self.max_frame_length {
                        return Err(io::ErrorKind::InvalidData.into());
                    }
                    self.state = FrameReaderState::ReadBody {
                        buf: Vec::with_capacity(len),
                        len,
                    };
                }

                FrameReaderState::ReadBody { buf, len } if buf.len() < *len => {
                    let num_to_read = *len - buf.len();

                    let num_read = poll_fn(|cx| {
                        let spare = buf.spare_capacity_mut();

                        assert!(spare.len() >= num_to_read);

                        let mut buf = ReadBuf::uninit(&mut spare[..num_to_read]);

                        match std::pin::Pin::new(&mut self.reader).poll_read(cx, &mut buf) {
                            Poll::Ready(Ok(())) => Poll::Ready(Ok(buf.filled().len())),
                            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                            Poll::Pending => Poll::Pending,
                        }
                    })
                    .await?;

                    if num_read == 0 {
                        return Err(io::ErrorKind::UnexpectedEof.into());
                    }
                    // SAFETY: Adding the number of bytes that were just read should be the correct length of this buffer
                    unsafe {
                        buf.set_len(buf.len() + num_read);
                    }
                }

                FrameReaderState::ReadBody { buf, len } => {
                    assert_eq!(buf.len(), *len);
                    let frame = take(buf).into();
                    self.state = FrameReaderState::ReadLen {
                        buf: [0; 8],
                        off: 0,
                    };
                    return Ok(Some(frame));
                }
            }
        }
    }
}

/// A Writer for message frames. `FrameWrite` requires the user to drive
/// the underlying state machines through (possibly) successive calls to
/// `send`, retaining cancellation safety. The `FrameWrite` owns the underlying
/// writer until the frame has been written to completion.
pub struct FrameWrite<W, B> {
    writer: W,
    len_buf: Bytes,
    body: B,
}

impl<W, B: Buf> fmt::Debug for FrameWrite<W, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FrameWrite")
            .field("len_buf(remaining)", &self.len_buf.remaining())
            .field("body(remaining)", &self.body.remaining())
            .finish()
    }
}

impl<W: AsyncWrite + Unpin, B: Buf> FrameWrite<W, B> {
    /// Create a new frame writer, writing `body` to `writer`.
    ///
    /// The frame is length-prefixed with an 8-byte big-endian `u64`.
    ///
    /// # Arguments
    ///
    /// * `writer` — the `AsyncWrite` sink to write into.
    /// * `body` — the serialized frame body to send.
    /// * `max_len` — maximum allowed frame length; frames larger than this
    ///   yield an `io::ErrorKind::InvalidData`.
    ///
    /// # Returns
    ///
    /// On success, returns a new [`FrameWrite`] ready to write
    /// `body`.
    /// On error, returns the I/O error if the frame length exceeds
    /// `max_len`.
    /// frame length exceeds `max_len`.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use bytes::Bytes;
    /// use hyperactor::channel::net::framed::FrameWrite;
    ///
    /// // `writer` is any AsyncWrite + Unpin (e.g. a tokio `WriteHalf`)
    /// let mut fw = FrameWrite::new(writer, Bytes::from_static(b"hello"), 1024)?;
    /// fw.send().await?;
    /// let writer = fw.complete();
    /// ```
    ///
    /// # See also
    ///
    /// For a one-shot convenience wrapper, see [`write_frame`].
    pub fn new(writer: W, body: B, max_len: usize) -> Result<Self, (W, io::Error)> {
        let len = body.remaining();
        if len > max_len {
            return Err((
                writer,
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("frame length {} exceeds max {}", len, max_len),
                ),
            ));
        }
        let mut len_buf = BytesMut::with_capacity(8);
        len_buf.put_u64(len as u64);
        let len_buf = len_buf.freeze();
        Ok(Self {
            writer,
            len_buf,
            body,
        })
    }

    /// Drive the underlying state machine. The frame is written when this
    /// `send` returns successfully.
    ///
    /// This method is cancellation safe in the sense that each invocation to `send`
    /// preserves progress (the future can be safely dropped at any time). Thus, the
    /// user can drive the state machine by calling `send` multiple times, dropping the
    /// returned futures at any time. Upon completion, the frame is guaranteed to be
    /// written, unless an error was encountered, in which case the underlying stream
    /// is in an undefined state.
    pub async fn send(&mut self) -> io::Result<()> {
        loop {
            if self.len_buf.has_remaining() {
                self.writer.write_all_buf(&mut self.len_buf).await?;
            } else if self.body.has_remaining() {
                // This is safe. According to the docs for write_vectored:
                //
                // > This method is cancellation safe in the sense that if it is used as
                // > the event in a [`tokio::select!`](crate::select) statement and some
                // > other branch completes first, then it is guaranteed that no data was
                // > written to this `AsyncWrite`.
                //
                // We write at most 4 chunks at a time (to be tuned). We may also consider
                // using MaybeUninit here to avoid initialization overhead.
                let mut chunks = [
                    IoSlice::new(&[]),
                    IoSlice::new(&[]),
                    IoSlice::new(&[]),
                    IoSlice::new(&[]),
                ];
                let num_chunks = self.body.chunks_vectored(&mut chunks);
                let count = self.writer.write_vectored(&chunks[0..num_chunks]).await?;
                self.body.advance(count);
            } else {
                // Not all transport types do implicit flushes, so we need to
                // explicitly flush here.
                //
                // One example is rusttls, whose doc says:
                // > You must call poll_flush to ensure that it is written to TcpStream.
                // https://docs.rs/tokio-rustls/0.26.2/tokio_rustls/index.html
                self.writer.flush().await?;
                return Ok(());
            }
        }
    }

    /// Complete the write, returning ownership of the underlying writer.
    /// This should only be called after successful sends; at other times
    /// the underlying stream is in an undefined state.
    pub fn complete(self) -> W {
        let Self { writer, .. } = self;
        writer
    }

    /// Writes a single frame into the underlying writer and returns
    /// it.
    ///
    /// This is a convenience for the common pattern:
    /// `FrameWrite::new(writer, bytes, max)?.send().await?.complete()`.
    ///
    /// Frame writes are atomic: either the entire frame is sent, or
    /// an error is returned. No partial frames are observed by the
    /// receiver.
    ///
    /// # Arguments
    ///
    /// * `writer` — the `AsyncWrite` sink to write into.
    /// * `bytes` — the serialized frame body to send.
    /// * `max` — maximum allowed frame length; frames larger than this
    ///   yield an `io::ErrorKind::InvalidData`.
    ///
    /// # Returns
    ///
    /// On success, returns the underlying writer so the caller can
    /// continue using it for further frames. On error, returns both
    /// the writer and the I/O error.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use bytes::Bytes;
    ///
    /// // `writer` is any AsyncWrite + Unpin (e.g. a tokio `WriteHalf`)
    /// let writer = FrameWrite::write_frame(writer, Bytes::from_static(b"hello"), 10usize).await?;
    /// ```
    pub async fn write_frame(writer: W, buf: B, max: usize) -> Result<W, (W, io::Error)> {
        let mut fw = FrameWrite::new(writer, buf, max)?;
        let res = fw.send().await;
        let writer = fw.complete();
        match res {
            Ok(()) => Ok(writer),
            Err(e) => Err((writer, e)),
        }
    }
}

#[cfg(test)]
mod test_support {
    use std::io;
    use std::io::IoSlice;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use proptest::prelude::*;
    use serde_multipart::Message;
    use serde_multipart::Part;

    use super::*;

    /// A wrapper around an `AsyncWrite` that throttles how many bytes
    /// may be written per poll.
    ///
    /// We are going to use this to simulate partial writes to test
    /// cancellation safety: when the budget is 0, `poll_write`
    /// returns `Poll::Pending` and calls the waker so the task is
    /// scheduled to be polled again later.
    #[cfg(test)]
    pub(crate) struct Throttled<W> {
        pub(crate) inner: W,
        // Number of bytes allowed to be written in the next poll. If
        // 0, writes return `Poll::Pending`.
        pub(crate) budget: usize,
    }

    #[cfg(test)]
    impl<W> Throttled<W> {
        pub(crate) fn new(inner: W) -> Self {
            Self {
                inner,
                budget: usize::MAX,
            }
        }

        pub(crate) fn set_budget(&mut self, n: usize) {
            self.budget = n;
        }
    }

    #[cfg(test)]
    impl<W: AsyncWrite + Unpin> AsyncWrite for Throttled<W> {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            // No budget left this poll. Return "not ready" and ask to
            // be polled again later.
            if self.budget == 0 {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            let n = buf.len().min(self.budget);
            self.budget -= n;
            // Delegate a write of the first `n` bytes to the inner
            // writer.
            Pin::new(&mut self.inner).poll_write(cx, &buf[..n])
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            // Delegate to `inner` for flushing.
            Pin::new(&mut self.inner).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            // Delegate to `inner` (ensure resources are released and
            // `EOF` is signaled downstream).
            Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    /// A cloneable writer that delegates to a shared inner `W`.
    pub(crate) struct SharedWriter<W>(pub(crate) Arc<Mutex<W>>);

    impl<W> SharedWriter<W> {
        /// Create a new cloneable writer.
        pub(crate) fn new(w: W) -> Self {
            Self(Arc::new(Mutex::new(w)))
        }

        /// Acquire a blocking lock on the inner writer.
        ///
        /// This returns a [`MutexGuard`] giving mutable access to the
        /// underlying writer `W`. The guard releases the lock when it
        /// is dropped.
        ///
        /// # Panics
        ///
        /// Panics if the mutex is poisoned (i.e., another thread
        /// holding the lock panicked).
        pub(crate) fn lock_guard(&self) -> std::sync::MutexGuard<'_, W> {
            self.0.lock().unwrap()
        }
    }

    // Manual clone avoids needlessly deriving a `Clone` bound on `W`.
    impl<W> Clone for SharedWriter<W> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }

    impl<W: AsyncWrite + Unpin> AsyncWrite for SharedWriter<W> {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let mut w = self.0.lock().unwrap();
            Pin::new(&mut *w).poll_write(cx, buf)
        }

        fn poll_write_vectored(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            let mut w = self.0.lock().unwrap();
            Pin::new(&mut *w).poll_write_vectored(cx, bufs)
        }

        fn is_write_vectored(&self) -> bool {
            let w = self.0.lock().unwrap();
            (*w).is_write_vectored()
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            let mut w = self.0.lock().unwrap();
            Pin::new(&mut *w).poll_flush(cx)
        }

        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            let mut w = self.0.lock().unwrap();
            Pin::new(&mut *w).poll_shutdown(cx)
        }
    }

    /// A shared, waker-aware budget gate for coordinating progress
    /// between async tasks.
    ///
    /// The gate tracks an atomic `budget` of "units" (e.g., bytes
    /// allowed to write). Callers try to consume budget with
    /// [`Gate::take_chunk`]. If none is available (the gate is
    /// closed), `take_chunk` stores the task’s [`Waker`] and returns
    /// `0`, so the caller can yield `Poll::Pending`.
    ///
    /// When more budget is added via [`Gate::add`], the counter is
    /// bumped atomically and the parked waker (if any) is woken,
    /// scheduling the blocked task to be polled again.
    ///
    /// Concurrency:
    ///
    /// - `budget` is an [`AtomicUsize`] so producers/consumers can
    ///   update without a global lock.
    /// - The waker lives behind a [`Mutex<Option<Waker>>`] to update
    ///   safely; we only support a single waiter (sufficient for
    ///   tests).
    /// - `take_chunk` uses a CAS loop so concurrent consumers don’t
    ///   underflow/wrap.
    ///
    /// Typical flow:
    /// - A budgeted writer's `poll_write` calls `take_chunk(want,
    ///   cx)`. If it gets `0`, it returns `Poll::Pending`.
    /// - An external driver (timer/test) calls `add(n)` to replenish
    ///   budget.
    /// - On the next poll, the writer observes budget and makes
    ///   progress.
    #[derive(Clone)]
    pub(crate) struct Gate(pub(crate) Arc<GateInner>);
    pub(crate) struct GateInner {
        pub(crate) budget: AtomicUsize,
        pub(crate) waker: Mutex<Option<Waker>>,
    }

    impl Gate {
        /// Create a new `Gate` with zero initial budget and no
        /// registered waker.
        ///
        /// The returned gate starts in the "closed" state: any call
        /// to [`Gate::take_chunk`] will return `0` until budget is
        /// added via [`Gate::add`]. Once budget is added, tasks may
        /// consume it and will be woken if they had previously parked
        /// on the gate.
        pub(crate) fn new() -> Self {
            Self(Arc::new(GateInner {
                budget: AtomicUsize::new(0),
                waker: Mutex::new(None),
            }))
        }

        /// Add `n` units of budget and wake any parked task.
        ///
        /// Increments the internal counter atomically. If a task had
        /// previously called [`Gate::take_chunk`] and parked its
        /// waker because no budget was available, that waker is
        /// removed and signaled here. On its next poll, the task will
        /// observe the replenished budget and continue making
        /// progress.
        pub(crate) fn add(&self, n: usize) {
            self.0.budget.fetch_add(n, Ordering::AcqRel);
            if let Some(w) = self.0.waker.lock().unwrap().take() {
                w.wake(); // schedule waiter
            }
        }

        /// Try to consume up to `want` units of budget.
        ///
        /// - If some budget is available, atomically subtracts the
        ///   granted amount (up to `want`) and returns it.
        /// - If no budget is available, stores the current task’s
        ///   [`Waker`] so it can be notified when [`Gate::add`]
        ///   replenishes the pool, then returns `0`. The caller
        ///   should yield `Poll::Pending`.
        ///
        /// Internally this uses a CAS loop to prevent underflow when
        /// multiple tasks contend for the budget. Before parking the
        /// waker, it re-checks the budget under the lock to avoid a
        /// lost wakeup race where budget arrives just as the task was
        /// about to sleep.
        ///
        /// Typical usage is inside an I/O primitive’s `poll_write`:
        /// if `take_chunk` returns `0`, the writer yields pending;
        /// otherwise it writes the granted slice of the buffer.
        pub(crate) fn take_chunk(&self, want: usize, cx: &mut Context<'_>) -> usize {
            loop {
                let have = self.0.budget.load(Ordering::Acquire);
                if have == 0 {
                    // Park waker, but re-check budget under the lock
                    // to avoid lost wakeups.
                    let mut slot = self.0.waker.lock().unwrap();
                    // If budget arrived while we were preparing to
                    // park, don't park; try again.
                    if self.0.budget.load(Ordering::Acquire) > 0 {
                        drop(slot); // release the lock before looping
                        continue;
                    }
                    *slot = Some(cx.waker().clone());
                    return 0;
                }
                let grant = have.min(want);
                // Safe subtraction via CAS so we never underflow.
                if self
                    .0
                    .budget
                    .compare_exchange_weak(have, have - grant, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    return grant;
                }
                // raced; retry
            }
        }
    }

    /// A writer wrapper that enforces progress via a shared [`Gate`].
    ///
    /// Both the budget pool *and* the underlying writer are shared:
    /// - All clones of `BudgetedWriter` point to the same [`Gate`],
    ///   so cancellation in one attempt and resumption in another
    ///   draw from the same counter.
    /// - They also share the same underlying writer via
    ///   [`SharedWriter<W>`] (`Arc<Mutex<W>>`), so writes are
    ///   serialized and coordinated across tasks.
    ///
    /// This makes `BudgetedWriter` a useful test harness for
    /// cancellation-safety: multiple futures may be spawned and
    /// cancelled, but they all compete for the same writer and
    /// budget.
    pub(crate) struct BudgetedWriter<W> {
        inner: SharedWriter<W>,
        gate: Gate,
    }

    impl<W> BudgetedWriter<W> {
        /// Construct from a shared writer handle.
        pub(crate) fn new(inner: SharedWriter<W>, gate: Gate) -> Self {
            Self { inner, gate }
        }

        /// Convenience: wrap a raw writer and also return the shared
        /// handle (useful if you want to build multiple wrappers that
        /// share the writer).
        pub(crate) fn from_writer(writer: W, gate: Gate) -> (SharedWriter<W>, Self) {
            let inner = SharedWriter::new(writer);
            let me = Self {
                inner: inner.clone(),
                gate,
            };
            (inner, me)
        }

        /// Access the underlying shared writer (e.g., if you need to
        /// flush/shutdown elsewhere).
        pub(crate) fn inner(&self) -> &SharedWriter<W> {
            &self.inner
        }
    }

    impl<W: AsyncWrite + Unpin> AsyncWrite for BudgetedWriter<W> {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            // Ask the gate how much we're allowed to attempt now.
            // **Cancel-safety rule** (mirrors vectored path):
            //   We must credit back any portion of `grant` not durably
            //   consumed by the inner writer in this poll:
            //   - `Ok(written < grant)` => add(grant - written)
            //   - `Err(_)` => `add(grant)`
            //   - `Pending` => `add(grant)`
            // This prevents budget from "sticking" in flight and
            // guarantees forward progress across
            // cancellations/retries.
            let grant = self.gate.take_chunk(buf.len(), cx);
            if grant == 0 {
                return Poll::Pending;
            }

            debug_assert!(grant <= buf.len());

            // Hold the inner writer lock only for the duration of the
            // *poll*. `poll_*` must not `.await`, so a synchronous
            // lock here is fine. Only attempt to write up to the
            // granted amount.
            let mut guard = self.inner.lock_guard();
            match Pin::new(&mut *guard).poll_write(cx, &buf[..grant]) {
                Poll::Ready(Ok(written)) => {
                    debug_assert!(written <= grant);

                    // If the inner wrote fewer than we granted,
                    // credit back the unused.
                    if written < grant {
                        self.gate.add(grant - written);
                    }
                    Poll::Ready(Ok(written))
                }
                Poll::Ready(Err(e)) => {
                    // Nothing consumed; credit back entire grant.
                    self.gate.add(grant);
                    Poll::Ready(Err(e))
                }
                Poll::Pending => {
                    // Nothing consumed; credit back entire grant.
                    self.gate.add(grant);
                    Poll::Pending
                }
            }
        }

        fn poll_write_vectored(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            // Total bytes we *could* write this call.
            let total_len: usize = bufs.iter().map(|b| b.len()).sum();
            // Check with the gate how many bytes we're allowed to
            // write.
            let grant = self.gate.take_chunk(total_len, cx);
            if grant == 0 {
                return Poll::Pending;
            }

            // Truncate the iovecs to not exceed grant.
            let mut left = grant;
            let mut granted: Vec<IoSlice<'_>> = Vec::with_capacity(bufs.len());
            for s in bufs {
                if left == 0 {
                    break;
                }
                let take = s.len().min(left);
                if take > 0 {
                    // SAFETY: IoSlice derefs to [u8], so slicing is fine.
                    granted.push(IoSlice::new(&s[..take]));
                    left -= take;
                }
            }

            // Hold the inner writer lock only for the duration of the
            // *poll*. `poll_*` must not `.await`, so a synchronous
            // lock here is fine.
            // Invariants at this point:
            //   - `granted`’s total length ≤ `grant` (we truncated
            //     above)
            //   - We must **credit back** any portion of `grant` not
            //     durably consumed to prevent budget from "sticking" in
            //     flight.
            let mut guard = self.inner.lock_guard();
            match Pin::new(&mut *guard).poll_write_vectored(cx, &granted) {
                Poll::Ready(Ok(written)) => {
                    debug_assert!(written <= grant);
                    // Short write: inner accepted fewer bytes than we
                    // were allowed to attempt. Return the unused
                    // portion to the bucket so other waiters can make
                    // progress. (Cancel-safety: no budget is lost.)
                    if written < grant {
                        self.gate.add(grant - written);
                    }
                    Poll::Ready(Ok(written))
                }
                Poll::Ready(Err(e)) => {
                    // Error path consumed nothing from the grant;
                    // refund all of it.
                    self.gate.add(grant);
                    Poll::Ready(Err(e))
                }
                Poll::Pending => {
                    // Not ready: nothing written; refund the entire
                    // grant so the producer/waker can re-schedule us
                    // later without starvation.
                    self.gate.add(grant);
                    Poll::Pending
                }
            }
        }

        fn is_write_vectored(&self) -> bool {
            self.inner.is_write_vectored()
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            let mut guard = self.inner.lock_guard();
            Pin::new(&mut *guard).poll_flush(cx)
        }

        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            let mut guard = self.inner.lock_guard();
            Pin::new(&mut *guard).poll_shutdown(cx)
        }
    }

    /// Generate a random drip sequence of budget increments.
    ///
    /// Each element is the number of units to add in one step.
    /// - Sequence length: 1 to 8 steps
    /// - Each step adds between 1 and 8 units (inclusive)
    ///
    /// Mathematically:
    ///   len(drips) ∈ [1, 8],
    ///   ∀ i, drips[i] ∈ [0, 8].
    ///
    /// Useful for fuzzing `BudgetedWriter` or `FrameWrite` under
    /// cancellation/resumption: the budget is replenished in small,
    /// irregular bursts instead of a single large chunk.
    pub fn budget_drips() -> impl Strategy<Value = Vec<usize>> {
        // length: 1..=8, step: 1..=8 (no zeros)
        prop::collection::vec(1..=8usize, 1..=8)
    }

    /// Generate a single multipart `Part` with up to `max_len` bytes.
    /// Includes empty parts to exercise edge cases and boundary
    /// slicing.
    pub fn multipart_part(max_len: usize) -> impl Strategy<Value = Part> {
        proptest::collection::vec(any::<u8>(), 0..=max_len).prop_map(Part::from)
    }

    /// Generate a `Message` that is either unipart or true multipart
    /// (body + 0..=4 additional parts). Kept small for fast tests.
    pub fn multipart_message() -> impl Strategy<Value = Message> {
        let max_parts = 4usize; // total parts = 1..=4
        let max_len = 64usize; // bytes per part

        prop_oneof![
            // Unipart (compat path): body only
            multipart_part(max_len).prop_map(|body| Message::from_body_and_parts(body, vec![])),
            // Multipart: body + 1..=4 extra parts
            (
                multipart_part(max_len),
                proptest::collection::vec(multipart_part(max_len), 1..=max_parts - 1)
            )
                .prop_map(|(body, parts)| Message::from_body_and_parts(body, parts)),
        ]
    }

    /// Generate a `Message` that is *strictly* multipart (body +
    /// 1..=3 additional parts). Use this when you want to force the
    /// vectored path every time.
    pub fn multipart_message_only() -> impl Strategy<Value = Message> {
        let max_parts = 3usize; // total parts = 2..=3
        let max_len = 64usize;

        (
            multipart_part(max_len),
            proptest::collection::vec(multipart_part(max_len), 1..=max_parts - 1),
        )
            .prop_map(|(body, parts)| Message::from_body_and_parts(body, parts))
    }
}

#[cfg(test)]
mod tests {

    use bytes::Bytes;
    use rand::Rng;
    use rand::thread_rng;
    use test_support::Throttled;
    use tokio::io::AsyncWriteExt;

    use super::*;

    fn random_buffer(max_len: usize) -> Bytes {
        let mut rng = thread_rng();
        let len = rng.gen_range(0..max_len);
        let mut buf = vec![0u8; len];
        rng.fill(buf.as_mut_slice());
        Bytes::from(buf)
    }

    #[tokio::test]
    async fn test_framer_roundtrip() {
        const MAX_LEN: usize = 1024;

        let (reader, writer) = tokio::io::duplex(MAX_LEN + 8);
        let mut reader = FrameReader::new(reader, MAX_LEN);

        let mut writer = Some(writer);

        for _ in 0..1024 {
            let body = random_buffer(MAX_LEN);
            let mut frame_write = FrameWrite::new(writer.take().unwrap(), body.clone(), MAX_LEN)
                .map_err(|(_, e)| e)
                .unwrap();
            frame_write.send().await.unwrap();
            writer = Some(frame_write.complete());

            let frame = reader.next().await.unwrap().unwrap();
            assert_eq!(frame, body);
        }
    }

    #[tokio::test]
    async fn test_write_frame_smoke() {
        const MAX_LEN: usize = 1024;

        let (a, b) = tokio::io::duplex(4096);
        let (r, _w_unused) = tokio::io::split(a);
        let (_r_unused, w) = tokio::io::split(b);

        let mut reader = FrameReader::new(r, MAX_LEN);

        eprintln!("write 1");
        let w = FrameWrite::write_frame(w, Bytes::from_static(b"hello"), MAX_LEN)
            .await
            .map_err(|(_, e)| e)
            .unwrap();
        eprintln!("write 2");
        let _ = FrameWrite::write_frame(w, Bytes::from_static(b"world"), MAX_LEN)
            .await
            .map_err(|(_, e)| e)
            .unwrap();

        eprintln!("read 1");
        let f1 = reader.next().await.unwrap().unwrap();
        eprintln!("read 2");
        let f2 = reader.next().await.unwrap().unwrap();

        assert_eq!(f1.as_ref(), b"hello");
        assert_eq!(f2.as_ref(), b"world");
    }

    #[tokio::test]
    async fn test_reader_eof_at_boundary() {
        const MAX_LEN: usize = 1024;

        let (a, b) = tokio::io::duplex(4096);
        let (r, _wu) = tokio::io::split(a);
        let (_ru, mut w) = tokio::io::split(b);
        let mut reader = FrameReader::new(r, 1024);

        // Write a complete frame.
        w = FrameWrite::write_frame(w, Bytes::from_static(b"done"), MAX_LEN)
            .await
            .map_err(|(_, e)| e)
            .unwrap();
        // Now, shutdown the writer so the peer gets an EOF.
        w.shutdown().await.unwrap();
        drop(w);
        assert_eq!(
            reader.next().await.unwrap(),
            Some(Bytes::from_static(b"done"))
        );
        // Boundary EOF.
        assert!(reader.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_reader_eof_mid_frame() {
        const MAX_LEN: usize = 1024;

        let (a, b) = tokio::io::duplex(4096);
        let (r, _wu) = tokio::io::split(a);
        let (_ru, mut w) = tokio::io::split(b);
        let mut reader = FrameReader::new(r, MAX_LEN);

        // Start a frame of length 5.
        let mut len = bytes::BytesMut::with_capacity(8);
        len.put_u64(5);
        w.write_all(&len.freeze()).await.unwrap();
        // Write only 2 bytes of the body.
        w.write_all(b"he").await.unwrap();
        // Shutdown the writer so there's an EOF mid frame.
        w.shutdown().await.unwrap();

        // Reading back the frame will manifest an error.
        let err = reader.next().await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    #[allow(clippy::disallowed_methods)]
    async fn test_writer_cancellation_resume() {
        const MAX_LEN: usize = 1024 * 1024;

        let (a, b) = tokio::io::duplex(4096);
        let (r, _wu) = tokio::io::split(a);
        let (_ru, w) = tokio::io::split(b);

        let w = Throttled::new(w);
        // 256 bytes, all = 0x2A ('*'), "the answer"
        let body = Bytes::from_static(&[42u8; 256]);
        let mut reader = FrameReader::new(r, MAX_LEN);
        let mut fw = FrameWrite::new(w, body.clone(), MAX_LEN)
            .map_err(|(_, e)| e)
            .unwrap();

        // Allow only the 8-byte length to be written, then cancel.
        fw.writer.set_budget(8);
        let fut = fw.send();
        tokio::select! {
            _ = fut => panic!("send unexpectedly completed"),
            _ = tokio::time::sleep(std::time::Duration::from_millis(5)) => {}
        }
        // The `fut` is dropped here i.e. "cancellation".
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), async {
                reader.next().await
            })
            .await
            .is_err(),
            "a full frame isn't available yet, so reader.next().await should block"
        );

        // Now allow the remaining body to flush and complete the
        // frame.
        fw.writer.set_budget(usize::MAX);
        fw.send().await.unwrap();
        let mut w = fw.complete();
        let got = reader.next().await.unwrap().unwrap();
        assert_eq!(got, body);

        // Shutdown and test for EOF on boundary.
        w.shutdown().await.unwrap();
        assert!(reader.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_reader_accepts_exact_max_len_frames() {
        const MAX: usize = 1024;
        const BUFSIZ: usize = 8 + MAX; // BUFSIZ (bytes) = 8 (len) + MAX (body)
        let (a, b) = tokio::io::duplex(BUFSIZ);
        let (r, _wu) = tokio::io::split(a);
        let (_ru, mut w) = tokio::io::split(b);
        let mut reader = FrameReader::new(r, MAX);

        let bytes_written = Bytes::from(vec![0xAB; MAX]);
        w = FrameWrite::write_frame(w, bytes_written.clone(), MAX)
            .await
            .map_err(|(_, e)| e)
            .unwrap();

        let bytes_read = reader.next().await.unwrap().unwrap();
        assert_eq!(bytes_read.len(), MAX);
        assert_eq!(bytes_read, bytes_written);

        w.shutdown().await.unwrap();
        assert!(reader.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_reader_rejects_over_max_len_frames() {
        const MAX: usize = 1024;
        const BUFSIZ: usize = 8 + MAX; // BUFSIZ (bytes) = 8 (len) + MAX (body)
        let (a, b) = tokio::io::duplex(BUFSIZ);
        let (r, _wu) = tokio::io::split(a);
        let (_ru, mut w) = tokio::io::split(b);
        let mut reader = FrameReader::new(r, MAX - 1);

        let bytes_written = Bytes::from(vec![0xAB; MAX]);
        w = FrameWrite::write_frame(w, bytes_written, MAX)
            .await
            .map_err(|(_, e)| e)
            .unwrap();

        let err = reader.next().await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);

        // Do NOT try to use `reader` beyond this point! There has
        // been a protocol violation: `InvalidData` means the stream
        // is corrupted and the only valid thing you can do with it is
        // `drop` it.
        drop(reader);

        w.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_reader_accepts_zero_len_frames() {
        const MAX: usize = 0;
        const BUFSIZ: usize = 8 + MAX; // BUFSIZ (bytes) = 8 (len) + MAX (body)
        let (a, b) = tokio::io::duplex(BUFSIZ);
        let (r, _wu) = tokio::io::split(a);
        let (_ru, mut w) = tokio::io::split(b);
        let mut reader = FrameReader::new(r, MAX);

        w = FrameWrite::write_frame(w, Bytes::new(), 0)
            .await
            .map_err(|(_, e)| e)
            .unwrap();
        assert_eq!(reader.next().await.unwrap().unwrap().len(), 0);
        w = FrameWrite::write_frame(w, Bytes::new(), 0)
            .await
            .map_err(|(_, e)| e)
            .unwrap();
        assert_eq!(reader.next().await.unwrap().unwrap().len(), 0);

        w.shutdown().await.unwrap();
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[cfg(test)]
mod property_tests {
    use proptest::prelude::*;

    use super::test_support::*;
    use super::*;
    use crate::assert_cancel_safe_async;

    // Theorem: For all generated drip sequences `drips`, the
    // following hold:
    //   1. 1 ≤ len(drips) ≤ 8
    //   2. ∀ i ∈ [0, len(drips)), 0 ≤ drips[i] ≤ 8
    proptest! {
        #[test]
        fn test_budget_sequence(drips in budget_drips()) {
            // 1. length bound
            prop_assert!((1..=8).contains(&drips.len()));

            // 2. value bounds; lower bound is tautological for usize
            prop_assert!(drips.iter().all(|&n| n <= 8));
        }
    }

    proptest! {
        // Sanity: multipart_message() yields either unipart or 1..=3
        // extra parts, frames to the advertised length, and
        // round-trips via from_framed().
        #[test]
        fn test_multipart_message_shape(msg in test_support::multipart_message()) {
            // Parts count bounds (unipart allowed)
            prop_assert!(msg.num_parts() <= 3);

            // `frame_len` matches actual framed length
            let mut framed = msg.clone().framed();
            let framed_bytes = framed.copy_to_bytes(framed.remaining());
            prop_assert_eq!(framed_bytes.len(), msg.frame_len());

            // round-trip `framed` → `Message` → `framed` produces
            // identical bytes
            let rt = serde_multipart::Message::from_framed(framed_bytes.clone()).unwrap();
            let mut rt_framed = rt.framed();
            let rt_bytes = rt_framed.copy_to_bytes(rt_framed.remaining());
            prop_assert_eq!(rt_bytes, framed_bytes);
        }

        // Sanity: multipart_message_only() always yields *strictly*
        // multipart (≥1 extra part), and also round-trips and
        // length-checks as above.
        #[test]
        fn test_multipart_message_only_shape(msg in test_support::multipart_message_only()) {
            // Strictly multipart.
            prop_assert!(msg.num_parts() >= 1 && msg.num_parts() <= 3);

            let mut framed = msg.clone().framed();
            let framed_bytes = framed.copy_to_bytes(framed.remaining());
            prop_assert_eq!(framed_bytes.len(), msg.frame_len());

            let rt = serde_multipart::Message::from_framed(framed_bytes.clone()).unwrap();
            let mut rt_framed = rt.framed();
            let rt_bytes = rt_framed.copy_to_bytes(rt_framed.remaining());
            prop_assert_eq!(rt_bytes, framed_bytes);
        }
    }

    // Theorem: `FrameWrite::send` is cancel-safe.
    //
    // Matches the cancel-safety contract from `test_utils::cancel_safe`:
    // - State remains valid across cancellations.
    // - Restartability: a fresh `send` can resume from shared state.
    // - No partial side effects: either no frame is observed, or the
    //   complete frame is observed.
    //
    // Semi-formal:
    //   ∀ drip sequences D, ∀ finite cancellation schedules C:
    //     if Σ D ≥ 8 + |body|, then
    //       restarting `send(body)` under C eventually yields Ok(())
    //       and the reader observes exactly one frame = `body`.
    //
    // Intuition: Even if the future is cancelled at any
    // `Poll::Pending`, shared state (`Gate` + `SharedWriter`) ensures
    // eventual completion with the correct frame, provided enough
    // budget is dripped.
    proptest! {
        #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]
        #[test]
        fn framewrite_cancellation_is_safe(drips in budget_drips()) {
            // proptest! generates a plain `#[test]`, not
            // `#[tokio::test]`, so no runtime is provided
            // automatically. Hence here we build a small
            // current-thread runtime with time enabled to drive the
            // async code.
            let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
            // Block the async test body in this runtime.
            rt.block_on(async move {
                let (a, b) = tokio::io::duplex(4096);
                let (r, _wu) = tokio::io::split(a);
                let (_ru, w) = tokio::io::split(b);

                let gate = Gate::new();
                let shared = SharedWriter::new(w);

                // Small body for fewer polls; termination should
                // occur due to the fallback `total_need` (len-prefix
                // + body), wakeups via `Gate::add`, and the outer
                // timeout. Still hedged: bugs in wakeup/budget logic
                // could in theory stall progress, hence the timeout.
                let body = Bytes::from_static(&[42u8; 64]);
                let mut reader = FrameReader::new(r, 1024 * 1024);

                // Seed the drip sequence with 8 so the 8-byte length
                // prefix can always be written. Without this, leading
                // zeros can stall the prefix, inflate the number of
                // pending polls (O(P²) across cancel points), and
                // waste fuzz time without exercising the
                // cancel/resume path on the body.
                let mut drips = drips.clone();
                drips.insert(0, 8);

                let idx = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
                let pending_ticks = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
                let total_need = 8 + body.len();

                // Wrap the whole run in a 2s timeout. This does not
                // affect the success criterion (theorem is about
                // eventual completion), but acts as a guardrail: if
                // wakeups or budget logic regresses and the future
                // stalls forever, the test fails quickly instead of
                // hanging the fuzz run.
                #[allow(clippy::disallowed_methods)]
                tokio::time::timeout(std::time::Duration::from_secs(2), async {
                    assert_cancel_safe_async!(
                        // `mk`: Build a *fresh* send future each attempt.
                        // - We clone `shared` and `gate` (both
                        //   `Arc`-backed), so the underlying writer +
                        //   budget pool persist across cancellations.
                        // - We construct a new `FrameWrite` each time
                        //   (the future under test), so
                        //   `assert_cancel_safe_async` can cancel at
                        //   any Pending boundary, drop it, and then
                        //   retry from the same shared world state.
                        // - Map `Result<(), io::Error>` →
                        //   `Result<(),()>` so the `expected` value
                        //   (`Ok(())`) is equality comparable (requires
                        //   `PartialEq`).
                        {
                            let bw = BudgetedWriter::new(shared.clone(), gate.clone());
                            let mut fw = FrameWrite::new(bw, body.clone(), 1024).map_err(|(_, e)| e).unwrap();
                            async move { fw.send().await.map_err(|_| ()) }
                        },
                        Ok(()),
                        // - `step`: invoked on each `Poll::Pending` to
                        //   advance external state.
                        // - Index into the fuzzed `drips` sequence
                        //   (shared via `idx`).
                        // - Add that budget to the shared `Gate`,
                        //   waking any waiter.
                        // - If we run out of drips, fall back to
                        //   `total_need` to guarantee eventual
                        //   completion.
                        // - Returned future performs the actual
                        //   `gate.add` each tick.
                        // - Occasionally yield so timers make
                        // progress without slowing every step.
                        {
                            let gate = gate.clone();
                            let idx = idx.clone();
                            let pt_outer = pending_ticks.clone();
                            move || {
                                let i = idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                let add = *drips.get(i).unwrap_or(&total_need);
                                let gate = gate.clone();
                                let pt = pt_outer.clone();

                                async move {
                                    gate.add(add);
                                    if pt.fetch_add(1, std::sync::atomic::Ordering::Relaxed) & 31 == 0 {
                                        tokio::task::yield_now().await;
                                    }
                                }
                            }
                        }
                    );
                    // At this point `fw.send()` has completed
                    // successfully, so the frame must be fully
                    // written to the wire. Verifying that the next
                    // read yields exactly `body` checks the
                    // postcondition of `send()`.
                    let got = reader.next().await.unwrap().unwrap();
                    assert_eq!(got, body);
                }
                ).await.expect("cancel-safety run timed out");
            });
        }
    }

    // Theorem: FrameWrite::send is cancel-safe for *strictly
    // multipart* messages (ensures vectored writes are actually
    // exercised).
    proptest! {
        #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]
        #[test]
        fn framewrite_cancellation_is_safe_multipart_only(
            msg in test_support::multipart_message_only(),
            drips in test_support::budget_drips()) {
            let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
            rt.block_on(async move {
                 // Big in-memory pipe (16 MB = 16 * 1024 * 1024
                 // bytes) to avoid artificial backpressure stalls. In
                 // these cancel-safety tests we *repeatedly* drive
                 // the writer future to completion across many
                 // cancellation points (the harness cancels/ retries
                 // at multiple `Poll::Pending` sites) *before we read
                 // even a single frame*. With a small duplex buffer
                 // (e.g., 4 KB), large multipart frames can fill the
                 // pipe and deadlock the writer, conflating pipe
                 // capacity with the cancel-safety property under
                 // test. A large buffer keeps the test focused on the
                 // refund/credit-back invariants rather than I/O
                 // buffering behavior.
                const MB: usize = 1024 * 1024; // 1 MB (binary);
                let (a, b) = tokio::io::duplex(16 * MB);
                let (r, _wu) = tokio::io::split(a);
                let (_ru, w) = tokio::io::split(b);

                let gate = Gate::new();
                let shared = SharedWriter::new(w);

                // Sanity: underlying writer should support vectored
                // writes; our BudgetedWriter forwards
                // poll_write_vectored and preserves that capability.
                assert!(shared.is_write_vectored(), "underlying writer is not vectored");

                // FrameReader configured to reject any frame >1 MB.
                let mut reader = FrameReader::new(r, MB);

                // Compute expected bytes once (len-prefix stripped).
                let mut framed = msg.clone().framed();
                let expected_body = framed.copy_to_bytes(framed.remaining());
                let total_need = 8 + expected_body.len();

                let mut drips = drips.clone();
                drips.insert(0, 8);

                let idx = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
                let pending_ticks = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

                let step_idx   = idx.clone();
                let make_gate  = gate.clone(); // used by mk-future
                let step_gate  = gate.clone(); // used by step-closure
                let step_ticks = pending_ticks.clone();
                let step_drips = drips.clone();

                #[allow(clippy::disallowed_methods)]
                tokio::time::timeout(std::time::Duration::from_secs(3), async {
                    assert_cancel_safe_async!(
                        // `mk`: Build a *fresh* send future each attempt.
                        // - We clone `shared` and `make_gate` (both
                        //   `Arc`-backed), so the underlying writer +
                        //   budget pool persist across cancellations.
                        // - We clone and frame `msg` each time; with
                        //   ≥2 part this drives the vectored write
                        //   path.
                        // - We construct a new `FrameWrite` each time
                        //   (the future under test), so
                        //   `assert_cancel_safe_async` can cancel at
                        //   any `Poll::Pending`, drop it, and retry
                        //   from the same shared world state.
                        // - Map `Result<(), io::Error>` → `Result<(),()>`
                        //   so the `expected` value (`Ok(())`) is
                        //   equality comparable (requires `PartialEq`).
                        {
                            let bw = BudgetedWriter::new(shared.clone(), make_gate.clone());
                            let body = msg.clone().framed(); // multipart → vectored path
                            let mut fw = FrameWrite::new(bw, body, MB).map_err(|(_, e)| e).unwrap();
                            async move { fw.send().await.map_err(|_| ()) }
                        },
                        Ok(()),

                        // - `step`: invoked on each `Poll::Pending` to
                        //   advance external state.
                        // - Index into the fuzzed `drips` sequence
                        //   (shared via `step_idx`).
                        // - Add that budget to the shared `Gate` (via
                        //   `step_gate`), waking any waiter.
                        // - If we run out of drips, fall back to
                        //   `total_need` to guarantee eventual
                        //   completion.
                        // - Returned future performs the actual
                        //   `gate.add` each tick.
                        // - Occasionally yield so timers make
                        // progress without slowing every step.
                        move || {
                            let i = step_idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let add = *step_drips.get(i).unwrap_or(&total_need);
                            let gate = step_gate.clone();
                            let ticks = step_ticks.clone();

                            async move {
                                gate.add(add);
                                if ticks.fetch_add(1, std::sync::atomic::Ordering::Relaxed) & 31 == 0 {
                                    tokio::task::yield_now().await;
                                }
                            }
                        }
                    );
                    // At this point `fw.send()` has completed
                    // successfully, so the frame must be fully
                    // written to the wire. Verifying that the next
                    // read yields exactly `expected_body` checks the
                    // postcondition of `send()` (the reader sees the
                    // body bytes, with the length prefix stripped).
                    let got = tokio::time::timeout(std::time::Duration::from_secs(1), reader.next())
                        .await.expect("reader stalled").unwrap().unwrap();
                    assert_eq!(got, expected_body);
                }).await.expect("strict multipart cancel-safety run timed out");
            });
        }
    }

    // Theorem: `FrameWrite::send` is cancel-safe for messages that
    // may be unipart OR multipart. (May or may not take the vectored
    // path.)
    proptest! {
        #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]
        #[test]
        fn framewrite_cancellation_is_safe_unipart_or_multipart(
            msg in test_support::multipart_message(),
            drips in test_support::budget_drips()
        ) {
            let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
            rt.block_on(async move {
                 // Big in-memory pipe (16 MB = 16 * 1024 * 1024
                 // bytes) to avoid artificial backpressure stalls. In
                 // these cancel-safety tests we *repeatedly* drive
                 // the writer future to completion across many
                 // cancellation points (the harness cancels/ retries
                 // at multiple `Poll::Pending` sites) *before we read
                 // even a single frame*. With a small duplex buffer
                 // (e.g., 4 KB), large multipart frames can fill the
                 // pipe and deadlock the writer, conflating pipe
                 // capacity with the cancel-safety property under
                 // test. A large buffer keeps the test focused on the
                 // refund/credit-back invariants rather than I/O
                 // buffering behavior.
                const MB: usize = 1024 * 1024; // 1 MB (binary)
                let (a, b) = tokio::io::duplex(16 * MB);
                let (r, _wu) = tokio::io::split(a);
                let (_ru, w) = tokio::io::split(b);

                let gate   = Gate::new();
                let shared = SharedWriter::new(w);

                // FrameReader configured to reject any frame >1 MB.
                let mut reader = FrameReader::new(r, MB);

                // Compute expected bytes once (len-prefix stripped).
                let mut framed = msg.clone().framed();
                let expected_body = framed.copy_to_bytes(framed.remaining());
                let total_need = 8 + expected_body.len();

                let mut drips = drips.clone();
                drips.insert(0, 8);

                let idx            = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
                let pending_ticks  = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

                let step_idx   = idx.clone();
                let make_gate  = gate.clone(); // used by mk-future
                let step_gate  = gate.clone(); // used by step-closure
                let step_ticks = pending_ticks.clone();
                let step_drips = drips.clone();

                #[allow(clippy::disallowed_methods)]
                tokio::time::timeout(std::time::Duration::from_secs(3), async {
                    assert_cancel_safe_async!(
                        // `mk`: Build a *fresh* send future each attempt.
                        // - Clone `shared` and `make_gate` (Arc-backed) so
                        //   writer + budget persist across cancellations.
                        // - Clone+frame `msg` each time; if it's multipart (≥2 parts)
                        //   this will exercise the vectored write path, otherwise it
                        //   goes down the scalar path — both are valid.
                        // - Construct a new `FrameWrite` each attempt so the harness
                        //   can cancel at any `Poll::Pending` and retry from shared state.
                        // - Map `Result<(), io::Error>` → `Result<(),()>` so `Ok(())`
                        //   is equality comparable.
                        {
                            let bw = BudgetedWriter::new(shared.clone(), make_gate.clone());
                            let body = msg.clone().framed(); // may or may not be multipart
                            let mut fw = FrameWrite::new(bw, body, MB).map_err(|(_, e)| e).unwrap();
                            async move { fw.send().await.map_err(|_| ()) }
                        },
                        Ok(()),

                        // `step`: invoked on each `Poll::Pending` to advance external state.
                        // - Index into the fuzzed `drips` (via `step_idx`).
                        // - Add budget to the shared `Gate` (`step_gate`).
                        // - If we run out of drips, fall back to `total_need`.
                        // - Occasionally yield so timers progress without slowing every step.
                        move || {
                            let i = step_idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let add = *step_drips.get(i).unwrap_or(&total_need);
                            let gate = step_gate.clone();
                            let ticks = step_ticks.clone();

                            async move {
                                gate.add(add);
                                if ticks.fetch_add(1, std::sync::atomic::Ordering::Relaxed) & 31 == 0 {
                                    tokio::task::yield_now().await;
                                }
                            }
                        }
                    );

                    // At this point `fw.send()` has completed successfully, so the frame
                    // must be fully written to the wire. Verifying that the next read
                    // yields exactly `expected_body` checks the postcondition of `send()`
                    // (the reader sees the body bytes, with the length prefix stripped).
                    let got = tokio::time::timeout(std::time::Duration::from_secs(1), reader.next())
                        .await.expect("reader stalled").unwrap().unwrap();
                    assert_eq!(got, expected_body);
                }).await.expect("unipart/multipart cancel-safety run timed out");
            });
        }
    }
}
