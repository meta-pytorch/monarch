/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::pin::Pin;
use std::task::Context as TaskContext;
use std::task::Poll;

use tokio::io;

/// A line-prefixing AsyncWrite wrapper that adds "[<local_rank>] " prefix to each line.
pub struct LinePrefixingWriter<W>
where
    W: io::AsyncWrite + Unpin,
{
    inner: W,
    prefix: Vec<u8>,   // Pre-formatted prefix like "[42] "
    need_prefix: bool, // true when we need to write a prefix before the next data
}

impl<W> LinePrefixingWriter<W>
where
    W: io::AsyncWrite + Unpin,
{
    /// Creates a new LinePrefixingWriter that will prefix each line with "[<local_rank>] ".
    #[allow(dead_code)]
    pub fn new(local_rank: usize, inner: W) -> Self {
        let prefix = format!("[{}] ", local_rank).into_bytes();
        Self {
            inner,
            prefix,
            need_prefix: true, // Start by needing a prefix
        }
    }
}

impl<W> io::AsyncWrite for LinePrefixingWriter<W>
where
    W: io::AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let this = self.get_mut();
        let mut output = Vec::with_capacity(buf.len());

        let mut pos = 0;
        while pos < buf.len() {
            if this.need_prefix {
                output.extend_from_slice(&this.prefix);
                this.need_prefix = false;
            }

            let remaining = &buf[pos..];
            if let Some(newline_offset) = remaining.iter().position(|&b| b == b'\n') {
                // Write up to and including the newline
                let end_pos = newline_offset + 1;
                output.extend_from_slice(&remaining[..end_pos]);
                pos += end_pos;
                this.need_prefix = true;
            } else {
                output.extend_from_slice(remaining);
                break;
            }
        }

        match Pin::new(&mut this.inner).poll_write(cx, &output) {
            Poll::Ready(Ok(n)) => {
                if n == output.len() {
                    // Need to return buf.len() bytes written to the outer call,
                    // which is unaware that we are making the actual write
                    // slightly larger
                    Poll::Ready(Ok(buf.len()))
                } else {
                    // This case is annoying to handle, so just return an error as
                    // partial writes are extremely rare
                    Poll::Ready(Err(io::Error::other("Partial write to inner writer")))
                }
            }
            Poll::Ready(err) => Poll::Ready(err),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Result<(), io::Error>> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Result<(), io::Error>> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use tokio::io::AsyncWriteExt;

    use super::*;

    // Mock implementation of AsyncWrite that captures written data
    struct MockWriter {
        data: Arc<Mutex<Vec<u8>>>,
    }

    impl MockWriter {
        fn new() -> (Self, Arc<Mutex<Vec<u8>>>) {
            let data = Arc::new(Mutex::new(Vec::new()));
            (Self { data: data.clone() }, data)
        }
    }

    impl io::AsyncWrite for MockWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, io::Error>> {
            let mut data = self.data.lock().unwrap();
            data.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
        ) -> Poll<Result<(), io::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
        ) -> Poll<Result<(), io::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    // Mock writer that can simulate partial writes and pending states
    struct PartialMockWriter {
        data: Arc<Mutex<Vec<u8>>>,
        write_behavior: Arc<Mutex<Vec<WriteResult>>>,
        call_count: Arc<Mutex<usize>>,
    }

    #[derive(Clone, Debug)]
    enum WriteResult {
        Full,           // Write all bytes successfully
        Partial(usize), // Write only N bytes
        Pending,        // Return Poll::Pending
        Error,          // Return an error
    }

    impl PartialMockWriter {
        fn new(write_behavior: Vec<WriteResult>) -> (Self, Arc<Mutex<Vec<u8>>>) {
            let data = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    data: data.clone(),
                    write_behavior: Arc::new(Mutex::new(write_behavior)),
                    call_count: Arc::new(Mutex::new(0)),
                },
                data,
            )
        }
    }

    impl io::AsyncWrite for PartialMockWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, io::Error>> {
            let mut call_count = self.call_count.lock().unwrap();
            let behaviors = self.write_behavior.lock().unwrap();

            if *call_count >= behaviors.len() {
                // Default to writing all bytes if no more behaviors specified
                let mut data = self.data.lock().unwrap();
                data.extend_from_slice(buf);
                return Poll::Ready(Ok(buf.len()));
            }

            let behavior = behaviors[*call_count].clone();
            *call_count += 1;

            match behavior {
                WriteResult::Full => {
                    let mut data = self.data.lock().unwrap();
                    data.extend_from_slice(buf);
                    Poll::Ready(Ok(buf.len()))
                }
                WriteResult::Partial(n) => {
                    let n = n.min(buf.len());
                    let mut data = self.data.lock().unwrap();
                    data.extend_from_slice(&buf[..n]);
                    Poll::Ready(Ok(n))
                }
                WriteResult::Pending => Poll::Pending,
                WriteResult::Error => Poll::Ready(Err(io::Error::other("Mock error"))),
            }
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
        ) -> Poll<Result<(), io::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
        ) -> Poll<Result<(), io::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn test_line_prefixing_writer_single_line() {
        let (mock_writer, data) = MockWriter::new();
        let mut writer = LinePrefixingWriter::new(42, mock_writer);

        // Write a single line without newline
        writer.write_all(b"Hello, world!").await.unwrap();
        writer.flush().await.unwrap();

        // Check that prefix was added
        let written_data = data.lock().unwrap();
        assert_eq!(*written_data, b"[42] Hello, world!");
    }

    #[tokio::test]
    async fn test_line_prefixing_writer_single_line_with_newline() {
        let (mock_writer, data) = MockWriter::new();
        let mut writer = LinePrefixingWriter::new(42, mock_writer);

        // Write a single line with newline
        writer.write_all(b"Hello, world!\n").await.unwrap();
        writer.flush().await.unwrap();

        // Check that prefix was added
        let written_data = data.lock().unwrap();
        assert_eq!(*written_data, b"[42] Hello, world!\n");
    }

    #[tokio::test]
    async fn test_line_prefixing_writer_multiple_lines() {
        let (mock_writer, data) = MockWriter::new();
        let mut writer = LinePrefixingWriter::new(42, mock_writer);

        // Write multiple lines
        writer
            .write_all(b"First line\nSecond line\nThird line")
            .await
            .unwrap();
        writer.flush().await.unwrap();

        // Check that prefix was added to each line
        let written_data = data.lock().unwrap();
        assert_eq!(
            *written_data,
            b"[42] First line\n[42] Second line\n[42] Third line"
        );
    }

    #[tokio::test]
    async fn test_line_prefixing_writer_multiple_writes() {
        let (mock_writer, data) = MockWriter::new();
        let mut writer = LinePrefixingWriter::new(42, mock_writer);

        // Write first line
        writer.write_all(b"First line\n").await.unwrap();
        // Write second line
        writer.write_all(b"Second line\n").await.unwrap();
        writer.flush().await.unwrap();

        // Check that prefix was added to each line
        let written_data = data.lock().unwrap();
        assert_eq!(
            String::from_utf8_lossy(&written_data),
            "[42] First line\n[42] Second line\n"
        );
    }

    #[tokio::test]
    async fn test_line_prefixing_writer_empty_lines() {
        let (mock_writer, data) = MockWriter::new();
        let mut writer = LinePrefixingWriter::new(42, mock_writer);

        // Write empty lines
        writer.write_all(b"\n\n").await.unwrap();
        writer.flush().await.unwrap();

        // Check that prefix was added to each empty line
        let written_data = data.lock().unwrap();
        assert_eq!(*written_data, b"[42] \n[42] \n");
    }

    #[tokio::test]
    async fn test_line_prefixing_writer_line_continuation() {
        let (mock_writer, data) = MockWriter::new();
        let mut writer = LinePrefixingWriter::new(42, mock_writer);

        // Write partial line
        writer.write_all(b"Start of line").await.unwrap();
        // Continue same line
        writer.write_all(b" continuation").await.unwrap();
        // End the line
        writer.write_all(b" end\n").await.unwrap();
        writer.flush().await.unwrap();

        // Check that prefix was added only once at the beginning
        let written_data = data.lock().unwrap();
        assert_eq!(*written_data, b"[42] Start of line continuation end\n");
    }

    #[tokio::test]
    async fn test_line_prefixing_writer_different_ranks() {
        let (mock_writer1, data1) = MockWriter::new();
        let mut writer1 = LinePrefixingWriter::new(0, mock_writer1);

        let (mock_writer2, data2) = MockWriter::new();
        let mut writer2 = LinePrefixingWriter::new(999, mock_writer2);

        // Write to both writers
        writer1.write_all(b"Rank 0 message\n").await.unwrap();
        writer2.write_all(b"Rank 999 message\n").await.unwrap();

        writer1.flush().await.unwrap();
        writer2.flush().await.unwrap();

        // Check that different prefixes were used
        let written_data1 = data1.lock().unwrap();
        let written_data2 = data2.lock().unwrap();

        assert_eq!(*written_data1, b"[0] Rank 0 message\n");
        assert_eq!(*written_data2, b"[999] Rank 999 message\n");
    }

    #[tokio::test]
    async fn test_line_prefixing_writer_partial_writes() {
        // Test case: inner writer does partial writes of user data
        let (mock_writer, data) = PartialMockWriter::new(vec![
            WriteResult::Full,       // Prefix "[42] " writes fully
            WriteResult::Partial(5), // "Hello" writes but ", world!\n" doesn't
            WriteResult::Full,       // ", world!\n" writes on next call
        ]);
        let mut writer = LinePrefixingWriter::new(42, mock_writer);

        // Write should succeed even with partial inner writes
        writer.write_all(b"Hello, world!\n").await.unwrap();
        writer.flush().await.unwrap();

        let written_data = data.lock().unwrap();
        assert_eq!(*written_data, b"[42] Hello, world!\n");
    }

    #[tokio::test]
    async fn test_line_prefixing_writer_partial_prefix_write() {
        // Test case: prefix itself is written partially - should return error
        let (mock_writer, data) = PartialMockWriter::new(vec![
            WriteResult::Partial(2), // Only "[4" of "[42] " is written
        ]);
        let mut writer = LinePrefixingWriter::new(42, mock_writer);

        // This should return an error for partial prefix write
        let result = std::future::poll_fn(|cx| {
            use tokio::io::AsyncWrite;
            Pin::new(&mut writer).poll_write(cx, b"Hello")
        })
        .await;

        assert!(result.is_err()); // Should be an error
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Other);

        let written_data = data.lock().unwrap();
        assert_eq!(*written_data, b"[4"); // Only partial prefix written
    }

    // Better test for pending with no progress using a custom future
    #[test]
    fn test_line_prefixing_writer_pending_no_progress_direct() {
        use std::sync::Arc;
        use std::task::Context;
        use std::task::Poll;
        use std::task::Wake;
        use std::task::Waker;

        struct DummyWaker;
        impl Wake for DummyWaker {
            fn wake(self: Arc<Self>) {}
        }

        let waker = Waker::from(Arc::new(DummyWaker));
        let mut cx = Context::from_waker(&waker);

        let (mock_writer, _data) = PartialMockWriter::new(vec![WriteResult::Pending]);
        let mut writer = LinePrefixingWriter::new(42, mock_writer);

        // First call should return Pending
        {
            use tokio::io::AsyncWrite;
            let result = Pin::new(&mut writer).poll_write(&mut cx, b"Hello");
            assert!(matches!(result, Poll::Pending));
        }
    }

    #[tokio::test]
    async fn test_line_prefixing_writer_error_propagation() {
        // Test that errors from inner writer are propagated
        let (mock_writer, _data) = PartialMockWriter::new(vec![WriteResult::Error]);
        let mut writer = LinePrefixingWriter::new(42, mock_writer);

        let result = writer.write_all(b"Hello").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_line_prefixing_writer_complex_partial_scenario() {
        // Complex scenario: partial prefix should return error
        let (mock_writer, data) = PartialMockWriter::new(vec![
            WriteResult::Partial(2), // "[4" of prefix "[42] " - causes error
        ]);
        let mut writer = LinePrefixingWriter::new(42, mock_writer);

        // Should return error due to partial prefix write
        let result = std::future::poll_fn(|cx| {
            use tokio::io::AsyncWrite;
            Pin::new(&mut writer).poll_write(cx, b"Hello\n")
        })
        .await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Other);

        let written_data = data.lock().unwrap();
        // Should have partial prefix written before the error
        assert_eq!(*written_data, b"[4");
    }
}
