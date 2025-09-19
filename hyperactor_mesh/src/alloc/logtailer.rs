/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![allow(dead_code)] // until used

use std::mem::swap;
use std::mem::take;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::Mutex;

use tokio::io;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;

/// Maximum byte size of a single log line before truncation
const MAX_BYTE_SIZE_LOG_LINE: usize = 256 * 1024;

/// A tailer (ring buffer) of (text) log lines.
pub struct LogTailer {
    state: Arc<Mutex<State>>,
    handle: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

#[derive(Clone, Default)]
struct State {
    next: usize,
    lines: Vec<String>,
}

impl LogTailer {
    /// Helper method to push a line to the ring buffer
    fn push_line_to_buffer(state: &Arc<Mutex<State>>, buffer: &mut String, max: usize) {
        while buffer.ends_with('\n') {
            buffer.pop();
        }
        let mut locked = state.lock().unwrap();
        let next = locked.next;
        if next < locked.lines.len() {
            swap(&mut locked.lines[next], buffer);
        } else {
            locked.lines.push(buffer.clone());
        }
        locked.next = (next + 1) % max;
    }

    /// Create a new tailer given a `stream`. The tailer tails the reader in the
    /// background, while keeping at most `max` log lines in its buffer. The tailer
    /// stops when the stream is ended (i.e., returns an EOF).
    pub fn new(max: usize, stream: impl AsyncRead + Send + Unpin + 'static) -> Self {
        Self::tee(max, stream, io::sink())
    }

    /// Create a new tailer given a `stream`. The tailer tails the reader in the
    /// background, while keeping at most `max` log lines in its buffer. The tailer
    /// stops when the stream is ended (i.e., returns an EOF). All lines read by the
    /// tailer are teed onto the provided `tee` stream.
    pub fn tee(
        max: usize,
        stream: impl AsyncRead + Send + Unpin + 'static,
        mut tee: impl AsyncWrite + Send + Unpin + 'static,
    ) -> Self {
        let state = Arc::new(Mutex::new(State {
            next: 0,
            lines: Vec::with_capacity(max),
        }));
        let cloned_state = Arc::clone(&state);

        // todo: handle error case, stuff the handle here,
        // and make this awaitable, etc
        let handle = tokio::spawn(async move {
            let mut reader = BufReader::new(stream);
            let mut skip_until_newline = false;
            let mut buffer = String::new();
            loop {
                // this gives at most a reference to 8KB of data in the internal buffer
                // based on internal implementation of BufReader's `DEFAULT_BUF_SIZE`
                let reader_buf = reader.fill_buf().await?;

                if reader_buf.is_empty() {
                    // EOF reached, write any remaining buffer content as a line
                    if !buffer.is_empty() {
                        tee.write_all(buffer.as_bytes()).await?;
                        Self::push_line_to_buffer(&state, &mut buffer, max);
                        buffer.clear()
                    }
                    break Ok(());
                }

                let mut _consumed = 0;
                for &b in reader_buf {
                    _consumed += 1;
                    if skip_until_newline {
                        if b == b'\n' {
                            skip_until_newline = false;
                        }
                        continue;
                    }

                    buffer.push(char::from(b));
                    if b == b'\n' {
                        // End of line reached, write buffer to state
                        if !buffer.is_empty() {
                            tee.write_all(buffer.as_bytes()).await?;
                            Self::push_line_to_buffer(&state, &mut buffer, max);
                            buffer.clear();
                        }
                    } else if buffer.len() == MAX_BYTE_SIZE_LOG_LINE {
                        buffer.push_str("<TRUNCATED>\n");
                        skip_until_newline = true;
                        tee.write_all(buffer.as_bytes()).await?;
                        Self::push_line_to_buffer(&state, &mut buffer, max);

                        buffer.clear();
                    }
                }
                // commit consumed bytes
                reader.consume(_consumed);
            }
        });

        LogTailer {
            state: cloned_state,
            handle,
        }
    }

    /// Get the last log lines in the buffer. Returns up to `max` lines.
    pub fn tail(&self) -> Vec<String> {
        let State { next, mut lines } = self.state.lock().unwrap().clone();
        lines.rotate_left(next);
        lines
    }
    /// Abort the tailer. This will stop any ongoing reads, and drop the
    /// stream. Abort is complete after `join` returns.
    pub fn abort(&self) {
        self.handle.abort()
    }

    /// Join the tailer. This waits for the internal tailing task to complete,
    /// and then returns the contents of the line buffer and the status of the
    /// tailer task.
    pub async fn join(self) -> (Vec<String>, Result<(), anyhow::Error>) {
        let result = match self.handle.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e.into()),
            Err(e) => Err(e.into()),
        };

        let State { next, mut lines } = take(self.state.lock().unwrap().deref_mut());
        lines.rotate_left(next);
        (lines, result)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use tokio::io::AsyncWriteExt;

    use super::*;

    #[tokio::test]
    async fn test_basic() {
        let reader = Cursor::new("hello\nworld\n".as_bytes());
        let (lines, result) = LogTailer::new(5, reader).join().await;
        assert!(result.is_ok());
        assert_eq!(lines, vec!["hello".to_string(), "world".to_string()]);
    }

    #[tokio::test]
    async fn test_tee() {
        let reader = Cursor::new("hello\nworld\n".as_bytes());
        let (write, read) = io::duplex(64); // 64-byte internal buffer
        let (lines, result) = LogTailer::tee(5, reader, write).join().await;
        assert!(result.is_ok());
        assert_eq!(lines, vec!["hello".to_string(), "world".to_string()]);
        let mut lines = BufReader::new(read).lines();
        assert_eq!(lines.next_line().await.unwrap().unwrap(), "hello");
        assert_eq!(lines.next_line().await.unwrap().unwrap(), "world");
    }

    #[tokio::test]
    async fn test_line_truncation() {
        // Create input with 3 MAX_BYTE_SIZE_LOG_LINE-byte lines
        let mut input_bytes = Vec::new();
        // first line is exactly `MAX_BYTE_SIZE_LOG_LINE` bytes including `\n`
        input_bytes.extend(vec![b'a'; MAX_BYTE_SIZE_LOG_LINE - 1]);
        input_bytes.extend([b'\n']);

        // second line is more than `MAX_BYTE_SIZE_LOG_LINE` bytes including `\n`
        input_bytes.extend(vec![b'b'; MAX_BYTE_SIZE_LOG_LINE]);
        input_bytes.extend([b'\n']);

        // last line of the input stream is < `MAX_BYTE_SIZE_LOG_LINE` bytes to ensure complete flush
        input_bytes.extend(vec![b'c'; MAX_BYTE_SIZE_LOG_LINE - 1]);

        let reader = Cursor::new(input_bytes);

        let (lines, result) = LogTailer::new(5, reader).join().await;
        assert!(result.is_ok());

        // Should have 3 lines
        assert_eq!(lines.len(), 3);

        // First line should be MAX_BYTE_SIZE_LOG_LINE-1 'a's
        assert_eq!(
            lines[0],
            format!("{}", "a".repeat(MAX_BYTE_SIZE_LOG_LINE - 1))
        );

        // Second line should be `MAX_BYTE_SIZE_LOG_LINE` 'b's + "<TRUNCATED>"
        assert_eq!(
            lines[1],
            format!("{}<TRUNCATED>", "b".repeat(MAX_BYTE_SIZE_LOG_LINE))
        );

        // last line before stream closes should be MAX_BYTE_SIZE_LOG_LINE-1 c's
        assert_eq!(lines[2], "c".repeat(MAX_BYTE_SIZE_LOG_LINE - 1));
    }

    #[tokio::test]
    async fn test_ring_buffer_behavior() {
        let input = "line1\nline2\nline3\nline4\nline5\nline6\nline7\n";
        let reader = Cursor::new(input.as_bytes());
        let max_lines = 3; // Small ring buffer for easy testing

        let (lines, result) = LogTailer::new(max_lines, reader).join().await;
        assert!(result.is_ok());

        // Should only have the last 3 lines (ring buffer behavior)
        // Lines 1-4 should be overwritten (lost due to ring buffer)
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0], "line5"); // oldest in current buffer
        assert_eq!(lines[1], "line6"); // middle
        assert_eq!(lines[2], "line7"); // newest
    }

    #[tokio::test]
    async fn test_streaming_logtailer() {
        let (reader, mut writer) = tokio::io::simplex(1);

        let writer_handle = tokio::spawn(async move {
            for i in 1.. {
                writer.write_all(i.to_string().as_bytes()).await.unwrap();
                writer.write_all("\n".as_bytes()).await.unwrap();
            }
        });

        let max_lines = 5;
        let tailer = LogTailer::new(max_lines, reader);

        let target = 1000; // require at least 1000 lines
        let mut last = 0;
        while last < target {
            tokio::task::yield_now().await;
            let lines: Vec<_> = tailer
                .tail()
                .into_iter()
                .map(|line| line.parse::<usize>().unwrap())
                .collect();
            if lines.is_empty() {
                continue;
            }

            assert!(lines.len() <= max_lines);
            assert!(lines[0] > last);
            for i in 1..lines.len() {
                assert_eq!(lines[i], lines[i - 1] + 1);
            }
            last = lines[lines.len() - 1];
        }
        // Unfortunately, there is no way to close the simplex stream
        // from the write half only, so we just have to let this go.
        writer_handle.abort();
        let _ = writer_handle.await;
        tailer.abort();
        tailer.join().await.1.unwrap_err();
    }
}
