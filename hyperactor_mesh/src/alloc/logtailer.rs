/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::mem::take;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::Mutex;

use tokio::io;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncRead;
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
    fn push_line_to_buffer(state: &Arc<Mutex<State>>, byte_buffer: &mut [u8], max: usize) {
        // use lossy string rather than truncated valid utf8
        // from_utf8_lossy(b"Hello\xFF\xFEWorld") returns "Hello��World"
        let mut buffer: String = String::from_utf8_lossy(byte_buffer).to_string();
        // Remove trailing newline if present
        while buffer.ends_with('\n') {
            buffer.pop();
        }
        let mut locked = state.lock().unwrap();
        let next = locked.next;
        if next < locked.lines.len() {
            locked.lines[next] = buffer;
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
            let mut byte_buffer: Vec<u8> = Vec::new();
            loop {
                // this gives at most a reference to 8KB of data in the internal buffer
                // based on internal implementation of BufReader's `DEFAULT_BUF_SIZE`
                let reader_buf = reader.fill_buf().await?;

                if reader_buf.is_empty() {
                    // EOF reached, write any remaining buffer content as a line
                    if !byte_buffer.is_empty() {
                        Self::push_line_to_buffer(&state, &mut byte_buffer, max);
                    }
                    break Ok(());
                }

                // find newline pos or the end of buffer if no newline found
                let new_line_pos = reader_buf
                    .iter()
                    .position(|&b| b == b'\n')
                    .unwrap_or(reader_buf.len());

                if skip_until_newline {
                    // funnel through the tee stream
                    let mut to_consume = reader_buf.len();
                    if new_line_pos != reader_buf.len() {
                        to_consume = new_line_pos + 1;
                        skip_until_newline = false;
                    }
                    tee.write_all(&reader_buf[..to_consume]).await?;
                    reader.consume(to_consume);
                    continue;
                }

                let to_be_consumed = if new_line_pos != reader_buf.len() {
                    new_line_pos + 1
                } else {
                    reader_buf.len()
                };

                byte_buffer.extend(&reader_buf[..to_be_consumed]);
                tee.write_all(&reader_buf[..to_be_consumed]).await?;
                if byte_buffer.len() >= MAX_BYTE_SIZE_LOG_LINE || new_line_pos != reader_buf.len() {
                    skip_until_newline = byte_buffer.len() >= MAX_BYTE_SIZE_LOG_LINE
                        && new_line_pos == reader_buf.len();
                    // Truncate to MAX_BYTE_SIZE_LOG_LINE if necessary before pushing
                    if byte_buffer.len() > MAX_BYTE_SIZE_LOG_LINE {
                        byte_buffer.truncate(MAX_BYTE_SIZE_LOG_LINE);
                    }

                    // we are pushing a line that doesnt have a newline
                    if byte_buffer.len() == MAX_BYTE_SIZE_LOG_LINE
                        && new_line_pos == reader_buf.len()
                    {
                        byte_buffer.extend_from_slice("<TRUNCATED>".as_bytes());
                    }
                    Self::push_line_to_buffer(&state, &mut byte_buffer, max);
                    byte_buffer.clear();
                }

                reader.consume(to_be_consumed);
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
    async fn test_read_buffer_boundary() {
        let mut input_bytes = Vec::new();
        // reader buffer's default size is 8KB. We assert that the tee function reads
        // correctly when the lines are exactly 8KB and 8KB + 1 bytes
        input_bytes.extend(vec![b'a'; 8191]);
        input_bytes.extend([b'\n']);
        input_bytes.extend(vec![b'b'; 8192]);
        let reader = Cursor::new(input_bytes);

        let (lines, result) = LogTailer::new(5, reader).join().await;
        assert!(result.is_ok());

        // Should have 3 lines
        assert_eq!(lines.len(), 2);

        assert_eq!(lines[0], format!("{}", "a".repeat(8191)));

        assert_eq!(lines[1], format!("{}", "b".repeat(8192)));
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

    #[tokio::test]
    async fn test_multibyte_character_on_internal_buffer_boundary() {
        // Test: Multi-byte characters split across internal buffer boundaries
        let mut input_bytes = Vec::new();
        input_bytes.extend(vec![b'a'; 8191]);
        let euro_bytes = "€".as_bytes(); // [0xE2, 0x82, 0xAC]
        // add 3 bytes of the euro sign, but across internal buffer
        // 1st byte will be part of the first buffer call but remaining will spillover
        // to the next buffer call
        input_bytes.extend(euro_bytes);
        input_bytes.push(b'\n');
        input_bytes.extend(vec![b'b'; 8192]);
        let reader = Cursor::new(input_bytes);
        let (lines, result) = LogTailer::new(5, reader).join().await;

        assert!(result.is_ok());
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], format!("{}€", "a".repeat(8191)));
        assert_eq!(lines[1], format!("{}", "b".repeat(8192)));
    }

    #[tokio::test]
    async fn test_truncation_with_utf8_errors() {
        // Test: UTF-8 errors interacting with line length limits
        let mut input_bytes = Vec::new();

        // Fill near max capacity, then add invalid bytes
        input_bytes.extend(vec![b'a'; MAX_BYTE_SIZE_LOG_LINE - 1]);
        input_bytes.push(0xFF); // Invalid byte at the boundary of the limit
        input_bytes.extend(vec![b'b'; 100]); // Exceed limit, so skipped
        input_bytes.push(b'\n');
        input_bytes.extend(vec![b'c'; 100]); // new string after newline
        input_bytes.push(b'\n');
        input_bytes.push(0xFF); // Invalid byte at the start, expect <INVALID_UTF8>

        let reader = Cursor::new(input_bytes);
        let (lines, result) = LogTailer::new(5, reader).join().await;

        assert!(result.is_ok());
        assert_eq!(lines.len(), 3);
        assert_eq!(
            lines[0],
            format!(
                "{}{}",
                "a".repeat(MAX_BYTE_SIZE_LOG_LINE - 1),
                "�<TRUNCATED>"
            )
        );
        assert_eq!(lines[1], format!("{}", "c".repeat(100)));
        assert_eq!(lines[2], "�");
    }
}
