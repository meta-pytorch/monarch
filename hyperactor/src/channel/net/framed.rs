/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This module implements a cancellation-safe zero-copy framer for network channels.

use std::io;
use std::mem::take;

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

/// A FrameReader reads frames from an underlying [`AsyncRead`].
pub struct FrameReader<R> {
    reader: R,
    max_frame_length: usize,
    state: FrameReaderState,
}

enum FrameReaderState {
    /// Accumulating 8-byte length prefix.
    ReadLen { buf: BytesMut }, // buf.len() <= 8
    /// Accumulating body of exactly `len` bytes.
    ReadBody { len: usize, buf: BytesMut }, // buf.len() <= len
}

impl<R: AsyncRead + Unpin> FrameReader<R> {
    /// Create a new framer for `reader`. Frames exceeding `max_frame_length`
    /// in length result in an irrecoverable reader error.
    pub fn new(reader: R, max_frame_length: usize) -> Self {
        Self {
            reader,
            max_frame_length,
            state: FrameReaderState::ReadLen {
                buf: BytesMut::with_capacity(8),
            },
        }
    }

    /// Read the next frame from the underlying reader. If the frame exceeds
    /// the configured maximum length, `next` returns an `io::ErrorKind::InvalidData`
    /// error.
    ///
    /// The method is cancellation safe in the sense that, if it is used in a branch
    /// of a `tokio::select!` block, frames are never dropped.
    pub async fn next(&mut self) -> io::Result<Option<Bytes>> {
        loop {
            match &mut self.state {
                FrameReaderState::ReadLen { buf } if buf.len() < 8 => {
                    let n = self.reader.read_buf(buf).await?;

                    // https://docs.rs/tokio/latest/tokio/io/trait.AsyncReadExt.html#method.read_buf
                    // "This reader has reached its “end of file” and will likely no longer
                    // be able to produce bytes. Note that this does not mean that the reader
                    // will always no longer be able to produce bytes."
                    //
                    // In practice, this means EOF.
                    if n == 0 {
                        if buf.is_empty() {
                            // We ended on a frame boundary. End of stream:
                            return Ok(None);
                        } else {
                            return Err(io::ErrorKind::UnexpectedEof.into());
                        }
                    }
                }

                FrameReaderState::ReadLen { buf } => {
                    let len = buf.get_u64() as usize;
                    if len > self.max_frame_length {
                        return Err(io::ErrorKind::InvalidData.into());
                    }
                    self.state = FrameReaderState::ReadBody {
                        len,
                        buf: BytesMut::with_capacity(len),
                    };
                }

                FrameReaderState::ReadBody { len, buf } if buf.len() < *len => {
                    let n = self.reader.read_buf(buf).await?;
                    if n == 0 {
                        return Err(io::ErrorKind::UnexpectedEof.into());
                    }
                }

                FrameReaderState::ReadBody { len, buf } if buf.len() == *len => {
                    let frame = take(buf).freeze();
                    self.state = FrameReaderState::ReadLen {
                        buf: BytesMut::with_capacity(8),
                    };
                    return Ok(Some(frame));
                }
                _ => panic!("impossible state"),
            }
        }
    }
}

/// A Writer for message frames. FrameWrite requires the user to drive
/// the underlying state machines through (possibly) successive calls to
/// `send`, retaining cancellation safety. The FrameWrite owns the underlying
/// writer until the frame has been written to completion.
pub struct FrameWrite<W> {
    writer: W,
    state: FrameWriteState,
}
enum FrameWriteState {
    /// Writing frame length.
    WriteLen { len_buf: Bytes, body: Bytes },
    /// Writing the frame body.
    WriteBody { body: Bytes },
}

impl<W: AsyncWrite + Unpin> FrameWrite<W> {
    /// Create a new frame writer, writing `body` to `writer`.
    pub fn new(writer: W, body: Bytes) -> Self {
        let mut len_buf = BytesMut::with_capacity(8);
        len_buf.put_u64(body.len() as u64);
        let len_buf = len_buf.freeze();
        Self {
            writer,
            state: FrameWriteState::WriteLen { len_buf, body },
        }
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
            match &mut self.state {
                FrameWriteState::WriteLen { len_buf, .. } if !len_buf.is_empty() => {
                    self.writer.write_all_buf(len_buf).await?;
                }
                FrameWriteState::WriteLen { body, .. } => {
                    self.state = FrameWriteState::WriteBody {
                        body: body.clone(), // cheap, but let's get rid of it
                    }
                }
                FrameWriteState::WriteBody { body } if !body.is_empty() => {
                    self.writer.write_all_buf(body).await?;
                }
                FrameWriteState::WriteBody { .. } => {
                    return Ok(());
                }
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
    /// `FrameWrite::new(writer, bytes).send().await?.complete()`.
    ///
    /// Frame writes are atomic: either the entire frame is sent, or
    /// an error is returned. No partial frames are observed by the
    /// receiver.
    ///
    /// # Arguments
    ///
    /// * `writer` — the `AsyncWrite` sink to write into.
    /// * `bytes` — the serialized frame body to send.
    ///
    /// # Returns
    ///
    /// On success, returns the underlying writer so the caller can
    /// continue using it for further frames. On error, returns the
    /// I/O error from the underlying write.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use bytes::Bytes;
    ///
    /// // `writer` is any AsyncWrite + Unpin (e.g. a tokio `WriteHalf`)
    /// let writer = FrameWrite::write_frame(writer, Bytes::from_static(b"hello")).await?;
    /// ```
    pub async fn write_frame(writer: W, bytes: Bytes) -> std::io::Result<W> {
        let mut fw = FrameWrite::new(writer, bytes);
        fw.send().await?;
        Ok(fw.complete())
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use rand::thread_rng;
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
            let mut frame_write = FrameWrite::new(writer.take().unwrap(), body.clone());
            frame_write.send().await.unwrap();
            writer = Some(frame_write.complete());

            let frame = reader.next().await.unwrap().unwrap();
            assert_eq!(frame, body);
        }
    }

    #[tokio::test]
    async fn test_write_frame_smoke() {
        let (a, b) = tokio::io::duplex(4096);
        let (r, _w_unused) = tokio::io::split(a);
        let (_r_unused, w) = tokio::io::split(b);

        let mut reader = FrameReader::new(r, 1024);

        let w = FrameWrite::write_frame(w, Bytes::from_static(b"hello"))
            .await
            .unwrap();
        let _ = FrameWrite::write_frame(w, Bytes::from_static(b"world"))
            .await
            .unwrap();

        let f1 = reader.next().await.unwrap().unwrap();
        let f2 = reader.next().await.unwrap().unwrap();

        assert_eq!(f1.as_ref(), b"hello");
        assert_eq!(f2.as_ref(), b"world");
    }

    #[tokio::test]
    async fn test_reader_eof_at_boundary() {
        let (a, b) = tokio::io::duplex(4096);
        let (r, _wu) = tokio::io::split(a);
        let (_ru, mut w) = tokio::io::split(b);
        let mut reader = FrameReader::new(r, 1024);

        // Write a complete frame.
        w = FrameWrite::write_frame(w, Bytes::from_static(b"done"))
            .await
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
        let (a, b) = tokio::io::duplex(4096);
        let (r, _wu) = tokio::io::split(a);
        let (_ru, mut w) = tokio::io::split(b);
        let mut reader = FrameReader::new(r, 1024);

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

    // todo: test cancellation, frame size
}
