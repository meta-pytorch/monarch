/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Telemetry frame codec shared by socket ingest and the persist log.
//!
//! A frame is a `u16` big-endian name length, the UTF-8 name, a `u32`
//! big-endian payload length, and the payload. Data frames name a table and
//! carry one Arrow IPC stream. A persist log's clean-close marker is an
//! `__end` frame with an empty payload.

use std::io::ErrorKind;
use std::io::Read;

use crate::MAX_FRAME_LEN;
use crate::MAX_TABLE_NAME_LEN;
use crate::write_frame_header;

/// Empty control frame written only on clean close.
pub const END_FRAME: &str = "__end";

/// Whether `name` is reserved for control frames.
pub fn is_reserved(name: &str) -> bool {
    name.starts_with("__")
}

/// Validate a frame's table-name length.
pub fn check_table_name_len(len: usize) -> anyhow::Result<()> {
    if len == 0 || len > MAX_TABLE_NAME_LEN {
        anyhow::bail!("invalid table name length {len}");
    }
    Ok(())
}

/// Validate a frame's payload length.
pub fn check_payload_len(len: usize) -> anyhow::Result<()> {
    if len == 0 || len > MAX_FRAME_LEN {
        anyhow::bail!("invalid frame length {len}");
    }
    Ok(())
}

/// Append one complete frame to `buf`.
pub fn encode_frame(buf: &mut Vec<u8>, name: &str, payload: &[u8]) -> anyhow::Result<()> {
    write_frame_header(buf, name, payload.len())?;
    buf.extend_from_slice(payload);
    Ok(())
}

/// One undecoded frame.
#[derive(Debug)]
pub struct RawFrame {
    /// Table or control-frame name.
    pub name: String,
    /// Frame payload.
    pub payload: Vec<u8>,
}

/// Result of reading the next frame.
#[derive(Debug)]
pub enum ReadOutcome {
    /// A complete frame.
    Frame(RawFrame),
    /// Clean end of input at a frame boundary.
    End,
    /// An incomplete or invalid frame starting at byte `offset`.
    Torn { offset: u64 },
}

/// Sequential frame reader over a byte stream.
pub struct FrameReader<R> {
    reader: R,
    offset: u64,
}

impl<R: Read> FrameReader<R> {
    /// Read frames from `reader`, starting at offset zero.
    pub fn new(reader: R) -> Self {
        Self { reader, offset: 0 }
    }

    /// Read the next frame, reporting a torn tail instead of failing.
    pub fn read_frame(&mut self) -> anyhow::Result<ReadOutcome> {
        let start = self.offset;
        let torn = ReadOutcome::Torn { offset: start };

        let mut name_len = [0; 2];
        match self.fill(&mut name_len)? {
            0 => return Ok(ReadOutcome::End),
            2 => {}
            _ => return Ok(torn),
        }
        let name_len = u16::from_be_bytes(name_len) as usize;
        if check_table_name_len(name_len).is_err() {
            return Ok(torn);
        }

        let mut name = vec![0; name_len];
        if self.fill(&mut name)? != name_len {
            return Ok(torn);
        }
        let Ok(name) = String::from_utf8(name) else {
            return Ok(torn);
        };

        let mut payload_len = [0; 4];
        if self.fill(&mut payload_len)? != 4 {
            return Ok(torn);
        }
        let payload_len = u32::from_be_bytes(payload_len) as usize;
        if (name != END_FRAME || payload_len != 0) && check_payload_len(payload_len).is_err() {
            return Ok(torn);
        }

        let mut payload = vec![0; payload_len];
        if self.fill(&mut payload)? != payload_len {
            return Ok(torn);
        }
        Ok(ReadOutcome::Frame(RawFrame { name, payload }))
    }

    /// Read until `buf` is full or input ends, returning the bytes read.
    fn fill(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut filled = 0;
        while filled < buf.len() {
            match self.reader.read(&mut buf[filled..]) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        self.offset += filled as u64;
        Ok(filled)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    fn read_all(bytes: Vec<u8>) -> Vec<ReadOutcome> {
        let mut reader = FrameReader::new(Cursor::new(bytes));
        let mut outcomes = Vec::new();
        loop {
            let outcome = reader.read_frame().unwrap();
            let done = !matches!(outcome, ReadOutcome::Frame(_));
            outcomes.push(outcome);
            if done {
                return outcomes;
            }
        }
    }

    #[test]
    fn encode_then_read_round_trips() {
        let mut buf = Vec::new();
        encode_frame(&mut buf, "t", &[1, 2, 3]).unwrap();
        encode_frame(&mut buf, END_FRAME, &[]).unwrap();

        let outcomes = read_all(buf);

        assert_eq!(outcomes.len(), 3);
        let ReadOutcome::Frame(first) = &outcomes[0] else {
            panic!("expected a frame, got {:?}", outcomes[0]);
        };
        assert_eq!(first.name, "t");
        assert_eq!(first.payload, vec![1, 2, 3]);
        let ReadOutcome::Frame(second) = &outcomes[1] else {
            panic!("expected a frame, got {:?}", outcomes[1]);
        };
        assert_eq!(second.name, END_FRAME);
        assert!(second.payload.is_empty());
        assert!(matches!(outcomes[2], ReadOutcome::End));
    }

    #[test]
    fn reader_reports_torn_header() {
        let mut buf = Vec::new();
        encode_frame(&mut buf, "t", &[1, 2, 3]).unwrap();
        let first_len = buf.len() as u64;
        buf.push(0x00);

        let outcomes = read_all(buf);

        assert!(matches!(outcomes[0], ReadOutcome::Frame(_)));
        assert!(
            matches!(outcomes[1], ReadOutcome::Torn { offset } if offset == first_len),
            "half a name length after the first frame is torn at its end: {:?}",
            outcomes[1]
        );
    }

    #[test]
    fn reader_reports_invalid_length_as_torn() {
        let outcomes = read_all(0u16.to_be_bytes().to_vec());

        assert!(
            matches!(outcomes[0], ReadOutcome::Torn { offset: 0 }),
            "a zero name length is invalid: {:?}",
            outcomes[0]
        );
    }

    #[test]
    fn reserved_names() {
        assert!(is_reserved(END_FRAME));
        assert!(!is_reserved("spans"));
        assert!(!is_reserved("_x"));
    }
}
