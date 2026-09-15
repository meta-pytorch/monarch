/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::io;

use libsql::Value;
use thiserror::Error;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

use crate::Change;
use crate::SiteScope;
use crate::SiteSet;

const HELLO: u8 = 1;
const SCOPE: u8 = 2;
const BEGIN_BATCH: u8 = 3;
const BATCH_CHUNK: u8 = 4;
const COMMIT_BATCH: u8 = 5;
const ACK: u8 = 6;
const SCHEMA: u8 = 7;
const SYNCHRONIZED: u8 = 8;

const NULL: u8 = 0;
const INTEGER: u8 = 1;
const REAL: u8 = 2;
const TEXT: u8 = 3;
const BLOB: u8 = 4;
const DELETE: u8 = 0;
const UPSERT: u8 = 1;

const EXPLICIT_SCOPE: u8 = 0;
const COMPLEMENT_OF_PEER: u8 = 1;

pub(crate) const MAX_FRAME_LEN: usize = 16 * 1024 * 1024;
const MAX_SITE_IDS: usize = 65_536;
const MAX_CHANGES: usize = 65_536;
const MAX_VALUES: usize = 65_536;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("replication frame length {length} exceeds 16777216")]
    FrameTooLarge { length: usize },

    #[error("replication frame is truncated")]
    Truncated,

    #[error("replication frame has trailing bytes")]
    TrailingBytes,

    #[error("replication collection exceeds limit")]
    CollectionTooLarge,

    #[error("replication site set is invalid")]
    InvalidSiteSet,

    #[error("unknown replication site scope tag {0}")]
    UnknownSiteScope(u8),

    #[error("replication string is not UTF-8")]
    InvalidUtf8,

    #[error("unknown replication message tag {0}")]
    UnknownMessage(u8),

    #[error("unknown SQLite value tag {0}")]
    UnknownValue(u8),

    #[error("unknown row mutation tag {0}")]
    UnknownMutation(u8),

    #[error("invalid replicated table schema")]
    InvalidSchema,

    #[error(transparent)]
    Io(#[from] io::Error),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Message {
    Hello { site_id: Vec<u8>, scope: SiteScope },
    Scope(SiteScope),
    BeginBatch { batch_id: u64 },
    BatchChunk(Vec<Change>),
    CommitBatch,
    Ack { batch_id: u64 },
    Schema { table: String, hash: [u8; 32] },
    Synchronized,
}

pub(crate) fn encoded_change_len(change: &Change) -> Result<usize, ProtocolError> {
    let mut encoder = Encoder::default();
    encoder.change(change)?;
    Ok(encoder.bytes.len())
}

pub(crate) async fn send_message<W>(writer: &mut W, message: &Message) -> Result<(), ProtocolError>
where
    W: AsyncWrite + Unpin,
{
    let body = encode(message)?;
    let length = u32::try_from(body.len())
        .map_err(|_| ProtocolError::FrameTooLarge { length: body.len() })?;
    writer.write_all(&length.to_be_bytes()).await?;
    writer.write_all(&body).await?;
    writer.flush().await?;
    Ok(())
}

pub(crate) struct MessageReader<R> {
    reader: R,
    state: ReadState,
}

enum ReadState {
    Prefix { bytes: [u8; 4], filled: usize },
    Body { bytes: Vec<u8>, filled: usize },
}

impl<R> MessageReader<R>
where
    R: AsyncRead + Unpin,
{
    pub(crate) fn new(reader: R) -> Self {
        Self {
            reader,
            state: ReadState::Prefix {
                bytes: [0; 4],
                filled: 0,
            },
        }
    }

    pub(crate) async fn receive(&mut self) -> Result<Option<Message>, ProtocolError> {
        loop {
            match &mut self.state {
                ReadState::Prefix { bytes, filled } => {
                    let count = self.reader.read(&mut bytes[*filled..]).await?;
                    if count == 0 {
                        return if *filled == 0 {
                            Ok(None)
                        } else {
                            Err(ProtocolError::Truncated)
                        };
                    }
                    *filled += count;
                    if *filled < bytes.len() {
                        continue;
                    }
                    let length = u32::from_be_bytes(*bytes) as usize;
                    if length > MAX_FRAME_LEN {
                        return Err(ProtocolError::FrameTooLarge { length });
                    }
                    self.state = ReadState::Body {
                        bytes: vec![0; length],
                        filled: 0,
                    };
                }
                ReadState::Body { bytes, filled } => {
                    if *filled < bytes.len() {
                        let count = self.reader.read(&mut bytes[*filled..]).await?;
                        if count == 0 {
                            return Err(ProtocolError::Truncated);
                        }
                        *filled += count;
                        continue;
                    }
                    let ReadState::Body { bytes, .. } = std::mem::replace(
                        &mut self.state,
                        ReadState::Prefix {
                            bytes: [0; 4],
                            filled: 0,
                        },
                    ) else {
                        unreachable!("message reader state changed while decoding a body");
                    };
                    return decode(&bytes).map(Some);
                }
            }
        }
    }
}

fn encode(message: &Message) -> Result<Vec<u8>, ProtocolError> {
    let mut encoder = Encoder::default();
    match message {
        Message::Hello { site_id, scope } => {
            encoder.byte(HELLO);
            encoder.bytes(site_id)?;
            encoder.scope(scope)?;
        }
        Message::Scope(scope) => {
            encoder.byte(SCOPE);
            encoder.scope(scope)?;
        }
        Message::BeginBatch { batch_id } => {
            encoder.byte(BEGIN_BATCH);
            encoder.u64(*batch_id);
        }
        Message::BatchChunk(changes) => {
            encoder.byte(BATCH_CHUNK);
            encoder.count(changes.len(), MAX_CHANGES)?;
            for change in changes {
                encoder.change(change)?;
            }
        }
        Message::CommitBatch => encoder.byte(COMMIT_BATCH),
        Message::Ack { batch_id } => {
            encoder.byte(ACK);
            encoder.u64(*batch_id);
        }
        Message::Schema { table, hash } => {
            encoder.byte(SCHEMA);
            encoder.string(table)?;
            encoder.bytes(hash)?;
        }
        Message::Synchronized => encoder.byte(SYNCHRONIZED),
    }
    if encoder.bytes.len() > MAX_FRAME_LEN {
        return Err(ProtocolError::FrameTooLarge {
            length: encoder.bytes.len(),
        });
    }
    Ok(encoder.bytes)
}

fn decode(bytes: &[u8]) -> Result<Message, ProtocolError> {
    let mut decoder = Decoder::new(bytes);
    let message = match decoder.byte()? {
        HELLO => Message::Hello {
            site_id: decoder.bytes()?,
            scope: decoder.scope()?,
        },
        SCOPE => Message::Scope(decoder.scope()?),
        BEGIN_BATCH => Message::BeginBatch {
            batch_id: decoder.u64()?,
        },
        BATCH_CHUNK => {
            let count = decoder.count(MAX_CHANGES)?;
            let mut changes = Vec::with_capacity(count);
            for _ in 0..count {
                changes.push(decoder.change()?);
            }
            Message::BatchChunk(changes)
        }
        COMMIT_BATCH => Message::CommitBatch,
        ACK => Message::Ack {
            batch_id: decoder.u64()?,
        },
        SCHEMA => {
            let table = decoder.string()?;
            let hash = decoder
                .bytes()?
                .try_into()
                .map_err(|_| ProtocolError::InvalidSchema)?;
            Message::Schema { table, hash }
        }
        SYNCHRONIZED => Message::Synchronized,
        tag => return Err(ProtocolError::UnknownMessage(tag)),
    };
    if decoder.remaining() != 0 {
        return Err(ProtocolError::TrailingBytes);
    }
    Ok(message)
}

#[derive(Default)]
struct Encoder {
    bytes: Vec<u8>,
}

impl Encoder {
    fn byte(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn i64(&mut self, value: i64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn count(&mut self, value: usize, limit: usize) -> Result<(), ProtocolError> {
        if value > limit {
            return Err(ProtocolError::CollectionTooLarge);
        }
        let value = u32::try_from(value).map_err(|_| ProtocolError::CollectionTooLarge)?;
        self.bytes.extend_from_slice(&value.to_be_bytes());
        Ok(())
    }

    fn bytes(&mut self, value: &[u8]) -> Result<(), ProtocolError> {
        self.count(value.len(), MAX_FRAME_LEN)?;
        self.bytes.extend_from_slice(value);
        Ok(())
    }

    fn string(&mut self, value: &str) -> Result<(), ProtocolError> {
        self.bytes(value.as_bytes())
    }

    fn sites(&mut self, sites: &SiteSet) -> Result<(), ProtocolError> {
        self.u64(sites.generation());
        self.count(sites.site_ids().len(), MAX_SITE_IDS)?;
        for site_id in sites.site_ids() {
            self.bytes(site_id)?;
        }
        Ok(())
    }

    fn scope(&mut self, scope: &SiteScope) -> Result<(), ProtocolError> {
        match scope {
            SiteScope::Explicit(sites) => {
                self.byte(EXPLICIT_SCOPE);
                self.sites(sites)?;
            }
            SiteScope::ComplementOfPeer => self.byte(COMPLEMENT_OF_PEER),
        }
        Ok(())
    }

    fn value(&mut self, value: &Value) -> Result<(), ProtocolError> {
        match value {
            Value::Null => self.byte(NULL),
            Value::Integer(value) => {
                self.byte(INTEGER);
                self.i64(*value);
            }
            Value::Real(value) => {
                self.byte(REAL);
                self.bytes.extend_from_slice(&value.to_bits().to_be_bytes());
            }
            Value::Text(value) => {
                self.byte(TEXT);
                self.string(value)?;
            }
            Value::Blob(value) => {
                self.byte(BLOB);
                self.bytes(value)?;
            }
        }
        Ok(())
    }

    fn change(&mut self, change: &Change) -> Result<(), ProtocolError> {
        self.string(&change.table)?;
        self.values(&change.key)?;
        match &change.row {
            None => self.byte(DELETE),
            Some(row) => {
                self.byte(UPSERT);
                self.values(row)?;
            }
        }
        self.i64(change.db_version);
        self.bytes(&change.site_id)?;
        self.i64(change.seq);
        Ok(())
    }

    fn values(&mut self, values: &[Value]) -> Result<(), ProtocolError> {
        self.count(values.len(), MAX_VALUES)?;
        values.iter().try_for_each(|value| self.value(value))
    }
}

struct Decoder<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], ProtocolError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(ProtocolError::Truncated)?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(ProtocolError::Truncated)?;
        self.position = end;
        Ok(value)
    }

    fn byte(&mut self) -> Result<u8, ProtocolError> {
        Ok(self.take(1)?[0])
    }

    fn i64(&mut self) -> Result<i64, ProtocolError> {
        let bytes = self.take(8)?.try_into().expect("fixed-width integer");
        Ok(i64::from_be_bytes(bytes))
    }

    fn u64(&mut self) -> Result<u64, ProtocolError> {
        let bytes = self.take(8)?.try_into().expect("fixed-width integer");
        Ok(u64::from_be_bytes(bytes))
    }

    fn count(&mut self, limit: usize) -> Result<usize, ProtocolError> {
        let bytes = self.take(4)?.try_into().expect("fixed-width count");
        let value = u32::from_be_bytes(bytes) as usize;
        if value > limit {
            return Err(ProtocolError::CollectionTooLarge);
        }
        Ok(value)
    }

    fn bytes(&mut self) -> Result<Vec<u8>, ProtocolError> {
        let length = self.count(MAX_FRAME_LEN)?;
        Ok(self.take(length)?.to_vec())
    }

    fn string(&mut self) -> Result<String, ProtocolError> {
        String::from_utf8(self.bytes()?).map_err(|_| ProtocolError::InvalidUtf8)
    }

    fn sites(&mut self) -> Result<SiteSet, ProtocolError> {
        let generation = self.u64()?;
        let count = self.count(MAX_SITE_IDS)?;
        let mut site_ids = Vec::with_capacity(count);
        for _ in 0..count {
            site_ids.push(self.bytes()?);
        }
        SiteSet::try_new(generation, site_ids).map_err(|_| ProtocolError::InvalidSiteSet)
    }

    fn scope(&mut self) -> Result<SiteScope, ProtocolError> {
        match self.byte()? {
            EXPLICIT_SCOPE => Ok(SiteScope::Explicit(self.sites()?)),
            COMPLEMENT_OF_PEER => Ok(SiteScope::ComplementOfPeer),
            tag => Err(ProtocolError::UnknownSiteScope(tag)),
        }
    }

    fn value(&mut self) -> Result<Value, ProtocolError> {
        match self.byte()? {
            NULL => Ok(Value::Null),
            INTEGER => Ok(Value::Integer(self.i64()?)),
            REAL => {
                let bytes = self.take(8)?.try_into().expect("fixed-width real");
                Ok(Value::Real(f64::from_bits(u64::from_be_bytes(bytes))))
            }
            TEXT => Ok(Value::Text(self.string()?)),
            BLOB => Ok(Value::Blob(self.bytes()?)),
            tag => Err(ProtocolError::UnknownValue(tag)),
        }
    }

    fn change(&mut self) -> Result<Change, ProtocolError> {
        let table = self.string()?;
        let key = self.values()?;
        let row = match self.byte()? {
            DELETE => None,
            UPSERT => Some(self.values()?),
            tag => return Err(ProtocolError::UnknownMutation(tag)),
        };
        Ok(Change {
            table,
            key,
            row,
            db_version: self.i64()?,
            site_id: self.bytes()?,
            seq: self.i64()?,
        })
    }

    fn values(&mut self) -> Result<Vec<Value>, ProtocolError> {
        let count = self.count(MAX_VALUES)?;
        (0..count).map(|_| self.value()).collect()
    }
}

pub(crate) fn encode_values(values: &[Value]) -> Result<Vec<u8>, ProtocolError> {
    let mut encoder = Encoder::default();
    encoder.values(values)?;
    Ok(encoder.bytes)
}

pub(crate) fn decode_values(bytes: &[u8]) -> Result<Vec<Value>, ProtocolError> {
    let mut decoder = Decoder::new(bytes);
    let values = decoder.values()?;
    if decoder.remaining() != 0 {
        return Err(ProtocolError::TrailingBytes);
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::duplex;

    use super::*;

    fn messages() -> Vec<Message> {
        vec![
            Message::Hello {
                site_id: vec![1; 16],
                scope: SiteScope::explicit(7, vec![vec![1; 16], vec![2; 16]]).unwrap(),
            },
            Message::Scope(SiteScope::explicit(8, vec![vec![3; 16]]).unwrap()),
            Message::Scope(SiteScope::ComplementOfPeer),
            Message::BeginBatch { batch_id: 11 },
            Message::BatchChunk(vec![
                Change {
                    table: "items".into(),
                    key: vec![Value::Blob(vec![1, 2])],
                    row: None,
                    db_version: 4,
                    site_id: vec![5; 16],
                    seq: 7,
                },
                Change {
                    table: "items".into(),
                    key: vec![Value::Integer(8)],
                    row: Some(vec![Value::Integer(-9)]),
                    db_version: 11,
                    site_id: vec![12; 16],
                    seq: 14,
                },
                Change {
                    table: "items".into(),
                    key: vec![Value::Integer(15)],
                    row: Some(vec![Value::Real(1.5)]),
                    db_version: 17,
                    site_id: vec![18; 16],
                    seq: 20,
                },
                Change {
                    table: "items".into(),
                    key: vec![Value::Integer(21)],
                    row: Some(vec![Value::Text("hello".into())]),
                    db_version: 23,
                    site_id: vec![24; 16],
                    seq: 26,
                },
                Change {
                    table: "items".into(),
                    key: vec![Value::Integer(27)],
                    row: Some(vec![Value::Blob(vec![28, 29])]),
                    db_version: 31,
                    site_id: vec![32; 16],
                    seq: 34,
                },
            ]),
            Message::CommitBatch,
            Message::Ack { batch_id: 35 },
            Message::Schema {
                table: "items".into(),
                hash: [36; 32],
            },
            Message::Synchronized,
        ]
    }

    #[tokio::test]
    async fn messages_round_trip_over_fragmentable_stream() {
        for expected in messages() {
            let (mut writer, mut reader) = duplex(7);
            let sending = tokio::spawn({
                let expected = expected.clone();
                async move { send_message(&mut writer, &expected).await }
            });
            assert_eq!(
                MessageReader::new(&mut reader).receive().await.unwrap(),
                Some(expected)
            );
            sending.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn cancelled_receive_resumes_partial_frame() {
        let expected = Message::Schema {
            table: "items".into(),
            hash: [37; 32],
        };
        let body = encode(&expected).unwrap();
        let mut frame = (body.len() as u32).to_be_bytes().to_vec();
        frame.extend_from_slice(&body);
        let (mut writer, reader) = duplex(frame.len());
        writer.write_all(&frame[..7]).await.unwrap();
        let mut messages = MessageReader::new(reader);

        assert!(
            tokio::time::timeout(Duration::from_millis(1), messages.receive())
                .await
                .is_err()
        );

        writer.write_all(&frame[7..]).await.unwrap();
        assert_eq!(messages.receive().await.unwrap(), Some(expected));
    }

    #[tokio::test]
    async fn clean_eof_and_truncation_are_distinct() {
        let (writer, mut reader) = duplex(8);
        drop(writer);
        assert_eq!(
            MessageReader::new(&mut reader).receive().await.unwrap(),
            None
        );

        let (mut writer, mut reader) = duplex(8);
        writer.write_all(&[0, 0]).await.unwrap();
        drop(writer);
        assert!(matches!(
            MessageReader::new(&mut reader).receive().await,
            Err(ProtocolError::Truncated)
        ));
    }

    #[tokio::test]
    async fn rejects_oversized_frame_before_reading_its_body() {
        let (mut writer, mut reader) = duplex(8);
        writer
            .write_all(&u32::try_from(MAX_FRAME_LEN + 1).unwrap().to_be_bytes())
            .await
            .unwrap();
        assert!(matches!(
            MessageReader::new(&mut reader).receive().await,
            Err(ProtocolError::FrameTooLarge { .. })
        ));
    }

    #[test]
    fn rejects_trailing_and_unknown_data() {
        let mut encoded = encode(&Message::Ack { batch_id: 1 }).unwrap();
        encoded.push(0);
        assert!(matches!(
            decode(&encoded),
            Err(ProtocolError::TrailingBytes)
        ));
        assert!(matches!(
            decode(&[0xff]),
            Err(ProtocolError::UnknownMessage(0xff))
        ));

        let mut encoded = encode(&Message::Schema {
            table: "items".into(),
            hash: [1; 32],
        })
        .unwrap();
        encoded.pop();
        assert!(matches!(decode(&encoded), Err(ProtocolError::Truncated)));
    }
}
