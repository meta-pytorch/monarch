/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::str;

use chrysalis_core::Pid;
use chrysalis_transport::DatagramAddr;
use thiserror::Error;

use crate::protocol::EnumerationCursor;
use crate::protocol::EnumerationPage;
use crate::protocol::EnumerationResult;
use crate::protocol::LabelError;
use crate::protocol::Labels;
use crate::protocol::LinkId;
use crate::protocol::Locator;
use crate::protocol::Message;
use crate::protocol::ProcEntry;
use crate::protocol::ProtocolVersion;
use crate::protocol::PublicationSequence;
use crate::protocol::RejectCode;
use crate::protocol::RequestId;
use crate::protocol::Resolution;
use crate::protocol::ResolveConsistency;
use crate::protocol::Revision;
use crate::protocol::SnapshotId;
use crate::protocol::VersionRange;

pub(crate) const LENGTH_PREFIX_LEN: usize = 4;
const WIRE_VERSION: u8 = 3;
const ENVELOPE_LEN: usize = 2;

const HELLO: u8 = 1;
const WELCOME: u8 = 2;
const REJECT: u8 = 3;
const SNAPSHOT_BEGIN: u8 = 4;
const SNAPSHOT_CHUNK: u8 = 5;
const SNAPSHOT_END: u8 = 6;
const DELTA: u8 = 7;
const RESNAPSHOT_REQUIRED: u8 = 8;
const RESOLVE: u8 = 9;
const RESOLVE_RESULT: u8 = 10;
const CACHE_UPDATE: u8 = 11;
const PUBLICATION_ACK: u8 = 12;
const ENUMERATE: u8 = 13;
const ENUMERATE_RESULT: u8 = 14;

const RESOLUTION_FOUND: u8 = 1;
const RESOLUTION_NOT_FOUND: u8 = 2;

const ENUMERATION_PAGE: u8 = 1;
const ENUMERATION_STALE: u8 = 2;

const CONSISTENCY_CACHED: u8 = 1;
const CONSISTENCY_REFRESH: u8 = 2;

const REJECT_INCOMPATIBLE_VERSION: u8 = 1;
const REJECT_IDENTITY_MISMATCH: u8 = 2;
const REJECT_UNAVAILABLE: u8 = 3;
const REJECT_ALREADY_LINKED: u8 = 4;

/// The maximum encoded frame body, excluding its four-byte length prefix.
pub const MAX_FRAME_BODY_LEN: usize = 4 * 1024 * 1024;

const MAX_SCHEME_LEN: usize = 255;
const MAX_TLS_SERVER_NAME_LEN: usize = 255;
const MAX_ADDRESS_LEN: usize = 64 * 1024;
const MAX_LOCATORS_PER_ENTRY: usize = 64;
const MAX_LABELS_PER_ENTRY: usize = 256;
const MAX_LABEL_KEY_LEN: usize = 317;
const MAX_LABEL_VALUE_LEN: usize = 63;
const MAX_ENTRIES_PER_FRAME: usize = 4096;
const MAX_REMOVALS_PER_DELTA: usize = 4096;

/// A nameserver protocol framing or field error.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum CodecError {
    /// The frame ended before a complete value could be decoded.
    #[error("unexpected end of frame")]
    UnexpectedEof,

    /// The declared frame length did not match the supplied bytes.
    #[error("invalid frame length: declared {declared}, actual {actual}")]
    InvalidFrameLength { declared: usize, actual: usize },

    /// The frame exceeds the protocol limit.
    #[error("frame body is too large: {len} bytes, maximum {max}")]
    FrameTooLarge { len: usize, max: usize },

    /// The framing version is unknown.
    #[error("unsupported wire version {0}")]
    UnsupportedWireVersion(u8),

    /// The message tag is unknown.
    #[error("unknown message tag {0}")]
    UnknownMessageTag(u8),

    /// An enum field contains an unknown value.
    #[error("invalid {field} value {value}")]
    InvalidEnumValue { field: &'static str, value: u8 },

    /// A protocol version or range is invalid.
    #[error("invalid protocol version range")]
    InvalidVersionRange,

    /// A string field is not valid UTF-8.
    #[error("invalid UTF-8 in {0}")]
    InvalidUtf8(&'static str),

    /// A process label violates the Kubernetes label grammar.
    #[error(transparent)]
    InvalidLabel(#[from] LabelError),

    /// A length-delimited field exceeds its limit.
    #[error("{field} is too large: {len}, maximum {max}")]
    FieldTooLarge {
        field: &'static str,
        len: usize,
        max: usize,
    },

    /// A repeated field contains too many values.
    #[error("too many {field}: {len}, maximum {max}")]
    TooManyItems {
        field: &'static str,
        len: usize,
        max: usize,
    },

    /// Bytes remained after decoding one message.
    #[error("trailing bytes after message")]
    TrailingBytes,
}

/// Encodes one complete length-prefixed nameserver frame.
pub fn encode_frame(message: &Message) -> Result<Vec<u8>, CodecError> {
    let mut body = Vec::new();
    body.push(WIRE_VERSION);
    body.push(message_tag(message));
    encode_message(message, &mut body)?;
    if body.len() > MAX_FRAME_BODY_LEN {
        return Err(CodecError::FrameTooLarge {
            len: body.len(),
            max: MAX_FRAME_BODY_LEN,
        });
    }
    let mut frame = Vec::with_capacity(LENGTH_PREFIX_LEN + body.len());
    put_u32(&mut frame, body.len() as u32);
    frame.extend_from_slice(&body);
    Ok(frame)
}

/// Decodes one complete length-prefixed nameserver frame.
pub fn decode_frame(frame: &[u8]) -> Result<Message, CodecError> {
    let prefix: [u8; LENGTH_PREFIX_LEN] = frame
        .get(..LENGTH_PREFIX_LEN)
        .ok_or(CodecError::UnexpectedEof)?
        .try_into()
        .expect("checked length");
    let declared = frame_body_len(prefix)?;
    let actual = frame.len() - LENGTH_PREFIX_LEN;
    if declared != actual {
        return Err(CodecError::InvalidFrameLength { declared, actual });
    }
    let mut decoder = Decoder::new(&frame[LENGTH_PREFIX_LEN..]);
    let wire_version = decoder.u8()?;
    if wire_version != WIRE_VERSION {
        return Err(CodecError::UnsupportedWireVersion(wire_version));
    }
    let tag = decoder.u8()?;
    let message = decode_message(tag, &mut decoder)?;
    if !decoder.is_empty() {
        return Err(CodecError::TrailingBytes);
    }
    Ok(message)
}

/// Validates a four-byte frame prefix and returns its body length.
pub fn frame_body_len(prefix: [u8; LENGTH_PREFIX_LEN]) -> Result<usize, CodecError> {
    let len = u32::from_be_bytes(prefix) as usize;
    if len < ENVELOPE_LEN {
        return Err(CodecError::InvalidFrameLength {
            declared: len,
            actual: ENVELOPE_LEN,
        });
    }
    if len > MAX_FRAME_BODY_LEN {
        return Err(CodecError::FrameTooLarge {
            len,
            max: MAX_FRAME_BODY_LEN,
        });
    }
    Ok(len)
}

fn message_tag(message: &Message) -> u8 {
    match message {
        Message::Hello { .. } => HELLO,
        Message::Welcome { .. } => WELCOME,
        Message::Reject { .. } => REJECT,
        Message::SnapshotBegin { .. } => SNAPSHOT_BEGIN,
        Message::SnapshotChunk { .. } => SNAPSHOT_CHUNK,
        Message::SnapshotEnd { .. } => SNAPSHOT_END,
        Message::Delta { .. } => DELTA,
        Message::ResnapshotRequired { .. } => RESNAPSHOT_REQUIRED,
        Message::PublicationAck { .. } => PUBLICATION_ACK,
        Message::Resolve { .. } => RESOLVE,
        Message::ResolveResult { .. } => RESOLVE_RESULT,
        Message::CacheUpdate { .. } => CACHE_UPDATE,
        Message::Enumerate { .. } => ENUMERATE,
        Message::EnumerateResult { .. } => ENUMERATE_RESULT,
    }
}

fn encode_message(message: &Message, output: &mut Vec<u8>) -> Result<(), CodecError> {
    match message {
        Message::Hello { versions, child } => {
            put_version(output, versions.min());
            put_version(output, versions.max());
            put_pid(output, *child);
        }
        Message::Welcome {
            version,
            parent,
            link,
        } => {
            put_version(output, *version);
            put_pid(output, *parent);
            output.extend_from_slice(link.as_bytes());
        }
        Message::Reject { code } => output.push(reject_code(*code)),
        Message::SnapshotBegin {
            snapshot,
            base_sequence,
        } => {
            put_u64(output, snapshot.as_u64());
            put_u64(output, base_sequence.as_u64());
        }
        Message::SnapshotChunk {
            snapshot,
            chunk,
            entries,
        } => {
            put_u64(output, snapshot.as_u64());
            put_u32(output, *chunk);
            encode_entries(output, entries)?;
        }
        Message::SnapshotEnd { snapshot, chunks } => {
            put_u64(output, snapshot.as_u64());
            put_u32(output, *chunks);
        }
        Message::Delta {
            sequence,
            upserts,
            removals,
        } => {
            put_u64(output, sequence.as_u64());
            encode_entries(output, upserts)?;
            check_count("delta removals", removals.len(), MAX_REMOVALS_PER_DELTA)?;
            put_u32(output, removals.len() as u32);
            for pid in removals {
                put_pid(output, *pid);
            }
        }
        Message::ResnapshotRequired { expected_sequence } => {
            put_u64(output, expected_sequence.as_u64());
        }
        Message::PublicationAck { sequence } => put_u64(output, sequence.as_u64()),
        Message::Resolve {
            request,
            pid,
            consistency,
        } => {
            put_u64(output, request.as_u64());
            put_pid(output, *pid);
            output.push(resolve_consistency(*consistency));
        }
        Message::ResolveResult { request, result } => {
            put_u64(output, request.as_u64());
            encode_resolution(output, result)?;
        }
        Message::Enumerate {
            request,
            consistency,
            cursor,
            limit,
        } => {
            put_u64(output, request.as_u64());
            output.push(resolve_consistency(*consistency));
            encode_optional_cursor(output, *cursor);
            put_u32(output, *limit);
        }
        Message::EnumerateResult { request, result } => {
            put_u64(output, request.as_u64());
            encode_enumeration_result(output, result)?;
        }
        Message::CacheUpdate { result } => encode_resolution(output, result)?,
    }
    Ok(())
}

fn decode_message(tag: u8, decoder: &mut Decoder<'_>) -> Result<Message, CodecError> {
    match tag {
        HELLO => {
            let min = decoder.version()?;
            let max = decoder.version()?;
            let versions =
                VersionRange::try_new(min, max).ok_or(CodecError::InvalidVersionRange)?;
            Ok(Message::Hello {
                versions,
                child: decoder.pid()?,
            })
        }
        WELCOME => Ok(Message::Welcome {
            version: decoder.version()?,
            parent: decoder.pid()?,
            link: LinkId::from_bytes(decoder.array()?),
        }),
        REJECT => Ok(Message::Reject {
            code: decode_reject_code(decoder.u8()?)?,
        }),
        SNAPSHOT_BEGIN => Ok(Message::SnapshotBegin {
            snapshot: SnapshotId::from_u64(decoder.u64()?),
            base_sequence: PublicationSequence::from_u64(decoder.u64()?),
        }),
        SNAPSHOT_CHUNK => Ok(Message::SnapshotChunk {
            snapshot: SnapshotId::from_u64(decoder.u64()?),
            chunk: decoder.u32()?,
            entries: decoder.entries()?,
        }),
        SNAPSHOT_END => Ok(Message::SnapshotEnd {
            snapshot: SnapshotId::from_u64(decoder.u64()?),
            chunks: decoder.u32()?,
        }),
        DELTA => {
            let sequence = PublicationSequence::from_u64(decoder.u64()?);
            let upserts = decoder.entries()?;
            let removal_count = decoder.count("delta removals", MAX_REMOVALS_PER_DELTA)?;
            let mut removals = Vec::with_capacity(removal_count);
            for _ in 0..removal_count {
                removals.push(decoder.pid()?);
            }
            Ok(Message::Delta {
                sequence,
                upserts,
                removals,
            })
        }
        RESNAPSHOT_REQUIRED => Ok(Message::ResnapshotRequired {
            expected_sequence: PublicationSequence::from_u64(decoder.u64()?),
        }),
        PUBLICATION_ACK => Ok(Message::PublicationAck {
            sequence: PublicationSequence::from_u64(decoder.u64()?),
        }),
        RESOLVE => Ok(Message::Resolve {
            request: RequestId::from_u64(decoder.u64()?),
            pid: decoder.pid()?,
            consistency: decode_resolve_consistency(decoder.u8()?)?,
        }),
        RESOLVE_RESULT => Ok(Message::ResolveResult {
            request: RequestId::from_u64(decoder.u64()?),
            result: decoder.resolution()?,
        }),
        ENUMERATE => Ok(Message::Enumerate {
            request: RequestId::from_u64(decoder.u64()?),
            consistency: decode_resolve_consistency(decoder.u8()?)?,
            cursor: decoder.optional_cursor()?,
            limit: decoder.u32()?,
        }),
        ENUMERATE_RESULT => Ok(Message::EnumerateResult {
            request: RequestId::from_u64(decoder.u64()?),
            result: decoder.enumeration_result()?,
        }),
        CACHE_UPDATE => Ok(Message::CacheUpdate {
            result: decoder.resolution()?,
        }),
        _ => Err(CodecError::UnknownMessageTag(tag)),
    }
}

fn encode_entries(output: &mut Vec<u8>, entries: &[ProcEntry]) -> Result<(), CodecError> {
    check_count("entries", entries.len(), MAX_ENTRIES_PER_FRAME)?;
    put_u32(output, entries.len() as u32);
    for entry in entries {
        encode_entry(output, entry)?;
    }
    Ok(())
}

fn encode_entry(output: &mut Vec<u8>, entry: &ProcEntry) -> Result<(), CodecError> {
    put_pid(output, entry.pid);
    let server_name = entry.tls_server_name.as_bytes();
    check_field(
        "TLS server name",
        server_name.len(),
        MAX_TLS_SERVER_NAME_LEN,
    )?;
    put_u16(output, server_name.len() as u16);
    output.extend_from_slice(server_name);
    check_count("labels", entry.labels.len(), MAX_LABELS_PER_ENTRY)?;
    put_u32(output, entry.labels.len() as u32);
    for (key, value) in entry.labels.iter() {
        let key = key.as_str().as_bytes();
        let value = value.as_str().as_bytes();
        put_u16(output, key.len() as u16);
        output.extend_from_slice(key);
        put_u16(output, value.len() as u16);
        output.extend_from_slice(value);
    }
    check_count("locators", entry.locators.len(), MAX_LOCATORS_PER_ENTRY)?;
    put_u32(output, entry.locators.len() as u32);
    for locator in &entry.locators {
        put_u32(output, locator.priority);
        encode_address(output, &locator.address)?;
    }
    Ok(())
}

fn encode_address(output: &mut Vec<u8>, address: &DatagramAddr) -> Result<(), CodecError> {
    let scheme = address.scheme().as_bytes();
    check_field("address scheme", scheme.len(), MAX_SCHEME_LEN)?;
    check_field("address bytes", address.opaque().len(), MAX_ADDRESS_LEN)?;
    put_u16(output, scheme.len() as u16);
    output.extend_from_slice(scheme);
    put_u32(output, address.opaque().len() as u32);
    output.extend_from_slice(address.opaque());
    Ok(())
}

fn encode_resolution(output: &mut Vec<u8>, result: &Resolution) -> Result<(), CodecError> {
    match result {
        Resolution::Found { entry, revision } => {
            output.push(RESOLUTION_FOUND);
            encode_entry(output, entry)?;
            encode_revision(output, *revision);
        }
        Resolution::NotFound {
            pid,
            revision,
            valid_for_millis,
        } => {
            output.push(RESOLUTION_NOT_FOUND);
            put_pid(output, *pid);
            encode_revision(output, *revision);
            put_u64(output, *valid_for_millis);
        }
    }
    Ok(())
}

fn encode_optional_cursor(output: &mut Vec<u8>, cursor: Option<EnumerationCursor>) {
    match cursor {
        Some(cursor) => {
            output.push(1);
            encode_revision(output, cursor.revision);
            put_pid(output, cursor.after);
        }
        None => output.push(0),
    }
}

fn encode_enumeration_result(
    output: &mut Vec<u8>,
    result: &EnumerationResult,
) -> Result<(), CodecError> {
    match result {
        EnumerationResult::Page(page) => {
            output.push(ENUMERATION_PAGE);
            encode_entries(output, &page.entries)?;
            encode_revision(output, page.revision);
            encode_optional_cursor(output, page.next);
        }
        EnumerationResult::Stale { current } => {
            output.push(ENUMERATION_STALE);
            encode_revision(output, *current);
        }
    }
    Ok(())
}

fn encode_revision(output: &mut Vec<u8>, revision: Revision) {
    put_pid(output, revision.authority);
    put_u64(output, revision.value);
}

fn put_pid(output: &mut Vec<u8>, pid: Pid) {
    output.extend_from_slice(pid.as_bytes());
}

fn put_version(output: &mut Vec<u8>, version: ProtocolVersion) {
    put_u16(output, version.get());
}

fn put_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn put_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn put_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn check_count(field: &'static str, len: usize, max: usize) -> Result<(), CodecError> {
    if len > max {
        return Err(CodecError::TooManyItems { field, len, max });
    }
    Ok(())
}

fn check_field(field: &'static str, len: usize, max: usize) -> Result<(), CodecError> {
    if len > max {
        return Err(CodecError::FieldTooLarge { field, len, max });
    }
    Ok(())
}

fn reject_code(code: RejectCode) -> u8 {
    match code {
        RejectCode::IncompatibleVersion => REJECT_INCOMPATIBLE_VERSION,
        RejectCode::IdentityMismatch => REJECT_IDENTITY_MISMATCH,
        RejectCode::Unavailable => REJECT_UNAVAILABLE,
        RejectCode::AlreadyLinked => REJECT_ALREADY_LINKED,
    }
}

fn decode_reject_code(value: u8) -> Result<RejectCode, CodecError> {
    match value {
        REJECT_INCOMPATIBLE_VERSION => Ok(RejectCode::IncompatibleVersion),
        REJECT_IDENTITY_MISMATCH => Ok(RejectCode::IdentityMismatch),
        REJECT_UNAVAILABLE => Ok(RejectCode::Unavailable),
        REJECT_ALREADY_LINKED => Ok(RejectCode::AlreadyLinked),
        _ => Err(CodecError::InvalidEnumValue {
            field: "reject code",
            value,
        }),
    }
}

fn resolve_consistency(consistency: ResolveConsistency) -> u8 {
    match consistency {
        ResolveConsistency::Cached => CONSISTENCY_CACHED,
        ResolveConsistency::Refresh => CONSISTENCY_REFRESH,
    }
}

fn decode_resolve_consistency(value: u8) -> Result<ResolveConsistency, CodecError> {
    match value {
        CONSISTENCY_CACHED => Ok(ResolveConsistency::Cached),
        CONSISTENCY_REFRESH => Ok(ResolveConsistency::Refresh),
        _ => Err(CodecError::InvalidEnumValue {
            field: "resolve consistency",
            value,
        }),
    }
}

struct Decoder<'a> {
    remaining: &'a [u8],
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], CodecError> {
        let bytes = self.remaining.get(..len).ok_or(CodecError::UnexpectedEof)?;
        self.remaining = &self.remaining[len..];
        Ok(bytes)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], CodecError> {
        Ok(self.take(N)?.try_into().expect("checked length"))
    }

    fn u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, CodecError> {
        Ok(u16::from_be_bytes(self.array()?))
    }

    fn u32(&mut self) -> Result<u32, CodecError> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, CodecError> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    fn pid(&mut self) -> Result<Pid, CodecError> {
        Ok(Pid::from_bytes(self.array()?))
    }

    fn version(&mut self) -> Result<ProtocolVersion, CodecError> {
        ProtocolVersion::try_new(self.u16()?).ok_or(CodecError::InvalidVersionRange)
    }

    fn count(&mut self, field: &'static str, max: usize) -> Result<usize, CodecError> {
        let len = self.u32()? as usize;
        check_count(field, len, max)?;
        Ok(len)
    }

    fn entries(&mut self) -> Result<Vec<ProcEntry>, CodecError> {
        let len = self.count("entries", MAX_ENTRIES_PER_FRAME)?;
        let mut entries = Vec::with_capacity(len);
        for _ in 0..len {
            entries.push(self.entry()?);
        }
        Ok(entries)
    }

    fn entry(&mut self) -> Result<ProcEntry, CodecError> {
        let pid = self.pid()?;
        let server_name_len = self.u16()? as usize;
        check_field("TLS server name", server_name_len, MAX_TLS_SERVER_NAME_LEN)?;
        let tls_server_name = str::from_utf8(self.take(server_name_len)?)
            .map_err(|_| CodecError::InvalidUtf8("TLS server name"))?
            .to_owned();
        let label_count = self.count("labels", MAX_LABELS_PER_ENTRY)?;
        let mut labels = Vec::with_capacity(label_count);
        for _ in 0..label_count {
            let key_len = self.u16()? as usize;
            check_field("label key", key_len, MAX_LABEL_KEY_LEN)?;
            let key = str::from_utf8(self.take(key_len)?)
                .map_err(|_| CodecError::InvalidUtf8("label key"))?
                .to_owned();
            let value_len = self.u16()? as usize;
            check_field("label value", value_len, MAX_LABEL_VALUE_LEN)?;
            let value = str::from_utf8(self.take(value_len)?)
                .map_err(|_| CodecError::InvalidUtf8("label value"))?
                .to_owned();
            labels.push((key, value));
        }
        let labels = Labels::try_from_iter(labels)?;
        let len = self.count("locators", MAX_LOCATORS_PER_ENTRY)?;
        let mut locators = Vec::with_capacity(len);
        for _ in 0..len {
            locators.push(Locator {
                priority: self.u32()?,
                address: self.address()?,
            });
        }
        Ok(ProcEntry {
            pid,
            tls_server_name,
            labels,
            locators,
        })
    }

    fn address(&mut self) -> Result<DatagramAddr, CodecError> {
        let scheme_len = self.u16()? as usize;
        check_field("address scheme", scheme_len, MAX_SCHEME_LEN)?;
        let scheme = str::from_utf8(self.take(scheme_len)?)
            .map_err(|_| CodecError::InvalidUtf8("address scheme"))?;
        let address_len = self.u32()? as usize;
        check_field("address bytes", address_len, MAX_ADDRESS_LEN)?;
        let address = self.take(address_len)?.to_vec();
        Ok(DatagramAddr::new(scheme.to_owned(), address))
    }

    fn revision(&mut self) -> Result<Revision, CodecError> {
        Ok(Revision {
            authority: self.pid()?,
            value: self.u64()?,
        })
    }

    fn resolution(&mut self) -> Result<Resolution, CodecError> {
        let kind = self.u8()?;
        match kind {
            RESOLUTION_FOUND => Ok(Resolution::Found {
                entry: self.entry()?,
                revision: self.revision()?,
            }),
            RESOLUTION_NOT_FOUND => Ok(Resolution::NotFound {
                pid: self.pid()?,
                revision: self.revision()?,
                valid_for_millis: self.u64()?,
            }),
            _ => Err(CodecError::InvalidEnumValue {
                field: "resolution",
                value: kind,
            }),
        }
    }

    fn optional_cursor(&mut self) -> Result<Option<EnumerationCursor>, CodecError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(EnumerationCursor {
                revision: self.revision()?,
                after: self.pid()?,
            })),
            value => Err(CodecError::InvalidEnumValue {
                field: "optional enumeration cursor",
                value,
            }),
        }
    }

    fn enumeration_result(&mut self) -> Result<EnumerationResult, CodecError> {
        match self.u8()? {
            ENUMERATION_PAGE => Ok(EnumerationResult::Page(EnumerationPage {
                entries: self.entries()?,
                revision: self.revision()?,
                next: self.optional_cursor()?,
            })),
            ENUMERATION_STALE => Ok(EnumerationResult::Stale {
                current: self.revision()?,
            }),
            value => Err(CodecError::InvalidEnumValue {
                field: "enumeration result",
                value,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use chrysalis_core::PID_LEN;

    use super::*;
    use crate::protocol::VERSION_1;

    const CHILD: Pid = Pid::from_bytes([0x11; PID_LEN]);
    const PARENT: Pid = Pid::from_bytes([0x22; PID_LEN]);
    const TARGET: Pid = Pid::from_bytes([0x33; PID_LEN]);

    fn version_range() -> VersionRange {
        VersionRange::try_new(VERSION_1, VERSION_1).expect("ordered version range")
    }

    fn entry() -> ProcEntry {
        ProcEntry {
            pid: TARGET,
            tls_server_name: "target.test".into(),
            labels: Labels::try_from_iter([
                ("app.kubernetes.io/name", "target"),
                ("tier", "frontend"),
            ])
            .expect("valid labels"),
            locators: vec![
                Locator {
                    address: DatagramAddr::new("udp", [127, 0, 0, 1]),
                    priority: 10,
                },
                Locator {
                    address: DatagramAddr::new("unixgram", b"/tmp/chrysalis.sock".as_slice()),
                    priority: 20,
                },
            ],
        }
    }

    fn revision() -> Revision {
        Revision {
            authority: PARENT,
            value: 42,
        }
    }

    fn round_trip(message: Message) {
        let frame = encode_frame(&message).expect("encode frame");
        assert_eq!(
            frame_body_len(frame[..LENGTH_PREFIX_LEN].try_into().expect("frame prefix"))
                .expect("decode frame length"),
            frame.len() - LENGTH_PREFIX_LEN
        );
        assert_eq!(decode_frame(&frame).expect("decode frame"), message);
    }

    #[test]
    fn round_trips_every_message() {
        let found = Resolution::Found {
            entry: entry(),
            revision: revision(),
        };
        let not_found = Resolution::NotFound {
            pid: TARGET,
            revision: revision(),
            valid_for_millis: 5000,
        };
        let cursor = EnumerationCursor {
            revision: revision(),
            after: TARGET,
        };
        for message in [
            Message::Hello {
                versions: version_range(),
                child: CHILD,
            },
            Message::Welcome {
                version: VERSION_1,
                parent: PARENT,
                link: LinkId::from_bytes([0x44; 16]),
            },
            Message::Reject {
                code: RejectCode::Unavailable,
            },
            Message::Reject {
                code: RejectCode::AlreadyLinked,
            },
            Message::SnapshotBegin {
                snapshot: SnapshotId::from_u64(7),
                base_sequence: PublicationSequence::from_u64(10),
            },
            Message::SnapshotChunk {
                snapshot: SnapshotId::from_u64(7),
                chunk: 0,
                entries: vec![entry()],
            },
            Message::SnapshotEnd {
                snapshot: SnapshotId::from_u64(7),
                chunks: 1,
            },
            Message::Delta {
                sequence: PublicationSequence::from_u64(11),
                upserts: vec![entry()],
                removals: vec![CHILD],
            },
            Message::ResnapshotRequired {
                expected_sequence: PublicationSequence::from_u64(12),
            },
            Message::PublicationAck {
                sequence: PublicationSequence::from_u64(11),
            },
            Message::Resolve {
                request: RequestId::from_u64(9),
                pid: TARGET,
                consistency: ResolveConsistency::Refresh,
            },
            Message::ResolveResult {
                request: RequestId::from_u64(9),
                result: found.clone(),
            },
            Message::Enumerate {
                request: RequestId::from_u64(10),
                consistency: ResolveConsistency::Refresh,
                cursor: Some(cursor),
                limit: 100,
            },
            Message::EnumerateResult {
                request: RequestId::from_u64(10),
                result: EnumerationResult::Page(EnumerationPage {
                    entries: vec![entry()],
                    revision: revision(),
                    next: Some(cursor),
                }),
            },
            Message::EnumerateResult {
                request: RequestId::from_u64(11),
                result: EnumerationResult::Stale {
                    current: revision(),
                },
            },
            Message::CacheUpdate { result: not_found },
        ] {
            round_trip(message);
        }
    }

    #[test]
    fn rejects_truncated_and_mismatched_frames() {
        let frame = encode_frame(&Message::Hello {
            versions: version_range(),
            child: CHILD,
        })
        .expect("encode frame");

        assert_eq!(decode_frame(&frame[..3]), Err(CodecError::UnexpectedEof));
        assert!(matches!(
            decode_frame(&frame[..frame.len() - 1]),
            Err(CodecError::InvalidFrameLength { .. })
        ));
    }

    #[test]
    fn rejects_unknown_wire_version_and_message_tag() {
        let mut version = vec![0, 0, 0, 2, WIRE_VERSION + 1, HELLO];
        assert_eq!(
            decode_frame(&version),
            Err(CodecError::UnsupportedWireVersion(WIRE_VERSION + 1))
        );

        version[4] = WIRE_VERSION;
        version[5] = 0xff;
        assert_eq!(
            decode_frame(&version),
            Err(CodecError::UnknownMessageTag(0xff))
        );
    }

    #[test]
    fn rejects_invalid_version_ranges() {
        let mut frame = vec![0, 0, 0, 22, WIRE_VERSION, HELLO];
        put_u16(&mut frame, 2);
        put_u16(&mut frame, 1);
        frame.extend_from_slice(CHILD.as_bytes());

        assert_eq!(decode_frame(&frame), Err(CodecError::InvalidVersionRange));
    }

    #[test]
    fn rejects_oversized_address_fields() {
        let message = Message::SnapshotChunk {
            snapshot: SnapshotId::from_u64(1),
            chunk: 0,
            entries: vec![ProcEntry {
                pid: TARGET,
                tls_server_name: "target.test".into(),
                labels: Labels::new(),
                locators: vec![Locator {
                    address: DatagramAddr::new("x", vec![0; MAX_ADDRESS_LEN + 1]),
                    priority: 0,
                }],
            }],
        };

        assert_eq!(
            encode_frame(&message),
            Err(CodecError::FieldTooLarge {
                field: "address bytes",
                len: MAX_ADDRESS_LEN + 1,
                max: MAX_ADDRESS_LEN,
            })
        );
    }

    #[test]
    fn rejects_oversized_frame_prefix_before_allocation() {
        assert_eq!(
            frame_body_len(((MAX_FRAME_BODY_LEN + 1) as u32).to_be_bytes()),
            Err(CodecError::FrameTooLarge {
                len: MAX_FRAME_BODY_LEN + 1,
                max: MAX_FRAME_BODY_LEN,
            })
        );
    }
}
