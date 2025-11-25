/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Serde codec for multipart messages.
//!
//! Using [`serialize`] / [`deserialize`], fields typed [`Part`] are extracted
//! from the main payload and appended to a list of `parts`. Each part is backed by
//! [`bytes::Bytes`] for cheap, zero-copy sharing.
//!
//! On decode, the body and its parts are reassembled into the original value
//! without copying.
//!
//! The on-the-wire form is a [`Message`] (body + parts). Your transport sends
//! and receives [`Message`]s; the codec reconstructs the value, enabling
//! efficient network I/O without compacting data into a single buffer.
//!
//! Implementation note: this crate uses Rust's min_specialization feature to enable
//! the use of [`Part`]s with any Serde serializer or deserializer. This feature
//! is fairly restrictive, and thus the API offered by [`serialize`] / [`deserialize`]
//! is not customizable. If customization is needed, you need to add specialization
//! implementations for these codecs. See [`part::PartSerializer`] and [`part::PartDeserializer`]
//! for details.

#![feature(min_specialization)]
#![feature(assert_matches)]
#![feature(vec_deque_pop_if)]

use std::cell::UnsafeCell;
use std::cmp::min;
use std::collections::VecDeque;
use std::io::IoSlice;
use std::ptr::NonNull;

use bincode::Options;
use bytes::Buf;
use bytes::BufMut;
use bytes::buf::UninitSlice;

mod de;
mod part;
mod ser;
use bytes::Bytes;
use bytes::BytesMut;
pub use part::Part;
use serde::Deserialize;
use serde::Serialize;

/// A multi-part message, comprising a message body and a list of parts.
/// Messages only contain references to underlying byte buffers and are
/// cheaply cloned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    body: Part,
    parts: Vec<Part>,
}

impl Message {
    /// Returns a new message with the given body and parts.
    pub fn from_body_and_parts(body: Part, parts: Vec<Part>) -> Self {
        Self { body, parts }
    }

    /// The body of the message.
    pub fn body(&self) -> &Part {
        &self.body
    }

    /// The list of parts of the message.
    pub fn parts(&self) -> &[Part] {
        &self.parts
    }

    /// Returns the total number of parts (excluding the body) in the message.
    pub fn num_parts(&self) -> usize {
        self.parts.len()
    }

    /// Returns the total size (in bytes) of the message.
    pub fn len(&self) -> usize {
        self.body.len() + self.parts.iter().map(|part| part.len()).sum::<usize>()
    }

    /// Returns whether the message is empty. It is always false, since the body
    /// is always defined.
    pub fn is_empty(&self) -> bool {
        self.body.is_empty() && self.parts.iter().all(|part| part.is_empty())
    }

    /// Convert this message into its constituent components.
    pub fn into_inner(self) -> (Part, Vec<Part>) {
        (self.body, self.parts)
    }

    /// Returns the total size (in bytes) of the message when it is framed.
    pub fn frame_len(&self) -> usize {
        8 * (1 + self.num_parts()) + self.len()
    }

    /// Efficiently frames a message containing the body and all of its parts
    /// using a simple frame-length encoding:
    ///
    /// ```text
    /// +--------------------+-------------------+--------------------+-------------------+   ...   +
    /// | body_len (u64 BE)  |   body bytes      | part1_len (u64 BE) |   part1 bytes     |         |
    /// +--------------------+-------------------+--------------------+-------------------+         +
    ///                                                                                      repeat
    ///                                                                                        for
    ///                                                                                      each part
    /// ```
    pub fn framed(self) -> Frame {
        let (body, parts) = self.into_inner();

        let mut buffers = Vec::with_capacity(2 + 2 * parts.len());

        let body = body.into_inner();
        buffers.push(Bytes::from_owner(body.len().to_be_bytes()));
        buffers.push(body);

        for part in parts {
            let part = part.into_inner();
            buffers.push(Bytes::from_owner(part.len().to_be_bytes()));
            buffers.push(part);
        }

        Frame::from_buffers(buffers)
    }

    /// Reassembles a message from a framed encoding.
    pub fn from_framed(mut buf: Bytes) -> Result<Self, std::io::Error> {
        if buf.len() < 8 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        let body_len = buf.get_u64();
        let body = buf.split_to(body_len as usize);
        let mut parts = Vec::new();
        while !buf.is_empty() {
            parts.push(Self::split_part(&mut buf)?.into());
        }
        Ok(Self {
            body: body.into(),
            parts,
        })
    }

    fn split_part(buf: &mut Bytes) -> Result<Bytes, std::io::Error> {
        if buf.len() < 8 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        let at = buf.get_u64() as usize;
        if buf.len() < at {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        Ok(buf.split_to(at))
    }
}

/// An encoded [`Message`] frame. Implements [`bytes::Buf`],
/// and supports vectored writes. Thus, `Frame` is like a reader
/// of an encoded [`Message`].
#[derive(Clone)]
pub struct Frame {
    buffers: VecDeque<Bytes>,
}

impl Frame {
    /// Construct a new frame from the provided buffers. The frame is a
    /// concatenation of these buffers.
    fn from_buffers(buffers: Vec<Bytes>) -> Self {
        let mut buffers: VecDeque<Bytes> = buffers.into();
        buffers.retain(|buf| !buf.is_empty());
        Self { buffers }
    }

    /// **DO NOT USE THIS**
    pub fn illegal_unipart_frame(body: Bytes) -> Self {
        Self {
            buffers: vec![body].into(),
        }
    }
}

impl Buf for Frame {
    fn remaining(&self) -> usize {
        self.buffers.iter().map(|buf| buf.remaining()).sum()
    }

    fn chunk(&self) -> &[u8] {
        match self.buffers.front() {
            Some(buf) => buf.chunk(),
            None => &[],
        }
    }

    fn advance(&mut self, mut cnt: usize) {
        while cnt > 0 {
            let Some(buf) = self.buffers.front_mut() else {
                panic!("advanced beyond the buffer size");
            };

            if cnt >= buf.remaining() {
                cnt -= buf.remaining();
                self.buffers.pop_front();
                continue;
            }

            buf.advance(cnt);
            cnt = 0;
        }
    }

    // We implement our own chunks_vectored here, as the default implementation
    // does not do any vectoring (returning only a single IoSlice at a time).
    fn chunks_vectored<'a>(&'a self, dst: &mut [IoSlice<'a>]) -> usize {
        let n = min(dst.len(), self.buffers.len());
        for i in 0..n {
            dst[i] = IoSlice::new(self.buffers[i].chunk());
        }
        n
    }
}

/// An unsafe cell of a [`BytesMut`]. This is used to implement an io::Writer
/// for the serializer without exposing lifetime parameters (which cannot be)
/// specialized.
struct UnsafeBufCell {
    buf: UnsafeCell<BytesMut>,
}

impl UnsafeBufCell {
    /// Create a new cell from a [`BytesMut`].
    fn from_bytes_mut(bytes: BytesMut) -> Self {
        Self {
            buf: UnsafeCell::new(bytes),
        }
    }

    /// Convert this cell into its underlying [`BytesMut`].
    fn into_inner(self) -> BytesMut {
        self.buf.into_inner()
    }

    /// Borrow the cell, without lifetime checks. The caller must guarantee that
    /// the returned cell cannot be used after the cell is dropped (usually through
    /// [`UnsafeBufCell::into_inner`]).
    unsafe fn borrow_unchecked(&self) -> UnsafeBufCellRef {
        let ptr =
            // SAFETY: the user is providing the necessary invariants
            unsafe { NonNull::new_unchecked(self.buf.get()) };
        UnsafeBufCellRef { ptr }
    }
}

/// A borrowed reference to an [`UnsafeBufCell`].
struct UnsafeBufCellRef {
    ptr: NonNull<BytesMut>,
}

/// SAFETY: we're extending the implementation of the underlying [`BytesMut`];
/// adding an additional layer of danger by disregarding lifetimes.
unsafe impl BufMut for UnsafeBufCellRef {
    fn remaining_mut(&self) -> usize {
        // SAFETY: extending the implementation of the underlying [`BytesMut`]
        unsafe { self.ptr.as_ref().remaining_mut() }
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        // SAFETY: extending the implementation of the underlying [`BytesMut`]
        unsafe { self.ptr.as_mut().advance_mut(cnt) }
    }

    fn chunk_mut(&mut self) -> &mut UninitSlice {
        // SAFETY: extending the implementation of the underlying [`BytesMut`]
        unsafe { self.ptr.as_mut().chunk_mut() }
    }
}

/// Serialize the provided value into a multipart message. The value is encoded using an
/// extended version of [`bincode`] that skips serializing [`Part`]s, which are instead
/// held directly by the returned message.
///
/// Serialize uses the same codec options as [`bincode::serialize`] / [`bincode::deserialize`].
/// These are currently not customizable unless an explicit specialization is also provided.
pub fn serialize_bincode<S: ?Sized + serde::Serialize>(
    value: &S,
) -> Result<Message, bincode::Error> {
    let buffer = UnsafeBufCell::from_bytes_mut(BytesMut::new());
    // SAFETY: we know here that, once the below "value.serialize()" is done, there are no more
    // extant references to this buffer; we are thus safe to reclaim the buffer into the message
    let buffer_borrow = unsafe { buffer.borrow_unchecked() };
    let mut serializer: part::BincodeSerializer =
        ser::bincode::Serializer::new(bincode::Serializer::new(buffer_borrow.writer(), options()));
    value.serialize(&mut serializer)?;
    Ok(Message {
        body: Part(buffer.into_inner().freeze()),
        parts: serializer.into_parts(),
    })
}

/// Deserialize a message serialized by `[serialize]`, stitching together the original
/// message without copying the underlying buffers.
pub fn deserialize_bincode<T>(message: Message) -> Result<T, bincode::Error>
where
    T: serde::de::DeserializeOwned,
{
    let (body, parts) = message.into_inner();
    let mut deserializer = part::BincodeDeserializer::new(
        bincode::Deserializer::with_reader(body.into_inner().reader(), options()),
        parts.into(),
    );
    let value = T::deserialize(&mut deserializer)?;
    // Check that all parts were consumed:
    deserializer.end()?;
    Ok(value)
}

/// Construct the set of options used by the specialized serializer and deserializer.
fn options() -> part::BincodeOptionsType {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .allow_trailing_bytes()
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::net::SocketAddrV6;

    use proptest::prelude::*;
    use proptest_derive::Arbitrary;
    use serde::Deserialize;
    use serde::Serialize;
    use serde::de::DeserializeOwned;

    use super::*;

    fn test_roundtrip<T>(value: T, expected_parts: usize)
    where
        T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        // Test plain serialization roundtrip:
        let message = serialize_bincode(&value).unwrap();
        assert_eq!(message.num_parts(), expected_parts);
        let deserialized_value = deserialize_bincode(message.clone()).unwrap();
        assert_eq!(value, deserialized_value);

        // Framing roundtrip:
        let mut framed = message.clone().framed();
        let framed = framed.copy_to_bytes(framed.remaining());
        let unframed_message = Message::from_framed(framed).unwrap();
        assert_eq!(message, unframed_message);

        // Bincode passthrough:
        let bincode_serialized = bincode::serialize(&value).unwrap();
        let bincode_deserialized = bincode::deserialize(&bincode_serialized).unwrap();
        assert_eq!(value, bincode_deserialized);
    }

    #[test]
    fn test_specialized_serializer_basic() {
        test_roundtrip(Part::from("hello"), 1);
    }

    #[test]
    fn test_specialized_serializer_compound() {
        test_roundtrip(vec![Part::from("hello"), Part::from("world")], 2);
        test_roundtrip((Part::from("hello"), 1, 2, 3, Part::from("world")), 2);
        test_roundtrip(
            {
                #[derive(Serialize, Deserialize, Debug, PartialEq)]
                struct U {
                    parts: Vec<Part>,
                }
                #[derive(Serialize, Deserialize, Debug, PartialEq)]
                enum E {
                    First(Part),
                    Second(String),
                }

                #[derive(Serialize, Deserialize, Debug, PartialEq)]
                struct T {
                    field2: String,
                    field3: Part,
                    field4: Part,
                    field5: Vec<U>,
                    field6: E,
                }

                T {
                    field2: "hello".to_string(),
                    field3: Part::from("hello"),
                    field4: Part::from("world"),
                    field5: vec![
                        U {
                            parts: vec![Part::from("hello"), Part::from("world")],
                        },
                        U {
                            parts: vec![Part::from("five"), Part::from("six"), Part::from("seven")],
                        },
                    ],
                    field6: E::First(Part::from("eight")),
                }
            },
            8,
        );
        test_roundtrip(
            {
                #[derive(Serialize, Deserialize, Debug, PartialEq)]
                struct T {
                    field1: u64,
                    field2: String,
                    field3: Part,
                    field4: Part,
                    field5: u64,
                }
                T {
                    field1: 1,
                    field2: "hello".to_string(),
                    field3: Part::from("hello"),
                    field4: Part::from("world"),
                    field5: 2,
                }
            },
            2,
        );
    }

    #[test]
    fn test_recursive_message() {
        let message = serialize_bincode(&[Part::from("hello"), Part::from("world")]).unwrap();
        let message_message = serialize_bincode(&message).unwrap();

        // message.body + message.parts (x2):
        assert_eq!(message_message.num_parts(), 3);
    }

    #[test]
    fn test_malformed_messages() {
        let message = Message {
            body: Part::from("hello"),
            parts: vec![Part::from("world")],
        };
        let err = deserialize_bincode::<String>(message).unwrap_err();

        // Normal bincode errors work:
        assert_matches!(*err, bincode::ErrorKind::Io(err) if err.kind() == std::io::ErrorKind::UnexpectedEof);

        let mut message =
            serialize_bincode(&vec![Part::from("hello"), Part::from("world")]).unwrap();
        message.parts.push(Part::from("foo"));
        let err = deserialize_bincode::<Vec<Part>>(message).unwrap_err();
        assert_matches!(*err, bincode::ErrorKind::Custom(message) if message == "multipart overrun while decoding");

        let mut message =
            serialize_bincode(&vec![Part::from("hello"), Part::from("world")]).unwrap();
        let _dropped_message = message.parts.pop().unwrap();
        let err = deserialize_bincode::<Vec<Part>>(message).unwrap_err();
        assert_matches!(*err, bincode::ErrorKind::Custom(message) if message == "multipart underrun while decoding");
    }

    #[test]
    fn test_concat_buf() {
        let buffers = vec![
            Bytes::from("hello"),
            Bytes::from("world"),
            Bytes::from("1"),
            Bytes::from(""),
            Bytes::from("xyz"),
            Bytes::from("xyzd"),
        ];

        let mut concat = Frame::from_buffers(buffers.clone());

        assert_eq!(concat.remaining(), 18);
        concat.advance(2);
        assert_eq!(concat.remaining(), 16);
        assert_eq!(concat.chunk(), &b"llo"[..]);
        concat.advance(4);
        assert_eq!(concat.chunk(), &b"orld"[..]);
        concat.advance(5);
        assert_eq!(concat.chunk(), &b"xyz"[..]);

        let mut concat = Frame::from_buffers(buffers);
        let bytes = concat.copy_to_bytes(concat.remaining());
        assert_eq!(&*bytes, &b"helloworld1xyzxyzd"[..]);
    }

    #[test]
    fn test_framing() {
        let message = Message {
            body: Part::from("hello"),
            parts: vec![
                Part::from("world"),
                Part::from("1"),
                Part::from(""),
                Part::from("xyz"),
                Part::from("xyzd"),
            ],
        };

        let mut framed = message.clone().framed();
        let framed = framed.copy_to_bytes(framed.remaining());
        assert_eq!(Message::from_framed(framed).unwrap(), message);
    }

    #[test]
    fn test_socket_addr() {
        let socket_addr_v6: SocketAddrV6 =
            "[2401:db00:225c:2d09:face:0:223:0]:48483".parse().unwrap();
        {
            let message = serialize_bincode(&socket_addr_v6).unwrap();
            let deserialized: SocketAddrV6 = deserialize_bincode(message).unwrap();
            assert_eq!(socket_addr_v6, deserialized);
        }
        let socket_addr = SocketAddr::V6(socket_addr_v6);
        {
            let message = serialize_bincode(&socket_addr).unwrap();
            let deserialized: SocketAddr = deserialize_bincode(message).unwrap();
            assert_eq!(socket_addr, deserialized);
        }

        let mut address_book: HashMap<usize, SocketAddr> = HashMap::new();
        address_book.insert(1, socket_addr);
        {
            let message = serialize_bincode(&address_book).unwrap();
            let deserialized: HashMap<usize, SocketAddr> = deserialize_bincode(message).unwrap();
            assert_eq!(address_book, deserialized);
        }
    }

    prop_compose! {
        fn arb_bytes()(len in 0..1000000usize) -> Bytes {
            Bytes::from(vec![42; len])
        }
    }

    prop_compose! {
        fn arb_part()(bytes in arb_bytes()) -> Part {
            bytes.into()
        }
    }

    #[derive(Arbitrary, Serialize, Deserialize, Debug, PartialEq)]
    enum TupleEnum {
        One,
        Two(String),
        Three(u32),
    }

    #[derive(Arbitrary, Serialize, Deserialize, Debug, PartialEq)]
    enum StructEnum {
        One {
            a: i32,
        },
        Two {
            s: String,
        },
        Three {
            e: TupleEnum,
            s: String,
            u: u32,
        },
        Four {
            #[proptest(strategy = "arb_part()")]
            part: Part,
        },
    }

    #[derive(Arbitrary, Serialize, Deserialize, Debug, PartialEq)]
    struct S {
        field: String,
        tup: (StructEnum, i32, String, u32, f32),
        tup2: Option<(String, String, String, i32)>,
        e: StructEnum,
        maybe_e: Option<StructEnum>,
        many_e: Vec<(StructEnum, Option<TupleEnum>)>,
        #[proptest(strategy = "arb_bytes()")]
        some_bytes: Bytes,
    }

    #[derive(Arbitrary, Serialize, Deserialize, Debug, PartialEq)]
    struct N(S);

    proptest! {
        #[test]
        fn test_arbitrary_roundtrip(value in any::<N>()) {
            let message = serialize_bincode(&value).unwrap();
            let deserialized_value = deserialize_bincode(message.clone()).unwrap();
            assert_eq!(value, deserialized_value);
        }
    }
}
