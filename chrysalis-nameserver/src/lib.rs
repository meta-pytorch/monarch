/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Deterministic delegated process namespace for Chrysalis.
//!
//! A Chrysalis nameserver forms a tree of authenticated transport endpoints. A
//! child opens one ordered bidirectional QUIC stream to its parent using
//! [`NAMESERVER_LINK_PROTOCOL`]. PID 0 selects this link-local service; it is
//! not a process identity and must never appear in the delegated namespace.
//! The stream is both the protocol session and the lease for every process
//! entry published through it.
//!
//! QUIC authenticates the transport peer before this protocol begins. The
//! first message must be [`Message::Hello`], and its `child` PID must equal the
//! authenticated peer PID supplied by the transport. The PID in the message is
//! not authentication by itself. The parent selects the highest supported
//! [`ProtocolVersion`] in the offered [`VersionRange`], commits admission of
//! the child, and only then sends [`Message::Welcome`] with a parent-scoped
//! [`LinkId`]. A [`Message::Reject`] ends negotiation. No publication or query
//! message is valid before `Welcome`, and an implementation must not process
//! another message while admission is awaiting commit.
//!
//! Once admitted, the ordered stream carries interleaved publication and query
//! traffic:
//!
//! - The child publishes the namespace delegated to the link. A baseline is a
//!   [`Message::SnapshotBegin`], zero or more [`Message::SnapshotChunk`]
//!   messages with contiguous zero-based chunk numbers, and a matching
//!   [`Message::SnapshotEnd`]. The parent stages all chunks and makes the
//!   snapshot visible atomically only after the end marker verifies the exact
//!   chunk count. It then sends [`Message::PublicationAck`].
//! - After a baseline, [`Message::Delta`] messages carry contiguous
//!   [`PublicationSequence`] values. A gap produces
//!   [`Message::ResnapshotRequired`] with the next expected sequence instead
//!   of applying a partial history. A child has at most one publication update
//!   awaiting acknowledgement, so an acknowledgement unambiguously releases
//!   the published state associated with that update.
//! - The child may send [`Message::Resolve`] and [`Message::Enumerate`] while
//!   publication is active. The parent returns the corresponding result with
//!   the same [`RequestId`]. Request IDs are scoped to the link and share one
//!   namespace across both query kinds; a request ID must not be reused while
//!   either kind of request is outstanding. The parent may also send an
//!   unsolicited [`Message::CacheUpdate`] when fresher routing information is
//!   available.
//!
//! Snapshot identifiers and publication sequences are link-scoped. Exact
//! replay of the latest committed snapshot or delta is idempotent and may be
//! acknowledged again, but reuse of an identifier with different contents is
//! a protocol error. A snapshot base sequence must not move backwards. A
//! delta cannot precede a committed baseline or overlap a staged snapshot.
//! Published entries must have nonzero PIDs, must not claim the receiving
//! parent's PID, and must not duplicate a PID within an update. A PID may be
//! owned by only one active child link, and a link may remove only entries it
//! owns. An update cannot both upsert and remove the same PID.
//!
//! A [`ProcEntry`] describes how to reach and authenticate a process. Its
//! [`Locator`] values are candidate next hops, with lower numeric priorities
//! preferred. They are routing hints, not proof that the named process is
//! alive. Each parent aggregates its own entry with its descendants and
//! advertises those entries upward using locators appropriate for the
//! parent-child hop. The eventual QUIC handshake authenticates the destination
//! PID and TLS server name. Consequently, a stale resolution can cause a
//! failed connection but must not let a caller authenticate the wrong process.
//!
//! [`ResolveConsistency::Cached`] permits locally cached information, whereas
//! [`ResolveConsistency::Refresh`] asks the parent to refresh through the
//! hierarchy. Positive and negative [`Resolution`] values carry an
//! authority-scoped [`Revision`]. Revisions are ordered only within one
//! nameserver incarnation and must not be compared as a global clock. A
//! negative result's `valid_for_millis` is converted to a deadline when it is
//! received; replaying the same result must not extend that deadline.
//! Enumeration is ordered by ascending PID and is stable only for the revision
//! in its [`EnumerationCursor`]. If the directory changes between pages, the
//! server returns [`EnumerationResult::Stale`] and the caller restarts. A zero
//! limit requests [`DEFAULT_ENUMERATION_PAGE_SIZE`], and no request may exceed
//! [`MAX_ENUMERATION_PAGE_SIZE`].
//!
//! The namespace is soft state. Closing or losing the link first closes the
//! route into that delegated subtree, then atomically removes every entry
//! owned by the link and republishes the change upward. Implementations must
//! preserve that order: no packet may be forwarded into a child subtree after
//! the parent advertises the link as inactive. Resolution is linearizable at
//! one nameserver, but there is no transaction or globally comparable revision
//! spanning the hierarchy; updates converge upward asynchronously.
//!
//! Frames use a four-byte big-endian body length followed by a body containing
//! a framing version, a message tag, and the message fields. The length does
//! not include the prefix and cannot exceed [`MAX_FRAME_BODY_LEN`]. This
//! framing version is independent of the negotiated [`ProtocolVersion`]: the
//! former describes how bytes are decoded, while the latter selects session
//! semantics. [`frame_body_len`] validates the prefix before a caller allocates
//! or reads the body, and [`decode_frame`] rejects unsupported framing
//! versions, unknown tags, invalid text or labels, out-of-range counts, and
//! trailing bytes.
//!
//! The codec enforces structural validity only. A session implementation must
//! additionally enforce authentication binding, message direction, handshake
//! and publication ordering, request uniqueness, ownership, revision, and
//! link-lifetime invariants described above. Code must not treat a successfully
//! decoded [`Message`] as authorized or valid for the current session state.

mod cache;
mod child;
mod codec;
mod protocol;
mod session;
mod state;
mod stream;

pub use cache::CacheError;
pub use cache::CacheTime;
pub use cache::ResolverCache;
pub use child::ChildEvent;
pub use child::ChildSession;
pub use child::ChildSessionError;
pub use codec::CodecError;
pub use codec::MAX_FRAME_BODY_LEN;
pub use codec::decode_frame;
pub use codec::encode_frame;
pub use codec::frame_body_len;
pub use protocol::DEFAULT_ENUMERATION_PAGE_SIZE;
pub use protocol::EnumerationCursor;
pub use protocol::EnumerationPage;
pub use protocol::EnumerationResult;
pub use protocol::LinkId;
pub use protocol::Locator;
pub use protocol::MAX_ENUMERATION_PAGE_SIZE;
pub use protocol::Message;
pub use protocol::ProcEntry;
pub use protocol::ProtocolVersion;
pub use protocol::PublicationSequence;
pub use protocol::RejectCode;
pub use protocol::RequestId;
pub use protocol::Resolution;
pub use protocol::ResolveConsistency;
pub use protocol::Revision;
pub use protocol::SnapshotId;
pub use protocol::VERSION_1;
pub use protocol::VERSION_2;
pub use protocol::VersionRange;
pub use session::EnumerateRequest;
pub use session::ParentAction;
pub use session::ParentSession;
pub use session::ResolveRequest;
pub use session::SessionError;
pub use state::ApplyEffects;
pub use state::ApplyError;
pub use state::Command;
pub use state::DirectoryChange;
pub use state::LinkResponse;
pub use state::Nameserver;
pub use stream::MessageStreamError;

/// The reserved link-local stream protocol used by the nameserver.
pub const NAMESERVER_LINK_PROTOCOL: chrysalis_transport::LinkLocalProtocolId =
    chrysalis_transport::LinkLocalProtocolId::from_bytes(*b"chrysalis.ns.v2\0");
