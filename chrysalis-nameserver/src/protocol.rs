/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fmt;
use std::num::NonZeroU16;

pub use chrysalis_core::LinkId;
use chrysalis_core::Pid;
use chrysalis_transport::DatagramAddr;

#[path = "labels.rs"]
mod labels;

pub use labels::KeyError;
pub use labels::LabelError;
pub use labels::LabelKey;
pub use labels::LabelValue;
pub use labels::Labels;

/// The first Chrysalis nameserver protocol version.
pub const VERSION_1: ProtocolVersion = ProtocolVersion::new(1);

/// Adds a distinct rejection when a child PID already has an active link.
pub const VERSION_2: ProtocolVersion = ProtocolVersion::new(2);

/// Adds Kubernetes-style process labels to namespace entries.
pub const VERSION_3: ProtocolVersion = ProtocolVersion::new(3);

/// A negotiated nameserver protocol version.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ProtocolVersion(NonZeroU16);

impl ProtocolVersion {
    /// Constructs a nonzero protocol version.
    pub const fn try_new(value: u16) -> Option<Self> {
        match NonZeroU16::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Constructs a known-nonzero protocol version.
    pub const fn new(value: u16) -> Self {
        match Self::try_new(value) {
            Some(version) => version,
            None => panic!("protocol version must be nonzero"),
        }
    }

    /// Returns the wire value.
    pub const fn get(self) -> u16 {
        self.0.get()
    }
}

/// An inclusive range of protocol versions supported by one peer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VersionRange {
    min: ProtocolVersion,
    max: ProtocolVersion,
}

impl VersionRange {
    /// Constructs an ordered inclusive version range.
    pub const fn try_new(min: ProtocolVersion, max: ProtocolVersion) -> Option<Self> {
        if min.get() <= max.get() {
            Some(Self { min, max })
        } else {
            None
        }
    }

    /// Returns the minimum supported version.
    pub const fn min(self) -> ProtocolVersion {
        self.min
    }

    /// Returns the maximum supported version.
    pub const fn max(self) -> ProtocolVersion {
        self.max
    }
}

/// A request identifier scoped to one link.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestId(u64);

impl RequestId {
    /// Constructs a request ID.
    pub const fn from_u64(value: u64) -> Self {
        Self(value)
    }

    /// Returns the wire value.
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

/// An advertisement sequence number scoped to one link.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PublicationSequence(u64);

impl PublicationSequence {
    /// Constructs a publication sequence number.
    pub const fn from_u64(value: u64) -> Self {
        Self(value)
    }

    /// Returns the wire value.
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

/// An identifier for one staged publication snapshot.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SnapshotId(u64);

impl SnapshotId {
    /// Constructs a snapshot ID.
    pub const fn from_u64(value: u64) -> Self {
        Self(value)
    }

    /// Returns the wire value.
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

/// A revision scoped to one nameserver incarnation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Revision {
    /// The nameserver that issued this revision.
    pub authority: Pid,
    /// The authority-local monotonic revision.
    pub value: u64,
}

/// One contextual way to send datagrams toward a process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Locator {
    /// The carrier address to use as the next hop.
    pub address: DatagramAddr,
    /// Route priority, where lower values are preferred.
    pub priority: u32,
}

/// Connection information for one process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcEntry {
    /// The globally unique process ID.
    pub pid: Pid,
    /// The end-to-end TLS server name authenticated by the process certificate.
    pub tls_server_name: String,
    /// User-defined identifying attributes published with this process.
    pub labels: Labels,
    /// Contextual next-hop candidates for this process.
    pub locators: Vec<Locator>,
}

/// The default number of entries requested in one enumeration page.
pub const DEFAULT_ENUMERATION_PAGE_SIZE: u32 = 256;

/// The largest enumeration page accepted by a nameserver.
pub const MAX_ENUMERATION_PAGE_SIZE: u32 = 4096;

/// A position in one revision-stable nameserver enumeration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EnumerationCursor {
    /// The revision from which every page must be read.
    pub revision: Revision,
    /// The last PID returned by the preceding page.
    pub after: Pid,
}

/// One ordered page of nameserver entries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnumerationPage {
    /// The entries in ascending PID order.
    pub entries: Vec<ProcEntry>,
    /// The nameserver revision that produced this page.
    pub revision: Revision,
    /// The cursor for the next page, if more entries remain.
    pub next: Option<EnumerationCursor>,
}

/// The result of one paginated enumeration request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EnumerationResult {
    /// A page from the requested revision.
    Page(EnumerationPage),
    /// The cursor revision is no longer available and enumeration must restart.
    Stale {
        /// The nameserver's current revision.
        current: Revision,
    },
}

/// The freshness requested from the receiving nameserver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResolveConsistency {
    /// The receiving nameserver may answer from its cache.
    Cached,
    /// The receiving nameserver refreshes from its parent before answering.
    Refresh,
}

/// A positive or expiring negative resolution result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Resolution {
    /// The process resolved to live connection information.
    Found {
        /// The resolved process entry.
        entry: ProcEntry,
        /// The nameserver revision that produced the answer.
        revision: Revision,
    },
    /// The process was absent at one nameserver revision.
    NotFound {
        /// The absent process ID.
        pid: Pid,
        /// The nameserver revision that produced the answer.
        revision: Revision,
        /// How long the receiver may cache this negative result.
        valid_for_millis: u64,
    },
}

impl Resolution {
    /// Returns the process described by this result.
    pub const fn pid(&self) -> Pid {
        match self {
            Self::Found { entry, .. } => entry.pid,
            Self::NotFound { pid, .. } => *pid,
        }
    }

    /// Returns the nameserver revision that produced this result.
    pub const fn revision(&self) -> Revision {
        match self {
            Self::Found { revision, .. } | Self::NotFound { revision, .. } => *revision,
        }
    }
}

/// Why a parent rejected a link handshake.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RejectCode {
    /// The peers do not share a protocol version.
    IncompatibleVersion,
    /// The authenticated peer does not own the PID in its hello.
    IdentityMismatch,
    /// The parent cannot admit another link.
    Unavailable,
    /// The child PID already owns an active link at the parent.
    AlreadyLinked,
}

impl fmt::Display for RejectCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::IncompatibleVersion => "incompatible protocol version",
            Self::IdentityMismatch => "authenticated identity mismatch",
            Self::Unavailable => "parent unavailable",
            Self::AlreadyLinked => "already has an active link",
        })
    }
}

/// One framed message on a PID 0 nameserver link.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Message {
    /// Opens a child-to-parent link.
    Hello {
        /// Versions that the child can speak.
        versions: VersionRange,
        /// The child's authenticated, globally routable PID.
        child: Pid,
    },
    /// Accepts a link and selects its protocol version.
    Welcome {
        /// The selected protocol version.
        version: ProtocolVersion,
        /// The parent's authenticated, globally routable PID.
        parent: Pid,
        /// The one-shot link incarnation allocated by the parent.
        link: LinkId,
    },
    /// Rejects a link handshake.
    Reject {
        /// The rejection reason.
        code: RejectCode,
    },
    /// Begins staging a complete replacement of this link's publications.
    SnapshotBegin {
        /// The snapshot incarnation.
        snapshot: SnapshotId,
        /// The sequence represented by the complete snapshot.
        base_sequence: PublicationSequence,
    },
    /// Adds one ordered chunk to a staged snapshot.
    SnapshotChunk {
        /// The snapshot incarnation.
        snapshot: SnapshotId,
        /// The zero-based chunk index.
        chunk: u32,
        /// Entries in this chunk.
        entries: Vec<ProcEntry>,
    },
    /// Atomically activates a completely received snapshot.
    SnapshotEnd {
        /// The snapshot incarnation.
        snapshot: SnapshotId,
        /// The number of chunks in the snapshot.
        chunks: u32,
    },
    /// Applies one ordered incremental publication update.
    Delta {
        /// The sequence immediately following the previous accepted update.
        sequence: PublicationSequence,
        /// Complete entries to add or replace.
        upserts: Vec<ProcEntry>,
        /// Process IDs to remove.
        removals: Vec<Pid>,
    },
    /// Requests a new complete snapshot after publication state was lost.
    ResnapshotRequired {
        /// The next sequence the receiver expected.
        expected_sequence: PublicationSequence,
    },
    /// Cumulatively acknowledges committed publication state.
    PublicationAck {
        /// The highest snapshot baseline or delta sequence committed by the parent.
        sequence: PublicationSequence,
    },
    /// Resolves one process through the receiving nameserver.
    Resolve {
        /// The caller-generated correlation ID.
        request: RequestId,
        /// The process to resolve.
        pid: Pid,
        /// The requested freshness.
        consistency: ResolveConsistency,
    },
    /// Answers one correlated resolution request.
    ResolveResult {
        /// The request being answered.
        request: RequestId,
        /// The positive or negative result.
        result: Resolution,
    },
    /// Enumerates one ordered page of processes through the receiving nameserver.
    Enumerate {
        /// The caller-generated correlation ID.
        request: RequestId,
        /// The requested freshness.
        consistency: ResolveConsistency,
        /// The prior page position, or `None` to start a new enumeration.
        cursor: Option<EnumerationCursor>,
        /// Requested entries per page. Zero selects the server default.
        limit: u32,
    },
    /// Answers one correlated enumeration request.
    EnumerateResult {
        /// The request being answered.
        request: RequestId,
        /// The page or stale-cursor result.
        result: EnumerationResult,
    },
    /// Pushes a cache update independently of a request.
    CacheUpdate {
        /// The positive or negative cache value.
        result: Resolution,
    },
}
