/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use chrysalis_core::Pid;
use thiserror::Error;

use crate::EnumerationCursor;
use crate::EnumerationResult;
use crate::LinkId;
use crate::Message;
use crate::ProcEntry;
use crate::ProtocolVersion;
use crate::PublicationSequence;
use crate::RejectCode;
use crate::RequestId;
use crate::Resolution;
use crate::ResolveConsistency;
use crate::SnapshotId;
use crate::VersionRange;

/// An accepted parent message delivered to the parent-link task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChildEvent {
    /// The parent admitted this link.
    Established {
        /// The negotiated protocol version.
        version: ProtocolVersion,
        /// The parent-allocated one-shot link ID.
        link: LinkId,
    },
    /// The parent rejected this link.
    Rejected {
        /// The rejection reason.
        code: RejectCode,
    },
    /// The parent committed publication state through this sequence.
    PublicationAck {
        /// The cumulatively acknowledged sequence.
        sequence: PublicationSequence,
    },
    /// The parent lost incremental publication state.
    ResnapshotRequired {
        /// The next sequence expected by the parent.
        expected_sequence: PublicationSequence,
    },
    /// The parent completed one correlated resolution request.
    Resolved {
        /// The completed request identifier.
        request: RequestId,
        /// The positive or negative result.
        result: Resolution,
    },
    /// The parent completed one correlated enumeration request.
    Enumerated {
        /// The completed request identifier.
        request: RequestId,
        /// The page or stale-cursor result.
        result: EnumerationResult,
    },
    /// The parent pushed an unsolicited cache update.
    CacheUpdate {
        /// The positive or negative cache value.
        result: Resolution,
    },
}

/// A child-side parent-link protocol failure or caller contract violation.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ChildSessionError {
    /// A configured child or parent used the reserved link-local PID.
    #[error("reserved link-local nameserver PID")]
    ReservedPid,

    /// The parent claimed another authenticated identity.
    #[error("parent PID mismatch: expected {expected:?}, got {actual:?}")]
    ParentMismatch {
        /// The transport-authenticated parent PID.
        expected: Pid,
        /// The PID in the welcome message.
        actual: Pid,
    },

    /// The parent selected a protocol version outside the advertised range.
    #[error("parent selected unsupported protocol version {0:?}")]
    UnsupportedVersion(ProtocolVersion),

    /// The operation requires an established parent link.
    #[error("parent session is not active")]
    NotActive,

    /// The session is terminal.
    #[error("parent session is closed")]
    Closed,

    /// The parent sent a message that is invalid in the current state.
    #[error("unexpected {message} message while {state}")]
    UnexpectedMessage {
        /// The current child session state.
        state: &'static str,
        /// The received message kind.
        message: &'static str,
    },

    /// The caller reused an unresolved request identifier.
    #[error("request is already pending: {request:?}")]
    DuplicateRequest {
        /// The duplicate request identifier.
        request: RequestId,
    },

    /// The parent answered an unknown request identifier.
    #[error("unknown resolution request: {request:?}")]
    UnknownRequest {
        /// The unknown request identifier.
        request: RequestId,
    },

    /// The parent answered an unknown enumeration request identifier.
    #[error("unknown enumeration request: {request:?}")]
    UnknownEnumerationRequest {
        /// The unknown request identifier.
        request: RequestId,
    },

    /// The parent returned a result for another PID.
    #[error("resolution PID mismatch: expected {expected:?}, got {actual:?}")]
    ResolutionPidMismatch {
        /// The requested PID.
        expected: Pid,
        /// The PID in the result.
        actual: Pid,
    },
}

/// Drives the child side of one authenticated parent nameserver stream.
#[derive(Debug)]
pub struct ChildSession {
    child: Pid,
    parent: Pid,
    versions: VersionRange,
    state: State,
    pending_resolutions: BTreeMap<RequestId, Pid>,
    pending_enumerations: BTreeSet<RequestId>,
}

#[derive(Debug)]
enum State {
    AwaitingWelcome,
    Active {
        version: ProtocolVersion,
        link: LinkId,
    },
    Closed,
}

impl ChildSession {
    /// Constructs a session for a stream authenticated to `parent`.
    pub fn try_new(
        child: Pid,
        parent: Pid,
        versions: VersionRange,
    ) -> Result<Self, ChildSessionError> {
        if child.is_link_local() || parent.is_link_local() {
            return Err(ChildSessionError::ReservedPid);
        }
        Ok(Self {
            child,
            parent,
            versions,
            state: State::AwaitingWelcome,
            pending_resolutions: BTreeMap::new(),
            pending_enumerations: BTreeSet::new(),
        })
    }

    /// Returns the opening handshake message.
    pub fn hello(&self) -> Result<Message, ChildSessionError> {
        match self.state {
            State::AwaitingWelcome => Ok(Message::Hello {
                versions: self.versions,
                child: self.child,
            }),
            State::Active { .. } => Err(ChildSessionError::UnexpectedMessage {
                state: "active",
                message: "hello",
            }),
            State::Closed => Err(ChildSessionError::Closed),
        }
    }

    /// Handles one complete message received from the parent.
    pub fn receive(&mut self, message: Message) -> Result<ChildEvent, ChildSessionError> {
        match self.state {
            State::AwaitingWelcome => self.receive_handshake(message),
            State::Active { .. } => self.receive_active(message),
            State::Closed => Err(ChildSessionError::Closed),
        }
    }

    /// Creates a snapshot-begin message for the active parent link.
    pub fn snapshot_begin(
        &self,
        snapshot: SnapshotId,
        base_sequence: PublicationSequence,
    ) -> Result<Message, ChildSessionError> {
        self.require_active()?;
        Ok(Message::SnapshotBegin {
            snapshot,
            base_sequence,
        })
    }

    /// Creates one ordered snapshot-chunk message for the active parent link.
    pub fn snapshot_chunk(
        &self,
        snapshot: SnapshotId,
        chunk: u32,
        entries: Vec<ProcEntry>,
    ) -> Result<Message, ChildSessionError> {
        self.require_active()?;
        Ok(Message::SnapshotChunk {
            snapshot,
            chunk,
            entries,
        })
    }

    /// Creates a snapshot-end message for the active parent link.
    pub fn snapshot_end(
        &self,
        snapshot: SnapshotId,
        chunks: u32,
    ) -> Result<Message, ChildSessionError> {
        self.require_active()?;
        Ok(Message::SnapshotEnd { snapshot, chunks })
    }

    /// Creates one ordered publication delta for the active parent link.
    pub fn delta(
        &self,
        sequence: PublicationSequence,
        upserts: Vec<ProcEntry>,
        removals: Vec<Pid>,
    ) -> Result<Message, ChildSessionError> {
        self.require_active()?;
        Ok(Message::Delta {
            sequence,
            upserts,
            removals,
        })
    }

    /// Starts one correlated resolution request.
    pub fn resolve(
        &mut self,
        request: RequestId,
        pid: Pid,
        consistency: ResolveConsistency,
    ) -> Result<Message, ChildSessionError> {
        self.require_active()?;
        if self.request_is_pending(request) {
            return Err(ChildSessionError::DuplicateRequest { request });
        }
        self.pending_resolutions.insert(request, pid);
        Ok(Message::Resolve {
            request,
            pid,
            consistency,
        })
    }

    /// Starts one correlated enumeration request.
    pub fn enumerate(
        &mut self,
        request: RequestId,
        consistency: ResolveConsistency,
        cursor: Option<EnumerationCursor>,
        limit: u32,
    ) -> Result<Message, ChildSessionError> {
        self.require_active()?;
        if self.request_is_pending(request) {
            return Err(ChildSessionError::DuplicateRequest { request });
        }
        self.pending_enumerations.insert(request);
        Ok(Message::Enumerate {
            request,
            consistency,
            cursor,
            limit,
        })
    }

    /// Terminates this session and forgets every pending request.
    pub fn close(&mut self) {
        self.pending_resolutions.clear();
        self.pending_enumerations.clear();
        self.state = State::Closed;
    }

    /// Returns the negotiated link ID after admission.
    pub const fn link(&self) -> Option<LinkId> {
        match self.state {
            State::Active { link, .. } => Some(link),
            State::AwaitingWelcome | State::Closed => None,
        }
    }

    /// Returns the negotiated protocol version after admission.
    pub const fn version(&self) -> Option<ProtocolVersion> {
        match self.state {
            State::Active { version, .. } => Some(version),
            State::AwaitingWelcome | State::Closed => None,
        }
    }

    fn receive_handshake(&mut self, message: Message) -> Result<ChildEvent, ChildSessionError> {
        match message {
            Message::Welcome {
                version,
                parent,
                link,
            } => {
                if parent != self.parent {
                    return Err(ChildSessionError::ParentMismatch {
                        expected: self.parent,
                        actual: parent,
                    });
                }
                if version < self.versions.min() || version > self.versions.max() {
                    return Err(ChildSessionError::UnsupportedVersion(version));
                }
                self.state = State::Active { version, link };
                Ok(ChildEvent::Established { version, link })
            }
            Message::Reject { code } => {
                self.state = State::Closed;
                Ok(ChildEvent::Rejected { code })
            }
            message => Err(unexpected("awaiting welcome", &message)),
        }
    }

    fn receive_active(&mut self, message: Message) -> Result<ChildEvent, ChildSessionError> {
        match message {
            Message::PublicationAck { sequence } => Ok(ChildEvent::PublicationAck { sequence }),
            Message::ResnapshotRequired { expected_sequence } => {
                Ok(ChildEvent::ResnapshotRequired { expected_sequence })
            }
            Message::ResolveResult { request, result } => {
                let expected = self
                    .pending_resolutions
                    .get(&request)
                    .copied()
                    .ok_or(ChildSessionError::UnknownRequest { request })?;
                let actual = result.pid();
                if actual != expected {
                    return Err(ChildSessionError::ResolutionPidMismatch { expected, actual });
                }
                self.pending_resolutions.remove(&request);
                Ok(ChildEvent::Resolved { request, result })
            }
            Message::EnumerateResult { request, result } => {
                if !self.pending_enumerations.remove(&request) {
                    return Err(ChildSessionError::UnknownEnumerationRequest { request });
                }
                Ok(ChildEvent::Enumerated { request, result })
            }
            Message::CacheUpdate { result } => Ok(ChildEvent::CacheUpdate { result }),
            message => Err(unexpected("active", &message)),
        }
    }

    fn require_active(&self) -> Result<(), ChildSessionError> {
        match self.state {
            State::Active { .. } => Ok(()),
            State::AwaitingWelcome => Err(ChildSessionError::NotActive),
            State::Closed => Err(ChildSessionError::Closed),
        }
    }

    fn request_is_pending(&self, request: RequestId) -> bool {
        self.pending_resolutions.contains_key(&request)
            || self.pending_enumerations.contains(&request)
    }
}

fn unexpected(state: &'static str, message: &Message) -> ChildSessionError {
    ChildSessionError::UnexpectedMessage {
        state,
        message: message_kind(message),
    }
}

fn message_kind(message: &Message) -> &'static str {
    match message {
        Message::Hello { .. } => "hello",
        Message::Welcome { .. } => "welcome",
        Message::Reject { .. } => "reject",
        Message::SnapshotBegin { .. } => "snapshot begin",
        Message::SnapshotChunk { .. } => "snapshot chunk",
        Message::SnapshotEnd { .. } => "snapshot end",
        Message::Delta { .. } => "delta",
        Message::ResnapshotRequired { .. } => "resnapshot required",
        Message::PublicationAck { .. } => "publication acknowledgment",
        Message::Resolve { .. } => "resolve",
        Message::ResolveResult { .. } => "resolve result",
        Message::Enumerate { .. } => "enumerate",
        Message::EnumerateResult { .. } => "enumerate result",
        Message::CacheUpdate { .. } => "cache update",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Revision;

    const CHILD: Pid = Pid::from_bytes([1; 16]);
    const PARENT: Pid = Pid::from_bytes([2; 16]);
    const TARGET: Pid = Pid::from_bytes([3; 16]);
    const OTHER: Pid = Pid::from_bytes([4; 16]);
    const LINK: LinkId = LinkId::from_bytes([5; 16]);

    fn versions() -> VersionRange {
        VersionRange::try_new(ProtocolVersion::new(1), ProtocolVersion::new(3)).unwrap()
    }

    fn session() -> ChildSession {
        ChildSession::try_new(CHILD, PARENT, versions()).unwrap()
    }

    fn welcome() -> Message {
        Message::Welcome {
            version: ProtocolVersion::new(2),
            parent: PARENT,
            link: LINK,
        }
    }

    fn found(pid: Pid) -> Resolution {
        Resolution::Found {
            entry: ProcEntry {
                pid,
                tls_server_name: "target.test".into(),
                labels: crate::protocol::Labels::new(),
                locators: Vec::new(),
            },
            revision: Revision {
                authority: PARENT,
                value: 1,
            },
        }
    }

    fn establish(session: &mut ChildSession) {
        assert_eq!(
            session.receive(welcome()).unwrap(),
            ChildEvent::Established {
                version: ProtocolVersion::new(2),
                link: LINK,
            }
        );
    }

    #[test]
    fn handshake_binds_authenticated_parent_and_version() {
        let mut session = session();
        assert_eq!(
            session.hello().unwrap(),
            Message::Hello {
                versions: versions(),
                child: CHILD,
            }
        );
        establish(&mut session);
        assert_eq!(session.link(), Some(LINK));
        assert_eq!(session.version(), Some(ProtocolVersion::new(2)));
        assert!(matches!(
            session.hello(),
            Err(ChildSessionError::UnexpectedMessage { .. })
        ));
    }

    #[test]
    fn handshake_rejects_parent_and_version_mismatches() {
        let mut parent = session();
        assert_eq!(
            parent.receive(Message::Welcome {
                version: ProtocolVersion::new(2),
                parent: OTHER,
                link: LINK,
            }),
            Err(ChildSessionError::ParentMismatch {
                expected: PARENT,
                actual: OTHER,
            })
        );
        let mut version = session();
        assert_eq!(
            version.receive(Message::Welcome {
                version: ProtocolVersion::new(4),
                parent: PARENT,
                link: LINK,
            }),
            Err(ChildSessionError::UnsupportedVersion(ProtocolVersion::new(
                4
            )))
        );
    }

    #[test]
    fn rejection_is_terminal() {
        let mut session = session();
        assert_eq!(
            session
                .receive(Message::Reject {
                    code: RejectCode::Unavailable,
                })
                .unwrap(),
            ChildEvent::Rejected {
                code: RejectCode::Unavailable,
            }
        );
        assert_eq!(session.hello(), Err(ChildSessionError::Closed));
    }

    #[test]
    fn publication_messages_require_established_link() {
        let mut session = session();
        assert_eq!(
            session.snapshot_begin(SnapshotId::from_u64(1), PublicationSequence::from_u64(0)),
            Err(ChildSessionError::NotActive)
        );
        establish(&mut session);
        assert!(matches!(
            session.delta(
                PublicationSequence::from_u64(1),
                vec![ProcEntry {
                    pid: TARGET,
                    tls_server_name: "target.test".into(),
                    labels: crate::protocol::Labels::new(),
                    locators: Vec::new(),
                }],
                Vec::new(),
            ),
            Ok(Message::Delta { .. })
        ));
    }

    #[test]
    fn parent_publication_responses_become_events() {
        let mut session = session();
        establish(&mut session);
        assert_eq!(
            session
                .receive(Message::PublicationAck {
                    sequence: PublicationSequence::from_u64(7),
                })
                .unwrap(),
            ChildEvent::PublicationAck {
                sequence: PublicationSequence::from_u64(7),
            }
        );
        assert_eq!(
            session
                .receive(Message::ResnapshotRequired {
                    expected_sequence: PublicationSequence::from_u64(8),
                })
                .unwrap(),
            ChildEvent::ResnapshotRequired {
                expected_sequence: PublicationSequence::from_u64(8),
            }
        );
    }

    #[test]
    fn resolution_results_are_correlated_without_overwrite() {
        let mut session = session();
        establish(&mut session);
        let request = RequestId::from_u64(9);
        assert!(matches!(
            session
                .resolve(request, TARGET, ResolveConsistency::Refresh)
                .unwrap(),
            Message::Resolve { .. }
        ));
        assert_eq!(
            session.resolve(request, OTHER, ResolveConsistency::Cached),
            Err(ChildSessionError::DuplicateRequest { request })
        );
        assert_eq!(
            session.receive(Message::ResolveResult {
                request,
                result: found(OTHER),
            }),
            Err(ChildSessionError::ResolutionPidMismatch {
                expected: TARGET,
                actual: OTHER,
            })
        );
        assert_eq!(
            session
                .receive(Message::ResolveResult {
                    request,
                    result: found(TARGET),
                })
                .unwrap(),
            ChildEvent::Resolved {
                request,
                result: found(TARGET),
            }
        );
    }

    #[test]
    fn cache_update_and_invalid_direction_are_distinct() {
        let mut session = session();
        establish(&mut session);
        assert_eq!(
            session
                .receive(Message::CacheUpdate {
                    result: found(TARGET),
                })
                .unwrap(),
            ChildEvent::CacheUpdate {
                result: found(TARGET),
            }
        );
        assert!(matches!(
            session.receive(Message::SnapshotEnd {
                snapshot: SnapshotId::from_u64(1),
                chunks: 0,
            }),
            Err(ChildSessionError::UnexpectedMessage {
                state: "active",
                message: "snapshot end",
            })
        ));
    }

    #[test]
    fn reserved_pid_and_close_contracts() {
        assert_eq!(
            ChildSession::try_new(Pid::LINK_LOCAL, PARENT, versions()).unwrap_err(),
            ChildSessionError::ReservedPid
        );
        let mut session = session();
        session.close();
        assert_eq!(session.receive(welcome()), Err(ChildSessionError::Closed));
    }
}
