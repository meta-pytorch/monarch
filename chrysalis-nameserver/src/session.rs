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

use crate::ApplyEffects;
use crate::ApplyError;
use crate::Command;
use crate::EnumerationCursor;
use crate::EnumerationResult;
use crate::LinkId;
use crate::LinkResponse;
use crate::Message;
use crate::ProtocolVersion;
use crate::RejectCode;
use crate::RequestId;
use crate::Resolution;
use crate::ResolveConsistency;
use crate::VERSION_2;
use crate::VersionRange;

/// Resolution work requested by one child link.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolveRequest {
    /// The request identifier scoped to the link.
    pub request: RequestId,
    /// The process to resolve.
    pub pid: Pid,
    /// The requested cache freshness.
    pub consistency: ResolveConsistency,
}

/// Enumeration work requested by one child link.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EnumerateRequest {
    /// The request identifier scoped to the link.
    pub request: RequestId,
    /// The requested cache freshness.
    pub consistency: ResolveConsistency,
    /// The prior page position, if this continues an enumeration.
    pub cursor: Option<EnumerationCursor>,
    /// The requested page size, or zero for the server default.
    pub limit: u32,
}

/// Work produced by a parent-side nameserver session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParentAction {
    /// Commits a deterministic command through the nameserver replication layer.
    Commit(Command),
    /// Resolves a process through the local resolver.
    Resolve(ResolveRequest),
    /// Enumerates a page through the local nameserver.
    Enumerate(EnumerateRequest),
    /// Sends one protocol message while keeping the link open.
    Send(Message),
    /// Sends one final protocol message and then closes the link.
    SendAndClose(Message),
}

/// A parent-side session protocol failure or caller contract violation.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SessionError {
    /// A nameserver or authenticated peer used the reserved link-local PID.
    #[error("reserved link-local nameserver PID")]
    ReservedPid,

    /// The caller supplied another message before completing the pending commit.
    #[error("nameserver commit is pending")]
    CommitPending,

    /// The caller completed a commit when the session had none pending.
    #[error("no nameserver commit is pending")]
    NoCommitPending,

    /// The operation requires an admitted link.
    #[error("nameserver session is not active")]
    NotActive,

    /// The session is terminal.
    #[error("nameserver session is closed")]
    Closed,

    /// The child sent a message that is invalid in the current protocol state.
    #[error("unexpected {message} message while {state}")]
    UnexpectedMessage {
        /// The current session state.
        state: &'static str,
        /// The received message kind.
        message: &'static str,
    },

    /// The child reused an unresolved request identifier.
    #[error("request is already pending: {request:?}")]
    DuplicateRequest {
        /// The duplicate request identifier.
        request: RequestId,
    },

    /// The resolver completed an unknown request.
    #[error("unknown resolution request: {request:?}")]
    UnknownRequest {
        /// The unknown request identifier.
        request: RequestId,
    },

    /// The enumerator completed an unknown request.
    #[error("unknown enumeration request: {request:?}")]
    UnknownEnumerationRequest {
        /// The unknown request identifier.
        request: RequestId,
    },

    /// The resolver returned a result for another process.
    #[error("resolution PID mismatch: expected {expected:?}, got {actual:?}")]
    ResolutionPidMismatch {
        /// The process originally requested.
        expected: Pid,
        /// The process in the supplied result.
        actual: Pid,
    },

    /// The replication layer returned effects that do not match the pending command.
    #[error("unexpected committed effects: {0}")]
    UnexpectedEffects(&'static str),

    /// The deterministic nameserver rejected a command.
    #[error(transparent)]
    Apply(ApplyError),
}

/// Drives one authenticated child stream at a parent nameserver.
///
/// The driver performs no I/O and owns no nameserver state. A [`ParentAction::Commit`] must be
/// passed to the replication layer, then completed with [`Self::complete_commit`]. The caller must
/// not deliver another protocol message while that commit is pending. This separation prevents a
/// publication acknowledgment from preceding the corresponding committed mutation.
#[derive(Debug)]
pub struct ParentSession {
    parent: Pid,
    peer: Pid,
    link: LinkId,
    versions: VersionRange,
    state: State,
    pending_resolutions: BTreeMap<RequestId, Pid>,
    pending_enumerations: BTreeSet<RequestId>,
    close_requested: bool,
}

#[derive(Debug)]
enum State {
    AwaitingHello,
    Pending(PendingCommit),
    Active { version: ProtocolVersion },
    Closed,
}

#[derive(Debug)]
enum PendingCommit {
    Admit {
        version: ProtocolVersion,
    },
    Publication {
        version: ProtocolVersion,
        reply: PublicationReply,
    },
    Remove,
}

#[derive(Clone, Copy, Debug)]
enum PublicationReply {
    None,
    Snapshot,
    Delta,
}

impl ParentSession {
    /// Constructs a session for one authenticated peer and parent-allocated link ID.
    pub fn try_new(
        parent: Pid,
        peer: Pid,
        link: LinkId,
        versions: VersionRange,
    ) -> Result<Self, SessionError> {
        if parent.is_link_local() || peer.is_link_local() {
            return Err(SessionError::ReservedPid);
        }
        Ok(Self {
            parent,
            peer,
            link,
            versions,
            state: State::AwaitingHello,
            pending_resolutions: BTreeMap::new(),
            pending_enumerations: BTreeSet::new(),
            close_requested: false,
        })
    }

    /// Returns the authenticated child PID.
    pub const fn peer(&self) -> Pid {
        self.peer
    }

    /// Returns the one-shot link ID allocated for this session.
    pub const fn link(&self) -> LinkId {
        self.link
    }

    /// Returns the negotiated version after admission.
    pub const fn version(&self) -> Option<ProtocolVersion> {
        match &self.state {
            State::Pending(PendingCommit::Admit { version })
            | State::Pending(PendingCommit::Publication { version, .. })
            | State::Active { version } => Some(*version),
            State::AwaitingHello | State::Pending(PendingCommit::Remove) | State::Closed => None,
        }
    }

    /// Returns whether the session has reached its terminal state.
    pub const fn is_closed(&self) -> bool {
        matches!(self.state, State::Closed)
    }

    /// Handles one complete message received from the child.
    pub fn receive(&mut self, message: Message) -> Result<ParentAction, SessionError> {
        match &self.state {
            State::AwaitingHello => self.receive_hello(message),
            State::Pending(_) => Err(SessionError::CommitPending),
            State::Active { version } => self.receive_active(*version, message),
            State::Closed => Err(SessionError::Closed),
        }
    }

    /// Completes the pending deterministic commit.
    ///
    /// Passing an error never produces an acknowledgment. Admission failures become an
    /// `Unavailable` rejection. Publication failures leave the admitted link active so that the
    /// caller can close it and commit its removal.
    pub fn complete_commit(
        &mut self,
        result: Result<ApplyEffects, ApplyError>,
    ) -> Result<Option<ParentAction>, SessionError> {
        let pending = match std::mem::replace(&mut self.state, State::Closed) {
            State::Pending(pending) => pending,
            state => {
                self.state = state;
                return Err(SessionError::NoCommitPending);
            }
        };
        match pending {
            PendingCommit::Admit { version } => self.complete_admit(version, result),
            PendingCommit::Publication { version, reply } => {
                self.complete_publication(version, reply, result)
            }
            PendingCommit::Remove => self.complete_remove(result),
        }
    }

    /// Completes one resolution request and returns its response message.
    pub fn resolved(
        &mut self,
        request: RequestId,
        result: Resolution,
    ) -> Result<ParentAction, SessionError> {
        self.require_admitted()?;
        let expected = self
            .pending_resolutions
            .get(&request)
            .copied()
            .ok_or(SessionError::UnknownRequest { request })?;
        let actual = result.pid();
        if actual != expected {
            return Err(SessionError::ResolutionPidMismatch { expected, actual });
        }
        self.pending_resolutions.remove(&request);
        Ok(ParentAction::Send(Message::ResolveResult {
            request,
            result,
        }))
    }

    /// Completes one enumeration request and returns its response message.
    pub fn enumerated(
        &mut self,
        request: RequestId,
        result: EnumerationResult,
    ) -> Result<ParentAction, SessionError> {
        self.require_admitted()?;
        if !self.pending_enumerations.remove(&request) {
            return Err(SessionError::UnknownEnumerationRequest { request });
        }
        Ok(ParentAction::Send(Message::EnumerateResult {
            request,
            result,
        }))
    }

    /// Produces an unsolicited cache update for the child.
    pub fn push_cache(&self, result: Resolution) -> Result<ParentAction, SessionError> {
        self.require_admitted()?;
        Ok(ParentAction::Send(Message::CacheUpdate { result }))
    }

    /// Requests terminal link teardown.
    ///
    /// If a commit is in flight, the driver remembers the request and emits `RemoveLink` when that
    /// commit completes. This ensures cancellation cannot strand a committed admitted link.
    pub fn close(&mut self) -> Result<Option<ParentAction>, SessionError> {
        self.pending_resolutions.clear();
        self.pending_enumerations.clear();
        match self.state {
            State::AwaitingHello => {
                self.state = State::Closed;
                Ok(None)
            }
            State::Pending(PendingCommit::Admit { .. })
            | State::Pending(PendingCommit::Publication { .. }) => {
                self.close_requested = true;
                Ok(None)
            }
            State::Pending(PendingCommit::Remove) => Ok(None),
            State::Active { .. } => Ok(Some(self.begin_remove())),
            State::Closed => Ok(None),
        }
    }

    fn receive_hello(&mut self, message: Message) -> Result<ParentAction, SessionError> {
        let Message::Hello { versions, child } = message else {
            return Err(self.unexpected("awaiting hello", &message));
        };
        if child != self.peer {
            self.state = State::Closed;
            return Ok(ParentAction::SendAndClose(Message::Reject {
                code: RejectCode::IdentityMismatch,
            }));
        }
        let Some(version) = negotiate(self.versions, versions) else {
            self.state = State::Closed;
            return Ok(ParentAction::SendAndClose(Message::Reject {
                code: RejectCode::IncompatibleVersion,
            }));
        };
        self.state = State::Pending(PendingCommit::Admit { version });
        Ok(ParentAction::Commit(Command::AdmitLink {
            link: self.link,
            child,
        }))
    }

    fn receive_active(
        &mut self,
        version: ProtocolVersion,
        message: Message,
    ) -> Result<ParentAction, SessionError> {
        let (command, reply) = match message {
            Message::SnapshotBegin {
                snapshot,
                base_sequence,
            } => (
                Command::BeginSnapshot {
                    link: self.link,
                    snapshot,
                    base_sequence,
                },
                PublicationReply::None,
            ),
            Message::SnapshotChunk {
                snapshot,
                chunk,
                entries,
            } => (
                Command::AppendSnapshotChunk {
                    link: self.link,
                    snapshot,
                    chunk,
                    entries,
                },
                PublicationReply::None,
            ),
            Message::SnapshotEnd { snapshot, chunks } => (
                Command::CommitSnapshot {
                    link: self.link,
                    snapshot,
                    chunks,
                },
                PublicationReply::Snapshot,
            ),
            Message::Delta {
                sequence,
                upserts,
                removals,
            } => (
                Command::ApplyDelta {
                    link: self.link,
                    sequence,
                    upserts,
                    removals,
                },
                PublicationReply::Delta,
            ),
            Message::Resolve {
                request,
                pid,
                consistency,
            } => {
                if self.request_is_pending(request) {
                    return Err(SessionError::DuplicateRequest { request });
                }
                self.pending_resolutions.insert(request, pid);
                return Ok(ParentAction::Resolve(ResolveRequest {
                    request,
                    pid,
                    consistency,
                }));
            }
            Message::Enumerate {
                request,
                consistency,
                cursor,
                limit,
            } => {
                if self.request_is_pending(request) {
                    return Err(SessionError::DuplicateRequest { request });
                }
                self.pending_enumerations.insert(request);
                return Ok(ParentAction::Enumerate(EnumerateRequest {
                    request,
                    consistency,
                    cursor,
                    limit,
                }));
            }
            message => return Err(self.unexpected("active", &message)),
        };
        self.state = State::Pending(PendingCommit::Publication { version, reply });
        Ok(ParentAction::Commit(command))
    }

    fn complete_admit(
        &mut self,
        version: ProtocolVersion,
        result: Result<ApplyEffects, ApplyError>,
    ) -> Result<Option<ParentAction>, SessionError> {
        let effects = match result {
            Ok(effects) => effects,
            Err(_) if self.close_requested => {
                self.state = State::Closed;
                return Ok(None);
            }
            Err(error) => {
                self.state = State::Closed;
                return Ok(Some(ParentAction::SendAndClose(Message::Reject {
                    code: admission_reject_code(version, error),
                })));
            }
        };
        if effects != ApplyEffects::default() {
            self.state = State::Active { version };
            return Err(SessionError::UnexpectedEffects(
                "link admission produced observable effects",
            ));
        }
        if self.close_requested {
            return Ok(Some(self.begin_remove()));
        }
        self.state = State::Active { version };
        Ok(Some(ParentAction::Send(Message::Welcome {
            version,
            parent: self.parent,
            link: self.link,
        })))
    }

    fn complete_publication(
        &mut self,
        version: ProtocolVersion,
        reply: PublicationReply,
        result: Result<ApplyEffects, ApplyError>,
    ) -> Result<Option<ParentAction>, SessionError> {
        let effects = match result {
            Ok(effects) => effects,
            Err(error) => {
                self.state = State::Active { version };
                if self.close_requested {
                    return Ok(Some(self.begin_remove()));
                }
                return Err(SessionError::Apply(error));
            }
        };
        if let Some(change) = &effects.directory_change
            && change.link != self.link
        {
            self.state = State::Active { version };
            return Err(SessionError::UnexpectedEffects(
                "directory change belongs to another link",
            ));
        }
        if matches!(reply, PublicationReply::None) && effects.directory_change.is_some() {
            self.state = State::Active { version };
            return Err(SessionError::UnexpectedEffects(
                "staging command changed the visible directory",
            ));
        }
        let response = validate_publication_response(reply, effects.link_response);
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                self.state = State::Active { version };
                return Err(error);
            }
        };
        if self.close_requested {
            return Ok(Some(self.begin_remove()));
        }
        self.state = State::Active { version };
        Ok(response.map(|response| ParentAction::Send(response.into())))
    }

    fn begin_remove(&mut self) -> ParentAction {
        self.close_requested = true;
        self.state = State::Pending(PendingCommit::Remove);
        ParentAction::Commit(Command::RemoveLink { link: self.link })
    }

    fn complete_remove(
        &mut self,
        result: Result<ApplyEffects, ApplyError>,
    ) -> Result<Option<ParentAction>, SessionError> {
        let effects = match result {
            Ok(effects) => effects,
            Err(error) => {
                self.state = State::Pending(PendingCommit::Remove);
                return Err(SessionError::Apply(error));
            }
        };
        if effects.link_response.is_some() {
            self.state = State::Pending(PendingCommit::Remove);
            return Err(SessionError::UnexpectedEffects(
                "link removal produced a link response",
            ));
        }
        if let Some(change) = &effects.directory_change
            && change.link != self.link
        {
            self.state = State::Pending(PendingCommit::Remove);
            return Err(SessionError::UnexpectedEffects(
                "link removal changed another link",
            ));
        }
        self.pending_resolutions.clear();
        self.pending_enumerations.clear();
        self.state = State::Closed;
        Ok(None)
    }

    fn require_admitted(&self) -> Result<(), SessionError> {
        match self.state {
            State::Active { .. } | State::Pending(PendingCommit::Publication { .. }) => Ok(()),
            State::Closed | State::Pending(PendingCommit::Remove) => Err(SessionError::Closed),
            State::AwaitingHello | State::Pending(PendingCommit::Admit { .. }) => {
                Err(SessionError::NotActive)
            }
        }
    }

    fn request_is_pending(&self, request: RequestId) -> bool {
        self.pending_resolutions.contains_key(&request)
            || self.pending_enumerations.contains(&request)
    }

    fn unexpected(&self, state: &'static str, message: &Message) -> SessionError {
        SessionError::UnexpectedMessage {
            state,
            message: message_kind(message),
        }
    }
}

fn admission_reject_code(version: ProtocolVersion, error: ApplyError) -> RejectCode {
    match error {
        ApplyError::ChildAlreadyLinked { .. } if version >= VERSION_2 => RejectCode::AlreadyLinked,
        _ => RejectCode::Unavailable,
    }
}

fn negotiate(local: VersionRange, remote: VersionRange) -> Option<ProtocolVersion> {
    let min = local.min().get().max(remote.min().get());
    let max = local.max().get().min(remote.max().get());
    (min <= max).then(|| ProtocolVersion::new(max))
}

fn validate_publication_response(
    expected: PublicationReply,
    response: Option<LinkResponse>,
) -> Result<Option<LinkResponse>, SessionError> {
    match (expected, response) {
        (PublicationReply::None, None) => Ok(None),
        (PublicationReply::Snapshot, Some(response @ LinkResponse::PublicationAck { .. })) => {
            Ok(Some(response))
        }
        (
            PublicationReply::Delta,
            Some(
                response @ (LinkResponse::PublicationAck { .. }
                | LinkResponse::ResnapshotRequired { .. }),
            ),
        ) => Ok(Some(response)),
        (PublicationReply::None, Some(_)) => Err(SessionError::UnexpectedEffects(
            "staging command produced a link response",
        )),
        (PublicationReply::Snapshot | PublicationReply::Delta, None) => Err(
            SessionError::UnexpectedEffects("committing command omitted its link response"),
        ),
        (PublicationReply::Snapshot, Some(LinkResponse::ResnapshotRequired { .. })) => Err(
            SessionError::UnexpectedEffects("snapshot commit requested another snapshot"),
        ),
    }
}

impl From<LinkResponse> for Message {
    fn from(response: LinkResponse) -> Self {
        match response {
            LinkResponse::PublicationAck { sequence } => Self::PublicationAck { sequence },
            LinkResponse::ResnapshotRequired { expected_sequence } => {
                Self::ResnapshotRequired { expected_sequence }
            }
        }
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
    use chrysalis_core::Pid;

    use super::*;
    use crate::Nameserver;
    use crate::ProcEntry;
    use crate::PublicationSequence;
    use crate::Revision;
    use crate::SnapshotId;

    const PARENT: Pid = Pid::from_bytes([1; 16]);
    const CHILD: Pid = Pid::from_bytes([2; 16]);
    const TARGET: Pid = Pid::from_bytes([3; 16]);
    const OTHER: Pid = Pid::from_bytes([4; 16]);
    const LINK: LinkId = LinkId::from_bytes([5; 16]);

    fn versions(min: u16, max: u16) -> VersionRange {
        VersionRange::try_new(ProtocolVersion::new(min), ProtocolVersion::new(max)).unwrap()
    }

    fn new_session() -> ParentSession {
        ParentSession::try_new(PARENT, CHILD, LINK, versions(1, 3)).unwrap()
    }

    fn hello() -> Message {
        Message::Hello {
            versions: versions(2, 4),
            child: CHILD,
        }
    }

    fn entry(pid: Pid) -> ProcEntry {
        ProcEntry {
            pid,
            tls_server_name: "target.test".into(),
            labels: crate::protocol::Labels::new(),
            locators: Vec::new(),
        }
    }

    fn found(pid: Pid, value: u64) -> Resolution {
        Resolution::Found {
            entry: entry(pid),
            revision: Revision {
                authority: PARENT,
                value,
            },
        }
    }

    fn admit(session: &mut ParentSession, nameserver: &mut Nameserver) {
        let ParentAction::Commit(command) = session.receive(hello()).unwrap() else {
            panic!("hello must request admission");
        };
        assert_eq!(
            session.complete_commit(nameserver.apply(command)).unwrap(),
            Some(ParentAction::Send(Message::Welcome {
                version: ProtocolVersion::new(3),
                parent: PARENT,
                link: LINK,
            }))
        );
    }

    fn commit_message(
        session: &mut ParentSession,
        nameserver: &mut Nameserver,
        message: Message,
    ) -> Option<ParentAction> {
        let ParentAction::Commit(command) = session.receive(message).unwrap() else {
            panic!("message must produce a command");
        };
        session.complete_commit(nameserver.apply(command)).unwrap()
    }

    fn publish_baseline(session: &mut ParentSession, nameserver: &mut Nameserver) {
        assert_eq!(
            commit_message(
                session,
                nameserver,
                Message::SnapshotBegin {
                    snapshot: SnapshotId::from_u64(10),
                    base_sequence: PublicationSequence::from_u64(1),
                },
            ),
            None
        );
        assert_eq!(
            commit_message(
                session,
                nameserver,
                Message::SnapshotChunk {
                    snapshot: SnapshotId::from_u64(10),
                    chunk: 0,
                    entries: vec![entry(TARGET)],
                },
            ),
            None
        );
        assert_eq!(
            commit_message(
                session,
                nameserver,
                Message::SnapshotEnd {
                    snapshot: SnapshotId::from_u64(10),
                    chunks: 1,
                },
            ),
            Some(ParentAction::Send(Message::PublicationAck {
                sequence: PublicationSequence::from_u64(1),
            }))
        );
    }

    #[test]
    fn handshake_negotiates_highest_version_after_committed_admission() {
        let mut session = new_session();
        let mut nameserver = Nameserver::try_new(PARENT).unwrap();

        let action = session.receive(hello()).unwrap();
        assert_eq!(session.version(), Some(ProtocolVersion::new(3)));
        assert!(matches!(
            action,
            ParentAction::Commit(Command::AdmitLink { .. })
        ));
        assert_eq!(session.receive(hello()), Err(SessionError::CommitPending));

        let ParentAction::Commit(command) = action else {
            unreachable!();
        };
        let effects = nameserver.apply(command).unwrap();
        assert_eq!(
            session.complete_commit(Ok(effects)).unwrap(),
            Some(ParentAction::Send(Message::Welcome {
                version: ProtocolVersion::new(3),
                parent: PARENT,
                link: LINK,
            }))
        );
        assert_eq!(session.peer(), CHILD);
        assert_eq!(session.link(), LINK);
        assert!(!session.is_closed());
    }

    #[test]
    fn handshake_rejects_identity_and_version_mismatches() {
        let mut identity = new_session();
        assert_eq!(
            identity
                .receive(Message::Hello {
                    versions: versions(1, 1),
                    child: OTHER,
                })
                .unwrap(),
            ParentAction::SendAndClose(Message::Reject {
                code: RejectCode::IdentityMismatch,
            })
        );
        assert!(identity.is_closed());

        let mut version = new_session();
        assert_eq!(
            version
                .receive(Message::Hello {
                    versions: versions(4, 5),
                    child: CHILD,
                })
                .unwrap(),
            ParentAction::SendAndClose(Message::Reject {
                code: RejectCode::IncompatibleVersion,
            })
        );
        assert!(version.is_closed());
    }

    #[test]
    fn duplicate_child_is_rejected_without_welcome() {
        let mut nameserver = Nameserver::try_new(PARENT).unwrap();
        nameserver
            .apply(Command::AdmitLink {
                link: LinkId::from_bytes([9; 16]),
                child: CHILD,
            })
            .unwrap();
        let mut session = new_session();
        let ParentAction::Commit(command) = session.receive(hello()).unwrap() else {
            unreachable!();
        };
        assert_eq!(
            session.complete_commit(nameserver.apply(command)).unwrap(),
            Some(ParentAction::SendAndClose(Message::Reject {
                code: RejectCode::AlreadyLinked,
            }))
        );
        assert!(session.is_closed());
    }

    #[test]
    fn version_one_maps_duplicate_child_to_unavailable() {
        let mut nameserver = Nameserver::try_new(PARENT).unwrap();
        nameserver
            .apply(Command::AdmitLink {
                link: LinkId::from_bytes([9; 16]),
                child: CHILD,
            })
            .unwrap();
        let mut session = ParentSession::try_new(PARENT, CHILD, LINK, versions(1, 1)).unwrap();
        let ParentAction::Commit(command) = session
            .receive(Message::Hello {
                versions: versions(1, 1),
                child: CHILD,
            })
            .unwrap()
        else {
            unreachable!();
        };
        assert_eq!(
            session.complete_commit(nameserver.apply(command)).unwrap(),
            Some(ParentAction::SendAndClose(Message::Reject {
                code: RejectCode::Unavailable,
            }))
        );
        assert!(session.is_closed());
    }

    #[test]
    fn publication_messages_commit_before_acknowledgment() {
        let mut session = new_session();
        let mut nameserver = Nameserver::try_new(PARENT).unwrap();
        admit(&mut session, &mut nameserver);
        publish_baseline(&mut session, &mut nameserver);

        assert_eq!(nameserver.get(TARGET), Some(&entry(TARGET)));
        assert_eq!(nameserver.owner(TARGET), Some(LINK));
        assert_eq!(
            commit_message(
                &mut session,
                &mut nameserver,
                Message::Delta {
                    sequence: PublicationSequence::from_u64(2),
                    upserts: vec![entry(OTHER)],
                    removals: vec![TARGET],
                },
            ),
            Some(ParentAction::Send(Message::PublicationAck {
                sequence: PublicationSequence::from_u64(2),
            }))
        );
        assert_eq!(nameserver.get(TARGET), None);
        assert_eq!(nameserver.get(OTHER), Some(&entry(OTHER)));
    }

    #[test]
    fn delta_gap_becomes_resnapshot_request() {
        let mut session = new_session();
        let mut nameserver = Nameserver::try_new(PARENT).unwrap();
        admit(&mut session, &mut nameserver);
        publish_baseline(&mut session, &mut nameserver);

        assert_eq!(
            commit_message(
                &mut session,
                &mut nameserver,
                Message::Delta {
                    sequence: PublicationSequence::from_u64(3),
                    upserts: vec![entry(OTHER)],
                    removals: Vec::new(),
                },
            ),
            Some(ParentAction::Send(Message::ResnapshotRequired {
                expected_sequence: PublicationSequence::from_u64(2),
            }))
        );
        assert_eq!(nameserver.get(OTHER), None);
    }

    #[test]
    fn close_commits_link_wide_revocation() {
        let mut session = new_session();
        let mut nameserver = Nameserver::try_new(PARENT).unwrap();
        admit(&mut session, &mut nameserver);
        publish_baseline(&mut session, &mut nameserver);

        let Some(ParentAction::Commit(command)) = session.close().unwrap() else {
            panic!("active close must remove the link");
        };
        let effects = nameserver.apply(command).unwrap();
        assert_eq!(
            effects
                .directory_change
                .as_ref()
                .map(|change| change.removals.as_slice()),
            Some([TARGET].as_slice())
        );
        assert_eq!(session.complete_commit(Ok(effects)).unwrap(), None);
        assert!(session.is_closed());
        assert_eq!(nameserver.get(TARGET), None);
        assert_eq!(session.close().unwrap(), None);
    }

    #[test]
    fn close_during_admission_removes_committed_link_without_welcome() {
        let mut session = new_session();
        let mut nameserver = Nameserver::try_new(PARENT).unwrap();
        let ParentAction::Commit(admit) = session.receive(hello()).unwrap() else {
            unreachable!();
        };
        assert_eq!(session.close().unwrap(), None);
        let Some(ParentAction::Commit(remove)) =
            session.complete_commit(nameserver.apply(admit)).unwrap()
        else {
            panic!("committed admission must be removed");
        };
        assert_eq!(remove, Command::RemoveLink { link: LINK });
        assert_eq!(
            session.complete_commit(nameserver.apply(remove)).unwrap(),
            None
        );
        assert!(session.is_closed());
    }

    #[test]
    fn close_during_publication_suppresses_ack_and_removes_link() {
        let mut session = new_session();
        let mut nameserver = Nameserver::try_new(PARENT).unwrap();
        admit(&mut session, &mut nameserver);
        publish_baseline(&mut session, &mut nameserver);

        let ParentAction::Commit(delta) = session
            .receive(Message::Delta {
                sequence: PublicationSequence::from_u64(2),
                upserts: vec![entry(OTHER)],
                removals: Vec::new(),
            })
            .unwrap()
        else {
            unreachable!();
        };
        assert_eq!(session.close().unwrap(), None);
        let Some(ParentAction::Commit(remove)) =
            session.complete_commit(nameserver.apply(delta)).unwrap()
        else {
            panic!("completed publication must proceed to removal");
        };
        assert_eq!(
            session.complete_commit(nameserver.apply(remove)).unwrap(),
            None
        );
        assert!(session.is_closed());
        assert!(nameserver.is_empty());
    }

    #[test]
    fn resolution_requests_are_correlated_and_pid_checked() {
        let mut session = new_session();
        let mut nameserver = Nameserver::try_new(PARENT).unwrap();
        admit(&mut session, &mut nameserver);
        let request = RequestId::from_u64(42);
        let resolve = Message::Resolve {
            request,
            pid: TARGET,
            consistency: ResolveConsistency::Refresh,
        };
        assert_eq!(
            session.receive(resolve.clone()).unwrap(),
            ParentAction::Resolve(ResolveRequest {
                request,
                pid: TARGET,
                consistency: ResolveConsistency::Refresh,
            })
        );
        assert_eq!(
            session.receive(Message::Resolve {
                request,
                pid: OTHER,
                consistency: ResolveConsistency::Cached,
            }),
            Err(SessionError::DuplicateRequest { request })
        );
        assert_eq!(
            session.resolved(request, found(OTHER, 1)),
            Err(SessionError::ResolutionPidMismatch {
                expected: TARGET,
                actual: OTHER,
            })
        );
        assert_eq!(
            session.resolved(request, found(TARGET, 2)).unwrap(),
            ParentAction::Send(Message::ResolveResult {
                request,
                result: found(TARGET, 2),
            })
        );
        assert_eq!(
            session.resolved(request, found(TARGET, 3)),
            Err(SessionError::UnknownRequest { request })
        );
    }

    #[test]
    fn cache_updates_require_an_admitted_link() {
        let mut session = new_session();
        assert_eq!(
            session.push_cache(found(TARGET, 1)),
            Err(SessionError::NotActive)
        );
        let mut nameserver = Nameserver::try_new(PARENT).unwrap();
        admit(&mut session, &mut nameserver);
        assert_eq!(
            session.push_cache(found(TARGET, 1)).unwrap(),
            ParentAction::Send(Message::CacheUpdate {
                result: found(TARGET, 1),
            })
        );
    }

    #[test]
    fn invalid_direction_and_effects_are_rejected() {
        let mut session = new_session();
        assert!(matches!(
            session.receive(Message::PublicationAck {
                sequence: PublicationSequence::from_u64(1),
            }),
            Err(SessionError::UnexpectedMessage {
                state: "awaiting hello",
                message: "publication acknowledgment",
            })
        ));

        let mut nameserver = Nameserver::try_new(PARENT).unwrap();
        admit(&mut session, &mut nameserver);
        let ParentAction::Commit(_) = session
            .receive(Message::SnapshotBegin {
                snapshot: SnapshotId::from_u64(1),
                base_sequence: PublicationSequence::from_u64(1),
            })
            .unwrap()
        else {
            unreachable!();
        };
        assert_eq!(
            session.complete_commit(Ok(ApplyEffects {
                directory_change: None,
                link_response: Some(LinkResponse::PublicationAck {
                    sequence: PublicationSequence::from_u64(1),
                }),
            })),
            Err(SessionError::UnexpectedEffects(
                "staging command produced a link response"
            ))
        );
        assert!(matches!(
            session.close().unwrap(),
            Some(ParentAction::Commit(Command::RemoveLink { link: LINK }))
        ));
    }

    #[test]
    fn staging_command_rejects_visible_directory_effects() {
        let mut session = new_session();
        let mut nameserver = Nameserver::try_new(PARENT).unwrap();
        admit(&mut session, &mut nameserver);
        let ParentAction::Commit(_) = session
            .receive(Message::SnapshotBegin {
                snapshot: SnapshotId::from_u64(1),
                base_sequence: PublicationSequence::from_u64(1),
            })
            .unwrap()
        else {
            unreachable!();
        };
        assert_eq!(
            session.complete_commit(Ok(ApplyEffects {
                directory_change: Some(crate::DirectoryChange {
                    link: LINK,
                    revision: Revision {
                        authority: PARENT,
                        value: 1,
                    },
                    upserts: vec![entry(TARGET)],
                    removals: Vec::new(),
                }),
                link_response: None,
            })),
            Err(SessionError::UnexpectedEffects(
                "staging command changed the visible directory"
            ))
        );
    }

    #[test]
    fn rejected_publication_never_produces_an_acknowledgment() {
        let mut session = new_session();
        let mut nameserver = Nameserver::try_new(PARENT).unwrap();
        admit(&mut session, &mut nameserver);
        let ParentAction::Commit(command) = session
            .receive(Message::SnapshotChunk {
                snapshot: SnapshotId::from_u64(1),
                chunk: 0,
                entries: vec![entry(TARGET)],
            })
            .unwrap()
        else {
            unreachable!();
        };
        let error = nameserver.apply(command).unwrap_err();
        assert_eq!(
            session.complete_commit(Err(error.clone())),
            Err(SessionError::Apply(error))
        );
        assert!(matches!(
            session.close().unwrap(),
            Some(ParentAction::Commit(Command::RemoveLink { link: LINK }))
        ));
    }

    #[test]
    fn link_removal_validates_committed_effect_ownership() {
        let mut session = new_session();
        let mut nameserver = Nameserver::try_new(PARENT).unwrap();
        admit(&mut session, &mut nameserver);
        assert!(matches!(
            session.close().unwrap(),
            Some(ParentAction::Commit(Command::RemoveLink { link: LINK }))
        ));
        assert_eq!(
            session.complete_commit(Ok(ApplyEffects {
                directory_change: Some(crate::DirectoryChange {
                    link: LinkId::from_bytes([9; 16]),
                    revision: Revision {
                        authority: PARENT,
                        value: 1,
                    },
                    upserts: Vec::new(),
                    removals: vec![TARGET],
                }),
                link_response: None,
            })),
            Err(SessionError::UnexpectedEffects(
                "link removal changed another link"
            ))
        );
        assert!(!session.is_closed());
        assert_eq!(session.close().unwrap(), None);
        assert_eq!(
            session
                .complete_commit(nameserver.apply(Command::RemoveLink { link: LINK }))
                .unwrap(),
            None
        );
        assert!(session.is_closed());
    }

    #[test]
    fn constructor_and_commit_completion_enforce_session_contract() {
        assert_eq!(
            ParentSession::try_new(Pid::LINK_LOCAL, CHILD, LINK, versions(1, 1)).unwrap_err(),
            SessionError::ReservedPid
        );
        assert_eq!(
            ParentSession::try_new(PARENT, Pid::LINK_LOCAL, LINK, versions(1, 1)).unwrap_err(),
            SessionError::ReservedPid
        );
        let mut session = new_session();
        assert_eq!(
            session.complete_commit(Ok(ApplyEffects::default())),
            Err(SessionError::NoCommitPending)
        );
        session.close().unwrap();
        assert_eq!(session.receive(hello()), Err(SessionError::Closed));
    }
}
