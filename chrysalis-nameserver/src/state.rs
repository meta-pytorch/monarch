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

use crate::LinkId;
use crate::ProcEntry;
use crate::PublicationSequence;
use crate::Resolution;
use crate::Revision;
use crate::SnapshotId;

/// A deterministic mutation that may be committed through replication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Command {
    /// Admits one authenticated child link.
    AdmitLink {
        /// The parent-allocated one-shot link ID.
        link: LinkId,
        /// The authenticated child PID.
        child: Pid,
    },
    /// Starts staging a complete publication snapshot.
    BeginSnapshot {
        /// The link that owns the publication.
        link: LinkId,
        /// The snapshot incarnation.
        snapshot: SnapshotId,
        /// The sequence represented by the complete snapshot.
        base_sequence: PublicationSequence,
    },
    /// Appends one ordered snapshot chunk.
    AppendSnapshotChunk {
        /// The link that owns the publication.
        link: LinkId,
        /// The snapshot incarnation.
        snapshot: SnapshotId,
        /// The zero-based chunk index.
        chunk: u32,
        /// Complete entries in this chunk.
        entries: Vec<ProcEntry>,
    },
    /// Atomically activates a staged snapshot.
    CommitSnapshot {
        /// The link that owns the publication.
        link: LinkId,
        /// The snapshot incarnation.
        snapshot: SnapshotId,
        /// The expected number of staged chunks.
        chunks: u32,
    },
    /// Applies one contiguous incremental publication update.
    ApplyDelta {
        /// The link that owns the publication.
        link: LinkId,
        /// The next publication sequence.
        sequence: PublicationSequence,
        /// Complete entries to add or replace.
        upserts: Vec<ProcEntry>,
        /// Entries to remove.
        removals: Vec<Pid>,
    },
    /// Removes a link and every entry that it owns.
    RemoveLink {
        /// The terminal link ID.
        link: LinkId,
    },
}

/// A response that the session driver sends after a committed command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LinkResponse {
    /// Cumulatively acknowledges publication state.
    PublicationAck {
        /// The acknowledged sequence.
        sequence: PublicationSequence,
    },
    /// Requests a complete snapshot after a sequence gap.
    ResnapshotRequired {
        /// The next expected sequence, or zero when no baseline exists.
        expected_sequence: PublicationSequence,
    },
}

/// One atomic change to the visible directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryChange {
    /// The link that owns every entry affected by this change.
    pub link: LinkId,
    /// The revision assigned to this change.
    pub revision: Revision,
    /// Complete entries added or replaced by the change.
    pub upserts: Vec<ProcEntry>,
    /// Process IDs removed by the change.
    pub removals: Vec<Pid>,
}

/// Observable effects of one committed command.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ApplyEffects {
    /// The visible directory mutation, if any.
    pub directory_change: Option<DirectoryChange>,
    /// The response to send on the affected link, if any.
    pub link_response: Option<LinkResponse>,
}

/// A rejected nameserver state transition.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ApplyError {
    /// PID zero cannot identify a nameserver incarnation or publication.
    #[error("reserved link-local nameserver PID")]
    ReservedPid,

    /// The link ID is already owned by another child.
    #[error("link already exists: {link:?}")]
    LinkAlreadyExists { link: LinkId },

    /// The child already owns another active link.
    #[error("child already has an active link: {child:?}")]
    ChildAlreadyLinked { child: Pid },

    /// The command refers to an unknown link.
    #[error("unknown link: {link:?}")]
    UnknownLink { link: LinkId },

    /// A replay reused an identifier with different contents.
    #[error("conflicting command replay")]
    ConflictingReplay,

    /// The command refers to a snapshot other than the one being staged.
    #[error("snapshot is not being staged: {snapshot:?}")]
    WrongSnapshot { snapshot: SnapshotId },

    /// An incremental update arrived before the staged snapshot committed.
    #[error("snapshot is in progress")]
    SnapshotInProgress,

    /// A snapshot attempted to move publication sequence backward.
    #[error("stale publication sequence: current {current}, proposed {proposed}")]
    StalePublicationSequence { current: u64, proposed: u64 },

    /// A snapshot chunk arrived out of order.
    #[error("unexpected snapshot chunk: expected {expected}, got {actual}")]
    UnexpectedSnapshotChunk { expected: u32, actual: u32 },

    /// Snapshot end did not match the number of staged chunks.
    #[error("incomplete snapshot: expected {expected} chunks, got {actual}")]
    IncompleteSnapshot { expected: u32, actual: u32 },

    /// One publication contains the same PID more than once.
    #[error("duplicate PID in publication: {pid:?}")]
    DuplicatePid { pid: Pid },

    /// Another link already owns the PID.
    #[error("PID is owned by another link: {pid:?}")]
    PidOwnedByAnotherLink { pid: Pid },

    /// A child attempted to publish this nameserver's own authority PID.
    #[error("child publication contains nameserver authority PID")]
    AuthorityPid,

    /// A link attempted to remove a PID that it does not own.
    #[error("PID is not owned by link: {pid:?}")]
    PidNotOwnedByLink { pid: Pid },

    /// A delta attempted to upsert and remove the same PID.
    #[error("PID is both upserted and removed: {pid:?}")]
    PidBothUpsertedAndRemoved { pid: Pid },

    /// A monotonic counter cannot advance further.
    #[error("{0} exhausted")]
    CounterExhausted(&'static str),
}

/// The deterministic authoritative state of one nameserver incarnation.
#[derive(Debug)]
pub struct Nameserver {
    authority: Pid,
    revision: u64,
    links: BTreeMap<LinkId, LinkState>,
    children: BTreeMap<Pid, LinkId>,
    directory: BTreeMap<Pid, DirectoryRecord>,
}

impl Nameserver {
    /// Constructs an empty nameserver with a globally routable authority PID.
    pub fn try_new(authority: Pid) -> Result<Self, ApplyError> {
        if authority.is_link_local() {
            return Err(ApplyError::ReservedPid);
        }
        Ok(Self {
            authority,
            revision: 0,
            links: BTreeMap::new(),
            children: BTreeMap::new(),
            directory: BTreeMap::new(),
        })
    }

    /// Returns this nameserver's current directory revision.
    pub const fn revision(&self) -> Revision {
        Revision {
            authority: self.authority,
            value: self.revision,
        }
    }

    /// Returns this nameserver's globally routable authority PID.
    pub const fn authority(&self) -> Pid {
        self.authority
    }

    /// Returns a deterministic complete snapshot of the visible directory.
    pub fn snapshot(&self) -> Vec<ProcEntry> {
        self.directory
            .values()
            .map(|record| record.entry.clone())
            .collect()
    }

    /// Returns one visible process entry.
    pub fn get(&self, pid: Pid) -> Option<&ProcEntry> {
        self.directory.get(&pid).map(|record| &record.entry)
    }

    /// Returns the link that owns one visible process entry.
    pub fn owner(&self, pid: Pid) -> Option<LinkId> {
        self.directory.get(&pid).map(|record| record.owner)
    }

    /// Iterates over directly admitted child links in PID order.
    pub fn child_links(&self) -> impl Iterator<Item = (Pid, LinkId)> + '_ {
        self.children.iter().map(|(child, link)| (*child, *link))
    }

    /// Resolves one process from this authoritative directory.
    pub fn resolve(&self, pid: Pid) -> Option<Resolution> {
        self.get(pid).cloned().map(|entry| Resolution::Found {
            entry,
            revision: self.revision(),
        })
    }

    /// Returns the number of visible process entries.
    pub fn len(&self) -> usize {
        self.directory.len()
    }

    /// Returns whether the visible directory is empty.
    pub fn is_empty(&self) -> bool {
        self.directory.is_empty()
    }

    /// Applies one committed deterministic command.
    pub fn apply(&mut self, command: Command) -> Result<ApplyEffects, ApplyError> {
        match command {
            Command::AdmitLink { link, child } => self.admit_link(link, child),
            Command::BeginSnapshot {
                link,
                snapshot,
                base_sequence,
            } => self.begin_snapshot(link, snapshot, base_sequence),
            Command::AppendSnapshotChunk {
                link,
                snapshot,
                chunk,
                entries,
            } => self.append_snapshot_chunk(link, snapshot, chunk, entries),
            Command::CommitSnapshot {
                link,
                snapshot,
                chunks,
            } => self.commit_snapshot(link, snapshot, chunks),
            Command::ApplyDelta {
                link,
                sequence,
                upserts,
                removals,
            } => self.apply_delta(link, sequence, upserts, removals),
            Command::RemoveLink { link } => self.remove_link(link),
        }
    }

    fn admit_link(&mut self, link: LinkId, child: Pid) -> Result<ApplyEffects, ApplyError> {
        if child.is_link_local() {
            return Err(ApplyError::ReservedPid);
        }
        if let Some(existing) = self.links.get(&link) {
            if existing.child == child {
                return Ok(ApplyEffects::default());
            }
            return Err(ApplyError::LinkAlreadyExists { link });
        }
        if self.children.contains_key(&child) {
            return Err(ApplyError::ChildAlreadyLinked { child });
        }
        self.links.insert(link, LinkState::new(child));
        self.children.insert(child, link);
        Ok(ApplyEffects::default())
    }

    fn begin_snapshot(
        &mut self,
        link: LinkId,
        snapshot: SnapshotId,
        base_sequence: PublicationSequence,
    ) -> Result<ApplyEffects, ApplyError> {
        let state = self.link_mut(link)?;
        if let Some(staging) = &state.staging
            && staging.snapshot == snapshot
        {
            if staging.base_sequence == base_sequence {
                return Ok(ApplyEffects::default());
            }
            return Err(ApplyError::ConflictingReplay);
        }
        if let Some(active) = &state.active {
            if active.sequence == base_sequence
                && matches!(
                    &active.last_commit,
                    CommitRecord::Snapshot {
                        snapshot: committed,
                        ..
                    } if *committed == snapshot
                )
            {
                return Ok(ApplyEffects::default());
            }
            if base_sequence.as_u64() < active.sequence.as_u64() {
                return Err(ApplyError::StalePublicationSequence {
                    current: active.sequence.as_u64(),
                    proposed: base_sequence.as_u64(),
                });
            }
        }
        state.staging = Some(StagedSnapshot::new(snapshot, base_sequence));
        Ok(ApplyEffects::default())
    }

    fn append_snapshot_chunk(
        &mut self,
        link: LinkId,
        snapshot: SnapshotId,
        chunk: u32,
        entries: Vec<ProcEntry>,
    ) -> Result<ApplyEffects, ApplyError> {
        validate_entries(&entries)?;
        self.validate_child_entries(&entries)?;
        let state = self.link_mut(link)?;
        if state.staging.is_none()
            && let Some(active) = &state.active
            && let CommitRecord::Snapshot {
                snapshot: committed,
                chunks,
            } = &active.last_commit
            && *committed == snapshot
        {
            return match chunks.get(chunk as usize) {
                Some(committed) if *committed == entries => Ok(ApplyEffects::default()),
                Some(_) => Err(ApplyError::ConflictingReplay),
                None => Err(ApplyError::UnexpectedSnapshotChunk {
                    expected: chunks.len() as u32,
                    actual: chunk,
                }),
            };
        }
        let staging = state
            .staging
            .as_mut()
            .filter(|staging| staging.snapshot == snapshot)
            .ok_or(ApplyError::WrongSnapshot { snapshot })?;
        let expected = u32::try_from(staging.chunks.len())
            .map_err(|_| ApplyError::CounterExhausted("snapshot chunk index"))?;
        if chunk < expected {
            if staging.chunks[chunk as usize] == entries {
                return Ok(ApplyEffects::default());
            }
            return Err(ApplyError::ConflictingReplay);
        }
        if chunk > expected {
            return Err(ApplyError::UnexpectedSnapshotChunk {
                expected,
                actual: chunk,
            });
        }
        if let Some(entry) = entries
            .iter()
            .find(|entry| staging.pids.contains(&entry.pid))
        {
            return Err(ApplyError::DuplicatePid { pid: entry.pid });
        }
        staging.pids.extend(entries.iter().map(|entry| entry.pid));
        staging.chunks.push(entries);
        Ok(ApplyEffects::default())
    }

    fn commit_snapshot(
        &mut self,
        link: LinkId,
        snapshot: SnapshotId,
        chunks: u32,
    ) -> Result<ApplyEffects, ApplyError> {
        {
            let state = self.link(link)?;
            let Some(staging) = &state.staging else {
                if let Some(active) = &state.active
                    && let CommitRecord::Snapshot {
                        snapshot: committed,
                        chunks: committed_chunks,
                    } = &active.last_commit
                    && *committed == snapshot
                    && committed_chunks.len() == chunks as usize
                {
                    return Ok(ack(active.sequence));
                }
                return Err(ApplyError::WrongSnapshot { snapshot });
            };
            if staging.snapshot != snapshot {
                return Err(ApplyError::WrongSnapshot { snapshot });
            }
            let actual = u32::try_from(staging.chunks.len())
                .map_err(|_| ApplyError::CounterExhausted("snapshot chunk count"))?;
            if chunks != actual {
                return Err(ApplyError::IncompleteSnapshot {
                    expected: chunks,
                    actual,
                });
            }
            self.validate_ownership(link, staging.chunks.iter().flatten().map(|entry| entry.pid))?;
        }
        let staging = self
            .link_mut(link)?
            .staging
            .take()
            .expect("validated snapshot staging should remain present");
        let entries = staging
            .chunks
            .iter()
            .flatten()
            .cloned()
            .map(|entry| (entry.pid, entry))
            .collect::<BTreeMap<_, _>>();
        let directory_change = self.replace_link_entries(link, entries)?;
        let state = self.link_mut(link)?;
        state.active = Some(ActivePublication {
            sequence: staging.base_sequence,
            pids: staging.pids,
            last_commit: CommitRecord::Snapshot {
                snapshot,
                chunks: staging.chunks,
            },
        });
        Ok(ApplyEffects {
            directory_change,
            link_response: Some(LinkResponse::PublicationAck {
                sequence: staging.base_sequence,
            }),
        })
    }

    fn apply_delta(
        &mut self,
        link: LinkId,
        sequence: PublicationSequence,
        upserts: Vec<ProcEntry>,
        removals: Vec<Pid>,
    ) -> Result<ApplyEffects, ApplyError> {
        validate_entries(&upserts)?;
        self.validate_child_entries(&upserts)?;
        let state = self.link(link)?;
        if state.staging.is_some() {
            return Err(ApplyError::SnapshotInProgress);
        }
        let active = match &state.active {
            Some(active) => active,
            None => return Ok(resnapshot(PublicationSequence::from_u64(0))),
        };
        let current = active.sequence.as_u64();
        let incoming = sequence.as_u64();
        if incoming < current {
            return Ok(ack(active.sequence));
        }
        if incoming == current {
            return match &active.last_commit {
                CommitRecord::Delta {
                    upserts: committed_upserts,
                    removals: committed_removals,
                } if *committed_upserts == upserts && *committed_removals == removals => {
                    Ok(ack(active.sequence))
                }
                _ => Err(ApplyError::ConflictingReplay),
            };
        }
        let expected = current
            .checked_add(1)
            .ok_or(ApplyError::CounterExhausted("publication sequence"))?;
        if incoming != expected {
            return Ok(resnapshot(PublicationSequence::from_u64(expected)));
        }

        let upsert_map = upserts
            .iter()
            .cloned()
            .map(|entry| (entry.pid, entry))
            .collect::<BTreeMap<_, _>>();
        let removal_set = removals.iter().copied().collect::<BTreeSet<_>>();
        if removal_set.len() != removals.len() {
            let duplicate = first_duplicate(removals.iter().copied()).expect("length differs");
            return Err(ApplyError::DuplicatePid { pid: duplicate });
        }
        if let Some(pid) = upsert_map
            .keys()
            .find(|pid| removal_set.contains(pid))
            .copied()
        {
            return Err(ApplyError::PidBothUpsertedAndRemoved { pid });
        }
        self.validate_ownership(link, upsert_map.keys().copied())?;
        for pid in &removal_set {
            if !active.pids.contains(pid) {
                return Err(ApplyError::PidNotOwnedByLink { pid: *pid });
            }
        }

        let directory_change = self.apply_delta_entries(link, &upsert_map, &removal_set)?;
        let active = self
            .link_mut(link)?
            .active
            .take()
            .expect("validated active publication should remain present");
        let mut pids = active.pids;
        pids.extend(upsert_map.keys().copied());
        pids.retain(|pid| !removal_set.contains(pid));
        self.link_mut(link)?.active = Some(ActivePublication {
            sequence,
            pids,
            last_commit: CommitRecord::Delta { upserts, removals },
        });
        Ok(ApplyEffects {
            directory_change,
            link_response: Some(LinkResponse::PublicationAck { sequence }),
        })
    }

    fn remove_link(&mut self, link: LinkId) -> Result<ApplyEffects, ApplyError> {
        let Some(state) = self.links.get(&link) else {
            return Ok(ApplyEffects::default());
        };
        let child = state.child;
        let removals = state
            .active
            .as_ref()
            .map(|active| active.pids.clone())
            .unwrap_or_default();
        if removals.is_empty() {
            self.links.remove(&link);
            assert_eq!(self.children.remove(&child), Some(link));
            return Ok(ApplyEffects::default());
        }
        let revision = self.next_revision()?;
        self.links.remove(&link);
        assert_eq!(self.children.remove(&child), Some(link));
        for pid in &removals {
            let removed = self.directory.remove(pid).expect("active PID must exist");
            assert_eq!(removed.owner, link, "active PID must be owned by its link");
        }
        self.revision = revision.value;
        Ok(ApplyEffects {
            directory_change: Some(DirectoryChange {
                link,
                revision,
                upserts: Vec::new(),
                removals: removals.into_iter().collect(),
            }),
            link_response: None,
        })
    }

    fn replace_link_entries(
        &mut self,
        link: LinkId,
        entries: BTreeMap<Pid, ProcEntry>,
    ) -> Result<Option<DirectoryChange>, ApplyError> {
        let old_pids = self
            .link(link)?
            .active
            .as_ref()
            .map(|active| active.pids.clone())
            .unwrap_or_default();
        let new_pids = entries.keys().copied().collect::<BTreeSet<_>>();
        let removals = old_pids.difference(&new_pids).copied().collect::<Vec<_>>();
        let upserts = entries
            .into_values()
            .filter(|entry| {
                self.directory
                    .get(&entry.pid)
                    .is_none_or(|record| record.entry != *entry)
            })
            .collect::<Vec<_>>();
        if removals.is_empty() && upserts.is_empty() {
            return Ok(None);
        }
        let revision = self.next_revision()?;
        for pid in &removals {
            self.directory.remove(pid);
        }
        for entry in &upserts {
            self.directory.insert(
                entry.pid,
                DirectoryRecord {
                    owner: link,
                    entry: entry.clone(),
                },
            );
        }
        self.revision = revision.value;
        Ok(Some(DirectoryChange {
            link,
            revision,
            upserts,
            removals,
        }))
    }

    fn apply_delta_entries(
        &mut self,
        link: LinkId,
        upserts: &BTreeMap<Pid, ProcEntry>,
        removals: &BTreeSet<Pid>,
    ) -> Result<Option<DirectoryChange>, ApplyError> {
        let changed_upserts = upserts
            .values()
            .filter(|entry| {
                self.directory
                    .get(&entry.pid)
                    .is_none_or(|record| record.entry != **entry)
            })
            .cloned()
            .collect::<Vec<_>>();
        if changed_upserts.is_empty() && removals.is_empty() {
            return Ok(None);
        }
        let revision = self.next_revision()?;
        for pid in removals {
            self.directory.remove(pid);
        }
        for entry in &changed_upserts {
            self.directory.insert(
                entry.pid,
                DirectoryRecord {
                    owner: link,
                    entry: entry.clone(),
                },
            );
        }
        self.revision = revision.value;
        Ok(Some(DirectoryChange {
            link,
            revision,
            upserts: changed_upserts,
            removals: removals.iter().copied().collect(),
        }))
    }

    fn validate_ownership(
        &self,
        link: LinkId,
        pids: impl Iterator<Item = Pid>,
    ) -> Result<(), ApplyError> {
        for pid in pids {
            if let Some(record) = self.directory.get(&pid)
                && record.owner != link
            {
                return Err(ApplyError::PidOwnedByAnotherLink { pid });
            }
        }
        Ok(())
    }

    fn validate_child_entries(&self, entries: &[ProcEntry]) -> Result<(), ApplyError> {
        if entries.iter().any(|entry| entry.pid == self.authority) {
            return Err(ApplyError::AuthorityPid);
        }
        Ok(())
    }

    fn next_revision(&self) -> Result<Revision, ApplyError> {
        let value = self
            .revision
            .checked_add(1)
            .ok_or(ApplyError::CounterExhausted("directory revision"))?;
        Ok(Revision {
            authority: self.authority,
            value,
        })
    }

    fn link(&self, link: LinkId) -> Result<&LinkState, ApplyError> {
        self.links
            .get(&link)
            .ok_or(ApplyError::UnknownLink { link })
    }

    fn link_mut(&mut self, link: LinkId) -> Result<&mut LinkState, ApplyError> {
        self.links
            .get_mut(&link)
            .ok_or(ApplyError::UnknownLink { link })
    }
}

#[derive(Debug)]
struct DirectoryRecord {
    owner: LinkId,
    entry: ProcEntry,
}

#[derive(Debug)]
struct LinkState {
    child: Pid,
    active: Option<ActivePublication>,
    staging: Option<StagedSnapshot>,
}

impl LinkState {
    fn new(child: Pid) -> Self {
        Self {
            child,
            active: None,
            staging: None,
        }
    }
}

#[derive(Clone, Debug)]
struct ActivePublication {
    sequence: PublicationSequence,
    pids: BTreeSet<Pid>,
    last_commit: CommitRecord,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CommitRecord {
    Snapshot {
        snapshot: SnapshotId,
        chunks: Vec<Vec<ProcEntry>>,
    },
    Delta {
        upserts: Vec<ProcEntry>,
        removals: Vec<Pid>,
    },
}

#[derive(Clone, Debug)]
struct StagedSnapshot {
    snapshot: SnapshotId,
    base_sequence: PublicationSequence,
    chunks: Vec<Vec<ProcEntry>>,
    pids: BTreeSet<Pid>,
}

impl StagedSnapshot {
    fn new(snapshot: SnapshotId, base_sequence: PublicationSequence) -> Self {
        Self {
            snapshot,
            base_sequence,
            chunks: Vec::new(),
            pids: BTreeSet::new(),
        }
    }
}

fn validate_entries(entries: &[ProcEntry]) -> Result<(), ApplyError> {
    let mut pids = BTreeSet::new();
    for entry in entries {
        if entry.pid.is_link_local() {
            return Err(ApplyError::ReservedPid);
        }
        if !pids.insert(entry.pid) {
            return Err(ApplyError::DuplicatePid { pid: entry.pid });
        }
    }
    Ok(())
}

fn first_duplicate(values: impl Iterator<Item = Pid>) -> Option<Pid> {
    let mut seen = BTreeSet::new();
    values.into_iter().find(|value| !seen.insert(*value))
}

fn ack(sequence: PublicationSequence) -> ApplyEffects {
    ApplyEffects {
        directory_change: None,
        link_response: Some(LinkResponse::PublicationAck { sequence }),
    }
}

fn resnapshot(expected_sequence: PublicationSequence) -> ApplyEffects {
    ApplyEffects {
        directory_change: None,
        link_response: Some(LinkResponse::ResnapshotRequired { expected_sequence }),
    }
}

#[cfg(test)]
mod tests {
    use chrysalis_transport::DatagramAddr;

    use super::*;
    use crate::Locator;

    const AUTHORITY: Pid = Pid::from_bytes([0x10; 16]);
    const CHILD_1: Pid = Pid::from_bytes([0x11; 16]);
    const CHILD_2: Pid = Pid::from_bytes([0x12; 16]);
    const PID_A: Pid = Pid::from_bytes([0xa0; 16]);
    const PID_B: Pid = Pid::from_bytes([0xb0; 16]);
    const PID_C: Pid = Pid::from_bytes([0xc0; 16]);

    fn link(value: u8) -> LinkId {
        LinkId::from_bytes([value; 16])
    }

    fn entry(pid: Pid, value: u8) -> ProcEntry {
        ProcEntry {
            pid,
            tls_server_name: "target.test".into(),
            labels: crate::protocol::Labels::new(),
            locators: vec![Locator {
                address: DatagramAddr::new("test", [value]),
                priority: u32::from(value),
            }],
        }
    }

    fn nameserver() -> Nameserver {
        Nameserver::try_new(AUTHORITY).expect("construct nameserver")
    }

    fn admit(nameserver: &mut Nameserver, link: LinkId, child: Pid) {
        assert_eq!(
            nameserver
                .apply(Command::AdmitLink { link, child })
                .expect("admit link"),
            ApplyEffects::default()
        );
    }

    fn publish(
        nameserver: &mut Nameserver,
        link: LinkId,
        snapshot: SnapshotId,
        sequence: PublicationSequence,
        entries: Vec<ProcEntry>,
    ) -> ApplyEffects {
        nameserver
            .apply(Command::BeginSnapshot {
                link,
                snapshot,
                base_sequence: sequence,
            })
            .expect("begin snapshot");
        nameserver
            .apply(Command::AppendSnapshotChunk {
                link,
                snapshot,
                chunk: 0,
                entries,
            })
            .expect("append snapshot chunk");
        nameserver
            .apply(Command::CommitSnapshot {
                link,
                snapshot,
                chunks: 1,
            })
            .expect("commit snapshot")
    }

    #[test]
    fn snapshot_is_invisible_until_commit_and_acknowledged_afterward() {
        let mut nameserver = nameserver();
        let link = link(1);
        let snapshot = SnapshotId::from_u64(1);
        let sequence = PublicationSequence::from_u64(5);
        let expected = entry(PID_A, 1);
        admit(&mut nameserver, link, CHILD_1);

        nameserver
            .apply(Command::BeginSnapshot {
                link,
                snapshot,
                base_sequence: sequence,
            })
            .expect("begin snapshot");
        nameserver
            .apply(Command::AppendSnapshotChunk {
                link,
                snapshot,
                chunk: 0,
                entries: vec![expected.clone()],
            })
            .expect("append snapshot");
        assert!(nameserver.get(PID_A).is_none());
        assert_eq!(nameserver.revision().value, 0);

        let effects = nameserver
            .apply(Command::CommitSnapshot {
                link,
                snapshot,
                chunks: 1,
            })
            .expect("commit snapshot");
        assert_eq!(nameserver.get(PID_A), Some(&expected));
        assert_eq!(nameserver.owner(PID_A), Some(link));
        assert_eq!(
            effects.link_response,
            Some(LinkResponse::PublicationAck { sequence })
        );
        assert_eq!(
            effects.directory_change,
            Some(DirectoryChange {
                link,
                revision: Revision {
                    authority: AUTHORITY,
                    value: 1,
                },
                upserts: vec![expected.clone()],
                removals: vec![],
            })
        );

        assert_eq!(
            nameserver
                .apply(Command::AppendSnapshotChunk {
                    link,
                    snapshot,
                    chunk: 0,
                    entries: vec![expected],
                })
                .expect("replay committed snapshot chunk"),
            ApplyEffects::default()
        );
        assert_eq!(
            nameserver.apply(Command::AppendSnapshotChunk {
                link,
                snapshot,
                chunk: 0,
                entries: vec![entry(PID_B, 2)],
            }),
            Err(ApplyError::ConflictingReplay)
        );

        assert_eq!(
            nameserver
                .apply(Command::CommitSnapshot {
                    link,
                    snapshot,
                    chunks: 1,
                })
                .expect("replay snapshot commit"),
            ack(sequence)
        );
        assert_eq!(nameserver.revision().value, 1);
    }

    #[test]
    fn replacement_snapshot_keeps_old_state_until_atomic_commit() {
        let mut nameserver = nameserver();
        let link = link(1);
        admit(&mut nameserver, link, CHILD_1);
        publish(
            &mut nameserver,
            link,
            SnapshotId::from_u64(1),
            PublicationSequence::from_u64(1),
            vec![entry(PID_A, 1), entry(PID_B, 1)],
        );

        let replacement_b = entry(PID_B, 2);
        let replacement_c = entry(PID_C, 2);
        nameserver
            .apply(Command::BeginSnapshot {
                link,
                snapshot: SnapshotId::from_u64(2),
                base_sequence: PublicationSequence::from_u64(8),
            })
            .expect("begin replacement snapshot");
        nameserver
            .apply(Command::AppendSnapshotChunk {
                link,
                snapshot: SnapshotId::from_u64(2),
                chunk: 0,
                entries: vec![replacement_b.clone(), replacement_c.clone()],
            })
            .expect("append replacement snapshot");
        assert_eq!(nameserver.get(PID_A), Some(&entry(PID_A, 1)));
        assert_eq!(nameserver.get(PID_B), Some(&entry(PID_B, 1)));
        assert!(nameserver.get(PID_C).is_none());

        let effects = nameserver
            .apply(Command::CommitSnapshot {
                link,
                snapshot: SnapshotId::from_u64(2),
                chunks: 1,
            })
            .expect("commit replacement snapshot");
        assert!(nameserver.get(PID_A).is_none());
        assert_eq!(nameserver.get(PID_B), Some(&replacement_b));
        assert_eq!(nameserver.get(PID_C), Some(&replacement_c));
        let change = effects.directory_change.expect("directory changed");
        assert_eq!(change.revision.value, 2);
        assert_eq!(change.upserts, vec![replacement_b, replacement_c]);
        assert_eq!(change.removals, vec![PID_A]);
    }

    #[test]
    fn snapshot_chunks_require_order_and_exact_replays() {
        let mut nameserver = nameserver();
        let link = link(1);
        let snapshot = SnapshotId::from_u64(1);
        admit(&mut nameserver, link, CHILD_1);
        nameserver
            .apply(Command::BeginSnapshot {
                link,
                snapshot,
                base_sequence: PublicationSequence::from_u64(1),
            })
            .expect("begin snapshot");

        assert_eq!(
            nameserver.apply(Command::AppendSnapshotChunk {
                link,
                snapshot,
                chunk: 1,
                entries: vec![],
            }),
            Err(ApplyError::UnexpectedSnapshotChunk {
                expected: 0,
                actual: 1,
            })
        );
        let chunk = vec![entry(PID_A, 1)];
        nameserver
            .apply(Command::AppendSnapshotChunk {
                link,
                snapshot,
                chunk: 0,
                entries: chunk.clone(),
            })
            .expect("append first chunk");
        assert_eq!(
            nameserver
                .apply(Command::AppendSnapshotChunk {
                    link,
                    snapshot,
                    chunk: 0,
                    entries: chunk,
                })
                .expect("replay first chunk"),
            ApplyEffects::default()
        );
        assert_eq!(
            nameserver.apply(Command::AppendSnapshotChunk {
                link,
                snapshot,
                chunk: 0,
                entries: vec![entry(PID_B, 1)],
            }),
            Err(ApplyError::ConflictingReplay)
        );
        assert_eq!(
            nameserver.apply(Command::AppendSnapshotChunk {
                link,
                snapshot,
                chunk: 1,
                entries: vec![entry(PID_A, 2)],
            }),
            Err(ApplyError::DuplicatePid { pid: PID_A })
        );
        nameserver
            .apply(Command::AppendSnapshotChunk {
                link,
                snapshot,
                chunk: 1,
                entries: vec![entry(PID_B, 2)],
            })
            .expect("append chunk after rejected duplicate");
    }

    #[test]
    fn link_and_snapshot_incarnations_cannot_regress() {
        let mut nameserver = nameserver();
        let first = link(1);
        admit(&mut nameserver, first, CHILD_1);
        assert_eq!(
            nameserver.apply(Command::AdmitLink {
                link: link(2),
                child: CHILD_1,
            }),
            Err(ApplyError::ChildAlreadyLinked { child: CHILD_1 })
        );
        let snapshot = SnapshotId::from_u64(1);
        let sequence = PublicationSequence::from_u64(10);
        publish(
            &mut nameserver,
            first,
            snapshot,
            sequence,
            vec![entry(PID_A, 1)],
        );
        assert_eq!(
            nameserver
                .apply(Command::BeginSnapshot {
                    link: first,
                    snapshot,
                    base_sequence: sequence,
                })
                .expect("replay committed snapshot begin"),
            ApplyEffects::default()
        );
        assert_eq!(
            nameserver.apply(Command::BeginSnapshot {
                link: first,
                snapshot: SnapshotId::from_u64(2),
                base_sequence: PublicationSequence::from_u64(9),
            }),
            Err(ApplyError::StalePublicationSequence {
                current: 10,
                proposed: 9,
            })
        );
    }

    #[test]
    fn delta_gap_requests_snapshot_and_contiguous_delta_is_atomic() {
        let mut nameserver = nameserver();
        let link = link(1);
        admit(&mut nameserver, link, CHILD_1);
        publish(
            &mut nameserver,
            link,
            SnapshotId::from_u64(1),
            PublicationSequence::from_u64(10),
            vec![entry(PID_A, 1)],
        );
        assert_eq!(
            nameserver.apply(Command::ApplyDelta {
                link,
                sequence: PublicationSequence::from_u64(10),
                upserts: vec![],
                removals: vec![],
            }),
            Err(ApplyError::ConflictingReplay)
        );
        let upserts = vec![entry(PID_B, 2)];
        let removals = vec![PID_A];

        assert_eq!(
            nameserver
                .apply(Command::ApplyDelta {
                    link,
                    sequence: PublicationSequence::from_u64(12),
                    upserts: upserts.clone(),
                    removals: removals.clone(),
                })
                .expect("detect sequence gap"),
            resnapshot(PublicationSequence::from_u64(11))
        );
        assert!(nameserver.get(PID_A).is_some());
        assert!(nameserver.get(PID_B).is_none());

        let command = Command::ApplyDelta {
            link,
            sequence: PublicationSequence::from_u64(11),
            upserts: upserts.clone(),
            removals: removals.clone(),
        };
        let effects = nameserver.apply(command.clone()).expect("apply delta");
        assert!(nameserver.get(PID_A).is_none());
        assert_eq!(nameserver.get(PID_B), Some(&upserts[0]));
        assert_eq!(
            effects.link_response,
            Some(LinkResponse::PublicationAck {
                sequence: PublicationSequence::from_u64(11),
            })
        );
        assert_eq!(
            effects
                .directory_change
                .expect("directory change")
                .revision
                .value,
            2
        );
        assert_eq!(
            nameserver.apply(command).expect("replay delta"),
            ack(PublicationSequence::from_u64(11))
        );
        assert_eq!(nameserver.revision().value, 2);
        assert_eq!(
            nameserver.apply(Command::ApplyDelta {
                link,
                sequence: PublicationSequence::from_u64(11),
                upserts: vec![entry(PID_C, 3)],
                removals,
            }),
            Err(ApplyError::ConflictingReplay)
        );
    }

    #[test]
    fn delta_is_rejected_while_replacement_snapshot_is_staging() {
        let mut nameserver = nameserver();
        let link = link(1);
        admit(&mut nameserver, link, CHILD_1);
        publish(
            &mut nameserver,
            link,
            SnapshotId::from_u64(1),
            PublicationSequence::from_u64(1),
            vec![entry(PID_A, 1)],
        );
        nameserver
            .apply(Command::BeginSnapshot {
                link,
                snapshot: SnapshotId::from_u64(2),
                base_sequence: PublicationSequence::from_u64(2),
            })
            .expect("begin replacement snapshot");

        assert_eq!(
            nameserver.apply(Command::ApplyDelta {
                link,
                sequence: PublicationSequence::from_u64(2),
                upserts: vec![entry(PID_B, 2)],
                removals: vec![PID_A],
            }),
            Err(ApplyError::SnapshotInProgress)
        );
        assert_eq!(nameserver.get(PID_A), Some(&entry(PID_A, 1)));
        assert!(nameserver.get(PID_B).is_none());
    }

    #[test]
    fn publication_conflicts_are_rejected_without_partial_changes() {
        let mut nameserver = nameserver();
        let first = link(1);
        let second = link(2);
        admit(&mut nameserver, first, CHILD_1);
        admit(&mut nameserver, second, CHILD_2);
        publish(
            &mut nameserver,
            first,
            SnapshotId::from_u64(1),
            PublicationSequence::from_u64(1),
            vec![entry(PID_A, 1)],
        );

        nameserver
            .apply(Command::BeginSnapshot {
                link: second,
                snapshot: SnapshotId::from_u64(2),
                base_sequence: PublicationSequence::from_u64(1),
            })
            .expect("begin conflicting snapshot");
        nameserver
            .apply(Command::AppendSnapshotChunk {
                link: second,
                snapshot: SnapshotId::from_u64(2),
                chunk: 0,
                entries: vec![entry(PID_A, 2), entry(PID_B, 2)],
            })
            .expect("stage conflicting snapshot");
        assert_eq!(
            nameserver.apply(Command::CommitSnapshot {
                link: second,
                snapshot: SnapshotId::from_u64(2),
                chunks: 1,
            }),
            Err(ApplyError::PidOwnedByAnotherLink { pid: PID_A })
        );
        assert_eq!(nameserver.get(PID_A), Some(&entry(PID_A, 1)));
        assert!(nameserver.get(PID_B).is_none());
        assert_eq!(nameserver.revision().value, 1);
    }

    #[test]
    fn invalid_deltas_cannot_modify_another_links_state() {
        let mut nameserver = nameserver();
        let first = link(1);
        let second = link(2);
        admit(&mut nameserver, first, CHILD_1);
        admit(&mut nameserver, second, CHILD_2);
        publish(
            &mut nameserver,
            first,
            SnapshotId::from_u64(1),
            PublicationSequence::from_u64(1),
            vec![entry(PID_A, 1)],
        );
        publish(
            &mut nameserver,
            second,
            SnapshotId::from_u64(2),
            PublicationSequence::from_u64(1),
            vec![entry(PID_B, 2)],
        );

        assert_eq!(
            nameserver.apply(Command::ApplyDelta {
                link: second,
                sequence: PublicationSequence::from_u64(2),
                upserts: vec![entry(PID_A, 3)],
                removals: vec![],
            }),
            Err(ApplyError::PidOwnedByAnotherLink { pid: PID_A })
        );
        assert_eq!(
            nameserver.apply(Command::ApplyDelta {
                link: second,
                sequence: PublicationSequence::from_u64(2),
                upserts: vec![],
                removals: vec![PID_A],
            }),
            Err(ApplyError::PidNotOwnedByLink { pid: PID_A })
        );
        assert_eq!(
            nameserver.apply(Command::ApplyDelta {
                link: second,
                sequence: PublicationSequence::from_u64(2),
                upserts: vec![entry(PID_B, 3)],
                removals: vec![PID_B],
            }),
            Err(ApplyError::PidBothUpsertedAndRemoved { pid: PID_B })
        );
        assert_eq!(
            nameserver.apply(Command::ApplyDelta {
                link: second,
                sequence: PublicationSequence::from_u64(2),
                upserts: vec![],
                removals: vec![PID_B, PID_B],
            }),
            Err(ApplyError::DuplicatePid { pid: PID_B })
        );
        assert_eq!(nameserver.get(PID_A), Some(&entry(PID_A, 1)));
        assert_eq!(nameserver.get(PID_B), Some(&entry(PID_B, 2)));
        assert_eq!(nameserver.revision().value, 2);
    }

    #[test]
    fn incomplete_or_wrong_snapshot_cannot_become_visible() {
        let mut nameserver = nameserver();
        let link = link(1);
        let snapshot = SnapshotId::from_u64(1);
        admit(&mut nameserver, link, CHILD_1);
        nameserver
            .apply(Command::BeginSnapshot {
                link,
                snapshot,
                base_sequence: PublicationSequence::from_u64(1),
            })
            .expect("begin snapshot");
        nameserver
            .apply(Command::AppendSnapshotChunk {
                link,
                snapshot,
                chunk: 0,
                entries: vec![entry(PID_A, 1)],
            })
            .expect("append snapshot chunk");

        assert_eq!(
            nameserver.apply(Command::CommitSnapshot {
                link,
                snapshot,
                chunks: 2,
            }),
            Err(ApplyError::IncompleteSnapshot {
                expected: 2,
                actual: 1,
            })
        );
        assert_eq!(
            nameserver.apply(Command::CommitSnapshot {
                link,
                snapshot: SnapshotId::from_u64(2),
                chunks: 1,
            }),
            Err(ApplyError::WrongSnapshot {
                snapshot: SnapshotId::from_u64(2),
            })
        );
        assert!(nameserver.is_empty());
        assert_eq!(nameserver.revision().value, 0);
    }

    #[test]
    fn removing_link_revokes_every_owned_entry_once() {
        let mut nameserver = nameserver();
        let link = link(1);
        admit(&mut nameserver, link, CHILD_1);
        publish(
            &mut nameserver,
            link,
            SnapshotId::from_u64(1),
            PublicationSequence::from_u64(1),
            vec![entry(PID_A, 1), entry(PID_B, 1)],
        );

        let effects = nameserver
            .apply(Command::RemoveLink { link })
            .expect("remove link");
        assert!(nameserver.is_empty());
        assert_eq!(nameserver.owner(PID_A), None);
        assert_eq!(
            effects.directory_change,
            Some(DirectoryChange {
                link,
                revision: Revision {
                    authority: AUTHORITY,
                    value: 2,
                },
                upserts: vec![],
                removals: vec![PID_A, PID_B],
            })
        );
        assert_eq!(
            nameserver
                .apply(Command::RemoveLink { link })
                .expect("replay link removal"),
            ApplyEffects::default()
        );
        assert_eq!(nameserver.revision().value, 2);
        admit(&mut nameserver, LinkId::from_bytes([2; 16]), CHILD_1);
    }

    #[test]
    fn rejects_reserved_pid_and_delta_without_baseline() {
        assert!(matches!(
            Nameserver::try_new(Pid::LINK_LOCAL),
            Err(ApplyError::ReservedPid)
        ));
        let mut nameserver = nameserver();
        let link = link(1);
        admit(&mut nameserver, link, CHILD_1);
        assert_eq!(
            nameserver.apply(Command::ApplyDelta {
                link,
                sequence: PublicationSequence::from_u64(1),
                upserts: vec![],
                removals: vec![],
            }),
            Ok(resnapshot(PublicationSequence::from_u64(0)))
        );
        assert_eq!(
            nameserver.apply(Command::AppendSnapshotChunk {
                link,
                snapshot: SnapshotId::from_u64(1),
                chunk: 0,
                entries: vec![entry(Pid::LINK_LOCAL, 1)],
            }),
            Err(ApplyError::ReservedPid)
        );
    }

    #[test]
    fn rejects_child_publication_of_authority_pid() {
        let mut nameserver = nameserver();
        let link = link(1);
        admit(&mut nameserver, link, CHILD_1);
        nameserver
            .apply(Command::BeginSnapshot {
                link,
                snapshot: SnapshotId::from_u64(1),
                base_sequence: PublicationSequence::from_u64(1),
            })
            .unwrap();
        assert_eq!(
            nameserver.apply(Command::AppendSnapshotChunk {
                link,
                snapshot: SnapshotId::from_u64(1),
                chunk: 0,
                entries: vec![entry(AUTHORITY, 1)],
            }),
            Err(ApplyError::AuthorityPid)
        );
    }

    #[test]
    fn snapshot_is_pid_ordered_and_excludes_authority() {
        let mut nameserver = nameserver();
        let link = link(1);
        admit(&mut nameserver, link, CHILD_1);
        publish(
            &mut nameserver,
            link,
            SnapshotId::from_u64(1),
            PublicationSequence::from_u64(1),
            vec![entry(PID_B, 1), entry(PID_A, 1)],
        );
        assert_eq!(nameserver.authority(), AUTHORITY);
        assert_eq!(
            nameserver.snapshot(),
            vec![entry(PID_A, 1), entry(PID_B, 1)]
        );
    }
}
