/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::BTreeMap;

use chrysalis_core::Pid;
use thiserror::Error;

use crate::Resolution;

/// A caller-supplied monotonic timestamp in milliseconds.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CacheTime(u64);

impl CacheTime {
    /// Constructs a monotonic cache timestamp.
    pub const fn from_millis(value: u64) -> Self {
        Self(value)
    }

    /// Returns the monotonic millisecond value.
    pub const fn as_millis(self) -> u64 {
        self.0
    }
}

/// A resolution cache update error.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CacheError {
    /// PID zero cannot identify a cache authority.
    #[error("reserved link-local nameserver PID")]
    ReservedPid,

    /// The update came from another nameserver incarnation.
    #[error("cache authority mismatch: expected {expected:?}, got {actual:?}")]
    AuthorityMismatch { expected: Pid, actual: Pid },

    /// One authority reused a revision for a different result.
    #[error("conflicting result at revision {revision}")]
    ConflictingRevision { revision: u64 },

    /// The negative cache deadline cannot be represented.
    #[error("negative cache deadline overflow")]
    DeadlineOverflow,
}

/// Positive and expiring negative resolution state from one parent incarnation.
#[derive(Debug)]
pub struct ResolverCache {
    authority: Pid,
    entries: BTreeMap<Pid, CachedResolution>,
}

impl ResolverCache {
    /// Constructs an empty cache scoped to one parent nameserver incarnation.
    pub fn try_new(authority: Pid) -> Result<Self, CacheError> {
        if authority.is_link_local() {
            return Err(CacheError::ReservedPid);
        }
        Ok(Self {
            authority,
            entries: BTreeMap::new(),
        })
    }

    /// Applies an ordered cache update.
    ///
    /// Returns `true` when the cache value changed. Replaying an equal update does not extend a
    /// negative result's original deadline.
    pub fn update(&mut self, result: Resolution, now: CacheTime) -> Result<bool, CacheError> {
        let revision = result.revision();
        if revision.authority != self.authority {
            return Err(CacheError::AuthorityMismatch {
                expected: self.authority,
                actual: revision.authority,
            });
        }
        let pid = result.pid();
        if let Some(current) = self.entries.get(&pid) {
            if revision.value < current.result.revision().value {
                return Ok(false);
            }
            if revision.value == current.result.revision().value {
                if result == current.result {
                    return Ok(false);
                }
                return Err(CacheError::ConflictingRevision {
                    revision: revision.value,
                });
            }
        }
        let expires_at = match &result {
            Resolution::Found { .. } => None,
            Resolution::NotFound {
                valid_for_millis, ..
            } => Some(CacheTime(
                now.0
                    .checked_add(*valid_for_millis)
                    .ok_or(CacheError::DeadlineOverflow)?,
            )),
        };
        self.entries
            .insert(pid, CachedResolution { result, expires_at });
        Ok(true)
    }

    /// Returns a currently usable cached result.
    pub fn resolve(&self, pid: Pid, now: CacheTime) -> Option<Resolution> {
        let cached = self.entries.get(&pid)?;
        if cached.expires_at.is_some_and(|deadline| now >= deadline) {
            return None;
        }
        Some(cached.result.clone())
    }

    /// Clears all values and revision floors when the parent link closes.
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

#[derive(Debug)]
struct CachedResolution {
    result: Resolution,
    expires_at: Option<CacheTime>,
}

#[cfg(test)]
mod tests {
    use chrysalis_transport::DatagramAddr;

    use super::*;
    use crate::Locator;
    use crate::ProcEntry;
    use crate::Revision;

    const AUTHORITY: Pid = Pid::from_bytes([0x10; 16]);
    const OTHER_AUTHORITY: Pid = Pid::from_bytes([0x20; 16]);
    const TARGET: Pid = Pid::from_bytes([0x30; 16]);

    fn revision(authority: Pid, value: u64) -> Revision {
        Revision { authority, value }
    }

    fn found(value: u8, revision_value: u64) -> Resolution {
        Resolution::Found {
            entry: ProcEntry {
                pid: TARGET,
                tls_server_name: "target.test".into(),
                labels: crate::protocol::Labels::new(),
                locators: vec![Locator {
                    address: DatagramAddr::new("test", [value]),
                    priority: u32::from(value),
                }],
            },
            revision: revision(AUTHORITY, revision_value),
        }
    }

    fn not_found(revision_value: u64, valid_for_millis: u64) -> Resolution {
        Resolution::NotFound {
            pid: TARGET,
            revision: revision(AUTHORITY, revision_value),
            valid_for_millis,
        }
    }

    #[test]
    fn positive_updates_are_revision_ordered() {
        let mut cache = ResolverCache::try_new(AUTHORITY).expect("construct cache");
        let initial = found(1, 2);
        assert!(
            cache
                .update(initial.clone(), CacheTime::from_millis(10))
                .expect("insert initial result")
        );
        assert_eq!(
            cache.resolve(TARGET, CacheTime::from_millis(u64::MAX)),
            Some(initial.clone())
        );
        assert!(
            !cache
                .update(found(0, 1), CacheTime::from_millis(20))
                .expect("ignore stale result")
        );
        assert!(
            !cache
                .update(initial.clone(), CacheTime::from_millis(20))
                .expect("replay result")
        );
        assert_eq!(
            cache.update(found(2, 2), CacheTime::from_millis(20)),
            Err(CacheError::ConflictingRevision { revision: 2 })
        );
        let replacement = found(3, 3);
        assert!(
            cache
                .update(replacement.clone(), CacheTime::from_millis(20))
                .expect("replace result")
        );
        assert_eq!(
            cache.resolve(TARGET, CacheTime::from_millis(30)),
            Some(replacement)
        );
    }

    #[test]
    fn negative_expiry_is_receiver_relative_and_replay_does_not_extend_it() {
        let mut cache = ResolverCache::try_new(AUTHORITY).expect("construct cache");
        let negative = not_found(1, 50);
        cache
            .update(negative.clone(), CacheTime::from_millis(100))
            .expect("insert negative result");
        assert_eq!(
            cache.resolve(TARGET, CacheTime::from_millis(149)),
            Some(negative.clone())
        );
        cache
            .update(negative, CacheTime::from_millis(140))
            .expect("replay negative result");
        assert_eq!(cache.resolve(TARGET, CacheTime::from_millis(150)), None);

        assert!(
            !cache
                .update(not_found(0, 1000), CacheTime::from_millis(150))
                .expect("ignore stale result after expiry")
        );
        assert_eq!(cache.resolve(TARGET, CacheTime::from_millis(151)), None);
    }

    #[test]
    fn validates_authority_deadline_and_link_clear() {
        assert!(matches!(
            ResolverCache::try_new(Pid::LINK_LOCAL),
            Err(CacheError::ReservedPid)
        ));
        let mut cache = ResolverCache::try_new(AUTHORITY).expect("construct cache");
        assert_eq!(
            cache.update(
                Resolution::NotFound {
                    pid: TARGET,
                    revision: revision(OTHER_AUTHORITY, 1),
                    valid_for_millis: 1,
                },
                CacheTime::from_millis(0),
            ),
            Err(CacheError::AuthorityMismatch {
                expected: AUTHORITY,
                actual: OTHER_AUTHORITY,
            })
        );
        assert_eq!(
            cache.update(not_found(1, 1), CacheTime::from_millis(u64::MAX)),
            Err(CacheError::DeadlineOverflow)
        );
        cache
            .update(found(1, 2), CacheTime::from_millis(0))
            .expect("insert positive result");
        cache.clear();
        assert_eq!(cache.resolve(TARGET, CacheTime::from_millis(0)), None);
    }
}
