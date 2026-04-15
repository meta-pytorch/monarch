/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Mesh identity types.
//!
//! [`ResourceId`] is the common identity type for mesh resources: a uid
//! for identity and an optional label for human readability. The mesh-specific
//! newtypes [`ActorMeshId`], [`ProcMeshId`], and [`HostMeshId`] provide type
//! safety at mesh struct boundaries while converting freely to [`ResourceId`]
//! for resource message plumbing.

use std::cmp::Ordering;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::str::FromStr;

use hyperactor::id::Label;
use hyperactor::id::LabelError;
use hyperactor::id::Uid;
use hyperactor::id::UidParseError;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::Name;

/// Errors that can occur when parsing a [`ResourceId`] from a string.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResourceIdParseError {
    /// Error parsing the uid component.
    #[error("invalid uid: {0}")]
    InvalidUid(#[from] UidParseError),
    /// Error parsing the label component.
    #[error("invalid label: {0}")]
    InvalidLabel(#[from] LabelError),
}

/// Identifies a resource in the mesh system.
///
/// Identity (Eq, Hash, Ord) is determined solely by `uid`; `label` is
/// informational metadata excluded from comparisons.
#[derive(Clone, Serialize, Deserialize, Named)]
pub struct ResourceId {
    uid: Uid,
    label: Option<Label>,
}
wirevalue::register_type!(ResourceId);

impl ResourceId {
    /// Create a [`ResourceId`] with explicit uid and label.
    pub fn new(uid: Uid, label: Option<Label>) -> Self {
        Self { uid, label }
    }

    /// Create a singleton [`ResourceId`] identified by label.
    /// The label becomes the uid; no separate label metadata is stored.
    pub fn singleton(label: Label) -> Self {
        Self {
            uid: Uid::Singleton(label),
            label: None,
        }
    }

    /// Create a unique [`ResourceId`] with a random uid and the given label.
    pub fn unique(label: Label) -> Self {
        Self {
            uid: Uid::instance(),
            label: Some(label),
        }
    }

    /// Returns the uid.
    pub fn uid(&self) -> &Uid {
        &self.uid
    }

    /// Returns the label.
    pub fn label(&self) -> Option<&Label> {
        self.label.as_ref()
    }
}

impl PartialEq for ResourceId {
    fn eq(&self, other: &Self) -> bool {
        self.uid == other.uid
    }
}

impl Eq for ResourceId {}

impl Hash for ResourceId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.uid.hash(state);
    }
}

impl PartialOrd for ResourceId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ResourceId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.uid.cmp(&other.uid)
    }
}

/// Displays as:
/// - `_label` for singletons (e.g., `_local`)
/// - `label-hex16` for labeled instances (e.g., `workers-a1b2c3d4e5f6a7b8`)
/// - `hex16` for unlabeled instances (e.g., `a1b2c3d4e5f6a7b8`)
impl fmt::Display for ResourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.uid, &self.label) {
            (Uid::Singleton(label), _) => write!(f, "_{label}"),
            (Uid::Instance(uid), Some(label)) => write!(f, "{label}-{uid:016x}"),
            (Uid::Instance(uid), None) => write!(f, "{uid:016x}"),
        }
    }
}

impl fmt::Debug for ResourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.uid, &self.label) {
            (Uid::Singleton(label), _) => write!(f, "<_{label}>"),
            (Uid::Instance(uid), Some(label)) => write!(f, "<'{label}' {uid:016x}>"),
            (Uid::Instance(uid), None) => write!(f, "<{uid:016x}>"),
        }
    }
}

/// Parses:
/// - `_label` as a singleton
/// - `label-hex16` as a labeled instance (hex16 = exactly 16 hex digits)
/// - `hex16` as an unlabeled instance
impl FromStr for ResourceId {
    type Err = ResourceIdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(rest) = s.strip_prefix('_') {
            let label = Label::new(rest)?;
            return Ok(Self::singleton(label));
        }

        // Labeled instance: `label-hex16`. The hex portion is always exactly
        // 16 characters (zero-padded), so the dash is at `len - 17`.
        if s.len() >= 18 {
            let dash_pos = s.len() - 17;
            if s.as_bytes()[dash_pos] == b'-' {
                let label_part = &s[..dash_pos];
                let hex_part = &s[dash_pos + 1..];
                if hex_part.len() == 16 && hex_part.chars().all(|c| c.is_ascii_hexdigit()) {
                    if let Ok(label) = Label::new(label_part) {
                        let uid = u64::from_str_radix(hex_part, 16)
                            .map_err(|_| UidParseError::InvalidHex(hex_part.to_string()))?;
                        return Ok(Self {
                            uid: Uid::Instance(uid),
                            label: Some(label),
                        });
                    }
                }
            }
        }

        let uid: Uid = s.parse()?;
        Ok(Self { uid, label: None })
    }
}

/// Converts a [`Name`] to a [`ResourceId`] for migration.
///
/// `Name::Suffixed` maps to an instance uid (preserving the `ShortUuid`'s
/// inner u64) with a stripped label. `Name::Reserved` maps to a singleton
/// uid with the name as the label.
impl From<Name> for ResourceId {
    fn from(name: Name) -> Self {
        match name {
            Name::Suffixed(s, uuid) => ResourceId {
                uid: Uid::Instance(uuid.0),
                label: Some(Label::strip(&s)),
            },
            Name::Reserved(s) => {
                let label = Label::new(&s).unwrap_or_else(|_| Label::strip(&s));
                ResourceId::singleton(label)
            }
        }
    }
}

macro_rules! define_mesh_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Serialize, Deserialize, Named)]
        #[serde(transparent)]
        pub struct $name(ResourceId);
        wirevalue::register_type!($name);

        impl $name {
            /// Create a mesh id with explicit uid and label.
            pub fn new(uid: Uid, label: Option<Label>) -> Self {
                Self(ResourceId::new(uid, label))
            }

            /// Create a singleton mesh id identified by label.
            pub fn singleton(label: Label) -> Self {
                Self(ResourceId::singleton(label))
            }

            /// Create a unique mesh id with a random uid and the given label.
            pub fn unique(label: Label) -> Self {
                Self(ResourceId::unique(label))
            }

            /// Returns the uid.
            pub fn uid(&self) -> &Uid {
                self.0.uid()
            }

            /// Returns the label.
            pub fn label(&self) -> Option<&Label> {
                self.0.label()
            }

            /// Returns the inner [`ResourceId`].
            pub fn resource_id(&self) -> &ResourceId {
                &self.0
            }
        }

        impl From<$name> for ResourceId {
            fn from(id: $name) -> Self {
                id.0
            }
        }

        impl From<ResourceId> for $name {
            fn from(id: ResourceId) -> Self {
                Self(id)
            }
        }

        impl From<Name> for $name {
            fn from(name: Name) -> Self {
                Self(ResourceId::from(name))
            }
        }

        impl PartialEq for $name {
            fn eq(&self, other: &Self) -> bool {
                self.0 == other.0
            }
        }

        impl Eq for $name {}

        impl Hash for $name {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.0.hash(state);
            }
        }

        impl PartialOrd for $name {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        impl Ord for $name {
            fn cmp(&self, other: &Self) -> Ordering {
                self.0.cmp(&other.0)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Debug::fmt(&self.0, f)
            }
        }

        impl FromStr for $name {
            type Err = ResourceIdParseError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok(Self(s.parse()?))
            }
        }
    };
}

define_mesh_id!(
    /// Identifies a host mesh.
    HostMeshId
);

define_mesh_id!(
    /// Identifies a proc mesh.
    ProcMeshId
);

define_mesh_id!(
    /// Identifies an actor mesh.
    ActorMeshId
);

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;

    use super::*;

    #[test]
    fn test_resource_id_singleton() {
        let id = ResourceId::singleton(Label::new("local").unwrap());
        assert_eq!(*id.uid(), Uid::Singleton(Label::new("local").unwrap()));
        assert_eq!(id.label(), None);
        assert_eq!(id.to_string(), "_local");
    }

    #[test]
    fn test_resource_id_unique() {
        let id = ResourceId::unique(Label::new("workers").unwrap());
        assert!(matches!(id.uid(), Uid::Instance(_)));
        assert_eq!(id.label().map(|l| l.as_str()), Some("workers"));
    }

    #[test]
    fn test_resource_id_unlabeled() {
        let id = ResourceId::new(Uid::Instance(0xabcdef), None);
        assert_eq!(id.to_string(), "0000000000abcdef");
        assert_eq!(id.label(), None);
    }

    #[test]
    fn test_resource_id_eq_by_uid_only() {
        let uid = Uid::Instance(0x42);
        let a = ResourceId::new(uid.clone(), Some(Label::new("alpha").unwrap()));
        let b = ResourceId::new(uid, Some(Label::new("beta").unwrap()));
        assert_eq!(a, b);
    }

    #[test]
    fn test_resource_id_neq_different_uid() {
        let a = ResourceId::new(Uid::Instance(1), Some(Label::new("same").unwrap()));
        let b = ResourceId::new(Uid::Instance(2), Some(Label::new("same").unwrap()));
        assert_ne!(a, b);
    }

    #[test]
    fn test_resource_id_hash_by_uid_only() {
        let uid = Uid::Instance(0x42);
        let a = ResourceId::new(uid.clone(), Some(Label::new("alpha").unwrap()));
        let b = ResourceId::new(uid, Some(Label::new("beta").unwrap()));

        let hash = |id: &ResourceId| {
            let mut h = DefaultHasher::new();
            id.hash(&mut h);
            h.finish()
        };
        assert_eq!(hash(&a), hash(&b));
    }

    #[test]
    fn test_resource_id_ord_by_uid_only() {
        let a = ResourceId::new(Uid::Instance(1), Some(Label::new("zzz").unwrap()));
        let b = ResourceId::new(Uid::Instance(2), Some(Label::new("aaa").unwrap()));
        assert!(a < b);
    }

    #[test]
    fn test_resource_id_display_singleton() {
        let id = ResourceId::singleton(Label::new("local").unwrap());
        assert_eq!(id.to_string(), "_local");
    }

    #[test]
    fn test_resource_id_display_labeled_instance() {
        let id = ResourceId::new(
            Uid::Instance(0xd5d54d7201103869),
            Some(Label::new("workers").unwrap()),
        );
        assert_eq!(id.to_string(), "workers-d5d54d7201103869");
    }

    #[test]
    fn test_resource_id_display_unlabeled_instance() {
        let id = ResourceId::new(Uid::Instance(0xd5d54d7201103869), None);
        assert_eq!(id.to_string(), "d5d54d7201103869");
    }

    #[test]
    fn test_resource_id_debug() {
        let singleton = ResourceId::singleton(Label::new("local").unwrap());
        assert_eq!(format!("{:?}", singleton), "<_local>");

        let labeled = ResourceId::new(
            Uid::Instance(0xd5d54d7201103869),
            Some(Label::new("workers").unwrap()),
        );
        assert_eq!(format!("{:?}", labeled), "<'workers' d5d54d7201103869>");

        let unlabeled = ResourceId::new(Uid::Instance(0xd5d54d7201103869), None);
        assert_eq!(format!("{:?}", unlabeled), "<d5d54d7201103869>");
    }

    #[test]
    fn test_resource_id_fromstr_singleton() {
        let parsed: ResourceId = "_local".parse().unwrap();
        assert_eq!(*parsed.uid(), Uid::Singleton(Label::new("local").unwrap()));
        assert_eq!(parsed.label(), None);
    }

    #[test]
    fn test_resource_id_fromstr_labeled_instance() {
        let parsed: ResourceId = "workers-d5d54d7201103869".parse().unwrap();
        assert_eq!(*parsed.uid(), Uid::Instance(0xd5d54d7201103869));
        assert_eq!(parsed.label().map(|l| l.as_str()), Some("workers"));
    }

    #[test]
    fn test_resource_id_fromstr_unlabeled_instance() {
        let parsed: ResourceId = "d5d54d7201103869".parse().unwrap();
        assert_eq!(*parsed.uid(), Uid::Instance(0xd5d54d7201103869));
        assert_eq!(parsed.label(), None);
    }

    #[test]
    fn test_resource_id_fromstr_labeled_with_hyphens() {
        let parsed: ResourceId = "my-service-d5d54d7201103869".parse().unwrap();
        assert_eq!(*parsed.uid(), Uid::Instance(0xd5d54d7201103869));
        assert_eq!(parsed.label().map(|l| l.as_str()), Some("my-service"));
    }

    #[test]
    fn test_resource_id_display_fromstr_roundtrip() {
        let cases = vec![
            ResourceId::singleton(Label::new("local").unwrap()),
            ResourceId::new(
                Uid::Instance(0xd5d54d7201103869),
                Some(Label::new("workers").unwrap()),
            ),
            ResourceId::new(Uid::Instance(0xd5d54d7201103869), None),
            ResourceId::new(
                Uid::Instance(0xd5d54d7201103869),
                Some(Label::new("my-service").unwrap()),
            ),
            ResourceId::new(Uid::Instance(1), Some(Label::new("a").unwrap())),
        ];
        for id in cases {
            let s = id.to_string();
            let parsed: ResourceId = s.parse().unwrap();
            assert_eq!(id, parsed, "round-trip failed for {s}");
        }
    }

    #[test]
    fn test_resource_id_serde_roundtrip() {
        let cases = vec![
            ResourceId::singleton(Label::new("local").unwrap()),
            ResourceId::new(
                Uid::Instance(0xabcdef),
                Some(Label::new("workers").unwrap()),
            ),
            ResourceId::new(Uid::Instance(0xabcdef), None),
        ];
        for id in cases {
            let json = serde_json::to_string(&id).unwrap();
            let parsed: ResourceId = serde_json::from_str(&json).unwrap();
            assert_eq!(id, parsed);
            // Verify label is preserved through serde.
            assert_eq!(
                id.label().map(|l| l.as_str()),
                parsed.label().map(|l| l.as_str())
            );
        }
    }

    #[test]
    fn test_resource_id_from_name_suffixed() {
        let name = Name::new("workers").unwrap();
        let uuid_val = name.uuid().unwrap().0;
        let id = ResourceId::from(name);
        assert_eq!(*id.uid(), Uid::Instance(uuid_val));
        assert_eq!(id.label().map(|l| l.as_str()), Some("workers"));
    }

    #[test]
    fn test_resource_id_from_name_reserved() {
        let name = Name::new_reserved("local").unwrap();
        let id = ResourceId::from(name);
        assert_eq!(*id.uid(), Uid::Singleton(Label::new("local").unwrap()));
        assert_eq!(id.label(), None);
    }

    #[test]
    fn test_resource_id_from_name_reserved_with_underscores() {
        let name = Name::new_reserved("host_agent").unwrap();
        let id = ResourceId::from(name);
        assert_eq!(*id.uid(), Uid::Singleton(Label::new("host_agent").unwrap()));
    }

    #[test]
    fn test_mesh_id_construction() {
        let host = HostMeshId::singleton(Label::new("local").unwrap());
        assert_eq!(host.to_string(), "_local");
        assert_eq!(*host.uid(), Uid::Singleton(Label::new("local").unwrap()));

        let proc_ = ProcMeshId::unique(Label::new("workers").unwrap());
        assert!(matches!(proc_.uid(), Uid::Instance(_)));
        assert_eq!(proc_.label().map(|l| l.as_str()), Some("workers"));

        let actor = ActorMeshId::unique(Label::new("trainers").unwrap());
        assert!(matches!(actor.uid(), Uid::Instance(_)));
        assert_eq!(actor.label().map(|l| l.as_str()), Some("trainers"));
    }

    #[test]
    fn test_mesh_id_eq_by_uid_only() {
        let uid = Uid::Instance(0x42);
        let a = HostMeshId::new(uid.clone(), Some(Label::new("alpha").unwrap()));
        let b = HostMeshId::new(uid, Some(Label::new("beta").unwrap()));
        assert_eq!(a, b);
    }

    #[test]
    fn test_mesh_id_display_fromstr_roundtrip() {
        let ids: Vec<HostMeshId> = vec![
            HostMeshId::singleton(Label::new("local").unwrap()),
            HostMeshId::new(
                Uid::Instance(0xd5d54d7201103869),
                Some(Label::new("workers").unwrap()),
            ),
            HostMeshId::new(Uid::Instance(0xd5d54d7201103869), None),
        ];
        for id in ids {
            let s = id.to_string();
            let parsed: HostMeshId = s.parse().unwrap();
            assert_eq!(id, parsed, "round-trip failed for {s}");
        }
    }

    #[test]
    fn test_mesh_id_resource_id_conversion() {
        let host = HostMeshId::unique(Label::new("test").unwrap());
        let resource_id: ResourceId = host.clone().into();
        assert_eq!(host.uid(), resource_id.uid());
        assert_eq!(
            host.label().map(|l| l.as_str()),
            resource_id.label().map(|l| l.as_str())
        );

        let back: HostMeshId = resource_id.into();
        assert_eq!(host, back);
    }

    #[test]
    fn test_mesh_id_from_name() {
        let name = Name::new("workers").unwrap();
        let uuid_val = name.uuid().unwrap().0;
        let id = ActorMeshId::from(name);
        assert_eq!(*id.uid(), Uid::Instance(uuid_val));
        assert_eq!(id.label().map(|l| l.as_str()), Some("workers"));
    }

    #[test]
    fn test_mesh_id_serde_transparent() {
        let host = HostMeshId::new(Uid::Instance(0xabcdef), Some(Label::new("test").unwrap()));
        let resource = ResourceId::new(Uid::Instance(0xabcdef), Some(Label::new("test").unwrap()));

        let host_json = serde_json::to_string(&host).unwrap();
        let resource_json = serde_json::to_string(&resource).unwrap();
        assert_eq!(host_json, resource_json);
    }

    #[test]
    fn test_unique_ids_differ() {
        let a = ResourceId::unique(Label::new("test").unwrap());
        let b = ResourceId::unique(Label::new("test").unwrap());
        assert_ne!(a, b);
    }

    #[test]
    fn test_singleton_ids_match() {
        let a = ResourceId::singleton(Label::new("local").unwrap());
        let b = ResourceId::singleton(Label::new("local").unwrap());
        assert_eq!(a, b);
    }
}
