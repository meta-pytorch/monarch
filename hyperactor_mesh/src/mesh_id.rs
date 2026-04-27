/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Mesh identity types.
//!
//! [`ResourceId`] is the common control-plane identifier for mesh resources:
//! a uid for identity and an optional label for human readability. The
//! mesh-specific newtypes [`ActorMeshId`], [`ProcMeshId`], and [`HostMeshId`]
//! provide type safety at mesh struct boundaries while converting freely to
//! [`ResourceId`] for resource message plumbing.
//!
//! # Where resource ids are used
//!
//! `ResourceId` is the stable key used by resource messages in `resource.rs`
//! (`GetRankStatus`, `WaitRankStatus`, `CreateOrUpdate`, `Stop`, `GetState`,
//! `StreamState`, and `List`) and by the internal state maps in `proc_agent.rs`
//! and `host_mesh/host_agent.rs`.
//!
//! The same logical resource may be rendered into other name spaces:
//!
//! - The control-plane resource name is `ResourceId::to_string()`.
//! - The runtime actor name is `ActorMeshId::actor_name()`, which is consumed
//!   by `ProcRef::actor_id()` in `proc_mesh.rs` and by `ProcAgent` when it
//!   calls `remote.gspawn(...)`.
//! - The runtime proc name is `ResourceId::to_string()`, which is consumed by
//!   `HostRef::named_proc()` in `host_mesh.rs` and by `HostAgent` when it
//!   spawns a proc on a host.
//! - Telemetry uses `display_label()` for human-facing `given_name`, while
//!   `to_string()` is emitted as the stable `full_name`.
//!
//! # String formats
//!
//! `ResourceId` has three externally visible string forms:
//!
//! - Singleton: `label`
//! - Labeled instance: `label-<instance_uid>`
//! - Unlabeled instance: `<instance_uid>`
//!
//! Here `<instance_uid>` is the hexadecimal instance component produced by
//! [`Uid::Instance`], without angle brackets. Parsing accepts the display
//! forms above. For backwards compatibility, instance uids wrapped in angle
//! brackets are also accepted when parsing.
//!
//! Identity is uid-only: labels are descriptive metadata and do not
//! participate in `Eq`, `Hash`, or `Ord`.

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

    /// Returns the explicit label metadata, if any.
    pub fn label(&self) -> Option<&Label> {
        self.label.as_ref()
    }

    /// Returns the human-facing label for this resource id.
    ///
    /// This is the explicit label metadata for instances, or the singleton
    /// label embedded in the uid. Telemetry uses this for `given_name`.
    pub fn display_label(&self) -> Option<&Label> {
        self.label.as_ref().or(match &self.uid {
            Uid::Singleton(label) => Some(label),
            _ => None,
        })
    }

    /// Returns the actor-runtime name derived from this resource id.
    ///
    /// This is the name passed into `ProcId::actor_id(...)` and used when
    /// `ProcAgent` spawns the actor process-local runtime object. Keep this
    /// helper as the single mapping point from control-plane resource ids to
    /// actor-runtime names.
    pub fn actor_name(&self) -> String {
        match (&self.uid, &self.label) {
            (Uid::Singleton(label), _) => label.to_string(),
            (Uid::Instance(_), Some(label)) => format!("{label}{}", self.uid),
            (Uid::Instance(_), None) => self.uid.to_string(),
        }
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

fn fmt_instance_uid(uid: u64) -> String {
    Uid::Instance(uid)
        .to_string()
        .trim_start_matches('<')
        .trim_end_matches('>')
        .to_string()
}

fn parse_instance_uid(s: &str) -> Result<u64, UidParseError> {
    let mut last_err = None;
    for candidate in [s.to_string(), format!("<{s}>")] {
        match Uid::from_str(&candidate) {
            Ok(Uid::Instance(uid)) => return Ok(uid),
            Ok(Uid::Singleton(_)) => {}
            Err(err) => last_err = Some(err),
        }
    }
    Err(last_err
        .expect("instance uid parse should yield an error when it does not yield an instance"))
}

fn fmt_id_component(f: &mut fmt::Formatter<'_>, uid: &Uid, label: Option<&Label>) -> fmt::Result {
    match uid {
        Uid::Singleton(singleton) => write!(f, "{singleton}"),
        Uid::Instance(uid) => match label {
            Some(label) => write!(f, "{label}-{}", fmt_instance_uid(*uid)),
            None => write!(f, "{}", fmt_instance_uid(*uid)),
        },
    }
}

impl fmt::Display for ResourceId {
    /// Formats the canonical control-plane string form of this resource id.
    ///
    /// This string is used for resource message keys, proc names on hosts,
    /// and telemetry `full_name`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_id_component(f, &self.uid, self.label.as_ref())
    }
}

impl fmt::Debug for ResourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.uid, &self.label) {
            (Uid::Singleton(label), _) => write!(f, "<{label}>"),
            (Uid::Instance(uid), Some(label)) => {
                write!(f, "<'{label}' {}>", fmt_instance_uid(*uid))
            }
            (Uid::Instance(uid), None) => write!(f, "<{}>", fmt_instance_uid(*uid)),
        }
    }
}

fn parse_id_component(s: &str) -> Result<(Uid, Option<Label>), ResourceIdParseError> {
    if let Ok(uid) = parse_instance_uid(s) {
        return Ok((Uid::Instance(uid), None));
    }

    if let Some(split) = s.rfind('-') {
        let label_part = &s[..split];
        let uid_part = &s[split + 1..];
        if let (Ok(label), Ok(uid)) = (Label::new(label_part), parse_instance_uid(uid_part)) {
            return Ok((Uid::Instance(uid), Some(label)));
        }
    }

    let label = Label::new(s)?;
    Ok((Uid::Singleton(label), None))
}

impl FromStr for ResourceId {
    type Err = ResourceIdParseError;

    /// Parses the canonical resource-id string forms accepted by the mesh
    /// control plane.
    ///
    /// Accepted inputs are:
    /// - `label` for singletons
    /// - `label-<instance_uid>` for labeled instances
    /// - `<instance_uid>` for unlabeled instances
    ///
    /// For backwards compatibility, `<instance_uid>` may also be wrapped in
    /// angle brackets.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (uid, label) = parse_id_component(s)?;
        Ok(Self { uid, label })
    }
}

macro_rules! define_mesh_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Named)]
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

            /// Returns the explicit label metadata, if any.
            pub fn label(&self) -> Option<&Label> {
                self.0.label()
            }

            /// Returns the human-facing label for this mesh id.
            ///
            /// Telemetry uses this for `given_name`.
            pub fn display_label(&self) -> Option<&Label> {
                self.0.display_label()
            }

            /// Returns the actor-runtime name derived from this mesh id.
            pub fn actor_name(&self) -> String {
                self.0.actor_name()
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

impl hyperactor_config::AttrValue for ActorMeshId {
    fn display(&self) -> String {
        self.to_string()
    }

    fn parse(value: &str) -> Result<Self, anyhow::Error> {
        Ok(value.parse()?)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;

    use super::*;

    #[test]
    fn test_resource_id_singleton() {
        let id = ResourceId::singleton(Label::new("local").unwrap());
        assert_eq!(*id.uid(), Uid::Singleton(Label::new("local").unwrap()));
        assert_eq!(id.label(), None);
        assert_eq!(id.to_string(), "local");
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
        assert_eq!(id.to_string(), fmt_instance_uid(0xabcdef));
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
        assert_eq!(id.to_string(), "local");
    }

    #[test]
    fn test_resource_id_display_labeled_instance() {
        let id = ResourceId::new(
            Uid::Instance(0xd5d54d7201103869),
            Some(Label::new("workers").unwrap()),
        );
        assert_eq!(
            id.to_string(),
            format!("workers-{}", fmt_instance_uid(0xd5d54d7201103869))
        );
    }

    #[test]
    fn test_resource_id_display_unlabeled_instance() {
        let id = ResourceId::new(Uid::Instance(0xd5d54d7201103869), None);
        assert_eq!(id.to_string(), fmt_instance_uid(0xd5d54d7201103869));
    }

    #[test]
    fn test_resource_id_actor_name_singleton() {
        let id = ResourceId::singleton(Label::new("local").unwrap());
        assert_eq!(id.actor_name(), "local");
    }

    #[test]
    fn test_resource_id_actor_name_labeled_instance() {
        let id = ResourceId::new(
            Uid::Instance(0xd5d54d7201103869),
            Some(Label::new("workers").unwrap()),
        );
        assert_eq!(
            id.actor_name(),
            format!("workers{}", fmt_instance_uid(0xd5d54d7201103869))
        );
    }

    #[test]
    fn test_resource_id_actor_name_unlabeled_instance() {
        let id = ResourceId::new(Uid::Instance(0xd5d54d7201103869), None);
        assert_eq!(id.actor_name(), fmt_instance_uid(0xd5d54d7201103869));
    }

    #[test]
    fn test_resource_id_debug() {
        let singleton = ResourceId::singleton(Label::new("local").unwrap());
        assert_eq!(format!("{:?}", singleton), "<local>");

        let labeled = ResourceId::new(
            Uid::Instance(0xd5d54d7201103869),
            Some(Label::new("workers").unwrap()),
        );
        assert_eq!(
            format!("{:?}", labeled),
            format!("<'workers' {}>", fmt_instance_uid(0xd5d54d7201103869))
        );

        let unlabeled = ResourceId::new(Uid::Instance(0xd5d54d7201103869), None);
        assert_eq!(
            format!("{:?}", unlabeled),
            format!("<{}>", fmt_instance_uid(0xd5d54d7201103869))
        );
    }

    #[test]
    fn test_resource_id_fromstr_singleton() {
        let parsed: ResourceId = "local".parse().unwrap();
        assert_eq!(*parsed.uid(), Uid::Singleton(Label::new("local").unwrap()));
        assert_eq!(parsed.label(), None);
    }

    #[test]
    fn test_resource_id_fromstr_labeled_instance() {
        let parsed: ResourceId = format!("workers-{}", fmt_instance_uid(0xd5d54d7201103869))
            .parse()
            .unwrap();
        assert_eq!(*parsed.uid(), Uid::Instance(0xd5d54d7201103869));
        assert_eq!(parsed.label().map(|l| l.as_str()), Some("workers"));
    }

    #[test]
    fn test_resource_id_fromstr_unlabeled_instance() {
        let parsed: ResourceId = fmt_instance_uid(0xd5d54d7201103869).parse().unwrap();
        assert_eq!(*parsed.uid(), Uid::Instance(0xd5d54d7201103869));
        assert_eq!(parsed.label(), None);
    }

    #[test]
    fn test_resource_id_fromstr_labeled_with_hyphens() {
        let parsed: ResourceId = format!("my-service-{}", fmt_instance_uid(0xd5d54d7201103869))
            .parse()
            .unwrap();
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
    fn test_mesh_id_construction() {
        let host = HostMeshId::singleton(Label::new("local").unwrap());
        assert_eq!(host.to_string(), "local");
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
