/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Mesh identity types.
//!
//! [`ResourceId`] is the common control-plane identifier for mesh resources.
//! The mesh-specific newtypes [`ActorMeshId`], [`ProcMeshId`], and
//! [`HostMeshId`] provide type safety at mesh struct boundaries while
//! converting freely to [`ResourceId`] for resource message plumbing.
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
//! - The runtime actor id is carried as `ActorMeshId::uid()` by
//!   `ProcRef::actor_id()` in `proc_mesh.rs` and by `ProcAgent` when it calls
//!   `remote.gspawn(...)`.
//! - The runtime proc name is `ResourceId::to_string()`, which is consumed by
//!   `HostRef::named_proc()` in `host_mesh.rs` and by `HostAgent` when it
//!   spawns a proc on a host.
//! - Telemetry uses `display_label()` for human-facing `given_name`, while
//!   `to_string()` is emitted as the stable `full_name`.
//!
//! # String formats
//!
//! `ResourceId` has two externally visible string forms:
//!
//! - Singleton: `label`
//! - Labeled instance: `label-uid58`
//!
//! Here `uid58` is the base58 instance component produced by [`Uid::Instance`],
//! without angle brackets. Instances always render with a label. When an
//! instance has no explicit label metadata, the formatter uses the id type's
//! default label, such as `proc`, `actor`, or `resource`.
//!
//! Identity is uid-only: labels are descriptive metadata and do not
//! participate in `Eq`, `Hash`, or `Ord`.

use std::cmp::Ordering;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::str::FromStr;

use hyperactor::ActorAddr;
use hyperactor::ActorId;
use hyperactor::Location;
use hyperactor::ProcAddr;
use hyperactor::ProcId;
use hyperactor::id::Label;
use hyperactor::id::LabelError;
use hyperactor::id::Uid;
use hyperactor::id::UidParseError;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

const RESOURCE_ID_DEFAULT_LABEL: &str = "resource";
const HOST_MESH_ID_DEFAULT_LABEL: &str = "host";
const PROC_MESH_ID_DEFAULT_LABEL: &str = "proc";
const ACTOR_MESH_ID_DEFAULT_LABEL: &str = "actor";

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
/// Identity (Eq, Hash, Ord) is determined by the underlying [`Uid`].
#[derive(Clone, Serialize, Deserialize, Named)]
pub struct ResourceId(Uid);
wirevalue::register_type!(ResourceId);

impl ResourceId {
    /// Create a [`ResourceId`] with explicit uid and label.
    pub fn new(uid: Uid, label: Option<Label>) -> Self {
        Self(uid.with_label(label))
    }

    /// Create a singleton [`ResourceId`] identified by label.
    /// The label becomes the uid; no separate label metadata is stored.
    pub fn singleton(label: Label) -> Self {
        Self(Uid::Singleton(label))
    }

    /// Create an instance [`ResourceId`] with a random uid and the given label.
    pub fn instance(label: Label) -> Self {
        Self(Uid::instance_labeled(label))
    }

    /// Create a unique [`ResourceId`] with a random uid and the given label.
    pub fn unique(label: Label) -> Self {
        Self::instance(label)
    }

    /// Create a resource id from a resource-name string.
    ///
    /// This accepts the mesh resource-id grammar, falling back to a stripped
    /// singleton label for legacy call sites that pass arbitrary names.
    pub fn from_name(name: impl AsRef<str>) -> Self {
        name.as_ref()
            .parse()
            .unwrap_or_else(|_| Self::singleton(Label::strip(name.as_ref())))
    }

    /// Returns the uid.
    pub fn uid(&self) -> &Uid {
        &self.0
    }

    /// Returns the explicit label metadata, if any.
    pub fn label(&self) -> Option<&Label> {
        match &self.0 {
            Uid::Singleton(_) => None,
            Uid::Instance(_, label) => label.as_ref(),
        }
    }

    /// Returns the human-facing label for this resource id.
    ///
    /// This is the explicit label metadata for instances, or the singleton
    /// label embedded in the uid. Telemetry uses this for `given_name`.
    pub fn display_label(&self) -> Option<&Label> {
        self.0.label()
    }

    /// Converts this resource id into a hyperactor proc id.
    pub fn proc_id(&self) -> ProcId {
        ProcId::new(self.0.clone(), None)
    }

    /// Converts this resource id into a hyperactor proc addr at `location`.
    pub fn proc_addr(&self, location: impl Into<Location>) -> ProcAddr {
        ProcAddr::new(self.proc_id(), location.into())
    }

    /// Creates a hyperactor proc addr from a mesh resource-name string.
    pub fn proc_addr_from_name(location: impl Into<Location>, name: impl AsRef<str>) -> ProcAddr {
        Self::from_name(name).proc_addr(location)
    }
}

impl From<ResourceId> for Uid {
    fn from(id: ResourceId) -> Self {
        id.0
    }
}

impl From<&ResourceId> for Uid {
    fn from(id: &ResourceId) -> Self {
        id.0.clone()
    }
}

impl From<ResourceId> for ProcId {
    fn from(id: ResourceId) -> Self {
        Self::new(id.0, None)
    }
}

impl From<&ResourceId> for ProcId {
    fn from(id: &ResourceId) -> Self {
        id.proc_id()
    }
}

impl From<ProcId> for ResourceId {
    fn from(id: ProcId) -> Self {
        Self(id.uid().clone())
    }
}

impl From<&ProcId> for ResourceId {
    fn from(id: &ProcId) -> Self {
        Self(id.uid().clone())
    }
}

impl From<&ProcAddr> for ResourceId {
    fn from(addr: &ProcAddr) -> Self {
        Self::from(addr.id())
    }
}

impl PartialEq for ResourceId {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for ResourceId {}

impl Hash for ResourceId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl PartialOrd for ResourceId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ResourceId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

fn fmt_instance_uid(uid: u64) -> String {
    Uid::Instance(uid, None)
        .instance_uid_base58()
        .expect("instance uid should have base58 representation")
}

fn parse_instance_uid(s: &str) -> Result<u64, UidParseError> {
    Uid::parse_instance_uid_base58(s)
}

fn fmt_id_component(
    f: &mut fmt::Formatter<'_>,
    uid: &Uid,
    label: Option<&Label>,
    default_instance_label: &str,
) -> fmt::Result {
    match uid {
        Uid::Singleton(singleton) => write!(f, "{singleton}"),
        Uid::Instance(uid, _) => match label {
            Some(label) => write!(f, "{label}-{}", fmt_instance_uid(*uid)),
            None => write!(f, "{}-{}", default_instance_label, fmt_instance_uid(*uid)),
        },
    }
}

impl fmt::Display for ResourceId {
    /// Formats the canonical control-plane string form of this resource id.
    ///
    /// This string is used for resource message keys, proc names on hosts,
    /// and telemetry `full_name`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_id_component(f, &self.0, self.label(), RESOURCE_ID_DEFAULT_LABEL)
    }
}

impl fmt::Debug for ResourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.0, self.label()) {
            (Uid::Singleton(label), _) => write!(f, "<{label}>"),
            (Uid::Instance(uid, _), Some(label)) => {
                write!(f, "<'{label}' {}>", fmt_instance_uid(*uid))
            }
            (Uid::Instance(uid, _), None) => write!(f, "<{}>", fmt_instance_uid(*uid)),
        }
    }
}

fn parse_id_component(s: &str, default_instance_label: &str) -> Result<Uid, ResourceIdParseError> {
    if let Some(split) = s.rfind('-') {
        let label_part = &s[..split];
        let uid_part = &s[split + 1..];
        if uid_part.len() >= 8
            && let (Ok(label), Ok(uid)) = (Label::new(label_part), parse_instance_uid(uid_part))
        {
            if label.as_str() == default_instance_label {
                return Ok(Uid::Instance(uid, None));
            }
            return Ok(Uid::Instance(uid, Some(label)));
        }
    }

    let label = Label::new(s)?;
    Ok(Uid::Singleton(label))
}

impl FromStr for ResourceId {
    type Err = ResourceIdParseError;

    /// Parses the canonical resource-id string forms accepted by the mesh
    /// control plane.
    ///
    /// Accepted inputs are:
    /// - `label` for singletons
    /// - `label-uid58` for labeled instances
    ///
    /// `resource-uid58` parses as an instance without explicit label metadata.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(parse_id_component(s, RESOURCE_ID_DEFAULT_LABEL)?))
    }
}

macro_rules! define_mesh_id {
    ($(#[$meta:meta])* $name:ident, $default_label:expr) => {
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

            /// Create an instance mesh id with a random uid and the given label.
            pub fn instance(label: Label) -> Self {
                Self(ResourceId::instance(label))
            }

            /// Create a unique mesh id with a random uid and the given label.
            pub fn unique(label: Label) -> Self {
                Self::instance(label)
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

            /// Returns the inner [`ResourceId`].
            pub fn resource_id(&self) -> &ResourceId {
                &self.0
            }

            /// Returns the default instance label for this mesh id type.
            pub fn default_instance_label() -> &'static str {
                $default_label
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
                fmt_id_component(f, self.uid(), self.label(), Self::default_instance_label())
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
                Ok(Self(ResourceId(parse_id_component(
                    s,
                    Self::default_instance_label(),
                )?)))
            }
        }
    };
}

define_mesh_id!(
    /// Identifies a host mesh.
    HostMeshId,
    HOST_MESH_ID_DEFAULT_LABEL
);

define_mesh_id!(
    /// Identifies a proc mesh.
    ProcMeshId,
    PROC_MESH_ID_DEFAULT_LABEL
);

define_mesh_id!(
    /// Identifies an actor mesh.
    ActorMeshId,
    ACTOR_MESH_ID_DEFAULT_LABEL
);

impl ProcMeshId {
    /// Converts this mesh id into a hyperactor proc id.
    pub fn proc_id(&self) -> ProcId {
        self.resource_id().proc_id()
    }

    /// Converts this mesh id into a hyperactor proc addr at `location`.
    pub fn proc_addr(&self, location: impl Into<Location>) -> ProcAddr {
        self.resource_id().proc_addr(location)
    }
}

impl ActorMeshId {
    /// Converts this mesh id into a hyperactor actor id within `proc_id`.
    pub fn actor_id(&self, proc_id: ProcId) -> ActorId {
        ActorId::new(self.uid().clone(), proc_id, None)
    }

    /// Converts this mesh id into a hyperactor actor addr within `proc_addr`.
    pub fn actor_addr(&self, proc_addr: ProcAddr) -> ActorAddr {
        ActorAddr::new_from_uid(proc_addr, self.uid().clone())
    }
}

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
        assert!(id.uid().is_instance());
        assert_eq!(id.label().map(|l| l.as_str()), Some("workers"));
    }

    #[test]
    fn test_resource_id_unlabeled() {
        let id = ResourceId::new(Uid::Instance(0xabcdef, None), None);
        assert_eq!(
            id.to_string(),
            format!("resource-{}", fmt_instance_uid(0xabcdef))
        );
        assert_eq!(id.label(), None);
    }

    #[test]
    fn test_resource_id_eq_by_uid_only() {
        let uid = Uid::Instance(0x42, None);
        let a = ResourceId::new(uid.clone(), Some(Label::new("alpha").unwrap()));
        let b = ResourceId::new(uid, Some(Label::new("beta").unwrap()));
        assert_eq!(a, b);
    }

    #[test]
    fn test_resource_id_neq_different_uid() {
        let a = ResourceId::new(Uid::Instance(1, None), Some(Label::new("same").unwrap()));
        let b = ResourceId::new(Uid::Instance(2, None), Some(Label::new("same").unwrap()));
        assert_ne!(a, b);
    }

    #[test]
    fn test_resource_id_hash_by_uid_only() {
        let uid = Uid::Instance(0x42, None);
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
        let a = ResourceId::new(Uid::Instance(1, None), Some(Label::new("zzz").unwrap()));
        let b = ResourceId::new(Uid::Instance(2, None), Some(Label::new("aaa").unwrap()));
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
            Uid::Instance(0xd5d54d7201103869, None),
            Some(Label::new("workers").unwrap()),
        );
        assert_eq!(
            id.to_string(),
            format!("workers-{}", fmt_instance_uid(0xd5d54d7201103869))
        );
    }

    #[test]
    fn test_resource_id_display_unlabeled_instance() {
        let id = ResourceId::new(Uid::Instance(0xd5d54d7201103869, None), None);
        assert_eq!(
            id.to_string(),
            format!("resource-{}", fmt_instance_uid(0xd5d54d7201103869))
        );
    }

    #[test]
    fn test_resource_id_debug() {
        let singleton = ResourceId::singleton(Label::new("local").unwrap());
        assert_eq!(format!("{:?}", singleton), "<local>");

        let labeled = ResourceId::new(
            Uid::Instance(0xd5d54d7201103869, None),
            Some(Label::new("workers").unwrap()),
        );
        assert_eq!(
            format!("{:?}", labeled),
            format!("<'workers' {}>", fmt_instance_uid(0xd5d54d7201103869))
        );

        let unlabeled = ResourceId::new(Uid::Instance(0xd5d54d7201103869, None), None);
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
    fn test_resource_id_fromstr_base58_like_singleton() {
        let parsed: ResourceId = "service".parse().unwrap();
        assert_eq!(
            *parsed.uid(),
            Uid::Singleton(Label::new("service").unwrap())
        );
        assert_eq!(parsed.label(), None);
    }

    #[test]
    fn test_resource_id_fromstr_short_suffix_singleton() {
        let parsed: ResourceId = "env-vars".parse().unwrap();
        assert_eq!(
            *parsed.uid(),
            Uid::Singleton(Label::new("env-vars").unwrap())
        );
        assert_eq!(parsed.label(), None);
    }

    #[test]
    fn test_resource_id_fromstr_labeled_instance() {
        let parsed: ResourceId = format!("workers-{}", fmt_instance_uid(0xd5d54d7201103869))
            .parse()
            .unwrap();
        assert_eq!(
            *parsed.uid(),
            Uid::Instance(0xd5d54d7201103869, Some(Label::new("workers").unwrap()))
        );
        assert_eq!(parsed.label().map(|l| l.as_str()), Some("workers"));
    }

    #[test]
    fn test_resource_id_fromstr_default_labeled_instance() {
        let parsed: ResourceId = format!("resource-{}", fmt_instance_uid(0xd5d54d7201103869))
            .parse()
            .unwrap();
        assert_eq!(*parsed.uid(), Uid::Instance(0xd5d54d7201103869, None));
        assert_eq!(parsed.label(), None);
    }

    #[test]
    fn test_resource_id_fromstr_rejects_unlabeled_instance() {
        let result: Result<ResourceId, _> =
            format!("<{}>", fmt_instance_uid(0xd5d54d7201103869)).parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_resource_id_fromstr_labeled_with_hyphens() {
        let parsed: ResourceId = format!("my-service-{}", fmt_instance_uid(0xd5d54d7201103869))
            .parse()
            .unwrap();
        assert_eq!(
            *parsed.uid(),
            Uid::Instance(0xd5d54d7201103869, Some(Label::new("my-service").unwrap()))
        );
        assert_eq!(parsed.label().map(|l| l.as_str()), Some("my-service"));
    }

    #[test]
    fn test_resource_id_display_fromstr_roundtrip() {
        let cases = vec![
            ResourceId::singleton(Label::new("local").unwrap()),
            ResourceId::new(
                Uid::Instance(0xd5d54d7201103869, None),
                Some(Label::new("workers").unwrap()),
            ),
            ResourceId::new(Uid::Instance(0xd5d54d7201103869, None), None),
            ResourceId::new(
                Uid::Instance(0xd5d54d7201103869, None),
                Some(Label::new("my-service").unwrap()),
            ),
            ResourceId::new(
                Uid::Instance(0xd5d54d7201103869, None),
                Some(Label::new("a").unwrap()),
            ),
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
                Uid::Instance(0xabcdef, None),
                Some(Label::new("workers").unwrap()),
            ),
            ResourceId::new(Uid::Instance(0xabcdef, None), None),
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
        assert!(proc_.uid().is_instance());
        assert_eq!(proc_.label().map(|l| l.as_str()), Some("workers"));

        let actor = ActorMeshId::unique(Label::new("trainers").unwrap());
        assert!(actor.uid().is_instance());
        assert_eq!(actor.label().map(|l| l.as_str()), Some("trainers"));
    }

    #[test]
    fn test_mesh_id_eq_by_uid_only() {
        let uid = Uid::Instance(0x42, None);
        let a = HostMeshId::new(uid.clone(), Some(Label::new("alpha").unwrap()));
        let b = HostMeshId::new(uid, Some(Label::new("beta").unwrap()));
        assert_eq!(a, b);
    }

    #[test]
    fn test_mesh_id_display_fromstr_roundtrip() {
        let ids: Vec<HostMeshId> = vec![
            HostMeshId::singleton(Label::new("local").unwrap()),
            HostMeshId::new(
                Uid::Instance(0xd5d54d7201103869, None),
                Some(Label::new("workers").unwrap()),
            ),
            HostMeshId::new(Uid::Instance(0xd5d54d7201103869, None), None),
        ];
        for id in ids {
            let s = id.to_string();
            let parsed: HostMeshId = s.parse().unwrap();
            assert_eq!(id, parsed, "round-trip failed for {s}");
        }
    }

    #[test]
    fn test_typed_mesh_id_display_uses_type_default_label() {
        let uid = Uid::Instance(0xd5d54d7201103869, None);
        assert_eq!(
            HostMeshId::new(uid.clone(), None).to_string(),
            format!("host-{}", fmt_instance_uid(0xd5d54d7201103869))
        );
        assert_eq!(
            ProcMeshId::new(uid.clone(), None).to_string(),
            format!("proc-{}", fmt_instance_uid(0xd5d54d7201103869))
        );
        assert_eq!(
            ActorMeshId::new(uid, None).to_string(),
            format!("actor-{}", fmt_instance_uid(0xd5d54d7201103869))
        );
    }

    #[test]
    fn test_typed_mesh_id_parse_omits_type_default_label() {
        let proc: ProcMeshId = format!("proc-{}", fmt_instance_uid(0xd5d54d7201103869))
            .parse()
            .unwrap();
        assert_eq!(*proc.uid(), Uid::Instance(0xd5d54d7201103869, None));
        assert_eq!(proc.label(), None);

        let actor: ActorMeshId = format!("actor-{}", fmt_instance_uid(0xd5d54d7201103869))
            .parse()
            .unwrap();
        assert_eq!(*actor.uid(), Uid::Instance(0xd5d54d7201103869, None));
        assert_eq!(actor.label(), None);
    }

    #[test]
    fn test_typed_mesh_id_parse_preserves_non_default_label() {
        let proc: ProcMeshId = format!("worker-{}", fmt_instance_uid(0xd5d54d7201103869))
            .parse()
            .unwrap();
        assert_eq!(
            *proc.uid(),
            Uid::Instance(0xd5d54d7201103869, Some(Label::new("worker").unwrap()))
        );
        assert_eq!(proc.label().map(|l| l.as_str()), Some("worker"));
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
        let host = HostMeshId::new(
            Uid::Instance(0xabcdef, None),
            Some(Label::new("test").unwrap()),
        );
        let resource = ResourceId::new(
            Uid::Instance(0xabcdef, None),
            Some(Label::new("test").unwrap()),
        );

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
