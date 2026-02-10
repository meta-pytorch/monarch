/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This crate provides hyperactor's mesh abstractions.

#![feature(assert_matches)]
#![feature(exit_status_error)]
#![feature(impl_trait_in_bindings)]
#![feature(get_disjoint_mut_helpers)]
#![feature(exact_size_is_empty)]
#![feature(async_fn_track_caller)]

pub mod actor_mesh;
pub mod admin_proxy;
pub mod alloc;
mod assign;
pub mod bootstrap;
pub mod casting;
pub mod comm;
pub mod config;
pub mod connect;
pub mod global_client;
pub mod host_mesh;
pub mod logging;
pub mod mesh;
pub mod mesh_admin;
pub mod mesh_agent;
pub mod mesh_controller;
pub mod mesh_selection;
mod metrics;
pub mod namespace;
pub mod proc_launcher;
pub mod proc_mesh;
pub mod reference;
pub mod resource;
pub mod router;
pub mod shared_cell;
pub mod shortuuid;
pub mod supervision;
#[cfg(target_os = "linux")]
mod systemd;
pub mod test_utils;
pub mod testactor;
pub mod testing;
mod testresource;
pub mod transport;
pub mod value_mesh;

use std::io;
use std::str::FromStr;

pub use actor_mesh::ActorMesh;
pub use actor_mesh::ActorMeshRef;
pub use bootstrap::Bootstrap;
pub use bootstrap::bootstrap;
pub use bootstrap::bootstrap_or_die;
pub use casting::CastError;
pub use comm::CommActor;
pub use dashmap;
use enum_as_inner::EnumAsInner;
pub use global_client::GlobalClientActor;
pub use global_client::global_root_client;
pub use host_mesh::HostMeshRef;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::ProcId;
use hyperactor::host::HostError;
use hyperactor::mailbox::MailboxSenderError;
use hyperactor::reference as hyperactor_reference;
pub use hyperactor_mesh_macros::sel;
pub use mesh::Mesh;
// Re-exported for internal test binaries that don't have ndslice as a direct dependency
pub use ndslice::extent;
use ndslice::view;
pub use proc_mesh::ProcMesh;
pub use proc_mesh::ProcMeshRef;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;
pub use value_mesh::ValueMesh;

use crate::host_mesh::HostMeshAgent;
use crate::host_mesh::HostMeshRefParseError;
use crate::host_mesh::mesh_agent::ProcState;
use crate::resource::RankedValues;
use crate::resource::Status;
use crate::shortuuid::ShortUuid;
use crate::supervision::MeshFailure;

/// A mesh of per-rank lifecycle statuses.
///
/// `StatusMesh` is `ValueMesh<Status>` and supports dense or
/// compressed encodings. Updates are applied via sparse overlays with
/// **last-writer-wins** semantics (see
/// [`ValueMesh::merge_from_overlay`]). The mesh's `Region` defines
/// the rank space; all updates must match that region.
pub type StatusMesh = ValueMesh<Status>;

/// A sparse set of `(Range<usize>, Status)` updates for a
/// [`StatusMesh`].
///
/// `StatusOverlay` carries **normalized** runs (sorted,
/// non-overlapping, and coalesced). Applying an overlay to a
/// `StatusMesh` uses **right-wins** semantics on overlap and
/// preserves first-appearance order in the compressed table.
/// Construct via `ValueOverlay::try_from_runs` after normalizing.
pub type StatusOverlay = value_mesh::ValueOverlay<Status>;

/// Errors that occur during mesh operations.
#[derive(Debug, EnumAsInner, thiserror::Error)]
pub enum Error {
    #[error("invalid mesh ref: expected {expected} ranks, but contains {actual} ranks")]
    InvalidRankCardinality { expected: usize, actual: usize },

    #[error(transparent)]
    NameParseError(#[from] NameParseError),

    #[error(transparent)]
    HostMeshRefParseError(#[from] HostMeshRefParseError),

    #[error(transparent)]
    AllocatorError(#[from] Box<crate::alloc::AllocatorError>),

    #[error(transparent)]
    ChannelError(#[from] Box<hyperactor::channel::ChannelError>),

    #[error(transparent)]
    MailboxError(#[from] Box<hyperactor::mailbox::MailboxError>),

    #[error(transparent)]
    CodecError(#[from] CodecError),

    #[error("error during mesh configuration: {0}")]
    ConfigurationError(anyhow::Error),

    // This is a temporary error to ensure we don't create unroutable
    // meshes.
    #[error("configuration error: mesh is unroutable")]
    UnroutableMesh(),

    #[error("error while calling actor {0}: {1}")]
    CallError(ActorId, anyhow::Error),

    #[error("actor not registered for type {0}")]
    ActorTypeNotRegistered(String),

    // TODO: this should be a valuemesh of statuses
    #[error("error while spawning actor {0}: {1}")]
    GspawnError(Name, String),

    #[error("error while sending message to actor {0}: {1}")]
    SendingError(ActorId, Box<MailboxSenderError>),

    #[error("error while casting message to {0}: {1}")]
    CastingError(Name, anyhow::Error),

    #[error("error configuring host mesh agent {0}: {1}")]
    HostMeshAgentConfigurationError(ActorId, String),

    #[error(
        "error creating proc (host rank {host_rank}) on host mesh agent {mesh_agent}, state: {state}"
    )]
    ProcCreationError {
        state: Box<resource::State<ProcState>>,
        host_rank: usize,
        mesh_agent: ActorRef<HostMeshAgent>,
    },

    #[error(
        "error spawning proc mesh: statuses: {}",
        RankedValues::invert(statuses)
    )]
    ProcSpawnError { statuses: RankedValues<Status> },

    #[error(
        "error spawning actor mesh: statuses: {}",
        RankedValues::invert(statuses)
    )]
    ActorSpawnError { statuses: RankedValues<Status> },

    #[error(
        "error stopping actor mesh: statuses: {}",
        RankedValues::invert(statuses)
    )]
    ActorStopError { statuses: RankedValues<Status> },

    #[error("error spawning actor: {0}")]
    SingletonActorSpawnError(anyhow::Error),

    #[error("error spawning controller actor for mesh {0}: {1}")]
    ControllerActorSpawnError(Name, anyhow::Error),

    #[error("proc {0} must be direct-addressable")]
    RankedProc(ProcId),

    #[error("{0}")]
    Supervision(Box<MeshFailure>),

    #[error("error: {0} does not exist")]
    NotExist(Name),

    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    Host(#[from] HostError),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Errors that occur during serialization and deserialization.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error(transparent)]
    BincodeError(#[from] Box<bincode::Error>),
    #[error(transparent)]
    JsonError(#[from] Box<serde_json::Error>),
    #[error(transparent)]
    Base64Error(#[from] Box<base64::DecodeError>),
    #[error(transparent)]
    Utf8Error(#[from] Box<std::str::Utf8Error>),
}

impl From<bincode::Error> for Error {
    fn from(e: bincode::Error) -> Self {
        Error::CodecError(Box::new(e).into())
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::CodecError(Box::new(e).into())
    }
}

impl From<base64::DecodeError> for Error {
    fn from(e: base64::DecodeError) -> Self {
        Error::CodecError(Box::new(e).into())
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(e: std::str::Utf8Error) -> Self {
        Error::CodecError(Box::new(e).into())
    }
}

impl From<crate::alloc::AllocatorError> for Error {
    fn from(e: crate::alloc::AllocatorError) -> Self {
        Error::AllocatorError(Box::new(e))
    }
}

impl From<hyperactor::channel::ChannelError> for Error {
    fn from(e: hyperactor::channel::ChannelError) -> Self {
        Error::ChannelError(Box::new(e))
    }
}

impl From<hyperactor::mailbox::MailboxError> for Error {
    fn from(e: hyperactor::mailbox::MailboxError) -> Self {
        Error::MailboxError(Box::new(e))
    }
}

impl From<view::InvalidCardinality> for Error {
    fn from(e: view::InvalidCardinality) -> Self {
        Error::InvalidRankCardinality {
            expected: e.expected,
            actual: e.actual,
        }
    }
}

/// The type of result used in `hyperactor_mesh`.
pub type Result<T> = std::result::Result<T, Error>;

/// Names are used to identify objects in the system. They have a user-provided name,
/// and a unique UUID.
///
/// Names have a concrete syntax--`{name}-{uuid}`--printed by `Display` and parsed by `FromStr`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Named, EnumAsInner)]
pub enum Name {
    /// Normal names for most actors.
    Suffixed(String, ShortUuid),
    /// Reserved names for system actors without UUIDs.
    Reserved(String),
}
wirevalue::register_type!(Name);

// The delimiter between the name and the uuid when a Name::Suffixed is stringified.
// Actor names must be parseable as an actor identifier. We do not allow this delimiter
// in reserved names so that these names parse unambiguously.
static NAME_SUFFIX_DELIMITER: &str = "-";

impl Name {
    /// Create a new `Name` from a user-provided base name.
    pub fn new(name: impl Into<String>) -> Result<Self> {
        Ok(Self::new_with_uuid(name, Some(ShortUuid::generate()))?)
    }

    /// Create a Reserved `Name` with no uuid. Only for use by system actors.
    pub fn new_reserved(name: impl Into<String>) -> Result<Self> {
        Ok(Self::new_with_uuid(name, None)?)
    }

    fn new_with_uuid(
        name: impl Into<String>,
        uuid: Option<ShortUuid>,
    ) -> std::result::Result<Self, NameParseError> {
        let mut name = name.into();
        if name.is_empty() {
            name = "unnamed".to_string();
        }
        if !hyperactor_reference::is_valid_ident(&name) {
            return Err(NameParseError::InvalidName(name));
        }
        if let Some(uuid) = uuid {
            Ok(Self::Suffixed(name, uuid))
        } else {
            Ok(Self::Reserved(name))
        }
    }

    /// The name portion of this `Name`.
    pub fn name(&self) -> &str {
        match self {
            Self::Suffixed(n, _) => n,
            Self::Reserved(n) => n,
        }
    }

    /// The UUID portion of this `Name`.
    /// Only Some for Name::Suffixed, if called on Name::Reserved it'll be None.
    pub fn uuid(&self) -> Option<&ShortUuid> {
        match self {
            Self::Suffixed(_, uuid) => Some(uuid),
            Self::Reserved(_) => None,
        }
    }
}

impl Serialize for Name {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Consider doing this only when `serializer.is_human_readable()`:
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Name {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Name::from_str(&s).map_err(serde::de::Error::custom)
    }
}

/// Errors that occur when parsing names.
#[derive(thiserror::Error, Debug)]
pub enum NameParseError {
    #[error("invalid name: missing name")]
    MissingName,

    #[error("invalid name: missing uuid")]
    MissingUuid,

    /// Strictly speaking Monarch identifier also supports other characters. But
    /// to avoid confusion for the user, we only state intuitive characters here
    /// so the error message is more actionable.
    #[error(
        "invalid name '{0}': names must contain only alphanumeric characters \
        and underscores, and must start with a letter or underscore"
    )]
    InvalidName(String),

    #[error(transparent)]
    InvalidUuid(#[from] <ShortUuid as FromStr>::Err),

    #[error("invalid name: missing separator")]
    MissingSeparator,
}

impl FromStr for Name {
    type Err = NameParseError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        // The delimiter ('-') is allowable in elements, but not identifiers;
        // thus splitting on this unambiguously parses suffixed and reserved names.
        if let Some((name, uuid)) = s.split_once(NAME_SUFFIX_DELIMITER) {
            if name.is_empty() {
                return Err(NameParseError::MissingName);
            }
            if uuid.is_empty() {
                return Err(NameParseError::MissingName);
            }

            Name::new_with_uuid(name.to_string(), Some(uuid.parse()?))
        } else {
            if s.is_empty() {
                return Err(NameParseError::MissingName);
            }
            Name::new_with_uuid(s, None)
        }
    }
}

impl std::fmt::Display for Name {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Suffixed(n, uuid) => {
                write!(f, "{}{}", n, NAME_SUFFIX_DELIMITER)?;
                uuid.format(f, true /*raw*/)
            }
            Self::Reserved(n) => write!(f, "{}", n),
        }
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn basic() {
        use ndslice::selection::dsl;
        use ndslice::selection::structurally_equal;

        let actual = sel!(*, 0:4, *);
        let expected = dsl::all(dsl::range(
            ndslice::shape::Range(0, Some(4), 1),
            dsl::all(dsl::true_()),
        ));
        assert!(structurally_equal(&actual, &expected));
    }

    #[cfg(FALSE)]
    #[test]
    fn shouldnt_compile() {
        let _ = sel!(foobar);
    }
    // error: sel! parse failed: unexpected token: Ident { sym: foobar, span: #0 bytes(605..611) }
    //   --> fbcode/monarch/hyperactor_mesh_macros/tests/basic.rs:19:13
    //    |
    // 19 |     let _ = sel!(foobar);
    //    |             ^^^^^^^^^^^^ in this macro invocation
    //   --> fbcode/monarch/hyperactor_mesh_macros/src/lib.rs:12:1
    //    |
    //    = note: in this expansion of `sel!`

    use std::str::FromStr;

    use hyperactor_mesh_macros::sel;
    use ndslice::assert_round_trip;
    use ndslice::assert_structurally_eq;
    use ndslice::selection::Selection;

    macro_rules! assert_round_trip_match {
        ($left:expr, $right:expr) => {{
            assert_structurally_eq!($left, $right);
            assert_round_trip!($left);
            assert_round_trip!($right);
        }};
    }

    #[test]
    fn token_parser() {
        use ndslice::selection::dsl::*;
        use ndslice::shape;

        assert_round_trip_match!(all(true_()), sel!(*));
        assert_round_trip_match!(range(3, true_()), sel!(3));
        assert_round_trip_match!(range(1..4, true_()), sel!(1:4));
        assert_round_trip_match!(all(range(1..4, true_())), sel!(*, 1:4));
        assert_round_trip_match!(range(shape::Range(0, None, 1), true_()), sel!(:));
        assert_round_trip_match!(any(true_()), sel!(?));
        assert_round_trip_match!(any(range(1..4, all(true_()))), sel!(?, 1:4, *));
        assert_round_trip_match!(union(range(0, true_()), range(1, true_())), sel!(0 | 1));
        assert_round_trip_match!(
            intersection(range(0..4, true_()), range(2..6, true_())),
            sel!(0:4 & 2:6)
        );
        assert_round_trip_match!(range(shape::Range(0, None, 1), true_()), sel!(:));
        assert_round_trip_match!(all(true_()), sel!(*));
        assert_round_trip_match!(any(true_()), sel!(?));
        assert_round_trip_match!(all(all(all(true_()))), sel!(*, *, *));
        assert_round_trip_match!(intersection(all(true_()), all(true_())), sel!(* & *));
        assert_round_trip_match!(
            all(all(union(
                range(0..2, true_()),
                range(shape::Range(6, None, 1), true_())
            ))),
            sel!(*, *, (:2|6:))
        );
        assert_round_trip_match!(
            all(all(range(shape::Range(1, None, 2), true_()))),
            sel!(*, *, 1::2)
        );
        assert_round_trip_match!(
            range(
                shape::Range(0, Some(1), 1),
                any(range(shape::Range(0, Some(4), 1), true_()))
            ),
            sel!(0, ?, :4)
        );
        assert_round_trip_match!(range(shape::Range(1, Some(4), 2), true_()), sel!(1:4:2));
        assert_round_trip_match!(range(shape::Range(0, None, 2), true_()), sel!(::2));
        assert_round_trip_match!(
            union(range(0..4, true_()), range(4..8, true_())),
            sel!(0:4 | 4:8)
        );
        assert_round_trip_match!(
            intersection(range(0..4, true_()), range(2..6, true_())),
            sel!(0:4 & 2:6)
        );
        assert_round_trip_match!(
            all(union(range(1..4, all(true_())), range(5..6, all(true_())))),
            sel!(*, (1:4 | 5:6), *)
        );
        assert_round_trip_match!(
            range(
                0,
                intersection(
                    range(1..4, range(7, true_())),
                    range(2..5, range(7, true_()))
                )
            ),
            sel!(0, (1:4 & 2:5), 7)
        );
        assert_round_trip_match!(
            all(all(union(
                union(range(0..2, true_()), range(4..6, true_())),
                range(shape::Range(6, None, 1), true_())
            ))),
            sel!(*, *, (:2 | 4:6 | 6:))
        );
        assert_round_trip_match!(intersection(all(true_()), all(true_())), sel!(* & *));
        assert_round_trip_match!(union(all(true_()), all(true_())), sel!(* | *));
        assert_round_trip_match!(
            intersection(
                range(0..2, true_()),
                union(range(1, true_()), range(2, true_()))
            ),
            sel!(0:2 & (1 | 2))
        );
        assert_round_trip_match!(
            all(all(intersection(
                range(1..2, true_()),
                range(2..3, true_())
            ))),
            sel!(*,*,(1:2&2:3))
        );
        assert_round_trip_match!(
            intersection(all(all(all(true_()))), all(all(all(true_())))),
            sel!((*,*,*) & (*,*,*))
        );
        assert_round_trip_match!(
            intersection(
                range(0, all(all(true_()))),
                range(0, union(range(1, all(true_())), range(3, all(true_()))))
            ),
            sel!((0, *, *) & (0, (1 | 3), *))
        );
        assert_round_trip_match!(
            intersection(
                range(0, all(all(true_()))),
                range(
                    0,
                    union(
                        range(1, range(2..5, true_())),
                        range(3, range(2..5, true_()))
                    )
                )
            ),
            sel!((0, *, *) & (0, (1 | 3), 2:5))
        );
        assert_round_trip_match!(all(true_()), sel!((*)));
        assert_round_trip_match!(range(1..4, range(2, true_())), sel!(((1:4), 2)));
        assert_round_trip_match!(sel!(1:4 & 5:6 | 7:8), sel!((1:4 & 5:6) | 7:8));
        assert_round_trip_match!(
            union(
                intersection(all(all(true_())), all(all(true_()))),
                all(all(true_()))
            ),
            sel!((*,*) & (*,*) | (*,*))
        );
        assert_round_trip_match!(all(true_()), sel!(*));
        assert_round_trip_match!(sel!(((1:4))), sel!(1:4));
        assert_round_trip_match!(sel!(*, (*)), sel!(*, *));
        assert_round_trip_match!(
            intersection(
                range(0, range(1..4, true_())),
                range(0, union(range(2, all(true_())), range(3, all(true_()))))
            ),
            sel!((0,1:4)&(0,(2|3),*))
        );

        //assert_round_trip_match!(true_(), sel!(foo)); // sel! macro: parse error: Parsing Error: Error { input: "foo", code: Tag }

        assert_round_trip_match!(
            sel!(0 & (0, (1|3), *)),
            intersection(
                range(0, true_()),
                range(0, union(range(1, all(true_())), range(3, all(true_()))))
            )
        );
        assert_round_trip_match!(
            sel!(0 & (0, (3|1), *)),
            intersection(
                range(0, true_()),
                range(0, union(range(3, all(true_())), range(1, all(true_()))))
            )
        );
        assert_round_trip_match!(
            sel!((*, *, *) & (*, *, (2 | 4))),
            intersection(
                all(all(all(true_()))),
                all(all(union(range(2, true_()), range(4, true_()))))
            )
        );
        assert_round_trip_match!(
            sel!((*, *, *) & (*, *, (4 | 2))),
            intersection(
                all(all(all(true_()))),
                all(all(union(range(4, true_()), range(2, true_()))))
            )
        );
        assert_round_trip_match!(
            sel!((*, (1|2)) & (*, (2|1))),
            intersection(
                all(union(range(1, true_()), range(2, true_()))),
                all(union(range(2, true_()), range(1, true_())))
            )
        );
        assert_round_trip_match!(
            sel!((*, *, *) & *),
            intersection(all(all(all(true_()))), all(true_()))
        );
        assert_round_trip_match!(
            sel!(* & (*, *, *)),
            intersection(all(true_()), all(all(all(true_()))))
        );

        assert_round_trip_match!(
            sel!( (*, *, *) & ((*, *, *) & (*, *, *)) ),
            intersection(
                all(all(all(true_()))),
                intersection(all(all(all(true_()))), all(all(all(true_()))))
            )
        );
        assert_round_trip_match!(
            sel!((1, *, *) | (0 & (0, 3, *))),
            union(
                range(1, all(all(true_()))),
                intersection(range(0, true_()), range(0, range(3, all(true_()))))
            )
        );
        assert_round_trip_match!(
            sel!(((0, *)| (1, *)) & ((1, *) | (0, *))),
            intersection(
                union(range(0, all(true_())), range(1, all(true_()))),
                union(range(1, all(true_())), range(0, all(true_())))
            )
        );
        assert_round_trip_match!(sel!(*, 8:8), all(range(8..8, true_())));
        assert_round_trip_match!(
            sel!((*, 1) & (*, 8 : 8)),
            intersection(all(range(1..2, true_())), all(range(8..8, true_())))
        );
        assert_round_trip_match!(
            sel!((*, 8 : 8) | (*, 1)),
            union(all(range(8..8, true_())), all(range(1..2, true_())))
        );
        assert_round_trip_match!(
            sel!((*, 1) | (*, 2:8)),
            union(all(range(1..2, true_())), all(range(2..8, true_())))
        );
        assert_round_trip_match!(
            sel!((*, *, *) & (*, *, 2:8)),
            intersection(all(all(all(true_()))), all(all(range(2..8, true_()))))
        );
    }

    #[test]
    fn test_name_unique() {
        use super::Name;
        assert_ne!(Name::new("foo").unwrap(), Name::new("foo").unwrap());
        let name = Name::new("foo").unwrap();
        assert_eq!(name, name);
    }

    #[test]
    fn test_name_roundtrip() {
        use super::Name;
        use super::ShortUuid;
        let uuid = "111111111111".parse::<ShortUuid>().unwrap();
        let name = Name::new_with_uuid("foo", Some(uuid)).unwrap();
        let str = name.to_string();
        assert_eq!(str, "foo-111111111111");
        assert_eq!(name, Name::from_str(&str).unwrap());
    }

    #[test]
    fn test_name_roundtrip_with_underscore() {
        use super::Name;
        use super::ShortUuid;
        // A ShortUuid may have an underscore prefix if the first character is a digit.
        // Make sure this doesn't impact parsing.
        let uuid = "_1a2b3c4d5e6f".parse::<ShortUuid>().unwrap();
        let name = Name::new_with_uuid("foo", Some(uuid)).unwrap();
        let str = name.to_string();
        // Leading underscore is stripped as not needed.
        assert_eq!(str, "foo-1a2b3c4d5e6f");
        assert_eq!(name, Name::from_str(&str).unwrap());
    }

    #[test]
    fn test_name_roundtrip_random() {
        use super::Name;
        let name = Name::new("foo").unwrap();
        assert_eq!(name, Name::from_str(&name.to_string()).unwrap());
    }

    #[test]
    fn test_name_roundtrip_reserved() {
        use super::Name;
        let name = Name::new_reserved("foo").unwrap();
        let str = name.to_string();
        assert_eq!(str, "foo");
        assert_eq!(name, Name::from_str(&str).unwrap());
    }

    #[test]
    fn test_name_parse() {
        use super::Name;
        // Multiple underscores are allowed in the name, as ShortUuid will choose
        // the part after the last underscore.
        let name = Name::from_str("foo_bar_1a2b3c4d5e6f").unwrap();
        assert_eq!(format!("{}", name), "foo_bar_1a2b3c4d5e6f");
    }

    #[test]
    fn test_invalid() {
        use super::Name;
        // We assign "unnamed" to empty names.
        assert!(Name::new("").is_ok());
        // These are not valid identifiers:
        assert!(Name::new("foo-").is_err());
        assert!(Name::new("foo-bar").is_err());
    }
}
