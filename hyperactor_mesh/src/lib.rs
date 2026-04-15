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
// EnumAsInner generates code that triggers a false positive
// unused_assignments lint on struct variant fields. #[allow] on the
// enum itself doesn't propagate into derive-macro-generated code, so
// the suppression must be at module scope.
#![allow(unused_assignments)]

pub mod actor_mesh;
pub mod alloc;
mod assign;
pub mod bootstrap;
pub mod casting;
pub mod comm;
pub mod config;
pub mod config_dump;
pub mod connect;
pub mod global_context;
pub mod host_mesh;
pub mod introspect;
pub mod logging;
pub mod mesh;
pub mod mesh_admin;
pub mod mesh_admin_client;
pub mod mesh_controller;
pub mod mesh_id;
pub mod mesh_selection;
mod metrics;
pub mod proc_agent;
pub mod proc_launcher;
pub mod proc_mesh;
pub mod pyspy;
pub mod reference;
pub mod resource;
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

pub use actor_mesh::ActorMesh;
pub use actor_mesh::ActorMeshRef;
pub use bootstrap::Bootstrap;
pub use bootstrap::bootstrap;
pub use bootstrap::bootstrap_or_die;
pub use casting::CastError;
pub use comm::CommActor;
pub use dashmap;
use enum_as_inner::EnumAsInner;
pub use global_context::GlobalClientActor;
pub use global_context::GlobalContext;
pub use global_context::context;
pub use global_context::this_host;
pub use global_context::this_proc;
pub use host_mesh::HostMeshRef;
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
pub use value_mesh::ValueMesh;

use crate::host_mesh::HostAgent;
use crate::host_mesh::HostMeshRefParseError;
use crate::host_mesh::host_agent::ProcState;
use crate::resource::RankedValues;
use crate::resource::Status;
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
    ResourceIdParseError(#[from] mesh_id::ResourceIdParseError),

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
    CallError(hyperactor_reference::ActorId, anyhow::Error),

    #[error("actor not registered for type {0}")]
    ActorTypeNotRegistered(String),

    // TODO: this should be a valuemesh of statuses
    #[error("error while spawning actor {0}: {1}")]
    GspawnError(mesh_id::ActorMeshId, String),

    #[error("error while sending message to actor {0}: {1}")]
    SendingError(hyperactor_reference::ActorId, Box<MailboxSenderError>),

    #[error("error while casting message to {0}: {1}")]
    CastingError(mesh_id::ActorMeshId, anyhow::Error),

    #[error("error configuring host mesh agent {0}: {1}")]
    HostMeshAgentConfigurationError(hyperactor_reference::ActorId, String),

    #[error(
        "error creating proc (host rank {host_rank}) on host mesh agent {mesh_agent}, state: {state}"
    )]
    ProcCreationError {
        state: Box<resource::State<ProcState>>,
        host_rank: usize,
        mesh_agent: hyperactor_reference::ActorRef<HostAgent>,
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
    ControllerActorSpawnError(mesh_id::ResourceId, anyhow::Error),

    #[error("proc {0} must be direct-addressable")]
    RankedProc(hyperactor_reference::ProcId),

    #[error("{0}")]
    Supervision(Box<MeshFailure>),

    #[error("error: {0} does not exist")]
    NotExist(mesh_id::ResourceId),

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

/// Construct a per-actor display name from a mesh-level base name and a
/// rank's coordinates. Inserts `point.format_as_dict()` before the last
/// `>` in `base`, or appends it if no `>` is found. Returns `base`
/// unchanged for scalar (empty) points.
pub(crate) fn actor_display_name(base: &str, point: &view::Point) -> String {
    if point.is_empty() {
        return base.to_string();
    }
    let coords = point.format_as_dict();
    if let Some(pos) = base.rfind('>') {
        format!("{}{}{}", &base[..pos], coords, &base[pos..])
    } else {
        format!("{}{}", base, coords)
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
}
