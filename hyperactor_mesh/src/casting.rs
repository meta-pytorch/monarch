/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Casting utilities for actor meshes.

use std::collections::BTreeSet;

use hyperactor::ActorRef;
use hyperactor::RemoteHandles;
use hyperactor::RemoteMessage;
use hyperactor::actor::Referable;
use hyperactor::context;
use hyperactor::mailbox;
use hyperactor::mailbox::MailboxSenderError;
use hyperactor::mailbox::MessageEnvelope;
use hyperactor::mailbox::Undeliverable;
use hyperactor::message::Castable;
use hyperactor::message::IndexedErasedUnbound;
use hyperactor_config::Flattrs;
use hyperactor_config::attrs::declare_attrs;
use ndslice::Selection;
use ndslice::Shape;
use ndslice::ShapeError;
use ndslice::SliceError;
use ndslice::reshape::Limit;
use ndslice::reshape::ReshapeError;
use ndslice::reshape::ReshapeSliceExt;
use ndslice::reshape::reshape_selection;
use ndslice::selection;
use ndslice::selection::EvalOpts;
use ndslice::selection::ReifySlice;
use ndslice::selection::normal;

use crate::CommActor;
use crate::comm::multicast::CAST_ORIGINATING_SENDER;
use crate::comm::multicast::CastMessage;
use crate::comm::multicast::CastMessageEnvelope;
use crate::comm::multicast::Uslice;
use crate::config::MAX_CAST_DIMENSION_SIZE;
use crate::metrics;
use crate::reference::ActorMeshId;

declare_attrs! {
    /// Which mesh this message was cast to. Used for undeliverable message
    /// handling, where the CastMessageEnvelope is serialized, and its content
    /// cannot be inspected.
    pub attr CAST_ACTOR_MESH_ID: ActorMeshId;
}

/// An undeliverable might have its sender address set as the comm actor instead
/// of the original sender. Update it based on the headers present in the message
/// so it matches the sender.
pub fn update_undeliverable_envelope_for_casting(
    mut envelope: Undeliverable<MessageEnvelope>,
) -> Undeliverable<MessageEnvelope> {
    let old_actor = envelope.0.sender().clone();
    if let Some(actor_id) = envelope.0.headers().get(CAST_ORIGINATING_SENDER) {
        tracing::debug!(
            actor_id = %old_actor,
            "remapped comm-actor id to id from CAST_ORIGINATING_SENDER {}", actor_id
        );
        envelope.0.update_sender(actor_id);
    }
    // Else do nothing, it wasn't from a comm actor.
    envelope
}

/// Common implementation for `ActorMesh`s and `ActorMeshRef`s to cast
/// an `M`-typed message
#[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `CastError`.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn actor_mesh_cast<A, M>(
    cx: &impl context::Actor,
    actor_mesh_id: ActorMeshId,
    comm_actor_ref: &ActorRef<CommActor>,
    selection_of_root: Selection,
    root_mesh_shape: &Shape,
    cast_mesh_shape: &Shape,
    message: M,
) -> Result<(), CastError>
where
    A: Referable + RemoteHandles<IndexedErasedUnbound<M>>,
    M: Castable + RemoteMessage,
{
    let _ = metrics::ACTOR_MESH_CAST_DURATION.start(hyperactor::kv_pairs!(
        "message_type" => M::typename(),
        "message_variant" => message.arm().unwrap_or_default(),
    ));

    let mut headers = Flattrs::new();
    mailbox::headers::set_send_timestamp(&mut headers);
    mailbox::headers::set_rust_message_type::<M>(&mut headers);
    headers.set(CAST_ACTOR_MESH_ID, actor_mesh_id.clone());
    let message = CastMessageEnvelope::new::<A, M>(
        actor_mesh_id.clone(),
        cx.mailbox().actor_id().clone(),
        cast_mesh_shape.clone(),
        headers,
        message,
    )?;

    // Mesh's shape might have large extents on some dimensions. Those
    // dimensions would cause large fanout in our comm actor
    // implementation. To avoid that, we reshape it by increasing
    // dimensionality and limiting the extent of each dimension. Note
    // the reshape is only visible to the internal algorithm. The
    // shape that user sees maintains intact.
    //
    // For example, a typical shape is [hosts=1024, gpus=8]. By using
    // limit 8, it becomes [8, 8, 8, 2, 8] during casting. In other
    // words, it adds 3 extra layers to the comm actor tree, while
    // keeping the fanout in each layer per dimension at 8 or smaller.
    //
    // An important note here is that max dimension size != max fanout.
    // Rank 0 must send a message to all ranks at index 0 for every dimension.
    // If our reshaped shape is [8, 8, 8, 2, 8], rank 0 must send
    // 7 + 7 + 7 + 1 + 7 = 21 messages.

    let slice_of_root = root_mesh_shape.slice();

    let max_cast_dimension_size = hyperactor_config::global::get(MAX_CAST_DIMENSION_SIZE);

    let slice_of_cast = slice_of_root.reshape_with_limit(Limit::from(max_cast_dimension_size));

    let selection_of_cast =
        reshape_selection(selection_of_root, root_mesh_shape.slice(), &slice_of_cast)?;

    let cast_message = CastMessage {
        dest: Uslice {
            slice: slice_of_cast,
            selection: selection_of_cast,
        },
        message,
    };

    // TEMPORARY: remove with v0 support
    let mut headers = Flattrs::new();
    headers.set(CAST_ACTOR_MESH_ID, actor_mesh_id);

    comm_actor_ref
        .port()
        .send_with_headers(cx, headers, cast_message)?;

    Ok(())
}

#[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `CastError`.
pub(crate) fn cast_to_sliced_mesh<A, M>(
    cx: &impl context::Actor,
    actor_mesh_id: ActorMeshId,
    comm_actor_ref: &ActorRef<CommActor>,
    sel_of_sliced: &Selection,
    message: M,
    sliced_shape: &Shape,
    root_mesh_shape: &Shape,
) -> Result<(), CastError>
where
    A: Referable + RemoteHandles<IndexedErasedUnbound<M>>,
    M: Castable + RemoteMessage,
{
    let root_slice = root_mesh_shape.slice();

    // Casting to `*`?
    let sel_of_root = if selection::normalize(sel_of_sliced) == normal::NormalizedSelection::True {
        // Reify this view into base.
        root_slice.reify_slice(sliced_shape.slice())?
    } else {
        // No, fall back on `of_ranks`.
        let ranks = sel_of_sliced
            .eval(&EvalOpts::strict(), sliced_shape.slice())?
            .collect::<BTreeSet<_>>();
        Selection::of_ranks(root_slice, &ranks)?
    };

    // Cast.
    actor_mesh_cast::<A, M>(
        cx,
        actor_mesh_id,
        comm_actor_ref,
        sel_of_root,
        root_mesh_shape,
        sliced_shape,
        message,
    )
}

/// The type of error of casting operations.
#[derive(Debug, thiserror::Error)]
pub enum CastError {
    #[error("invalid selection {0}: {1}")]
    InvalidSelection(Selection, ShapeError),

    #[error("send on rank {0}: {1}")]
    MailboxSenderError(usize, MailboxSenderError),

    #[error("unsupported selection: {0}")]
    SelectionNotSupported(String),

    #[error(transparent)]
    RootMailboxSenderError(#[from] MailboxSenderError),

    #[error(transparent)]
    ShapeError(#[from] ShapeError),

    #[error(transparent)]
    SliceError(#[from] SliceError),

    #[error(transparent)]
    SerializationError(#[from] bincode::Error),

    #[error(transparent)]
    Other(#[from] anyhow::Error),

    #[error(transparent)]
    ReshapeError(#[from] ReshapeError),
}
