/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Casting utilities for actor meshes.

use hyperactor::Actor;
use hyperactor::ActorAddr;
use hyperactor::Context;
use hyperactor::mailbox::MailboxSenderError;
use hyperactor::mailbox::MessageEnvelope;
use hyperactor::mailbox::Undeliverable;
pub use hyperactor_cast::cast_actor::CAST_ORIGINATING_SENDER;
pub use hyperactor_cast::cast_actor::CAST_POINT;
use hyperactor_config::Flattrs;
use hyperactor_config::attrs::declare_attrs;
use ndslice::Extent;
use ndslice::Point;
use ndslice::Selection;
use ndslice::ShapeError;
use ndslice::SliceError;
use ndslice::reshape::ReshapeError;

use crate::mesh_id::ActorMeshId;

declare_attrs! {
    /// Which mesh this message was cast to. Used for undeliverable message
    /// handling when the serialized cast payload cannot be inspected.
    pub attr CAST_ACTOR_MESH_ID: ActorMeshId;
}

/// Update a cast undeliverable with the original sender recorded in its headers.
pub fn update_undeliverable_envelope_for_casting(
    mut envelope: Undeliverable<MessageEnvelope>,
) -> Undeliverable<MessageEnvelope> {
    let Some(message) = envelope.as_message_mut() else {
        return envelope;
    };
    let old_actor = message.sender().clone();
    if let Some(actor_id) = message.headers().get(CAST_ORIGINATING_SENDER) {
        tracing::debug!(
            actor_id = %old_actor,
            "remapped cast sender to id from CAST_ORIGINATING_SENDER {}", actor_id
        );
        message.update_sender(actor_id);
    }
    // Else do nothing: the message was not sent through cast routing.
    envelope
}

pub fn set_cast_info_on_headers(headers: &mut Flattrs, cast_point: Point, sender: ActorAddr) {
    headers.set(
        hyperactor::mailbox::headers::SENDER_ACTOR_ID_HASH,
        hyperactor_telemetry::hash_to_u64(sender.id()),
    );
    headers.set(CAST_POINT, cast_point);
    headers.set(CAST_ORIGINATING_SENDER, sender);
}

pub trait CastInfo {
    /// Get the cast rank and cast shape.
    /// If something wasn't explicitly sent via a cast, represent it as the only
    /// member of a zero-dimensional cast shape, which is the same as a singleton.
    fn cast_point(&self) -> Point;
    fn sender(&self) -> ActorAddr;
}

impl<A: Actor> CastInfo for Context<'_, A> {
    fn cast_point(&self) -> Point {
        match self.headers().get(CAST_POINT) {
            Some(point) => point,
            None => Extent::unity()
                .point_of_rank(0)
                .expect("the unity extent contains rank 0"),
        }
    }

    fn sender(&self) -> ActorAddr {
        self.headers()
            .get(CAST_ORIGINATING_SENDER)
            .expect("cast headers contain the originating sender")
    }
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
    SerializationEncodeError(#[from] bincode::error::EncodeError),

    #[error(transparent)]
    SerializationDecodeError(#[from] bincode::error::DecodeError),

    #[error(transparent)]
    Other(#[from] anyhow::Error),

    #[error(transparent)]
    ReshapeError(#[from] ReshapeError),
}
