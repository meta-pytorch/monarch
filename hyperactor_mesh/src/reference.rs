/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cmp::Ord;
use std::cmp::PartialOrd;
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;
use std::str::FromStr;

use hyperactor::ActorRef;
use hyperactor::RemoteHandles;
use hyperactor::RemoteMessage;
use hyperactor::actor::Referable;
use hyperactor::context;
use hyperactor::message::Castable;
use hyperactor::message::IndexedErasedUnbound;
use hyperactor_config::AttrValue;
use ndslice::Range;
use ndslice::Selection;
use ndslice::Shape;
use ndslice::ShapeError;
use ndslice::selection::ReifySlice;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::CommActor;
use crate::actor_mesh::CastError;
use crate::actor_mesh::actor_mesh_cast;
use crate::actor_mesh::cast_to_sliced_mesh;
use crate::v1::Name;

#[derive(
    Debug,
    Serialize,
    Deserialize,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Hash,
    Ord,
    Named
)]
pub struct ProcMeshId(pub String);

/// Actor Mesh ID.
#[derive(
    Debug,
    Serialize,
    Deserialize,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Hash,
    Ord,
    Named,
    AttrValue
)]
pub struct ActorMeshId(pub Name);

impl fmt::Display for ActorMeshId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for ActorMeshId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ActorMeshId(Name::from_str(s)?))
    }
}

/// Types references to Actor Meshes.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct ActorMeshRef<A: Referable> {
    pub(crate) mesh_id: ActorMeshId,
    /// The shape of the root mesh.
    root: Shape,
    /// If some, it mean this mesh ref points to a sliced mesh, and this field
    /// is this sliced mesh's shape. If None, it means this mesh ref points to
    /// the root mesh.
    sliced: Option<Shape>,
    /// The reference to the comm actor of the underlying Proc Mesh.
    comm_actor_ref: ActorRef<CommActor>,
    phantom: PhantomData<A>,
}

impl<A: Referable> ActorMeshRef<A> {
    /// The caller guarantees that the provided mesh ID is also a valid,
    /// typed reference.  This is usually invoked to provide a guarantee
    /// that an externally-provided mesh ID (e.g., through a command
    /// line argument) is a valid reference.
    pub fn attest(mesh_id: ActorMeshId, root: Shape, comm_actor_ref: ActorRef<CommActor>) -> Self {
        Self {
            mesh_id,
            root,
            sliced: None,
            comm_actor_ref,
            phantom: PhantomData,
        }
    }

    /// The Actor Mesh ID corresponding with this reference.
    pub fn mesh_id(&self) -> &ActorMeshId {
        &self.mesh_id
    }

    /// Shape of the Actor Mesh.
    pub fn shape(&self) -> &Shape {
        match &self.sliced {
            Some(s) => s,
            None => &self.root,
        }
    }

    /// Cast an [`M`]-typed message to the ranks selected by `sel`
    /// in this ActorMesh.
    #[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `CastError`.
    pub fn cast<M>(
        &self,
        cx: &impl context::Actor,
        selection: Selection,
        message: M,
    ) -> Result<(), CastError>
    where
        A: RemoteHandles<M> + RemoteHandles<IndexedErasedUnbound<M>>,
        M: Castable + RemoteMessage,
    {
        match &self.sliced {
            Some(sliced_shape) => cast_to_sliced_mesh::<A, M>(
                cx,
                self.mesh_id.clone(),
                &self.comm_actor_ref,
                &selection,
                message,
                sliced_shape,
                &self.root,
            ),
            None => actor_mesh_cast::<A, M>(
                cx,
                self.mesh_id.clone(),
                &self.comm_actor_ref,
                selection,
                &self.root,
                &self.root,
                message,
            ),
        }
    }

    pub fn select<R: Into<Range>>(&self, label: &str, range: R) -> Result<Self, ShapeError> {
        let sliced = self.shape().select(label, range)?;
        Ok(Self {
            mesh_id: self.mesh_id.clone(),
            root: self.root.clone(),
            sliced: Some(sliced),
            comm_actor_ref: self.comm_actor_ref.clone(),
            phantom: PhantomData,
        })
    }

    pub fn new_with_shape(&self, new_shape: Shape) -> anyhow::Result<Self> {
        let base_slice = self.shape().slice();
        base_slice.reify_slice(new_shape.slice()).map_err(|e| {
            anyhow::anyhow!(
                "failed to reify the new shape into the base shape; this \
                normally means the new shape is not a valid slice of the base \
                error is: {e:?}"
            )
        })?;

        Ok(Self {
            mesh_id: self.mesh_id.clone(),
            root: self.root.clone(),
            sliced: Some(new_shape),
            comm_actor_ref: self.comm_actor_ref.clone(),
            phantom: PhantomData,
        })
    }
}

impl<A: Referable> Clone for ActorMeshRef<A> {
    fn clone(&self) -> Self {
        Self {
            mesh_id: self.mesh_id.clone(),
            root: self.root.clone(),
            sliced: self.sliced.clone(),
            comm_actor_ref: self.comm_actor_ref.clone(),
            phantom: PhantomData,
        }
    }
}
