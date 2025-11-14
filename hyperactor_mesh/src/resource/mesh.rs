/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![allow(dead_code)]

use hyperactor::Named;
/// This module defines common types for mesh resources. Meshes are managed as
/// resources, usually by a controller actor implementing the [`crate::resource`]
/// behavior.
///
/// The mesh controller manages all aspects of the mesh lifecycle, and the owning
/// actor uses the resource behavior directly to query the state of the mesh.
use ndslice::Extent;
use serde::Deserialize;
use serde::Serialize;

use crate::resource::CreateOrUpdate;
use crate::resource::GetState;
use crate::resource::Status;
use crate::resource::Stop;
use crate::v1::ValueMesh;

/// Mesh specs
#[derive(Debug, Named, Serialize, Deserialize)]
pub struct Spec<S> {
    /// All meshes have an extent
    extent: Extent,
    // supervisor: PortHandle<SupervisionEvent(?)>
    /// The mesh-specific spec.
    spec: S,
}

/// Mesh states
#[derive(Debug, Named, Serialize, Deserialize)]
pub struct State<S> {
    /// The current status for each rank in the mesh.
    statuses: ValueMesh<Status>,
    /// Mesh-specific state.
    state: S,
}

// The behavior of a mesh controllšr.
// hyperactor::behavior!(
//     Controller<Sp, St>,
//     CreateOrUpdate<Spec<Sp>>,
//     GetState<State<St>>,
//     Stop,
// );
