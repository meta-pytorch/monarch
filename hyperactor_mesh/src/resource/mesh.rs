/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![allow(dead_code)]

//! This module defines common types for mesh resources. Meshes are managed as
//! resources, usually by a controller actor implementing the [`crate::resource`]
//! behavior.
//!
//! The mesh controller manages all aspects of the mesh lifecycle, and the owning
//! actor uses the resource behavior directly to query the state of the mesh.

use hyperactor::Bind;
use hyperactor::Unbind;
use ndslice::Extent;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::resource::Resource;
use crate::resource::Status;
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
#[derive(Debug, Named, Bind, Unbind, Serialize, Deserialize)]
pub struct State<S> {
    /// The current status for each rank in the mesh.
    pub statuses: ValueMesh<Status>,
    /// Mesh-specific state.
    pub state: S,
}

/// A mesh trait bundles a set of types that together define a mesh resource.
pub trait Mesh {
    /// The mesh-specific specification for this resource.
    type Spec: typeuri::Named
        + Serialize
        + for<'de> Deserialize<'de>
        + Send
        + Sync
        + std::fmt::Debug;

    /// The mesh-specific state for this resource.
    type State: typeuri::Named
        + Serialize
        + for<'de> Deserialize<'de>
        + Send
        + Sync
        + std::fmt::Debug;
}

impl<M: Mesh> Resource for M {
    type Spec = Spec<M::Spec>;
    type State = State<M::State>;
}

#[cfg(test)]
mod test {
    use hyperactor::Actor;
    use hyperactor::Context;
    use hyperactor::Handler;

    use super::*;
    use crate::resource::Controller;
    use crate::resource::CreateOrUpdate;
    use crate::resource::GetState;
    use crate::resource::Stop;

    // Consider upstreaming this into `hyperactor` -- lightweight handler definitions
    // can be quite useful.
    macro_rules! handler {
        (
            $actor:path,
            $(
                $name:ident: $msg:ty => $body:expr
            ),* $(,)?
        ) => {
            $(
                #[async_trait::async_trait]
                impl Handler<$msg> for $actor {
                    async fn handle(
                        &mut self,
                        #[allow(unused_variables)]
                        cx: & Context<Self>,
                        $name: $msg
                    ) -> anyhow::Result<()> {
                        $body
                    }
                }
            )*
        };
    }

    #[derive(Debug, Named, Serialize, Deserialize)]
    struct TestMesh;

    impl Mesh for TestMesh {
        type Spec = ();
        type State = ();
    }

    #[derive(Debug, Default, Named, Serialize, Deserialize)]
    struct TestMeshController;

    impl Actor for TestMeshController {}

    // Ensure that TestMeshController conforms to the Controller behavior for TestMesh.
    handler! {
        TestMeshController,
        _message: CreateOrUpdate<Spec<()>> => unimplemented!(),
        _message: GetState<State<()>> => unimplemented!(),
        _message: Stop => unimplemented!(),
    }

    hyperactor::assert_behaves!(TestMeshController as Controller<TestMesh>);

    #[test]
    fn test_state_serialize_and_deserialize_with_bincode() {
        let region: ndslice::Region = ndslice::extent!(x = 5).into();
        let num_ranks = region.num_ranks();
        let data = State {
            statuses: ValueMesh::new(region, vec![Status::Running; num_ranks]).unwrap(),
            state: 0,
        };
        let encoded = bincode::serialize(&data).expect("serialization failed");
        let decoded: State<i32> = bincode::deserialize(&encoded).expect("deserialization failed");
        assert_eq!(decoded.state, data.state);
        assert_eq!(decoded.statuses, data.statuses);
    }
}
