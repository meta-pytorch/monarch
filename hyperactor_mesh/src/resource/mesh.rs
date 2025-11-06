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

/// A mesh trait bundles a set of types that together define a mesh resource.
pub trait Mesh {
    /// The mesh-specific specification for this resource.
    type Spec: Named + Serialize + for<'de> Deserialize<'de> + Send + Sync + std::fmt::Debug;

    /// The mesh-specific state for thsi resource.
    type State: Named + Serialize + for<'de> Deserialize<'de> + Send + Sync + std::fmt::Debug;
}

// A behavior defining the interface for a mesh controller.
hyperactor::behavior!(
    Controller<M: Mesh>,
    CreateOrUpdate<Spec<M::Spec>>,
    GetState<State<M::State>>,
    Stop,
);

#[cfg(test)]
mod test {
    use hyperactor::Actor;
    use hyperactor::ActorRef;
    use hyperactor::Context;
    use hyperactor::Handler;

    use super::*;

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

    #[derive(Actor, Debug, Default, Named, Serialize, Deserialize)]
    struct TestMeshController;

    // Ensure that TestMeshController conforms to the Controller behavior for TestMesh.
    handler! {
        TestMeshController,
        _message: CreateOrUpdate<Spec<()>> => unimplemented!(),
        _message: GetState<State<()>> => unimplemented!(),
        _message: Stop => unimplemented!(),
    }

    #[test]
    fn test_controller_behavior() {
        use hyperactor::ActorHandle;

        // This is a compile-time check that TestMeshController implements
        // the Controller<TestMesh> behavior correctly.
        fn _assert_bind(handle: ActorHandle<TestMeshController>) -> ActorRef<Controller<TestMesh>> {
            handle.bind()
        }
    }
}
