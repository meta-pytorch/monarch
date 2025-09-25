/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This modules defines a set of common message types used for managing resources
//! in hyperactor meshes.

use std::fmt::Debug;

use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Named;
use hyperactor::PortRef;
use hyperactor::RefClient;
use hyperactor::RemoteMessage;
use hyperactor::message::Bind;
use hyperactor::message::Bindings;
use hyperactor::message::Unbind;
use serde::Deserialize;
use serde::Serialize;

use crate::v1::Name;

/// The current lifecycle status of a resource.
#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    Named,
    PartialOrd,
    Ord,
    PartialEq,
    Eq
)]
pub enum Status {
    /// The resource does not exist.
    NotExist,
    /// The resource is being created.
    Initializing,
    /// The resource is running.
    Running,
    /// The resource is being stopped.
    Stopping,
    /// The resource is stopped.
    Stopped,
    /// The resource has failed, with an error message.
    Failed(String),
}

/// The state of a resource.
#[derive(Clone, Debug, Serialize, Deserialize, Named, PartialEq, Eq)]
pub struct State<S> {
    /// The name of the resource.
    pub name: Name,
    /// Its status.
    pub status: Status,
    /// Optionally, a resource-defined state.
    pub state: Option<S>,
}

/// Create or update a resource according to a spec.
#[derive(Debug, Serialize, Deserialize, Named, Handler, HandleClient, RefClient)]
pub struct CreateOrUpdate<S> {
    /// The name of the resource to create or update.
    pub name: Name,
    /// The specification of the resource.
    pub spec: S,
    /// Whether the operation succeeded.
    #[reply]
    pub reply: PortRef<bool>,
}

/// Retrieve the current state of the resource.
#[derive(Debug, Serialize, Deserialize, Named, Handler, HandleClient, RefClient)]
pub struct GetState<S> {
    /// The name of the resource.
    pub name: Name,
    /// A reply containing the state.
    #[reply]
    pub reply: PortRef<State<S>>,
}

// Cannot derive Bind and Unbind for this generic, implement manually.
impl<S> Unbind for GetState<S>
where
    S: RemoteMessage,
    S: Unbind,
{
    fn unbind(&self, bindings: &mut Bindings) -> anyhow::Result<()> {
        self.reply.unbind(bindings)
    }
}

impl<S> Bind for GetState<S>
where
    S: RemoteMessage,
    S: Bind,
{
    fn bind(&mut self, bindings: &mut Bindings) -> anyhow::Result<()> {
        self.reply.bind(bindings)
    }
}

impl<S> Clone for GetState<S>
where
    S: RemoteMessage,
{
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            reply: self.reply.clone(),
        }
    }
}
