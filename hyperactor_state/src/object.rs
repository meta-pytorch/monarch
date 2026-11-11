/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use hyperactor::Named;
use hyperactor::data::Serialized;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StateMetadata {
    /// Name of the state object.
    pub name: String,
    /// Kind of the object.
    pub kind: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StateObject<S, T> {
    metadata: StateMetadata,
    spec: S,
    state: T,
}

impl<S, T> StateObject<S, T> {
    #[allow(dead_code)]
    pub fn new(metadata: StateMetadata, spec: S, state: T) -> Self {
        Self {
            metadata,
            spec,
            state,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogSpec;

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct LogState {
    /// A monotonically increasing sequence number.
    seq: usize,
    /// The message in the log.
    message: String,
}

impl LogState {
    #[allow(dead_code)]
    pub fn new(seq: usize, message: String) -> Self {
        Self { seq, message }
    }
}

/// A generic state object which is the partially serialized version of a
/// [`StateObject`]. Since [`StateObject`] takes generic types, those type information
/// can be retrieved from the metadata to deserialize [`GenericStateObject`] into
/// a [`StateObject<S, T>`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Named)]
pub struct GenericStateObject {
    metadata: StateMetadata,
    data: Serialized,
}

impl<S, T> TryFrom<StateObject<S, T>> for GenericStateObject
where
    S: Spec,
    T: State,
    StateObject<S, T>: Named,
{
    type Error = anyhow::Error;

    fn try_from(obj: StateObject<S, T>) -> Result<Self, Self::Error> {
        Ok(Self {
            metadata: obj.metadata.clone(),
            data: Serialized::serialize(&obj)?,
        })
    }
}

impl GenericStateObject {
    pub fn metadata(&self) -> &StateMetadata {
        &self.metadata
    }

    pub fn data(&self) -> &Serialized {
        &self.data
    }
}

/// Spec is the define the desired state of an object, defined by the user.
pub trait Spec: Serialize + for<'de> Deserialize<'de> {}
/// State is the current state of an object.
pub trait State: Serialize + for<'de> Deserialize<'de> {}

impl Spec for LogSpec {}
impl State for LogState {}

// Implement Named for StateObject<LogSpec, LogState> to support serialization
impl Named for StateObject<LogSpec, LogState> {
    fn typename() -> &'static str {
        "hyperactor_state::object::StateObject<LogSpec, LogState>"
    }
}
