/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Remote supervision protocol and adapters for Hyperactor.

#![deny(missing_docs)]

pub mod link;
pub mod proto;

pub use link::LinkSpec;
pub use proto::Link;
pub use proto::LinkOptions;
pub use proto::OrphanPolicy;
pub use proto::RemoteActorDisposition;
pub use proto::SupervisedWorker;
pub use proto::WorkerSupervisor;
