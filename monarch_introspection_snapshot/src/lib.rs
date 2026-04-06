/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Bridge crate for snapshot-based mesh introspection.
//!
//! Will sit above `hyperactor_mesh` (which owns live mesh topology)
//! and `monarch_distributed_telemetry` (which owns table storage).
//!
//! Currently provides the relational row schema ([`schema`] module)
//! that defines the Arrow table shapes for mesh snapshots. Capture
//! and ingestion are planned but not yet implemented.

pub mod schema;
