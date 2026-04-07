/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Bridge crate for snapshot-based mesh introspection.
//!
//! Sits above `hyperactor_mesh` (which owns live mesh topology)
//! and `monarch_distributed_telemetry` (which owns table storage).
//!
//! Currently provides:
//! - [`schema`] — relational row definitions (Arrow table shapes)
//! - [`convert`] — `NodePayload` → row projection (`ConvertedNode`)
//! - [`capture`] — BFS capture of a mesh topology into `SnapshotData`
//! - [`push`] — drain `SnapshotData` into `TableStore` tables

pub mod capture;
pub mod convert;
pub mod push;
pub mod schema;
pub mod service;
