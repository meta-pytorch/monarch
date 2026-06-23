/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Casting domains for O(log n) multicast over arbitrary sets of actors.
//!
//! A casting domain is an immutable, ordered set of actors with a
//! communication tree that enables O(log n) multicast. Domains are
//! created once and never modified -- to change membership, create a
//! new domain.

pub mod cast_actor;
mod tile;

pub use tile::TilingPolicy;
