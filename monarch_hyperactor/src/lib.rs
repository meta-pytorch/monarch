/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![allow(unsafe_op_in_unsafe_fn)]
#![feature(exit_status_error)]
#![feature(mapped_lock_guards)]
#![feature(rwlock_downgrade)]

pub mod actor;
pub mod actor_mesh;
pub mod alloc;
pub mod bootstrap;
pub mod buffers;
pub mod channel;
pub mod code_sync;
pub mod config;
pub mod local_state_broker;
pub mod logging;
pub mod mailbox;
pub mod ndslice;
pub mod proc;
pub mod proc_mesh;
pub mod pytokio;
pub mod runtime;
pub mod selection;
pub mod shape;
pub mod supervision;
pub mod telemetry;
pub mod value_mesh;

#[cfg(fbcode_build)]
pub mod meta;
