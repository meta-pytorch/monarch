/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Runtime-neutral UDP packet I/O for the Chrysalis QUIC driver.
//!
//! [`UdpDriver`] owns an unconnected UDP socket, one io_uring, and stable receive and transmit
//! slots. A QUIC implementation fills a [`SendSlot`] in place, then submits it with a destination
//! and optional userspace pacing deadline. Received datagrams remain in their kernel-written slot
//! until the caller explicitly releases them. This preserves the direct GSO/GRO data path measured
//! by `quic-udp-roofline` without imposing Tokio or an async API.
//!
//! This crate deliberately stops at UDP completions. The quiche layer will consume stream
//! submissions from `chrysalis-transport-core`, drive QUIC state, and translate these packet
//! completions into stream-operation completions.

mod config;
mod driver;
mod socket;

pub use config::DriverConfig;
pub use driver::DriverStats;
pub use driver::DriverStatsHandle;
pub use driver::IoEvent;
pub use driver::PollOutcome;
pub use driver::ReceiveSlotId;
pub use driver::ReceivedDatagram;
pub use driver::SendSlot;
pub use driver::TransmitCompletion;
pub use driver::UdpDriver;
pub use driver::WakeHandle;
