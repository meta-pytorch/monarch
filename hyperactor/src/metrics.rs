/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Hyperactor metrics.
//!
//! This module contains metrics definitions for various components of hyperactor.

use hyperactor_telemetry::declare_static_counter;
use hyperactor_telemetry::declare_static_histogram;
use hyperactor_telemetry::declare_static_timer;
use hyperactor_telemetry::declare_static_up_down_counter;

/// Error types for channel-related errors. Only used for telemetry.
#[derive(Debug, Clone, Copy)]
pub enum ChannelErrorType {
    /// Error occurred while sending a message.
    SendError,
    /// Error occurred while connecting to a channel.
    ConnectionError,
    /// Error occurred while deframing a message.
    DeframeError,
    /// Error occurred while deserializing a message.
    DeserializeError,
}

impl ChannelErrorType {
    /// Returns the string representation of the error type.
    pub fn as_str(&self) -> &'static str {
        match self {
            ChannelErrorType::SendError => "send_error",
            ChannelErrorType::ConnectionError => "connection_error",
            ChannelErrorType::DeframeError => "deframe_error",
            ChannelErrorType::DeserializeError => "deserialize_error",
        }
    }
}

// MAILBOX
// Tracks messages that couldn't be delivered to their destination and were returned as undeliverable
declare_static_counter!(
    MAILBOX_UNDELIVERABLE_MESSAGES,
    "mailbox.undeliverable_messages"
);
// Tracks the number of messages that were posted.
hyperactor_telemetry::declare_static_counter!(MAILBOX_POSTS, "mailbox.posts");

// ACTOR
// Tracks the current size of the message queue for actors (increases when messages are queued, decreases when processed)
declare_static_up_down_counter!(ACTOR_MESSAGE_QUEUE_SIZE, "actor.message_queue_size");
// Tracks the total number of messages sent by actors
declare_static_counter!(ACTOR_MESSAGES_SENT, "actor.messages_sent");
// Tracks the total number of messages received by actors
declare_static_counter!(ACTOR_MESSAGES_RECEIVED, "actor.messages_received");
// Tracks errors that occur when receiving messages
declare_static_counter!(ACTOR_MESSAGE_RECEIVE_ERRORS, "actor.message_receive_errors");
// Measures the time taken to handle messages by actors
declare_static_timer!(
    ACTOR_MESSAGE_HANDLER_DURATION,
    "actor.message_handler_duration",
    hyperactor_telemetry::TimeUnit::Nanos
);

// CHANNEL
declare_static_histogram!(REMOTE_MESSAGE_SEND_SIZE, "channel.remote_message_send_size");
// Tracks the number of new channel connections established (client and server)
declare_static_counter!(CHANNEL_CONNECTIONS, "channel.connections");
// Tracks the number of channel reconnection attempts
declare_static_counter!(CHANNEL_RECONNECTIONS, "channel.reconnections");
// Tracks errors for each channel pair
declare_static_counter!(CHANNEL_ERRORS, "channel.errors");
// Tracks the number of NetRx encountering full buffer, i.e. its mpsc channel.

// This metric counts how often the NetRx→client mpsc channel remains full,
// incrementing once per CHANNEL_NET_RX_BUFFER_FULL_CHECK_INTERVAL while blocked.
declare_static_counter!(CHANNEL_NET_RX_BUFFER_FULL, "channel.net_rx_buffer_full");

// Tracks throughput (bytes sent)
declare_static_counter!(CHANNEL_THROUGHPUT_BYTES, "channel.throughput.bytes");
// Tracks throughput (message count)
declare_static_counter!(CHANNEL_THROUGHPUT_MESSAGES, "channel.throughput.messages");
// Tracks message latency for each channel pair in microseconds
declare_static_histogram!(CHANNEL_LATENCY_MICROS, "channel.latency.us");

// PROC MESH
// Tracks the number of active processes in the process mesh
declare_static_counter!(PROC_MESH_ALLOCATION, "proc_mesh.active_procs");
// Tracks the number of process failures in the process mesh
declare_static_counter!(PROC_MESH_PROC_STOPPED, "proc_mesh.proc_failures");
// Tracks the number of actor failures within the process mesh
declare_static_counter!(PROC_MESH_ACTOR_FAILURES, "proc_mesh.actor_failures");

// MESSAGE LATENCY
// Tracks end-to-end message latency in microseconds (sampled at 1% by default)
declare_static_histogram!(MESSAGE_LATENCY_MICROS, "message.e2e_latency.us");
