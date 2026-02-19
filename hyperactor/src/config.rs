/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Configuration keys and I/O for hyperactor.
//!
//! This module declares hyperactor-specific config keys.

use std::io::Cursor;
use std::io::Read;
use std::path::PathBuf;
use std::time::Duration;

use hyperactor_config::AttrValue;
use hyperactor_config::CONFIG;
use hyperactor_config::ConfigAttr;
use hyperactor_config::attrs::declare_attrs;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

/// Stores a PEM-encoded value, either specified directly or read from a file.
#[derive(Clone, Debug, Serialize, Named)]
#[named("hyperactor::config::Pem")]
#[serde(tag = "type", content = "value")]
pub enum Pem {
    /// Raw PEM value stored directly.
    Value(Vec<u8>),
    /// Path to a file containing the PEM data.
    File(PathBuf),
    /// Static path string (for use in const/static contexts).
    StaticPath(&'static str),
}

// Custom Deserialize implementation because StaticPath contains &'static str
impl<'de> Deserialize<'de> for Pem {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(tag = "type", content = "value")]
        enum PemDeserialize {
            Value(Vec<u8>),
            File(PathBuf),
            // StaticPath deserializes as a File since we can't deserialize &'static str
            StaticPath(String),
        }

        match PemDeserialize::deserialize(deserializer)? {
            PemDeserialize::Value(v) => Ok(Pem::Value(v)),
            PemDeserialize::File(p) => Ok(Pem::File(p)),
            // Convert StaticPath string to File during deserialization
            PemDeserialize::StaticPath(s) => Ok(Pem::File(PathBuf::from(s))),
        }
    }
}

impl AttrValue for Pem {
    fn display(&self) -> String {
        match self {
            Pem::Value(data) => String::from_utf8_lossy(data).to_string(),
            Pem::File(path) => path.display().to_string(),
            Pem::StaticPath(path) => path.to_string(),
        }
    }

    fn parse(value: &str) -> Result<Self, anyhow::Error> {
        // PEM data starts with "-----BEGIN"
        if value.trim_start().starts_with("-----BEGIN") {
            Ok(Pem::Value(value.as_bytes().to_vec()))
        } else {
            Ok(Pem::File(PathBuf::from(value)))
        }
    }
}

impl Pem {
    /// Returns a reader for the PEM data.
    pub fn reader(&self) -> std::io::Result<Box<dyn Read + '_>> {
        match self {
            Pem::Value(data) => Ok(Box::new(Cursor::new(data))),
            Pem::File(path) => Ok(Box::new(std::fs::File::open(path)?)),
            Pem::StaticPath(path) => Ok(Box::new(std::fs::File::open(path)?)),
        }
    }
}

/// A bundle of PEM files for TLS configuration: CA certificate, server/client certificate, and private key.
#[derive(Clone, Debug)]
pub struct PemBundle {
    /// The CA certificate used to verify peer certificates.
    pub ca: Pem,
    /// The certificate to present to peers.
    pub cert: Pem,
    /// The private key for the certificate.
    pub key: Pem,
}

// Declare hyperactor-specific configuration keys
declare_attrs! {
    /// Maximum frame length for codec
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_CODEC_MAX_FRAME_LENGTH".to_string()),
        Some("codec_max_frame_length".to_string()),
    ))
    pub attr CODEC_MAX_FRAME_LENGTH: usize = 10 * 1024 * 1024 * 1024; // 10 GiB

    /// Message delivery timeout
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT".to_string()),
        Some("message_delivery_timeout".to_string()),
    ))
    pub attr MESSAGE_DELIVERY_TIMEOUT: Duration = Duration::from_secs(30);

    /// Timeout used by allocator for stopping a proc.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_PROCESS_EXIT_TIMEOUT".to_string()),
        Some("process_exit_timeout".to_string()),
    ))
    pub attr PROCESS_EXIT_TIMEOUT: Duration = Duration::from_secs(10);

    /// Message acknowledgment interval
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESSAGE_ACK_TIME_INTERVAL".to_string()),
        Some("message_ack_time_interval".to_string()),
    ))
    pub attr MESSAGE_ACK_TIME_INTERVAL: Duration = Duration::from_millis(500);

    /// Number of messages after which to send an acknowledgment
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESSAGE_ACK_EVERY_N_MESSAGES".to_string()),
        Some("message_ack_every_n_messages".to_string()),
    ))
    pub attr MESSAGE_ACK_EVERY_N_MESSAGES: u64 = 1000;

    /// Default hop Time-To-Live for message envelopes.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESSAGE_TTL_DEFAULT".to_string()),
        Some("message_ttl_default".to_string()),
    ))
    pub attr MESSAGE_TTL_DEFAULT : u8 = 64;

    /// Maximum buffer size for split port messages
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_SPLIT_MAX_BUFFER_SIZE".to_string()),
        Some("split_max_buffer_size".to_string()),
    ))
    pub attr SPLIT_MAX_BUFFER_SIZE: usize = 5;

    /// The maximum time an update can be buffered before being reduced.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_SPLIT_MAX_BUFFER_AGE".to_string()),
        Some("split_max_buffer_age".to_string()),
    ))
    pub attr SPLIT_MAX_BUFFER_AGE: Duration = Duration::from_millis(50);

    /// Timeout used by proc mesh for stopping an actor.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_STOP_ACTOR_TIMEOUT".to_string()),
        Some("stop_actor_timeout".to_string()),
    ))
    pub attr STOP_ACTOR_TIMEOUT: Duration = Duration::from_secs(10);

    /// Timeout used by proc for running the cleanup callback on an actor.
    /// Should be less than the timeout for STOP_ACTOR_TIMEOUT.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_CLEANUP_TIMEOUT".to_string()),
        Some("cleanup_timeout".to_string()),
    ))
    pub attr CLEANUP_TIMEOUT: Duration = Duration::from_secs(3);

    /// Heartbeat interval for remote allocator. We do not rely on this heartbeat
    /// anymore in v1, and it should be removed after we finishing the v0
    /// deprecation.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL".to_string()),
        Some("remote_allocator_heartbeat_interval".to_string()),
    ))
    pub attr REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL: Duration = Duration::from_mins(5);

    /// How often to check for full MPSC channel on NetRx.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_CHANNEL_NET_RX_BUFFER_FULL_CHECK_INTERVAL".to_string()),
        Some("channel_net_rx_buffer_full_check_interval".to_string()),
    ))
    pub attr CHANNEL_NET_RX_BUFFER_FULL_CHECK_INTERVAL: Duration = Duration::from_secs(5);

    /// Sampling rate for logging message latency
    /// Set to 0.01 for 1% sampling, 0.1 for 10% sampling, 0.90 for 90% sampling, etc.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_MESSAGE_LATENCY_SAMPLING_RATE".to_string()),
        Some("message_latency_sampling_rate".to_string()),
    ))
    pub attr MESSAGE_LATENCY_SAMPLING_RATE: f32 = 0.01;

    /// Whether to enable dest actor reordering buffer.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_ENABLE_DEST_ACTOR_REORDERING_BUFFER".to_string()),
        Some("enable_dest_actor_reordering_buffer".to_string()),
    ))
    pub attr ENABLE_DEST_ACTOR_REORDERING_BUFFER: bool = false;

    /// Timeout for [`Host::spawn`] to await proc readiness.
    ///
    /// Default: 30 seconds. If set to zero, disables the timeout and
    /// waits indefinitely.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_HOST_SPAWN_READY_TIMEOUT".to_string()),
        Some("host_spawn_ready_timeout".to_string()),
    ))
    pub attr HOST_SPAWN_READY_TIMEOUT: Duration = Duration::from_secs(30);

    /// Heartbeat interval for server health metrics. The server emits a
    /// heartbeat metric at this interval to indicate it is alive.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_SERVER_HEARTBEAT_INTERVAL".to_string()),
        Some("server_heartbeat_interval".to_string()),
    ))
    pub attr SERVER_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

    /// Path to TLS certificate file for the 'tls' transport.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_TLS_CERT".to_string()),
        Some("hyperactor_tls_cert".to_string()),
    ).process_local())
    pub attr TLS_CERT: Pem = Pem::StaticPath("/etc/hyperactor/tls/tls.crt");

    /// Path to TLS private key file for the 'tls' transport.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_TLS_KEY".to_string()),
        Some("hyperactor_tls_key".to_string()),
    ).process_local())
    pub attr TLS_KEY: Pem = Pem::StaticPath("/etc/hyperactor/tls/tls.key");

    /// Path to TLS CA certificate file for the 'tls' transport.
    @meta(CONFIG = ConfigAttr::new(
        Some("HYPERACTOR_TLS_CA".to_string()),
        Some("hyperactor_tls_ca".to_string()),
    ).process_local())
    pub attr TLS_CA: Pem = Pem::StaticPath("/etc/hyperactor/tls/ca.crt");
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use hyperactor_config::Attrs;
    use hyperactor_config::from_env;
    use indoc::indoc;

    use super::*;

    const CODEC_MAX_FRAME_LENGTH_DEFAULT: usize = 10 * 1024 * 1024 * 1024;

    #[test]
    fn test_default_config() {
        let config = Attrs::new();
        assert_eq!(
            config[CODEC_MAX_FRAME_LENGTH],
            CODEC_MAX_FRAME_LENGTH_DEFAULT
        );
        assert_eq!(config[MESSAGE_DELIVERY_TIMEOUT], Duration::from_secs(30));
        assert_eq!(
            config[MESSAGE_ACK_TIME_INTERVAL],
            Duration::from_millis(500)
        );
        assert_eq!(config[MESSAGE_ACK_EVERY_N_MESSAGES], 1000);
        assert_eq!(config[SPLIT_MAX_BUFFER_SIZE], 5);
        assert_eq!(
            config[REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL],
            Duration::from_mins(5)
        );
    }

    #[tracing_test::traced_test]
    #[test]
    // TODO: OSS: The logs_assert function returned an error: missing log lines: {"# export HYPERACTOR_DEFAULT_ENCODING=serde_multipart", ...}
    #[cfg_attr(not(fbcode_build), ignore)]
    fn test_from_env() {
        // Set environment variables
        // SAFETY: TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("HYPERACTOR_CODEC_MAX_FRAME_LENGTH", "1024") };
        // SAFETY: TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT", "60s") };

        let config = from_env();

        assert_eq!(config[CODEC_MAX_FRAME_LENGTH], 1024);
        assert_eq!(config[MESSAGE_DELIVERY_TIMEOUT], Duration::from_mins(1));
        assert_eq!(
            config[MESSAGE_ACK_TIME_INTERVAL],
            Duration::from_millis(500)
        ); // Default value

        let expected_lines: HashSet<&str> = indoc! {"
            # export HYPERACTOR_MESSAGE_LATENCY_SAMPLING_RATE=0.01
            # export HYPERACTOR_CHANNEL_NET_RX_BUFFER_FULL_CHECK_INTERVAL=5s
            # export HYPERACTOR_REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL=5m
            # export HYPERACTOR_STOP_ACTOR_TIMEOUT=10s
            # export HYPERACTOR_SPLIT_MAX_BUFFER_SIZE=5
            # export HYPERACTOR_MESSAGE_TTL_DEFAULT=64
            # export HYPERACTOR_MESSAGE_ACK_EVERY_N_MESSAGES=1000
            # export HYPERACTOR_MESSAGE_ACK_TIME_INTERVAL=500ms
            # export HYPERACTOR_PROCESS_EXIT_TIMEOUT=10s
            # export HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT=30s
            export HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT=1m
            # export HYPERACTOR_CODEC_MAX_FRAME_LENGTH=10737418240
            export HYPERACTOR_CODEC_MAX_FRAME_LENGTH=1024
            # export HYPERACTOR_CLEANUP_TIMEOUT=3s
            # export HYPERACTOR_SPLIT_MAX_BUFFER_AGE=50ms
            # export HYPERACTOR_DEFAULT_ENCODING=serde_multipart
            # export HYPERACTOR_HOST_SPAWN_READY_TIMEOUT=30s
        "}
        .trim_end()
        .lines()
        .collect();

        // For some reason, logs_contaqin fails to find these lines individually
        // (possibly to do with the fact that we have newlines in our log entries);
        // instead, we test it manually.
        logs_assert(|logged_lines: &[&str]| {
            let mut expected_lines = expected_lines.clone(); // this is an `Fn` closure
            for logged in logged_lines {
                expected_lines.remove(logged);
            }

            if expected_lines.is_empty() {
                Ok(())
            } else {
                Err(format!("missing log lines: {:?}", expected_lines))
            }
        });

        // Clean up
        // SAFETY: TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("HYPERACTOR_CODEC_MAX_FRAME_LENGTH") };
        // SAFETY: TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT_SECS") };
    }

    #[test]
    fn test_defaults() {
        // Test that empty config now returns defaults via get_or_default
        let config = Attrs::new();

        // Verify that the config is empty (no values explicitly set)
        assert!(config.is_empty());

        // But getters should still return the defaults from the keys
        assert_eq!(
            config[CODEC_MAX_FRAME_LENGTH],
            CODEC_MAX_FRAME_LENGTH_DEFAULT
        );
        assert_eq!(config[MESSAGE_DELIVERY_TIMEOUT], Duration::from_secs(30));
        assert_eq!(
            config[MESSAGE_ACK_TIME_INTERVAL],
            Duration::from_millis(500)
        );
        assert_eq!(config[MESSAGE_ACK_EVERY_N_MESSAGES], 1000);
        assert_eq!(config[SPLIT_MAX_BUFFER_SIZE], 5);

        // Verify the keys have defaults
        assert!(CODEC_MAX_FRAME_LENGTH.has_default());
        assert!(MESSAGE_DELIVERY_TIMEOUT.has_default());
        assert!(MESSAGE_ACK_TIME_INTERVAL.has_default());
        assert!(MESSAGE_ACK_EVERY_N_MESSAGES.has_default());
        assert!(SPLIT_MAX_BUFFER_SIZE.has_default());

        // Verify we can get defaults directly from keys
        assert_eq!(
            CODEC_MAX_FRAME_LENGTH.default(),
            Some(&(CODEC_MAX_FRAME_LENGTH_DEFAULT))
        );
        assert_eq!(
            MESSAGE_DELIVERY_TIMEOUT.default(),
            Some(&Duration::from_secs(30))
        );
        assert_eq!(
            MESSAGE_ACK_TIME_INTERVAL.default(),
            Some(&Duration::from_millis(500))
        );
        assert_eq!(MESSAGE_ACK_EVERY_N_MESSAGES.default(), Some(&1000));
        assert_eq!(SPLIT_MAX_BUFFER_SIZE.default(), Some(&5));
    }

    #[test]
    fn test_serialization_only_includes_set_values() {
        let mut config = Attrs::new();

        // Initially empty, serialization should be empty
        let serialized = serde_json::to_string(&config).unwrap();
        assert_eq!(serialized, "{}");

        config[CODEC_MAX_FRAME_LENGTH] = 1024;

        let serialized = serde_json::to_string(&config).unwrap();
        assert!(serialized.contains("codec_max_frame_length"));
        assert!(!serialized.contains("message_delivery_timeout")); // Default not serialized

        // Deserialize back
        let restored_config: Attrs = serde_json::from_str(&serialized).unwrap();

        // Custom value should be preserved
        assert_eq!(restored_config[CODEC_MAX_FRAME_LENGTH], 1024);

        // Defaults should still work for other values
        assert_eq!(
            restored_config[MESSAGE_DELIVERY_TIMEOUT],
            Duration::from_secs(30)
        );
    }
}
