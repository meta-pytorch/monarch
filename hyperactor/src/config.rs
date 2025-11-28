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

use std::time::Duration;

use hyperactor_config::CONFIG;
use hyperactor_config::ConfigAttr;
use hyperactor_config::attrs::declare_attrs;

use crate::data::Encoding;

// Declare hyperactor-specific configuration keys
declare_attrs! {
    /// Maximum frame length for codec
    @meta(CONFIG = ConfigAttr {
        env_name: Some("HYPERACTOR_CODEC_MAX_FRAME_LENGTH".to_string()),
        py_name: None,
    })
    pub attr CODEC_MAX_FRAME_LENGTH: usize = 10 * 1024 * 1024 * 1024; // 10 GiB

    /// Message delivery timeout
    @meta(CONFIG = ConfigAttr {
        env_name: Some("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT".to_string()),
        py_name: None,
    })
    pub attr MESSAGE_DELIVERY_TIMEOUT: Duration = Duration::from_secs(30);

    /// Timeout used by allocator for stopping a proc.
    @meta(CONFIG = ConfigAttr {
        env_name: Some("HYPERACTOR_PROCESS_EXIT_TIMEOUT".to_string()),
        py_name: None,
    })
    pub attr PROCESS_EXIT_TIMEOUT: Duration = Duration::from_secs(10);

    /// Message acknowledgment interval
    @meta(CONFIG = ConfigAttr {
        env_name: Some("HYPERACTOR_MESSAGE_ACK_TIME_INTERVAL".to_string()),
        py_name: None,
    })
    pub attr MESSAGE_ACK_TIME_INTERVAL: Duration = Duration::from_millis(500);

    /// Number of messages after which to send an acknowledgment
    @meta(CONFIG = ConfigAttr {
        env_name: Some("HYPERACTOR_MESSAGE_ACK_EVERY_N_MESSAGES".to_string()),
        py_name: None,
    })
    pub attr MESSAGE_ACK_EVERY_N_MESSAGES: u64 = 1000;

    /// Default hop Time-To-Live for message envelopes.
    @meta(CONFIG = ConfigAttr {
        env_name: Some("HYPERACTOR_MESSAGE_TTL_DEFAULT".to_string()),
        py_name: None,
    })
    pub attr MESSAGE_TTL_DEFAULT : u8 = 64;

    /// Maximum buffer size for split port messages
    @meta(CONFIG = ConfigAttr {
        env_name: Some("HYPERACTOR_SPLIT_MAX_BUFFER_SIZE".to_string()),
        py_name: None,
    })
    pub attr SPLIT_MAX_BUFFER_SIZE: usize = 5;

    /// The maximum time an update can be buffered before being reduced.
    @meta(CONFIG = ConfigAttr {
        env_name: Some("HYPERACTOR_SPLIT_MAX_BUFFER_AGE".to_string()),
        py_name: None,
    })
    pub attr SPLIT_MAX_BUFFER_AGE: Duration = Duration::from_millis(50);

    /// Timeout used by proc mesh for stopping an actor.
    @meta(CONFIG = ConfigAttr {
        env_name: Some("HYPERACTOR_STOP_ACTOR_TIMEOUT".to_string()),
        py_name: None,
    })
    pub attr STOP_ACTOR_TIMEOUT: Duration = Duration::from_secs(10);

    /// Timeout used by proc for running the cleanup callback on an actor.
    /// Should be less than the timeout for STOP_ACTOR_TIMEOUT.
    @meta(CONFIG = ConfigAttr {
        env_name: Some("HYPERACTOR_CLEANUP_TIMEOUT".to_string()),
        py_name: None,
    })
    pub attr CLEANUP_TIMEOUT: Duration = Duration::from_secs(3);

    /// Heartbeat interval for remote allocator. We do not rely on this heartbeat
    /// anymore in v1, and it should be removed after we finishing the v0
    /// deprecation.
    @meta(CONFIG = ConfigAttr {
        env_name: Some("HYPERACTOR_REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL".to_string()),
        py_name: None,
    })
    pub attr REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(300);

    /// The default encoding to be used.
    @meta(CONFIG = ConfigAttr {
        env_name: Some("HYPERACTOR_DEFAULT_ENCODING".to_string()),
        py_name: None,
    })
    pub attr DEFAULT_ENCODING: Encoding = Encoding::Multipart;

    /// Whether to use multipart encoding for network channel communications.
    @meta(CONFIG = ConfigAttr {
        env_name: Some("HYPERACTOR_CHANNEL_MULTIPART".to_string()),
        py_name: None,
    })
    pub attr CHANNEL_MULTIPART: bool = true;

    /// How often to check for full MPSC channel on NetRx.
    @meta(CONFIG = ConfigAttr {
        env_name: Some("HYPERACTOR_CHANNEL_NET_RX_BUFFER_FULL_CHECK_INTERVAL".to_string()),
        py_name: None,
    })
    pub attr CHANNEL_NET_RX_BUFFER_FULL_CHECK_INTERVAL: Duration = Duration::from_secs(5);

    /// Sampling rate for logging message latency
    /// Set to 0.01 for 1% sampling, 0.1 for 10% sampling, 0.90 for 90% sampling, etc.
    @meta(CONFIG = ConfigAttr {
        env_name: Some("HYPERACTOR_MESSAGE_LATENCY_SAMPLING_RATE".to_string()),
        py_name: None,
    })
    pub attr MESSAGE_LATENCY_SAMPLING_RATE: f32 = 0.01;

    /// Whether to enable client sequence assignment.
    pub attr ENABLE_CLIENT_SEQ_ASSIGNMENT: bool = false;

    /// Timeout for [`Host::spawn`] to await proc readiness.
    ///
    /// Default: 30 seconds. If set to zero, disables the timeout and
    /// waits indefinitely.
    @meta(CONFIG = ConfigAttr {
        env_name: Some("HYPERACTOR_HOST_SPAWN_READY_TIMEOUT".to_string()),
        py_name: None,
    })
    pub attr HOST_SPAWN_READY_TIMEOUT: Duration = Duration::from_secs(30);
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use indoc::indoc;

    use super::*;

    const CODEC_MAX_FRAME_LENGTH_DEFAULT: usize = 10 * 1024 * 1024 * 1024;

    #[test]
    fn test_default_config() {
        let config = hyperactor_config::attrs::Attrs::new();
        assert_eq!(
            config[CODEC_MAX_FRAME_LENGTH],
            CODEC_MAX_FRAME_LENGTH_DEFAULT
        );
        assert_eq!(config[MESSAGE_DELIVERY_TIMEOUT], Duration::from_secs(30));
        assert_eq!(
            config[MESSAGE_ACK_TIME_INTERVAL],
            Duration::from_millis(500)
        );
    }

    #[test]
    fn test_config_attr_meta() {
        let meta = CODEC_MAX_FRAME_LENGTH.attrs();
        let cfg = meta.get(CONFIG).unwrap();
        assert_eq!(
            cfg.env_name.as_deref(),
            Some("HYPERACTOR_CODEC_MAX_FRAME_LENGTH")
        );
    }

    #[test]
    fn test_from_yaml() {
        let yaml = indoc! {"
            hyperactor::config::codec_max_frame_length: 1000000
            hyperactor::config::message_delivery_timeout:
              secs: 10
              nanos: 0
        "};
        let config: hyperactor_config::attrs::Attrs = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config[CODEC_MAX_FRAME_LENGTH], 1000000);
        assert_eq!(config[MESSAGE_DELIVERY_TIMEOUT], Duration::from_secs(10));
    }

    #[test]
    fn test_unique_env_names() {
        let mut seen = HashSet::new();
        for key in inventory::iter::<hyperactor_config::attrs::AttrKeyInfo>() {
            if let Some(cfg_meta) = key.meta.get(CONFIG) {
                if let Some(env_name) = &cfg_meta.env_name {
                    assert!(
                        seen.insert(env_name.clone()),
                        "Duplicate environment variable: {}",
                        env_name
                    );
                }
            }
        }
    }
}
