/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Configuration keys and I/O for hyperactor.
//!
//! This module declares all config keys (`declare_attrs!`) and
//! provides helpers to load/save `Attrs` (from env via `from_env`,
//! from YAML via `from_yaml`, and `to_yaml`). It also re-exports the
//! process-wide layered store under [`crate::config::global`].
//!
//! For reading/writing the process-global configuration (layered
//! resolution, test overrides), see [`crate::config::global`].

/// Global layered configuration store.
///
/// This submodule defines the process-wide configuration layers
/// (`File`, `Env`, `Runtime`, and `TestOverride`), resolution order,
/// and guard types (`ConfigLock`, `ConfigValueGuard`) used for
/// testing. Use this when you need to read or temporarily override
/// values in the global configuration state.
pub mod global;

use std::env;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::RwLock;
use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;
use shell_quote::QuoteRefExt;

use crate as hyperactor;
use crate::attrs::AttrKeyInfo;
use crate::attrs::AttrValue;
use crate::attrs::Attrs;
use crate::attrs::SerializableValue;
use crate::attrs::declare_attrs;
use crate::data::Encoding; // for macros

/// Metadata describing how a configuration key is exposed across
/// environments.
///
/// Each `ConfigAttr` entry defines how a Rust configuration key maps
/// to external representations:
///  - `env_name`: the environment variable consulted by
///    [`init_from_env()`] when loading configuration.
///  - `py_name`: the Python keyword argument accepted by
///    `monarch.configure(...)` and returned by `get_configuration()`.
///
/// All configuration keys should carry this meta-attribute via
/// `@meta(CONFIG = ConfigAttr { ... })`.
#[derive(Clone, Debug, Serialize, Deserialize, hyperactor::Named)]
pub struct ConfigAttr {
    /// Environment variable consulted by `init_from_env()`.
    pub env_name: Option<String>,

    /// Python kwarg name used by `monarch.configure(...)` and
    /// `get_configuration()`.
    pub py_name: Option<String>,
}

impl AttrValue for ConfigAttr {
    fn display(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "<invalid ConfigAttr>".into())
    }
    fn parse(s: &str) -> Result<Self, anyhow::Error> {
        Ok(serde_json::from_str(s)?)
    }
}

// Declare configuration keys using the new attrs system with defaults
declare_attrs! {
    /// This is a meta-attribute marking a configuration key.
    ///
    /// It carries metadata used to bridge Rust, environment
    /// variables, and Python:
    ///  - `env_name`: environment variable name consulted by
    ///    `init_from_env()`.
    ///  - `py_name`: keyword argument name recognized by
    ///    `monarch.configure(...)`.
    ///
    /// All configuration keys should be annotated with this
    /// attribute.
    pub attr CONFIG: ConfigAttr;

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

    /// Heartbeat interval for remote allocator
    @meta(CONFIG = ConfigAttr {
        env_name: Some("HYPERACTOR_REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL".to_string()),
        py_name: None,
    })
    pub attr REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

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

    /// How often to check for full MSPC channel on NetRx.
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

/// Load configuration from environment variables
pub fn from_env() -> Attrs {
    let mut config = Attrs::new();
    let mut output = String::new();

    fn export(env_var: &str, value: Option<&dyn SerializableValue>) -> String {
        let env_var: String = env_var.quoted(shell_quote::Bash);
        let value: String = value
            .map_or("".to_string(), SerializableValue::display)
            .quoted(shell_quote::Bash);
        format!("export {}={}\n", env_var, value)
    }

    for key in inventory::iter::<AttrKeyInfo>() {
        // Skip keys that are not marked as CONFIG or that do not
        // declare an environment variable mapping. Only CONFIG-marked
        // keys with an `env_name` participate in environment
        // initialization.
        let Some(cfg_meta) = key.meta.get(CONFIG) else {
            continue;
        };
        let Some(env_var) = cfg_meta.env_name.as_deref() else {
            continue;
        };

        let Ok(val) = env::var(env_var) else {
            // Default value
            output.push_str("# ");
            output.push_str(&export(env_var, key.default));
            continue;
        };

        match (key.parse)(&val) {
            Err(e) => {
                tracing::error!(
                    "failed to override config key {} from value \"{}\" in ${}: {})",
                    key.name,
                    val,
                    env_var,
                    e
                );
                output.push_str("# ");
                output.push_str(&export(env_var, key.default));
            }
            Ok(parsed) => {
                output.push_str("# ");
                output.push_str(&export(env_var, key.default));
                output.push_str(&export(env_var, Some(parsed.as_ref())));
                config.insert_value_by_name_unchecked(key.name, parsed);
            }
        }
    }

    tracing::info!(
        "loaded configuration from environment:\n{}",
        output.trim_end()
    );

    config
}

/// Load configuration from a YAML file
pub fn from_yaml<P: AsRef<Path>>(path: P) -> Result<Attrs, anyhow::Error> {
    let mut file = File::open(path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    Ok(serde_yaml::from_str(&contents)?)
}

/// Save configuration to a YAML file
pub fn to_yaml<P: AsRef<Path>>(attrs: &Attrs, path: P) -> Result<(), anyhow::Error> {
    let yaml = serde_yaml::to_string(attrs)?;
    std::fs::write(path, yaml)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

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
            Duration::from_secs(5)
        );
    }

    #[tracing_test::traced_test]
    #[test]
    // TODO: OSS: The logs_assert function returned an error: missing log lines: {"# export HYPERACTOR_DEFAULT_ENCODING=serde_multipart", ...}
    #[cfg_attr(not(feature = "fb"), ignore)]
    fn test_from_env() {
        // Set environment variables
        // SAFETY: TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("HYPERACTOR_CODEC_MAX_FRAME_LENGTH", "1024") };
        // SAFETY: TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT", "60s") };

        let config = from_env();

        assert_eq!(config[CODEC_MAX_FRAME_LENGTH], 1024);
        assert_eq!(config[MESSAGE_DELIVERY_TIMEOUT], Duration::from_secs(60));
        assert_eq!(
            config[MESSAGE_ACK_TIME_INTERVAL],
            Duration::from_millis(500)
        ); // Default value

        let expected_lines: HashSet<&str> = indoc! {"
            # export HYPERACTOR_MESSAGE_LATENCY_SAMPLING_RATE=0.01
            # export HYPERACTOR_CHANNEL_NET_RX_BUFFER_FULL_CHECK_INTERVAL=5s
            # export HYPERACTOR_CHANNEL_MULTIPART=true
            # export HYPERACTOR_DEFAULT_ENCODING=serde_multipart
            # export HYPERACTOR_REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL=5s
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
