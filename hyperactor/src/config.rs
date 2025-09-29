/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Configuration for Hyperactor.
//!
//! This module provides a centralized way to manage configuration settings for Hyperactor.
//! It uses the attrs system for type-safe, flexible configuration management that supports
//! environment variables, YAML files, and temporary modifications for tests.

use std::env;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::RwLock;
use std::time::Duration;

use shell_quote::QuoteRefExt;

use crate::attrs::AttrKeyInfo;
use crate::attrs::Attrs;
use crate::attrs::SerializableValue;
use crate::attrs::declare_attrs;
use crate::data::Encoding;

// Declare configuration keys using the new attrs system with defaults
declare_attrs! {
    /// This is a meta-attribute specifying the environment variable used by the configuration
    /// key.
    pub attr CONFIG_ENV_VAR: String;

    /// Maximum frame length for codec
    @meta(CONFIG_ENV_VAR = "HYPERACTOR_CODEC_MAX_FRAME_LENGTH".to_string())
    pub attr CODEC_MAX_FRAME_LENGTH: usize = 10 * 1024 * 1024 * 1024; // 10 GiB

    /// Message delivery timeout
    @meta(CONFIG_ENV_VAR = "HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT".to_string())
    pub attr MESSAGE_DELIVERY_TIMEOUT: Duration = Duration::from_secs(30);

    /// Timeout used by allocator for stopping a proc.
    @meta(CONFIG_ENV_VAR = "HYPERACTOR_PROCESS_EXIT_TIMEOUT".to_string())
    pub attr PROCESS_EXIT_TIMEOUT: Duration = Duration::from_secs(10);

    /// Message acknowledgment interval
    @meta(CONFIG_ENV_VAR = "HYPERACTOR_MESSAGE_ACK_TIME_INTERVAL".to_string())
    pub attr MESSAGE_ACK_TIME_INTERVAL: Duration = Duration::from_millis(500);

    /// Number of messages after which to send an acknowledgment
    @meta(CONFIG_ENV_VAR = "HYPERACTOR_MESSAGE_ACK_EVERY_N_MESSAGES".to_string())
    pub attr MESSAGE_ACK_EVERY_N_MESSAGES: u64 = 1000;

    /// Default hop Time-To-Live for message envelopes.
    @meta(CONFIG_ENV_VAR = "HYPERACTOR_MESSAGE_TTL_DEFAULT".to_string())
    pub attr MESSAGE_TTL_DEFAULT : u8 = 64;

    /// Maximum buffer size for split port messages
    @meta(CONFIG_ENV_VAR = "HYPERACTOR_SPLIT_MAX_BUFFER_SIZE".to_string())
    pub attr SPLIT_MAX_BUFFER_SIZE: usize = 5;

    /// Timeout used by proc mesh for stopping an actor.
    @meta(CONFIG_ENV_VAR = "HYPERACTOR_STOP_ACTOR_TIMEOUT".to_string())
    pub attr STOP_ACTOR_TIMEOUT: Duration = Duration::from_secs(1);

    /// Heartbeat interval for remote allocator
    @meta(CONFIG_ENV_VAR = "HYPERACTOR_REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL".to_string())
    pub attr REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

    /// The default encoding to be used.
    @meta(CONFIG_ENV_VAR = "HYPERACTOR_DEFAULT_ENCODING".to_string())
    pub attr DEFAULT_ENCODING: Encoding = Encoding::Multipart;

    /// Whether to use multipart encoding for network channel communications.
    @meta(CONFIG_ENV_VAR = "HYPERACTOR_CHANNEL_MULTIPART".to_string())
    pub attr CHANNEL_MULTIPART: bool = true;

    /// How often to check for full MSPC channel on NetRx.
    @meta(CONFIG_ENV_VAR = "HYPERACTOR_CHANNEL_NET_RX_BUFFER_FULL_CHECK_INTERVAL".to_string())
    pub attr CHANNEL_NET_RX_BUFFER_FULL_CHECK_INTERVAL: Duration = Duration::from_secs(5);

    /// Sampling rate for logging message latency
    /// Set to 0.01 for 1% sampling, 0.1 for 10% sampling, 0.90 for 90% sampling, etc.
    @meta(CONFIG_ENV_VAR = "HYPERACTOR_MESSAGE_LATENCY_SAMPLING_RATE".to_string())
    pub attr MESSAGE_LATENCY_SAMPLING_RATE: f32 = 0.01;

    /// Whether to enable client sequence assignment.
    pub attr ENABLE_CLIENT_SEQ_ASSIGNMENT: bool = false;

    /// Timeout for [`Host::spawn`] to await proc readiness.
    ///
    /// Default: 10 seconds. If set to zero, disables the timeout and
    /// waits indefinitely.
    @meta(CONFIG_ENV_VAR = "HYPERACTOR_HOST_SPAWN_READY_TIMEOUT".to_string())
    pub attr HOST_SPAWN_READY_TIMEOUT: Duration = Duration::from_secs(10);
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
        let Some(env_var) = key.meta.get(CONFIG_ENV_VAR) else {
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

/// Global configuration functions
///
/// This module provides global configuration access and testing utilities.
///
/// # Testing with Global Configuration
///
/// Tests can override global configuration using [`global::lock`]. This ensures that
/// such tests are serialized (and cannot clobber each other's overrides).
///
/// ```ignore rust
/// #[test]
/// fn test_my_feature() {
///     let config = hyperactor::config::global::lock();
///     let _guard = config.override_key(SOME_CONFIG_KEY, test_value);
///     // ... test logic here ...
/// }
/// ```
pub mod global {
    use std::marker::PhantomData;

    use super::*;
    use crate::attrs::AttrValue;
    use crate::attrs::Key;

    /// Global configuration instance, initialized from environment variables.
    static CONFIG: LazyLock<Arc<RwLock<Attrs>>> =
        LazyLock::new(|| Arc::new(RwLock::new(from_env())));

    /// Acquire the global configuration lock for testing.
    ///
    /// This function returns a ConfigLock that acts as both a write lock guard (preventing
    /// other tests from modifying global config concurrently) and as the only way to
    /// create configuration overrides.
    ///
    /// Example usage:
    /// ```ignore rust
    /// let config = hyperactor::config::global::lock();
    /// let _guard = config.override_key(CONFIG_KEY, "value");
    /// // ... test code using the overridden config ...
    /// ```
    pub fn lock() -> ConfigLock {
        static MUTEX: LazyLock<std::sync::Mutex<()>> = LazyLock::new(|| std::sync::Mutex::new(()));
        ConfigLock {
            _guard: MUTEX.lock().unwrap(),
        }
    }

    /// Initialize the global configuration from environment variables
    pub fn init_from_env() {
        let config = from_env();
        let mut global_config = CONFIG.write().unwrap();
        *global_config = config;
    }

    /// Initialize the global configuration from a YAML file
    pub fn init_from_yaml<P: AsRef<Path>>(path: P) -> Result<(), anyhow::Error> {
        let config = from_yaml(path)?;
        let mut global_config = CONFIG.write().unwrap();
        *global_config = config;
        Ok(())
    }

    /// Get a key from the global configuration. Currently only available for Copy types.
    /// `get` assumes that the key has a default value.
    pub fn get<T: AttrValue + Copy>(key: Key<T>) -> T {
        *CONFIG.read().unwrap().get(key).unwrap()
    }

    /// Get a key from the global configuration by cloning the value.
    pub fn get_cloned<T: AttrValue>(key: Key<T>) -> T {
        CONFIG.read().unwrap().get(key).unwrap().clone()
    }

    /// Get the global attrs
    pub fn attrs() -> Attrs {
        CONFIG.read().unwrap().clone()
    }

    /// Reset the global configuration to defaults (for testing only)
    ///
    /// Note: This should be called from within with_test_lock() to ensure thread safety.
    /// Available in all builds to support tests in other crates.
    pub fn reset_to_defaults() {
        let mut config = CONFIG.write().unwrap();
        *config = Attrs::new();
    }

    /// A guard that holds the global configuration lock and provides override functionality.
    ///
    /// This struct acts as both a lock guard (preventing other tests from modifying global config)
    /// and as the only way to create configuration overrides. Override guards cannot outlive
    /// this ConfigLock, ensuring proper synchronization.
    pub struct ConfigLock {
        _guard: std::sync::MutexGuard<'static, ()>,
    }

    impl ConfigLock {
        /// Create a configuration override that will be restored when the guard is dropped.
        ///
        /// The returned guard must not outlive this ConfigLock.
        pub fn override_key<'a, T: AttrValue>(
            &'a self,
            key: crate::attrs::Key<T>,
            value: T,
        ) -> ConfigValueGuard<'a, T> {
            let orig = {
                let mut config = CONFIG.write().unwrap();
                let orig = config.remove_value(key);
                config.set(key, value.clone());
                orig
            };

            let orig_env = if let Some(env_var) = key.attrs().get(CONFIG_ENV_VAR) {
                let orig = std::env::var(env_var).ok();
                // SAFETY: this is used in tests
                unsafe {
                    std::env::set_var(env_var, value.display());
                }
                Some((env_var.clone(), orig))
            } else {
                None
            };

            ConfigValueGuard {
                key,
                orig,
                orig_env,
                _phantom: PhantomData,
            }
        }
    }

    /// A guard that restores a single configuration value when dropped
    pub struct ConfigValueGuard<'a, T: 'static> {
        key: crate::attrs::Key<T>,
        orig: Option<Box<dyn crate::attrs::SerializableValue>>,
        orig_env: Option<(String, Option<String>)>,
        // This is here so we can hold onto a 'a lifetime.
        _phantom: PhantomData<&'a ()>,
    }

    impl<T: 'static> Drop for ConfigValueGuard<'_, T> {
        fn drop(&mut self) {
            let mut config = CONFIG.write().unwrap();
            if let Some(orig) = self.orig.take() {
                config.insert_value(self.key, orig);
            } else {
                config.remove_value(self.key);
            }
            if let Some((key, value)) = self.orig_env.take() {
                if let Some(value) = value {
                    // SAFETY: this is used in tests
                    unsafe {
                        std::env::set_var(key, value);
                    }
                } else {
                    // SAFETY: this is used in tests
                    unsafe {
                        std::env::remove_var(&key);
                    }
                }
            }
        }
    }
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
            # export HYPERACTOR_STOP_ACTOR_TIMEOUT=1s
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
    fn test_global_config() {
        let config = global::lock();

        // Reset global config to defaults to avoid interference from other tests
        global::reset_to_defaults();

        assert_eq!(
            global::get(CODEC_MAX_FRAME_LENGTH),
            CODEC_MAX_FRAME_LENGTH_DEFAULT
        );
        {
            let _guard = config.override_key(CODEC_MAX_FRAME_LENGTH, 1024);
            assert_eq!(global::get(CODEC_MAX_FRAME_LENGTH), 1024);
            // The configuration will be automatically restored when _guard goes out of scope
        }

        assert_eq!(
            global::get(CODEC_MAX_FRAME_LENGTH),
            CODEC_MAX_FRAME_LENGTH_DEFAULT
        );
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

    #[test]
    fn test_overrides() {
        let config = global::lock();

        // Reset global config to defaults to avoid interference from other tests
        global::reset_to_defaults();

        // Test the new lock/override API for individual config values
        assert_eq!(
            global::get(CODEC_MAX_FRAME_LENGTH),
            CODEC_MAX_FRAME_LENGTH_DEFAULT
        );
        assert_eq!(
            global::get(MESSAGE_DELIVERY_TIMEOUT),
            Duration::from_secs(30)
        );

        // Test single value override
        {
            let _guard = config.override_key(CODEC_MAX_FRAME_LENGTH, 2048);
            assert_eq!(global::get(CODEC_MAX_FRAME_LENGTH), 2048);
            assert_eq!(
                global::get(MESSAGE_DELIVERY_TIMEOUT),
                Duration::from_secs(30)
            ); // Unchanged
        }

        // Values should be restored after guard is dropped
        assert_eq!(
            global::get(CODEC_MAX_FRAME_LENGTH),
            CODEC_MAX_FRAME_LENGTH_DEFAULT
        );

        // Test multiple overrides
        let orig_value = std::env::var("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT").ok();
        {
            let _guard1 = config.override_key(CODEC_MAX_FRAME_LENGTH, 4096);
            let _guard2 = config.override_key(MESSAGE_DELIVERY_TIMEOUT, Duration::from_secs(60));

            assert_eq!(global::get(CODEC_MAX_FRAME_LENGTH), 4096);
            assert_eq!(
                global::get(MESSAGE_DELIVERY_TIMEOUT),
                Duration::from_secs(60)
            );
            // This was overridden:
            assert_eq!(
                std::env::var("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT").unwrap(),
                "1m"
            );
        }
        assert_eq!(
            std::env::var("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT").ok(),
            orig_value
        );

        // All values should be restored
        assert_eq!(
            global::get(CODEC_MAX_FRAME_LENGTH),
            CODEC_MAX_FRAME_LENGTH_DEFAULT
        );
        assert_eq!(
            global::get(MESSAGE_DELIVERY_TIMEOUT),
            Duration::from_secs(30)
        );
    }
}
