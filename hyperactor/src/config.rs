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

use crate::attrs::Attrs;
use crate::attrs::declare_attrs;
use crate::data::Encoding;

// Declare configuration keys using the new attrs system with defaults
declare_attrs! {
    /// Maximum frame length for codec
    pub attr CODEC_MAX_FRAME_LENGTH: usize = 10 * 1024 * 1024 * 1024; // 10 GiB

    /// Message delivery timeout
    pub attr MESSAGE_DELIVERY_TIMEOUT: Duration = Duration::from_secs(30);

    /// Timeout used by allocator for stopping a proc.
    pub attr PROCESS_EXIT_TIMEOUT: Duration = Duration::from_secs(10);

    /// Message acknowledgment interval
    pub attr MESSAGE_ACK_TIME_INTERVAL: Duration = Duration::from_millis(500);

    /// Number of messages after which to send an acknowledgment
    pub attr MESSAGE_ACK_EVERY_N_MESSAGES: u64 = 1000;

    /// Default hop Time-To-Live for message envelopes.
    pub attr MESSAGE_TTL_DEFAULT : u8 = 64;

    /// Maximum buffer size for split port messages
    pub attr SPLIT_MAX_BUFFER_SIZE: usize = 5;

    /// Timeout used by proc mesh for stopping an actor.
    pub attr STOP_ACTOR_TIMEOUT: Duration = Duration::from_secs(1);

    /// Heartbeat interval for remote allocator
    pub attr REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

    /// The default encoding to be used.
    pub attr DEFAULT_ENCODING: Encoding = Encoding::Multipart;

    /// Whether to use multipart encoding for network channel communications.
    pub attr CHANNEL_MULTIPART: bool = true;

    /// How often to check for full MSPC channel on NetRx.
    pub attr CHANNEL_NET_RX_BUFFER_FULL_CHECK_INTERVAL: Duration = Duration::from_secs(5);
}

/// Load configuration from environment variables
pub fn from_env() -> Attrs {
    let mut config = Attrs::new();

    // Load codec max frame length
    if let Ok(val) = env::var("HYPERACTOR_CODEC_MAX_FRAME_LENGTH") {
        if let Ok(parsed) = val.parse::<usize>() {
            tracing::info!("overriding CODEC_MAX_FRAME_LENGTH to {}", parsed);
            config[CODEC_MAX_FRAME_LENGTH] = parsed;
        }
    }

    // Load message delivery timeout
    if let Ok(val) = env::var("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT_SECS") {
        if let Ok(parsed) = val.parse::<u64>() {
            tracing::info!("overriding MESSAGE_DELIVERY_TIMEOUT to {}", parsed);
            config[MESSAGE_DELIVERY_TIMEOUT] = Duration::from_secs(parsed);
        }
    }

    // Load message ack time interval
    if let Ok(val) = env::var("HYPERACTOR_MESSAGE_ACK_TIME_INTERVAL_MS") {
        if let Ok(parsed) = val.parse::<u64>() {
            tracing::info!("overriding MESSAGE_ACK_TIME_INTERVAL to {}", parsed);
            config[MESSAGE_ACK_TIME_INTERVAL] = Duration::from_millis(parsed);
        }
    }

    // Load message ttl default
    if let Ok(val) = env::var("HYPERACTOR_MESSAGE_TTL_DEFAULT") {
        if let Ok(parsed) = val.parse::<u8>() {
            tracing::info!("overriding MESSAGE_TTL_DEFAULT to {}", parsed);
            config[MESSAGE_TTL_DEFAULT] = parsed;
        }
    }

    // Load message ack every n messages
    if let Ok(val) = env::var("HYPERACTOR_MESSAGE_ACK_EVERY_N_MESSAGES") {
        if let Ok(parsed) = val.parse::<u64>() {
            tracing::info!("overriding MESSAGE_ACK_EVERY_N_MESSAGES to {}", parsed);
            config[MESSAGE_ACK_EVERY_N_MESSAGES] = parsed;
        }
    }

    // Load split max buffer size
    if let Ok(val) = env::var("HYPERACTOR_SPLIT_MAX_BUFFER_SIZE") {
        if let Ok(parsed) = val.parse::<usize>() {
            tracing::info!("overriding SPLIT_MAX_BUFFER_SIZE to {}", parsed);
            config[SPLIT_MAX_BUFFER_SIZE] = parsed;
        }
    }

    // Load remote allocator heartbeat interval
    if let Ok(val) = env::var("HYPERACTOR_REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL_SECS") {
        if let Ok(parsed) = val.parse::<u64>() {
            tracing::info!(
                "overriding REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL to {}",
                parsed
            );
            config[REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL] = Duration::from_secs(parsed);
        }
    }

    // Load default encoding
    if let Ok(val) = env::var("HYPERACTOR_DEFAULT_ENCODING") {
        if let Ok(parsed) = val.parse::<Encoding>() {
            tracing::info!("overriding DEFAULT_ENCODING to {}", parsed);
            config[DEFAULT_ENCODING] = parsed;
        }
    }

    // Load channel multipart.
    if let Ok(val) = env::var("HYPERACTOR_CHANNEL_MULTIPART") {
        if let Ok(parsed) = val.parse::<bool>() {
            tracing::info!("overriding CHANNEL_MULTIPART to {}", parsed);
            config[CHANNEL_MULTIPART] = parsed;
        }
    }

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

/// Merge with another configuration, with the other taking precedence
pub fn merge(config: &mut Attrs, other: &Attrs) {
    if other.contains_key(CODEC_MAX_FRAME_LENGTH) {
        config[CODEC_MAX_FRAME_LENGTH] = other[CODEC_MAX_FRAME_LENGTH];
    }
    if other.contains_key(MESSAGE_DELIVERY_TIMEOUT) {
        config[MESSAGE_DELIVERY_TIMEOUT] = other[MESSAGE_DELIVERY_TIMEOUT];
    }
    if other.contains_key(MESSAGE_ACK_TIME_INTERVAL) {
        config[MESSAGE_ACK_TIME_INTERVAL] = other[MESSAGE_ACK_TIME_INTERVAL];
    }
    if other.contains_key(MESSAGE_ACK_EVERY_N_MESSAGES) {
        config[MESSAGE_ACK_EVERY_N_MESSAGES] = other[MESSAGE_ACK_EVERY_N_MESSAGES];
    }
    if other.contains_key(MESSAGE_TTL_DEFAULT) {
        config[MESSAGE_TTL_DEFAULT] = other[MESSAGE_TTL_DEFAULT];
    }
    if other.contains_key(SPLIT_MAX_BUFFER_SIZE) {
        config[SPLIT_MAX_BUFFER_SIZE] = other[SPLIT_MAX_BUFFER_SIZE];
    }
    if other.contains_key(REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL) {
        config[REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL] = other[REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL];
    }
    if other.contains_key(DEFAULT_ENCODING) {
        config[DEFAULT_ENCODING] = other[DEFAULT_ENCODING];
    }
    if other.contains_key(CHANNEL_MULTIPART) {
        config[CHANNEL_MULTIPART] = other[CHANNEL_MULTIPART];
    }
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
    pub fn get<
        T: Send
            + Sync
            + Copy
            + serde::Serialize
            + serde::de::DeserializeOwned
            + crate::data::Named
            + 'static,
    >(
        key: Key<T>,
    ) -> T {
        *CONFIG.read().unwrap().get(key).unwrap()
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
        pub fn override_key<
            'a,
            T: Send
                + Sync
                + serde::Serialize
                + serde::de::DeserializeOwned
                + crate::data::Named
                + Clone
                + 'static,
        >(
            &'a self,
            key: crate::attrs::Key<T>,
            value: T,
        ) -> ConfigValueGuard<'a, T> {
            let orig = {
                let mut config = CONFIG.write().unwrap();
                let orig = config.take_value(key);
                config.set(key, value);
                orig
            };

            ConfigValueGuard {
                key,
                orig,
                _phantom: PhantomData,
            }
        }
    }

    /// A guard that restores a single configuration value when dropped
    pub struct ConfigValueGuard<'a, T: 'static> {
        key: crate::attrs::Key<T>,
        orig: Option<Box<dyn crate::attrs::SerializableValue>>,
        // This is here so we can hold onto a 'a lifetime.
        _phantom: PhantomData<&'a ()>,
    }

    impl<T: 'static> Drop for ConfigValueGuard<'_, T> {
        fn drop(&mut self) {
            let mut config = CONFIG.write().unwrap();
            if let Some(orig) = self.orig.take() {
                config.restore_value(self.key, orig);
            } else {
                config.remove_value(self.key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
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

    #[test]
    fn test_from_env() {
        // Set environment variables
        // SAFETY: TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("HYPERACTOR_CODEC_MAX_FRAME_LENGTH", "1024") };
        // SAFETY: TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT_SECS", "60") };

        let config = from_env();

        assert_eq!(config[CODEC_MAX_FRAME_LENGTH], 1024);
        assert_eq!(config[MESSAGE_DELIVERY_TIMEOUT], Duration::from_secs(60));
        assert_eq!(
            config[MESSAGE_ACK_TIME_INTERVAL],
            Duration::from_millis(500)
        ); // Default value

        // Clean up
        // SAFETY: TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("HYPERACTOR_CODEC_MAX_FRAME_LENGTH") };
        // SAFETY: TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT_SECS") };
    }

    #[test]
    fn test_merge() {
        let mut config1 = Attrs::new();
        let mut config2 = Attrs::new();
        config2[CODEC_MAX_FRAME_LENGTH] = 1024;
        config2[MESSAGE_DELIVERY_TIMEOUT] = Duration::from_secs(60);

        merge(&mut config1, &config2);

        assert_eq!(config1[CODEC_MAX_FRAME_LENGTH], 1024);
        assert_eq!(config1[MESSAGE_DELIVERY_TIMEOUT], Duration::from_secs(60));
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
        {
            let _guard1 = config.override_key(CODEC_MAX_FRAME_LENGTH, 4096);
            let _guard2 = config.override_key(MESSAGE_DELIVERY_TIMEOUT, Duration::from_secs(60));

            assert_eq!(global::get(CODEC_MAX_FRAME_LENGTH), 4096);
            assert_eq!(
                global::get(MESSAGE_DELIVERY_TIMEOUT),
                Duration::from_secs(60)
            );
        }

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
