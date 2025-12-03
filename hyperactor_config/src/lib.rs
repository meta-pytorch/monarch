/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Core configuration and attribute infrastructure for Hyperactor.
//!
//! This crate provides the core infrastructure for type-safe configuration
//! management including:
//! - `ConfigAttr`: Metadata for configuration keys
//! - Helper functions to load/save `Attrs` (from env via `from_env`,
//!   from YAML via `from_yaml`, and `to_yaml`)
//! - Global layered configuration store under [`crate::global`]
//!
//! Individual crates should declare their own config keys using `declare_attrs!`
//! and import `ConfigAttr`, `CONFIG`, and other infrastructure from this crate.

use std::env;
use std::fs::File;
use std::io::Read;
use std::path::Path;

use hyperactor_named::Named;
use serde::Deserialize;
use serde::Serialize;
use shell_quote::QuoteRefExt;

pub mod attrs;
pub mod global;

// Re-export commonly used items
pub use attrs::AttrKeyInfo;
pub use attrs::AttrValue;
pub use attrs::Attrs;
pub use attrs::Key;
pub use attrs::SerializableValue;
// Re-export hyperactor_named for macro usage
#[doc(hidden)]
pub use hyperactor_named;
// Re-export macros needed by declare_attrs!
pub use inventory::submit;
pub use paste::paste;

// declare_attrs is already exported via #[macro_export] in attrs.rs

/// Metadata describing how a configuration key is exposed across
/// environments.
///
/// Each `ConfigAttr` entry defines how a Rust configuration key maps
/// to external representations:
///  - `env_name`: the environment variable consulted by
///    [`global::init_from_env()`] when loading configuration.
///  - `py_name`: the Python keyword argument accepted by
///    `monarch.configure(...)` and returned by `get_configuration()`.
///
/// All configuration keys should carry this meta-attribute via
/// `@meta(CONFIG = ConfigAttr { ... })`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConfigAttr {
    /// Environment variable consulted by `global::init_from_env()`.
    pub env_name: Option<String>,

    /// Python kwarg name used by `monarch.configure(...)` and
    /// `get_configuration()`.
    pub py_name: Option<String>,
}

impl Named for ConfigAttr {
    fn typename() -> &'static str {
        "hyperactor_config::ConfigAttr"
    }
}

impl AttrValue for ConfigAttr {
    fn display(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "<invalid ConfigAttr>".into())
    }
    fn parse(s: &str) -> Result<Self, anyhow::Error> {
        Ok(serde_json::from_str(s)?)
    }
}

// Declare the CONFIG meta-attribute
declare_attrs! {
    /// This is a meta-attribute marking a configuration key.
    ///
    /// It carries metadata used to bridge Rust, environment
    /// variables, and Python:
    ///  - `env_name`: environment variable name consulted by
    ///    `global::init_from_env()`.
    ///  - `py_name`: keyword argument name recognized by
    ///    `monarch.configure(...)`.
    ///
    /// All configuration keys should be annotated with this
    /// attribute.
    pub attr CONFIG: ConfigAttr;
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
    use std::net::Ipv4Addr;

    use indoc::indoc;

    use crate::CONFIG;
    use crate::ConfigAttr;
    use crate::attrs::declare_attrs;
    use crate::from_env;
    use crate::from_yaml;
    use crate::to_yaml;

    #[derive(
        Debug,
        Clone,
        Copy,
        PartialEq,
        Eq,
        serde::Serialize,
        serde::Deserialize
    )]
    pub(crate) enum TestMode {
        Development,
        Staging,
        Production,
    }

    impl std::fmt::Display for TestMode {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                TestMode::Development => write!(f, "dev"),
                TestMode::Staging => write!(f, "staging"),
                TestMode::Production => write!(f, "prod"),
            }
        }
    }

    impl std::str::FromStr for TestMode {
        type Err = anyhow::Error;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            match s {
                "dev" => Ok(TestMode::Development),
                "staging" => Ok(TestMode::Staging),
                "prod" => Ok(TestMode::Production),
                _ => Err(anyhow::anyhow!("unknown mode: {}", s)),
            }
        }
    }

    impl hyperactor_named::Named for TestMode {
        fn typename() -> &'static str {
            "hyperactor_config::tests::TestMode"
        }
    }

    impl crate::attrs::AttrValue for TestMode {
        fn display(&self) -> String {
            self.to_string()
        }

        fn parse(s: &str) -> Result<Self, anyhow::Error> {
            s.parse()
        }
    }

    declare_attrs! {
        @meta(CONFIG = ConfigAttr {
            env_name: Some("TEST_USIZE_KEY".to_string()),
            py_name: None,
        })
        pub attr USIZE_KEY: usize = 10;

        @meta(CONFIG = ConfigAttr {
            env_name: Some("TEST_STRING_KEY".to_string()),
            py_name: None,
        })
        pub attr STRING_KEY: String = String::new();

        @meta(CONFIG = ConfigAttr {
            env_name: Some("TEST_BOOL_KEY".to_string()),
            py_name: None,
        })
        pub attr BOOL_KEY: bool = false;

        @meta(CONFIG = ConfigAttr {
            env_name: Some("TEST_I64_KEY".to_string()),
            py_name: None,
        })
        pub attr I64_KEY: i64 = -42;

        @meta(CONFIG = ConfigAttr {
            env_name: Some("TEST_F64_KEY".to_string()),
            py_name: None,
        })
        pub attr F64_KEY: f64 = 3.14;

        @meta(CONFIG = ConfigAttr {
            env_name: Some("TEST_U32_KEY".to_string()),
            py_name: Some("test_u32_key".to_string()),
        })
        pub attr U32_KEY: u32 = 100;

        @meta(CONFIG = ConfigAttr {
            env_name: Some("TEST_DURATION_KEY".to_string()),
            py_name: None,
        })
        pub attr DURATION_KEY: std::time::Duration = std::time::Duration::from_secs(60);

        @meta(CONFIG = ConfigAttr {
            env_name: Some("TEST_MODE_KEY".to_string()),
            py_name: None,
        })
        pub attr MODE_KEY: TestMode = TestMode::Development;

        @meta(CONFIG = ConfigAttr {
            env_name: Some("TEST_IP_KEY".to_string()),
            py_name: None,
        })
        pub attr IP_KEY: Ipv4Addr = Ipv4Addr::new(127, 0, 0, 1);

        @meta(CONFIG = ConfigAttr {
            env_name: Some("TEST_SYSTEMTIME_KEY".to_string()),
            py_name: None,
        })
        pub attr SYSTEMTIME_KEY: std::time::SystemTime = std::time::UNIX_EPOCH;

        @meta(CONFIG = ConfigAttr {
            env_name: None,
            py_name: Some("test_no_env_key".to_string()),
        })
        pub attr NO_ENV_KEY: usize = 999;
    }

    #[tracing_test::traced_test]
    #[test]
    // TODO: OSS: The logs_assert function returned an error: missing log lines: {"# export HYPERACTOR_DEFAULT_ENCODING=serde_multipart", ...}
    #[cfg_attr(not(fbcode_build), ignore)]
    fn test_from_env() {
        // Set environment variables
        // SAFETY: TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("TEST_USIZE_KEY", "1024") };
        // SAFETY: TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("TEST_STRING_KEY", "world") };
        // SAFETY: TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("TEST_BOOL_KEY", "true") };
        // SAFETY: TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("TEST_I64_KEY", "-999") };
        // SAFETY: TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("TEST_F64_KEY", "2.718") };
        // SAFETY: TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("TEST_U32_KEY", "500") };
        // SAFETY: TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("TEST_DURATION_KEY", "5s") };
        // SAFETY: TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("TEST_MODE_KEY", "prod") };
        // SAFETY: TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("TEST_IP_KEY", "192.168.1.1") };
        // SAFETY: TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("TEST_SYSTEMTIME_KEY", "2024-01-15T10:30:00Z") };

        let config = from_env();

        // Verify values loaded from environment
        assert_eq!(config[USIZE_KEY], 1024);
        assert_eq!(config[STRING_KEY], "world");
        assert_eq!(config[BOOL_KEY], true);
        assert_eq!(config[I64_KEY], -999);
        assert_eq!(config[F64_KEY], 2.718);
        assert_eq!(config[U32_KEY], 500);
        assert_eq!(config[DURATION_KEY], std::time::Duration::from_secs(5));
        assert_eq!(config[MODE_KEY], TestMode::Production);
        assert_eq!(config[IP_KEY], Ipv4Addr::new(192, 168, 1, 1));
        assert_eq!(
            config[SYSTEMTIME_KEY],
            std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1705314600) // 2024-01-15T10:30:00Z
        );

        // Verify key without env_name uses default
        assert_eq!(config[NO_ENV_KEY], 999);

        let expected_lines: HashSet<&str> = indoc! {"
            # export TEST_USIZE_KEY=10
            export TEST_USIZE_KEY=1024
            # export TEST_STRING_KEY=''
            export TEST_STRING_KEY=world
            # export TEST_BOOL_KEY=0
            export TEST_BOOL_KEY=1
            # export TEST_I64_KEY=-42
            export TEST_I64_KEY=-999
            # export TEST_F64_KEY=3.14
            export TEST_F64_KEY=2.718
            # export TEST_U32_KEY=100
            export TEST_U32_KEY=500
            # export TEST_DURATION_KEY=1m
            export TEST_DURATION_KEY=5s
            # export TEST_MODE_KEY=dev
            export TEST_MODE_KEY=prod
            # export TEST_IP_KEY=127.0.0.1
            export TEST_IP_KEY=192.168.1.1
        "}
        .trim_end()
        .lines()
        .collect();

        // For some reason, logs_contain fails to find these lines individually
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
        unsafe { std::env::remove_var("TEST_USIZE_KEY") };
        // SAFETY: TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("TEST_STRING_KEY") };
        // SAFETY: TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("TEST_BOOL_KEY") };
        // SAFETY: TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("TEST_I64_KEY") };
        // SAFETY: TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("TEST_F64_KEY") };
        // SAFETY: TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("TEST_U32_KEY") };
        // SAFETY: TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("TEST_DURATION_KEY") };
        // SAFETY: TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("TEST_MODE_KEY") };
        // SAFETY: TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("TEST_IP_KEY") };
        // SAFETY: TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("TEST_SYSTEMTIME_KEY") };
    }

    #[test]
    fn test_yaml_round_trip() {
        let temp_path = std::env::temp_dir().join("test_config.yaml");

        let mut config = crate::Attrs::new();
        config.set(USIZE_KEY, 2048);
        config.set(STRING_KEY, "hello_yaml".to_string());
        config.set(BOOL_KEY, true);
        config.set(I64_KEY, -123);
        config.set(F64_KEY, 1.414);
        config.set(U32_KEY, 777);
        config.set(DURATION_KEY, std::time::Duration::from_secs(120));
        config.set(MODE_KEY, TestMode::Staging);
        config.set(IP_KEY, Ipv4Addr::new(10, 0, 0, 1));
        config.set(
            SYSTEMTIME_KEY,
            std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1609459200),
        );

        to_yaml(&config, &temp_path).unwrap();

        let yaml_content = std::fs::read_to_string(&temp_path).unwrap();

        eprintln!("YAML content:\n{}", yaml_content);

        assert!(yaml_content.contains("2048"));
        assert!(yaml_content.contains("hello_yaml"));
        assert!(yaml_content.contains("Staging"));

        let loaded_config = from_yaml(&temp_path).unwrap();

        assert_eq!(loaded_config[USIZE_KEY], 2048);
        assert_eq!(loaded_config[STRING_KEY], "hello_yaml");
        assert_eq!(loaded_config[BOOL_KEY], true);
        assert_eq!(loaded_config[I64_KEY], -123);
        assert_eq!(loaded_config[F64_KEY], 1.414);
        assert_eq!(loaded_config[U32_KEY], 777);
        assert_eq!(
            loaded_config[DURATION_KEY],
            std::time::Duration::from_secs(120)
        );
        assert_eq!(loaded_config[MODE_KEY], TestMode::Staging);
        assert_eq!(loaded_config[IP_KEY], Ipv4Addr::new(10, 0, 0, 1));
        assert_eq!(
            loaded_config[SYSTEMTIME_KEY],
            std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1609459200)
        );

        let _ = std::fs::remove_file(&temp_path);
    }
}
