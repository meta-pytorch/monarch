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
    use super::*;

    #[test]
    fn test_config_attr() {
        let cfg = ConfigAttr {
            env_name: Some("TEST_VAR".to_string()),
            py_name: Some("test_var".to_string()),
        };

        assert_eq!(cfg.env_name, Some("TEST_VAR".to_string()));
        assert_eq!(cfg.py_name, Some("test_var".to_string()));
    }

    #[test]
    fn test_config_attr_typename() {
        assert_eq!(ConfigAttr::typename(), "hyperactor_config::ConfigAttr");
    }
}
