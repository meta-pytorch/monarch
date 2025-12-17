/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Configuration bridge for Monarch Hyperactor.
//!
//! This module defines Monarch-specific configuration keys and their
//! Python bindings on top of the core `hyperactor::config::global`
//! system. It wires those keys into the layered config and exposes
//! Python-facing helpers such as `configure(...)`,
//! `get_global_config()`, `get_runtime_config()`, and
//! `clear_runtime_config()`, which together implement the "Runtime"
//! configuration layer used by the Monarch Python API.

use std::collections::HashMap;
use std::fmt::Debug;
use std::time::Duration;

use hyperactor::Named;
use hyperactor::channel::BindSpec;
use hyperactor_config::AttrValue;
use hyperactor_config::CONFIG;
use hyperactor_config::ConfigAttr;
use hyperactor_config::attrs::AttrKeyInfo;
use hyperactor_config::attrs::Attrs;
use hyperactor_config::attrs::ErasedKey;
use hyperactor_config::attrs::declare_attrs;
use hyperactor_config::global::Source;
use pyo3::conversion::IntoPyObject;
use pyo3::conversion::IntoPyObjectExt;
use pyo3::exceptions::PyTypeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::channel::PyBindSpec;

/// Python wrapper for Duration, using humantime format strings.
///
/// This type bridges between Python strings (e.g., "30s", "5m") and
/// Rust's `std::time::Duration`. It uses the `humantime` crate for
/// parsing and formatting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PyDuration(pub Duration);

impl From<PyDuration> for Duration {
    fn from(d: PyDuration) -> Self {
        d.0
    }
}

impl From<Duration> for PyDuration {
    fn from(d: Duration) -> Self {
        PyDuration(d)
    }
}

impl<'py> FromPyObject<'py> for PyDuration {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let s: String = ob.extract()?;
        let duration = humantime::parse_duration(&s).map_err(|e| {
            PyValueError::new_err(format!("Invalid duration format '{}': {}", s, e))
        })?;
        Ok(PyDuration(duration))
    }
}

impl<'py> IntoPyObject<'py> for PyDuration {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let formatted = humantime::format_duration(self.0).to_string();
        formatted.into_bound_py_any(py)
    }
}

// Declare monarch-specific configuration keys
declare_attrs! {
    /// Use a single asyncio runtime for all Python actors, rather than one per actor
    pub attr SHARED_ASYNCIO_RUNTIME: bool = false;
}

/// Python API for configuration management
///
/// Reload configuration from environment variables
#[pyfunction()]
pub fn reload_config_from_env() -> PyResult<()> {
    // Reload the hyperactor global configuration from environment variables
    hyperactor_config::global::init_from_env();
    Ok(())
}

#[pyfunction()]
pub fn reset_config_to_defaults() -> PyResult<()> {
    // Set all config values to defaults, ignoring even environment variables.
    hyperactor_config::global::reset_to_defaults();
    Ok(())
}

/// Map from the kwarg name passed to `monarch.configure(...)` to the
/// `Key<T>` associated with that kwarg. This contains all attribute
/// keys whose `@meta(CONFIG = ConfigAttr { py_name: Some(...), .. })`
/// specifies a kwarg name.
static KEY_BY_NAME: std::sync::LazyLock<HashMap<&'static str, &'static dyn ErasedKey>> =
    std::sync::LazyLock::new(|| {
        inventory::iter::<AttrKeyInfo>()
            .filter_map(|info| {
                info.meta
                    .get(CONFIG)
                    .and_then(|cfg: &ConfigAttr| cfg.py_name.as_deref())
                    .map(|py_name| (py_name, info.erased))
            })
            .collect()
    });

/// Map from typehash to an info struct that can be used to downcast an `ErasedKey`
/// to a concrete `Key<T>` and use it to get/set values in the global configl
static TYPEHASH_TO_INFO: std::sync::LazyLock<HashMap<u64, &'static PythonConfigTypeInfo>> =
    std::sync::LazyLock::new(|| {
        inventory::iter::<PythonConfigTypeInfo>()
            .map(|info| ((info.typehash)(), info))
            .collect()
    });

/// Fetch a config value from the layered global config and convert it
/// to Python.
///
/// Looks up `key` in the full configuration
/// (Defaults/File/Env/Runtime/ TestOverride), clones the `T`-typed
/// value if present, converts it to `P`, then into a `PyObject`. If
/// the key is unset in all layers, returns `Ok(None)`.
fn get_global_config_py<'py, P, T>(
    py: Python<'py>,
    key: &'static dyn ErasedKey,
) -> PyResult<Option<PyObject>>
where
    T: AttrValue + TryInto<P>,
    P: IntoPyObjectExt<'py>,
    PyErr: From<<T as TryInto<P>>::Error>,
{
    // The error case should never happen. If somehow it ever does
    // we'll represent "our typing assumptions are wrong" by returning
    // a PyTypeError rather than a panic.
    let key = key.downcast_ref::<T>().ok_or_else(|| {
        PyTypeError::new_err(format!(
            "internal config type mismatch for key `{}`",
            key.name(),
        ))
    })?;
    let val: Option<P> = hyperactor_config::global::try_get_cloned(key.clone())
        .map(|v| v.try_into())
        .transpose()?;
    val.map(|v| v.into_py_any(py)).transpose()
}

/// Fetch a config value from the **Runtime** layer only and convert
/// it to Python.
///
/// This mirrors [`get_global_config_py`] but restricts the lookup to
/// the `Source::Runtime` layer (ignoring
/// TestOverride/Env/File/defaults). If the key has a runtime
/// override, it is cloned as `T`, converted to `P`, then to a
/// `PyObject`; otherwise `Ok(None)` is returned.
fn get_runtime_config_py<'py, P, T>(
    py: Python<'py>,
    key: &'static dyn ErasedKey,
) -> PyResult<Option<PyObject>>
where
    T: AttrValue + TryInto<P>,
    P: IntoPyObjectExt<'py>,
    PyErr: From<<T as TryInto<P>>::Error>,
{
    let key = key.downcast_ref::<T>().expect("cannot fail");
    let runtime = hyperactor_config::global::runtime_attrs();
    let val: Option<P> = runtime
        .get(key.clone())
        .cloned()
        .map(|v| v.try_into())
        .transpose()?;
    val.map(|v| v.into_py_any(py)).transpose()
}

/// Store a Python-provided config value into the **Runtime** layer.
///
/// This is the write-path for the "Python configuration layer": it
/// takes a typed key/value and merges it into `Source::Runtime` via
/// `create_or_merge`. No other layers
/// (Env/File/TestOverride/Defaults) are affected.
fn set_runtime_config_py<T: AttrValue + Debug>(
    key: &'static dyn ErasedKey,
    value: T,
) -> PyResult<()> {
    // Again, can't fail unless there's a bug in the code in this file.
    let key = key.downcast_ref().expect("cannot fail");
    let mut attrs = Attrs::new();
    attrs.set(key.clone(), value);
    hyperactor_config::global::create_or_merge(Source::Runtime, attrs);
    Ok(())
}

/// Bridge a single Python kwarg into a typed Runtime config update.
///
/// This is the write-path behind `configure(**kwargs)`:
/// - `configure(...)` calls this for each `(name, value)` pair,
/// - we resolve `name` to an erased config key via `KEY_BY_NAME`,
/// - we use the key's `typehash` to find the registered
///   `PythonConfigTypeInfo`,
/// - and finally call its `set_runtime_config` closure, which
///   downcasts the value and forwards to `set_runtime_config_py` to
///   write into the `Source::Runtime` layer.
///
/// Unknown keys or keys without a Python conversion registered result
/// in a `ValueError` / `TypeError` back to Python.
fn configure_kwarg(py: Python<'_>, name: &str, val: PyObject) -> PyResult<()> {
    // Get the `ErasedKey` from the kwarg `name` passed to
    // `monarch.configure(...)`.
    let key = match KEY_BY_NAME.get(name) {
        None => {
            return Err(PyValueError::new_err(format!(
                "invalid configuration key: `{}`",
                name
            )));
        }
        Some(key) => *key,
    };

    // Using the typehash from the erased key, get/call the function
    // that can downcast the key and set the value on the global
    // config.
    match TYPEHASH_TO_INFO.get(&key.typehash()) {
        None => Err(PyTypeError::new_err(format!(
            "configuration key `{}` has type `{}`, but configuring with values of this type from Python is not supported.",
            name,
            key.typename()
        ))),
        Some(info) => (info.set_runtime_config)(py, key, val),
    }
}

/// Per-type adapter for the Python config bridge.
///
/// Each `PythonConfigTypeInfo` provides type-specific get/set logic
/// for a particular `Key<T>` via the type-erased `ErasedKey`
/// interface.
///
/// Since we only have `&'static dyn ErasedKey` at runtime (we don't
/// know `T`), we use **type erasure with recovery via function
/// pointers**: the `declare_py_config_type!` macro bakes the concrete
/// type `T` into each function pointer at compile time, allowing
/// runtime dispatch to recover the type.
///
/// Fields:
/// - `typehash`: Identifies the underlying `T` for runtime lookup
/// - `set_global_config`: Knows how to extract `PyObject` as `T` and
///   write to Runtime layer
/// - `get_global_config`: Reads `T` from merged config (all layers)
///   and converts to `PyObject`
/// - `get_runtime_config`: Reads `T` from Runtime layer only and
///   converts to `PyObject`
///
/// Instances are registered via `inventory` and collected into
/// `TYPEHASH_TO_INFO`, enabling dynamic dispatch by typehash in
/// `configure()` and `get_*_config()`.
struct PythonConfigTypeInfo {
    /// Identifies the underlying `T` (matches `T::typehash()`).
    typehash: fn() -> u64,
    /// Read this key from the merged layered config into a PyObject.
    get_global_config:
        fn(py: Python<'_>, key: &'static dyn ErasedKey) -> PyResult<Option<PyObject>>,
    /// Write a Python value into the Runtime layer for this key.
    set_runtime_config:
        fn(py: Python<'_>, key: &'static dyn ErasedKey, val: PyObject) -> PyResult<()>,
    /// Read this key from the Runtime layer into a PyObject.
    get_runtime_config:
        fn(py: Python<'_>, key: &'static dyn ErasedKey) -> PyResult<Option<PyObject>>,
}

// Collect all `PythonConfigTypeInfo` instances registered by
// `declare_py_config_type!`. These are later gathered into
// `TYPEHASH_TO_INFO` via `inventory::iter()`.
inventory::collect!(PythonConfigTypeInfo);

/// Macro to declare that keys of this type can be configured
/// from python using `monarch.configure(...)`. For types
/// like `String` that are convertible directly to/from PyObjects,
/// you can just use `declare_py_config_type!(String)`. For types
/// that must first be converted to/from a rust python wrapper
/// (e.g., keys with type `BindSpec` must use `PyBindSpec`
/// as an intermediate step), the usage is
/// `declare_py_config_type!(PyBindSpec as BindSpec)`.
macro_rules! declare_py_config_type {
    ($($ty:ty),+ $(,)?) => {
        hyperactor::paste! {
            $(
                hyperactor::submit! {
                    PythonConfigTypeInfo {
                        typehash: $ty::typehash,
                        set_runtime_config: |py, key, val| {
                            let val: $ty = val.extract::<$ty>(py).map_err(|err| PyTypeError::new_err(format!(
                                "invalid value `{}` for configuration key `{}` ({})",
                                val, key.name(), err
                            )))?;
                            set_runtime_config_py(key, val)
                        },
                        get_global_config: |py, key| {
                            get_global_config_py::<$ty, $ty>(py, key)
                        },
                        get_runtime_config: |py, key| {
                            get_runtime_config_py::<$ty, $ty>(py, key)
                        }
                    }
                }
            )+
        }
    };
    ($py_ty:ty as $ty:ty) => {
        hyperactor::paste! {
            hyperactor::submit! {
                PythonConfigTypeInfo {
                    typehash: $ty::typehash,
                    set_runtime_config: |py, key, val| {
                        let val: $ty = val.extract::<$py_ty>(py).map_err(|err| PyTypeError::new_err(format!(
                            "invalid value `{}` for configuration key `{}` ({})",
                            val, key.name(), err
                        )))?.into();
                        set_runtime_config_py(key, val)
                    },
                    get_global_config: |py, key| {
                        get_global_config_py::<$py_ty, $ty>(py, key)
                    },
                    get_runtime_config: |py, key| {
                        get_runtime_config_py::<$py_ty, $ty>(py, key)
                    }
                }
            }
        }
    };
}

declare_py_config_type!(PyBindSpec as BindSpec);
declare_py_config_type!(PyDuration as Duration);
declare_py_config_type!(
    i8, i16, i32, i64, u8, u16, u32, u64, usize, f32, f64, bool, String
);

/// Python entrypoint for `monarch_hyperactor.config.configure(...)`.
///
/// This takes the keyword arguments passed from Python, resolves each
/// kwarg name to a typed config key via `KEY_BY_NAME` (populated from
/// `@meta(CONFIG = ConfigAttr { py_name: Some(...), .. })`), and then
/// uses `configure_kwarg` to downcast the value and write it into the
/// **Runtime** configuration layer.
///
/// In other words, this is the write-path from `configure(**kwargs)`
/// into `Source::Runtime`; other layers
/// (Env/File/TestOverride/Defaults) are untouched.
///
/// The name `configure(...)` is historical – conceptually this is
/// `set_runtime_config(...)` for the Python-owned Runtime layer, but
/// we keep the shorter name for API stability.
#[pyfunction]
#[pyo3(signature = (**kwargs))]
fn configure(py: Python<'_>, kwargs: Option<HashMap<String, PyObject>>) -> PyResult<()> {
    kwargs
        .map(|kwargs| {
            kwargs.into_iter().try_for_each(|(key, val)| {
                // Special handling for default_transport: convert ChannelTransport
                // enum or string to PyBindSpec before processing
                let val = if key == "default_transport" {
                    PyBindSpec::new(val.bind(py))?
                        .into_pyobject(py)?
                        .into_any()
                        .unbind()
                } else {
                    val
                };
                configure_kwarg(py, &key, val)
            })
        })
        .transpose()?;
    Ok(())
}

/// Return a snapshot of the current Hyperactor configuration for
/// Python-exposed keys.
///
/// Iterates over all attribute keys whose `@meta(CONFIG = ConfigAttr
/// { py_name: Some(...), .. })` declares a Python kwarg name, looks
/// up each key in the **layered** global config
/// (Defaults/File/Env/Runtime/TestOverride), and, if set, converts
/// the value to a `PyObject`.
///
/// The result is a plain `HashMap` from kwarg name to value for all
/// such keys that currently have a value in the global config; keys
/// with no value in any layer are omitted.
#[pyfunction]
fn get_global_config(py: Python<'_>) -> PyResult<HashMap<String, PyObject>> {
    KEY_BY_NAME
        .iter()
        .filter_map(|(name, key)| match TYPEHASH_TO_INFO.get(&key.typehash()) {
            None => None,
            Some(info) => match (info.get_global_config)(py, *key) {
                Err(err) => Some(Err(err)),
                Ok(val) => val.map(|val| Ok(((*name).into(), val))),
            },
        })
        .collect()
}

/// Get only the Runtime layer configuration (Python-exposed keys).
///
/// The Runtime layer is effectively the "Python configuration layer",
/// populated exclusively via `configure(**kwargs)` from Python. This
/// function returns only the Python-exposed keys (those with
/// `@meta(CONFIG = ConfigAttr { py_name: Some(...), .. })`) that are
/// currently set in the Runtime layer.
///
/// This can be used to implement a `configured()` context manager to
/// snapshot and restore the Runtime layer for composable, nested
/// configuration overrides:
///
/// ```python
/// prev = get_runtime_config()
/// try:
///     configure(**overrides)
///     yield get_global_config()
/// finally:
///     clear_runtime_config()
///     configure(**prev)
/// ```
///
/// Unlike `get_global_config()`, which returns the merged view across
/// all layers (File, Env, Runtime, TestOverride, defaults), this
/// returns only what's explicitly set in the Runtime layer.
#[pyfunction]
fn get_runtime_config(py: Python<'_>) -> PyResult<HashMap<String, PyObject>> {
    KEY_BY_NAME
        .iter()
        .filter_map(|(name, key)| match TYPEHASH_TO_INFO.get(&key.typehash()) {
            None => None,
            Some(info) => match (info.get_runtime_config)(py, *key) {
                Err(err) => Some(Err(err)),
                Ok(val) => val.map(|val| Ok(((*name).into(), val))),
            },
        })
        .collect()
}

/// Clear runtime configuration overrides.
///
/// This removes all entries from the Runtime config layer for this
/// process. The Runtime layer is exclusively populated via Python's
/// `configure(**kwargs)`, so clearing it is SAFE — it will not
/// destroy configuration from other sources (environment variables,
/// config files, or built-in defaults).
///
/// This is primarily used by Python's `configured()` context manager
/// to restore configuration state after applying temporary overrides.
/// Other layers (Env, File, TestOverride, defaults) are unaffected.
#[pyfunction]
fn clear_runtime_config(_py: Python<'_>) -> PyResult<()> {
    hyperactor_config::global::clear(Source::Runtime);
    Ok(())
}

/// Register Python bindings for the config module
pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let reload = wrap_pyfunction!(reload_config_from_env, module)?;
    reload.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.config",
    )?;
    module.add_function(reload)?;

    let reset = wrap_pyfunction!(reset_config_to_defaults, module)?;
    reset.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.config",
    )?;
    module.add_function(reset)?;

    let configure = wrap_pyfunction!(configure, module)?;
    configure.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.config",
    )?;
    module.add_function(configure)?;

    let get_global_config = wrap_pyfunction!(get_global_config, module)?;
    get_global_config.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.config",
    )?;
    module.add_function(get_global_config)?;

    let get_runtime_config = wrap_pyfunction!(get_runtime_config, module)?;
    get_runtime_config.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.config",
    )?;
    module.add_function(get_runtime_config)?;

    let clear_runtime_config = wrap_pyfunction!(clear_runtime_config, module)?;
    clear_runtime_config.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.config",
    )?;
    module.add_function(clear_runtime_config)?;

    Ok(())
}
