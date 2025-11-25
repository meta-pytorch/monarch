/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Configuration for Monarch Hyperactor.
//!
//! This module provides monarch-specific configuration attributes that extend
//! the base hyperactor configuration system.

use std::collections::HashMap;
use std::fmt::Debug;

use hyperactor::AttrValue;
use hyperactor::Named;
use hyperactor::attrs::AttrKeyInfo;
use hyperactor::attrs::Attrs;
use hyperactor::attrs::ErasedKey;
use hyperactor::attrs::declare_attrs;
use hyperactor::channel::ChannelTransport;
use hyperactor::config::CONFIG;
use hyperactor::config::ConfigAttr;
use hyperactor::config::global::Source;
use pyo3::conversion::IntoPyObjectExt;
use pyo3::exceptions::PyTypeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::channel::PyChannelTransport;

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
    hyperactor::config::global::init_from_env();
    Ok(())
}

#[pyfunction()]
pub fn reset_config_to_defaults() -> PyResult<()> {
    // Set all config values to defaults, ignoring even environment variables.
    hyperactor::config::global::reset_to_defaults();
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

/// Given a key, get the associated `T`-typed value from the global config, then
/// convert it to a `P`-typed object that can be converted to PyObject, and
/// return that PyObject.
fn get_global_config<'py, P, T>(
    py: Python<'py>,
    key: &'static dyn ErasedKey,
) -> PyResult<Option<PyObject>>
where
    T: AttrValue + TryInto<P>,
    P: IntoPyObjectExt<'py>,
    PyErr: From<<T as TryInto<P>>::Error>,
{
    // Well, it can't fail unless there's a bug in the code in this file.
    let key = key.downcast_ref::<T>().expect("cannot fail");
    let val: Option<P> = hyperactor::config::global::try_get_cloned(key.clone())
        .map(|v| v.try_into())
        .transpose()?;
    val.map(|v| v.into_py_any(py)).transpose()
}

/// Fetch a config value from the **Runtime** layer only and convert
/// it to Python.
///
/// This mirrors [`get_global_config`] but restricts the lookup to the
/// `Source::Runtime` layer (ignoring TestOverride/Env/File/defaults).
/// If the key has a runtime override, it is cloned as `T`, converted
/// to `P`, then to a `PyObject`; otherwise `Ok(None)` is returned.
fn get_runtime_config<'py, P, T>(
    py: Python<'py>,
    key: &'static dyn ErasedKey,
) -> PyResult<Option<PyObject>>
where
    T: AttrValue + TryInto<P>,
    P: IntoPyObjectExt<'py>,
    PyErr: From<<T as TryInto<P>>::Error>,
{
    let key = key.downcast_ref::<T>().expect("cannot fail");
    let runtime = hyperactor::config::global::runtime_attrs();
    let val: Option<P> = runtime
        .get(key.clone())
        .cloned()
        .map(|v| v.try_into())
        .transpose()?;
    val.map(|v| v.into_py_any(py)).transpose()
}

/// Note that this function writes strictly into the `Runtime` layer.
fn set_runtime_config<T: AttrValue + Debug>(key: &'static dyn ErasedKey, value: T) -> PyResult<()> {
    // Again, can't fail unless there's a bug in the code in this file.
    let key = key.downcast_ref().expect("cannot fail");
    let mut attrs = Attrs::new();
    attrs.set(key.clone(), value);
    hyperactor::config::global::create_or_merge(Source::Runtime, attrs);
    Ok(())
}

fn set_runtime_config_from_py_obj(py: Python<'_>, name: &str, val: PyObject) -> PyResult<()> {
    // Get the `ErasedKey` from the kwarg `name` passed to `monarch.configure(...)`.
    let key = match KEY_BY_NAME.get(name) {
        None => {
            return Err(PyValueError::new_err(format!(
                "invalid configuration key: `{}`",
                name
            )));
        }
        Some(key) => *key,
    };

    // Using the typehash from the erased key, get/call the function that can downcast
    // the key and set the value on the global config.
    match TYPEHASH_TO_INFO.get(&key.typehash()) {
        None => Err(PyTypeError::new_err(format!(
            "configuration key `{}` has type `{}`, but configuring with values of this type from Python is not supported.",
            name,
            key.typename()
        ))),
        Some(info) => (info.set_runtime_config)(py, key, val),
    }
}

/// Struct to associate a typehash with functions for getting/setting
/// values in the global config with keys of type `Key<T>`, where
/// `T::typehash() == PythonConfigTypeInfo::typehash()`.
struct PythonConfigTypeInfo {
    typehash: fn() -> u64,

    get_global_config:
        fn(py: Python<'_>, key: &'static dyn ErasedKey) -> PyResult<Option<PyObject>>,

    set_runtime_config:
        fn(py: Python<'_>, key: &'static dyn ErasedKey, val: PyObject) -> PyResult<()>,

    get_runtime_config:
        fn(py: Python<'_>, key: &'static dyn ErasedKey) -> PyResult<Option<PyObject>>,
}

inventory::collect!(PythonConfigTypeInfo);

/// Macro to declare that keys of this type can be configured
/// from python using `monarch.configure(...)`. For types
/// like `String` that are convertible directly to/from PyObjects,
/// you can just use `declare_py_config_type!(String)`. For types
/// that must first be converted to/from a rust python wrapper
/// (e.g., keys with type `ChannelTransport` must use `PyChannelTransport`
/// as an intermediate step), the usage is
/// `declare_py_config_type!(PyChannelTransport as ChannelTransport)`.
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
                            set_runtime_config(key, val)
                        },
                        get_global_config: |py, key| {
                            get_global_config::<$ty, $ty>(py, key)
                        },
                        get_runtime_config: |py, key| {
                            get_runtime_config::<$ty, $ty>(py, key)
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
                        set_runtime_config(key, val)
                    },
                    get_global_config: |py, key| {
                        get_global_config::<$py_ty, $ty>(py, key)
                    },
                    get_runtime_config: |py, key| {
                        get_runtime_config::<$py_ty, $ty>(py, key)
                    }
                }
            }
        }
    };
}

declare_py_config_type!(PyChannelTransport as ChannelTransport);
declare_py_config_type!(
    i8, i16, i32, i64, u8, u16, u32, u64, usize, f32, f64, bool, String
);

/// Iterate over each key-value pair. Attempt to retrieve the `Key<T>`
/// associated with the key and convert the value to `T`, then set
/// them on the global config. The association between kwarg and
/// `Key<T>` is specified using the `CONFIG` meta-attribute.
#[pyfunction]
#[pyo3(signature = (**kwargs))]
fn configure(py: Python<'_>, kwargs: Option<HashMap<String, PyObject>>) -> PyResult<()> {
    kwargs
        .map(|kwargs| {
            kwargs
                .into_iter()
                .try_for_each(|(key, val)| set_runtime_config_from_py_obj(py, &key, val))
        })
        .transpose()?;
    Ok(())
}

/// For all attribute keys whose `@meta(CONFIG = ConfigAttr { py_name:
/// Some(...), .. })` specifies a kwarg name, return the current
/// associated value in the global config. Keys with no value in the
/// global config are omitted from the result.
#[pyfunction]
fn get_configuration(py: Python<'_>) -> PyResult<HashMap<String, PyObject>> {
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
/// This is used by Python's `configured()` context manager to
/// snapshot and restore the Runtime layer for composable, nested
/// configuration overrides:
///
/// ```python
/// prev = get_runtime_configuration()
/// try:
///     configure(**overrides)
///     yield get_configuration()
/// finally:
///     clear_runtime_configuration()
///     configure(**prev)
/// ```
///
/// Unlike `get_configuration()`, which returns the merged view across
/// all layers (File, Env, Runtime, TestOverride), this returns only
/// what's explicitly set in the Runtime layer.
#[pyfunction]
fn get_runtime_configuration(py: Python<'_>) -> PyResult<HashMap<String, PyObject>> {
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
fn clear_runtime_configuration(_py: Python<'_>) -> PyResult<()> {
    hyperactor::config::global::clear(Source::Runtime);
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

    let get_configuration = wrap_pyfunction!(get_configuration, module)?;
    get_configuration.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.config",
    )?;
    module.add_function(get_configuration)?;

    let get_runtime_configuration = wrap_pyfunction!(get_runtime_configuration, module)?;
    get_runtime_configuration.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.config",
    )?;
    module.add_function(get_runtime_configuration)?;

    let clear_runtime_configuration = wrap_pyfunction!(clear_runtime_configuration, module)?;
    clear_runtime_configuration.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.config",
    )?;
    module.add_function(clear_runtime_configuration)?;

    Ok(())
}
