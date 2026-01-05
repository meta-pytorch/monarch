/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Global layered configuration for Hyperactor.
//!
//! This module provides the process-wide configuration store and APIs
//! to access it. Configuration values are resolved via a **layered
//! model**: `TestOverride → Env → Runtime → File → ClientOverride →
//! Default`.
//!
//! - Reads (`get`, `get_cloned`) consult layers in that order, falling
//!   back to defaults if no explicit value is set.
//! - `attrs()` returns a complete snapshot of all CONFIG-marked keys at
//!   call time: it materializes defaults for keys not set in any layer.
//!   Keys without @meta(CONFIG = …) are excluded.
//! - In tests, `lock()` and `override_key` allow temporary overrides
//!   that are removed automatically when the guard drops.
//! - In normal operation, a parent process can capture its effective
//!   config via `attrs()` and pass that snapshot to a child during
//!   bootstrap. The child installs it as a `ClientOverride` layer.
//!   Note that Env and Runtime layers will take precedence over this
//!   inherited configuration.
//!
//! This design provides flexibility (easy test overrides, runtime
//! updates, YAML/Env baselines) while ensuring type safety and
//! predictable resolution order.
//!
//! # Testing
//!
//! Tests can override global configuration using [`lock`]. This
//! ensures such tests are serialized (and cannot clobber each other's
//! overrides).
//!
//! ```ignore
//! #[test]
//! fn test_my_feature() {
//!     let config = hyperactor::config::global::lock();
//!     let _guard = config.override_key(SOME_CONFIG_KEY, test_value);
//!     // ... test logic here ...
//! }
//! ```
use std::collections::HashMap;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::RwLock;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use crate::CONFIG;
use crate::attrs::AttrKeyInfo;
use crate::attrs::AttrValue;
use crate::attrs::Attrs;
use crate::attrs::Key;
use crate::from_env;
use crate::from_yaml;

/// Configuration source layers in priority order.
///
/// Resolution order is always: **TestOverride -> Env -> Runtime
/// -> File -> ClientOverride -> Default**.
///
/// Smaller `priority()` number = higher precedence.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Source {
    /// Values set by the config snapshot sent from the client
    /// during proc bootstrap.
    ClientOverride,
    /// Values loaded from configuration files (e.g., YAML).
    File,
    /// Values read from environment variables at process startup.
    /// Higher priority than Runtime, File, and ClientOverride, but
    /// lower than TestOverride.
    Env,
    /// Values set programmatically at runtime. High-priority layer
    /// but overridden by Env and TestOverride.
    Runtime,
    /// Ephemeral values inserted by tests via
    /// `ConfigLock::override_key`. Always wins over all other
    /// sources; removed when the guard drops.
    TestOverride,
}

/// Return the numeric priority for a source.
///
/// Smaller number = higher precedence. Matches the documented order:
/// TestOverride (0) -> Env (1) -> Runtime (2) -> File (3) ->
/// ClientOverride (4).
fn priority(s: Source) -> u8 {
    match s {
        Source::TestOverride => 0,
        Source::Env => 1,
        Source::Runtime => 2,
        Source::File => 3,
        Source::ClientOverride => 4,
    }
}

/// The full set of configuration layers in priority order.
///
/// `Layers` wraps a vector of [`Layer`]s, always kept sorted by
/// [`priority`] (lowest number = highest precedence).
///
/// Resolution (`get`, `get_cloned`, `attrs`) consults `ordered` from
/// front to back, returning the first value found for each key and
/// falling back to defaults if none are set in any layer.
struct Layers {
    /// Kept sorted by `priority` (lowest number first = highest
    /// priority).
    ordered: Vec<Layer>,
}

/// A single configuration layer in the global configuration model.
///
/// Layers are consulted in priority order (`TestOverride → Env →
/// Runtime → File → ClientOverride → Default`) when resolving
/// configuration values. Each variant holds an [`Attrs`] map of
/// key/value pairs.
///
/// The `TestOverride` variant additionally maintains per-key override
/// stacks to support nested and out-of-order test overrides.
///
/// Variants:
/// - [`Layer::ClientOverride`] - Values set by the config snapshot
///   sent from the client during proc bootstrap.
/// - [`Layer::File`] — Values loaded from configuration files.
/// - [`Layer::Env`] — Values sourced from process environment
///   variables.
/// - [`Layer::Runtime`] — Programmatically set runtime overrides.
/// - [`Layer::TestOverride`] — Temporary in-test overrides applied
///   under [`ConfigLock`].
///
/// Layers are stored in [`Layers::ordered`], kept sorted by their
/// effective [`Source`] priority (`TestOverride` first, `Default`
/// last).
enum Layer {
    /// Values set by the config snapshot sent from the client
    /// during proc bootstrap.
    ClientOverride(Attrs),

    /// Values loaded from configuration files.
    File(Attrs),

    /// Values read from process environment variables. Typically
    /// installed at startup via [`init_from_env`].
    Env(Attrs),

    /// Values set programmatically at runtime. High-priority layer
    /// used for dynamic updates (e.g., Python `configure()` API), but
    /// overridden by Env and TestOverride layers.
    Runtime(Attrs),

    /// Ephemeral values inserted during tests via
    /// [`ConfigLock::override_key`]. Always takes precedence over all
    /// other layers. Holds both the active `attrs` map and a per-key
    /// `stacks` table to support nested and out-of-order test
    /// overrides.
    TestOverride {
        attrs: Attrs,
        stacks: HashMap<&'static str, OverrideStack>,
    },
}

/// A per-key stack of test overrides used by the
/// [`Layer::TestOverride`] layer.
///
/// Each stack tracks the sequence of active overrides applied to a
/// single configuration key. The topmost frame represents the
/// currently effective override; earlier frames represent older
/// (still live) guards that may drop out of order.
///
/// Fields:
/// - `env_var`: The associated process environment variable name, if
///   any.
/// - `saved_env`: The original environment variable value before the
///   first override was applied (or `None` if it did not exist).
/// - `frames`: The stack of active override frames, with the top
///   being the last element in the vector.
struct OverrideStack {
    /// The name of the process environment variable associated with
    /// this configuration key, if any.
    ///
    /// Used to mirror changes to the environment when overrides are
    /// applied or removed. `None` if the key has no
    /// `CONFIG.env_name`.
    env_var: Option<String>,

    /// The original value of the environment variable before the
    /// first override was applied.
    ///
    /// Stored so it can be restored once the last frame is dropped.
    /// `None` means the variable did not exist prior to overriding.
    saved_env: Option<String>,

    /// The sequence of active override frames for this key.
    ///
    /// Each frame represents one active test override; the last
    /// element (`frames.last()`) is the current top-of-stack and
    /// defines the effective value seen in the configuration and
    /// environment.
    frames: Vec<OverrideFrame>,
}

/// A single entry in a per-key override stack.
///
/// Each `OverrideFrame` represents one active test override applied
/// via [`ConfigLock::override_key`]. Frames are uniquely identified
/// by a monotonically increasing token and record both the value
/// being overridden and its string form for environment mirroring.
///
/// When a guard drops, its frame is removed from the stack; if it was
/// the top, the next frame (if any) becomes active, or the original
/// environment is restored when the stack becomes empty.
struct OverrideFrame {
    /// A unique, monotonically increasing identifier for this
    /// override frame.
    ///
    /// Used to associate a dropping [`ConfigValueGuard`] with its
    /// corresponding entry in the stack, even if drops occur out of
    /// order.
    token: u64,

    /// The serialized configuration value active while this frame is
    /// on top of its stack.
    ///
    /// Stored as a boxed [`SerializableValue`] to match how values
    /// are kept within [`Attrs`].
    value: Box<dyn crate::attrs::SerializableValue>,

    /// Pre-rendered string form of the value, used for environment
    /// variable updates when this frame becomes active.
    ///
    /// Avoids recomputing `value.display()` on every push or pop.
    env_str: String,
}

/// Return the [`Source`] corresponding to a given [`Layer`].
///
/// This provides a uniform way to retrieve a layer's logical source
/// (File, Env, Runtime, TestOverride, or ClientOverride) regardless
/// of its internal representation. Used for sorting layers by
/// priority and for source-based lookups or removals.
fn layer_source(l: &Layer) -> Source {
    match l {
        Layer::File(_) => Source::File,
        Layer::Env(_) => Source::Env,
        Layer::Runtime(_) => Source::Runtime,
        Layer::TestOverride { .. } => Source::TestOverride,
        Layer::ClientOverride(_) => Source::ClientOverride,
    }
}

/// Return an immutable reference to the [`Attrs`] contained in a
/// [`Layer`].
///
/// This abstracts over the specific layer variant so callers can read
/// configuration values uniformly without needing to pattern-match on
/// the layer type. For `TestOverride`, this returns the current
/// top-level attributes reflecting the active overrides.
fn layer_attrs(l: &Layer) -> &Attrs {
    match l {
        Layer::File(a) | Layer::Env(a) | Layer::Runtime(a) | Layer::ClientOverride(a) => a,
        Layer::TestOverride { attrs, .. } => attrs,
    }
}

/// Return a mutable reference to the [`Attrs`] contained in a
/// [`Layer`].
///
/// This allows callers to modify configuration values within any
/// layer without needing to pattern-match on its variant. For
/// `TestOverride`, the returned [`Attrs`] always reflect the current
/// top-of-stack overrides for each key.
fn layer_attrs_mut(l: &mut Layer) -> &mut Attrs {
    match l {
        Layer::File(a) | Layer::Env(a) | Layer::Runtime(a) | Layer::ClientOverride(a) => a,
        Layer::TestOverride { attrs, .. } => attrs,
    }
}

/// Return the index of the [`Layer::TestOverride`] within the
/// [`Layers`] vector.
///
/// If a TestOverride layer is present, its position in the ordered
/// list is returned; otherwise, `None` is returned. This is used to
/// locate the active test override layer for inserting or restoring
/// temporary configuration values.
fn test_override_index(layers: &Layers) -> Option<usize> {
    layers
        .ordered
        .iter()
        .position(|l| matches!(l, Layer::TestOverride { .. }))
}

/// Global layered configuration store.
///
/// This is the single authoritative store for configuration in the
/// process. It is always present, protected by an `RwLock`, and holds
/// a [`Layers`] struct containing all active sources.
///
/// On startup it is seeded with a single [`Source::Env`] layer
/// (values loaded from process environment variables). Additional
/// layers can be installed later via [`set`] or cleared with
/// [`clear`]. Reads (`get`, `get_cloned`, `attrs`) consult the layers
/// in priority order.
///
/// In tests, a [`Source::TestOverride`] layer is pushed on demand by
/// [`ConfigLock::override_key`]. This layer always takes precedence
/// and is automatically removed when the guard drops.
///
/// In normal operation, a parent process may capture its config with
/// [`attrs`] and pass it to a child during bootstrap. The child
/// installs this snapshot as its [`Source::ClientOverride`] layer,
/// which has the lowest precedence among explicit layers.
static LAYERS: LazyLock<Arc<RwLock<Layers>>> = LazyLock::new(|| {
    let env = from_env();
    let layers = Layers {
        ordered: vec![Layer::Env(env)],
    };
    Arc::new(RwLock::new(layers))
});

/// Monotonically increasing sequence used to assign unique tokens to
/// each test override frame.
///
/// Tokens identify individual [`ConfigValueGuard`] instances within a
/// key's override stack, allowing frames to be removed safely even
/// when guards are dropped out of order. The counter starts at 1 and
/// uses relaxed atomic ordering since exact sequencing across threads
/// is not required—only uniqueness.
static OVERRIDE_TOKEN_SEQ: AtomicU64 = AtomicU64::new(1);

/// Acquire the global configuration lock.
///
/// This lock serializes all mutations of the global configuration,
/// ensuring they cannot clobber each other. It returns a
/// [`ConfigLock`] guard, which must be held for the duration of any
/// mutation (e.g. inserting or overriding values).
///
/// Most commonly used in tests, where it provides exclusive access to
/// push a [`Source::TestOverride`] layer via
/// [`ConfigLock::override_key`]. The override layer is automatically
/// removed when the guard drops, restoring the original state.
///
/// # Example
/// ```rust,ignore
/// let lock = hyperactor::config::global::lock();
/// let _guard = lock.override_key(CONFIG_KEY, "test_value");
/// // Code under test sees the overridden config.
/// // On drop, the key is restored.
/// ```
pub fn lock() -> ConfigLock {
    static MUTEX: LazyLock<std::sync::Mutex<()>> = LazyLock::new(|| std::sync::Mutex::new(()));
    ConfigLock {
        _guard: MUTEX.lock().unwrap(),
    }
}

/// Initialize the global configuration from environment variables.
///
/// Reads values from process environment variables, using each key's
/// `CONFIG.env_name` (from `@meta(CONFIG = ConfigAttr { … })`) to
/// determine its mapping. The resulting values are installed as the
/// [`Source::Env`] layer. Keys without a corresponding environment
/// variable fall back to lower-priority sources or defaults.
///
/// Typically invoked once at process startup to overlay config values
/// from the environment. Repeated calls replace the existing Env
/// layer.
pub fn init_from_env() {
    set(Source::Env, from_env());
}

/// Initialize the global configuration from a YAML file.
///
/// Loads values from the specified YAML file and installs them as the
/// [`Source::File`] layer. During resolution, File is consulted after
/// TestOverride, Env, and Runtime layers, and before ClientOverride
/// and defaults.
///
/// Typically invoked once at process startup to provide a baseline
/// configuration. Repeated calls replace the existing File layer.
pub fn init_from_yaml<P: AsRef<Path>>(path: P) -> Result<(), anyhow::Error> {
    let file = from_yaml(path)?;
    set(Source::File, file);
    Ok(())
}

/// Get a key from the global configuration (Copy types).
///
/// Resolution order: TestOverride -> Env -> Runtime -> File ->
/// ClientOverride -> Default. Panics if the key has no default and is
/// not set in any layer.
pub fn get<T: AttrValue + Copy>(key: Key<T>) -> T {
    let layers = LAYERS.read().unwrap();
    for layer in &layers.ordered {
        let a = layer_attrs(layer);
        if let Some(value) = a.get_value_by_name(key.name()) {
            let t = value.as_any().downcast_ref::<T>().unwrap_or_else(|| {
                panic!(
                    "cannot cast to type {} for key {}",
                    std::any::type_name::<T>(),
                    key.name()
                )
            });
            return *t;
        }
    }
    *key.default().expect("key must have a default")
}

/// Return the override value for `key` if it is explicitly present in
/// `overrides`, otherwise fall back to the global value for that key.
pub fn override_or_global<T: AttrValue + Copy>(overrides: &Attrs, key: Key<T>) -> T {
    if overrides.contains_key(key) {
        *overrides.get(key).unwrap()
    } else {
        get(key)
    }
}

/// Get a key by cloning the value.
///
/// Resolution order: TestOverride -> Env -> Runtime -> File ->
/// ClientOverride -> Default. Panics if the key has no default and
/// is not set in any layer.
pub fn get_cloned<T: AttrValue>(key: Key<T>) -> T {
    try_get_cloned(key)
        .expect("key must have a default")
        .clone()
}

/// Try to get a key by cloning the value.
///
/// Resolution order: TestOverride -> Env -> Runtime -> File ->
/// ClientOverride -> Default. Returns None if the key has no default
/// and is not set in any layer.
pub fn try_get_cloned<T: AttrValue>(key: Key<T>) -> Option<T> {
    let layers = LAYERS.read().unwrap();
    for layer in &layers.ordered {
        let a = layer_attrs(layer);
        if a.contains_key(key) {
            return a.get(key).cloned();
        }
    }
    key.default().cloned()
}

/// Construct a [`Layer`] for the given [`Source`] using the provided
/// `attrs`.
///
/// Used by [`set`] and [`create_or_merge`] when installing a new
/// configuration layer.
fn make_layer(source: Source, attrs: Attrs) -> Layer {
    match source {
        Source::File => Layer::File(attrs),
        Source::Env => Layer::Env(attrs),
        Source::Runtime => Layer::Runtime(attrs),
        Source::TestOverride => Layer::TestOverride {
            attrs,
            stacks: HashMap::new(),
        },
        Source::ClientOverride => Layer::ClientOverride(attrs),
    }
}

/// Insert or replace a configuration layer for the given source.
///
/// If a layer with the same [`Source`] already exists, its contents
/// are replaced with the provided `attrs`. Otherwise a new layer is
/// added. After insertion, layers are re-sorted so that
/// higher-priority sources (e.g. [`Source::TestOverride`],
/// [`Source::Env`]) appear before lower-priority ones
/// ([`Source::Runtime`], [`Source::File`]).
///
/// This function is used by initialization routines (e.g.
/// `init_from_env`, `init_from_yaml`) and by tests when overriding
/// configuration values.
pub fn set(source: Source, attrs: Attrs) {
    let mut g = LAYERS.write().unwrap();
    if let Some(l) = g.ordered.iter_mut().find(|l| layer_source(l) == source) {
        *layer_attrs_mut(l) = attrs;
    } else {
        g.ordered.push(make_layer(source, attrs));
    }
    g.ordered.sort_by_key(|l| priority(layer_source(l))); // TestOverride < Env < Runtime < File < ClientOverride
}

/// Insert or update a configuration layer for the given [`Source`].
///
/// If a layer with the same [`Source`] already exists, its attributes
/// are **updated in place**: all keys present in `attrs` are absorbed
/// into the existing layer, overwriting any previous values for those
/// keys while leaving all other keys in that layer unchanged.
///
/// If no layer for `source` exists yet, this behaves like [`set`]: a
/// new layer is created with the provided `attrs`.
///
/// This is useful for incremental / additive updates (for example,
/// runtime configuration driven by a Python API), where callers want
/// to change a subset of keys without discarding previously installed
/// values in the same layer.
///
/// By contrast, [`set`] replaces the entire layer for `source` with
/// `attrs`, discarding any existing values in that layer.
pub fn create_or_merge(source: Source, attrs: Attrs) {
    let mut g = LAYERS.write().unwrap();
    if let Some(layer) = g.ordered.iter_mut().find(|l| layer_source(l) == source) {
        layer_attrs_mut(layer).merge(attrs);
    } else {
        g.ordered.push(make_layer(source, attrs));
    }
    g.ordered.sort_by_key(|l| priority(layer_source(l))); // TestOverride < Env < Runtime < File < ClientOverride
}

/// Remove the configuration layer for the given [`Source`], if
/// present.
///
/// After this call, values from that source will no longer contribute
/// to resolution in [`get`], [`get_cloned`], or [`attrs`]. Defaults
/// and any remaining layers continue to apply in their normal
/// priority order.
pub fn clear(source: Source) {
    let mut g = LAYERS.write().unwrap();
    g.ordered.retain(|l| layer_source(l) != source);
}

/// Return a complete, merged snapshot of the effective configuration
/// **(only keys marked with `@meta(CONFIG = ...)`)**.
///
/// Resolution per key:
/// 1) First explicit value found in layers (TestOverride → Env →
///    Runtime → File → ClientOverride).
/// 2) Otherwise, the key's default (if any).
///
/// Notes:
/// - This materializes defaults into the returned Attrs for all
///   CONFIG-marked keys, so it's self-contained.
/// - Keys without `CONFIG` meta are excluded.
pub fn attrs() -> Attrs {
    let layers = LAYERS.read().unwrap();
    let mut merged = Attrs::new();

    // Iterate all declared keys (registered via `declare_attrs!` +
    // inventory).
    for info in inventory::iter::<AttrKeyInfo>() {
        // Skip keys not marked as `CONFIG`.
        if info.meta.get(CONFIG).is_none() {
            continue;
        }

        let name = info.name;

        // Try to resolve from highest -> lowest priority layer.
        let mut chosen: Option<Box<dyn crate::attrs::SerializableValue>> = None;
        for layer in &layers.ordered {
            if let Some(v) = layer_attrs(layer).get_value_by_name(name) {
                chosen = Some(v.cloned());
                break;
            }
        }

        // If no explicit value, materialize the default if there is
        // one.
        let boxed = match chosen {
            Some(b) => b,
            None => {
                if let Some(default) = info.default {
                    default.cloned()
                } else {
                    // No explicit value and no default — skip
                    // this key.
                    continue;
                }
            }
        };

        merged.insert_value_by_name_unchecked(name, boxed);
    }

    merged
}

/// Return a snapshot of the attributes for a specific configuration
/// source.
///
/// If a layer with the given [`Source`] exists, this clones and
/// returns its [`Attrs`]. Otherwise an empty [`Attrs`] is returned.
/// The returned map is detached from the global store – mutating it
/// does **not** affect the underlying layer; use [`set`] or
/// [`create_or_merge`] to modify layers.
fn layer_attrs_for(source: Source) -> Attrs {
    let layers = LAYERS.read().unwrap();
    if let Some(layer) = layers.ordered.iter().find(|l| layer_source(l) == source) {
        layer_attrs(layer).clone()
    } else {
        Attrs::new()
    }
}

/// Snapshot the current attributes in the **Runtime** configuration
/// layer.
///
/// This returns a cloned [`Attrs`] containing only values explicitly
/// set in the [`Source::Runtime`] layer (no merging with
/// Env/File/Defaults). If no Runtime layer is present, an empty
/// [`Attrs`] is returned.
pub fn runtime_attrs() -> Attrs {
    layer_attrs_for(Source::Runtime)
}

/// Reset the global configuration to only Defaults (for testing).
///
/// This clears all explicit layers (`File`, `Env`, `Runtime`,
/// `ClientOverride`, and `TestOverride`). Subsequent lookups will
/// resolve keys entirely from their declared defaults.
///
/// Note: Should be called while holding [`global::lock`] in tests, to
/// ensure no concurrent modifications happen.
pub fn reset_to_defaults() {
    let mut g = LAYERS.write().unwrap();
    g.ordered.clear();
}

/// A guard that holds the global configuration lock and provides
/// override functionality.
///
/// This struct acts as both a lock guard (preventing other tests from
/// modifying global config) and as the only way to create
/// configuration overrides. Override guards cannot outlive this
/// ConfigLock, ensuring proper synchronization.
pub struct ConfigLock {
    _guard: std::sync::MutexGuard<'static, ()>,
}

impl ConfigLock {
    /// Create a configuration override that is active until the
    /// returned guard is dropped.
    ///
    /// Each call pushes a new frame onto a per-key override stack
    /// within the [`Source::TestOverride`] layer. The topmost frame
    /// defines the effective value seen by `get()` and in the
    /// mirrored environment variable (if any). When a guard is
    /// dropped, its frame is removed: if it was the top, the previous
    /// frame (if any) becomes active or the key and env var are
    /// restored to their prior state.
    ///
    /// The returned guard must not outlive this [`ConfigLock`].
    pub fn override_key<'a, T: AttrValue>(
        &'a self,
        key: crate::attrs::Key<T>,
        value: T,
    ) -> ConfigValueGuard<'a, T> {
        let token = OVERRIDE_TOKEN_SEQ.fetch_add(1, Ordering::Relaxed);

        let mut g = LAYERS.write().unwrap();

        // Ensure TestOverride layer exists.
        let idx = if let Some(i) = test_override_index(&g) {
            i
        } else {
            g.ordered.push(Layer::TestOverride {
                attrs: Attrs::new(),
                stacks: HashMap::new(),
            });
            g.ordered.sort_by_key(|l| priority(layer_source(l)));
            test_override_index(&g).expect("just inserted TestOverride layer")
        };

        // Mutably access TestOverride's attrs + stacks.
        let (attrs, stacks) = match &mut g.ordered[idx] {
            Layer::TestOverride { attrs, stacks } => (attrs, stacks),
            _ => unreachable!(),
        };

        // Compute env var (if any) for this key once.
        let (env_var, env_str) = if let Some(cfg) = key.attrs().get(crate::CONFIG) {
            if let Some(name) = &cfg.env_name {
                (Some(name.clone()), value.display())
            } else {
                (None, String::new())
            }
        } else {
            (None, String::new())
        };

        // Get per-key stack (by declared name).
        let key_name = key.name();
        let stack = stacks.entry(key_name).or_insert_with(|| OverrideStack {
            env_var: env_var.clone(),
            saved_env: env_var.as_ref().and_then(|n| std::env::var(n).ok()),
            frames: Vec::new(),
        });

        // Push the new frame.
        let boxed: Box<dyn crate::attrs::SerializableValue> = Box::new(value.clone());
        stack.frames.push(OverrideFrame {
            token,
            value: boxed,
            env_str,
        });

        // Make this frame the active value in TestOverride attrs.
        attrs.set(key, value.clone());

        // Update process env to reflect new top-of-stack.
        if let (Some(var), Some(top)) = (stack.env_var.as_ref(), stack.frames.last()) {
            // SAFETY: Under global ConfigLock during tests.
            unsafe { std::env::set_var(var, &top.env_str) }
        }

        ConfigValueGuard {
            key,
            token,
            _phantom: PhantomData,
        }
    }
}

/// When a [`ConfigLock`] is dropped, the special
/// [`Source::TestOverride`] layer (if present) is removed entirely.
/// This discards all temporary overrides created under the lock,
/// ensuring they cannot leak into subsequent tests or callers. Other
/// layers (`Runtime`, `Env`, `File`, `ClientOverride`, and defaults)
/// are left untouched.
///
/// Note: individual values within the TestOverride layer may already
/// have been restored by [`ConfigValueGuard`]s as they drop. This
/// final removal guarantees no residual layer remains once the lock
/// itself is released.
impl Drop for ConfigLock {
    fn drop(&mut self) {
        let mut guard = LAYERS.write().unwrap();
        if let Some(pos) = test_override_index(&guard) {
            guard.ordered.remove(pos);
        }
    }
}

/// A guard that restores a single configuration value when dropped.
pub struct ConfigValueGuard<'a, T: 'static> {
    key: crate::attrs::Key<T>,
    token: u64,
    // This is here so we can hold onto a 'a lifetime.
    _phantom: PhantomData<&'a ()>,
}

/// When a [`ConfigValueGuard`] is dropped, it restores configuration
/// state for the key it was guarding.
///
/// Behavior:
/// - Each key maintains a stack of override frames. The most recent
///   frame (top of stack) defines the effective value in
///   [`Source::TestOverride`].
/// - Dropping a guard removes its frame. If it was the top frame, the
///   next frame (if any) becomes active and both the config and
///   mirrored env var are updated accordingly.
/// - If the dropped frame was not on top, no changes occur until the
///   active frame is dropped.
/// - When the last frame for a key is removed, the key is deleted
///   from the TestOverride layer and its associated environment
///   variable (if any) is restored to its original value or removed
///   if it did not exist.
///
/// This guarantees that nested or out-of-order test overrides are
/// restored deterministically and without leaking state into
/// subsequent tests.
impl<T: 'static> Drop for ConfigValueGuard<'_, T> {
    fn drop(&mut self) {
        let mut g = LAYERS.write().unwrap();
        let i = if let Some(i) = test_override_index(&g) {
            i
        } else {
            return;
        };

        // Access TestOverride internals
        let (attrs, stacks) = match &mut g.ordered[i] {
            Layer::TestOverride { attrs, stacks } => (attrs, stacks),
            _ => unreachable!("TestOverride index points to non-TestOverride layer"),
        };

        let key_name = self.key.name();

        // We need a tiny scope for the &mut borrow of the stack so we
        // can call `stacks.remove(key_name)` afterward if it becomes
        // empty.
        let mut remove_empty_stack = false;
        let mut restore_env_var: Option<String> = None;
        let mut restore_env_to: Option<String> = None;

        if let Some(stack) = stacks.get_mut(key_name) {
            // Find this guard's frame by token.
            if let Some(pos) = stack.frames.iter().position(|f| f.token == self.token) {
                let is_top = pos + 1 == stack.frames.len();

                if is_top {
                    // Pop the active frame
                    stack.frames.pop();

                    if let Some(new_top) = stack.frames.last() {
                        // New top becomes active: update attrs and env.
                        attrs.insert_value(self.key, (*new_top.value).cloned());
                        if let Some(var) = stack.env_var.as_ref() {
                            // SAFETY: Under global ConfigLock during tests.
                            unsafe { std::env::set_var(var, &new_top.env_str) }
                        }
                    } else {
                        // Stack empty: remove the key now, then after
                        // releasing the &mut borrow of the stack,
                        // restore the env var and remove the stack
                        // entry.
                        let _ = attrs.remove_value(self.key);

                        // Capture restoration details while we still
                        // have access to the stack.
                        if let Some(var) = stack.env_var.as_ref() {
                            restore_env_var = Some(var.clone());
                            restore_env_to = stack.saved_env.clone(); // None => unset
                        }
                        remove_empty_stack = true
                    }
                } else {
                    // Out-of-order drop: remove only that frame:
                    // active top stays
                    stack.frames.remove(pos);
                    // No changes to attrs or env here.
                }
            } // else: token already handled; nothing to do
        } // &mut stack borrow ends here

        // If we emptied the stack for this key, restore env and drop
        // the stack entry.
        if remove_empty_stack {
            if let Some(var) = restore_env_var.as_ref() {
                // SAFETY: Under global ConfigLock during tests.
                unsafe {
                    if let Some(val) = restore_env_to.as_ref() {
                        std::env::set_var(var, val);
                    } else {
                        std::env::remove_var(var);
                    }
                }
            }
            // Now it's safe to remove the stack from the map.
            let _ = stacks.remove(key_name);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::ConfigAttr;
    use crate::attrs::declare_attrs;

    // Test configuration keys used to exercise the layered config
    // infrastructure. These mirror hyperactor's config keys but are
    // declared locally to keep hyperactor_config independent.

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

        /// Number of messages after which to send an acknowledgment
        @meta(CONFIG = ConfigAttr {
            env_name: Some("HYPERACTOR_MESSAGE_ACK_EVERY_N_MESSAGES".to_string()),
            py_name: None,
        })
        pub attr MESSAGE_ACK_EVERY_N_MESSAGES: u64 = 1000;

        /// Maximum buffer size for split port messages
        @meta(CONFIG = ConfigAttr {
            env_name: Some("HYPERACTOR_SPLIT_MAX_BUFFER_SIZE".to_string()),
            py_name: None,
        })
        pub attr SPLIT_MAX_BUFFER_SIZE: usize = 5;

        /// Whether to use multipart encoding for network channel communications
        @meta(CONFIG = ConfigAttr {
            env_name: Some("HYPERACTOR_CHANNEL_MULTIPART".to_string()),
            py_name: None,
        })
        pub attr CHANNEL_MULTIPART: bool = true;

        /// Default hop Time-To-Live for message envelopes
        @meta(CONFIG = ConfigAttr {
            env_name: Some("HYPERACTOR_MESSAGE_TTL_DEFAULT".to_string()),
            py_name: None,
        })
        pub attr MESSAGE_TTL_DEFAULT: u8 = 64;

        /// A test key with no environment variable mapping
        @meta(CONFIG = ConfigAttr {
            env_name: None,
            py_name: None,
        })
        pub attr CONFIG_KEY_NO_ENV: u32 = 100;
    }

    #[test]
    fn test_global_config() {
        let config = lock();

        // Reset global config to defaults to avoid interference from
        // other tests
        reset_to_defaults();

        assert_eq!(get(CODEC_MAX_FRAME_LENGTH), CODEC_MAX_FRAME_LENGTH_DEFAULT);
        {
            let _guard = config.override_key(CODEC_MAX_FRAME_LENGTH, 1024);
            assert_eq!(get(CODEC_MAX_FRAME_LENGTH), 1024);
            // The configuration will be automatically restored when
            // _guard goes out of scope
        }

        assert_eq!(get(CODEC_MAX_FRAME_LENGTH), CODEC_MAX_FRAME_LENGTH_DEFAULT);
    }

    #[test]
    fn test_overrides() {
        let config = lock();

        // Reset global config to defaults to avoid interference from
        // other tests
        reset_to_defaults();

        // Test the new lock/override API for individual config values
        assert_eq!(get(CODEC_MAX_FRAME_LENGTH), CODEC_MAX_FRAME_LENGTH_DEFAULT);
        assert_eq!(get(MESSAGE_DELIVERY_TIMEOUT), Duration::from_secs(30));

        // Test single value override
        {
            let _guard = config.override_key(CODEC_MAX_FRAME_LENGTH, 2048);
            assert_eq!(get(CODEC_MAX_FRAME_LENGTH), 2048);
            assert_eq!(get(MESSAGE_DELIVERY_TIMEOUT), Duration::from_secs(30)); // Unchanged
        }

        // Values should be restored after guard is dropped
        assert_eq!(get(CODEC_MAX_FRAME_LENGTH), CODEC_MAX_FRAME_LENGTH_DEFAULT);

        // Test multiple overrides
        let orig_value = std::env::var("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT").ok();
        {
            let _guard1 = config.override_key(CODEC_MAX_FRAME_LENGTH, 4096);
            let _guard2 = config.override_key(MESSAGE_DELIVERY_TIMEOUT, Duration::from_mins(1));

            assert_eq!(get(CODEC_MAX_FRAME_LENGTH), 4096);
            assert_eq!(get(MESSAGE_DELIVERY_TIMEOUT), Duration::from_mins(1));
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
        assert_eq!(get(CODEC_MAX_FRAME_LENGTH), CODEC_MAX_FRAME_LENGTH_DEFAULT);
        assert_eq!(get(MESSAGE_DELIVERY_TIMEOUT), Duration::from_secs(30));
    }

    #[test]
    fn test_layer_precedence_env_over_file_and_replacement() {
        let _lock = lock();
        reset_to_defaults();

        // File sets a value.
        let mut file = Attrs::new();
        file[CODEC_MAX_FRAME_LENGTH] = 1111;
        set(Source::File, file);

        // Env sets a different value.
        let mut env = Attrs::new();
        env[CODEC_MAX_FRAME_LENGTH] = 2222;
        set(Source::Env, env);

        // Env should win over File.
        assert_eq!(get(CODEC_MAX_FRAME_LENGTH), 2222);

        // Replace Env layer with a new value.
        let mut env2 = Attrs::new();
        env2[CODEC_MAX_FRAME_LENGTH] = 3333;
        set(Source::Env, env2);

        assert_eq!(get(CODEC_MAX_FRAME_LENGTH), 3333);
    }

    #[test]
    fn test_layer_precedence_read_file_if_not_found_in_env() {
        let _lock = lock();
        reset_to_defaults();

        // Read the default value because no layers have been set.
        assert_eq!(get(CODEC_MAX_FRAME_LENGTH), 10737418240);

        // File sets a value.
        let mut file = Attrs::new();
        file[CODEC_MAX_FRAME_LENGTH] = 1111;
        set(Source::File, file);

        // Env does not have any attribute.
        let env = Attrs::new();
        set(Source::Env, env);

        // Should read from File.
        assert_eq!(get(CODEC_MAX_FRAME_LENGTH), 1111);

        // Replace Env layer with a new value.
        let mut env2 = Attrs::new();
        env2[CODEC_MAX_FRAME_LENGTH] = 2222;
        set(Source::Env, env2);

        // Env should win over File.
        assert_eq!(get(CODEC_MAX_FRAME_LENGTH), 2222);
    }

    #[test]
    fn test_runtime_overrides_and_clear_restores_lower_layers() {
        let _lock = lock();
        reset_to_defaults();

        // File baseline.
        let mut file = Attrs::new();
        file[MESSAGE_DELIVERY_TIMEOUT] = Duration::from_secs(30);
        set(Source::File, file);

        // Env override.
        let mut env = Attrs::new();
        env[MESSAGE_DELIVERY_TIMEOUT] = Duration::from_secs(40);
        set(Source::Env, env);

        // Runtime layer (but Env beats it).
        let mut rt = Attrs::new();
        rt[MESSAGE_DELIVERY_TIMEOUT] = Duration::from_secs(50);
        set(Source::Runtime, rt);

        assert_eq!(get(MESSAGE_DELIVERY_TIMEOUT), Duration::from_secs(40));

        // Clearing Env should reveal Runtime.
        clear(Source::Env);

        // With the Env layer gone, Runtime wins over File.
        assert_eq!(get(MESSAGE_DELIVERY_TIMEOUT), Duration::from_secs(50));
    }

    #[test]
    fn test_attrs_snapshot_materializes_defaults_and_omits_meta() {
        let _lock = lock();
        reset_to_defaults();

        // No explicit layers: values should come from Defaults.
        let snap = attrs();

        // A few representative defaults are materialized:
        assert_eq!(snap[CODEC_MAX_FRAME_LENGTH], 10 * 1024 * 1024 * 1024);
        assert_eq!(snap[MESSAGE_DELIVERY_TIMEOUT], Duration::from_secs(30));

        // CONFIG has no default and wasn't explicitly set: should be
        // omitted.
        let json = serde_json::to_string(&snap).unwrap();
        assert!(
            !json.contains("hyperactor::config::config"),
            "CONFIG must not appear in snapshot unless explicitly set"
        );
    }

    #[test]
    fn test_parent_child_snapshot_as_clientoverride_layer() {
        let _lock = lock();
        reset_to_defaults();

        // Parent effective config (pretend it's a parent process).
        let mut parent_env = Attrs::new();
        parent_env[MESSAGE_ACK_EVERY_N_MESSAGES] = 12345;
        set(Source::Env, parent_env);

        let parent_snap = attrs();

        // "Child" process: start clean, install parent snapshot as
        // ClientOverride.
        reset_to_defaults();
        set(Source::ClientOverride, parent_snap);

        // Child should observe parent's effective value from the
        // ClientOverride layer (since child has no Env/Runtime/File
        // layers set).
        assert_eq!(get(MESSAGE_ACK_EVERY_N_MESSAGES), 12345);
    }

    #[test]
    fn test_testoverride_layer_override_and_env_restore() {
        let lock = lock();
        reset_to_defaults();

        assert_eq!(get(MESSAGE_DELIVERY_TIMEOUT), Duration::from_secs(30));

        // SAFETY: single-threaded test.
        unsafe {
            std::env::remove_var("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT");
        }

        {
            let _guard = lock.override_key(MESSAGE_DELIVERY_TIMEOUT, Duration::from_secs(99));
            // Override wins:
            assert_eq!(get(MESSAGE_DELIVERY_TIMEOUT), Duration::from_secs(99));

            // Env should be mirrored to the same duration (string may
            // be "1m 39s")
            let s = std::env::var("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT").unwrap();
            let parsed = humantime::parse_duration(&s).unwrap();
            assert_eq!(parsed, Duration::from_secs(99));
        }

        // After drop, value and env restored:
        assert_eq!(get(MESSAGE_DELIVERY_TIMEOUT), Duration::from_secs(30));
        assert!(std::env::var("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT").is_err());
    }

    #[test]
    fn test_reset_to_defaults_clears_all_layers() {
        let _lock = lock();
        reset_to_defaults();

        // Seed multiple layers.
        let mut file = Attrs::new();
        file[SPLIT_MAX_BUFFER_SIZE] = 7;
        set(Source::File, file);

        let mut env = Attrs::new();
        env[SPLIT_MAX_BUFFER_SIZE] = 8;
        set(Source::Env, env);

        let mut rt = Attrs::new();
        rt[SPLIT_MAX_BUFFER_SIZE] = 9;
        set(Source::Runtime, rt);

        // Sanity: Env wins over Runtime and File.
        assert_eq!(get(SPLIT_MAX_BUFFER_SIZE), 8);

        // Reset clears all explicit layers; defaults apply.
        reset_to_defaults();
        assert_eq!(get(SPLIT_MAX_BUFFER_SIZE), 5); // default
    }

    #[test]
    fn test_get_cloned_resolution_matches_get() {
        let _lock = lock();
        reset_to_defaults();

        let mut env = Attrs::new();
        env[MESSAGE_DELIVERY_TIMEOUT] = Duration::from_mins(2);
        set(Source::Env, env);

        assert_eq!(get(MESSAGE_DELIVERY_TIMEOUT), Duration::from_mins(2));
        let v = get_cloned(MESSAGE_DELIVERY_TIMEOUT);
        assert_eq!(v, Duration::from_mins(2));
    }

    #[test]
    fn test_attrs_snapshot_respects_layer_precedence_per_key() {
        let _lock = lock();
        reset_to_defaults();

        let mut file = Attrs::new();
        file[MESSAGE_TTL_DEFAULT] = 10;
        set(Source::File, file);

        let mut env = Attrs::new();
        env[MESSAGE_TTL_DEFAULT] = 20;
        set(Source::Env, env);

        let snap = attrs();
        assert_eq!(snap[MESSAGE_TTL_DEFAULT], 20); // Env beats File
    }

    declare_attrs! {
      @meta(CONFIG = ConfigAttr {
          env_name: None,
          py_name: None,
      })
      pub attr CONFIG_KEY: bool = true;

      pub attr NON_CONFIG_KEY: bool = true;
    }

    #[test]
    fn test_attrs_excludes_non_config_keys() {
        let _lock = lock();
        reset_to_defaults();

        let snap = attrs();
        let json = serde_json::to_string(&snap).unwrap();

        // Expect our CONFIG_KEY to be present.
        assert!(
            json.contains("hyperactor_config::global::tests::config_key"),
            "attrs() should include keys with @meta(CONFIG = ...)"
        );
        // Expect our NON_CONFIG_KEY to be omitted.
        assert!(
            !json.contains("hyperactor_config::global::tests::non_config_key"),
            "attrs() should exclude keys without @meta(CONFIG = ...)"
        );
    }

    #[test]
    fn test_testoverride_multiple_stacked_overrides_lifo() {
        let lock = lock();
        reset_to_defaults();

        // Baseline sanity.
        assert_eq!(get(MESSAGE_DELIVERY_TIMEOUT), Duration::from_secs(30));

        // Start from a clean env so we can assert restoration to "unset".
        // SAFETY: single-threaded tests.
        unsafe {
            std::env::remove_var("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT");
        }
        assert!(std::env::var("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT").is_err());

        // Stack A: 40s (becomes top)
        let guard_a = lock.override_key(MESSAGE_DELIVERY_TIMEOUT, Duration::from_secs(40));
        assert_eq!(get(MESSAGE_DELIVERY_TIMEOUT), Duration::from_secs(40));
        {
            let s = std::env::var("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT").unwrap();
            assert_eq!(
                humantime::parse_duration(&s).unwrap(),
                Duration::from_secs(40)
            );
        }

        // Stack B: 50s (new top)
        let guard_b = lock.override_key(MESSAGE_DELIVERY_TIMEOUT, Duration::from_secs(50));
        assert_eq!(get(MESSAGE_DELIVERY_TIMEOUT), Duration::from_secs(50));
        {
            let s = std::env::var("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT").unwrap();
            assert_eq!(
                humantime::parse_duration(&s).unwrap(),
                Duration::from_secs(50)
            );
        }

        // Drop B first → should reveal A (LIFO)
        std::mem::drop(guard_b);
        assert_eq!(get(MESSAGE_DELIVERY_TIMEOUT), Duration::from_secs(40));
        {
            let s = std::env::var("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT").unwrap();
            assert_eq!(
                humantime::parse_duration(&s).unwrap(),
                Duration::from_secs(40)
            );
        }

        // Drop A → should restore default and unset env.
        std::mem::drop(guard_a);
        assert_eq!(get(MESSAGE_DELIVERY_TIMEOUT), Duration::from_secs(30));
        assert!(std::env::var("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT").is_err());
    }

    #[test]
    fn test_testoverride_out_of_order_drop_keeps_top_stable() {
        let lock = lock();
        reset_to_defaults();

        // Clean env baseline.
        // SAFETY: single-threaded tests.
        unsafe {
            std::env::remove_var("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT");
        }
        assert!(std::env::var("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT").is_err());

        // Push three frames in order: A=40s, B=50s, C=70s (C is top).
        let guard_a = lock.override_key(MESSAGE_DELIVERY_TIMEOUT, Duration::from_secs(40));
        let guard_b = lock.override_key(MESSAGE_DELIVERY_TIMEOUT, Duration::from_secs(50));
        let guard_c = lock.override_key(MESSAGE_DELIVERY_TIMEOUT, Duration::from_secs(70));

        // Top is C.
        assert_eq!(get(MESSAGE_DELIVERY_TIMEOUT), Duration::from_secs(70));
        {
            let s = std::env::var("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT").unwrap();
            assert_eq!(
                humantime::parse_duration(&s).unwrap(),
                Duration::from_secs(70)
            );
        }

        // Drop the *middle* frame (B) first → top must remain C, env unchanged.
        std::mem::drop(guard_b);
        assert_eq!(get(MESSAGE_DELIVERY_TIMEOUT), Duration::from_secs(70));
        {
            let s = std::env::var("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT").unwrap();
            assert_eq!(
                humantime::parse_duration(&s).unwrap(),
                Duration::from_secs(70)
            );
        }

        // Now drop C → A becomes top, env follows A.
        std::mem::drop(guard_c);
        assert_eq!(get(MESSAGE_DELIVERY_TIMEOUT), Duration::from_secs(40));
        {
            let s = std::env::var("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT").unwrap();
            assert_eq!(
                humantime::parse_duration(&s).unwrap(),
                Duration::from_secs(40)
            );
        }

        // Drop A → restore default and clear env.
        std::mem::drop(guard_a);
        assert_eq!(get(MESSAGE_DELIVERY_TIMEOUT), Duration::from_secs(30));
        assert!(std::env::var("HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT").is_err());
    }

    #[test]
    fn test_priority_order() {
        use Source::*;
        assert!(priority(TestOverride) < priority(Env));
        assert!(priority(Env) < priority(Runtime));
        assert!(priority(Runtime) < priority(File));
        assert!(priority(File) < priority(ClientOverride));
    }

    #[test]
    fn test_create_or_merge_runtime_merges_keys() {
        let _lock = lock();
        reset_to_defaults();

        // Seed Runtime with one key.
        let mut rt = Attrs::new();
        rt[MESSAGE_TTL_DEFAULT] = 10;
        set(Source::Runtime, rt);

        // Now update Runtime with a different key via
        // `create_or_merge`.
        let mut update = Attrs::new();
        update[MESSAGE_ACK_EVERY_N_MESSAGES] = 123;
        create_or_merge(Source::Runtime, update);

        // Both keys should now be visible from Runtime.
        assert_eq!(get(MESSAGE_TTL_DEFAULT), 10);
        assert_eq!(get(MESSAGE_ACK_EVERY_N_MESSAGES), 123);
    }

    #[test]
    fn test_create_or_merge_runtime_creates_layer_if_missing() {
        let _lock = lock();
        reset_to_defaults();

        let mut rt = Attrs::new();
        rt[MESSAGE_TTL_DEFAULT] = 42;
        create_or_merge(Source::Runtime, rt);

        assert_eq!(get(MESSAGE_TTL_DEFAULT), 42);
    }

    #[test]
    fn test_clientoverride_precedence_loses_to_all_other_layers() {
        let _lock = lock();
        reset_to_defaults();

        // ClientOverride sets a baseline value.
        let mut client = Attrs::new();
        client[MESSAGE_TTL_DEFAULT] = 10;
        set(Source::ClientOverride, client);
        assert_eq!(get(MESSAGE_TTL_DEFAULT), 10);

        // File should beat ClientOverride.
        let mut file = Attrs::new();
        file[MESSAGE_TTL_DEFAULT] = 20;
        set(Source::File, file);
        assert_eq!(get(MESSAGE_TTL_DEFAULT), 20);

        // Runtime should beat both File and ClientOverride.
        let mut runtime = Attrs::new();
        runtime[MESSAGE_TTL_DEFAULT] = 30;
        set(Source::Runtime, runtime);
        assert_eq!(get(MESSAGE_TTL_DEFAULT), 30);

        // Env should beat Runtime, File, and ClientOverride.
        let mut env = Attrs::new();
        env[MESSAGE_TTL_DEFAULT] = 40;
        set(Source::Env, env);
        assert_eq!(get(MESSAGE_TTL_DEFAULT), 40);

        // Clear higher layers one by one to verify fallback.
        clear(Source::Env);
        assert_eq!(get(MESSAGE_TTL_DEFAULT), 30); // Runtime

        clear(Source::Runtime);
        assert_eq!(get(MESSAGE_TTL_DEFAULT), 20); // File

        clear(Source::File);
        assert_eq!(get(MESSAGE_TTL_DEFAULT), 10); // ClientOverride
    }

    #[test]
    fn test_create_or_merge_clientoverride() {
        let _lock = lock();
        reset_to_defaults();

        // Seed ClientOverride with one key.
        let mut client = Attrs::new();
        client[MESSAGE_TTL_DEFAULT] = 10;
        set(Source::ClientOverride, client);

        // Merge in a different key.
        let mut update = Attrs::new();
        update[MESSAGE_ACK_EVERY_N_MESSAGES] = 123;
        create_or_merge(Source::ClientOverride, update);

        // Both keys should now be visible.
        assert_eq!(get(MESSAGE_TTL_DEFAULT), 10);
        assert_eq!(get(MESSAGE_ACK_EVERY_N_MESSAGES), 123);
    }

    #[test]
    fn test_override_or_global_returns_override_when_present() {
        let _lock = lock();
        reset_to_defaults();

        // Set a global value via Env.
        let mut env = Attrs::new();
        env[MESSAGE_TTL_DEFAULT] = 99;
        set(Source::Env, env);

        // Create an override Attrs with a different value.
        let mut overrides = Attrs::new();
        overrides[MESSAGE_TTL_DEFAULT] = 42;

        // Should return the override value, not global.
        assert_eq!(override_or_global(&overrides, MESSAGE_TTL_DEFAULT), 42);
    }

    #[test]
    fn test_override_or_global_returns_global_when_not_present() {
        let _lock = lock();
        reset_to_defaults();

        // Set a global value via Env.
        let mut env = Attrs::new();
        env[MESSAGE_TTL_DEFAULT] = 99;
        set(Source::Env, env);

        // Empty overrides.
        let overrides = Attrs::new();

        // Should return the global value.
        assert_eq!(override_or_global(&overrides, MESSAGE_TTL_DEFAULT), 99);
    }

    #[test]
    fn test_runtime_attrs_returns_only_runtime_layer() {
        let _lock = lock();
        reset_to_defaults();

        // Set values in multiple layers.
        let mut file = Attrs::new();
        file[MESSAGE_TTL_DEFAULT] = 10;
        set(Source::File, file);

        let mut env = Attrs::new();
        env[SPLIT_MAX_BUFFER_SIZE] = 20;
        set(Source::Env, env);

        let mut runtime = Attrs::new();
        runtime[MESSAGE_ACK_EVERY_N_MESSAGES] = 123;
        set(Source::Runtime, runtime);

        // runtime_attrs() should return only Runtime layer contents.
        let rt = runtime_attrs();

        // Should have the Runtime key.
        assert_eq!(rt[MESSAGE_ACK_EVERY_N_MESSAGES], 123);

        // Should NOT have File or Env keys.
        assert!(!rt.contains_key(MESSAGE_TTL_DEFAULT));
        assert!(!rt.contains_key(SPLIT_MAX_BUFFER_SIZE));
    }

    #[test]
    fn test_override_key_without_env_name_does_not_mirror_to_env() {
        let lock = lock();
        reset_to_defaults();

        // Verify default value.
        assert_eq!(get(CONFIG_KEY_NO_ENV), 100);

        // Override the key (which has no env_name).
        let _guard = lock.override_key(CONFIG_KEY_NO_ENV, 999);

        // Should see the override value.
        assert_eq!(get(CONFIG_KEY_NO_ENV), 999);

        // No env var should have been set (test doesn't crash,
        // behavior is clean). This test mainly ensures no panic
        // occurs during override/restore.

        drop(_guard);

        // Should restore to default.
        assert_eq!(get(CONFIG_KEY_NO_ENV), 100);
    }

    #[test]
    fn test_multiple_different_keys_overridden_simultaneously() {
        let lock = lock();
        reset_to_defaults();

        // SAFETY: single-threaded test.
        unsafe {
            std::env::remove_var("HYPERACTOR_CODEC_MAX_FRAME_LENGTH");
            std::env::remove_var("HYPERACTOR_MESSAGE_TTL_DEFAULT");
        }

        // Override multiple different keys at once.
        let guard1 = lock.override_key(CODEC_MAX_FRAME_LENGTH, 1111);
        let guard2 = lock.override_key(MESSAGE_TTL_DEFAULT, 42);
        let guard3 = lock.override_key(CHANNEL_MULTIPART, false);

        // All should reflect their override values.
        assert_eq!(get(CODEC_MAX_FRAME_LENGTH), 1111);
        assert_eq!(get(MESSAGE_TTL_DEFAULT), 42);
        assert!(!get(CHANNEL_MULTIPART));

        // Env vars should be mirrored.
        assert_eq!(
            std::env::var("HYPERACTOR_CODEC_MAX_FRAME_LENGTH").unwrap(),
            "1111"
        );
        assert_eq!(
            std::env::var("HYPERACTOR_MESSAGE_TTL_DEFAULT").unwrap(),
            "42"
        );

        // Drop guards in arbitrary order.
        drop(guard2); // Drop MESSAGE_TTL_DEFAULT first

        // MESSAGE_TTL_DEFAULT should restore, others should remain.
        assert_eq!(get(MESSAGE_TTL_DEFAULT), MESSAGE_TTL_DEFAULT_DEFAULT);
        assert_eq!(get(CODEC_MAX_FRAME_LENGTH), 1111);
        assert!(!get(CHANNEL_MULTIPART));

        // Env for MESSAGE_TTL_DEFAULT should be cleared.
        assert!(std::env::var("HYPERACTOR_MESSAGE_TTL_DEFAULT").is_err());

        drop(guard1);
        drop(guard3);

        // All should be restored.
        assert_eq!(get(CODEC_MAX_FRAME_LENGTH), CODEC_MAX_FRAME_LENGTH_DEFAULT);
        assert_eq!(get(CHANNEL_MULTIPART), CHANNEL_MULTIPART_DEFAULT);
    }
}
