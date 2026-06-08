/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Backend-agnostic ibverbs device abstraction.
//!
//! [`IbvDevice`] owns the per-process state for a single opened RDMA
//! device: an `Arc<IbvContext>`, an [`IbvConfig`], and a map of named
//! `Arc<IbvDomain>`s allocated against the device.
//! Per-backend behavior — claiming a device as the backend's,
//! allocating a domain, and seeding config defaults — is provided by
//! an [`IbvDeviceImpl`].
//!
//! Each [`IbvDeviceImpl`] registers itself via the `inventory` crate
//! (using [`register_ibv_device_impl!`]). At first access,
//! [`DEVICE_NAMES_BY_IMPL`] walks the ibverbs device list once and
//! assigns each device to the first registered impl whose
//! [`IbvDeviceImpl::is_instance`] claims it, caching the resulting
//! `typename() → device-infos` map. [`IbvDevice::open`] consults this
//! map: it returns `None` when `name` is not advertised under
//! `I::typename()`, and otherwise opens the device using the cached
//! [`IbvDeviceInfo`].

use std::collections::HashMap;
use std::ffi::CStr;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::LazyLock;

use typeuri::Named;

use super::domain::IbvDomain;
use super::primitives::IbvConfig;
use super::primitives::IbvDeviceInfo;
use super::primitives::query_device_info;

/// Per-backend driver for [`IbvDevice`]. Concrete impls register
/// themselves via [`register_ibv_device_impl!`].
pub(crate) trait IbvDeviceImpl: Named + Send + Sync + 'static {
    /// Human-readable display name for the backend this impl
    /// drives (e.g., `"mellanox"`, `"efa"`). Surfaced in
    /// diagnostics; not used as a registry key.
    fn backend_name() -> &'static str;

    /// Returns `true` if `ctx` belongs to this backend. Called
    /// transiently, once per ibverbs device, while
    /// [`DEVICE_NAMES_BY_IMPL`] is built.
    fn is_instance(ctx: Arc<IbvContext>) -> bool;

    /// Allocates a protection domain against `ctx` and wraps it in
    /// an [`IbvDomain`]. The returned `Arc<IbvDomain>` is the sole
    /// owner of the PD; the last drop runs `ibv_dealloc_pd`.
    ///
    /// The default implementation calls `ibv_alloc_pd` on
    /// `ctx.as_ptr()` and constructs a plain [`IbvDomain`].
    /// In a followup, we'll add an `IbvDomainImpl` trait to allow for
    /// backend-specific domain implementations.
    fn create_domain(ctx: Arc<IbvContext>) -> anyhow::Result<Arc<IbvDomain>> {
        // SAFETY: `ctx.as_ptr()` is the non-null context owned by
        // the `Arc<IbvContext>` for the duration of this call.
        let pd = unsafe { rdmaxcel_sys::ibv_alloc_pd(ctx.as_ptr()) };
        if pd.is_null() {
            anyhow::bail!("ibv_alloc_pd failed: {}", std::io::Error::last_os_error());
        }
        // TODO: `IbvDomain` should hold the `Arc<IbvContext>`
        // itself so the context outlives the PD by construction;
        // for now we copy the raw pointer and rely on the
        // surrounding [`IbvDevice`] to keep the context alive.
        Ok(Arc::new(IbvDomain {
            context: ctx.as_ptr(),
            pd,
        }))
    }

    /// Seeds an [`IbvConfig`] with backend-appropriate defaults
    /// (e.g., EFA caps `max_send_sge` at 1).
    fn apply_config_defaults(config: &mut IbvConfig);
}

/// Inventory entry submitted by [`register_ibv_device_impl!`] for
/// each concrete [`IbvDeviceImpl`]. Captures function pointers to
/// the impl's `typename()`, `backend_name()`, and `is_instance()`,
/// all erased of the concrete type.
pub struct IbvDeviceImplRegistration {
    typename: fn() -> &'static str,
    backend_name: fn() -> &'static str,
    is_instance: fn(Arc<IbvContext>) -> bool,
}

/// Per-impl entry stored in [`DEVICE_NAMES_BY_IMPL`]. Keyed by
/// `typename()`; carries the human-readable `backend_name` for
/// diagnostics alongside the impl's device infos.
#[derive(Debug)]
struct RegisteredBackend {
    #[expect(
        dead_code,
        reason = "read only via Debug for the IbvDevice::open diagnostic warning"
    )]
    backend_name: &'static str,
    devices: Vec<IbvDeviceInfo>,
}

inventory::collect!(IbvDeviceImplRegistration);

/// Submits an `inventory` entry for a concrete [`IbvDeviceImpl`].
///
/// Place one invocation per impl at module scope. The expansion
/// references `inventory` and `typeuri` by bare crate name, so the
/// calling crate must list both as direct dependencies.
///
/// ```ignore
/// register_ibv_device_impl!(StandardImpl);
/// ```
#[macro_export]
macro_rules! register_ibv_device_impl {
    ($impl_ty:ty) => {
        inventory::submit! {
            $crate::backend::ibverbs::device::IbvDeviceImplRegistration::new(
                <$impl_ty as typeuri::Named>::typename,
                <$impl_ty as $crate::backend::ibverbs::device::IbvDeviceImpl>::backend_name,
                <$impl_ty as $crate::backend::ibverbs::device::IbvDeviceImpl>::is_instance,
            )
        }
    };
}

impl IbvDeviceImplRegistration {
    /// Construct a registration. Visible to the rest of the crate
    /// so the [`register_ibv_device_impl!`] macro can call it from
    /// sibling call sites.
    pub(crate) const fn new(
        typename: fn() -> &'static str,
        backend_name: fn() -> &'static str,
        is_instance: fn(Arc<IbvContext>) -> bool,
    ) -> Self {
        Self {
            typename,
            backend_name,
            is_instance,
        }
    }
}

/// Process-wide map from `IbvDeviceImpl::typename()` to a
/// [`RegisteredBackend`] holding the impl's display name and the
/// device infos it claims. Built once on first access by a single
/// walk of the ibverbs device list, assigning each device to the
/// first registration whose `is_instance` claims it.
static DEVICE_NAMES_BY_IMPL: LazyLock<HashMap<&'static str, RegisteredBackend>> =
    LazyLock::new(|| {
        let registrations: Vec<&IbvDeviceImplRegistration> =
            inventory::iter::<IbvDeviceImplRegistration>().collect();
        let mut by_impl: HashMap<&'static str, RegisteredBackend> = registrations
            .iter()
            .map(|reg| {
                (
                    (reg.typename)(),
                    RegisteredBackend {
                        backend_name: (reg.backend_name)(),
                        devices: Vec::new(),
                    },
                )
            })
            .collect();

        let mut num_devices = 0i32;
        // SAFETY: `ibv_get_device_list` populates `num_devices` and
        // returns either null or a pointer to `num_devices` entries;
        // we free it before returning.
        let device_list = unsafe { rdmaxcel_sys::ibv_get_device_list(&mut num_devices) };
        // A non-null list must be freed even when empty, so only the
        // null case returns early; the loop below is a no-op at zero
        // devices and the `ibv_free_device_list` at the end still runs.
        if device_list.is_null() {
            return by_impl;
        }
        for i in 0..num_devices {
            // SAFETY: `device_list` is non-null with `num_devices`
            // valid entries (checked above).
            let device = unsafe { *device_list.add(i as usize) };
            if device.is_null() {
                continue;
            }
            // SAFETY: `device` is non-null per the check above.
            let raw_ctx = unsafe { rdmaxcel_sys::ibv_open_device(device) };
            if raw_ctx.is_null() {
                continue;
            }
            let ctx = Arc::new(IbvContext(raw_ctx));
            // Assign the device to the first impl that claims it.
            if let Some(reg) = registrations
                .iter()
                .find(|reg| (reg.is_instance)(Arc::clone(&ctx)))
            {
                // SAFETY: `device` and `ctx.as_ptr()` are non-null and
                // `ctx.as_ptr()` was returned by `ibv_open_device(device)`.
                if let Some(info) = unsafe { query_device_info(device, ctx.as_ptr()) } {
                    by_impl
                        .get_mut((reg.typename)())
                        .expect("registration typename present in map")
                        .devices
                        .push(info);
                }
            }
            // `ctx` drops here, closing the transient context.
        }
        // SAFETY: `device_list` was returned by `ibv_get_device_list`
        // above and has not been freed.
        unsafe { rdmaxcel_sys::ibv_free_device_list(device_list) };
        by_impl
    });

/// RAII owner of a raw `ibv_context*`.
///
/// Closes the context in [`Drop`]. Held inside an `Arc` not because
/// clones are expected to outlive the owning [`IbvDevice`], but so
/// the raw pointer can be passed around with a lifetime safeguard
/// where borrow-checked references aren't practical.
pub(crate) struct IbvContext(*mut rdmaxcel_sys::ibv_context);

// SAFETY: libibverbs treats `ibv_context*` as thread-safe for the
// operations we perform (allocation, polling, and the final close).
unsafe impl Send for IbvContext {}
unsafe impl Sync for IbvContext {}

impl IbvContext {
    /// Returns the raw `ibv_context*`. The pointer is valid for
    /// the lifetime of `&self`.
    pub(crate) fn as_ptr(&self) -> *mut rdmaxcel_sys::ibv_context {
        self.0
    }
}

impl Drop for IbvContext {
    fn drop(&mut self) {
        if self.0.is_null() {
            return;
        }
        // SAFETY: `self.0` was returned by `ibv_open_device` in
        // `IbvDevice::open` and has not been closed elsewhere. The
        // intended ownership is a single `Arc<IbvContext>`, whose
        // final `Drop` calls `ibv_close_device` exactly once.
        let result = unsafe { rdmaxcel_sys::ibv_close_device(self.0) };
        if result != 0 {
            tracing::error!(
                "ibv_close_device failed for context {:p}: error code {}",
                self.0,
                result
            );
        }
    }
}

/// An opened RDMA device.
///
/// Owns an `Arc<IbvContext>`, the queried [`IbvDeviceInfo`]
/// metadata, a device-scoped [`IbvDeviceConfig`], and a map of
/// named `Arc<IbvDomain>`s allocated against the device (one PD
/// per name, created lazily by [`Self::get_or_create_domain`]).
///
/// `I` is the backend driver type, parameterizing all per-backend
/// behavior via [`IbvDeviceImpl`].
///
/// Field declaration order is significant: `domains` is declared
/// before `context` so that, on drop, every `Arc<IbvDomain>` in
/// the map releases its PD before the final `Arc<IbvContext>`
/// reference closes the context.
pub(crate) struct IbvDevice<I: IbvDeviceImpl> {
    domains: HashMap<String, Arc<IbvDomain>>,
    device_info: IbvDeviceInfo,
    config: IbvConfig,
    context: Arc<IbvContext>,
    _marker: PhantomData<I>,
}

#[expect(dead_code, reason = "called by manager_actor in follow-up commits")]
impl<I: IbvDeviceImpl> IbvDevice<I> {
    /// Opens `name` under impl `I`, storing `config` as-is. Returns
    /// `None` if `name` is not one of the devices registered for
    /// `I::typename()`; otherwise returns an `IbvDevice<I>` with the
    /// device's queried [`IbvDeviceInfo`] and the given [`IbvConfig`].
    ///
    /// # Panics
    ///
    /// Panics if a registered device cannot be opened — e.g. it
    /// disappeared from the system between registration and open.
    /// Such a failure indicates a broken driver invariant rather
    /// than a runtime condition the caller can recover from.
    pub(crate) fn open(name: &str, config: IbvConfig) -> Option<Self> {
        let device_info = DEVICE_NAMES_BY_IMPL
            .get(I::typename())
            .and_then(|entry| entry.devices.iter().find(|d| d.name() == name).cloned());
        let device_info = match device_info {
            Some(info) => info,
            None => {
                tracing::warn!(
                    "ibv device {} not found under backend {}; available: {:?}",
                    name,
                    I::backend_name(),
                    *DEVICE_NAMES_BY_IMPL,
                );
                return None;
            }
        };

        // The registry already confirmed `name` belongs to `I`, so
        // reopen the device by name; a miss here is a broken invariant.
        let mut num_devices = 0i32;
        // SAFETY: `ibv_get_device_list` populates `num_devices` and
        // returns either null or a pointer to `num_devices` entries;
        // we free it before returning.
        let device_list = unsafe { rdmaxcel_sys::ibv_get_device_list(&mut num_devices) };
        assert!(
            !device_list.is_null(),
            "RDMA device list was null while opening registered device {}",
            name,
        );
        if num_devices == 0 {
            // A non-null list must be freed even when empty, per the
            // libibverbs contract.
            // SAFETY: `device_list` is non-null (asserted above) and was
            // returned by `ibv_get_device_list`.
            unsafe { rdmaxcel_sys::ibv_free_device_list(device_list) };
            panic!(
                "no RDMA devices found while opening registered device {}",
                name
            );
        }
        let mut target = std::ptr::null_mut();
        for i in 0..num_devices {
            // SAFETY: `device_list` is non-null with `num_devices`
            // valid entries (checked above).
            let device = unsafe { *device_list.add(i as usize) };
            if device.is_null() {
                continue;
            }
            // SAFETY: `device` is non-null per the check above;
            // `ibv_get_device_name` returns a null-terminated C string
            // owned by the device list.
            let dev_name = unsafe { CStr::from_ptr(rdmaxcel_sys::ibv_get_device_name(device)) };
            if dev_name.to_bytes() == name.as_bytes() {
                target = device;
                break;
            }
        }
        let mut failure = None;
        let mut context: *mut crate::rdmaxcel_sys::ibv_context = std::ptr::null_mut();
        if target.is_null() {
            failure = Some(format!(
                "ibv device with name {} not found by ibv_get_device_list",
                name
            ));
        } else {
            // Open while `target` still points into `device_list`, then free.
            // SAFETY: `target`, when non-null, is one of `device_list`'s
            // entries; `ibv_open_device` returns null on failure.
            context = unsafe { rdmaxcel_sys::ibv_open_device(target) };
            if context.is_null() {
                failure = Some(format!(
                    "registered ibv device {} could not be opened: {}",
                    name,
                    std::io::Error::last_os_error(),
                ));
            }
        }

        // SAFETY: `device_list` was returned by `ibv_get_device_list`
        // above and has not been freed.
        unsafe { rdmaxcel_sys::ibv_free_device_list(device_list) };

        if let Some(failure) = failure {
            panic!("{}", failure);
        }

        Some(Self {
            domains: HashMap::new(),
            device_info,
            config,
            context: Arc::new(IbvContext(context)),
            _marker: PhantomData,
        })
    }

    /// Returns the `Arc<IbvContext>` for this device.
    pub(crate) fn context(&self) -> Arc<IbvContext> {
        Arc::clone(&self.context)
    }

    /// Returns the queried [`IbvDeviceInfo`] metadata for this
    /// device.
    pub(crate) fn device_info(&self) -> &IbvDeviceInfo {
        &self.device_info
    }

    /// Returns the [`IbvConfig`] this device was opened with.
    pub(crate) fn config(&self) -> &IbvConfig {
        &self.config
    }

    /// Returns the `Arc<IbvDomain>` registered under `name`,
    /// creating (and caching) a new one via
    /// [`IbvDeviceImpl::create_domain`] on first access.
    pub(crate) fn get_or_create_domain(&mut self, name: &str) -> anyhow::Result<Arc<IbvDomain>> {
        if let Some(domain) = self.domains.get(name) {
            return Ok(Arc::clone(domain));
        }
        let domain = I::create_domain(Arc::clone(&self.context))?;
        self.domains.insert(name.to_string(), Arc::clone(&domain));
        Ok(domain)
    }
}
