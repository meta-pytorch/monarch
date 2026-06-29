/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! # Ibverbs Manager
//!
//! Contains ibverbs-specific RDMA logic.
//!
//! Manages ibverbs resources including:
//! - Memory registration (CPU and CUDA via dmabuf or segment scanning)
//! - Queue pair creation and connection establishment
//! - RDMA domain and protection domain management
//! - Device selection and PCI-to-RDMA device mapping

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::Endpoint as _;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::OncePortHandle;
use hyperactor::OncePortRef;
use hyperactor::PortHandle;
use hyperactor::RefClient;
use hyperactor::actor::Referable;
use hyperactor::context::Mailbox;
use hyperactor::mailbox::OncePortReceiver;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use super::IbvBuffer;
use super::IbvOp;
use super::device::IbvDevice;
use super::device::IbvDeviceImpl;
use super::device_selection::resolve_target;
use super::domain::IbvDomain;
use super::domain::IbvDomainImpl;
use super::efa_device::EfaDevice;
use super::memory_region::IbvMemoryRegionView;
use super::mlx_device::MlxDevice;
use super::primitives::IbvConfig;
use super::primitives::IbvDeviceInfo;
use super::primitives::IbvQpInfo;
use super::primitives::ibverbs_supported;
use super::queue_pair::IbvQueuePair;
use super::queue_pair::OpResult;
use super::queue_pair::ProcessOps;
use super::queue_pair::QpKey;
use super::queue_pair::QueuePairActor;
use crate::RdmaOp;
use crate::RdmaTransportLevel;
use crate::backend::RdmaBackend;
use crate::backend::RdmaConfig;
use crate::backend::ResolveRemoteBackendContext;
use crate::local_memory::KeepaliveLocalMemory;
use crate::local_memory::is_device_ptr;
use crate::rdma_components::RdmaRemoteBuffer;
use crate::rdma_manager_actor::RdmaManagerActor;
use crate::validate_execution_context;

/// Cross-proc message: the active side asks the peer's manager to
/// create and connect a mirror QP for an in-flight [`QueuePairActor`].
/// Generic over the manager actor type so test code can swap in a
/// mock.
#[derive(Debug, Serialize, Deserialize, Named)]
#[serde(bound(serialize = "", deserialize = ""))]
pub(super) struct CreatePeerQueuePair<M: Referable> {
    /// The active side's manager.
    pub(super) sender: ActorRef<M>,
    /// Device the active side picked for its QP.
    pub(super) sender_device: String,
    /// Device the peer should create its mirror QP on.
    pub(super) receiver_device: String,
    /// Active side's endpoint, captured right after QP creation.
    pub(super) sender_info: IbvQpInfo,
    /// One-shot reply carrying the peer's endpoint, or an error.
    pub(super) reply: OncePortRef<Result<IbvQpInfo, String>>,
}
wirevalue::register_type!(CreatePeerQueuePair<IbvManagerActor<MlxDevice>>);
wirevalue::register_type!(CreatePeerQueuePair<IbvManagerActor<EfaDevice>>);

/// Local-only message: submit a batch of RDMA ops for end-to-end
/// execution. The manager iterates the batch, resolves each op's
/// local MR via [`IbvManagerActor::resolve_local_mr`], looks up
/// (or spawns) the active-side [`QueuePairActor`] for the op's
/// [`QpKey`], and immediately dispatches a one-item [`ProcessOps`]
/// to that QP — so the QP can start posting op `i` while the
/// manager resolves the MR for op `i+1`.
///
/// Per-op completion notifications stream back on `reply` as
/// [`OpResult`] values.
pub(super) struct SubmitOps<I: IbvDeviceImpl> {
    pub(super) ops: Vec<IbvOp<IbvManagerActor<I>>>,
    pub(super) reply: PortHandle<OpResult>,
}

/// Local-only message: create an [`IbvQueuePair`] on `self_device`,
/// drive a handshake with `peer` (whose mirror QP lands on
/// `peer_device`), and return the connected QP. Lets doorbell tests
/// and the `cuda_ping_pong` example poke a real QP without going
/// through [`QueuePairActor`].
pub struct RawQueuePair<I: IbvDeviceImpl> {
    pub peer: ActorRef<IbvManagerActor<I>>,
    pub self_device: String,
    pub peer_device: String,
    pub reply: OncePortHandle<Result<<I::Domain as IbvDomainImpl>::QueuePair, String>>,
}

/// Cross-proc messages handled by [`IbvManagerActor`].
#[derive(Handler, HandleClient, RefClient, Debug, Serialize, Deserialize, Named)]
pub enum IbvManagerMessage {
    /// Release a buffer registration by `remote_buf_id`. Fire-and-forget
    /// (no reply port) to avoid blocking the caller during teardown.
    ReleaseBuffer { remote_buf_id: usize },
}
wirevalue::register_type!(IbvManagerMessage);

/// Local-only messages for [`IbvManagerActor`].
#[derive(Handler, HandleClient, Debug)]
pub enum IbvManagerLocalMessage {
    /// Register a remote-facing buffer's MR and return its
    /// [`IbvBuffer`]. Called by
    /// [`crate::rdma_manager_actor::RdmaManagerActor::request_buffer`]
    /// at buffer-creation time.
    ///
    /// The MR lives in [`IbvManagerActor::buffer_registrations`] and
    /// is deregistered on [`IbvManagerMessage::ReleaseBuffer`].
    RegisterRemoteBuffer {
        remote_buf_id: usize,
        local: KeepaliveLocalMemory,
        #[reply]
        reply: OncePortHandle<Result<IbvBuffer, String>>,
    },
}

/// Default key used for the per-device protection domain inside
/// each [`IbvDevice<I>`] entry of [`IbvManagerActor::devices`].
const DEFAULT_DOMAIN: &str = "default";

/// Manages all ibverbs-specific RDMA resources and operations.
///
/// This struct handles memory registration, queue pair management,
/// and connection establishment using the ibverbs API.
///
/// Generic over `I: IbvDeviceImpl` so the same actor implementation
/// drives every concrete backend (`IbvManagerActor<MlxDevice>`,
/// `IbvManagerActor<EfaDevice>`, ...).
#[derive(Debug)]
#[hyperactor::export(
    handlers = [
        IbvManagerMessage,
        CreatePeerQueuePair<IbvManagerActor<I>>,
    ],
)]
pub struct IbvManagerActor<I: IbvDeviceImpl> {
    owner: OnceLock<ActorHandle<RdmaManagerActor>>,

    /// Active-side [`QueuePairActor`] children, keyed from this
    /// manager's perspective. Lazily populated on the first
    /// [`SubmitOps`] that targets a new `(self_device, peer,
    /// other_device)` triple.
    qp_handles: HashMap<
        QpKey,
        ActorHandle<QueuePairActor<IbvManagerActor<I>, <I::Domain as IbvDomainImpl>::QueuePair>>,
    >,

    /// Passive-side mirror QPs, created in response to a peer's
    /// [`CreatePeerQueuePair`]. The peer's [`QueuePairActor`] owns
    /// the active side; we hold the connected mirror here so the
    /// peer can read/write our memory. The map's `Drop` destroys
    /// each QP via its own `Drop`.
    peer_created_qps: HashMap<QpKey, <I::Domain as IbvDomainImpl>::QueuePair>,

    /// Map of RDMA device names to their opened [`IbvDevice<I>`], each of
    /// which owns the per-device `Arc<IbvContext>` and the `DEFAULT_DOMAIN`
    /// `Arc<IbvDomain>`.
    devices: HashMap<String, IbvDevice<I>>,

    config: IbvConfig,

    /// Map from buffer_id to the registered MR view. The view keeps the MR (and
    /// its PD) alive for the lifetime of the registration; `ReleaseBuffer` drops
    /// the entry, and the FFI resources are released by the `Arc`s' `Drop`s once
    /// no other holder of the view remains. The wire-facing [`IbvBuffer`] is
    /// derived from the view on demand.
    buffer_registrations: HashMap<usize, IbvMemoryRegionView>,
}

#[async_trait]
impl<I: IbvDeviceImpl> Actor for IbvManagerActor<I> {
    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        let owner = if let Some(owner) = this.parent_handle() {
            owner
        } else {
            anyhow::bail!("RdmaManagerActor not found as parent of IbvManagerActor");
        };
        self.owner
            .set(owner)
            .expect("owner should only be set once during init");
        Ok(())
    }

    // This actor is implemented in Rust, but the RDMA registration path may enter
    // Python and take the GIL. Run its loop on the dedicated rdma runtime rather
    // than the shared control-plane runtime; see `crate::rdma_runtime`.
    fn spawn_server_task<F>(future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        crate::rdma_runtime::spawn_on_rdma_runtime(future)
    }
}

impl<I: IbvDeviceImpl> Drop for IbvManagerActor<I> {
    fn drop(&mut self) {
        // Drain active-side QP actors. Each child owns its
        // `IbvQueuePair`; `drain_and_stop` schedules the actor to
        // finish in-flight ops and exit, dropping the QP via its
        // own `Drop`.
        for (_key, handle) in self.qp_handles.drain() {
            let _ = handle.drain_and_stop("IbvManagerActor dropped");
        }

        // The remaining fields (`peer_created_qps`,
        // `buffer_registrations`, `devices`) free their FFI resources
        // through their elements' `Drop`s when this struct is dropped.
    }
}

impl<I: IbvDeviceImpl> IbvManagerActor<I> {
    /// Create a new IbvManagerActor with the given configuration.
    pub async fn new(params: Option<IbvConfig>) -> Result<Self, anyhow::Error> {
        if !ibverbs_supported() {
            return Err(anyhow::anyhow!(
                "Cannot create IbvManagerActor because RDMA is not supported on this machine"
            ));
        }

        // Use the caller's config; when none is given, start from the
        // defaults and let the backend seed its own.
        let mut config = match params {
            Some(config) => config,
            None => {
                let mut config = IbvConfig::default();
                I::apply_config_defaults(&mut config);
                config
            }
        };
        tracing::debug!("rdma is enabled, config target: {:?}", config.target);

        // check config and hardware support align
        if config.use_gpu_direct {
            match validate_execution_context().await {
                Ok(_) => {
                    tracing::info!("GPU Direct RDMA execution context validated successfully");
                }
                Err(e) => {
                    tracing::warn!(
                        "GPU Direct RDMA execution context validation failed: {}. Downgrading to standard ibverbs mode.",
                        e
                    );
                    config.use_gpu_direct = false;
                }
            }
        }

        let actor = Self {
            owner: OnceLock::new(),
            qp_handles: HashMap::new(),
            peer_created_qps: HashMap::new(),
            devices: HashMap::new(),
            config,
            buffer_registrations: HashMap::new(),
        };

        Ok(actor)
    }

    /// Get or create the `DEFAULT_DOMAIN` for the named RDMA device, opening
    /// the device on first use.
    fn get_or_create_device_domain(
        &mut self,
        device_name: &str,
    ) -> Result<Arc<IbvDomain<I::Domain>>, anyhow::Error> {
        if let Some(device) = self.devices.get_mut(device_name) {
            return device.get_or_create_domain(DEFAULT_DOMAIN);
        }

        let mut device =
            IbvDevice::<I>::open(device_name, self.config.clone()).ok_or_else(|| {
                anyhow::anyhow!("{} does not advertise {}", I::backend_name(), device_name,)
            })?;
        let domain = device.get_or_create_domain(DEFAULT_DOMAIN)?;

        // Print device info if MONARCH_DEBUG_RDMA=1 is set.
        crate::print_device_info_if_debug_enabled(domain.context.as_ptr());

        self.devices.insert(device_name.to_string(), device);
        Ok(domain)
    }

    /// Resolve `mem` to an [`IbvMemoryRegionView`] using the slot shared by
    /// every clone of `mem`. On a cold slot, picks the RDMA device (the
    /// CUDA-co-located NIC for device memory, else the config fallback) and
    /// registers the region through that device's [`IbvDomainImpl`] strategy,
    /// installing the result; on a warm slot, returns the cached view.
    fn resolve_local_mr(
        &mut self,
        mem: &KeepaliveLocalMemory,
    ) -> Result<IbvMemoryRegionView, anyhow::Error> {
        if let Some(mrv) = mem.mr_slot().get() {
            return Ok(mrv.clone());
        }
        let addr = mem.addr();

        // Pick the RDMA device: for device memory, the CUDA-co-located NIC;
        // otherwise the configured fallback.
        let cuda_nic = if is_device_ptr(addr) {
            let mut device_ordinal: i32 = -1;
            // SAFETY: `addr` is a CUDA device pointer (per `is_device_ptr`); the
            // FFI call writes the owning device ordinal through the out-pointer.
            let err = unsafe {
                rdmaxcel_sys::rdmaxcel_cuPointerGetAttribute(
                    &mut device_ordinal as *mut _ as *mut std::ffi::c_void,
                    rdmaxcel_sys::CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL,
                    addr as rdmaxcel_sys::CUdeviceptr,
                )
            };
            // A non-success code yields no NIC.
            let ordinal = (err == rdmaxcel_sys::CUDA_SUCCESS).then_some(device_ordinal);
            ordinal.filter(|o| *o >= 0).and_then(|o| {
                super::device_selection::get_cuda_device_to_ibv_device::<I>()
                    .get(o as usize)
                    .and_then(|d| d.clone())
            })
        } else {
            None
        };
        let device_name = cuda_nic.map(|info| info.name().clone()).unwrap_or_else(|| {
            resolve_target::<I>(&self.config.target)
                .unwrap_or_else(|| IbvDeviceInfo::default::<I>())
                .name()
                .clone()
        });
        tracing::debug!(
            "Using RDMA device: {} for memory at 0x{:x}",
            device_name,
            addr
        );

        let domain = self.get_or_create_device_domain(&device_name)?;
        // The backend strategy handles host vs. device memory (standard MR,
        // dmabuf MR, or a device-specific segment binding).
        let mrv = domain.register_mr(mem)?;
        Ok(mem.mr_slot().get_or_init(|| mrv).clone())
    }

    /// Build a passive-side mirror QP for `qp_key`, connect it to
    /// `sender_info`, and store it in [`Self::peer_created_qps`].
    /// Returns the local endpoint the active side needs to finish
    /// its own `connect`. Called from
    /// [`Handler<CreatePeerQueuePair>`].
    fn create_peer_qp(
        &mut self,
        qp_key: &QpKey,
        sender_info: &IbvQpInfo,
    ) -> Result<IbvQpInfo, anyhow::Error> {
        if self.peer_created_qps.contains_key(qp_key) {
            anyhow::bail!("peer queue pair already exists for {qp_key:?}");
        }
        let self_device = &qp_key.self_device;
        let domain = self.get_or_create_device_domain(self_device)?;
        let mut qp = domain
            .create_queue_pair(&self.config)
            .map_err(|e| anyhow::anyhow!("could not create peer IbvQueuePair: {}", e))?;
        let local_info = qp
            .get_qp_info()
            .map_err(|e| anyhow::anyhow!("could not extract peer QP info: {}", e))?;
        qp.connect(sender_info)
            .map_err(|e| anyhow::anyhow!("could not connect peer QP: {}", e))?;
        self.peer_created_qps.insert(qp_key.clone(), qp);
        Ok(local_info)
    }

    /// Lazy active-side QP actor: if `qp_key` is absent from
    /// [`Self::qp_handles`], create an [`IbvQueuePair`] on the
    /// requested device and spawn a [`QueuePairActor`] to drive its
    /// handshake + data path. Returns a clone of the actor handle.
    fn ensure_qp_actor(
        &mut self,
        cx: &Context<'_, Self>,
        qp_key: &QpKey,
        peer_manager: ActorRef<Self>,
    ) -> Result<
        ActorHandle<QueuePairActor<Self, <I::Domain as IbvDomainImpl>::QueuePair>>,
        anyhow::Error,
    > {
        if let Some(h) = self.qp_handles.get(qp_key) {
            return Ok(h.clone());
        }
        let self_device = &qp_key.self_device;
        let domain = self.get_or_create_device_domain(self_device)?;
        let qp = domain
            .create_queue_pair(&self.config)
            .map_err(|e| anyhow::anyhow!("could not create IbvQueuePair for {qp_key:?}: {}", e))?;
        let local_manager: ActorRef<Self> = cx.bind();
        let is_loopback = local_manager.actor_addr() == peer_manager.actor_addr()
            && qp_key.self_device == qp_key.other_device;
        let actor = cx.spawn(QueuePairActor::new(
            qp_key.clone(),
            local_manager,
            peer_manager,
            qp,
            is_loopback,
            self.config.max_send_wr,
            self.config.max_rd_atomic as u32,
        ));
        self.qp_handles.insert(qp_key.clone(), actor.clone());
        Ok(actor)
    }
}

#[async_trait]
impl<I: IbvDeviceImpl> IbvManagerMessageHandler for IbvManagerActor<I> {
    async fn release_buffer(
        &mut self,
        _cx: &Context<Self>,
        remote_buf_id: usize,
    ) -> Result<(), anyhow::Error> {
        // Dropping the entry releases the manager's `Arc` clones on
        // the view's MR and PD; FFI cleanup happens via their `Drop`s
        // once the last referencing view is gone.
        self.buffer_registrations.remove(&remote_buf_id);
        Ok(())
    }
}

// `#[hyperactor::handle(IbvManagerMessage)]` would generate a
// non-generic `impl Handler<...> for IbvManagerActor<I>` that
// can't see `I`; we write the generic delegation by hand.
#[async_trait]
impl<I: IbvDeviceImpl> Handler<IbvManagerMessage> for IbvManagerActor<I> {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: IbvManagerMessage,
    ) -> Result<(), anyhow::Error> {
        <Self as IbvManagerMessageHandler>::handle(self, cx, message).await
    }
}

#[async_trait]
impl<I: IbvDeviceImpl> Handler<SubmitOps<I>> for IbvManagerActor<I> {
    async fn handle(&mut self, cx: &Context<Self>, msg: SubmitOps<I>) -> Result<(), anyhow::Error> {
        let SubmitOps { ops, reply } = msg;

        // Interleave MR resolution with QP dispatch: as soon as op `i`'s
        // local MR is resolved and its QP actor is in place, ship a
        // one-item `ProcessOps` to that QP. The QP can then post and
        // poll op `i` while we run `resolve_local_mr` for op `i+1`.
        for (i, op) in ops.into_iter().enumerate() {
            let mrv = match self.resolve_local_mr(&op.local_memory) {
                Ok(mrv) => mrv,
                Err(e) => {
                    reply.try_post(
                        cx,
                        OpResult {
                            op_idx: i,
                            result: Err(e.to_string()),
                        },
                    )?;
                    continue;
                }
            };
            let qp_key = QpKey {
                self_device: mrv.device_name.clone(),
                other_id: op.remote_manager.actor_addr().id().clone(),
                other_device: op.remote_buffer.device_name.clone(),
            };
            let peer_manager = op.remote_manager.clone();
            let handle = match self.ensure_qp_actor(cx, &qp_key, peer_manager) {
                Ok(h) => h,
                Err(e) => {
                    reply.try_post(
                        cx,
                        OpResult {
                            op_idx: i,
                            result: Err(e.to_string()),
                        },
                    )?;
                    continue;
                }
            };
            handle.try_post(
                cx,
                ProcessOps {
                    items: vec![(i, op, mrv)],
                    reply: reply.clone(),
                },
            )?;
        }
        Ok(())
    }
}

impl<I: IbvDeviceImpl> IbvManagerActor<I> {
    /// Synchronous portion of [`RawQueuePair`] handling: create the
    /// local QP, post `CreatePeerQueuePair` to `peer`, and return the
    /// in-flight reply receiver. The follow-up work (awaiting the
    /// peer's reply and connecting the QP) runs in a tokio task off
    /// the manager's mailbox; see [`Self::raw_queue_pair_impl`]'s
    /// rewrite into [`Handler<RawQueuePair>`].
    fn raw_queue_pair_setup(
        &mut self,
        cx: &Context<'_, Self>,
        peer: &ActorRef<IbvManagerActor<I>>,
        self_device: String,
        peer_device: String,
    ) -> Result<
        (
            <I::Domain as IbvDomainImpl>::QueuePair,
            OncePortReceiver<Result<IbvQpInfo, String>>,
        ),
        anyhow::Error,
    > {
        let domain = self.get_or_create_device_domain(&self_device)?;
        let mut qp = domain
            .create_queue_pair(&self.config)
            .map_err(|e| anyhow::anyhow!("could not create IbvQueuePair: {e}"))?;
        let sender_info = qp
            .get_qp_info()
            .map_err(|e| anyhow::anyhow!("could not extract QP info: {e}"))?;
        let (reply, rx) = Mailbox::mailbox(cx).open_once_port::<Result<IbvQpInfo, String>>();
        peer.post(
            cx,
            CreatePeerQueuePair::<IbvManagerActor<I>> {
                sender: cx.bind(),
                sender_device: self_device,
                receiver_device: peer_device,
                sender_info,
                reply: reply.bind(),
            },
        );
        Ok((qp, rx))
    }
}

#[async_trait]
impl<I: IbvDeviceImpl> Handler<RawQueuePair<I>> for IbvManagerActor<I> {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        msg: RawQueuePair<I>,
    ) -> Result<(), anyhow::Error> {
        let RawQueuePair {
            peer,
            self_device,
            peer_device,
            reply,
        } = msg;

        // Do the sync setup (QP create + peer dispatch) while we
        // still own the manager's actor loop.
        let setup = self.raw_queue_pair_setup(cx, &peer, self_device, peer_device);
        let (mut qp, rx) = match setup {
            Ok(state) => state,
            Err(e) => {
                let _ = reply.try_post(cx, Err(e.to_string()));
                return Ok(());
            }
        };

        // Hand off the wait to a tokio task using a freshly-minted
        // `Proc::client` as the posting context. Without this hop the
        // handler would park on `rx.recv()` while still holding the
        // manager's mailbox; a symmetric `RawQueuePair` from the peer
        // would then queue a `CreatePeerQueuePair` we can't dispatch,
        // and both managers deadlock until timeout.
        let client_name = ActorId::anonymous(cx.proc().proc_id().clone()).to_string();
        let client = cx.proc().client(&client_name);
        let timeout = hyperactor_config::global::get(crate::config::RDMA_QP_INIT_TIMEOUT);

        tokio::spawn(async move {
            let result: Result<<I::Domain as IbvDomainImpl>::QueuePair, String> = async {
                let peer_info_result = tokio::time::timeout(timeout, rx.recv())
                    .await
                    .map_err(|_| format!("RawQueuePair init timed out after {timeout:?}"))?
                    .map_err(|e| format!("RawQueuePair reply port closed: {e}"))?;
                let peer_info = peer_info_result?;
                qp.connect(&peer_info)
                    .map_err(|e| format!("could not connect QP: {e}"))?;
                Ok(qp)
            }
            .await;
            let _ = reply.try_post(&client, result);
        });
        Ok(())
    }
}

#[async_trait]
impl<I: IbvDeviceImpl> Handler<CreatePeerQueuePair<IbvManagerActor<I>>> for IbvManagerActor<I> {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        msg: CreatePeerQueuePair<IbvManagerActor<I>>,
    ) -> Result<(), anyhow::Error> {
        let CreatePeerQueuePair {
            sender,
            sender_device,
            receiver_device,
            sender_info,
            reply,
        } = msg;
        let qp_key = QpKey {
            self_device: receiver_device,
            other_id: sender.actor_addr().id().clone(),
            other_device: sender_device,
        };
        match self.create_peer_qp(&qp_key, &sender_info) {
            Ok(local_info) => reply.post(cx, Ok(local_info)),
            Err(e) => reply.post(cx, Err(e.to_string())),
        }
        Ok(())
    }
}

#[async_trait]
impl<I: IbvDeviceImpl> IbvManagerLocalMessageHandler for IbvManagerActor<I> {
    async fn register_remote_buffer(
        &mut self,
        _cx: &Context<Self>,
        remote_buf_id: usize,
        local: KeepaliveLocalMemory,
    ) -> Result<Result<IbvBuffer, String>, anyhow::Error> {
        if let Some(mrv) = self.buffer_registrations.get(&remote_buf_id) {
            return Ok(Ok(IbvBuffer::from(mrv)));
        }
        // `resolve_local_mr` installs the view in `local`'s shared MR
        // slot, so every clone of this handle — including the one the
        // caller holds — reuses this registration instead of registering
        // the same region again.
        let mrv = match self.resolve_local_mr(&local) {
            Ok(v) => v,
            Err(e) => return Ok(Err(e.to_string())),
        };
        let buf = IbvBuffer::from(&mrv);
        self.buffer_registrations.insert(remote_buf_id, mrv);
        Ok(Ok(buf))
    }
}

// `#[hyperactor::handle(IbvManagerLocalMessage)]` analogue, written
// generically; see the `IbvManagerMessage` block above.
#[async_trait]
impl<I: IbvDeviceImpl> Handler<IbvManagerLocalMessage> for IbvManagerActor<I> {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: IbvManagerLocalMessage,
    ) -> Result<(), anyhow::Error> {
        <Self as IbvManagerLocalMessageHandler>::handle(self, cx, message).await
    }
}

/// Wrapper around [`ActorHandle<IbvManagerActor<I>>`] that moves the RDMA
/// data-plane (post send/recv, poll CQ) off the actor loop while keeping
/// state-mutating operations (MR registration/deregistration, QP management)
/// serialized through actor messages.
#[derive(Debug)]
pub struct IbvBackend<I: IbvDeviceImpl>(pub ActorHandle<IbvManagerActor<I>>);

impl<I: IbvDeviceImpl> Clone for IbvBackend<I> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<I: IbvDeviceImpl> std::ops::Deref for IbvBackend<I> {
    type Target = ActorHandle<IbvManagerActor<I>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Serializable per-buffer context for an ibverbs backend: the manager
/// to route ops through and the wire description of the registered MR.
#[derive(Serialize, Deserialize, Named)]
#[serde(bound = "")]
pub struct IbvRemoteBackendContext<I: IbvDeviceImpl> {
    pub manager: ActorRef<IbvManagerActor<I>>,
    pub buffer: IbvBuffer,
}

// `Clone` and `Debug` are hand-rolled to avoid the spurious `I: Clone`
// and `I: Debug` bounds the derives would impose; neither field depends
// on `I` implementing them.
impl<I: IbvDeviceImpl> Clone for IbvRemoteBackendContext<I> {
    fn clone(&self) -> Self {
        Self {
            manager: self.manager.clone(),
            buffer: self.buffer.clone(),
        }
    }
}

impl<I: IbvDeviceImpl> std::fmt::Debug for IbvRemoteBackendContext<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IbvRemoteBackendContext")
            .field("manager", &self.manager)
            .field("buffer", &self.buffer)
            .finish()
    }
}

#[async_trait]
impl<I: IbvDeviceImpl> RdmaBackend for IbvBackend<I>
where
    RdmaRemoteBuffer: ResolveRemoteBackendContext<IbvBackend<I>>,
{
    type RemoteBackendContext = IbvRemoteBackendContext<I>;
    type TransportInfo = ();

    fn available() -> bool {
        if IbvDevice::<I>::available() {
            if hyperactor_config::global::get(crate::config::RDMA_DISABLE_IBVERBS) {
                tracing::warn!(
                    "ibverbs ({}) is available, but it was disabled by configuration (RDMA_DISABLE_IBVERBS=true)",
                    I::backend_name()
                );
                return false;
            }
            return true;
        }
        false
    }

    fn transport_level(&self) -> RdmaTransportLevel {
        RdmaTransportLevel::Nic
    }

    fn transport_info(&self) -> Option<Self::TransportInfo> {
        None
    }

    async fn spawn(
        cx: &(impl hyperactor::context::Actor + Send + Sync),
        config: &RdmaConfig,
    ) -> Result<Self> {
        let actor = IbvManagerActor::<I>::new(config.ibv.clone()).await?;
        Ok(IbvBackend(cx.spawn(actor)))
    }

    async fn register_remote_buffer(
        &self,
        cx: &(impl hyperactor::context::Actor + Send + Sync),
        remote_buf_id: usize,
        local: KeepaliveLocalMemory,
    ) -> Result<IbvRemoteBackendContext<I>> {
        let buffer = self
            .0
            .register_remote_buffer(cx, remote_buf_id, local)
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(IbvRemoteBackendContext {
            manager: self.0.bind(),
            buffer,
        })
    }

    async fn release_buffer(
        &self,
        cx: &(impl hyperactor::context::Actor + Send + Sync),
        remote_buf_id: usize,
    ) -> Result<()> {
        self.0.release_buffer(cx, remote_buf_id).await
    }

    /// Submit a batch of RDMA operations.
    ///
    /// Translates each op to an `IbvOp`, then ships the whole batch to
    /// [`IbvManagerActor`] via [`SubmitOps`]. The manager interleaves
    /// local-MR resolution with per-op dispatch: each op is sent to its
    /// [`QueuePairActor`] as a one-item [`ProcessOps`] the moment its MR
    /// is ready, so QP work on op `i` overlaps MR registration for op
    /// `i+1`.
    ///
    /// Always waits for exactly `ops.len()` per-op replies before
    /// returning. Per-op failures are collected and formatted into a single
    /// multi-line `Err` listing each `op_idx` and its error message.
    async fn submit(
        &self,
        cx: &(impl hyperactor::context::Actor + Send + Sync),
        ops: Vec<RdmaOp>,
        timeout: Duration,
    ) -> Result<(), anyhow::Error> {
        let mut ibv_ops = Vec::with_capacity(ops.len());
        for op in ops {
            let ctx = <RdmaRemoteBuffer as ResolveRemoteBackendContext<IbvBackend<I>>>::resolve(
                &op.remote,
            )
            .expect("op routed to incompatible backend");
            ibv_ops.push(IbvOp {
                op_type: op.op_type,
                local_memory: op.local.clone(),
                remote_buffer: ctx.buffer,
                remote_manager: ctx.manager,
            });
        }
        let n = ibv_ops.len();

        let (reply, mut reply_rx) = cx.mailbox().open_port::<OpResult>();

        self.0.try_post(
            cx,
            SubmitOps {
                ops: ibv_ops,
                reply,
            },
        )?;

        let mut failures: Vec<(usize, String)> = Vec::with_capacity(n);
        let mut received = 0usize;
        let mut terminal: Option<String> = None;
        let deadline = tokio::time::Instant::now() + timeout;
        while received < n {
            tokio::select! {
                () = tokio::time::sleep_until(deadline) => {
                    terminal = Some(format!(
                        "submit timed out after {received}/{n} replies with {} failures",
                        failures.len()
                    ));
                    break;
                }
                recv = reply_rx.recv() => {
                    match recv {
                        Ok(OpResult { result: Ok(()), .. }) => received += 1,
                        Ok(OpResult { op_idx, result: Err(e) }) => {
                            received += 1;
                            failures.push((op_idx, e));
                        }
                        Err(e) => {
                            terminal = Some(format!(
                                "SubmitOps reply port closed after {received}/{n} replies with {} failures: {e}",
                                failures.len()
                            ));
                            break;
                        }
                    }
                }
            }
        }

        if terminal.is_none() && failures.is_empty() {
            return Ok(());
        }

        failures.sort_by_key(|(idx, _)| *idx);
        let mut msg = terminal.unwrap_or_else(|| format!("{}/{n} ops failed", failures.len()));
        if !failures.is_empty() {
            msg.push(':');
            for (idx, err) in &failures {
                write!(msg, "\n  op {idx}: {err}").expect("infallible String write");
            }
        }
        Err(anyhow::anyhow!(msg))
    }
}

#[cfg(test)]
mod tests {
    //! End-to-end coverage of the [`SubmitOps`] → [`ProcessOps`] →
    //! [`QueuePairActor`] data path.
    //!
    //! Each test stands up two RDMA participants in two
    //! [`Proc::direct`] procs in the test process. Each proc hosts an
    //! [`RdmaManagerActor`] and a [`BufferHelperActor`]. Tests
    //! allocate buffers on either side via the helpers, drive RDMA
    //! through [`IbvBackend::submit`] (called inside the helper
    //! actor), and verify by reading back local contents through
    //! [`BufferHelperMessage::ReadContents`]. The
    //! [`BufferHelperActor::cleanup`] impl releases any CUDA
    //! allocations when the actor stops; [`TestEnv::shutdown`]
    //! explicitly drains both procs.

    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use async_trait::async_trait;
    use hyperactor::Actor;
    use hyperactor::ActorRef;
    use hyperactor::Context;
    use hyperactor::Handler;
    use hyperactor::Instance;
    use hyperactor::Label;
    use hyperactor::OncePortRef;
    use hyperactor::Proc;
    use hyperactor::RefClient;
    use hyperactor::RemoteSpawn;
    use hyperactor::Uid;
    use hyperactor::actor::ActorError;
    use hyperactor::channel::ChannelAddr;
    use hyperactor::channel::ChannelTransport;
    use hyperactor_config::Flattrs;
    use serde::Deserialize;
    use serde::Serialize;
    use typeuri::Named;

    use crate::IbvConfig;
    use crate::RdmaManagerActor;
    use crate::RdmaManagerMessageClient;
    use crate::RdmaOp;
    use crate::RdmaOpType;
    use crate::RdmaRemoteBuffer;
    use crate::backend::RdmaBackendHandle;
    use crate::backend::cuda_test_utils::CudaAllocation;
    use crate::backend::cuda_test_utils::CudaAllocator;
    use crate::backend::ibverbs::device::list_all_devices;
    use crate::backend::ibverbs::device_selection::IbvDeviceTarget;
    use crate::backend::ibverbs::primitives::IbvQpType;
    use crate::local_memory::KeepaliveLocalMemory;

    // ====================================================================
    // BufferHelperActor
    // ====================================================================

    /// Device a test buffer is allocated on.
    #[derive(Debug, Clone, Copy, Serialize, Deserialize, Named)]
    pub enum BufferDevice {
        Cpu,
        Cuda(i32),
    }

    /// One op for [`BufferHelperMessage::Submit`]. The helper looks up
    /// the local memory behind `local_buf` (registered earlier via
    /// `Allocate`) and pairs it with `remote_buf` to form an
    /// [`RdmaOp`].
    #[derive(Debug, Clone, Serialize, Deserialize, Named)]
    pub struct BufferHelperOp {
        op_type: RdmaOpType,
        local_buf: RdmaRemoteBuffer,
        remote_buf: RdmaRemoteBuffer,
    }

    /// Test helper that owns local buffers (CPU or CUDA) and drives
    /// [`IbvBackend::submit`] against its own [`RdmaManagerActor`].
    #[hyperactor::export(handlers = [BufferHelperMessage])]
    #[hyperactor::spawnable]
    #[derive(Debug)]
    pub struct BufferHelperActor {
        rdma_manager: ActorRef<RdmaManagerActor>,
        /// CUDA allocations tracked for cleanup. Each is also held as
        /// `Keepalive` inside the registered `KeepaliveLocalMemory`;
        /// both clones must drop before the FFI memory is released.
        cuda_allocs: Vec<CudaAllocation>,
    }

    #[async_trait]
    impl Actor for BufferHelperActor {
        async fn cleanup(
            &mut self,
            _this: &Instance<Self>,
            _err: Option<&ActorError>,
        ) -> Result<(), anyhow::Error> {
            for alloc in self.cuda_allocs.drain(..) {
                alloc.try_free();
            }
            Ok(())
        }
    }

    #[async_trait]
    impl RemoteSpawn for BufferHelperActor {
        type Params = ActorRef<RdmaManagerActor>;

        async fn new(
            rdma_manager: ActorRef<RdmaManagerActor>,
            _env: Flattrs,
        ) -> Result<Self, anyhow::Error> {
            Ok(Self {
                rdma_manager,
                cuda_allocs: Vec::new(),
            })
        }
    }

    #[derive(Handler, RefClient, Named, Serialize, Deserialize, Debug)]
    pub enum BufferHelperMessage {
        /// Allocate `size` bytes on `device`, pre-fill with `pattern`,
        /// register with the local `RdmaManagerActor`, and reply with
        /// the resulting `RdmaRemoteBuffer`.
        Allocate {
            size: usize,
            device: BufferDevice,
            pattern: u8,
            #[reply]
            reply: OncePortRef<RdmaRemoteBuffer>,
        },
        /// Look up the local memory behind `remote.id` and reply with
        /// the byte range `[offset, offset + len)`. Tests use this to
        /// sample buffers too large to ship over a single actor
        /// message in one piece.
        ReadContents {
            remote: Box<RdmaRemoteBuffer>,
            offset: usize,
            len: usize,
            #[reply]
            reply: OncePortRef<Vec<u8>>,
        },
        /// Drive a batch of RDMA ops through `IbvBackend::submit`.
        /// Each op's `local_buf` is resolved against this helper's
        /// `RdmaManagerActor`; `remote_buf` is shipped as-is to the
        /// peer.
        Submit {
            ops: Vec<BufferHelperOp>,
            timeout_secs: u64,
            #[reply]
            reply: OncePortRef<Result<(), String>>,
        },
    }

    impl BufferHelperActor {
        async fn allocate_impl(
            &mut self,
            cx: &Context<'_, Self>,
            size: usize,
            device: BufferDevice,
            pattern: u8,
        ) -> Result<RdmaRemoteBuffer, anyhow::Error> {
            let local = match device {
                BufferDevice::Cpu => {
                    let buf: Box<[u8]> = vec![pattern; size].into_boxed_slice();
                    KeepaliveLocalMemory::new(Arc::new(buf))
                }
                BufferDevice::Cuda(device_id) => {
                    let alloc = CudaAllocator::get().allocate(device_id, size, size);
                    let local = KeepaliveLocalMemory::new(Arc::new(alloc.clone()));
                    self.cuda_allocs.push(alloc);
                    let fill = vec![pattern; size];
                    // SAFETY: `local` is freshly constructed; no other
                    // holder touches this CUDA range yet.
                    unsafe { local.write_at(0, &fill) }?;
                    local
                }
            };
            let handle = self
                .rdma_manager
                .downcast_handle(cx)
                .ok_or_else(|| anyhow::anyhow!("rdma_manager not local to BufferHelperActor"))?;
            handle.request_buffer(cx, local).await
        }

        async fn read_contents_impl(
            &mut self,
            cx: &Context<'_, Self>,
            remote: RdmaRemoteBuffer,
            offset: usize,
            len: usize,
        ) -> Result<Vec<u8>, anyhow::Error> {
            let handle = self
                .rdma_manager
                .downcast_handle(cx)
                .ok_or_else(|| anyhow::anyhow!("rdma_manager not local"))?;
            let local = handle
                .request_local_memory(cx, remote.id)
                .await?
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "no local memory registered on this side for remote_buf_id={}",
                        remote.id,
                    )
                })?;
            let mut out = vec![0u8; len];
            // SAFETY: by convention the caller has ensured all RDMA
            // ops against this buffer have completed before invoking
            // ReadContents.
            unsafe { local.read_at(offset, &mut out)? };
            Ok(out)
        }

        async fn submit_impl(
            &mut self,
            cx: &Context<'_, Self>,
            ops: Vec<BufferHelperOp>,
            timeout_secs: u64,
        ) -> Result<Result<(), String>, anyhow::Error> {
            let handle = self
                .rdma_manager
                .downcast_handle(cx)
                .ok_or_else(|| anyhow::anyhow!("rdma_manager not local"))?;
            let mut rdma_ops = Vec::with_capacity(ops.len());
            for (i, op) in ops.into_iter().enumerate() {
                let local = handle
                    .request_local_memory(cx, op.local_buf.id)
                    .await?
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "op {i}: no local memory registered for remote_buf_id={}",
                            op.local_buf.id,
                        )
                    })?;
                rdma_ops.push(RdmaOp {
                    op_type: op.op_type,
                    local,
                    remote: op.remote_buf,
                });
            }
            let nic = RdmaManagerActor::local_handle(cx)
                .get_backend_handles(cx)
                .await?
                .into_iter()
                .find(|h| !matches!(h, RdmaBackendHandle::Tcp(_)))
                .ok_or_else(|| anyhow::anyhow!("no NIC backend on this proc"))?;
            let result = nic
                .submit(cx, rdma_ops, Duration::from_secs(timeout_secs))
                .await;
            Ok(result.map_err(|e| format!("{e}")))
        }
    }

    #[async_trait]
    #[hyperactor::handle(BufferHelperMessage)]
    impl BufferHelperMessageHandler for BufferHelperActor {
        async fn allocate(
            &mut self,
            cx: &Context<Self>,
            size: usize,
            device: BufferDevice,
            pattern: u8,
        ) -> Result<RdmaRemoteBuffer, anyhow::Error> {
            self.allocate_impl(cx, size, device, pattern).await
        }

        async fn read_contents(
            &mut self,
            cx: &Context<Self>,
            remote: Box<RdmaRemoteBuffer>,
            offset: usize,
            len: usize,
        ) -> Result<Vec<u8>, anyhow::Error> {
            self.read_contents_impl(cx, *remote, offset, len).await
        }

        async fn submit(
            &mut self,
            cx: &Context<Self>,
            ops: Vec<BufferHelperOp>,
            timeout_secs: u64,
        ) -> Result<Result<(), String>, anyhow::Error> {
            self.submit_impl(cx, ops, timeout_secs).await
        }
    }

    // ====================================================================
    // TestEnv
    // ====================================================================

    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    /// Two-sided test environment.
    ///
    /// Each side is a `Proc::direct` in the test process hosting its
    /// own `RdmaManagerActor` and `BufferHelperActor`. A client minted
    /// from `proc_b` drives both helpers through their `ActorRef`s.
    struct TestEnv {
        client: hyperactor::Client,
        proc_a: Proc,
        helper_a: ActorRef<BufferHelperActor>,
        proc_b: Proc,
        helper_b: ActorRef<BufferHelperActor>,
    }

    impl TestEnv {
        /// Asymmetric setup: side A uses `config_a`, side B uses `config_b`.
        async fn new(config_a: IbvConfig, config_b: IbvConfig) -> Result<Self, anyhow::Error> {
            let id = COUNTER.fetch_add(1, Ordering::Relaxed);
            let proc_a = Proc::direct(
                ChannelAddr::any(ChannelTransport::Unix),
                format!("rdma_side_a_{id}"),
            )?;
            let helper_a = Self::spawn_side(&proc_a, config_a).await?;
            let proc_b = Proc::direct(
                ChannelAddr::any(ChannelTransport::Unix),
                format!("rdma_side_b_{id}"),
            )?;
            let helper_b = Self::spawn_side(&proc_b, config_b).await?;
            let client = proc_b.client("test_client");
            Ok(Self {
                client,
                proc_a,
                helper_a,
                proc_b,
                helper_b,
            })
        }

        /// Symmetric setup: both sides use `config`.
        async fn same_config(config: IbvConfig) -> Result<Self, anyhow::Error> {
            Self::new(config.clone(), config).await
        }

        /// Spawn an `RdmaManagerActor` + `BufferHelperActor` on `proc`
        /// and return the helper's `ActorRef`.
        async fn spawn_side(
            proc: &Proc,
            config: IbvConfig,
        ) -> Result<ActorRef<BufferHelperActor>, anyhow::Error> {
            let rdma_actor = RdmaManagerActor::new(Some(config), Flattrs::default()).await?;
            // Must match `RdmaManagerActor::local_handle`'s singleton lookup of "rdma_manager".
            let rdma_handle =
                proc.spawn_with_uid(Uid::singleton(Label::strip("rdma_manager")), rdma_actor)?;
            let rdma: ActorRef<RdmaManagerActor> = rdma_handle.bind();
            let helper_actor = BufferHelperActor::new(rdma, Flattrs::default()).await?;
            let helper_handle = proc.spawn_with_label("helper", helper_actor);
            Ok(helper_handle.bind())
        }

        async fn shutdown(mut self) -> Result<(), anyhow::Error> {
            let _ = self
                .proc_a
                .destroy_and_wait(Duration::from_secs(10), "TestEnv shutdown proc_a")
                .await?;
            let _ = self
                .proc_b
                .destroy_and_wait(Duration::from_secs(10), "TestEnv shutdown proc_b")
                .await?;
            Ok(())
        }
    }

    // ====================================================================
    // Shared test bodies
    // ====================================================================

    async fn assert_remote_pattern(
        helper: &ActorRef<BufferHelperActor>,
        cx: &hyperactor::Client,
        remote: RdmaRemoteBuffer,
        size: usize,
        pattern: u8,
    ) -> Result<(), anyhow::Error> {
        let got = helper.read_contents(cx, Box::new(remote), 0, size).await?;
        assert_eq!(got, vec![pattern; size]);
        Ok(())
    }

    /// Drive a single write from side A's buffer into side B's buffer
    /// and assert the destination now matches the pattern.
    async fn run_cross_actor_write(
        env: &TestEnv,
        src_dev: BufferDevice,
        dst_dev: BufferDevice,
        size: usize,
        pattern: u8,
        timeout_secs: u64,
    ) -> Result<(), anyhow::Error> {
        let src = env
            .helper_a
            .allocate(&env.client, size, src_dev, pattern)
            .await?;
        let dst = env.helper_b.allocate(&env.client, size, dst_dev, 0).await?;
        env.helper_a
            .submit(
                &env.client,
                vec![BufferHelperOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local_buf: src,
                    remote_buf: dst.clone(),
                }],
                timeout_secs,
            )
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;
        assert_remote_pattern(&env.helper_b, &env.client, dst, size, pattern).await
    }

    /// Drive a single read from side B's buffer into side A's buffer
    /// and assert the destination now matches the pattern.
    async fn run_cross_actor_read(
        env: &TestEnv,
        dst_dev: BufferDevice,
        src_dev: BufferDevice,
        size: usize,
        pattern: u8,
        timeout_secs: u64,
    ) -> Result<(), anyhow::Error> {
        let dst = env.helper_a.allocate(&env.client, size, dst_dev, 0).await?;
        let src = env
            .helper_b
            .allocate(&env.client, size, src_dev, pattern)
            .await?;
        env.helper_a
            .submit(
                &env.client,
                vec![BufferHelperOp {
                    op_type: RdmaOpType::ReadIntoLocal,
                    local_buf: dst.clone(),
                    remote_buf: src,
                }],
                timeout_secs,
            )
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;
        assert_remote_pattern(&env.helper_a, &env.client, dst, size, pattern).await
    }

    /// Drive both a write and a read in a single
    /// `IbvBackend::submit` batch — both ops target the same peer
    /// QP and so resolve to a single `ProcessOps` group. After the
    /// batch completes, side B's `write_dst` and side A's `read_dst`
    /// both contain their respective patterns.
    async fn run_multi_op_same_qp(
        env: &TestEnv,
        dev_a: BufferDevice,
        dev_b: BufferDevice,
        size: usize,
        timeout_secs: u64,
    ) -> Result<(), anyhow::Error> {
        const WRITE_PATTERN: u8 = 0xa1;
        const READ_PATTERN: u8 = 0xb2;
        let write_src = env
            .helper_a
            .allocate(&env.client, size, dev_a, WRITE_PATTERN)
            .await?;
        let write_dst = env.helper_b.allocate(&env.client, size, dev_b, 0).await?;
        let read_dst = env.helper_a.allocate(&env.client, size, dev_a, 0).await?;
        let read_src = env
            .helper_b
            .allocate(&env.client, size, dev_b, READ_PATTERN)
            .await?;
        env.helper_a
            .submit(
                &env.client,
                vec![
                    BufferHelperOp {
                        op_type: RdmaOpType::WriteFromLocal,
                        local_buf: write_src,
                        remote_buf: write_dst.clone(),
                    },
                    BufferHelperOp {
                        op_type: RdmaOpType::ReadIntoLocal,
                        local_buf: read_dst.clone(),
                        remote_buf: read_src,
                    },
                ],
                timeout_secs,
            )
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;
        assert_remote_pattern(&env.helper_b, &env.client, write_dst, size, WRITE_PATTERN).await?;
        assert_remote_pattern(&env.helper_a, &env.client, read_dst, size, READ_PATTERN).await
    }

    /// Drive a write + read between two buffers registered with the
    /// *same* `RdmaManagerActor` on the *same* device. Exercises the
    /// loopback path (`is_loopback = true`) where the active actor
    /// connects its QP to its own endpoint and skips the
    /// `CreatePeerQueuePair` round trip.
    async fn run_true_loopback(
        env: &TestEnv,
        dev: BufferDevice,
        size: usize,
    ) -> Result<(), anyhow::Error> {
        const PATTERN: u8 = 0x5d;
        let src = env
            .helper_a
            .allocate(&env.client, size, dev, PATTERN)
            .await?;
        let dst = env.helper_a.allocate(&env.client, size, dev, 0).await?;
        env.helper_a
            .submit(
                &env.client,
                vec![BufferHelperOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local_buf: src,
                    remote_buf: dst.clone(),
                }],
                5,
            )
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;
        assert_remote_pattern(&env.helper_a, &env.client, dst, size, PATTERN).await
    }

    // ====================================================================
    // Helpers
    // ====================================================================

    fn require_rdma() {
        if list_all_devices().is_empty() {
            panic!("SKIPPED: no RDMA devices available");
        }
    }

    fn require_cuda() {
        if !crate::is_cuda_available() {
            panic!("SKIPPED: CUDA not available");
        }
    }

    // ====================================================================
    // Tests
    // ====================================================================

    /// `register_remote_buffer` must populate the MR slot shared by
    /// every clone of the `KeepaliveLocalMemory` it is handed, so that
    /// later `resolve_local_mr` calls reuse the registered MR instead
    /// of registering the same region again.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_register_remote_buffer_fills_mr_slot() -> Result<(), anyhow::Error> {
        require_rdma();
        let env = TestEnv::same_config(IbvConfig::targeting(IbvDeviceTarget::cpu(0))).await?;
        let buf: Box<[u8]> = vec![0u8; 1024].into_boxed_slice();
        let local = KeepaliveLocalMemory::new(Arc::new(buf));
        assert!(
            local.mr_slot().get().is_none(),
            "MR slot should be empty before registration",
        );
        RdmaManagerActor::local_handle(&env.client)
            .request_buffer(&env.client, local.clone())
            .await?;
        assert!(
            local.mr_slot().get().is_some(),
            "registration should populate the MR slot",
        );
        env.shutdown().await
    }

    /// Cross-actor RDMA write over a single device (both sides target
    /// `cpu:0`). The two `RdmaManagerActor`s differ — this exercises
    /// the asymmetric `CreatePeerQueuePair` handshake even though the
    /// underlying device is shared.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_cross_actor_same_device_write() -> Result<(), anyhow::Error> {
        require_rdma();
        let env = TestEnv::same_config(IbvConfig::targeting(IbvDeviceTarget::cpu(0))).await?;
        run_cross_actor_write(&env, BufferDevice::Cpu, BufferDevice::Cpu, 32, 0xa5, 5).await?;
        env.shutdown().await
    }

    /// Cross-actor RDMA read over a single device.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_cross_actor_same_device_read() -> Result<(), anyhow::Error> {
        require_rdma();
        let env = TestEnv::same_config(IbvConfig::targeting(IbvDeviceTarget::cpu(0))).await?;
        run_cross_actor_read(&env, BufferDevice::Cpu, BufferDevice::Cpu, 32, 0x3c, 5).await?;
        env.shutdown().await
    }

    /// True loopback write: both buffers registered with the same
    /// `RdmaManagerActor` on the same device. The `QueuePairActor`
    /// sees `is_loopback = true` and connects its QP to its own
    /// endpoint without going through `CreatePeerQueuePair`.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_loopback_write() -> Result<(), anyhow::Error> {
        require_rdma();
        let env = TestEnv::same_config(IbvConfig::targeting(IbvDeviceTarget::cpu(0))).await?;
        run_true_loopback(&env, BufferDevice::Cpu, 32).await?;
        env.shutdown().await
    }

    /// Cross-device write (cpu:0 → cpu:1).
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_cross_device_write() -> Result<(), anyhow::Error> {
        require_rdma();
        let env = TestEnv::new(
            IbvConfig::targeting(IbvDeviceTarget::cpu(0)),
            IbvConfig::targeting(IbvDeviceTarget::cpu(1)),
        )
        .await?;
        run_cross_actor_write(&env, BufferDevice::Cpu, BufferDevice::Cpu, 32, 0x77, 5).await?;
        env.shutdown().await
    }

    /// Cross-device read (cpu:0 ← cpu:1).
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_cross_device_read() -> Result<(), anyhow::Error> {
        require_rdma();
        let env = TestEnv::new(
            IbvConfig::targeting(IbvDeviceTarget::cpu(0)),
            IbvConfig::targeting(IbvDeviceTarget::cpu(1)),
        )
        .await?;
        run_cross_actor_read(&env, BufferDevice::Cpu, BufferDevice::Cpu, 32, 0x88, 5).await?;
        env.shutdown().await
    }

    /// One write + one read in a single `IbvBackend::submit` batch.
    /// Both ops share the same `QpKey` so the manager groups them
    /// into a single `ProcessOps` dispatched to one `QueuePairActor`.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_multi_op_same_qp_cpu() -> Result<(), anyhow::Error> {
        require_rdma();
        let env = TestEnv::same_config(IbvConfig::targeting(IbvDeviceTarget::cpu(0))).await?;
        run_multi_op_same_qp(&env, BufferDevice::Cpu, BufferDevice::Cpu, 64, 5).await?;
        env.shutdown().await
    }

    /// Same as `test_multi_op_same_qp_cpu` but with 2 MiB CUDA buffers,
    /// pulled apart into a separate test because the buffer-size +
    /// device split is the only thing that differs.
    #[timed_test::async_timed_test(timeout_secs = 120)]
    async fn test_multi_op_same_qp_cuda() -> Result<(), anyhow::Error> {
        require_rdma();
        require_cuda();
        const SIZE: usize = 2 * 1024 * 1024;
        let env = TestEnv::new(
            IbvConfig::targeting(IbvDeviceTarget::gpu(0)),
            IbvConfig::targeting(IbvDeviceTarget::gpu(1)),
        )
        .await?;
        run_multi_op_same_qp(&env, BufferDevice::Cuda(0), BufferDevice::Cuda(1), SIZE, 10).await?;
        env.shutdown().await
    }

    /// CUDA → CPU write.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_cuda_to_cpu_write() -> Result<(), anyhow::Error> {
        require_rdma();
        require_cuda();
        const SIZE: usize = 2 * 1024 * 1024;
        let env = TestEnv::new(
            IbvConfig::targeting(IbvDeviceTarget::gpu(0)),
            IbvConfig::targeting(IbvDeviceTarget::cpu(1)),
        )
        .await?;
        run_cross_actor_write(
            &env,
            BufferDevice::Cuda(0),
            BufferDevice::Cpu,
            SIZE,
            0x9b,
            10,
        )
        .await?;
        env.shutdown().await
    }

    /// CUDA → CPU read (source is the CUDA side).
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_cuda_to_cpu_read() -> Result<(), anyhow::Error> {
        require_rdma();
        require_cuda();
        const SIZE: usize = 2 * 1024 * 1024;
        let env = TestEnv::new(
            IbvConfig::targeting(IbvDeviceTarget::cpu(0)),
            IbvConfig::targeting(IbvDeviceTarget::gpu(1)),
        )
        .await?;
        run_cross_actor_read(
            &env,
            BufferDevice::Cpu,
            BufferDevice::Cuda(1),
            SIZE,
            0x37,
            10,
        )
        .await?;
        env.shutdown().await
    }

    /// CPU → CUDA write.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_cpu_to_cuda_write() -> Result<(), anyhow::Error> {
        require_rdma();
        require_cuda();
        const SIZE: usize = 2 * 1024 * 1024;
        let env = TestEnv::new(
            IbvConfig::targeting(IbvDeviceTarget::cpu(0)),
            IbvConfig::targeting(IbvDeviceTarget::gpu(1)),
        )
        .await?;
        run_cross_actor_write(
            &env,
            BufferDevice::Cpu,
            BufferDevice::Cuda(1),
            SIZE,
            0x5a,
            10,
        )
        .await?;
        env.shutdown().await
    }

    /// CPU → CUDA read (source is the CPU side).
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_cpu_to_cuda_read() -> Result<(), anyhow::Error> {
        require_rdma();
        require_cuda();
        const SIZE: usize = 2 * 1024 * 1024;
        let env = TestEnv::new(
            IbvConfig::targeting(IbvDeviceTarget::gpu(0)),
            IbvConfig::targeting(IbvDeviceTarget::cpu(1)),
        )
        .await?;
        run_cross_actor_read(
            &env,
            BufferDevice::Cuda(0),
            BufferDevice::Cpu,
            SIZE,
            0x4e,
            10,
        )
        .await?;
        env.shutdown().await
    }

    /// CUDA → CUDA write.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_cuda_to_cuda_write() -> Result<(), anyhow::Error> {
        require_rdma();
        require_cuda();
        const SIZE: usize = 2 * 1024 * 1024;
        let env = TestEnv::new(
            IbvConfig::targeting(IbvDeviceTarget::gpu(0)),
            IbvConfig::targeting(IbvDeviceTarget::gpu(1)),
        )
        .await?;
        run_cross_actor_write(
            &env,
            BufferDevice::Cuda(0),
            BufferDevice::Cuda(1),
            SIZE,
            0xee,
            10,
        )
        .await?;
        env.shutdown().await
    }

    /// CUDA → CUDA read.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_cuda_to_cuda_read() -> Result<(), anyhow::Error> {
        require_rdma();
        require_cuda();
        const SIZE: usize = 2 * 1024 * 1024;
        let env = TestEnv::new(
            IbvConfig::targeting(IbvDeviceTarget::gpu(0)),
            IbvConfig::targeting(IbvDeviceTarget::gpu(1)),
        )
        .await?;
        run_cross_actor_read(
            &env,
            BufferDevice::Cuda(0),
            BufferDevice::Cuda(1),
            SIZE,
            0x42,
            10,
        )
        .await?;
        env.shutdown().await
    }

    /// CUDA buffers with `IbvQpType::Standard` (no mlx5dv).
    /// Exercises the per-buffer dmabuf MR-registration path: without
    /// mlx5dv the manager cannot use indirect mkeys via segment
    /// scanning and instead registers each buffer as a standalone
    /// dmabuf MR (`ibv_reg_dmabuf_mr`).
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_standard_qp_cuda_dmabuf_fallback() -> Result<(), anyhow::Error> {
        require_rdma();
        require_cuda();
        const SIZE: usize = 16 * 1024 * 1024;
        let mut config_a = IbvConfig::targeting(IbvDeviceTarget::gpu(0));
        config_a.qp_type = IbvQpType::Standard;
        let mut config_b = IbvConfig::targeting(IbvDeviceTarget::gpu(1));
        config_b.qp_type = IbvQpType::Standard;
        let env = TestEnv::new(config_a, config_b).await?;
        run_cross_actor_write(
            &env,
            BufferDevice::Cuda(0),
            BufferDevice::Cuda(1),
            SIZE,
            0x33,
            10,
        )
        .await?;
        env.shutdown().await
    }

    /// Two `IbvBackend::submit` calls back-to-back through the same
    /// helper. The second batch reuses the cached `QueuePairActor`
    /// from the first (the manager's `qp_handles` entry persists
    /// across submits).
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_multi_batch_same_qp() -> Result<(), anyhow::Error> {
        require_rdma();
        let env = TestEnv::same_config(IbvConfig::targeting(IbvDeviceTarget::cpu(0))).await?;
        let src1 = env
            .helper_a
            .allocate(&env.client, 32, BufferDevice::Cpu, 0xa1)
            .await?;
        let dst1 = env
            .helper_b
            .allocate(&env.client, 32, BufferDevice::Cpu, 0)
            .await?;
        env.helper_a
            .submit(
                &env.client,
                vec![BufferHelperOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local_buf: src1,
                    remote_buf: dst1.clone(),
                }],
                5,
            )
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;
        assert_remote_pattern(&env.helper_b, &env.client, dst1, 32, 0xa1).await?;

        let src2 = env
            .helper_a
            .allocate(&env.client, 32, BufferDevice::Cpu, 0xb2)
            .await?;
        let dst2 = env
            .helper_b
            .allocate(&env.client, 32, BufferDevice::Cpu, 0)
            .await?;
        env.helper_a
            .submit(
                &env.client,
                vec![BufferHelperOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local_buf: src2,
                    remote_buf: dst2.clone(),
                }],
                5,
            )
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;
        assert_remote_pattern(&env.helper_b, &env.client, dst2, 32, 0xb2).await?;
        env.shutdown().await
    }

    /// Single submit batch with ops landing on multiple `QpKey`
    /// groups: loopback (helper_a → helper_a), cross-actor cpu↔cpu,
    /// cpu↔cuda, cuda↔cpu, cuda↔cuda. Exercises the manager's
    /// per-QP slicing and concurrent multi-QP dispatch.
    #[timed_test::async_timed_test(timeout_secs = 120)]
    async fn test_multi_op_multi_qp() -> Result<(), anyhow::Error> {
        require_rdma();
        require_cuda();
        const SIZE: usize = 2 * 1024 * 1024;

        let env = TestEnv::same_config(IbvConfig::default()).await?;

        const LOOPBACK_PAT: u8 = 0x11;
        let lb_src = env
            .helper_a
            .allocate(&env.client, SIZE, BufferDevice::Cpu, LOOPBACK_PAT)
            .await?;
        let lb_dst = env
            .helper_a
            .allocate(&env.client, SIZE, BufferDevice::Cpu, 0)
            .await?;

        const CC_PAT: u8 = 0x22;
        let cc_src = env
            .helper_a
            .allocate(&env.client, SIZE, BufferDevice::Cpu, CC_PAT)
            .await?;
        let cc_dst = env
            .helper_b
            .allocate(&env.client, SIZE, BufferDevice::Cpu, 0)
            .await?;

        const CG_PAT: u8 = 0x33;
        let cg_src = env
            .helper_a
            .allocate(&env.client, SIZE, BufferDevice::Cpu, CG_PAT)
            .await?;
        let cg_dst = env
            .helper_b
            .allocate(&env.client, SIZE, BufferDevice::Cuda(1), 0)
            .await?;

        const GC_PAT: u8 = 0x44;
        let gc_src = env
            .helper_a
            .allocate(&env.client, SIZE, BufferDevice::Cuda(0), GC_PAT)
            .await?;
        let gc_dst = env
            .helper_b
            .allocate(&env.client, SIZE, BufferDevice::Cpu, 0)
            .await?;

        const GG_PAT: u8 = 0x55;
        let gg_src = env
            .helper_b
            .allocate(&env.client, SIZE, BufferDevice::Cuda(1), GG_PAT)
            .await?;
        let gg_dst = env
            .helper_a
            .allocate(&env.client, SIZE, BufferDevice::Cuda(0), 0)
            .await?;

        env.helper_a
            .submit(
                &env.client,
                vec![
                    BufferHelperOp {
                        op_type: RdmaOpType::WriteFromLocal,
                        local_buf: lb_src,
                        remote_buf: lb_dst.clone(),
                    },
                    BufferHelperOp {
                        op_type: RdmaOpType::WriteFromLocal,
                        local_buf: cc_src,
                        remote_buf: cc_dst.clone(),
                    },
                    BufferHelperOp {
                        op_type: RdmaOpType::WriteFromLocal,
                        local_buf: cg_src,
                        remote_buf: cg_dst.clone(),
                    },
                    BufferHelperOp {
                        op_type: RdmaOpType::WriteFromLocal,
                        local_buf: gc_src,
                        remote_buf: gc_dst.clone(),
                    },
                    BufferHelperOp {
                        op_type: RdmaOpType::ReadIntoLocal,
                        local_buf: gg_dst.clone(),
                        remote_buf: gg_src,
                    },
                ],
                30,
            )
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;

        assert_remote_pattern(&env.helper_a, &env.client, lb_dst, SIZE, LOOPBACK_PAT).await?;
        assert_remote_pattern(&env.helper_b, &env.client, cc_dst, SIZE, CC_PAT).await?;
        assert_remote_pattern(&env.helper_b, &env.client, cg_dst, SIZE, CG_PAT).await?;
        assert_remote_pattern(&env.helper_b, &env.client, gc_dst, SIZE, GC_PAT).await?;
        assert_remote_pattern(&env.helper_a, &env.client, gg_dst, SIZE, GG_PAT).await?;

        env.shutdown().await
    }

    /// Force the timeout branch in `IbvBackend::submit`. A near-zero
    /// timeout fires before any per-op replies arrive, so the
    /// aggregated error reports the timeout terminal cause.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_submit_timeout() -> Result<(), anyhow::Error> {
        require_rdma();
        const SIZE: usize = 1024 * 1024;
        let env = TestEnv::same_config(IbvConfig::targeting(IbvDeviceTarget::cpu(0))).await?;
        let src = env
            .helper_a
            .allocate(&env.client, SIZE, BufferDevice::Cpu, 0x77)
            .await?;
        let dst = env
            .helper_b
            .allocate(&env.client, SIZE, BufferDevice::Cpu, 0)
            .await?;
        let result = env
            .helper_a
            .submit(
                &env.client,
                vec![BufferHelperOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local_buf: src,
                    remote_buf: dst,
                }],
                0,
            )
            .await?;
        let err = result.expect_err("expected submit to time out");
        assert!(
            err.contains("submit timed out"),
            "unexpected error message: {err}",
        );
        env.shutdown().await
    }

    /// Submit a batch with a bogus op in the middle. RC
    /// completions fire in posting order, so the good op before it
    /// completes normally, the bogus op fails with `REM_ACCESS_ERR`
    /// (it puts the QP into error state), and the good op after it
    /// gets flushed with `WC_WR_FLUSH_ERR`. Verifies (a) op 0 is
    /// absent from the aggregated error and its bytes transferred,
    /// (b) ops 1 and 2 both appear in the error, and (c) op 2's
    /// destination was *not* written (the flush meant nothing was
    /// transferred).
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_partial_failure_batch() -> Result<(), anyhow::Error> {
        require_rdma();
        const SIZE: usize = 32;
        let env = TestEnv::same_config(IbvConfig::targeting(IbvDeviceTarget::cpu(0))).await?;

        const GOOD_PAT: u8 = 0xc3;
        const POST_FLUSH_PAT: u8 = 0xde;

        let good_src_0 = env
            .helper_a
            .allocate(&env.client, SIZE, BufferDevice::Cpu, GOOD_PAT)
            .await?;
        let good_dst_0 = env
            .helper_b
            .allocate(&env.client, SIZE, BufferDevice::Cpu, 0)
            .await?;

        let bogus_src = env
            .helper_a
            .allocate(&env.client, SIZE, BufferDevice::Cpu, 0xee)
            .await?;
        let real_remote = env
            .helper_b
            .allocate(&env.client, SIZE, BufferDevice::Cpu, 0)
            .await?;
        let mut bogus_remote = real_remote.clone();
        let bufs = [
            bogus_remote
                .backends
                .mlx
                .as_mut()
                .map(|ctx| &mut ctx.buffer),
            bogus_remote
                .backends
                .efa
                .as_mut()
                .map(|ctx| &mut ctx.buffer),
        ];
        for buf in bufs.into_iter().flatten() {
            buf.rkey = 0xdead_beef;
            buf.addr = 0xdead_0000;
        }

        let post_flush_src = env
            .helper_a
            .allocate(&env.client, SIZE, BufferDevice::Cpu, POST_FLUSH_PAT)
            .await?;
        let post_flush_dst = env
            .helper_b
            .allocate(&env.client, SIZE, BufferDevice::Cpu, 0)
            .await?;

        let result = env
            .helper_a
            .submit(
                &env.client,
                vec![
                    BufferHelperOp {
                        op_type: RdmaOpType::WriteFromLocal,
                        local_buf: good_src_0,
                        remote_buf: good_dst_0.clone(),
                    },
                    BufferHelperOp {
                        op_type: RdmaOpType::WriteFromLocal,
                        local_buf: bogus_src,
                        remote_buf: bogus_remote,
                    },
                    BufferHelperOp {
                        op_type: RdmaOpType::WriteFromLocal,
                        local_buf: post_flush_src,
                        remote_buf: post_flush_dst.clone(),
                    },
                ],
                10,
            )
            .await?;
        let err = result.expect_err("expected at least one op to fail");
        let rem_access = format!(
            "status={:?}",
            rdmaxcel_sys::ibv_wc_status::IBV_WC_REM_ACCESS_ERR,
        );
        let wr_flush = format!(
            "status={:?}",
            rdmaxcel_sys::ibv_wc_status::IBV_WC_WR_FLUSH_ERR,
        );
        assert!(
            !err.contains("op 0:"),
            "op 0 should not appear in error: {err}",
        );
        let op1 = err
            .split("\n  ")
            .find(|line| line.starts_with("op 1:"))
            .unwrap_or_else(|| panic!("expected op 1 line in error: {err}"));
        assert!(
            op1.contains("completion failed") && op1.contains(&rem_access),
            "expected op 1 to fail with REM_ACCESS_ERR: {op1}",
        );
        let op2 = err
            .split("\n  ")
            .find(|line| line.starts_with("op 2:"))
            .unwrap_or_else(|| panic!("expected op 2 line in error: {err}"));
        assert!(
            op2.contains("completion failed") && op2.contains(&wr_flush),
            "expected op 2 to be flushed with WR_FLUSH_ERR: {op2}",
        );

        assert_remote_pattern(&env.helper_b, &env.client, good_dst_0, SIZE, GOOD_PAT).await?;
        // The flushed op was never transferred; destination stays zero.
        assert_remote_pattern(&env.helper_b, &env.client, post_flush_dst, SIZE, 0).await?;

        env.shutdown().await
    }
}
