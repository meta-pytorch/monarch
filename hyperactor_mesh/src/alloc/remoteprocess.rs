/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use dashmap::DashMap;
use futures::FutureExt;
use futures::future::join_all;
use futures::future::select_all;
use hyperactor::Named;
use hyperactor::WorldId;
use hyperactor::channel;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::ChannelRx;
use hyperactor::channel::ChannelTransport;
use hyperactor::channel::ChannelTx;
use hyperactor::channel::Rx;
use hyperactor::channel::TcpMode;
use hyperactor::channel::Tx;
use hyperactor::channel::TxStatus;
use hyperactor::clock;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor::config;
use hyperactor::mailbox::DialMailboxRouter;
use hyperactor::mailbox::MailboxServer;
use hyperactor::observe_async;
use hyperactor::observe_result;
use hyperactor::reference::Reference;
use hyperactor::serde_json;
use mockall::automock;
use ndslice::Region;
use ndslice::View;
use ndslice::ViewExt;
use ndslice::view::Extent;
use ndslice::view::Point;
use serde::Deserialize;
use serde::Serialize;
use strum::AsRefStr;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::mpsc::unbounded_channel;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::WatchStream;
use tokio_util::sync::CancellationToken;

use crate::alloc::Alloc;
use crate::alloc::AllocAssignedAddr;
use crate::alloc::AllocConstraints;
use crate::alloc::AllocSpec;
use crate::alloc::Allocator;
use crate::alloc::AllocatorError;
use crate::alloc::ProcState;
use crate::alloc::ProcStopReason;
use crate::alloc::ProcessAllocator;
use crate::alloc::REMOTE_ALLOC_BOOTSTRAP_ADDR;
use crate::alloc::process::CLIENT_TRACE_ID_LABEL;
use crate::alloc::process::ClientContext;
use crate::shortuuid::ShortUuid;

/// Control messages sent from remote process allocator to local allocator.
#[derive(Debug, Clone, Serialize, Deserialize, Named, AsRefStr)]
pub enum RemoteProcessAllocatorMessage {
    /// Create allocation with given spec and send updates to bootstrap_addr.
    Allocate {
        /// The key used to identify this allocation.
        alloc_key: ShortUuid,
        /// The extent to allocate.
        extent: Extent,
        /// Bootstrap address to be used for sending updates.
        bootstrap_addr: ChannelAddr,
        /// Ordered list of hosts in this allocation. Can be used to
        /// pre-populate the any local configurations such as torch.dist.
        hosts: Vec<String>,
        /// Client context which is passed to the ProcessAlloc
        /// Todo: Once RemoteProcessAllocator moves to mailbox,
        /// the client_context will go to the message header instead
        client_context: Option<ClientContext>,
        /// The address allocator should use for its forwarder.
        forwarder_addr: AllocAssignedAddr,
    },
    /// Stop allocation.
    Stop,
    /// Heartbeat message to check if remote process allocator and its
    /// host are alive.
    HeartBeat,
}

/// Control message sent from local allocator to remote allocator
/// relaying process state updates.
/// AsRefStr allows us to log the values
#[derive(Debug, Clone, Serialize, Deserialize, Named, AsRefStr)]
pub enum RemoteProcessProcStateMessage {
    /// Allocation successful and Update, Done messages will follow.
    Allocated {
        alloc_key: ShortUuid,
        world_id: WorldId,
    },
    /// ProcState updates.
    Update(ShortUuid, ProcState),
    /// Underlying Alloc is done.
    Done(ShortUuid),
    /// Heartbeat message to check if client is alive.
    HeartBeat,
}

/// Allocator with a service frontend that wraps ProcessAllocator.
pub struct RemoteProcessAllocator {
    cancel_token: CancellationToken,
}

async fn conditional_sleeper<F: futures::Future<Output = ()>>(t: Option<F>) {
    match t {
        Some(timer) => timer.await,
        None => futures::future::pending().await,
    }
}

impl RemoteProcessAllocator {
    /// Create a new allocator. It will not start until start() is called.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            cancel_token: CancellationToken::new(),
        })
    }

    /// Stop the allocator. This will stop any ongoing allocations.
    pub fn terminate(&self) {
        self.cancel_token.cancel();
    }

    /// Start a remote process allocator with given cmd listening for
    /// RemoteProcessAllocatorMessage on serve_addr. Call will block until cancelled.
    /// The implementation is simple such that it can only handle one Alloc at
    /// a time. Generally that's the most common use-case.
    /// Flow works as follows:
    /// 1. Client sends Allocate message to serve_addr.
    /// 2. Allocator connects to bootstrap_addr, creates Alloc and sends Allocated message.
    /// 3. Allocator streams one or more Update messages to bootstrap_addr as Alloc progresses
    ///    making the following changes:
    ///    * Remap mesh_agent listen address to our own forwarder actor address.
    /// 4. Allocator sends Done message to bootstrap_addr when Alloc is done.
    ///
    /// At any point, client can send Stop message to serve_addr to stop the allocator.
    /// If timeout is Some, the allocator will exit if no client connects within
    /// that timeout, and no child allocation is running.
    #[hyperactor::instrument]
    pub async fn start(
        &self,
        cmd: Command,
        serve_addr: ChannelAddr,
        timeout: Option<Duration>,
    ) -> Result<(), anyhow::Error> {
        let process_allocator = ProcessAllocator::new(cmd);
        self.start_with_allocator(serve_addr, process_allocator, timeout)
            .await
    }

    /// Start a remote process allocator with given allocator listening for
    /// RemoteProcessAllocatorMessage on serve_addr.
    /// Used for testing.
    pub async fn start_with_allocator<A: Allocator + Send + Sync + 'static>(
        &self,
        serve_addr: ChannelAddr,
        mut process_allocator: A,
        timeout: Option<Duration>,
    ) -> Result<(), anyhow::Error>
    where
        <A as Allocator>::Alloc: Send,
        <A as Allocator>::Alloc: Sync,
    {
        tracing::info!("starting remote allocator on: {}", serve_addr);
        let (_, mut rx) = channel::serve(serve_addr.clone(), "remote_allocator".to_string())
            .map_err(anyhow::Error::from)?;

        struct ActiveAllocation {
            handle: JoinHandle<()>,
            cancel_token: CancellationToken,
        }
        #[observe_async("RemoteProcessAllocator")]
        async fn ensure_previous_alloc_stopped(active_allocation: &mut Option<ActiveAllocation>) {
            if let Some(active_allocation) = active_allocation.take() {
                tracing::info!("previous alloc found, stopping");
                active_allocation.cancel_token.cancel();
                match active_allocation.handle.await {
                    Ok(_) => {
                        // Todo, add named tracing for state change here
                        tracing::info!("allocation stopped.")
                    }
                    Err(e) => {
                        tracing::error!("allocation handler failed: {}", e);
                    }
                }
            }
        }

        let mut active_allocation: Option<ActiveAllocation> = None;
        loop {
            // Refresh each loop iteration so the timer updates whenever a message
            // is received.
            let sleep = conditional_sleeper(timeout.map(|t| RealClock.sleep(t)));
            tokio::select! {
                msg = rx.recv() => {
                    match msg {
                        Ok(RemoteProcessAllocatorMessage::Allocate {
                            alloc_key,
                            extent,
                            bootstrap_addr,
                            hosts,
                            client_context,
                            forwarder_addr,
                        }) => {
                            tracing::info!("received allocation request for {} with extent {}", alloc_key, extent);
                            ensure_previous_alloc_stopped(&mut active_allocation).await;

                            // Create the corresponding local allocation spec.
                            let mut constraints: AllocConstraints = Default::default();
                            if let Some(context) = &client_context {
                                constraints = AllocConstraints {
                                    match_labels: HashMap::from([(
                                    CLIENT_TRACE_ID_LABEL.to_string(),
                                    context.trace_id.to_string(),
                                    )]
                                )};
                                tracing::info!(
                                    monarch_client_trace_id = context.trace_id.to_string(),
                                    "allocating...",
                                );
                            }


                            let spec = AllocSpec {
                                extent,
                                constraints,
                                proc_name: None, // TODO(meriksen, direct addressing): we need to pass the addressing mode here
                                transport: ChannelTransport::Unix,
                            };

                            match process_allocator.allocate(spec.clone()).await {
                                Ok(alloc) => {
                                    let cancel_token = CancellationToken::new();
                                    active_allocation = Some(ActiveAllocation {
                                        cancel_token: cancel_token.clone(),
                                        handle: tokio::spawn(Self::handle_allocation_request(
                                            Box::new(alloc) as Box<dyn Alloc + Send + Sync>,
                                            alloc_key,
                                            bootstrap_addr,
                                            hosts,
                                            cancel_token,
                                            forwarder_addr,
                                        )),
                                    })
                                }
                                Err(e) => {
                                    tracing::error!("allocation for {:?} failed: {}", spec, e);
                                    continue;
                                }
                            }
                        }
                        Ok(RemoteProcessAllocatorMessage::Stop) => {
                            tracing::info!("received stop request");

                            ensure_previous_alloc_stopped(&mut active_allocation).await;
                        }
                        // Hearbeat message is discarded immediately after being received, sender (client)
                        // relies on channel ack to know if the receiver (remote process allocator) is
                        // still alive. No state needs to be updated.
                        Ok(RemoteProcessAllocatorMessage::HeartBeat) => {}
                        Err(e) => {
                            tracing::error!("upstream channel error: {}", e);
                            continue;
                        }
                    }
                }
                _ = self.cancel_token.cancelled() => {
                    tracing::info!("main loop cancelled");

                    ensure_previous_alloc_stopped(&mut active_allocation).await;

                    break;
                }
                _ = sleep => {
                    // If there are any active allocations, reset the timeout.
                    if active_allocation.is_some() {
                        continue;
                    }
                    // Else, exit the loop as a client hasn't connected in a reasonable
                    // amount of time.
                    tracing::warn!("timeout of {} seconds elapsed without any allocations, exiting", timeout.unwrap_or_default().as_secs());
                    break;
                }
            }
        }

        Ok(())
    }

    #[observe_async("RemoteProcessAllocator")]
    async fn handle_allocation_request(
        alloc: Box<dyn Alloc + Send + Sync>,
        alloc_key: ShortUuid,
        bootstrap_addr: ChannelAddr,
        hosts: Vec<String>,
        cancel_token: CancellationToken,
        forwarder_addr: AllocAssignedAddr,
    ) {
        tracing::info!("handle allocation request, bootstrap_addr: {bootstrap_addr}");
        // start proc message forwarder
        let (forwarder_addr, forwarder_rx) = match forwarder_addr.serve_with_config() {
            Ok(v) => v,
            Err(e) => {
                tracing::error!("failed to to bootstrap forwarder actor: {}", e);
                return;
            }
        };
        let router = DialMailboxRouter::new();
        let mailbox_handle = router.clone().serve(forwarder_rx);
        tracing::info!("started forwarder on: {}", forwarder_addr);

        // Check if we need to write TORCH_ELASTIC_CUSTOM_HOSTNAMES_LIST_FILE
        // See: https://github.com/fairinternal/xlformers/blob/llama4_monarch/tools/launching/torchx/entrypoint/generate_ranks.py
        if let Ok(hosts_file) = std::env::var("TORCH_ELASTIC_CUSTOM_HOSTNAMES_LIST_FILE") {
            tracing::info!("writing hosts to {}", hosts_file);
            #[derive(Serialize)]
            struct Hosts {
                hostnames: Vec<String>,
            }
            match serde_json::to_string(&Hosts { hostnames: hosts }) {
                Ok(json) => match tokio::fs::File::create(&hosts_file).await {
                    Ok(mut file) => {
                        if file.write_all(json.as_bytes()).await.is_err() {
                            tracing::error!("failed to write hosts to {}", hosts_file);
                            return;
                        }
                    }
                    Err(e) => {
                        tracing::error!("failed to open hosts file {}: {}", hosts_file, e);
                        return;
                    }
                },
                Err(e) => {
                    tracing::error!("failed to serialize hosts: {}", e);
                    return;
                }
            }
        }

        Self::handle_allocation_loop(
            alloc,
            alloc_key,
            bootstrap_addr,
            router,
            forwarder_addr,
            cancel_token,
        )
        .await;

        mailbox_handle.stop("alloc stopped");
        if let Err(e) = mailbox_handle.await {
            tracing::error!("failed to join forwarder: {}", e);
        }
    }

    async fn handle_allocation_loop(
        mut alloc: Box<dyn Alloc + Send + Sync>,
        alloc_key: ShortUuid,
        bootstrap_addr: ChannelAddr,
        router: DialMailboxRouter,
        forward_addr: ChannelAddr,
        cancel_token: CancellationToken,
    ) {
        let world_id = alloc.world_id().clone();
        tracing::info!("starting handle allocation loop for {}", world_id);
        let tx = match channel::dial(bootstrap_addr, "bootstrap-alloc".to_string()) {
            Ok(tx) => tx,
            Err(err) => {
                tracing::error!("failed to dial bootstrap address: {}", err);
                return;
            }
        };
        let message = RemoteProcessProcStateMessage::Allocated {
            alloc_key: alloc_key.clone(),
            world_id,
        };
        tracing::info!(name = message.as_ref(), "sending allocated message",);
        if let Err(e) = tx.send(message).await {
            tracing::error!("failed to send Allocated message: {}", e);
            return;
        }

        let mut mesh_agents_by_create_key = HashMap::new();
        let mut running = true;
        let tx_status = tx.status().clone();
        let mut tx_watcher = WatchStream::new(tx_status);
        loop {
            tokio::select! {
                _ = cancel_token.cancelled(), if running => {
                    tracing::info!("cancelled, stopping allocation");
                    running = false;
                    if let Err(e) = alloc.stop().await {
                        tracing::error!("stop failed: {}", e);
                        break;
                    }
                }
                status = tx_watcher.next(), if running => {
                    match status  {
                        Some(TxStatus::Closed) => {
                            tracing::error!("upstream channel state closed");
                            break;
                        },
                        _ => {
                            tracing::debug!("got channel event: {:?}", status.unwrap());
                            continue;
                        }
                    }
                }
                e = alloc.next() => {
                    match e {
                        Some(event) => {
                            tracing::debug!(name = event.as_ref(), "got event: {:?}", event);
                            let event = match event {
                                ProcState::Created { .. } => event,
                                ProcState::Running { create_key, proc_id, mesh_agent, addr } => {
                                    // TODO(meriksen, direct addressing): disable remapping in direct addressing mode
                                    tracing::debug!("remapping mesh_agent {}: addr {} -> {}", mesh_agent, addr, forward_addr);
                                    mesh_agents_by_create_key.insert(create_key.clone(), mesh_agent.clone());
                                    router.bind(mesh_agent.actor_id().proc_id().clone().into(), addr);
                                    ProcState::Running { create_key, proc_id, mesh_agent, addr: forward_addr.clone() }
                                },
                                ProcState::Stopped { create_key, reason } => {
                                    match mesh_agents_by_create_key.remove(&create_key) {
                                        Some(mesh_agent) => {
                                            tracing::debug!("unmapping mesh_agent {}", mesh_agent);
                                            let agent_ref: Reference = mesh_agent.actor_id().proc_id().clone().into();
                                            router.unbind(&agent_ref);
                                        },
                                        None => {
                                            tracing::warn!("mesh_agent not found for create key {}", create_key);
                                        }
                                    }
                                    ProcState::Stopped { create_key, reason }
                                },
                                ProcState::Failed { ref world_id, ref description } => {
                                    tracing::error!("allocation failed for {}: {}", world_id, description);
                                    event
                                }
                            };
                            tracing::debug!(name = event.as_ref(), "sending event: {:?}", event);
                            tx.post(RemoteProcessProcStateMessage::Update(alloc_key.clone(), event));
                        }
                        None => {
                            tracing::debug!("sending done");
                            tx.post(RemoteProcessProcStateMessage::Done(alloc_key.clone()));
                            running = false;
                            break;
                        }
                    }
                }
                _ = RealClock.sleep(config::global::get(config::REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL)) => {
                    tracing::trace!("sending heartbeat");
                    tx.post(RemoteProcessProcStateMessage::HeartBeat);
                }
            }
        }
        tracing::info!("allocation handler loop exited");
        if running {
            tracing::info!("stopping processes");
            if let Err(e) = alloc.stop_and_wait().await {
                tracing::error!("stop failed: {}", e);
                return;
            }
            tracing::info!("stop finished");
        }
    }
}

/// HostId is a string-based identifier of the host. It may be the same
/// as the hostname but in some situations a separate ID may be used.
type HostId = String;

/// A host entry passed down from the initializer.
#[derive(Clone)]
pub struct RemoteProcessAllocHost {
    /// A unique identifier of that host. Hostname may be used by ID can
    /// be different in some situations.
    pub id: HostId,
    /// The FQDN of the host.
    pub hostname: String,
}

/// State of a host in the RemoteProcessAlloc.
struct RemoteProcessAllocHostState {
    /// The allocation key used to identify the host.
    alloc_key: ShortUuid,
    /// The host ID of the remote host.
    host_id: HostId,
    /// TX channel to the remote host allocator.
    tx: ChannelTx<RemoteProcessAllocatorMessage>,
    /// Set of active processes on this host.
    active_procs: HashSet<ShortUuid>,
    /// Region allocated by host.
    region: Region,
    /// World ID for this host as indicated from Allocated message.
    world_id: Option<WorldId>,
    /// If remote allocator sent us ProcState::Failed.
    failed: bool,
    /// If remote allocater has ever allocated a proc.
    allocated: bool,
}

#[automock]
#[async_trait]
/// Interface to provide the set of hosts to be used by RemoteProcessAlloc.
pub trait RemoteProcessAllocInitializer {
    /// Initializes and returns a list of hosts to be used by this RemoteProcessAlloc.
    async fn initialize_alloc(&mut self) -> Result<Vec<RemoteProcessAllocHost>, anyhow::Error>;
}

/// Wrapper struct around `HashMap<HostId, RemoteProcessAllocHostState>`
/// to ensure that host addresses are synced with the signal handler
struct HostStates {
    inner: HashMap<HostId, RemoteProcessAllocHostState>,
    host_addresses: Arc<DashMap<HostId, ChannelAddr>>,
}

impl HostStates {
    fn new(host_addresses: Arc<DashMap<HostId, ChannelAddr>>) -> HostStates {
        Self {
            inner: HashMap::new(),
            host_addresses,
        }
    }

    fn insert(
        &mut self,
        host_id: HostId,
        state: RemoteProcessAllocHostState,
        address: ChannelAddr,
    ) {
        self.host_addresses.insert(host_id.clone(), address);
        self.inner.insert(host_id, state);
    }

    fn get(&self, host_id: &HostId) -> Option<&RemoteProcessAllocHostState> {
        self.inner.get(host_id)
    }

    fn get_mut(&mut self, host_id: &HostId) -> Option<&mut RemoteProcessAllocHostState> {
        self.inner.get_mut(host_id)
    }

    fn remove(&mut self, host_id: &HostId) -> Option<RemoteProcessAllocHostState> {
        self.host_addresses.remove(host_id);
        self.inner.remove(host_id)
    }

    fn iter(&self) -> impl Iterator<Item = (&HostId, &RemoteProcessAllocHostState)> {
        self.inner.iter()
    }

    fn iter_mut(&mut self) -> impl Iterator<Item = (&HostId, &mut RemoteProcessAllocHostState)> {
        self.inner.iter_mut()
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
    // Any missing HashMap methods should be added here as needed
}

/// A generalized implementation of an Alloc using one or more hosts running
/// RemoteProcessAlloc for process allocation.
pub struct RemoteProcessAlloc {
    // The initializer to be called at the first next() invocation to obtain
    // allocated hosts.
    initializer: Box<dyn RemoteProcessAllocInitializer + Send + Sync>,
    spec: AllocSpec,
    remote_allocator_port: u16,
    world_id: WorldId,
    ordered_hosts: Vec<RemoteProcessAllocHost>,
    // Indicates that the initial remote allocation requests have been sent.
    started: bool,
    // Indicates that this Alloc is active (we have at least one remote process running).
    running: bool,
    // Inidicates that the allocation process has permanently failed.
    failed: bool,
    // Maps the alloc key to the host.
    alloc_to_host: HashMap<ShortUuid, HostId>,
    host_states: HostStates,
    world_offsets: HashMap<WorldId, usize>,
    event_queue: VecDeque<ProcState>,
    comm_watcher_tx: UnboundedSender<HostId>,
    comm_watcher_rx: UnboundedReceiver<HostId>,

    bootstrap_addr: ChannelAddr,
    rx: ChannelRx<RemoteProcessProcStateMessage>,
    _signal_cleanup_guard: hyperactor::SignalCleanupGuard,
}

impl RemoteProcessAlloc {
    /// Create a new Alloc. initializer will be called on the first invocation of next()
    /// to obtain a list of allocate hosts. Then Allocate message will be sent to all
    /// RemoteProcessAllocator on all hosts. Heartbeats will be used to maintain health
    /// status of remote hosts.
    #[observe_result("RemoteProcessAlloc")]
    pub async fn new(
        spec: AllocSpec,
        world_id: WorldId,
        remote_allocator_port: u16,
        initializer: impl RemoteProcessAllocInitializer + Send + Sync + 'static,
    ) -> Result<Self, anyhow::Error> {
        let alloc_serve_addr = match config::global::try_get_cloned(REMOTE_ALLOC_BOOTSTRAP_ADDR) {
            Some(addr_str) => AllocAssignedAddr::new(addr_str.parse()?),
            None => AllocAssignedAddr::new(ChannelAddr::any(spec.transport.clone())),
        };

        let (bootstrap_addr, rx) = alloc_serve_addr.serve_with_config()?;

        tracing::info!(
            "starting alloc for {} on: {}",
            world_id,
            bootstrap_addr.clone()
        );

        let (comm_watcher_tx, comm_watcher_rx) = unbounded_channel();

        let host_addresses = Arc::new(DashMap::<HostId, ChannelAddr>::new());
        let host_addresses_for_signal = host_addresses.clone();

        // Register cleanup callback with global signal manager
        let signal_cleanup_guard =
            hyperactor::register_signal_cleanup_scoped(Box::pin(async move {
                join_all(host_addresses_for_signal.iter().map(|entry| async move {
                    let addr = entry.value().clone();
                    match channel::dial(addr.clone(), "signal-cleanup".to_string()) {
                        Ok(tx) => {
                            if let Err(e) = tx.send(RemoteProcessAllocatorMessage::Stop).await {
                                tracing::error!("Failed to send Stop to {}: {}", addr, e);
                            }
                        }
                        Err(e) => {
                            tracing::error!("Failed to dial {} during signal cleanup: {}", addr, e);
                        }
                    }
                }))
                .await;
            }));

        Ok(Self {
            spec,
            world_id,
            remote_allocator_port,
            initializer: Box::new(initializer),
            world_offsets: HashMap::new(),
            ordered_hosts: Vec::new(),
            alloc_to_host: HashMap::new(),
            host_states: HostStates::new(host_addresses),
            bootstrap_addr,
            event_queue: VecDeque::new(),
            comm_watcher_tx,
            comm_watcher_rx,
            rx,
            started: false,
            running: true,
            failed: false,
            _signal_cleanup_guard: signal_cleanup_guard,
        })
    }

    /// Start a RemoteProcessAllocator tx watcher. It spawns a task that monitor the underlying
    /// TxStatus and repot to the main next() loop via comm_watcher_tx. It does not however
    /// send the actual heartbteats. Just watches the status.
    /// The task will terminate once comm_watcher_rx is closed.
    ///
    /// A task approach was used here as opposed to select! to avoid nesting complexities with
    /// select!() and select_all().
    async fn start_comm_watcher(&self) {
        let mut tx_watchers = Vec::new();
        for host in &self.ordered_hosts {
            let tx_status = self.host_states.get(&host.id).unwrap().tx.status().clone();
            let watcher = WatchStream::new(tx_status);
            tx_watchers.push((watcher, host.id.clone()));
        }
        assert!(!tx_watchers.is_empty());
        let tx = self.comm_watcher_tx.clone();
        tokio::spawn(async move {
            loop {
                let mut tx_status_futures = Vec::new();
                for (watcher, _) in &mut tx_watchers {
                    let fut = watcher.next().boxed();
                    tx_status_futures.push(fut);
                }
                let (tx_status, index, _) = select_all(tx_status_futures).await;
                let host_id = match tx_watchers.get(index) {
                    Some((_, host_id)) => host_id.clone(),
                    None => {
                        // Should never happen
                        tracing::error!(
                            "got selected index {} with no matching host in {}",
                            index,
                            tx_watchers.len()
                        );
                        continue;
                    }
                };
                if let Some(tx_status) = tx_status {
                    tracing::debug!("host {} channel event: {:?}", host_id, tx_status);
                    if tx_status == TxStatus::Closed {
                        if tx.send(host_id.clone()).is_err() {
                            // other side closed
                            break;
                        }
                        tx_watchers.remove(index);
                        if tx_watchers.is_empty() {
                            // All of the statuses have been closed, exit the loop.
                            break;
                        }
                    }
                }
            }
        });
    }

    /// Ensure that we have the list of allocated hosts and that we send Allocate
    /// request to all o fthem. Once done, start_comm_watcher() is called to start
    /// the channel watcher.
    /// Function is idempotent.
    async fn ensure_started(&mut self) -> Result<(), anyhow::Error> {
        if self.started || self.failed {
            return Ok(());
        }

        self.started = true;
        let hosts = self
            .initializer
            .initialize_alloc()
            .await
            .context("alloc initializer error")?;
        if hosts.is_empty() {
            anyhow::bail!("initializer returned empty list of hosts");
        }
        // prepare a list of host names in this allocation to be sent
        // to remote allocators.
        let hostnames: Vec<_> = hosts.iter().map(|e| e.hostname.clone()).collect();
        tracing::info!("obtained {} hosts for this allocation", hostnames.len());

        // We require at least a dimension for hosts, and one for sub-host (e.g., GPUs)
        anyhow::ensure!(
            self.spec.extent.len() >= 2,
            "invalid extent: {}, expected at least 2 dimensions",
            self.spec.extent
        );

        // We group by the innermost dimension of the extent.
        let split_dim = &self.spec.extent.labels()[self.spec.extent.len() - 1];
        for (i, region) in self.spec.extent.group_by(split_dim)?.enumerate() {
            let host = &hosts[i];
            tracing::debug!("allocating: {} for host: {}", region, host.id);

            let remote_addr = match self.spec.transport {
                ChannelTransport::MetaTls(_) => {
                    format!("metatls!{}:{}", host.hostname, self.remote_allocator_port)
                }
                ChannelTransport::Tcp(TcpMode::Localhost) => {
                    // TODO: @rusch see about moving over to config for this
                    format!("tcp![::1]:{}", self.remote_allocator_port)
                }
                ChannelTransport::Tcp(TcpMode::Hostname) => {
                    format!("tcp!{}:{}", host.hostname, self.remote_allocator_port)
                }
                // Used only for testing.
                ChannelTransport::Unix => host.hostname.clone(),
                _ => {
                    anyhow::bail!(
                        "unsupported transport for host {}: {:?}",
                        host.id,
                        self.spec.transport,
                    );
                }
            };

            tracing::debug!("dialing remote: {} for host {}", remote_addr, host.id);
            let remote_addr = remote_addr.parse::<ChannelAddr>()?;
            let tx = channel::dial(remote_addr.clone(), "remote-dial".to_string())
                .map_err(anyhow::Error::from)
                .context(format!(
                    "failed to dial remote {} for host {}",
                    remote_addr, host.id
                ))?;

            // Possibly we could use the HostId directly here.
            let alloc_key = ShortUuid::generate();
            assert!(
                self.alloc_to_host
                    .insert(alloc_key.clone(), host.id.clone())
                    .is_none()
            );

            let trace_id = hyperactor_telemetry::trace::get_or_create_trace_id();
            let client_context = Some(ClientContext { trace_id });
            let message = RemoteProcessAllocatorMessage::Allocate {
                alloc_key: alloc_key.clone(),
                extent: region.extent(),
                bootstrap_addr: self.bootstrap_addr.clone(),
                hosts: hostnames.clone(),
                client_context,
                // Make sure allocator's forwarder uses the same IP address
                // which is known to alloc. This is to avoid allocator picks
                // its host's private IP address, while its known addres to
                // alloc is a public IP address. In some environment, that
                // could lead to port unreachable error.
                forwarder_addr: AllocAssignedAddr::with_unspecified_port_or_any(&remote_addr),
            };
            tracing::info!(
                name = message.as_ref(),
                "sending allocate message to workers"
            );
            tx.post(message);

            self.host_states.insert(
                host.id.clone(),
                RemoteProcessAllocHostState {
                    alloc_key,
                    host_id: host.id.clone(),
                    tx,
                    active_procs: HashSet::new(),
                    region,
                    world_id: None,
                    failed: false,
                    allocated: false,
                },
                remote_addr,
            );
        }

        self.ordered_hosts = hosts;
        self.start_comm_watcher().await;
        self.started = true;

        Ok(())
    }

    // Given a proc_id, obtain the internal HostState structure.
    fn get_host_state_mut(
        &mut self,
        alloc_key: &ShortUuid,
    ) -> Result<&mut RemoteProcessAllocHostState, anyhow::Error> {
        let host_id: &HostId = self
            .alloc_to_host
            .get(alloc_key)
            .ok_or_else(|| anyhow::anyhow!("alloc with key {} not found", alloc_key))?;

        self.host_states
            .get_mut(host_id)
            .ok_or_else(|| anyhow::anyhow!("no host state found for host {}", host_id))
    }

    // Given a proc_id, obtain the internal HostState structure.
    fn get_host_state(
        &self,
        alloc_key: &ShortUuid,
    ) -> Result<&RemoteProcessAllocHostState, anyhow::Error> {
        let host_id: &HostId = self
            .alloc_to_host
            .get(alloc_key)
            .ok_or_else(|| anyhow::anyhow!("alloc with key {} not found", alloc_key))?;

        self.host_states
            .get(host_id)
            .ok_or_else(|| anyhow::anyhow!("no host state found for host {}", host_id))
    }

    fn remove_host_state(
        &mut self,
        alloc_key: &ShortUuid,
    ) -> Result<RemoteProcessAllocHostState, anyhow::Error> {
        let host_id: &HostId = self
            .alloc_to_host
            .get(alloc_key)
            .ok_or_else(|| anyhow::anyhow!("alloc with key {} not found", alloc_key))?;

        self.host_states
            .remove(host_id)
            .ok_or_else(|| anyhow::anyhow!("no host state found for host {}", host_id))
    }

    fn add_proc_id_to_host_state(
        &mut self,
        alloc_key: &ShortUuid,
        create_key: &ShortUuid,
    ) -> Result<(), anyhow::Error> {
        let task_state = self.get_host_state_mut(alloc_key)?;
        if !task_state.active_procs.insert(create_key.clone()) {
            // Should not happen but we can ignore
            tracing::error!("proc with create key {} already in host state", create_key);
        }
        task_state.allocated = true;
        Ok(())
    }

    fn remove_proc_from_host_state(
        &mut self,
        alloc_key: &ShortUuid,
        create_key: &ShortUuid,
    ) -> Result<(), anyhow::Error> {
        let task_state = self.get_host_state_mut(alloc_key)?;
        if !task_state.active_procs.remove(create_key) {
            // Should not happen but we can ignore
            tracing::error!("proc with create_key already in host state: {}", create_key);
        }
        Ok(())
    }

    // Reproject proc world coords to global shape coords.
    fn project_proc_into_global_extent(
        &self,
        alloc_key: &ShortUuid,
        point: &Point,
    ) -> Result<Point, anyhow::Error> {
        let global_rank = self
            .get_host_state(alloc_key)?
            .region
            .get(point.rank())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "rank {} out of bounds for in alloc {}",
                    point.rank(),
                    alloc_key
                )
            })?;
        Ok(self.spec.extent.point_of_rank(global_rank)?)
    }

    // Cleanup a comm-failed host information by its ID.
    fn cleanup_host_channel_closed(
        &mut self,
        host_id: HostId,
    ) -> Result<Vec<ShortUuid>, anyhow::Error> {
        let state = match self.host_states.remove(&host_id) {
            Some(state) => state,
            None => {
                // this should never happen.
                anyhow::bail!(
                    "got channel closed event for host {} which has no known state",
                    host_id
                );
            }
        };
        self.ordered_hosts.retain(|host| host.id != host_id);
        self.alloc_to_host.remove(&state.alloc_key);
        if let Some(world_id) = state.world_id {
            self.world_offsets.remove(&world_id);
        }
        let create_keys = state.active_procs.iter().cloned().collect();

        Ok(create_keys)
    }
}

#[async_trait]
impl Alloc for RemoteProcessAlloc {
    async fn next(&mut self) -> Option<ProcState> {
        loop {
            if let state @ Some(_) = self.event_queue.pop_front() {
                break state;
            }

            if !self.running {
                break None;
            }

            if let Err(e) = self.ensure_started().await {
                break Some(ProcState::Failed {
                    world_id: self.world_id.clone(),
                    description: format!("failed to ensure started: {:#}", e),
                });
            }

            let heartbeat_interval =
                config::global::get(config::REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL);
            let mut heartbeat_time = hyperactor::clock::RealClock.now() + heartbeat_interval;
            // rerun outer loop in case we pushed new items to the event queue
            let mut reloop = false;
            let update = loop {
                tokio::select! {
                    msg = self.rx.recv() => {
                        tracing::debug!("got ProcState message from allocator: {:?}", msg);
                        match msg {
                            Ok(RemoteProcessProcStateMessage::Allocated { alloc_key, world_id }) => {
                                tracing::info!("remote alloc {}: allocated", alloc_key);
                                match self.get_host_state_mut(&alloc_key) {
                                    Ok(state) => {
                                        state.world_id = Some(world_id.clone());
                                    }
                                    Err(err) => {
                                        // should never happenA
                                        tracing::error!(
                                            "received allocated message alloc: {} with no known state: {}",
                                            alloc_key, err,
                                        );
                                    }
                                }
                            }
                            Ok(RemoteProcessProcStateMessage::Update(alloc_key, proc_state)) => {
                                let update = match proc_state {
                                    ProcState::Created { ref create_key, .. } => {
                                        if let Err(e) = self.add_proc_id_to_host_state(&alloc_key, create_key) {
                                            tracing::error!("failed to add proc with create key {} host state: {}", create_key, e);
                                        }
                                        proc_state
                                    }
                                    ProcState::Stopped{ ref create_key, ..} => {
                                        if let Err(e) = self.remove_proc_from_host_state(&alloc_key, create_key) {
                                            tracing::error!("failed to remove proc with create key {} host state: {}", create_key, e);
                                        }
                                        proc_state
                                    }
                                    ProcState::Failed { ref world_id, ref description } => {
                                        match self.get_host_state_mut(&alloc_key) {
                                            Ok(state) => {
                                                state.failed = true;
                                                ProcState::Failed {
                                                    world_id: world_id.clone(),
                                                    description: format!("host {} failed: {}", state.host_id, description),
                                                }
                                            }
                                            Err(e) => {
                                                tracing::error!("failed to find host state for world id: {}: {}", world_id, e);
                                                proc_state
                                            }
                                        }
                                    }
                                    _ => proc_state
                                };

                                break Some((Some(alloc_key), update));
                            }
                            Ok(RemoteProcessProcStateMessage::Done(alloc_key)) => {
                                tracing::info!("allocator {} is done", alloc_key);

                                if let Ok(state) = self.remove_host_state(&alloc_key) {
                                    if !state.active_procs.is_empty() {
                                        tracing::error!("received done for alloc {} with active procs: {:?}", alloc_key, state.active_procs);
                                    }
                                } else {
                                    tracing::error!("received done for unknown alloc {}", alloc_key);
                                }

                                if self.host_states.is_empty() {
                                    self.running = false;
                                    break None;
                                }
                            }
                            // Hearbeat message is discarded immediately after being received, sender (remote
                            // process allocator) relies on channel ack to know if the receiver (client) is
                            // still alive. No state needs to be updated.
                            Ok(RemoteProcessProcStateMessage::HeartBeat) => {}
                            Err(e) => {
                                break Some((None, ProcState::Failed {world_id: self.world_id.clone(), description: format!("error receiving events: {}", e)}));
                            }
                        }
                    }

                    _ = clock::RealClock.sleep_until(heartbeat_time) => {
                        self.host_states.iter().for_each(|(_, host_state)| host_state.tx.post(RemoteProcessAllocatorMessage::HeartBeat));
                        heartbeat_time = hyperactor::clock::RealClock.now() + heartbeat_interval;
                    }

                    closed_host_id = self.comm_watcher_rx.recv() => {
                        if let Some(closed_host_id) = closed_host_id {
                            tracing::debug!("host {} channel closed, cleaning up", closed_host_id);
                            if let Some(state) = self.host_states.get(&closed_host_id)
                                && !state.allocated {
                                    break Some((None, ProcState::Failed {
                                        world_id: self.world_id.clone(),
                                        description: format!(
                                            "no process has ever been allocated on {} before the channel is closed; \
                                            a common issue could be the channel was never established",
                                            closed_host_id
                                        )}));
                                }
                            let create_keys = match self.cleanup_host_channel_closed(closed_host_id) {
                                Ok(create_keys) => create_keys,
                                Err(err) => {
                                    tracing::error!("failed to cleanup disconnected host: {}", err);
                                    continue;
                                }
                            };
                            for create_key in create_keys {
                                tracing::debug!("queuing Stopped state for proc with create key {}", create_key);
                                self.event_queue.push_back(
                                    ProcState::Stopped {
                                        create_key,
                                        reason: ProcStopReason::HostWatchdog
                                    }
                                );
                            }
                            // Check if there are any hosts left
                            if self.host_states.is_empty() {
                                tracing::info!("no more hosts left, stopping the alloc");
                                self.running = false;
                            }
                            // Kick back to the outer loop to pop off the queue if necessary
                            reloop = true;
                            break None;
                        } else {
                            // other side closed
                            tracing::warn!("unexpected comm watcher channel close");
                            break None;
                        }
                    }
                }
            };

            if reloop {
                // Kick back to the outer loop to pop off the queue.
                // Should not have produced an update.
                assert!(update.is_none());
                continue;
            }

            break match update {
                Some((
                    Some(alloc_key),
                    ProcState::Created {
                        create_key,
                        point,
                        pid,
                    },
                )) => match self.project_proc_into_global_extent(&alloc_key, &point) {
                    Ok(global_point) => {
                        tracing::debug!("reprojected coords: {} -> {}", point, global_point);
                        Some(ProcState::Created {
                            create_key,
                            point: global_point,
                            pid,
                        })
                    }
                    Err(e) => {
                        tracing::error!(
                            "failed to project coords for proc: {}.{}: {}",
                            alloc_key,
                            create_key,
                            e
                        );
                        None
                    }
                },
                Some((None, ProcState::Created { .. })) => {
                    panic!("illegal state: missing alloc_key for ProcState::Created event")
                }
                Some((_, update)) => {
                    if let ProcState::Failed { description, .. } = &update {
                        tracing::error!(description);
                        self.failed = true;
                    }
                    Some(update)
                }
                None => None,
            };
        }
    }

    fn spec(&self) -> &AllocSpec {
        &self.spec
    }

    fn extent(&self) -> &Extent {
        &self.spec.extent
    }

    fn world_id(&self) -> &WorldId {
        &self.world_id
    }

    async fn stop(&mut self) -> Result<(), AllocatorError> {
        tracing::info!("stopping alloc");

        for (host_id, task_state) in self.host_states.iter_mut() {
            tracing::debug!("stopping alloc at host {}", host_id);
            task_state.tx.post(RemoteProcessAllocatorMessage::Stop);
        }

        Ok(())
    }

    /// For Tcp and Metatls, return a router address which has the same IP address
    /// as the alloc's bootstrap address, but with port set as 0. We do this
    /// instead of using `ChannelAddr::any` is because we want alloc uses the
    /// same IP address for all its frontend ports. In some environment, the
    /// host can have public IP address and private IP address, and use the wrong
    /// one could lead to port unreachable error.
    ///
    /// For other channel types, this method still uses ChannelAddr::any.
    fn client_router_addr(&self) -> AllocAssignedAddr {
        AllocAssignedAddr::with_unspecified_port_or_any(&self.bootstrap_addr)
    }
}

impl Drop for RemoteProcessAlloc {
    fn drop(&mut self) {
        tracing::debug!("dropping RemoteProcessAlloc of world_id {}", self.world_id);
    }
}

#[cfg(test)]
mod test {
    use std::assert_matches::assert_matches;

    use hyperactor::ActorRef;
    use hyperactor::channel::ChannelRx;
    use hyperactor::clock::ClockKind;
    use hyperactor::id;
    use ndslice::extent;
    use tokio::sync::oneshot;

    use super::*;
    use crate::alloc::ChannelTransport;
    use crate::alloc::MockAlloc;
    use crate::alloc::MockAllocWrapper;
    use crate::alloc::MockAllocator;
    use crate::alloc::ProcStopReason;
    use crate::proc_mesh::mesh_agent::ProcMeshAgent;

    async fn read_all_created(rx: &mut ChannelRx<RemoteProcessProcStateMessage>, alloc_len: usize) {
        let mut i: usize = 0;
        while i < alloc_len {
            let m = rx.recv().await.unwrap();
            match m {
                RemoteProcessProcStateMessage::Update(_, ProcState::Created { .. }) => i += 1,
                RemoteProcessProcStateMessage::HeartBeat => {}
                _ => panic!("unexpected message: {:?}", m),
            }
        }
    }

    async fn read_all_running(rx: &mut ChannelRx<RemoteProcessProcStateMessage>, alloc_len: usize) {
        let mut i: usize = 0;
        while i < alloc_len {
            let m = rx.recv().await.unwrap();
            match m {
                RemoteProcessProcStateMessage::Update(_, ProcState::Running { .. }) => i += 1,
                RemoteProcessProcStateMessage::HeartBeat => {}
                _ => panic!("unexpected message: {:?}", m),
            }
        }
    }

    async fn read_all_stopped(rx: &mut ChannelRx<RemoteProcessProcStateMessage>, alloc_len: usize) {
        let mut i: usize = 0;
        while i < alloc_len {
            let m = rx.recv().await.unwrap();
            match m {
                RemoteProcessProcStateMessage::Update(_, ProcState::Stopped { .. }) => i += 1,
                RemoteProcessProcStateMessage::HeartBeat => {}
                _ => panic!("unexpected message: {:?}", m),
            }
        }
    }

    fn set_procstate_expectations(alloc: &mut MockAlloc, extent: Extent) {
        alloc.expect_extent().return_const(extent.clone());
        let mut create_keys = Vec::new();
        for point in extent.points() {
            let create_key = ShortUuid::generate();
            create_keys.push(create_key.clone());
            alloc.expect_next().times(1).return_once(move || {
                Some(ProcState::Created {
                    create_key: create_key.clone(),
                    point,
                    pid: 0,
                })
            });
        }
        for (i, create_key) in create_keys
            .iter()
            .take(extent.num_ranks())
            .cloned()
            .enumerate()
        {
            let proc_id = format!("test[{i}]").parse().unwrap();
            let mesh_agent = ActorRef::<ProcMeshAgent>::attest(
                format!("test[{i}].mesh_agent[{i}]").parse().unwrap(),
            );
            alloc.expect_next().times(1).return_once(move || {
                Some(ProcState::Running {
                    create_key,
                    proc_id,
                    addr: ChannelAddr::Unix("/proc0".parse().unwrap()),
                    mesh_agent,
                })
            });
        }
        for create_key in create_keys.iter().take(extent.num_ranks()).cloned() {
            alloc.expect_next().times(1).return_once(|| {
                Some(ProcState::Stopped {
                    create_key,
                    reason: ProcStopReason::Unknown,
                })
            });
        }
    }

    #[timed_test::async_timed_test(timeout_secs = 5)]
    async fn test_simple() {
        let config = hyperactor::config::global::lock();
        let _guard = config.override_key(
            hyperactor::config::REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL,
            Duration::from_millis(100),
        );
        hyperactor_telemetry::initialize_logging_for_test();
        let serve_addr = ChannelAddr::any(ChannelTransport::Unix);
        let bootstrap_addr = ChannelAddr::any(ChannelTransport::Unix);
        let (_, mut rx) = channel::serve(bootstrap_addr.clone(), "test".to_string()).unwrap();

        let extent = extent!(host = 1, gpu = 2);
        let tx = channel::dial(serve_addr.clone(), "test".to_string()).unwrap();

        let world_id: WorldId = id!(test_world_id);
        let mut alloc = MockAlloc::new();
        alloc.expect_world_id().return_const(world_id.clone());
        alloc.expect_extent().return_const(extent.clone());

        set_procstate_expectations(&mut alloc, extent.clone());

        // final none
        alloc.expect_next().return_const(None);

        let mut allocator = MockAllocator::new();
        let total_messages = extent.num_ranks() * 3 + 1;
        let mock_wrapper = MockAllocWrapper::new_block_next(
            alloc,
            // block after create, running, stopped and done.
            total_messages,
        );
        allocator
            .expect_allocate()
            .times(1)
            .return_once(move |_| Ok(mock_wrapper));

        let remote_allocator = RemoteProcessAllocator::new();
        let handle = tokio::spawn({
            let remote_allocator = remote_allocator.clone();
            async move {
                remote_allocator
                    .start_with_allocator(serve_addr, allocator, None)
                    .await
            }
        });

        let alloc_key = ShortUuid::generate();

        tx.send(RemoteProcessAllocatorMessage::Allocate {
            alloc_key: alloc_key.clone(),
            extent: extent.clone(),
            bootstrap_addr,
            hosts: vec![],
            client_context: None,
            forwarder_addr: AllocAssignedAddr::with_unspecified_port_or_any(&tx.addr()),
        })
        .await
        .unwrap();

        // Allocated
        let m = rx.recv().await.unwrap();
        assert_matches!(
            m, RemoteProcessProcStateMessage::Allocated { alloc_key: got_alloc_key, world_id: got_world_id }
                if got_world_id == world_id && got_alloc_key == alloc_key
        );

        // All Created events
        let mut rank: usize = 0;
        let mut create_keys = Vec::with_capacity(extent.num_ranks());
        while rank < extent.num_ranks() {
            let m = rx.recv().await.unwrap();
            match m {
                RemoteProcessProcStateMessage::Update(
                    got_alloc_key,
                    ProcState::Created {
                        create_key, point, ..
                    },
                ) => {
                    let expected_point = extent.point_of_rank(rank).unwrap();
                    assert_eq!(got_alloc_key, alloc_key);
                    assert_eq!(point, expected_point);
                    create_keys.push(create_key);
                    rank += 1;
                }
                RemoteProcessProcStateMessage::HeartBeat => {}
                _ => panic!("unexpected message: {:?}", m),
            }
        }
        // All Running events
        let mut rank: usize = 0;
        while rank < extent.num_ranks() {
            let m = rx.recv().await.unwrap();
            match m {
                RemoteProcessProcStateMessage::Update(
                    got_alloc_key,
                    ProcState::Running {
                        create_key,
                        proc_id,
                        mesh_agent,
                        addr: _,
                    },
                ) => {
                    assert_eq!(got_alloc_key, alloc_key);
                    assert_eq!(create_key, create_keys[rank]);
                    let expected_proc_id = format!("test[{}]", rank).parse().unwrap();
                    let expected_mesh_agent = ActorRef::<ProcMeshAgent>::attest(
                        format!("test[{}].mesh_agent[{}]", rank, rank)
                            .parse()
                            .unwrap(),
                    );
                    assert_eq!(proc_id, expected_proc_id);
                    assert_eq!(mesh_agent, expected_mesh_agent);
                    rank += 1;
                }
                RemoteProcessProcStateMessage::HeartBeat => {}
                _ => panic!("unexpected message: {:?}", m),
            }
        }
        // All Stopped events
        let mut rank: usize = 0;
        while rank < extent.num_ranks() {
            let m = rx.recv().await.unwrap();
            match m {
                RemoteProcessProcStateMessage::Update(
                    got_alloc_key,
                    ProcState::Stopped {
                        create_key,
                        reason: ProcStopReason::Unknown,
                    },
                ) => {
                    assert_eq!(got_alloc_key, alloc_key);
                    assert_eq!(create_key, create_keys[rank]);
                    rank += 1;
                }
                RemoteProcessProcStateMessage::HeartBeat => {}
                _ => panic!("unexpected message: {:?}", m),
            }
        }
        // Done
        loop {
            let m = rx.recv().await.unwrap();
            match m {
                RemoteProcessProcStateMessage::Done(got_alloc_key) => {
                    assert_eq!(got_alloc_key, alloc_key);
                    break;
                }
                RemoteProcessProcStateMessage::HeartBeat => {}
                _ => panic!("unexpected message: {:?}", m),
            }
        }

        remote_allocator.terminate();
        handle.await.unwrap().unwrap();
    }

    #[timed_test::async_timed_test(timeout_secs = 15)]
    async fn test_normal_stop() {
        let config = hyperactor::config::global::lock();
        let _guard = config.override_key(
            hyperactor::config::REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL,
            Duration::from_millis(100),
        );
        hyperactor_telemetry::initialize_logging_for_test();
        let serve_addr = ChannelAddr::any(ChannelTransport::Unix);
        let bootstrap_addr = ChannelAddr::any(ChannelTransport::Unix);
        let (_, mut rx) = channel::serve(bootstrap_addr.clone(), "test".to_string()).unwrap();

        let extent = extent!(host = 1, gpu = 2);

        let tx = channel::dial(serve_addr.clone(), "test".to_string()).unwrap();

        let world_id: WorldId = id!(test_world_id);
        let mut alloc = MockAllocWrapper::new_block_next(
            MockAlloc::new(),
            // block after all created, all running
            extent.num_ranks() * 2,
        );
        let next_tx = alloc.notify_tx();
        alloc.alloc.expect_world_id().return_const(world_id.clone());
        alloc.alloc.expect_extent().return_const(extent.clone());

        set_procstate_expectations(&mut alloc.alloc, extent.clone());

        alloc.alloc.expect_next().return_const(None);
        alloc.alloc.expect_stop().times(1).return_once(|| Ok(()));

        let mut allocator = MockAllocator::new();
        allocator
            .expect_allocate()
            .times(1)
            .return_once(|_| Ok(alloc));

        let remote_allocator = RemoteProcessAllocator::new();
        let handle = tokio::spawn({
            let remote_allocator = remote_allocator.clone();
            async move {
                remote_allocator
                    .start_with_allocator(serve_addr, allocator, None)
                    .await
            }
        });

        let alloc_key = ShortUuid::generate();
        tx.send(RemoteProcessAllocatorMessage::Allocate {
            alloc_key: alloc_key.clone(),
            extent: extent.clone(),
            bootstrap_addr,
            hosts: vec![],
            client_context: None,
            forwarder_addr: AllocAssignedAddr::with_unspecified_port_or_any(&tx.addr()),
        })
        .await
        .unwrap();

        // Allocated
        let m = rx.recv().await.unwrap();
        assert_matches!(
            m,
            RemoteProcessProcStateMessage::Allocated {  world_id: got_world_id, alloc_key: got_alloc_key }
            if world_id == got_world_id && alloc_key == got_alloc_key
        );

        read_all_created(&mut rx, extent.num_ranks()).await;
        read_all_running(&mut rx, extent.num_ranks()).await;

        // allocation finished. now we stop it.
        tracing::info!("stopping allocation");
        tx.send(RemoteProcessAllocatorMessage::Stop).await.unwrap();
        // receive all stops
        next_tx.send(()).unwrap();

        read_all_stopped(&mut rx, extent.num_ranks()).await;

        remote_allocator.terminate();
        handle.await.unwrap().unwrap();
    }

    #[timed_test::async_timed_test(timeout_secs = 15)]
    async fn test_realloc() {
        let config = hyperactor::config::global::lock();
        let _guard = config.override_key(
            hyperactor::config::REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL,
            Duration::from_millis(100),
        );
        hyperactor_telemetry::initialize_logging_for_test();
        let serve_addr = ChannelAddr::any(ChannelTransport::Unix);
        let bootstrap_addr = ChannelAddr::any(ChannelTransport::Unix);
        let (_, mut rx) = channel::serve(bootstrap_addr.clone(), "test".to_string()).unwrap();

        let extent = extent!(host = 1, gpu = 2);

        let tx = channel::dial(serve_addr.clone(), "test".to_string()).unwrap();

        let world_id: WorldId = id!(test_world_id);
        let mut alloc1 = MockAllocWrapper::new_block_next(
            MockAlloc::new(),
            // block after all created, all running
            extent.num_ranks() * 2,
        );
        let next_tx1 = alloc1.notify_tx();
        alloc1
            .alloc
            .expect_world_id()
            .return_const(world_id.clone());
        alloc1.alloc.expect_extent().return_const(extent.clone());

        set_procstate_expectations(&mut alloc1.alloc, extent.clone());
        alloc1.alloc.expect_next().return_const(None);
        alloc1.alloc.expect_stop().times(1).return_once(|| Ok(()));
        // second allocation
        let mut alloc2 = MockAllocWrapper::new_block_next(
            MockAlloc::new(),
            // block after all created, all running
            extent.num_ranks() * 2,
        );
        let next_tx2 = alloc2.notify_tx();
        alloc2
            .alloc
            .expect_world_id()
            .return_const(world_id.clone());
        alloc2.alloc.expect_extent().return_const(extent.clone());
        set_procstate_expectations(&mut alloc2.alloc, extent.clone());
        alloc2.alloc.expect_next().return_const(None);
        alloc2.alloc.expect_stop().times(1).return_once(|| Ok(()));

        let mut allocator = MockAllocator::new();
        allocator
            .expect_allocate()
            .times(1)
            .return_once(|_| Ok(alloc1));
        // second alloc
        allocator
            .expect_allocate()
            .times(1)
            .return_once(|_| Ok(alloc2));

        let remote_allocator = RemoteProcessAllocator::new();
        let handle = tokio::spawn({
            let remote_allocator = remote_allocator.clone();
            async move {
                remote_allocator
                    .start_with_allocator(serve_addr, allocator, None)
                    .await
            }
        });

        let alloc_key = ShortUuid::generate();

        tx.send(RemoteProcessAllocatorMessage::Allocate {
            alloc_key: alloc_key.clone(),
            extent: extent.clone(),
            bootstrap_addr: bootstrap_addr.clone(),
            hosts: vec![],
            client_context: None,
            forwarder_addr: AllocAssignedAddr::with_unspecified_port_or_any(&tx.addr()),
        })
        .await
        .unwrap();

        // Allocated
        let m = rx.recv().await.unwrap();
        assert_matches!(
            m,
            RemoteProcessProcStateMessage::Allocated { world_id: got_world_id, alloc_key: got_alloc_key }
            if got_world_id == world_id && got_alloc_key == alloc_key
        );

        read_all_created(&mut rx, extent.num_ranks()).await;
        read_all_running(&mut rx, extent.num_ranks()).await;

        let alloc_key = ShortUuid::generate();

        // allocation finished now we request a new one
        tx.send(RemoteProcessAllocatorMessage::Allocate {
            alloc_key: alloc_key.clone(),
            extent: extent.clone(),
            bootstrap_addr,
            hosts: vec![],
            client_context: None,
            forwarder_addr: AllocAssignedAddr::with_unspecified_port_or_any(&tx.addr()),
        })
        .await
        .unwrap();
        // unblock next for the first allocation
        next_tx1.send(()).unwrap();
        // we expect a stop(), then Stopped proc states, then a new Allocated
        read_all_stopped(&mut rx, extent.num_ranks()).await;
        let m = rx.recv().await.unwrap();
        assert_matches!(m, RemoteProcessProcStateMessage::Done(_));
        let m = rx.recv().await.unwrap();
        assert_matches!(
            m,
            RemoteProcessProcStateMessage::Allocated { world_id: got_world_id, alloc_key: got_alloc_key }
            if got_world_id == world_id && got_alloc_key == alloc_key
        );
        // ProcStates for the new allocation
        read_all_created(&mut rx, extent.num_ranks()).await;
        read_all_running(&mut rx, extent.num_ranks()).await;
        // finally stop
        tracing::info!("stopping allocation");
        tx.send(RemoteProcessAllocatorMessage::Stop).await.unwrap();
        // receive all stops
        next_tx2.send(()).unwrap();

        read_all_stopped(&mut rx, extent.num_ranks()).await;

        remote_allocator.terminate();
        handle.await.unwrap().unwrap();
    }

    #[timed_test::async_timed_test(timeout_secs = 15)]
    async fn test_upstream_closed() {
        // Use temporary config for this test
        let config = hyperactor::config::global::lock();
        let _guard1 = config.override_key(
            hyperactor::config::MESSAGE_DELIVERY_TIMEOUT,
            Duration::from_secs(1),
        );
        let _guard2 = config.override_key(
            hyperactor::config::REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL,
            Duration::from_millis(100),
        );

        hyperactor_telemetry::initialize_logging_for_test();
        let serve_addr = ChannelAddr::any(ChannelTransport::Unix);
        let bootstrap_addr = ChannelAddr::any(ChannelTransport::Unix);
        let (_, mut rx) = channel::serve(bootstrap_addr.clone(), "test".to_string()).unwrap();

        let extent = extent!(host = 1, gpu = 2);

        let tx = channel::dial(serve_addr.clone(), "test".to_string()).unwrap();

        let world_id: WorldId = id!(test_world_id);
        let mut alloc = MockAllocWrapper::new_block_next(
            MockAlloc::new(),
            // block after all created, all running
            extent.num_ranks() * 2,
        );
        let next_tx = alloc.notify_tx();
        alloc.alloc.expect_world_id().return_const(world_id.clone());
        alloc.alloc.expect_extent().return_const(extent.clone());

        set_procstate_expectations(&mut alloc.alloc, extent.clone());

        alloc.alloc.expect_next().return_const(None);
        // we expect a stop due to the failure
        // synchronize test with the stop
        let (stop_tx, stop_rx) = oneshot::channel();
        alloc.alloc.expect_stop().times(1).return_once(|| {
            stop_tx.send(()).unwrap();
            Ok(())
        });

        let mut allocator = MockAllocator::new();
        allocator
            .expect_allocate()
            .times(1)
            .return_once(|_| Ok(alloc));

        let remote_allocator = RemoteProcessAllocator::new();
        let handle = tokio::spawn({
            let remote_allocator = remote_allocator.clone();
            async move {
                remote_allocator
                    .start_with_allocator(serve_addr, allocator, None)
                    .await
            }
        });

        let alloc_key = ShortUuid::generate();

        tx.send(RemoteProcessAllocatorMessage::Allocate {
            alloc_key: alloc_key.clone(),
            extent: extent.clone(),
            bootstrap_addr,
            hosts: vec![],
            client_context: None,
            forwarder_addr: AllocAssignedAddr::with_unspecified_port_or_any(&tx.addr()),
        })
        .await
        .unwrap();

        // Allocated
        let m = rx.recv().await.unwrap();
        assert_matches!(
            m, RemoteProcessProcStateMessage::Allocated { alloc_key: got_alloc_key, world_id: got_world_id }
                if got_world_id == world_id && got_alloc_key == alloc_key
        );

        read_all_created(&mut rx, extent.num_ranks()).await;
        read_all_running(&mut rx, extent.num_ranks()).await;

        // allocation finished. terminate connection.
        tracing::info!("closing upstream");
        drop(rx);
        // wait for the heartbeat to expire
        #[allow(clippy::disallowed_methods)]
        tokio::time::sleep(Duration::from_secs(2)).await;
        // wait for the stop to be called
        stop_rx.await.unwrap();
        // unblock next
        next_tx.send(()).unwrap();
        remote_allocator.terminate();
        handle.await.unwrap().unwrap();
    }

    #[timed_test::async_timed_test(timeout_secs = 15)]
    async fn test_inner_alloc_failure() {
        let config = hyperactor::config::global::lock();
        let _guard = config.override_key(
            hyperactor::config::REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL,
            Duration::from_secs(60),
        );
        hyperactor_telemetry::initialize_logging_for_test();
        let serve_addr = ChannelAddr::any(ChannelTransport::Unix);
        let bootstrap_addr = ChannelAddr::any(ChannelTransport::Unix);
        let (_, mut rx) = channel::serve(bootstrap_addr.clone(), "test".to_string()).unwrap();

        let extent = extent!(host = 1, gpu = 2);

        let tx = channel::dial(serve_addr.clone(), "test".to_string()).unwrap();

        let test_world_id: WorldId = id!(test_world_id);
        let mut alloc = MockAllocWrapper::new_block_next(
            MockAlloc::new(),
            // block after the failure update
            1,
        );
        let next_tx = alloc.notify_tx();
        alloc
            .alloc
            .expect_world_id()
            .return_const(test_world_id.clone());
        alloc.alloc.expect_extent().return_const(extent.clone());
        alloc
            .alloc
            .expect_next()
            .times(1)
            .return_const(Some(ProcState::Failed {
                world_id: test_world_id.clone(),
                description: "test".to_string(),
            }));
        alloc.alloc.expect_next().times(1).return_const(None);

        alloc.alloc.expect_stop().times(1).return_once(|| Ok(()));

        let mut allocator = MockAllocator::new();
        allocator
            .expect_allocate()
            .times(1)
            .return_once(|_| Ok(alloc));

        let remote_allocator = RemoteProcessAllocator::new();
        let handle = tokio::spawn({
            let remote_allocator = remote_allocator.clone();
            async move {
                remote_allocator
                    .start_with_allocator(serve_addr, allocator, None)
                    .await
            }
        });

        let alloc_key = ShortUuid::generate();
        tx.send(RemoteProcessAllocatorMessage::Allocate {
            alloc_key: alloc_key.clone(),
            extent: extent.clone(),
            bootstrap_addr,
            hosts: vec![],
            client_context: None,
            forwarder_addr: AllocAssignedAddr::with_unspecified_port_or_any(&tx.addr()),
        })
        .await
        .unwrap();

        // Allocated
        let m = rx.recv().await.unwrap();
        assert_matches!(
            m,
            RemoteProcessProcStateMessage::Allocated {  world_id: got_world_id, alloc_key: got_alloc_key }
            if test_world_id == got_world_id && alloc_key == got_alloc_key
        );

        // Failed
        let m = rx.recv().await.unwrap();
        assert_matches!(
            m,
            RemoteProcessProcStateMessage::Update(
                got_alloc_key,
                ProcState::Failed { world_id, description }
            ) if got_alloc_key == alloc_key && world_id == test_world_id && description == "test"
        );

        tracing::info!("stopping allocation");
        tx.send(RemoteProcessAllocatorMessage::Stop).await.unwrap();
        // receive all stops
        next_tx.send(()).unwrap();
        // we are expecting 1 Done when Alloc successfully stops.
        let m = rx.recv().await.unwrap();
        assert_matches!(
            m,
            RemoteProcessProcStateMessage::Done(got_alloc_key)
            if got_alloc_key == alloc_key
        );

        remote_allocator.terminate();
        handle.await.unwrap().unwrap();
    }

    #[timed_test::async_timed_test(timeout_secs = 15)]
    async fn test_trace_id_propagation() {
        let config = hyperactor::config::global::lock();
        let _guard = config.override_key(
            hyperactor::config::REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL,
            Duration::from_secs(60),
        );
        hyperactor_telemetry::initialize_logging(ClockKind::default());
        let serve_addr = ChannelAddr::any(ChannelTransport::Unix);
        let bootstrap_addr = ChannelAddr::any(ChannelTransport::Unix);
        let (_, mut rx) = channel::serve(bootstrap_addr.clone(), "test".to_string()).unwrap();

        let extent = extent!(host = 1, gpu = 1);
        let tx = channel::dial(serve_addr.clone(), "test".to_string()).unwrap();
        let test_world_id: WorldId = id!(test_world_id);
        let test_trace_id = "test_trace_id_12345";

        // Create a mock alloc that we can verify receives the correct trace id
        let mut alloc = MockAlloc::new();
        alloc.expect_world_id().return_const(test_world_id.clone());
        alloc.expect_extent().return_const(extent.clone());
        alloc.expect_next().return_const(None);

        // Create a mock allocator that captures the AllocSpec passed to it
        let mut allocator = MockAllocator::new();
        allocator
            .expect_allocate()
            .times(1)
            .withf(move |spec: &AllocSpec| {
                // Verify that the trace id is correctly set in the constraints
                spec.constraints
                    .match_labels
                    .get(CLIENT_TRACE_ID_LABEL)
                    .is_some_and(|trace_id| trace_id == test_trace_id)
            })
            .return_once(|_| Ok(MockAllocWrapper::new(alloc)));

        let remote_allocator = RemoteProcessAllocator::new();
        let handle = tokio::spawn({
            let remote_allocator = remote_allocator.clone();
            async move {
                remote_allocator
                    .start_with_allocator(serve_addr, allocator, None)
                    .await
            }
        });

        let alloc_key = ShortUuid::generate();
        tx.send(RemoteProcessAllocatorMessage::Allocate {
            alloc_key: alloc_key.clone(),
            extent: extent.clone(),
            bootstrap_addr,
            hosts: vec![],
            client_context: Some(ClientContext {
                trace_id: test_trace_id.to_string(),
            }),
            forwarder_addr: AllocAssignedAddr::with_unspecified_port_or_any(&tx.addr()),
        })
        .await
        .unwrap();

        // Verify we get the allocated message
        let m = rx.recv().await.unwrap();
        assert_matches!(
            m,
            RemoteProcessProcStateMessage::Allocated { alloc_key: got_alloc_key, world_id: got_world_id }
            if got_world_id == test_world_id && got_alloc_key == alloc_key
        );

        // Verify we get the done message since the mock alloc returns None immediately
        let m = rx.recv().await.unwrap();
        assert_matches!(
            m,
            RemoteProcessProcStateMessage::Done(got_alloc_key)
            if alloc_key == got_alloc_key
        );

        remote_allocator.terminate();
        handle.await.unwrap().unwrap();
    }

    #[timed_test::async_timed_test(timeout_secs = 15)]
    async fn test_trace_id_propagation_no_client_context() {
        let config = hyperactor::config::global::lock();
        let _guard = config.override_key(
            hyperactor::config::REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL,
            Duration::from_secs(60),
        );
        hyperactor_telemetry::initialize_logging(ClockKind::default());
        let serve_addr = ChannelAddr::any(ChannelTransport::Unix);
        let bootstrap_addr = ChannelAddr::any(ChannelTransport::Unix);
        let (_, mut rx) = channel::serve(bootstrap_addr.clone(), "test".to_string()).unwrap();

        let extent = extent!(host = 1, gpu = 1);
        let tx = channel::dial(serve_addr.clone(), "test".to_string()).unwrap();
        let test_world_id: WorldId = id!(test_world_id);

        // Create a mock alloc
        let mut alloc = MockAlloc::new();
        alloc.expect_world_id().return_const(test_world_id.clone());
        alloc.expect_extent().return_const(extent.clone());
        alloc.expect_next().return_const(None);

        // Create a mock allocator that verifies no trace id is set when client_context is None
        let mut allocator = MockAllocator::new();
        allocator
            .expect_allocate()
            .times(1)
            .withf(move |spec: &AllocSpec| {
                // Verify that no trace id is set in the constraints when client_context is None
                spec.constraints.match_labels.is_empty()
            })
            .return_once(|_| Ok(MockAllocWrapper::new(alloc)));

        let remote_allocator = RemoteProcessAllocator::new();
        let handle = tokio::spawn({
            let remote_allocator = remote_allocator.clone();
            async move {
                remote_allocator
                    .start_with_allocator(serve_addr, allocator, None)
                    .await
            }
        });

        let alloc_key = ShortUuid::generate();
        tx.send(RemoteProcessAllocatorMessage::Allocate {
            alloc_key: alloc_key.clone(),
            extent: extent.clone(),
            bootstrap_addr,
            hosts: vec![],
            client_context: None,
            forwarder_addr: AllocAssignedAddr::with_unspecified_port_or_any(&tx.addr()),
        })
        .await
        .unwrap();

        // Verify we get the allocated message
        let m = rx.recv().await.unwrap();
        assert_matches!(
            m,
            RemoteProcessProcStateMessage::Allocated { alloc_key: got_alloc_key, world_id: got_world_id }
            if got_world_id == test_world_id && got_alloc_key == alloc_key
        );

        // Verify we get the done message since the mock alloc returns None immediately
        let m = rx.recv().await.unwrap();
        assert_matches!(
            m,
            RemoteProcessProcStateMessage::Done(got_alloc_key)
            if got_alloc_key == alloc_key
        );

        remote_allocator.terminate();
        handle.await.unwrap().unwrap();
    }
}

#[cfg(test)]
mod test_alloc {
    use std::os::unix::process::ExitStatusExt;

    use hyperactor::clock::ClockKind;
    use hyperactor::config;
    use ndslice::extent;
    use nix::sys::signal;
    use nix::unistd::Pid;
    use timed_test::async_timed_test;

    use super::*;

    #[async_timed_test(timeout_secs = 60)]
    async fn test_alloc_simple() {
        // Use temporary config for this test
        let config = hyperactor::config::global::lock();
        let _guard1 = config.override_key(
            hyperactor::config::MESSAGE_DELIVERY_TIMEOUT,
            Duration::from_secs(1),
        );
        let _guard2 = config.override_key(
            hyperactor::config::REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL,
            Duration::from_millis(100),
        );
        hyperactor_telemetry::initialize_logging(ClockKind::default());

        let spec = AllocSpec {
            extent: extent!(host = 2, gpu = 2),
            constraints: Default::default(),
            proc_name: None,
            transport: ChannelTransport::Unix,
        };
        let world_id = WorldId("test_world_id".to_string());

        let task1_allocator = RemoteProcessAllocator::new();
        let task1_addr = ChannelAddr::any(ChannelTransport::Unix);
        let task1_addr_string = task1_addr.to_string();
        let task1_cmd = Command::new(crate::testresource::get(
            "monarch/hyperactor_mesh/bootstrap",
        ));
        let task2_allocator = RemoteProcessAllocator::new();
        let task2_addr = ChannelAddr::any(ChannelTransport::Unix);
        let task2_addr_string = task2_addr.to_string();
        let task2_cmd = Command::new(crate::testresource::get(
            "monarch/hyperactor_mesh/bootstrap",
        ));
        let task1_allocator_copy = task1_allocator.clone();
        let task1_allocator_handle = tokio::spawn(async move {
            tracing::info!("spawning task1");
            task1_allocator_copy
                .start(task1_cmd, task1_addr, None)
                .await
                .unwrap();
        });
        let task2_allocator_copy = task2_allocator.clone();
        let task2_allocator_handle = tokio::spawn(async move {
            task2_allocator_copy
                .start(task2_cmd, task2_addr, None)
                .await
                .unwrap();
        });

        let mut initializer = MockRemoteProcessAllocInitializer::new();
        initializer.expect_initialize_alloc().return_once(move || {
            Ok(vec![
                RemoteProcessAllocHost {
                    hostname: task1_addr_string,
                    id: "task1".to_string(),
                },
                RemoteProcessAllocHost {
                    hostname: task2_addr_string,
                    id: "task2".to_string(),
                },
            ])
        });
        let mut alloc = RemoteProcessAlloc::new(spec.clone(), world_id, 0, initializer)
            .await
            .unwrap();
        let mut created = HashSet::new();
        let mut running_procs = HashSet::new();
        let mut proc_points = HashSet::new();
        for _ in 0..spec.extent.num_ranks() * 2 {
            let proc_state = alloc.next().await.unwrap();
            tracing::debug!("test got message: {:?}", proc_state);
            match proc_state {
                ProcState::Created {
                    create_key, point, ..
                } => {
                    created.insert(create_key);
                    proc_points.insert(point);
                }
                ProcState::Running { create_key, .. } => {
                    assert!(created.remove(&create_key));
                    running_procs.insert(create_key);
                }
                _ => panic!("expected Created or Running"),
            }
        }
        assert!(created.is_empty());
        // ensure coords coverage
        assert!(
            spec.extent
                .points()
                .all(|point| proc_points.contains(&point))
        );

        // ensure no more pending items
        let timeout = hyperactor::clock::RealClock.now() + std::time::Duration::from_millis(1000);
        tokio::select! {
            _ = hyperactor::clock::RealClock.sleep_until(timeout) => {},
            _ = alloc.next() => panic!("expected no more items"),
        }

        // stop the allocation
        alloc.stop().await.unwrap();
        for _ in 0..spec.extent.num_ranks() {
            let proc_state = alloc.next().await.unwrap();
            tracing::info!("test received next proc_state: {:?}", proc_state);
            match proc_state {
                ProcState::Stopped {
                    create_key, reason, ..
                } => {
                    assert!(running_procs.remove(&create_key));
                    assert_eq!(reason, ProcStopReason::Stopped);
                }
                _ => panic!("expected stopped"),
            }
        }
        // Exactly one None
        let proc_state = alloc.next().await;
        assert!(proc_state.is_none());
        // Anything afterwards is None
        let proc_state = alloc.next().await;
        assert!(proc_state.is_none());

        task1_allocator.terminate();
        task1_allocator_handle.await.unwrap();
        task2_allocator.terminate();
        task2_allocator_handle.await.unwrap();
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn test_alloc_host_failure() {
        // Use temporary config for this test
        let config = hyperactor::config::global::lock();
        let _guard1 = config.override_key(
            hyperactor::config::MESSAGE_DELIVERY_TIMEOUT,
            Duration::from_secs(1),
        );
        let _guard2 = config.override_key(
            hyperactor::config::REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL,
            Duration::from_millis(100),
        );
        hyperactor_telemetry::initialize_logging(ClockKind::default());

        let spec = AllocSpec {
            extent: extent!(host = 2, gpu = 2),
            constraints: Default::default(),
            proc_name: None,
            transport: ChannelTransport::Unix,
        };
        let world_id = WorldId("test_world_id".to_string());

        let task1_allocator = RemoteProcessAllocator::new();
        let task1_addr = ChannelAddr::any(ChannelTransport::Unix);
        let task1_addr_string = task1_addr.to_string();
        let task1_cmd = Command::new(crate::testresource::get(
            "monarch/hyperactor_mesh/bootstrap",
        ));
        let task2_allocator = RemoteProcessAllocator::new();
        let task2_addr = ChannelAddr::any(ChannelTransport::Unix);
        let task2_addr_string = task2_addr.to_string();
        let task2_cmd = Command::new(crate::testresource::get(
            "monarch/hyperactor_mesh/bootstrap",
        ));
        let task1_allocator_copy = task1_allocator.clone();
        let task1_allocator_handle = tokio::spawn(async move {
            tracing::info!("spawning task1");
            task1_allocator_copy
                .start(task1_cmd, task1_addr, None)
                .await
                .unwrap();
            tracing::info!("task1 terminated");
        });
        let task2_allocator_copy = task2_allocator.clone();
        let task2_allocator_handle = tokio::spawn(async move {
            task2_allocator_copy
                .start(task2_cmd, task2_addr, None)
                .await
                .unwrap();
            tracing::info!("task2 terminated");
        });

        let mut initializer = MockRemoteProcessAllocInitializer::new();
        initializer.expect_initialize_alloc().return_once(move || {
            Ok(vec![
                RemoteProcessAllocHost {
                    hostname: task1_addr_string,
                    id: "task1".to_string(),
                },
                RemoteProcessAllocHost {
                    hostname: task2_addr_string,
                    id: "task2".to_string(),
                },
            ])
        });
        let mut alloc = RemoteProcessAlloc::new(spec.clone(), world_id, 0, initializer)
            .await
            .unwrap();
        for _ in 0..spec.extent.num_ranks() * 2 {
            match alloc.next().await {
                Some(ProcState::Created { .. }) | Some(ProcState::Running { .. }) => {}
                _ => panic!("expected Created or Running"),
            }
        }

        // ensure no more pending items
        let timeout = hyperactor::clock::RealClock.now() + std::time::Duration::from_millis(1000);
        tokio::select! {
            _ = hyperactor::clock::RealClock
            .sleep_until(timeout) => {},
            _ = alloc.next() => panic!("expected no more items"),
        }

        // now we kill task1 and wait for timeout
        tracing::info!("aborting task1 allocator");
        task1_allocator_handle.abort();
        RealClock
            .sleep(config::global::get(config::REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL) * 2)
            .await;
        for _ in 0..spec.extent.num_ranks() / 2 {
            let proc_state = alloc.next().await.unwrap();
            tracing::info!("test received next proc_state: {:?}", proc_state);
            match proc_state {
                ProcState::Stopped { reason, .. } => {
                    assert_eq!(reason, ProcStopReason::HostWatchdog);
                }
                _ => panic!("expected stopped"),
            }
        }
        // no more events
        let timeout = hyperactor::clock::RealClock.now() + std::time::Duration::from_millis(1000);
        tokio::select! {
            _ = hyperactor::clock::RealClock
            .sleep_until(timeout) => {},
            _ = alloc.next() => panic!("expected no more items"),
        }

        // abort the second host
        tracing::info!("aborting task2 allocator");
        task2_allocator_handle.abort();
        RealClock
            .sleep(config::global::get(config::REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL) * 2)
            .await;
        for _ in 0..spec.extent.num_ranks() / 2 {
            let proc_state = alloc.next().await.unwrap();
            tracing::info!("test received next proc_state: {:?}", proc_state);
            match proc_state {
                ProcState::Stopped { reason, .. } => {
                    assert_eq!(reason, ProcStopReason::HostWatchdog);
                }
                _ => panic!("expected stopped"),
            }
        }
        // now the alloc has stopped we always get None
        let proc_state = alloc.next().await;
        assert!(proc_state.is_none());
        // Anything afterwards is None
        let proc_state = alloc.next().await;
        assert!(proc_state.is_none());
    }

    #[async_timed_test(timeout_secs = 15)]
    async fn test_alloc_inner_alloc_failure() {
        // SAFETY: Test happens in single-threaded code.
        unsafe {
            std::env::set_var("MONARCH_MESSAGE_DELIVERY_TIMEOUT_SECS", "1");
        }

        let config = hyperactor::config::global::lock();
        let _guard = config.override_key(
            hyperactor::config::REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL,
            Duration::from_millis(100),
        );
        hyperactor_telemetry::initialize_logging_for_test();

        let spec = AllocSpec {
            extent: extent!(host = 2, gpu = 2),
            constraints: Default::default(),
            proc_name: None,
            transport: ChannelTransport::Unix,
        };
        let world_id = WorldId("test_world_id".to_string());

        let task1_allocator = RemoteProcessAllocator::new();
        let task1_addr = ChannelAddr::any(ChannelTransport::Unix);
        let task1_addr_string = task1_addr.to_string();
        let task1_cmd = Command::new(crate::testresource::get(
            "monarch/hyperactor_mesh/bootstrap",
        ));
        let task2_allocator = RemoteProcessAllocator::new();
        let task2_addr = ChannelAddr::any(ChannelTransport::Unix);
        let task2_addr_string = task2_addr.to_string();
        // non-existent binary to fail the allocation
        let task2_cmd = Command::new("/caught/somewhere/in/time");
        let task1_allocator_copy = task1_allocator.clone();
        let task1_allocator_handle = tokio::spawn(async move {
            tracing::info!("spawning task1");
            task1_allocator_copy
                .start(task1_cmd, task1_addr, None)
                .await
                .unwrap();
        });
        let task2_allocator_copy = task2_allocator.clone();
        let task2_allocator_handle = tokio::spawn(async move {
            task2_allocator_copy
                .start(task2_cmd, task2_addr, None)
                .await
                .unwrap();
        });

        let mut initializer = MockRemoteProcessAllocInitializer::new();
        initializer.expect_initialize_alloc().return_once(move || {
            Ok(vec![
                RemoteProcessAllocHost {
                    hostname: task1_addr_string,
                    id: "task1".to_string(),
                },
                RemoteProcessAllocHost {
                    hostname: task2_addr_string,
                    id: "task2".to_string(),
                },
            ])
        });
        let mut alloc = RemoteProcessAlloc::new(spec.clone(), world_id, 0, initializer)
            .await
            .unwrap();
        let mut created = HashSet::new();
        let mut started_procs = HashSet::new();
        let mut proc_points = HashSet::new();
        let mut failed = 0;
        // task 1 procs + 1 failed event for task 2
        for _ in 0..spec.extent.num_ranks() + 1 {
            let proc_state = alloc.next().await.unwrap();
            tracing::debug!("test got message: {:?}", proc_state);
            match proc_state {
                ProcState::Created {
                    create_key, point, ..
                } => {
                    created.insert(create_key);
                    proc_points.insert(point);
                }
                ProcState::Running { create_key, .. } => {
                    assert!(created.remove(&create_key));
                    started_procs.insert(create_key);
                }
                ProcState::Failed { .. } => {
                    failed += 1;
                }
                _ => panic!("expected Created, Running or Failed"),
            }
        }
        assert!(created.is_empty());
        assert_eq!(failed, 1);
        // ensure coords coverage for task 1
        for rank in 0..spec.extent.num_ranks() / 2 {
            let point = spec.extent.point_of_rank(rank).unwrap();
            assert!(proc_points.contains(&point));
        }

        // ensure no more pending items
        let timeout = hyperactor::clock::RealClock.now() + std::time::Duration::from_millis(1000);
        tokio::select! {
            _ = hyperactor::clock::RealClock
            .sleep_until(timeout) => {},
            _ = alloc.next() => panic!("expected no more items"),
        }

        // stop the allocation
        alloc.stop().await.unwrap();
        for _ in 0..spec.extent.num_ranks() / 2 {
            let proc_state = alloc.next().await.unwrap();
            tracing::info!("test received next proc_state: {:?}", proc_state);
            match proc_state {
                ProcState::Stopped {
                    create_key, reason, ..
                } => {
                    assert!(started_procs.remove(&create_key));
                    assert_eq!(reason, ProcStopReason::Stopped);
                }
                _ => panic!("expected stopped"),
            }
        }
        // Exactly one None
        let proc_state = alloc.next().await;
        assert!(proc_state.is_none());
        // Anything afterwards is None
        let proc_state = alloc.next().await;
        assert!(proc_state.is_none());

        task1_allocator.terminate();
        task1_allocator_handle.await.unwrap();
        task2_allocator.terminate();
        task2_allocator_handle.await.unwrap();
    }

    #[tracing_test::traced_test]
    #[async_timed_test(timeout_secs = 60)]
    async fn test_remote_process_alloc_signal_handler() {
        let num_proc_meshes = 5;
        let hosts_per_proc_mesh = 5;

        let pid_addr = ChannelAddr::any(ChannelTransport::Unix);
        let (pid_addr, mut pid_rx) = channel::serve::<u32>(pid_addr, "test".to_string()).unwrap();

        let addresses = (0..(num_proc_meshes * hosts_per_proc_mesh))
            .map(|_| ChannelAddr::any(ChannelTransport::Unix).to_string())
            .collect::<Vec<_>>();

        let remote_process_allocators = addresses
            .iter()
            .map(|addr| {
                Command::new(crate::testresource::get(
                    "monarch/hyperactor_mesh/remote_process_allocator",
                ))
                .env("RUST_LOG", "info")
                .arg(format!("--addr={addr}"))
                .stdout(std::process::Stdio::piped())
                .spawn()
                .unwrap()
            })
            .collect::<Vec<_>>();

        let done_allocating_addr = ChannelAddr::any(ChannelTransport::Unix);
        let (done_allocating_addr, mut done_allocating_rx) =
            channel::serve::<()>(done_allocating_addr, "test".to_string()).unwrap();
        let mut remote_process_alloc = Command::new(crate::testresource::get(
            "monarch/hyperactor_mesh/remote_process_alloc",
        ))
        .arg(format!("--done-allocating-addr={}", done_allocating_addr))
        .arg(format!("--addresses={}", addresses.join(",")))
        .arg(format!("--num-proc-meshes={}", num_proc_meshes))
        .arg(format!("--hosts-per-proc-mesh={}", hosts_per_proc_mesh))
        .arg(format!("--pid-addr={}", pid_addr))
        .spawn()
        .unwrap();

        done_allocating_rx.recv().await.unwrap();
        let mut received_pids = Vec::new();
        while let Ok(pid) = pid_rx.recv().await {
            received_pids.push(pid);
            if received_pids.len() == remote_process_allocators.len() {
                break;
            }
        }

        signal::kill(
            Pid::from_raw(remote_process_alloc.id().unwrap() as i32),
            signal::SIGINT,
        )
        .unwrap();

        assert_eq!(
            remote_process_alloc.wait().await.unwrap().signal(),
            Some(signal::SIGINT as i32)
        );

        RealClock.sleep(tokio::time::Duration::from_secs(5)).await;

        // Assert that the processes spawned by ProcessAllocator have been killed
        for child_pid in received_pids {
            let pid_check = Command::new("kill")
                .arg("-0")
                .arg(child_pid.to_string())
                .output()
                .await
                .expect("Failed to check if PID is alive");

            assert!(
                !pid_check.status.success(),
                "PID {} should no longer be alive",
                child_pid
            );
        }

        // Cleanup remote process allocator processes as SIGINT causes the current
        // allocs to stop but not the RemoteProcessAllocator loops
        for mut remote_process_allocator in remote_process_allocators {
            remote_process_allocator.kill().await.unwrap();
        }
    }
}
