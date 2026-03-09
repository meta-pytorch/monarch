/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This module defines a proc allocator interface as well as a multi-process
//! (local) allocator, [`ProcessAllocator`].

// EnumAsInner generates code that triggers a false positive
// unused_assignments lint on struct variant fields. #[allow] on the
// enum itself doesn't propagate into derive-macro-generated code, so
// the suppression must be at module scope.
#![allow(unused_assignments)]

pub mod local;
pub mod process;
pub mod remoteprocess;

use std::collections::HashMap;
use std::fmt;

use async_trait::async_trait;
use enum_as_inner::EnumAsInner;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::ChannelTransport;
use hyperactor::channel::TlsAddr;
use hyperactor::reference as hyperactor_reference;
pub use local::LocalAlloc;
pub use local::LocalAllocator;
use mockall::predicate::*;
use mockall::*;
use ndslice::Shape;
use ndslice::Slice;
use ndslice::view::Extent;
use ndslice::view::Point;
pub use process::ProcessAlloc;
pub use process::ProcessAllocator;
use serde::Deserialize;
use serde::Serialize;
use strum::AsRefStr;
use typeuri::Named;

use crate::alloc::test_utils::MockAllocWrapper;
use crate::assign::Ranks;
use crate::host_mesh::host_agent::HostAgent;
use crate::shortuuid::ShortUuid;

/// A name uniquely identifying an allocation.
#[derive(
    Debug,
    Serialize,
    Deserialize,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Hash,
    Ord
)]
pub struct AllocName(pub String);

impl AllocName {
    /// The alloc name as a string slice.
    pub fn name(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AllocName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Errors that occur during allocation operations.
#[derive(Debug, thiserror::Error)]
pub enum AllocatorError {
    #[error("incomplete allocation; expected: {0}")]
    Incomplete(Extent),

    /// The requested shape is too large for the allocator.
    #[error("not enough resources; requested: {requested}, available: {available}")]
    NotEnoughResources { requested: Extent, available: usize },

    /// An uncategorized error from an underlying system.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Constraints on the allocation.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AllocConstraints {
    /// Aribitrary name/value pairs that are interpreted by individual
    /// allocators to control allocation process.
    pub match_labels: HashMap<String, String>,
}

/// Specifies how to interpret the extent dimensions for allocation.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[derive(Default)]
pub enum ProcAllocationMode {
    /// Proc-level allocation: splits extent to allocate multiple processes per host.
    /// Requires at least 2 dimensions (e.g., [hosts: N, gpus: M]).
    /// Splits by second-to-last dimension, creating N regions with M processes each.
    /// Used by MastAllocator.
    #[default]
    ProcLevel,
    /// Host-level allocation: each point in the extent is a host (no sub-host splitting).
    /// For extent!(region = 2, host = 4), create 8 regions, each representing 1 host.
    /// Used by MastHostAllocator.
    HostLevel,
}

/// A specification (desired state) of an alloc.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocSpec {
    /// The requested extent of the alloc.
    // We currently assume that this shape is dense.
    // This should be validated, or even enforced by
    // way of types.
    pub extent: Extent,

    /// Constraints on the allocation.
    pub constraints: AllocConstraints,

    /// If specified, return procs using direct addressing with
    /// the provided proc name.
    pub proc_name: Option<String>,

    /// The transport to use for the procs in this alloc.
    pub transport: ChannelTransport,

    /// Specifies how to interpret the extent dimensions for allocation.
    /// Defaults to ProcLevel for backward compatibility.
    #[serde(default = "default_proc_allocation_mode")]
    pub proc_allocation_mode: ProcAllocationMode,
}

fn default_proc_allocation_mode() -> ProcAllocationMode {
    ProcAllocationMode::ProcLevel
}

/// The core allocator trait, implemented by all allocators.
#[automock(type Alloc=MockAllocWrapper;)]
#[async_trait]
pub trait Allocator {
    /// The type of [`Alloc`] produced by this allocator.
    type Alloc: Alloc;

    /// Create a new allocation. The allocation itself is generally
    /// returned immediately (after validating parameters, etc.);
    /// the caller is expected to respond to allocation events as
    /// the underlying procs are incrementally allocated.
    async fn allocate(&mut self, spec: AllocSpec) -> Result<Self::Alloc, AllocatorError>;
}

/// A proc's status. A proc can only monotonically move from
/// `Created` to `Running` to `Stopped`.
#[derive(
    Clone,
    Debug,
    PartialEq,
    EnumAsInner,
    Serialize,
    Deserialize,
    AsRefStr,
    Named
)]
pub enum ProcState {
    /// A proc was added to the alloc.
    Created {
        /// A key to uniquely identify a created proc. The key is used again
        /// to identify the created proc as Running.
        create_key: ShortUuid,
        /// Its assigned point (in the alloc's extent).
        point: Point,
        /// The system process ID of the created child process.
        pid: u32,
    },
    /// A host was started.
    Running {
        /// The key used to identify the created proc/host.
        create_key: ShortUuid,
        /// The address the host is serving on.
        host_addr: ChannelAddr,
        /// Reference to this host's mesh agent.
        host_agent: hyperactor_reference::ActorRef<HostAgent>,
    },
    /// A proc was stopped.
    Stopped {
        create_key: ShortUuid,
        reason: ProcStopReason,
    },
    /// Allocation process encountered an irrecoverable error. Depending on the
    /// implementation, the allocation process may continue transiently and calls
    /// to next() may return some events. But eventually the allocation will not
    /// be complete. Callers can use the `description` to determine the reason for
    /// the failure.
    /// Allocation can then be cleaned up by calling `stop()`` on the `Alloc` and
    /// drain the iterator for clean shutdown.
    Failed {
        /// The name of the failed alloc.
        alloc_name: AllocName,
        /// A description of the failure.
        description: String,
    },
}

impl fmt::Display for ProcState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProcState::Created {
                create_key,
                point,
                pid,
            } => {
                write!(f, "{}: created at ({}) with PID {}", create_key, point, pid)
            }
            ProcState::Running {
                host_addr,
                host_agent,
                ..
            } => {
                write!(f, "{}: running at {}", host_agent.actor_id(), host_addr)
            }
            ProcState::Stopped { create_key, reason } => {
                write!(f, "{}: stopped: {}", create_key, reason)
            }
            ProcState::Failed {
                description,
                alloc_name,
            } => {
                write!(f, "{}: failed: {}", alloc_name, description)
            }
        }
    }
}

/// The reason a proc stopped.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, EnumAsInner)]
pub enum ProcStopReason {
    /// The proc stopped gracefully, e.g., with exit code 0.
    Stopped,
    /// The proc exited with the provided error code and stderr
    Exited(i32, String),
    /// The proc was killed. The signal number is indicated;
    /// the flags determines whether there was a core dump.
    Killed(i32, bool),
    /// The proc failed to respond to a watchdog request within a timeout.
    Watchdog,
    /// The host running the proc failed to respond to a watchdog request
    /// within a timeout.
    HostWatchdog,
    /// The proc failed for an unknown reason.
    Unknown,
}

impl fmt::Display for ProcStopReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stopped => write!(f, "stopped"),
            Self::Exited(code, stderr) => {
                if stderr.is_empty() {
                    write!(f, "exited with code {}", code)
                } else {
                    write!(f, "exited with code {}: {}", code, stderr)
                }
            }
            Self::Killed(signal, dumped) => {
                write!(f, "killed with signal {} (core dumped={})", signal, dumped)
            }
            Self::Watchdog => write!(f, "proc watchdog failure"),
            Self::HostWatchdog => write!(f, "host watchdog failure"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

/// An alloc is a specific allocation, returned by an [`Allocator`].
#[automock]
#[async_trait]
pub trait Alloc {
    /// Return the next proc event. `None` indicates that there are
    /// no more events, and that the alloc is stopped.
    async fn next(&mut self) -> Option<ProcState>;

    /// The spec against which this alloc is executing.
    fn spec(&self) -> &AllocSpec;

    /// The shape of the alloc.
    fn extent(&self) -> &Extent;

    /// The shape of the alloc. (Deprecated.)
    fn shape(&self) -> Shape {
        let slice = Slice::new_row_major(self.extent().sizes());
        Shape::new(self.extent().labels().to_vec(), slice).unwrap()
    }

    /// The name of this alloc, uniquely identifying it.
    fn alloc_name(&self) -> &AllocName;

    /// The channel transport used the procs in this alloc.
    fn transport(&self) -> ChannelTransport {
        self.spec().transport.clone()
    }

    /// Stop this alloc, shutting down all of its procs. A clean
    /// shutdown should result in Stop events from all allocs,
    /// followed by the end of the event stream.
    async fn stop(&mut self) -> Result<(), AllocatorError>;

    /// Stop this alloc and wait for all procs to stop. Call will
    /// block until all ProcState events have been drained.
    async fn stop_and_wait(&mut self) -> Result<(), AllocatorError> {
        tracing::error!(
            name = "AllocStatus",
            alloc_name = %self.alloc_name(),
            status = "StopAndWait",
        );
        self.stop().await?;
        while let Some(event) = self.next().await {
            tracing::debug!(
                alloc_name = %self.alloc_name(),
                "drained event: {event:?}"
            );
        }
        tracing::error!(
            name = "AllocStatus",
            alloc_name = %self.alloc_name(),
            status = "Stopped",
        );
        Ok(())
    }

    /// Returns whether the alloc is a local alloc: that is, its procs are
    /// not independent processes, but just threads in the selfsame process.
    fn is_local(&self) -> bool {
        false
    }

    /// The address that should be used to serve the client's router.
    fn client_router_addr(&self) -> ChannelAddr {
        ChannelAddr::any(self.transport())
    }
}

/// Allocated host information returned by [`AllocHostExt::initialize_hosts`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct AllocatedHost {
    pub create_key: ShortUuid,
    pub host_addr: ChannelAddr,
    pub host_agent: hyperactor_reference::ActorRef<HostAgent>,
}

/// Extension trait that drives an [`Alloc`] to collect host information.
#[async_trait]
pub(crate) trait AllocHostExt {
    /// Drive the alloc through Created → Running states, collecting
    /// host addresses and agent refs.
    async fn initialize_hosts(&mut self) -> Result<Vec<AllocatedHost>, AllocatorError>;
}

#[async_trait]
impl<A: ?Sized + Send + Alloc> AllocHostExt for A {
    async fn initialize_hosts(&mut self) -> Result<Vec<AllocatedHost>, AllocatorError> {
        let shape = self.shape().clone();

        let mut created = Ranks::new(shape.slice().len());
        let mut running = Ranks::new(shape.slice().len());

        while !running.is_full() {
            let Some(state) = self.next().await else {
                return Err(AllocatorError::Incomplete(self.extent().clone()));
            };

            let name = tracing::Span::current()
                .metadata()
                .map(|m| m.name())
                .unwrap_or("initialize_hosts");
            let status = format!("ProcState:{}", state.arm().unwrap_or("unknown"));

            match state {
                ProcState::Created {
                    create_key, point, ..
                } => {
                    let rank = point.rank();
                    if let Some(old_create_key) = created.insert(rank, create_key.clone()) {
                        tracing::warn!(
                            name,
                            status,
                            rank,
                            "rank {rank} reassigned from {old_create_key} to {create_key}"
                        );
                    }
                    tracing::info!(
                        name,
                        status,
                        rank,
                        "host with create key {}, rank {}: created",
                        create_key,
                        rank
                    );
                }
                ProcState::Running {
                    create_key,
                    host_addr,
                    host_agent,
                } => {
                    let Some(rank) = created.rank(&create_key) else {
                        tracing::warn!(
                            name,
                            status,
                            "host with create key {create_key} \
                            is running, but was not created"
                        );
                        continue;
                    };

                    let allocated = AllocatedHost {
                        create_key,
                        host_addr: host_addr.clone(),
                        host_agent: host_agent.clone(),
                    };
                    if running.insert(*rank, allocated).is_some() {
                        tracing::warn!(
                            name,
                            status,
                            rank,
                            "duplicate running notifications for rank {rank}"
                        )
                    }
                    tracing::info!(name, status, "host rank {}: running at {host_addr}", rank);
                }
                ProcState::Stopped { create_key, reason } => {
                    tracing::error!(
                        name,
                        status,
                        "allocation failed for host with create key {}: {}",
                        create_key,
                        reason
                    );
                    return Err(AllocatorError::Other(anyhow::Error::msg(reason)));
                }
                ProcState::Failed {
                    alloc_name,
                    description,
                } => {
                    tracing::error!(
                        name,
                        status,
                        "allocation failed for {}: {}",
                        alloc_name,
                        description
                    );
                    return Err(AllocatorError::Other(anyhow::Error::msg(description)));
                }
            }
        }

        Ok(running.into_iter().map(Option::unwrap).collect())
    }
}

/// If addr is Tcp or Metatls, use its IP address or hostname to create
/// a new addr with port unspecified.
///
/// for other types of addr, return "any" address.
pub(crate) fn with_unspecified_port_or_any(addr: &ChannelAddr) -> ChannelAddr {
    match addr {
        ChannelAddr::Tcp(socket) => {
            let mut new_socket = socket.clone();
            new_socket.set_port(0);
            ChannelAddr::Tcp(new_socket)
        }
        ChannelAddr::MetaTls(TlsAddr { hostname, .. }) => {
            ChannelAddr::MetaTls(TlsAddr::new(hostname.clone(), 0))
        }
        _ => addr.transport().any(),
    }
}

pub mod test_utils {
    use std::time::Duration;

    use hyperactor::Actor;
    use hyperactor::Context;
    use hyperactor::Handler;
    use libc::atexit;
    use tokio::sync::broadcast::Receiver;
    use tokio::sync::broadcast::Sender;
    use typeuri::Named;

    use super::*;

    extern "C" fn exit_handler() {
        loop {
            #[allow(clippy::disallowed_methods)]
            std::thread::sleep(Duration::from_mins(1));
        }
    }

    // This can't be defined under a `#[cfg(test)]` because there needs to
    // be an entry in the spawnable actor registry in the executable
    // 'hyperactor_mesh_test_bootstrap' for the `tests::process` actor
    // mesh test suite.
    #[derive(Debug, Default)]
    #[hyperactor::export(
        spawn = true,
        handlers = [
            Wait
        ],
    )]
    pub struct TestActor;

    impl Actor for TestActor {}

    #[derive(Debug, Serialize, Deserialize, Named, Clone)]
    pub struct Wait;

    #[async_trait]
    impl Handler<Wait> for TestActor {
        async fn handle(&mut self, _: &Context<Self>, _: Wait) -> Result<(), anyhow::Error> {
            // SAFETY:
            // This is in order to simulate a process in tests that never exits.
            unsafe {
                atexit(exit_handler);
            }
            Ok(())
        }
    }

    /// Test wrapper around MockAlloc to allow us to block next() calls since
    /// mockall doesn't support returning futures.
    pub struct MockAllocWrapper {
        pub alloc: MockAlloc,
        pub block_next_after: usize,
        notify_tx: Sender<()>,
        notify_rx: Receiver<()>,
        next_unblocked: bool,
    }

    impl MockAllocWrapper {
        pub fn new(alloc: MockAlloc) -> Self {
            Self::new_block_next(alloc, usize::MAX)
        }

        pub fn new_block_next(alloc: MockAlloc, count: usize) -> Self {
            let (tx, rx) = tokio::sync::broadcast::channel(1);
            Self {
                alloc,
                block_next_after: count,
                notify_tx: tx,
                notify_rx: rx,
                next_unblocked: false,
            }
        }

        pub fn notify_tx(&self) -> Sender<()> {
            self.notify_tx.clone()
        }
    }

    #[async_trait]
    impl Alloc for MockAllocWrapper {
        async fn next(&mut self) -> Option<ProcState> {
            match self.block_next_after {
                0 => {
                    if !self.next_unblocked {
                        self.notify_rx.recv().await.unwrap();
                        self.next_unblocked = true;
                    }
                }
                1.. => {
                    self.block_next_after -= 1;
                }
            }

            self.alloc.next().await
        }

        fn spec(&self) -> &AllocSpec {
            self.alloc.spec()
        }

        fn extent(&self) -> &Extent {
            self.alloc.extent()
        }

        fn alloc_name(&self) -> &AllocName {
            self.alloc.alloc_name()
        }

        async fn stop(&mut self) -> Result<(), AllocatorError> {
            self.alloc.stop().await
        }
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use core::panic;
    use std::collections::HashMap;
    use std::collections::HashSet;

    use ndslice::extent;

    use super::*;
    use crate::transport::default_transport;

    #[macro_export]
    macro_rules! alloc_test_suite {
        ($allocator:expr) => {
            #[tokio::test]
            async fn test_allocator_basic() {
                $crate::alloc::testing::test_allocator_basic($allocator).await;
            }
        };
    }

    pub(crate) async fn test_allocator_basic(mut allocator: impl Allocator) {
        let extent = extent!(replica = 4);
        let mut alloc = allocator
            .allocate(AllocSpec {
                extent: extent.clone(),
                constraints: Default::default(),
                proc_name: None,
                transport: default_transport(),
                proc_allocation_mode: Default::default(),
            })
            .await
            .unwrap();

        // Get everything up into running state.
        let mut created = HashMap::new();
        let mut running = HashSet::new();
        while running.len() != 4 {
            match alloc.next().await.unwrap() {
                ProcState::Created {
                    create_key, point, ..
                } => {
                    created.insert(create_key, point);
                }
                ProcState::Running { create_key, .. } => {
                    assert!(running.insert(create_key.clone()));
                    assert!(created.contains_key(&create_key));
                }
                event => panic!("unexpected event: {:?}", event),
            }
        }

        // We should have complete coverage of all points.
        let points: HashSet<_> = created.values().collect();
        for x in 0..4 {
            assert!(points.contains(&extent.point(vec![x]).unwrap()));
        }

        // Now, stop the alloc and make sure it shuts down cleanly.
        alloc.stop().await.unwrap();
        let mut stopped = HashSet::new();
        while let Some(ProcState::Stopped {
            create_key, reason, ..
        }) = alloc.next().await
        {
            assert_eq!(reason, ProcStopReason::Stopped);
            stopped.insert(create_key);
        }
        assert!(alloc.next().await.is_none());
        assert_eq!(stopped, running);
    }
}
