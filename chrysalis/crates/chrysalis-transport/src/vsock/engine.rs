/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::os::fd::OwnedFd;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use tokio::io::unix::AsyncFd;
use tokio::net::UnixListener;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

use self::accept::accept_native;
use self::accept::accept_unix;
use self::dial::Dial;
use super::address::ParsedDestination;
use super::address::VsockDatagramAddr;
use super::frame::FrameKind;
use super::frame::encode_frame;
use super::lock_or_recover;
use crate::DatagramAddr;
use crate::shutdown::ShutdownState;

mod accept;
mod connection;
mod dial;

/// Maximum datagrams that may wait to be sent on one connection, and maximum
/// received datagrams that may wait for the application to read them.
pub(super) const QUEUE_CAPACITY: usize = 1_024;

/// Internal engine owned by [`VsockDatagramSocket`] and shared with its tasks.
///
/// ```text
/// VsockDatagramSocket
/// └── inner: Arc<VsockDatagramEngine>  public socket's strong reference
///     ├── listener tasks               each holds an Arc clone
///     ├── dial tasks                   each holds an Arc clone
///     ├── connection tasks             each holds an Arc clone
///     └── shutdown supervisor          holds an Arc clone
/// ```
///
/// Tasks retain only `VsockDatagramEngine`; they never retain `VsockDatagramSocket`.
/// Dropping the caller's socket therefore runs its `Drop` implementation and
/// requests shutdown, while this engine remains alive until the tasks finish.
///
/// Every connection reader sends decoded datagrams through the same
/// `receive_tx`. The logical socket has one `receive_rx`, which
/// `poll_recv_from` drains.
///
/// Received datagrams therefore flow through one shared channel:
///
/// ```text
/// connection read tasks
///          │ send
///          ▼
///      receive_tx ─── shared queue ───► receive_rx
///                                        │ poll
///                                        ▼
///                                 poll_recv_from
/// ```
///
/// [`VsockDatagramSocket`]: super::VsockDatagramSocket
#[derive(Debug)]
pub(super) struct VsockDatagramEngine {
    /// The socket's logical `node:<name>` address.
    pub(super) local_addr: DatagramAddr,

    /// The HELLO frame written when this engine opens a connection.
    hello: Bytes,

    /// Tracks running, shutting-down, and terminated state and wakes waiters.
    pub(super) shutdown: ShutdownState,

    /// Established connections and dials in progress, keyed by peer address.
    pub(super) connections: Mutex<HashMap<DatagramAddr, ConnectionHandle>>,

    /// Allocates a distinct ID for each connection attempt.
    next_id: AtomicU64,

    /// Sending half of the engine's shared receive queue.
    receive_tx: mpsc::Sender<(DatagramAddr, Bytes)>,

    /// Receiving half of the same queue, polled by `poll_recv_from`.
    pub(super) receive_rx: Mutex<mpsc::Receiver<(DatagramAddr, Bytes)>>,

    /// Number of engine-owned tasks that have not ended.
    pub(super) tasks: AtomicUsize,

    /// Wakes [`supervise`] when the engine-owned task count reaches zero.
    tasks_finished: Notify,

    /// Unix socket files removed during shutdown.
    pub(super) listener_paths: Vec<PathBuf>,

    /// Number of connections accepted by this engine.
    #[cfg(test)]
    pub(super) accepted: AtomicUsize,

    /// Signals that a reader has started waiting for receive-queue space.
    #[cfg(test)]
    pub(super) backpressure: Arc<Notify>,
}

/// Handle for the current connection or connection attempt to one peer.
#[derive(Debug)]
pub(super) struct ConnectionHandle {
    /// Distinguishes this connection from older connections using the same
    /// address, so stale cleanup cannot remove its replacement.
    id: u64,

    /// Sends encoded datagram frames to this connection's dial or writer task.
    send_tx: mpsc::Sender<Bytes>,

    /// Stops this connection's dial, reader, and writer tasks.
    cancel: Arc<Cancel>,
}

/// Resources transferred to the reader and writer tasks for one established
/// local stream endpoint.
struct ConnectionResources<Reader, Writer> {
    /// Address used as the connection-map key and source of received datagrams.
    label: DatagramAddr,

    /// Identifies which connection incarnation owns the map entry.
    id: u64,

    /// Stops both tasks and coordinates connection replacement.
    cancel: Arc<Cancel>,

    /// Read half of this engine's local stream endpoint.
    read: Reader,

    /// Write half of this engine's local stream endpoint.
    write: Writer,

    /// Receiving half of this connection's send queue. `try_send_to` places
    /// frames into the matching `ConnectionHandle::send_tx`.
    send_rx: mpsc::Receiver<Bytes>,
}

/// Additional cancellation applied to one engine-owned task.
///
/// Every task stops for socket shutdown. Connection tasks also stop when their
/// connection is cancelled.
#[derive(Debug)]
pub(super) enum TaskCancellation {
    /// Engine shutdown is the task's only cancellation source.
    ShutdownOnly,

    /// The task also stops when this connection is cancelled.
    Connection(Arc<Cancel>),
}

impl VsockDatagramEngine {
    /// Initializes shared queues, lifecycle tracking, and cleanup metadata
    /// before any engine-owned task starts.
    pub(super) fn new(local_addr: DatagramAddr, listener_paths: Vec<PathBuf>) -> Self {
        let hello = encode_frame(FrameKind::Hello, local_addr.opaque());

        // One bounded queue merges datagrams received from every connection.
        let (receive_tx, receive_rx) = mpsc::channel(QUEUE_CAPACITY);

        Self {
            local_addr,
            hello,
            shutdown: ShutdownState::default(),
            connections: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(0),
            receive_tx,
            receive_rx: Mutex::new(receive_rx),
            tasks: AtomicUsize::new(0),
            tasks_finished: Notify::new(),
            listener_paths,
            #[cfg(test)]
            accepted: AtomicUsize::new(0),
            #[cfg(test)]
            backpressure: Arc::new(Notify::new()),
        }
    }

    /// Starts one engine-owned accept task per listener, then starts the
    /// shutdown supervisor.
    pub(super) fn start_tasks(
        self: &Arc<Self>,
        unix_listeners: Vec<UnixListener>,
        native_listeners: Vec<AsyncFd<OwnedFd>>,
    ) {
        for listener in unix_listeners {
            self.spawn(
                TaskCancellation::ShutdownOnly,
                accept_unix(self.clone(), listener),
            );
        }
        for listener in native_listeners {
            self.spawn(
                TaskCancellation::ShutdownOnly,
                accept_native(self.clone(), listener),
            );
        }

        // The supervisor is deliberately not counted as an engine-owned task:
        // it waits for that count to reach zero. Start it only after every
        // accept task has been counted, so it cannot observe a false zero.
        tokio::spawn(supervise(self.clone()));
    }

    /// Allocates an ID for one connection attempt.
    ///
    /// The counter needs atomic uniqueness but does not publish or order any
    /// other state, so relaxed ordering is sufficient.
    fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Sends `frame` through the connection assigned to `destination`.
    ///
    /// If a connection or dial already exists, the frame is queued there. A
    /// full queue drops the frame and still succeeds. A closed connection is
    /// removed.
    ///
    /// If no connection exists, a physical `Native`, `Hybrid`, or `Unix`
    /// address reserves one map entry and starts one dial. A logical `Node`
    /// address cannot be dialed and returns `HostUnreachable`.
    ///
    /// The connection map remains locked until a new dial has reserved its
    /// entry, preventing concurrent sends from starting duplicate dials.
    pub(super) fn queue_or_dial(
        self: &Arc<Self>,
        destination: ParsedDestination<'_>,
        mut frame: Bytes,
    ) -> io::Result<()> {
        let mut connections = lock_or_recover(&self.connections);

        // Reuse an established connection or an in-progress dial.
        if let Some(connection) = connections.get(destination.datagram_addr) {
            match connection.send_tx.try_send(frame) {
                // The connection's writer will send this frame.
                Ok(()) => return Ok(()),
                // Match UDP-style overload behavior: drop the datagram but
                // report success.
                Err(TrySendError::Full(_dropped_frame)) => return Ok(()),
                // The stored connection is dead. Remove it, recover the unsent
                // frame, and continue below to start a new dial for the same
                // datagram.
                Err(TrySendError::Closed(unsent_frame)) => {
                    connections.remove(destination.datagram_addr);
                    frame = unsent_frame;
                }
            }
        }

        // `node:<name>` is usable only when a peer's HELLO has installed a live
        // connection under that name. The lookup above found none, and the
        // name contains no physical address to dial.
        if matches!(&destination.stream_addr, VsockDatagramAddr::Node(_)) {
            return Err(io::Error::new(
                io::ErrorKind::HostUnreachable,
                "no live connection announced this vsock name",
            ));
        }

        // Reserve the destination before releasing the lock, so another sender
        // sees this dial instead of starting a second one.
        let (send_tx, send_rx) = mpsc::channel(QUEUE_CAPACITY);
        send_tx
            .try_send(frame)
            .expect("should fit: the queue is new and has capacity");
        let id = self.next_id();
        let cancel = Arc::new(Cancel::default());
        connections.insert(
            destination.datagram_addr.clone(),
            ConnectionHandle {
                id,
                send_tx,
                cancel: cancel.clone(),
            },
        );
        drop(connections);

        // The destination is reserved and the map is unlocked. If connection
        // setup fails or is cancelled, dropping `Dial` removes that exact
        // reservation and drops the datagrams queued behind it.
        let dial = Dial::new(self.clone(), destination.datagram_addr.clone(), id);
        // `Dial::run` creates the asynchronous connection attempt:
        // `stream_addr` selects how to connect, `cancel` continues into the
        // established connection's reader and writer, and `send_rx` holds
        // datagrams queued during setup. Successful setup connects the stream,
        // sends HELLO, starts those reader and writer tasks, hands the queue to
        // the writer, and transfers cleanup ownership to the new tasks.
        // `spawn` polls until setup returns, engine shutdown begins, or the same
        // cancellation signal fires. If setup never transfers ownership,
        // dropping the attempt removes its reserved map entry and discards its
        // queued datagrams.
        self.spawn(
            TaskCancellation::Connection(cancel.clone()),
            dial.run(destination.stream_addr, cancel, send_rx),
        );
        Ok(())
    }

    /// Runs one engine-owned future under shutdown and task-count tracking.
    ///
    /// The future is counted before shutdown is checked, preventing the
    /// shutdown supervisor from observing zero tasks while this future is being
    /// admitted. If shutdown has already started, the future is dropped
    /// immediately while it is still counted.
    ///
    /// Otherwise, a Tokio task polls the future until it completes or one of
    /// the sources named by `cancellation` fires. The future—and therefore
    /// every resource it owns—is dropped before the task count is decremented.
    /// This guarantees that
    /// [`VsockDatagramSocket::join`] cannot return while those resources are still
    /// alive.
    ///
    /// [`VsockDatagramSocket::join`]: super::VsockDatagramSocket::join
    pub(super) fn spawn<F>(self: &Arc<Self>, cancellation: TaskCancellation, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // Count the task before checking shutdown so the supervisor cannot
        // observe zero tasks while this task is being admitted.
        self.tasks.fetch_add(1, Ordering::AcqRel);

        // Dropping this guard decrements the count and wakes the supervisor
        // when it removes the final task.
        let guard = TaskGuard(self.clone());
        if !self.shutdown.is_running() {
            // Shutdown began after this task was counted but before it started.
            // Drop the task and everything it owns before dropping the guard
            // that removes it from the count.
            drop(task);
            drop(guard);
            return;
        }

        // Tokio owns both the task future and its counting guard from here.
        tokio::spawn(async move {
            let engine = guard.0.clone();
            match cancellation {
                TaskCancellation::ShutdownOnly => {
                    tokio::select! {
                        () = engine.shutdown.cancelled() => {}
                        () = task => {}
                    }
                }
                TaskCancellation::Connection(cancel) => {
                    tokio::select! {
                        () = engine.shutdown.cancelled() => {}
                        () = cancel.cancelled() => {}
                        () = task => {}
                    }
                }
            }
            // `select!` has dropped the task future and its resources. Only
            // now remove it from the task count.
            drop(guard);
        });
    }

    /// Removes `label` only when it still belongs to connection `id`.
    ///
    /// A newer connection may replace an older one under the same label before
    /// the older tasks finish. Their later cleanup must not remove the newer
    /// connection, so the stored connection ID must still match.
    fn remove_if_owned(&self, label: &DatagramAddr, id: u64) {
        let mut connections = lock_or_recover(&self.connections);
        if connections
            .get(label)
            .is_some_and(|connection| connection.id == id)
        {
            connections.remove(label);
        }
    }

    /// Places one received datagram in the engine's shared receive queue.
    ///
    /// If the queue has space, this returns immediately. If it is full, the
    /// connection reader waits for the application to remove a datagram rather
    /// than dropping the new one. Returns `false` when shutdown has closed the
    /// queue before the datagram could be accepted.
    async fn deliver(&self, source: DatagramAddr, payload: Bytes) -> bool {
        match self.receive_tx.try_send((source, payload)) {
            Ok(()) => true,
            Err(TrySendError::Full(datagram)) => {
                // Apply backpressure: preserve the datagram and wait for queue
                // space.
                #[cfg(test)]
                self.backpressure.notify_one();
                self.receive_tx.send(datagram).await.is_ok()
            }
            // Shutdown closed the queue; the caller should end its reader task.
            Err(TrySendError::Closed(_)) => false,
        }
    }
}

/// Tracks one engine-owned task until that task's future is dropped.
///
/// Every spawned task owns one `TaskGuard`. Dropping the guard decrements the
/// live-task count. If it removes the final task, it wakes the shutdown
/// supervisor so `join` can complete.
struct TaskGuard(Arc<VsockDatagramEngine>);

impl Drop for TaskGuard {
    /// Removes this task from the live-task count.
    ///
    /// `fetch_sub` returns the previous count, so `1` means this guard removed
    /// the final task. The supervisor is then woken to finish shutdown.
    fn drop(&mut self) {
        if self.0.tasks.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.tasks_finished.notify_waiters();
        }
    }
}

/// Shared one-way cancellation signal for one connection's tasks.
///
/// The first call to `cancel` records cancellation permanently and wakes every
/// current waiter. Tasks that begin waiting later observe the recorded flag and
/// return immediately.
#[derive(Debug, Default)]
pub(super) struct Cancel {
    /// Persistent cancellation state, also making `cancel` idempotent.
    cancelled: AtomicBool,

    /// Wakes tasks that are already waiting when cancellation is recorded.
    notify: Notify,
}

impl Cancel {
    /// Records cancellation and wakes current waiters exactly once.
    ///
    /// `swap` returns the previous value. Only the first `false → true`
    /// transition sends a notification; later calls are no-ops.
    fn cancel(&self) {
        if !self.cancelled.swap(true, Ordering::AcqRel) {
            self.notify.notify_waiters();
        }
    }

    /// Waits until cancellation has been recorded.
    ///
    /// The notification future is created before reading the flag, so
    /// cancellation cannot occur between those operations and be missed. The
    /// loop rechecks the persistent flag after every wake.
    async fn cancelled(&self) {
        loop {
            let notified = self.notify.notified();
            if self.cancelled.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

/// Coordinates the final phase of engine shutdown.
///
/// This supervisor is deliberately not included in the engine-owned task count.
/// It first waits for shutdown to begin, then waits until every counted task has
/// released its resources. It drops the remaining connection handles and marks
/// shutdown terminated, which wakes every [`VsockDatagramSocket::join`] caller.
///
/// [`VsockDatagramSocket::join`]: super::VsockDatagramSocket::join
async fn supervise(engine: Arc<VsockDatagramEngine>) {
    // Listener, dial, reader, and writer tasks continue normally until shutdown
    // is requested.
    engine.shutdown.cancelled().await;

    loop {
        // Register before reading the count so the final task cannot reach zero
        // between the check and the wait without waking this supervisor.
        let tasks_finished = engine.tasks_finished.notified();
        if engine.tasks.load(Ordering::Acquire) == 0 {
            break;
        }
        tasks_finished.await;
    }

    // No engine-owned task can use these handles now. Dropping them closes any
    // remaining send queues and cancellation signals.
    lock_or_recover(&engine.connections).clear();

    // Record completion and wake every caller waiting in `join`.
    engine.shutdown.terminate();
}
