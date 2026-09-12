/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use chrysalis_core::LINK_ID_LEN;
use chrysalis_core::LinkContext;
use chrysalis_core::LinkId;
use chrysalis_core::LinkSide;
use chrysalis_core::Pid;
use chrysalis_nameserver::ParentManagerStatus;
use chrysalis_transport::DatagramAddr;
use chrysalis_transport::IncomingStream;
use chrysalis_transport::LinkLocalProtocol;
use chrysalis_transport::LinkLocalProtocolId;
use chrysalis_transport::Stream;
use chrysalis_transport::SwitchSocket;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::sync::Notify;
use tokio::sync::watch;
use tokio::task::AbortHandle;
use tokio::task::JoinHandle;
use tokio::task::JoinSet;

const SESSION_ACK: u8 = 1;

pub(crate) type HandlerFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
pub(crate) type Handler = Arc<dyn Fn(LinkContext, Stream) -> HandlerFuture + Send + Sync>;

pub(crate) struct Registration {
    pub(crate) id: LinkLocalProtocolId,
    pub(crate) handler: Handler,
}

pub(crate) struct ParentTarget {
    pub(crate) retry_delay: Duration,
    pub(crate) status: watch::Receiver<ParentManagerStatus>,
}

pub(crate) struct LinkProtocolManager {
    completion: Arc<Completion>,
}

impl LinkProtocolManager {
    /// Starts one session supervisor for every registered link-local protocol.
    ///
    /// Each supervisor reconciles its outgoing session with `parent`: it opens a stream after the
    /// nameserver link becomes active, identifies that link with its parent-allocated [`LinkId`],
    /// restarts an accepted protocol session while the same parent link remains active, and aborts
    /// the session when the parent disconnects or reconnects under a new link ID. Incoming streams
    /// carry the same link ID; the supervisor dispatches them only when `(peer, link)` appears in
    /// the current `children` snapshot and aborts their handlers when that child link disappears.
    pub(crate) fn spawn(
        registrations: Vec<(Registration, LinkLocalProtocol<SwitchSocket>)>,
        parent: Option<ParentTarget>,
        children: watch::Receiver<BTreeMap<Pid, LinkId>>,
    ) -> Self {
        let completion = Arc::new(Completion::default());
        let task_completion = completion.clone();
        tokio::spawn(async move {
            let _guard = CompletionGuard(task_completion.clone());
            let mut protocols = JoinSet::new();
            for (registration, protocol) in registrations {
                protocols.spawn(run_protocol(
                    registration,
                    protocol,
                    parent.as_ref().map(|parent| ParentTarget {
                        retry_delay: parent.retry_delay,
                        status: parent.status.clone(),
                    }),
                    children.clone(),
                    task_completion.clone(),
                ));
            }
            tokio::select! {
                () = task_completion.cancelled() => {}
                _ = protocols.join_next(), if !protocols.is_empty() => {
                    task_completion.shutdown();
                }
            }
            protocols.abort_all();
            while protocols.join_next().await.is_some() {}
        });
        Self { completion }
    }

    pub(crate) fn shutdown(&self) {
        self.completion.shutdown();
    }

    pub(crate) async fn join(&self) {
        self.completion.join().await;
    }
}

impl Drop for LinkProtocolManager {
    fn drop(&mut self) {
        self.completion.shutdown();
    }
}

async fn run_protocol(
    registration: Registration,
    protocol: LinkLocalProtocol<SwitchSocket>,
    mut parent: Option<ParentTarget>,
    mut children: watch::Receiver<BTreeMap<Pid, LinkId>>,
    completion: Arc<Completion>,
) {
    let mut parent_attempted = None;
    let mut parent_session = None;
    let mut classifiers = JoinSet::new();
    let mut child_sessions = JoinSet::new();
    let mut active_children = HashMap::new();

    reconcile_parent(
        &registration,
        &protocol,
        parent.as_ref(),
        &mut parent_attempted,
        &mut parent_session,
    )
    .await;

    loop {
        tokio::select! {
            biased;
            () = completion.cancelled() => break,
            incoming = protocol.accept() => {
                let Ok(incoming) = incoming else {
                    break;
                };
                classifiers.spawn(classify(incoming));
            }
            classified = classifiers.join_next(), if !classifiers.is_empty() => {
                let Some(Ok(Some((peer, link, mut stream)))) = classified else {
                    continue;
                };
                let admitted = children.borrow().get(&peer).copied() == Some(link);
                if !admitted || active_children.contains_key(&link) {
                    continue;
                }
                if stream.send_mut().write_all(&[SESSION_ACK]).await.is_err() {
                    continue;
                }
                let handler = registration.handler.clone();
                let abort = child_sessions.spawn(async move {
                    handler(LinkContext::new(link, peer, LinkSide::Child), stream).await;
                    link
                });
                active_children.insert(link, abort);
            }
            completed = child_sessions.join_next(), if !child_sessions.is_empty() => {
                if let Some(Ok(link)) = completed {
                    active_children.remove(&link);
                }
                active_children.retain(|_, abort| !abort.is_finished());
            }
            changed = children.changed() => {
                if changed.is_err() {
                    break;
                }
                fence_removed_children(&children, &mut active_children);
            }
            changed = parent_changed(&mut parent) => {
                if changed.is_err() {
                    break;
                }
                reconcile_parent(
                    &registration,
                    &protocol,
                    parent.as_ref(),
                    &mut parent_attempted,
                    &mut parent_session,
                ).await;
            }
        }
    }

    if let Some((_, task)) = parent_session.take() {
        task.abort();
        let _ = task.await;
    }
    for (_, abort) in active_children.drain() {
        abort.abort();
    }
    child_sessions.abort_all();
    classifiers.abort_all();
    while child_sessions.join_next().await.is_some() {}
    while classifiers.join_next().await.is_some() {}
}

async fn parent_changed(parent: &mut Option<ParentTarget>) -> Result<(), watch::error::RecvError> {
    match parent {
        Some(parent) => parent.status.changed().await,
        None => std::future::pending().await,
    }
}

async fn reconcile_parent(
    registration: &Registration,
    protocol: &LinkLocalProtocol<SwitchSocket>,
    parent: Option<&ParentTarget>,
    attempted: &mut Option<LinkId>,
    session: &mut Option<(LinkId, JoinHandle<()>)>,
) {
    let desired = parent.and_then(|parent| match parent.status.borrow().clone() {
        ParentManagerStatus::Connected {
            peer,
            address,
            link,
        } => Some((peer, parent.retry_delay, address, link)),
        ParentManagerStatus::Connecting
        | ParentManagerStatus::Failed { .. }
        | ParentManagerStatus::Stopped => None,
    });
    let desired_link = desired.as_ref().map(|(_, _, _, link)| *link);
    if session.as_ref().map(|(link, _)| *link) != desired_link
        && let Some((_, task)) = session.take()
    {
        task.abort();
        let _ = task.await;
    }
    if attempted.is_some() && *attempted != desired_link {
        *attempted = None;
    }
    let Some((peer, retry_delay, address, link)) = desired else {
        return;
    };
    if *attempted == Some(link) {
        return;
    }
    *attempted = Some(link);
    let protocol = protocol.clone();
    let handler = registration.handler.clone();
    let task = tokio::spawn(async move {
        run_parent_session(protocol, handler, peer, address, link, retry_delay).await;
    });
    *session = Some((link, task));
}

async fn run_parent_session(
    protocol: LinkLocalProtocol<SwitchSocket>,
    handler: Handler,
    peer: Pid,
    address: DatagramAddr,
    link: LinkId,
    retry_delay: Duration,
) {
    let mut accepted = false;
    loop {
        let Ok(mut stream) = protocol.dial(peer, address.clone()).await else {
            tokio::time::sleep(retry_delay).await;
            continue;
        };
        if stream.send_mut().write_all(link.as_bytes()).await.is_err() {
            tokio::time::sleep(retry_delay).await;
            continue;
        }
        let mut ack = [0];
        if stream.recv_mut().read_exact(&mut ack).await.is_err() || ack[0] != SESSION_ACK {
            if !accepted {
                return;
            }
            tokio::time::sleep(retry_delay).await;
            continue;
        }
        accepted = true;
        handler(LinkContext::new(link, peer, LinkSide::Parent), stream).await;
        tokio::time::sleep(retry_delay).await;
    }
}

async fn classify(mut incoming: IncomingStream) -> Option<(Pid, LinkId, Stream)> {
    let peer = incoming.source();
    let mut bytes = [0; LINK_ID_LEN];
    incoming
        .stream_mut()
        .recv_mut()
        .read_exact(&mut bytes)
        .await
        .ok()?;
    let (_, stream) = incoming.into_parts();
    Some((peer, LinkId::from_bytes(bytes), stream))
}

fn fence_removed_children(
    children: &watch::Receiver<BTreeMap<Pid, LinkId>>,
    active: &mut HashMap<LinkId, AbortHandle>,
) {
    let links = children.borrow();
    active.retain(|link, abort| {
        if links.values().any(|active| active == link) {
            true
        } else {
            abort.abort();
            false
        }
    });
}

#[derive(Debug, Default)]
struct Completion {
    shutdown: AtomicBool,
    terminated: AtomicBool,
    shutdown_notify: Notify,
    terminated_notify: Notify,
}

impl Completion {
    fn shutdown(&self) {
        if !self.shutdown.swap(true, Ordering::AcqRel) {
            self.shutdown_notify.notify_waiters();
        }
    }

    async fn cancelled(&self) {
        loop {
            let notified = self.shutdown_notify.notified();
            if self.shutdown.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    fn terminate(&self) {
        if !self.terminated.swap(true, Ordering::AcqRel) {
            self.shutdown();
            self.terminated_notify.notify_waiters();
        }
    }

    async fn join(&self) {
        loop {
            let notified = self.terminated_notify.notified();
            if self.terminated.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

struct CompletionGuard(Arc<Completion>);

impl Drop for CompletionGuard {
    fn drop(&mut self) {
        self.0.terminate();
    }
}
