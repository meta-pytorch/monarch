/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Actor liveness monitoring.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_trait::async_trait;
use derivative::Derivative;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::watch;
use tokio::time;
use typeuri::Named;

use crate::Actor;
use crate::ActorAddr;
use crate::ActorHandle;
use crate::ActorRef;
use crate::Context;
use crate::Endpoint;
use crate::Handler;
use crate::Instance;
use crate::Message;
use crate::OncePortRef;
use crate::PortRef;
use crate::RemoteMessage;
use crate::StatusMessage;
use crate::actor::ActorStatus;
use crate::actor::Referable;
use crate::context;
use crate::mailbox::MailboxError;
use crate::mailbox::OncePortHandle;
use crate::mailbox::PortHandle;
use crate::mailbox::PortReceiver;
use crate::supervision::local_fence;

const DEFAULT_INITIAL_DELAY: Duration = Duration::from_secs(2);
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// The current state of an actor monitor.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Named)]
pub enum MonitorStatus {
    /// The monitor has not completed its first status request.
    Checking,
    /// The monitored actor responded with a non-terminal status.
    Alive(ActorStatus),
    /// The monitor detected a terminal condition.
    Failed(MonitorFailure),
}
wirevalue::register_type!(MonitorStatus);

/// A failure detected by an actor monitor.
#[derive(
    thiserror::Error,
    Debug,
    Clone,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    Named
)]
pub enum MonitorFailure {
    /// The monitored actor does not exist in the actor runtime.
    #[error("monitored actor {actor_id} is gone")]
    ActorGone {
        /// The monitored actor.
        actor_id: ActorAddr,
    },
    /// The monitored actor stopped normally.
    #[error("monitored actor {actor_id} stopped: {status}")]
    ActorStopped {
        /// The monitored actor.
        actor_id: ActorAddr,
        /// The actor's terminal status.
        status: ActorStatus,
    },
    /// The monitored actor failed.
    #[error("monitored actor {actor_id} failed: {status}")]
    ActorFailed {
        /// The monitored actor.
        actor_id: ActorAddr,
        /// The actor's terminal status.
        status: ActorStatus,
    },
    /// The status request did not complete before the monitor timeout.
    #[error("status request to monitored actor {actor_id} timed out after {timeout_millis}ms")]
    StatusRequestTimedOut {
        /// The monitored actor.
        actor_id: ActorAddr,
        /// The timeout, in milliseconds.
        timeout_millis: u64,
    },
    /// The status reply port closed before a reply arrived.
    #[error("status reply from monitored actor {actor_id} closed")]
    StatusReplyClosed {
        /// The monitored actor.
        actor_id: ActorAddr,
    },
    /// The monitor actor stopped before reporting a monitored failure.
    #[error("monitor for actor {actor_id} stopped before reporting a failure")]
    MonitorStopped {
        /// The monitored actor.
        actor_id: ActorAddr,
    },
}
wirevalue::register_type!(MonitorFailure);

/// Structured metadata for synthetic supervision events.
#[derive(
    thiserror::Error,
    Debug,
    Clone,
    Serialize,
    Deserialize,
    Derivative,
    Named
)]
#[derivative(PartialEq, Eq)]
#[error("synthetic supervision event for {subject}: {failure}")]
pub struct SyntheticSupervision {
    /// The actor whose liveness failure caused the synthetic event.
    pub subject: ActorAddr,
    /// The monitor failure that caused the event.
    pub failure: Box<MonitorFailure>,
    #[serde(skip, default = "local_fence")]
    #[derivative(PartialEq = "ignore")]
    pub(crate) local_fence: Arc<AtomicBool>,
}
wirevalue::register_type!(SyntheticSupervision);

/// A handle to a child actor that monitors another actor's liveness.
#[derive(Debug)]
pub struct ActorMonitor {
    inner: Option<MonitorInner>,
}

/// A monitor that reports detected failures through actor supervision.
#[derive(Debug)]
pub struct ActorSupervisor {
    inner: Option<MonitorInner>,
}

#[derive(Debug)]
struct MonitorInner {
    target: ActorAddr,
    handle: ActorHandle<MonitorActor>,
    status: watch::Receiver<MonitorStatus>,
    cancelled: Arc<AtomicBool>,
}

/// An endpoint whose owning actor can be monitored.
pub trait MonitorableEndpoint {
    /// Spawn a monitor actor for this endpoint's owning actor as a child of `cx`.
    fn monitor<C>(&self, cx: &C) -> ActorMonitor
    where
        C: context::Actor,
    {
        ActorMonitor::spawn(cx, self.monitored_actor_addr())
    }

    /// The actor whose liveness determines this endpoint's liveness.
    fn monitored_actor_addr(&self) -> ActorAddr;
}

impl<T> MonitorableEndpoint for &T
where
    T: MonitorableEndpoint + ?Sized,
{
    fn monitor<C>(&self, cx: &C) -> ActorMonitor
    where
        C: context::Actor,
    {
        (*self).monitor(cx)
    }

    fn monitored_actor_addr(&self) -> ActorAddr {
        (*self).monitored_actor_addr()
    }
}

impl<A> MonitorableEndpoint for ActorHandle<A>
where
    A: Actor,
{
    fn monitored_actor_addr(&self) -> ActorAddr {
        self.actor_addr().clone()
    }
}

impl<A> MonitorableEndpoint for ActorRef<A>
where
    A: Referable,
{
    fn monitored_actor_addr(&self) -> ActorAddr {
        self.actor_addr().clone()
    }
}

impl<M> MonitorableEndpoint for PortHandle<M>
where
    M: Message,
{
    fn monitored_actor_addr(&self) -> ActorAddr {
        self.location().actor_addr()
    }
}

impl<M> MonitorableEndpoint for OncePortHandle<M>
where
    M: Message,
{
    fn monitored_actor_addr(&self) -> ActorAddr {
        self.port_addr().actor_addr()
    }
}

impl<M> MonitorableEndpoint for PortRef<M>
where
    M: RemoteMessage,
{
    fn monitored_actor_addr(&self) -> ActorAddr {
        self.port_addr().actor_addr()
    }
}

impl<M> MonitorableEndpoint for OncePortRef<M>
where
    M: RemoteMessage,
{
    fn monitored_actor_addr(&self) -> ActorAddr {
        self.port_addr().actor_addr()
    }
}

impl ActorMonitor {
    /// Spawn a monitor actor for `target` as a child of `cx`.
    pub fn spawn<C>(cx: &C, target: ActorAddr) -> Self
    where
        C: context::Actor,
    {
        Self::spawn_with_timings(
            cx,
            target,
            DEFAULT_INITIAL_DELAY,
            DEFAULT_POLL_INTERVAL,
            DEFAULT_REQUEST_TIMEOUT,
        )
    }

    fn spawn_with_timings<C>(
        cx: &C,
        target: ActorAddr,
        initial_delay: Duration,
        poll_interval: Duration,
        request_timeout: Duration,
    ) -> Self
    where
        C: context::Actor,
    {
        let cancelled = Arc::new(AtomicBool::new(false));
        let (status_tx, status) = watch::channel(MonitorStatus::Checking);
        let handle = cx.spawn_with_label(
            "monitor",
            MonitorActor {
                target: target.clone(),
                initial_delay,
                poll_interval,
                request_timeout,
                status_tx,
                cancelled: cancelled.clone(),
                failure: None,
                pending_poll: false,
                supervised: false,
            },
        );
        Self {
            inner: Some(MonitorInner {
                target,
                handle,
                status,
                cancelled,
            }),
        }
    }

    /// The actor being monitored.
    pub fn target(&self) -> &ActorAddr {
        &self.inner().target
    }

    /// Return the monitor's current status.
    pub fn status(&self) -> MonitorStatus {
        self.inner().status.borrow().clone()
    }

    async fn wait_for_failure(&self) -> MonitorFailure {
        let target = self.inner().target.clone();
        let mut status = self.inner().status.clone();
        loop {
            if let MonitorStatus::Failed(failure) = &*status.borrow() {
                return failure.clone();
            }
            if status.changed().await.is_err() {
                return MonitorFailure::MonitorStopped { actor_id: target };
            }
        }
    }

    /// Convert this monitor into one that reports detected failures through actor supervision.
    pub fn into_supervisor<C>(mut self, cx: &C) -> ActorSupervisor
    where
        C: context::Actor,
    {
        let inner = self.inner.take().expect("monitor inner should be present");
        inner.handle.post(cx, MonitorCommand::Supervise);
        ActorSupervisor { inner: Some(inner) }
    }

    /// Run `fut` until it completes or the monitor fails.
    pub async fn guard<F>(&self, fut: F) -> Result<F::Output, MonitorFailure>
    where
        F: Future,
    {
        tokio::pin!(fut);
        tokio::select! {
            result = fut => Ok(result),
            failure = self.wait_for_failure() => Err(failure),
        }
    }

    /// Receive the next message from `receiver` or return a monitor failure.
    pub async fn recv<M>(
        &self,
        receiver: &mut PortReceiver<M>,
    ) -> Result<Result<M, MailboxError>, MonitorFailure>
    where
        M: Message,
    {
        self.guard(receiver.recv()).await
    }

    fn inner(&self) -> &MonitorInner {
        self.inner
            .as_ref()
            .expect("monitor inner should be present")
    }
}

impl Drop for ActorMonitor {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            inner.cancelled.store(true, Ordering::Release);
            let _ = inner.handle.stop("monitor dropped");
        }
    }
}

impl Drop for ActorSupervisor {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            inner.cancelled.store(true, Ordering::Release);
            let _ = inner.handle.stop("supervisor dropped");
        }
    }
}

#[derive(Debug)]
struct MonitorActor {
    target: ActorAddr,
    initial_delay: Duration,
    poll_interval: Duration,
    request_timeout: Duration,
    status_tx: watch::Sender<MonitorStatus>,
    cancelled: Arc<AtomicBool>,
    failure: Option<MonitorFailure>,
    pending_poll: bool,
    supervised: bool,
}

#[derive(Debug)]
struct MonitorTick;

#[derive(Debug)]
struct MonitorPollActor {
    target: ActorAddr,
    request_timeout: Duration,
    monitor: ActorHandle<MonitorActor>,
}

#[cfg(test)]
#[derive(Debug)]
struct MonitorProbe {
    reply: crate::OncePortRef<()>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Named)]
enum MonitorCommand {
    Supervise,
}
wirevalue::register_type!(MonitorCommand);

#[derive(Debug)]
enum MonitorPollResult {
    Status(Option<ActorStatus>),
    ReplyClosed,
    TimedOut,
}

#[async_trait]
impl Actor for MonitorActor {
    async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
        this.post_after(this, MonitorTick, self.initial_delay);
        Ok(())
    }
}

#[async_trait]
impl Actor for MonitorPollActor {
    async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
        let (reply_port, reply_rx) = this.open_once_port::<Option<ActorStatus>>();
        self.target.status_port().post(
            this,
            StatusMessage::GetStatus {
                reply: reply_port.bind(),
            },
        );

        let result = match time::timeout(self.request_timeout, reply_rx.recv()).await {
            Ok(Ok(status)) => MonitorPollResult::Status(status),
            Ok(Err(_)) => MonitorPollResult::ReplyClosed,
            Err(_) => MonitorPollResult::TimedOut,
        };
        self.monitor.post(this, result);
        this.exit("poll complete").map_err(anyhow::Error::from)
    }
}

#[async_trait]
impl Handler<MonitorTick> for MonitorActor {
    async fn handle(&mut self, cx: &Context<Self>, _message: MonitorTick) -> anyhow::Result<()> {
        if self.failure.is_some() || self.pending_poll {
            return Ok(());
        }

        self.start_poll(cx);
        Ok(())
    }
}

#[async_trait]
impl Handler<MonitorPollResult> for MonitorActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: MonitorPollResult,
    ) -> anyhow::Result<()> {
        if !self.pending_poll {
            return Ok(());
        }
        self.pending_poll = false;

        match message {
            MonitorPollResult::Status(status) => {
                let Some(status) = status else {
                    return self.record_failure(MonitorFailure::ActorGone {
                        actor_id: self.target.clone(),
                    });
                };

                if let Some(failure) = self.classify_failure(status.clone()) {
                    self.record_failure(failure)
                } else {
                    self.status_tx.send_replace(MonitorStatus::Alive(status));
                    cx.post_after(cx, MonitorTick, self.poll_interval);
                    Ok(())
                }
            }
            MonitorPollResult::ReplyClosed => {
                self.record_failure(MonitorFailure::StatusReplyClosed {
                    actor_id: self.target.clone(),
                })
            }
            MonitorPollResult::TimedOut => {
                self.record_failure(MonitorFailure::StatusRequestTimedOut {
                    actor_id: self.target.clone(),
                    timeout_millis: self.request_timeout.as_millis() as u64,
                })
            }
        }
    }
}

#[cfg(test)]
#[async_trait]
impl Handler<MonitorProbe> for MonitorActor {
    async fn handle(&mut self, cx: &Context<Self>, message: MonitorProbe) -> anyhow::Result<()> {
        message.reply.post(cx, ());
        Ok(())
    }
}

#[async_trait]
impl Handler<MonitorCommand> for MonitorActor {
    async fn handle(&mut self, _cx: &Context<Self>, message: MonitorCommand) -> anyhow::Result<()> {
        match message {
            MonitorCommand::Supervise => {
                self.supervised = true;
                if let Some(failure) = self.failure.clone()
                    && !self.cancelled.load(Ordering::Acquire)
                {
                    return self.fail_supervised(failure);
                }
                Ok(())
            }
        }
    }
}

impl MonitorActor {
    fn start_poll(&mut self, cx: &Context<'_, Self>) {
        assert!(
            !self.pending_poll,
            "monitor actor started a poll while one was already pending"
        );

        self.pending_poll = true;

        cx.spawn_with_label(
            "monitor_poll",
            MonitorPollActor {
                target: self.target.clone(),
                request_timeout: self.request_timeout,
                monitor: cx.handle(),
            },
        );
    }

    fn classify_failure(&self, status: ActorStatus) -> Option<MonitorFailure> {
        match status {
            ActorStatus::Stopped(_) => Some(MonitorFailure::ActorStopped {
                actor_id: self.target.clone(),
                status,
            }),
            ActorStatus::Failed(_)
            | ActorStatus::Stopping(crate::actor::ActorStoppingReason::Zombie(_))
            | ActorStatus::Unknown => Some(MonitorFailure::ActorFailed {
                actor_id: self.target.clone(),
                status,
            }),
            ActorStatus::Created
            | ActorStatus::Initializing
            | ActorStatus::Client
            | ActorStatus::Idle
            | ActorStatus::Processing(_, _)
            | ActorStatus::Stopping(_) => None,
        }
    }

    fn record_failure(&mut self, failure: MonitorFailure) -> anyhow::Result<()> {
        self.failure = Some(failure.clone());
        self.status_tx
            .send_replace(MonitorStatus::Failed(failure.clone()));
        if self.supervised && !self.cancelled.load(Ordering::Acquire) {
            self.fail_supervised(failure)
        } else {
            Ok(())
        }
    }

    fn fail_supervised(&self, failure: MonitorFailure) -> anyhow::Result<()> {
        anyhow::bail!(crate::actor::ActorErrorKind::SyntheticSupervision(
            Box::new(SyntheticSupervision {
                subject: self.target.clone(),
                failure: Box::new(failure),
                local_fence: self.cancelled.clone(),
            },)
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use async_trait::async_trait;
    use tokio::time;

    use super::*;
    use crate::Proc;
    use crate::actor::ActorErrorKind;
    use crate::supervision::ActorSupervisionEvent;

    #[derive(Debug, typeuri::Named)]
    struct TestActor;

    #[async_trait]
    impl Actor for TestActor {}

    impl crate::actor::Referable for TestActor {}

    #[derive(Debug)]
    struct SupervisorActor {
        target: ActorAddr,
        ready: Option<crate::OncePortRef<ActorAddr>>,
        events: crate::PortRef<ActorSupervisionEvent>,
        supervisor: Option<ActorSupervisor>,
    }

    #[derive(Debug)]
    struct ConvertFailedMonitorActor {
        target: ActorAddr,
        ready: Option<crate::OncePortRef<ActorAddr>>,
        events: crate::PortRef<ActorSupervisionEvent>,
        supervisor: Option<ActorSupervisor>,
    }

    #[derive(Debug)]
    struct DropSupervisorActor {
        target: ActorAddr,
        ready: Option<crate::OncePortRef<ActorAddr>>,
        events: crate::PortRef<ActorSupervisionEvent>,
    }

    #[derive(Debug)]
    struct DropQueuedSupervisorActor {
        target: ActorHandle<TestActor>,
        ready: Option<crate::OncePortRef<()>>,
        events: crate::PortRef<ActorSupervisionEvent>,
    }

    #[async_trait]
    impl Actor for SupervisorActor {
        async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
            let monitor = ActorMonitor::spawn_with_timings(
                this,
                self.target.clone(),
                Duration::ZERO,
                Duration::from_millis(10),
                Duration::from_millis(50),
            );
            let monitor_id = monitor
                .inner
                .as_ref()
                .expect("monitor inner should be present")
                .handle
                .actor_addr()
                .clone();
            self.supervisor = Some(monitor.into_supervisor(this));
            self.ready
                .take()
                .expect("ready port should be present")
                .post(this, monitor_id);
            Ok(())
        }

        async fn handle_supervision_event(
            &mut self,
            this: &Instance<Self>,
            event: &ActorSupervisionEvent,
        ) -> anyhow::Result<bool> {
            self.events.post(this, event.clone());
            Ok(true)
        }
    }

    #[async_trait]
    impl Actor for ConvertFailedMonitorActor {
        async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
            let monitor = ActorMonitor::spawn_with_timings(
                this,
                self.target.clone(),
                Duration::ZERO,
                Duration::from_millis(10),
                Duration::from_millis(50),
            );
            let monitor_id = monitor
                .inner
                .as_ref()
                .expect("monitor inner should be present")
                .handle
                .actor_addr()
                .clone();
            let _failure = monitor.wait_for_failure().await;
            self.supervisor = Some(monitor.into_supervisor(this));
            self.ready
                .take()
                .expect("ready port should be present")
                .post(this, monitor_id);
            Ok(())
        }

        async fn handle_supervision_event(
            &mut self,
            this: &Instance<Self>,
            event: &ActorSupervisionEvent,
        ) -> anyhow::Result<bool> {
            self.events.post(this, event.clone());
            Ok(true)
        }
    }

    #[async_trait]
    impl Actor for DropSupervisorActor {
        async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
            let monitor = ActorMonitor::spawn_with_timings(
                this,
                self.target.clone(),
                Duration::ZERO,
                Duration::from_millis(10),
                Duration::from_millis(50),
            );
            let monitor_id = monitor
                .inner
                .as_ref()
                .expect("monitor inner should be present")
                .handle
                .actor_addr()
                .clone();
            drop(monitor.into_supervisor(this));
            self.ready
                .take()
                .expect("ready port should be present")
                .post(this, monitor_id);
            Ok(())
        }

        async fn handle_supervision_event(
            &mut self,
            this: &Instance<Self>,
            event: &ActorSupervisionEvent,
        ) -> anyhow::Result<bool> {
            self.events.post(this, event.clone());
            Ok(true)
        }
    }

    #[async_trait]
    impl Actor for DropQueuedSupervisorActor {
        async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
            let monitor = ActorMonitor::spawn_with_timings(
                this,
                self.target.actor_addr().clone(),
                Duration::ZERO,
                Duration::from_millis(10),
                Duration::from_millis(50),
            );
            let supervisor = monitor.into_supervisor(this);
            let mut status = supervisor
                .inner
                .as_ref()
                .expect("supervisor inner should be present")
                .status
                .clone();

            self.target.drain_and_stop("done").unwrap();

            loop {
                if matches!(*status.borrow(), MonitorStatus::Failed(_)) {
                    break;
                }
                status.changed().await?;
            }

            drop(supervisor);
            self.ready
                .take()
                .expect("ready port should be present")
                .post(this, ());
            Ok(())
        }

        async fn handle_supervision_event(
            &mut self,
            this: &Instance<Self>,
            event: &ActorSupervisionEvent,
        ) -> anyhow::Result<bool> {
            self.events.post(this, event.clone());
            Ok(true)
        }
    }

    fn short_monitor(client: &crate::Client, target: ActorAddr) -> ActorMonitor {
        ActorMonitor::spawn_with_timings(
            client,
            target,
            Duration::ZERO,
            Duration::from_millis(10),
            Duration::from_millis(50),
        )
    }

    async fn wait_for_alive(monitor: &ActorMonitor) -> ActorStatus {
        let deadline = time::Instant::now() + Duration::from_secs(5);
        loop {
            if let MonitorStatus::Alive(status) = monitor.status() {
                return status;
            }
            assert!(
                time::Instant::now() < deadline,
                "timed out waiting for monitor to report alive"
            );
            time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn test_monitor_reports_alive_actor() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let handle = proc.spawn(TestActor);
        let monitor = short_monitor(&client, handle.actor_addr().clone());

        assert!(matches!(
            wait_for_alive(&monitor).await,
            ActorStatus::Idle | ActorStatus::Processing(_, _)
        ));
    }

    #[tokio::test]
    async fn test_monitor_reports_nonexistent_actor() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let missing = proc.proc_addr().actor_addr("missing");
        let monitor = short_monitor(&client, missing.clone());

        assert_eq!(
            monitor.wait_for_failure().await,
            MonitorFailure::ActorGone { actor_id: missing }
        );
    }

    #[tokio::test]
    async fn test_monitor_respects_initial_delay() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let missing = proc.proc_addr().actor_addr("missing");
        let monitor = ActorMonitor::spawn_with_timings(
            &client,
            missing.clone(),
            Duration::from_millis(100),
            Duration::from_millis(10),
            Duration::from_millis(50),
        );

        assert!(
            time::timeout(Duration::from_millis(20), monitor.wait_for_failure())
                .await
                .is_err()
        );
        assert_eq!(
            monitor.wait_for_failure().await,
            MonitorFailure::ActorGone { actor_id: missing }
        );
    }

    #[test]
    fn test_default_initial_delay_is_two_seconds() {
        assert_eq!(DEFAULT_INITIAL_DELAY, Duration::from_secs(2));
    }

    #[tokio::test]
    async fn test_dropping_monitor_before_first_tick_is_noop() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let missing = proc.proc_addr().actor_addr("missing");
        let monitor = ActorMonitor::spawn_with_timings(
            &client,
            missing,
            Duration::from_millis(200),
            Duration::from_millis(10),
            Duration::from_millis(50),
        );
        let mut status = monitor
            .inner
            .as_ref()
            .expect("monitor inner should be present")
            .status
            .clone();

        drop(monitor);

        time::timeout(Duration::from_secs(1), async {
            while status.changed().await.is_ok() {}
        })
        .await
        .expect("monitor actor should stop when monitor handle is dropped");
        assert_eq!(*status.borrow(), MonitorStatus::Checking);

        time::sleep(Duration::from_millis(250)).await;
        assert_eq!(*status.borrow(), MonitorStatus::Checking);
    }

    #[tokio::test]
    async fn test_monitor_reports_stopped_actor() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let handle = proc.spawn(TestActor);
        let actor_id = handle.actor_addr().clone();
        let monitor = short_monitor(&client, actor_id.clone());

        handle.drain_and_stop("done").unwrap();

        assert_eq!(
            monitor.wait_for_failure().await,
            MonitorFailure::ActorStopped {
                actor_id,
                status: ActorStatus::Stopped("done".to_string()),
            }
        );
    }

    #[tokio::test]
    async fn test_monitor_guard_returns_operation_success() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let handle = proc.spawn(TestActor);
        let monitor = short_monitor(&client, handle.actor_addr().clone());

        let result: Result<Result<u64, &'static str>, MonitorFailure> =
            monitor.guard(async { Ok(123u64) }).await;

        assert_eq!(result, Ok(Ok(123)));
    }

    #[tokio::test]
    async fn test_monitor_guard_returns_monitor_failure() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let missing = proc.proc_addr().actor_addr("missing");
        let monitor = short_monitor(&client, missing.clone());

        let result: Result<Result<(), &'static str>, MonitorFailure> =
            monitor.guard(std::future::pending()).await;

        assert_eq!(result, Err(MonitorFailure::ActorGone { actor_id: missing }));
    }

    #[tokio::test]
    async fn test_actor_ref_is_monitorable() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let handle = proc.spawn(TestActor);
        let actor_ref = ActorRef::<TestActor>::attest(handle.actor_addr().clone());
        let monitor = actor_ref.monitor(&client);

        assert_eq!(monitor.target(), handle.actor_addr());
        assert!(matches!(
            wait_for_alive(&monitor).await,
            ActorStatus::Idle | ActorStatus::Processing(_, _)
        ));
    }

    #[tokio::test]
    async fn test_ports_are_monitorable_by_owner_actor() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let (port, _rx) = client.open_port::<u64>();
        let port_ref = port.bind();
        let (once_port, _once_rx) = client.open_once_port::<u64>();
        let once_port_ref = once_port.bind();

        assert_eq!(port.monitor(&client).target(), client.self_addr());
        assert_eq!(port_ref.monitor(&client).target(), client.self_addr());
        assert_eq!(once_port_ref.monitor(&client).target(), client.self_addr());
    }

    #[tokio::test]
    async fn test_monitor_recv_returns_message() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let handle = proc.spawn(TestActor);
        let monitor = handle.monitor(&client);
        let (port, mut rx) = client.open_port::<u64>();

        port.post(&client, 123);

        assert!(matches!(monitor.recv(&mut rx).await, Ok(Ok(123))));
    }

    #[tokio::test]
    async fn test_monitor_recv_returns_monitor_failure() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let handle = proc.spawn(TestActor);
        let actor_id = handle.actor_addr().clone();
        let monitor = handle.monitor(&client);
        let (_port, mut rx) = client.open_port::<u64>();

        handle.drain_and_stop("done").unwrap();

        match monitor.recv(&mut rx).await {
            Err(MonitorFailure::ActorStopped {
                actor_id: failed_actor_id,
                status,
            }) => {
                assert_eq!(failed_actor_id, actor_id);
                assert_eq!(status, ActorStatus::Stopped("done".to_string()));
            }
            other => panic!("expected monitor failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_monitor_times_out_when_status_proc_is_unreachable() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let unreachable =
            crate::ProcAddr::instance(crate::channel::ChannelAddr::Local(1234), "gone")
                .actor_addr("actor");
        let monitor = short_monitor(&client, unreachable.clone());

        assert_eq!(
            monitor.wait_for_failure().await,
            MonitorFailure::StatusRequestTimedOut {
                actor_id: unreachable,
                timeout_millis: 50,
            }
        );
    }

    #[tokio::test]
    async fn test_monitor_actor_remains_responsive_while_poll_is_pending() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let unreachable =
            crate::ProcAddr::instance(crate::channel::ChannelAddr::Local(1234), "gone")
                .actor_addr("actor");
        let monitor = ActorMonitor::spawn_with_timings(
            &client,
            unreachable,
            Duration::ZERO,
            Duration::from_millis(10),
            Duration::from_secs(5),
        );

        time::sleep(Duration::from_millis(20)).await;
        let (reply, reply_rx) = client.open_once_port();
        monitor
            .inner
            .as_ref()
            .expect("monitor inner should be present")
            .handle
            .post(
                &client,
                MonitorProbe {
                    reply: reply.bind(),
                },
            );

        time::timeout(Duration::from_millis(100), reply_rx.recv())
            .await
            .expect("monitor actor should remain responsive")
            .expect("probe reply should arrive");
    }

    #[tokio::test]
    async fn test_supervised_monitor_reports_synthetic_supervision() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let target = proc.spawn(TestActor);
        let target_id = target.actor_addr().clone();
        let (ready, ready_rx) = client.open_once_port();
        let (events, mut event_rx) = client.open_port();
        let supervisor = proc.spawn(SupervisorActor {
            target: target_id.clone(),
            ready: Some(ready.bind()),
            events: events.bind(),
            supervisor: None,
        });

        let monitor_id = ready_rx.recv().await.unwrap();
        target.drain_and_stop("done").unwrap();

        let event = event_rx.recv().await.unwrap();
        assert_eq!(event.actor_id, monitor_id);
        let ActorStatus::Failed(ActorErrorKind::SyntheticSupervision(synthetic)) =
            event.actor_status
        else {
            panic!("expected synthetic supervision event");
        };
        assert_eq!(synthetic.subject, target_id);
        assert!(matches!(
            *synthetic.failure,
            MonitorFailure::ActorStopped {
                status: ActorStatus::Stopped(_),
                ..
            }
        ));
        supervisor.drain_and_stop("test complete").unwrap();
    }

    #[tokio::test]
    async fn test_failed_monitor_reports_synthetic_supervision_after_conversion() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let target_id = proc.proc_addr().actor_addr("missing");
        let (ready, ready_rx) = client.open_once_port();
        let (events, mut event_rx) = client.open_port();
        let supervisor = proc.spawn(ConvertFailedMonitorActor {
            target: target_id.clone(),
            ready: Some(ready.bind()),
            events: events.bind(),
            supervisor: None,
        });

        let monitor_id = ready_rx.recv().await.unwrap();

        let event = event_rx.recv().await.unwrap();
        assert_eq!(event.actor_id, monitor_id);
        let ActorStatus::Failed(ActorErrorKind::SyntheticSupervision(synthetic)) =
            event.actor_status
        else {
            panic!("expected synthetic supervision event");
        };
        assert_eq!(synthetic.subject, target_id);
        assert!(matches!(
            *synthetic.failure,
            MonitorFailure::ActorGone { .. }
        ));
        supervisor.drain_and_stop("test complete").unwrap();
    }

    #[tokio::test]
    async fn test_dropping_supervisor_disables_synthetic_supervision() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let target = proc.spawn(TestActor);
        let (ready, ready_rx) = client.open_once_port();
        let (events, mut event_rx) = client.open_port();
        let supervisor = proc.spawn(DropSupervisorActor {
            target: target.actor_addr().clone(),
            ready: Some(ready.bind()),
            events: events.bind(),
        });

        let monitor_id = ready_rx.recv().await.unwrap();

        let stop_event = event_rx.recv().await.unwrap();
        assert_eq!(stop_event.actor_id, monitor_id);
        assert!(matches!(stop_event.actor_status, ActorStatus::Stopped(_)));

        target.drain_and_stop("done").unwrap();

        assert!(
            time::timeout(Duration::from_millis(200), event_rx.recv())
                .await
                .is_err()
        );
        supervisor.drain_and_stop("test complete").unwrap();
    }

    #[tokio::test]
    async fn test_dropping_supervisor_drops_queued_synthetic_supervision() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let target = proc.spawn(TestActor);
        let (ready, ready_rx) = client.open_once_port();
        let (events, mut event_rx) = client.open_port();
        let supervisor = proc.spawn(DropQueuedSupervisorActor {
            target,
            ready: Some(ready.bind()),
            events: events.bind(),
        });

        ready_rx.recv().await.unwrap();

        assert!(
            time::timeout(Duration::from_millis(200), event_rx.recv())
                .await
                .is_err()
        );
        supervisor.drain_and_stop("test complete").unwrap();
    }
}
