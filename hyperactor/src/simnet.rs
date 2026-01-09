/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![allow(dead_code)]

//! A simulator capable of simulating Hyperactor's network channels (see: [`channel`]).
//! The simulator can simulate message delivery delays and failures, and is used for
//! testing and development of message distribution techniques.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use dashmap::DashSet;
use enum_as_inner::EnumAsInner;
use ndslice::view::Point;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand_distr::Distribution;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio::task::JoinError;
use tokio::task::JoinHandle;
use tokio::time::interval;

use crate::ActorId;
use crate::ProcId;
use crate::channel::ChannelAddr;
use crate::clock::Clock;
use crate::clock::RealClock;
use crate::clock::SimClock;

static HANDLE: OnceLock<SimNetHandle> = OnceLock::new();

/// A handle for SimNet through which you can send and schedule events in the
/// network.
///
/// Return the \[`NotStarted`\] error when called before `simnet::start()` has been called
#[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `SimNetError`.
pub fn simnet_handle() -> Result<&'static SimNetHandle, SimNetError> {
    match HANDLE.get() {
        Some(handle) => Ok(handle),
        None => Err(SimNetError::Closed("SimNet not started".to_string())),
    }
}

const OPERATIONAL_MESSAGE_BUFFER_SIZE: usize = 8;

/// This is used to define an Address-type for the network.
/// Addresses are bound to nodes in the network.
pub trait Address: Hash + Debug + Eq + PartialEq + Ord + PartialOrd + Clone {}
impl<A: Hash + Debug + Eq + PartialEq + Ord + PartialOrd + Clone> Address for A {}

/// Minimum time unit for the simulator.
pub type SimulatorTimeInstant = tokio::time::Instant;

/// The unit of execution for the simulator.
/// Using handle(), simnet can schedule executions in the network.
/// If you want to send a message for example, you would want to implement
/// a MessageDeliveryEvent much on the lines expressed in simnet tests.
/// You can also do other more advanced concepts such as node churn,
/// or even simulate process spawns in a distributed system. For example,
/// one can implement a SystemActorSimEvent in order to spawn a system
/// actor.
#[async_trait]
pub trait Event: Send + Sync + Debug {
    /// This is the method that will be called when the simulator fires the event
    /// at a particular time instant. Examples:
    /// For messages, it will be delivering the message to the dst's receiver queue.
    /// For a proc spawn, it will be creating the proc object and instantiating it.
    /// For any event that manipulates the network (like adding/removing nodes etc.)
    /// implement handle_network().
    async fn handle(&self) -> Result<(), SimNetError>;

    /// This is the method that will be called when the simulator fires the event
    /// Unless you need to make changes to the network, you do not have to implement this.
    /// Only implement handle() method for all non-simnet requirements.
    async fn handle_network(&self, _phantom: &SimNet) -> Result<(), SimNetError> {
        self.handle().await
    }

    /// The latency of the event. This could be network latency, induced latency (sleep), or
    /// GPU work latency.
    fn duration(&self) -> tokio::time::Duration;

    /// A user-friendly summary of the event
    fn summary(&self) -> String;
}

/// This is a simple event that is used to join a node to the network.
/// It is used to bind a node to a channel address.
#[derive(Debug)]
struct NodeJoinEvent {
    channel_addr: ChannelAddr,
}

#[async_trait]
impl Event for NodeJoinEvent {
    async fn handle(&self) -> Result<(), SimNetError> {
        Ok(())
    }

    async fn handle_network(&self, _simnet: &SimNet) -> Result<(), SimNetError> {
        self.handle().await
    }

    fn duration(&self) -> tokio::time::Duration {
        tokio::time::Duration::ZERO
    }

    fn summary(&self) -> String {
        "Node join".into()
    }
}

#[derive(Debug)]
/// A pytorch operation
pub struct TorchOpEvent {
    op: String,
    done_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    args_string: String,
    kwargs_string: String,
    worker_actor_id: ActorId,
}

#[async_trait]
impl Event for TorchOpEvent {
    async fn handle(&self) -> Result<(), SimNetError> {
        Ok(())
    }

    async fn handle_network(&self, _simnet: &SimNet) -> Result<(), SimNetError> {
        let mut guard = self.done_tx.lock().await;
        match guard.take() {
            Some(done_tx) => {
                let _ = done_tx
                    .send(())
                    .map_err(|_| SimNetError::Closed("done channel is closed".to_string()));
            }
            None => {
                return Err(SimNetError::Closed("already sent once".to_string()));
            }
        }
        Ok(())
    }

    fn duration(&self) -> tokio::time::Duration {
        tokio::time::Duration::from_millis(100)
    }

    fn summary(&self) -> String {
        let kwargs_string = if self.kwargs_string.is_empty() {
            "".to_string()
        } else {
            format!(", {}", self.kwargs_string)
        };
        format!(
            "[{}] Torch Op: {}({}{})",
            self.worker_actor_id, self.op, self.args_string, kwargs_string
        )
    }
}

impl TorchOpEvent {
    /// Creates a new TorchOpEvent.
    pub fn new(
        op: String,
        done_tx: oneshot::Sender<()>,
        args_string: String,
        kwargs_string: String,
        worker_actor_id: ActorId,
    ) -> Box<Self> {
        Box::new(Self {
            op,
            done_tx: Arc::new(Mutex::new(Some(done_tx))),
            args_string,
            kwargs_string,
            worker_actor_id,
        })
    }
}

#[derive(Debug)]
pub(crate) struct SleepEvent {
    done_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    duration: tokio::time::Duration,
}

impl SleepEvent {
    pub(crate) fn new(done_tx: oneshot::Sender<()>, duration: tokio::time::Duration) -> Box<Self> {
        Box::new(Self {
            done_tx: Arc::new(Mutex::new(Some(done_tx))),
            duration,
        })
    }
}

#[async_trait]
impl Event for SleepEvent {
    async fn handle(&self) -> Result<(), SimNetError> {
        Ok(())
    }

    async fn handle_network(&self, _simnet: &SimNet) -> Result<(), SimNetError> {
        let mut guard = self.done_tx.lock().await;
        match guard.take() {
            Some(done_tx) => {
                let _ = done_tx
                    .send(())
                    .map_err(|_| SimNetError::Closed("done channel is closed".to_string()));
            }
            None => {
                return Err(SimNetError::Closed("already sent once".to_string()));
            }
        }
        Ok(())
    }

    fn duration(&self) -> tokio::time::Duration {
        self.duration
    }

    fn summary(&self) -> String {
        format!("Sleeping for {} ms", self.duration.as_millis())
    }
}

/// Each message is timestamped with the delivery time
/// of the message to the sender.
/// The timestamp is used to determine the order in which
/// messages are delivered to senders.
#[derive(Debug)]
pub(crate) struct ScheduledEvent {
    pub(crate) time: SimulatorTimeInstant,
    pub(crate) event: Box<dyn Event>,
}

/// Dispatcher is a trait that defines the send operation.
/// The send operation takes a target address and a data buffer.
/// This method is called when the simulator is ready for the message to be received
/// by the target address.
#[async_trait]
pub trait Dispatcher<A> {
    /// Send a raw data blob to the given target.
    async fn send(&self, target: A, data: wirevalue::Any) -> Result<(), SimNetError>;
}

/// SimNetError is used to indicate errors that occur during
/// network simulation.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum SimNetError {
    /// An invalid address was encountered.
    #[error("invalid address: {0}")]
    InvalidAddress(String),

    /// An invalid node was encountered.
    #[error("invalid node: {0}")]
    InvalidNode(String, #[source] anyhow::Error),

    /// An invalid parameter was encountered.
    #[error("invalid arg: {0}")]
    InvalidArg(String),

    /// The simulator has been closed.
    #[error("closed: {0}")]
    Closed(String),

    /// Timeout when waiting for something.
    #[error("timeout after {} ms: {}", .0.as_millis(), .1)]
    Timeout(Duration, String),

    /// Cannot deliver the message because destination address is missing.
    #[error("missing destination address")]
    MissingDestinationAddress,

    /// SimnetHandle being accessed without starting simnet
    #[error("simnet not started")]
    NotStarted,
}

struct State {
    // The simnet is allowed to advance to the time of the earliest event in this queue at any time
    scheduled_events: BTreeMap<SimulatorTimeInstant, Vec<ScheduledEvent>>,
    // The simnet is allowed to advance to the time of the earliest event in this queue at any time
    // only if the earliest event in `scheduled_events` occurs after the earliest event in this queue
    // or some debounce period has passed where there are only events in this queue.
    unadvanceable_scheduled_events: BTreeMap<SimulatorTimeInstant, Vec<ScheduledEvent>>,
}

/// The state of the python training script.
#[derive(EnumAsInner, Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum TrainingScriptState {
    /// The training script is issuing commands
    Running,
    /// The training script is waiting for the backend to return a future result
    Waiting,
}

/// A distribution of latencies that can be sampled from
pub enum LatencyDistribution {
    /// A beta distribution scaled to a given range of values
    Beta(BetaDistribution),
}

impl LatencyDistribution {
    fn sample(&self, rng: &mut StdRng) -> tokio::time::Duration {
        match &self {
            LatencyDistribution::Beta(sampler) => sampler.sample(rng),
        }
    }
}

/// A beta distribution scaled to a given range of values.
pub struct BetaDistribution {
    min_duration: tokio::time::Duration,
    max_duration: tokio::time::Duration,
    dist: rand_distr::Beta<f64>,
}

impl BetaDistribution {
    /// Sample a sclaed value from the distribution.
    pub fn sample(&self, rng: &mut StdRng) -> tokio::time::Duration {
        let sample = self.dist.sample(rng);

        self.min_duration
            + tokio::time::Duration::from_micros(
                (sample * (self.max_duration - self.min_duration).as_micros() as f64) as u64,
            )
    }

    /// Create a new beta distribution.
    pub fn new(
        min_duration: tokio::time::Duration,
        max_duration: tokio::time::Duration,
        alpha: f64,
        beta: f64,
    ) -> anyhow::Result<Self> {
        if min_duration > max_duration {
            return Err(anyhow::anyhow!(
                "min_duration must not be greater than max_duration, got min_duration: {:?}, max_duration: {:?}",
                min_duration,
                max_duration
            ));
        }
        Ok(Self {
            min_duration,
            max_duration,
            dist: rand_distr::Beta::new(alpha, beta)?,
        })
    }
}
/// Configuration for latencies between distances for the simulator
pub struct LatencyConfig {
    /// inter-region latency distribution
    pub inter_region_distribution: LatencyDistribution,
    /// inter-data center latency distribution
    pub inter_dc_distribution: LatencyDistribution,
    /// inter-zone latency distribution
    pub inter_zone_distribution: LatencyDistribution,
    /// Single random number generator for all distributions to ensure deterministic sampling
    pub rng: StdRng,
}

impl LatencyConfig {
    fn from_distance(&mut self, distance: &Distance) -> tokio::time::Duration {
        match distance {
            Distance::Region => self.inter_region_distribution.sample(&mut self.rng),
            Distance::DataCenter => self.inter_dc_distribution.sample(&mut self.rng),
            Distance::Zone => self.inter_zone_distribution.sample(&mut self.rng),
            Distance::Rack | Distance::Host | Distance::Same => tokio::time::Duration::ZERO,
        }
    }
}

impl Default for LatencyConfig {
    fn default() -> Self {
        let seed: u64 = 0000;
        let mut seed_bytes = [0u8; 32];
        seed_bytes[..8].copy_from_slice(&seed.to_le_bytes());

        Self {
            inter_region_distribution: LatencyDistribution::Beta(
                BetaDistribution::new(
                    tokio::time::Duration::from_millis(500),
                    tokio::time::Duration::from_millis(1000),
                    2.0,
                    1.0,
                )
                .unwrap(),
            ),
            inter_dc_distribution: LatencyDistribution::Beta(
                BetaDistribution::new(
                    tokio::time::Duration::from_millis(50),
                    tokio::time::Duration::from_millis(100),
                    2.0,
                    1.0,
                )
                .unwrap(),
            ),
            inter_zone_distribution: LatencyDistribution::Beta(
                BetaDistribution::new(
                    tokio::time::Duration::from_millis(5),
                    tokio::time::Duration::from_millis(10),
                    2.0,
                    1.0,
                )
                .unwrap(),
            ),
            rng: StdRng::from_seed(seed_bytes),
        }
    }
}

/// A handle to a running [`SimNet`] instance.
pub struct SimNetHandle {
    join_handle: Mutex<Option<JoinHandle<Vec<SimulatorEventRecord>>>>,
    event_tx: UnboundedSender<(Box<dyn Event>, bool, Option<SimulatorTimeInstant>)>,
    pending_event_count: Arc<AtomicUsize>,
    /// A receiver to receive simulator operational messages.
    /// The receiver can be moved out of the simnet handle.
    training_script_state_tx: tokio::sync::watch::Sender<TrainingScriptState>,
    /// Signal to stop the simnet loop
    stop_signal: Arc<AtomicBool>,
    resources: DashMap<ProcId, Point>,
    latencies: std::sync::Mutex<LatencyConfig>,
}

impl SimNetHandle {
    /// Sends an event to be scheduled onto the simnet's event loop
    #[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `SimNetError`.
    pub fn send_event(&self, event: Box<dyn Event>) -> Result<(), SimNetError> {
        self.send_event_impl(event, true)
    }

    #[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `SimNetError`.
    fn send_event_impl(&self, event: Box<dyn Event>, advanceable: bool) -> Result<(), SimNetError> {
        self.pending_event_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.event_tx
            .send((event, advanceable, None))
            .map_err(|err| SimNetError::Closed(err.to_string()))
    }

    /// Sends an non-advanceable event to be scheduled onto the simnet's event loop
    /// A non-advanceable event is an event that cannot advance the simnet's time unless
    /// the earliest event in the simnet's advancing event queue occurs after the earliest
    /// event in the simnet's non-advancing event queue, or some debounce period has passed
    /// where there are only events in the simnet's non-advancing event queue.
    #[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `SimNetError`.
    pub fn send_nonadvanceable_event(&self, event: Box<dyn Event>) -> Result<(), SimNetError> {
        self.send_event_impl(event, false)
    }

    /// Sends an event that already has a scheduled time onto the simnet's event loop
    #[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `SimNetError`.
    pub(crate) fn send_scheduled_event(
        &self,
        ScheduledEvent { event, time }: ScheduledEvent,
    ) -> Result<(), SimNetError> {
        self.pending_event_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.event_tx
            .send((event, true, Some(time)))
            .map_err(|err| SimNetError::Closed(err.to_string()))
    }

    /// Let the simnet know if the training script is running or waiting for the backend
    /// to return a future result.
    pub fn set_training_script_state(&self, state: TrainingScriptState) {
        self.training_script_state_tx.send(state).unwrap();
    }

    /// Bind the given address to this simulator instance.
    #[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `SimNetError`.
    pub fn bind(&self, address: ChannelAddr) -> Result<(), SimNetError> {
        self.pending_event_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.event_tx
            .send((
                Box::new(NodeJoinEvent {
                    channel_addr: address,
                }),
                true,
                None,
            ))
            .map_err(|err| SimNetError::Closed(err.to_string()))
    }

    /// Close the simulator, processing pending messages before
    /// completing the returned future.
    pub async fn close(&self) -> Result<Vec<SimulatorEventRecord>, JoinError> {
        // Signal the simnet loop to stop
        self.stop_signal.store(true, Ordering::SeqCst);

        let mut guard = self.join_handle.lock().await;
        if let Some(handle) = guard.take() {
            handle.await
        } else {
            Ok(vec![])
        }
    }

    /// Wait for all of the received events to be scheduled for flight.
    /// It ticks the simnet time till all of the scheduled events are processed.
    pub async fn flush(&self, timeout: Duration) -> Result<(), SimNetError> {
        let pending_event_count = self.pending_event_count.clone();
        // poll for the pending event count to be zero.
        let mut interval = interval(Duration::from_millis(10));
        let deadline = RealClock.now() + timeout;
        while RealClock.now() < deadline {
            interval.tick().await;
            if pending_event_count.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                return Ok(());
            }
        }
        Err(SimNetError::Timeout(
            timeout,
            "timeout waiting for received events to be scheduled".to_string(),
        ))
    }

    /// Register the location in resource space for a Proc
    pub fn register_proc(&self, proc_id: ProcId, point: Point) {
        self.resources.insert(proc_id, point);
    }

    /// Sample a latency between two procs
    pub fn sample_latency(&self, src: &ProcId, dest: &ProcId) -> tokio::time::Duration {
        let distances = [
            Distance::Region,
            Distance::DataCenter,
            Distance::Zone,
            Distance::Rack,
            Distance::Host,
            Distance::Same,
        ];

        let src_coords = self
            .resources
            .get(src)
            .map(|point| point.coords().clone())
            .unwrap_or(distances.iter().map(|_| 0).collect::<Vec<usize>>());

        let dest_coords = self
            .resources
            .get(dest)
            .map(|point| point.coords().clone())
            .unwrap_or(distances.iter().map(|_| 0).collect::<Vec<usize>>());

        for ((src, dest), distance) in src_coords.into_iter().zip(dest_coords).zip(distances) {
            if src != dest {
                let mut guard = self.latencies.lock().unwrap_or_else(|e| e.into_inner());
                return guard.from_distance(&distance);
            }
        }

        let mut guard = self.latencies.lock().unwrap_or_else(|e| e.into_inner());
        guard.from_distance(&Distance::Same)
    }
}

#[derive(Debug)]
enum Distance {
    Region,
    DataCenter,
    Zone,
    Rack,
    Host,
    Same,
}

/// SimNet defines a network of nodes.
/// Each node is identified by a unique id.
/// The network is represented as a graph of nodes.
/// The graph is represented as a map of edges.
/// The network also has a cloud of inflight messages
pub struct SimNet {
    address_book: DashSet<ChannelAddr>,
    state: State,
    max_latency: Duration,
    records: Vec<SimulatorEventRecord>,
    // number of events that has been received but not yet processed.
    pending_event_count: Arc<AtomicUsize>,
}

/// Starts a sim net.
pub fn start() {
    start_with_config(LatencyConfig::default())
}

/// Starts a sim net with configured latencies between distances
pub fn start_with_config(config: LatencyConfig) {
    let max_duration_ms = 1000 * 10;
    // Construct a topology with one node: the default A.
    let address_book: DashSet<ChannelAddr> = DashSet::new();

    let (training_script_state_tx, training_script_state_rx) =
        tokio::sync::watch::channel(TrainingScriptState::Waiting);
    let (event_tx, event_rx) =
        mpsc::unbounded_channel::<(Box<dyn Event>, bool, Option<SimulatorTimeInstant>)>();
    let pending_event_count = Arc::new(AtomicUsize::new(0));
    let stop_signal = Arc::new(AtomicBool::new(false));

    let join_handle = Mutex::new(Some({
        let pending_event_count = pending_event_count.clone();
        let stop_signal = stop_signal.clone();

        tokio::spawn(async move {
            SimNet {
                address_book,
                state: State {
                    scheduled_events: BTreeMap::new(),
                    unadvanceable_scheduled_events: BTreeMap::new(),
                },
                max_latency: Duration::from_millis(max_duration_ms),
                records: Vec::new(),
                pending_event_count,
            }
            .run(event_rx, training_script_state_rx, stop_signal)
            .await
        })
    }));

    HANDLE.get_or_init(|| SimNetHandle {
        join_handle,
        event_tx,
        pending_event_count,
        training_script_state_tx,
        stop_signal,
        resources: DashMap::new(),
        latencies: std::sync::Mutex::new(config),
    });
}

impl SimNet {
    fn create_scheduled_event(&mut self, event: Box<dyn Event>) -> ScheduledEvent {
        // Get latency
        ScheduledEvent {
            time: SimClock.now() + event.duration(),
            event,
        }
    }

    /// Schedule the event into the network.
    fn schedule_event(&mut self, scheduled_event: ScheduledEvent, advanceable: bool) {
        let start_at = SimClock.now();
        let end_at = scheduled_event.time;

        self.records.push(SimulatorEventRecord {
            summary: scheduled_event.event.summary(),
            start_at: SimClock.duration_since_start(start_at).as_millis() as u64,
            end_at: SimClock.duration_since_start(end_at).as_millis() as u64,
        });

        if advanceable {
            self.state
                .scheduled_events
                .entry(scheduled_event.time)
                .or_default()
                .push(scheduled_event);
        } else {
            self.state
                .unadvanceable_scheduled_events
                .entry(scheduled_event.time)
                .or_default()
                .push(scheduled_event);
        }
    }

    /// Run the simulation. This will dispatch all the messages in the network.
    /// And wait for new ones.
    async fn run(
        &mut self,
        mut event_rx: UnboundedReceiver<(Box<dyn Event>, bool, Option<SimulatorTimeInstant>)>,
        training_script_state_rx: tokio::sync::watch::Receiver<TrainingScriptState>,
        stop_signal: Arc<AtomicBool>,
    ) -> Vec<SimulatorEventRecord> {
        // The simulated number of milliseconds the training script
        // has spent waiting for the backend to resolve a future
        let mut training_script_waiting_time = tokio::time::Duration::from_millis(0);
        // Duration elapsed while only non_advanceable_events has events
        let mut debounce_timer: Option<tokio::time::Instant> = None;

        let debounce_duration = std::env::var("SIM_DEBOUNCE")
            .ok()
            .and_then(|val| val.parse::<u64>().ok())
            .unwrap_or(1);

        'outer: loop {
            // Check if we should stop
            if stop_signal.load(Ordering::SeqCst) {
                break 'outer self.records.clone();
            }

            while let Ok(Some((event, advanceable, time))) = RealClock
                .timeout(
                    tokio::time::Duration::from_millis(debounce_duration),
                    event_rx.recv(),
                )
                .await
            {
                let scheduled_event = match time {
                    Some(time) => ScheduledEvent {
                        time: time + training_script_waiting_time,
                        event,
                    },
                    None => self.create_scheduled_event(event),
                };
                self.schedule_event(scheduled_event, advanceable);
            }

            {
                // If the training script is running and issuing commands
                // it is not safe to advance past the training script time
                // otherwise a command issued by the training script may
                // be scheduled for a time in the past
                if training_script_state_rx.borrow().is_running()
                    && self
                        .state
                        .scheduled_events
                        .first_key_value()
                        .is_some_and(|(time, _)| {
                            *time > RealClock.now() + training_script_waiting_time
                        })
                {
                    tokio::task::yield_now().await;
                    continue;
                }
                match (
                    self.state.scheduled_events.first_key_value(),
                    self.state.unadvanceable_scheduled_events.first_key_value(),
                ) {
                    (None, Some(_)) if debounce_timer.is_none() => {
                        // Start debounce timer when only the non-advancedable
                        // queue has events and the timer has not already started
                        debounce_timer = Some(RealClock.now());
                    }
                    // Timer already active
                    (None, Some(_)) => {}
                    // Reset timer when non-advanceable queue is not the only queue with events
                    _ => {
                        debounce_timer = None;
                    }
                }
                // process for next delivery time.
                let Some((scheduled_time, scheduled_events)) = (match (
                    self.state.scheduled_events.first_key_value(),
                    self.state.unadvanceable_scheduled_events.first_key_value(),
                ) {
                    (Some((advanceable_time, _)), Some((unadvanceable_time, _))) => {
                        if unadvanceable_time < advanceable_time {
                            self.state.unadvanceable_scheduled_events.pop_first()
                        } else {
                            self.state.scheduled_events.pop_first()
                        }
                    }
                    (Some(_), None) => self.state.scheduled_events.pop_first(),
                    (None, Some(_)) => match debounce_timer {
                        Some(time) => {
                            if time.elapsed() > tokio::time::Duration::from_millis(1000) {
                                // debounce interval has elapsed, reset timer
                                debounce_timer = None;
                                self.state.unadvanceable_scheduled_events.pop_first()
                            } else {
                                None
                            }
                        }
                        None => None,
                    },
                    (None, None) => None,
                }) else {
                    tokio::select! {
                        Some((event, advanceable, time)) = event_rx.recv() => {
                            let scheduled_event = match time {
                                Some(time) => ScheduledEvent {
                                    time: time + training_script_waiting_time,
                                    event,
                                },
                                None => self.create_scheduled_event(event),
                            };
                            self.schedule_event(scheduled_event, advanceable);
                        },
                        _ = RealClock.sleep(Duration::from_millis(10)) => {}
                    }
                    continue;
                };
                if training_script_state_rx.borrow().is_waiting() {
                    let advanced_time = scheduled_time - SimClock.now();
                    training_script_waiting_time += advanced_time;
                }
                SimClock.advance_to(scheduled_time);
                for scheduled_event in scheduled_events {
                    self.pending_event_count
                        .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                    if scheduled_event.event.handle_network(self).await.is_err() {
                        break 'outer self.records.clone(); //TODO
                    }
                }
            }
        }
    }
}

fn serialize_optional_channel_addr<S>(
    addr: &Option<ChannelAddr>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match addr {
        Some(addr) => serializer.serialize_str(&addr.to_string()),
        None => serializer.serialize_none(),
    }
}

fn deserialize_channel_addr<'de, D>(deserializer: D) -> Result<ChannelAddr, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    s.parse().map_err(serde::de::Error::custom)
}

/// DeliveryRecord is a structure to bookkeep the message events.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SimulatorEventRecord {
    /// Event dependent summary for user
    pub summary: String,
    /// The time at which the message delivery was started.
    pub start_at: u64,
    /// The time at which the message was delivered to the receiver.
    pub end_at: u64,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use async_trait::async_trait;
    use ndslice::extent;
    use tokio::sync::Mutex;

    use super::*;
    use crate::channel::sim::SimAddr;
    use crate::clock::Clock;
    use crate::clock::RealClock;
    use crate::clock::SimClock;
    use crate::id;
    use crate::simnet;
    use crate::simnet::Dispatcher;
    use crate::simnet::Event;
    use crate::simnet::SimNetError;

    #[derive(Debug)]
    struct MessageDeliveryEvent {
        src_addr: SimAddr,
        dest_addr: SimAddr,
        data: wirevalue::Any,
        duration: tokio::time::Duration,
        dispatcher: Option<TestDispatcher>,
    }

    #[async_trait]
    impl Event for MessageDeliveryEvent {
        async fn handle(&self) -> Result<(), simnet::SimNetError> {
            if let Some(dispatcher) = &self.dispatcher {
                dispatcher
                    .send(self.dest_addr.clone(), self.data.clone())
                    .await?;
            }
            Ok(())
        }
        fn duration(&self) -> tokio::time::Duration {
            self.duration
        }

        fn summary(&self) -> String {
            format!(
                "Sending message from {} to {}",
                self.src_addr.addr().clone(),
                self.dest_addr.addr().clone()
            )
        }
    }

    impl MessageDeliveryEvent {
        fn new(
            src_addr: SimAddr,
            dest_addr: SimAddr,
            data: wirevalue::Any,
            dispatcher: Option<TestDispatcher>,
            duration: tokio::time::Duration,
        ) -> Self {
            Self {
                src_addr,
                dest_addr,
                data,
                duration,
                dispatcher,
            }
        }
    }

    #[derive(Debug, Clone)]
    struct TestDispatcher {
        pub mbuffers: Arc<Mutex<HashMap<SimAddr, Vec<wirevalue::Any>>>>,
    }

    impl Default for TestDispatcher {
        fn default() -> Self {
            Self {
                mbuffers: Arc::new(Mutex::new(HashMap::new())),
            }
        }
    }

    #[async_trait]
    impl Dispatcher<SimAddr> for TestDispatcher {
        async fn send(&self, target: SimAddr, data: wirevalue::Any) -> Result<(), SimNetError> {
            let mut buf = self.mbuffers.lock().await;
            buf.entry(target).or_default().push(data);
            Ok(())
        }
    }

    #[cfg(target_os = "linux")]
    fn random_abstract_addr() -> ChannelAddr {
        use rand::Rng;
        use rand::distributions::Alphanumeric;

        let random_string = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(24)
            .map(char::from)
            .collect::<String>();
        format!("unix!@{random_string}").parse().unwrap()
    }

    #[tokio::test]
    async fn test_handle_instantiation() {
        start();
        simnet_handle().unwrap().close().await.unwrap();
    }

    #[tokio::test]
    async fn test_simnet_config() {
        // Tests that we can create a simnet, config latency between distances and sample latencies between procs.
        let ext = extent!(region = 1, dc = 2, zone = 2, rack = 4, host = 4, gpu = 8);

        let alice = id!(world[0]);
        let bob = id!(world[1]);
        let charlie = id!(world[2]);

        let config = LatencyConfig {
            inter_zone_distribution: LatencyDistribution::Beta(
                BetaDistribution::new(
                    tokio::time::Duration::from_millis(1000),
                    tokio::time::Duration::from_millis(1000),
                    1.0,
                    1.0,
                )
                .unwrap(),
            ),
            inter_dc_distribution: LatencyDistribution::Beta(
                BetaDistribution::new(
                    tokio::time::Duration::from_millis(2000),
                    tokio::time::Duration::from_millis(2000),
                    1.0,
                    1.0,
                )
                .unwrap(),
            ),
            ..Default::default()
        };
        start_with_config(config);

        let handle = simnet_handle().unwrap();
        handle.register_proc(alice.clone(), ext.point(vec![0, 0, 0, 0, 0, 0]).unwrap());
        handle.register_proc(bob.clone(), ext.point(vec![0, 0, 1, 0, 0, 0]).unwrap());
        handle.register_proc(charlie.clone(), ext.point(vec![0, 1, 0, 0, 0, 0]).unwrap());
        assert_eq!(
            handle.sample_latency(&alice, &bob),
            tokio::time::Duration::from_millis(1000)
        );
        assert_eq!(
            handle.sample_latency(&alice, &charlie),
            tokio::time::Duration::from_millis(2000)
        );
    }

    #[tokio::test]
    async fn test_simnet_debounce() {
        let config = LatencyConfig {
            inter_zone_distribution: LatencyDistribution::Beta(
                BetaDistribution::new(
                    tokio::time::Duration::from_millis(1000),
                    tokio::time::Duration::from_millis(1000),
                    1.0,
                    1.0,
                )
                .unwrap(),
            ),
            ..Default::default()
        };
        start_with_config(config);
        let alice = "local:1".parse::<simnet::ChannelAddr>().unwrap();
        let bob = "local:2".parse::<simnet::ChannelAddr>().unwrap();

        let latency = Duration::from_millis(10000);

        let alice = SimAddr::new(alice).unwrap();
        let bob = SimAddr::new(bob).unwrap();

        // Rapidly send 10 messages expecting that each one debounces the processing
        for _ in 0..10 {
            simnet_handle()
                .unwrap()
                .send_event(Box::new(MessageDeliveryEvent::new(
                    alice.clone(),
                    bob.clone(),
                    wirevalue::Any::serialize(&"123".to_string()).unwrap(),
                    None,
                    latency,
                )))
                .unwrap();
            RealClock
                .sleep(tokio::time::Duration::from_micros(500))
                .await;
        }

        simnet_handle()
            .unwrap()
            .flush(Duration::from_secs(20))
            .await
            .unwrap();

        let records = simnet_handle().unwrap().close().await;
        assert_eq!(records.as_ref().unwrap().len(), 10);

        // If debounce is successful, the simnet will not advance to the delivery of any of
        // the messages before all are received
        assert_eq!(
            records.unwrap().last().unwrap().end_at,
            latency.as_millis() as u64
        );
    }

    #[tokio::test]
    async fn test_sim_dispatch() {
        start();
        let sender = Some(TestDispatcher::default());
        let mut addresses: Vec<simnet::ChannelAddr> = Vec::new();
        // // Create a simple network of 4 nodes.
        for i in 0..4 {
            addresses.push(
                format!("local:{}", i)
                    .parse::<simnet::ChannelAddr>()
                    .unwrap(),
            );
        }

        let messages: Vec<wirevalue::Any> = vec!["First 0 1", "First 2 3", "Second 0 1"]
            .into_iter()
            .map(|s| wirevalue::Any::serialize(&s.to_string()).unwrap())
            .collect();

        let addr_0 = SimAddr::new(addresses[0].clone()).unwrap();
        let addr_1 = SimAddr::new(addresses[1].clone()).unwrap();
        let addr_2 = SimAddr::new(addresses[2].clone()).unwrap();
        let addr_3 = SimAddr::new(addresses[3].clone()).unwrap();
        let one = Box::new(MessageDeliveryEvent::new(
            addr_0.clone(),
            addr_1.clone(),
            messages[0].clone(),
            sender.clone(),
            tokio::time::Duration::ZERO,
        ));
        let two = Box::new(MessageDeliveryEvent::new(
            addr_2.clone(),
            addr_3.clone(),
            messages[1].clone(),
            sender.clone(),
            tokio::time::Duration::ZERO,
        ));
        let three = Box::new(MessageDeliveryEvent::new(
            addr_0.clone(),
            addr_1.clone(),
            messages[2].clone(),
            sender.clone(),
            tokio::time::Duration::ZERO,
        ));

        simnet_handle().unwrap().send_event(one).unwrap();
        simnet_handle().unwrap().send_event(two).unwrap();
        simnet_handle().unwrap().send_event(three).unwrap();

        simnet_handle()
            .unwrap()
            .flush(Duration::from_millis(1000))
            .await
            .unwrap();
        let records = simnet_handle().unwrap().close().await.unwrap();
        eprintln!("Records: {:?}", records);
        // Close the channel
        simnet_handle().unwrap().close().await.unwrap();

        // Check results
        let buf = sender.as_ref().unwrap().mbuffers.lock().await;
        assert_eq!(buf.len(), 2);
        assert_eq!(buf[&addr_1].len(), 2);
        assert_eq!(buf[&addr_3].len(), 1);

        assert_eq!(buf[&addr_1][0], messages[0]);
        assert_eq!(buf[&addr_1][1], messages[2]);
        assert_eq!(buf[&addr_3][0], messages[1]);
    }

    #[tokio::test]
    async fn test_sim_sleep() {
        start();

        let start = SimClock.now();
        assert_eq!(
            SimClock.duration_since_start(start),
            tokio::time::Duration::ZERO
        );

        SimClock.sleep(tokio::time::Duration::from_secs(10)).await;

        let end = SimClock.now();
        assert_eq!(
            SimClock.duration_since_start(end),
            tokio::time::Duration::from_secs(10)
        );
    }

    #[tokio::test]
    async fn test_torch_op() {
        start();
        let args_string = "1, 2".to_string();
        let kwargs_string = "a=2".to_string();

        let (tx, rx) = oneshot::channel();

        simnet_handle()
            .unwrap()
            .send_event(TorchOpEvent::new(
                "torch.ops.aten.ones.default".to_string(),
                tx,
                args_string,
                kwargs_string,
                id!(mesh_0_worker[0].worker_0),
            ))
            .unwrap();

        rx.await.unwrap();

        simnet_handle()
            .unwrap()
            .flush(Duration::from_millis(1000))
            .await
            .unwrap();
        let records = simnet_handle().unwrap().close().await;
        let expected_record = SimulatorEventRecord {
            summary:
                "[mesh_0_worker[0].worker_0[0]] Torch Op: torch.ops.aten.ones.default(1, 2, a=2)"
                    .to_string(),
            start_at: 0,
            end_at: 100,
        };
        assert!(records.as_ref().unwrap().len() == 1);
        assert_eq!(records.unwrap().first().unwrap(), &expected_record);
    }
}
