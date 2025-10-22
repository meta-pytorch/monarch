/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

// SimTx contains a way to send through the network.
// SimRx contains a way to receive messages.

//! Local simulated channel implementation.
use std::any::Any;
// send leads to add to network.
use std::marker::PhantomData;
use std::sync::Arc;

use dashmap::DashMap;
use regex::Regex;

use super::*;
use crate::channel;
use crate::clock::Clock;
use crate::clock::RealClock;
use crate::data::Serialized;
use crate::mailbox::MessageEnvelope;
use crate::simnet::Dispatcher;
use crate::simnet::Event;
use crate::simnet::ScheduledEvent;
use crate::simnet::SimNetError;
use crate::simnet::simnet_handle;

lazy_static! {
    static ref SENDER: SimDispatcher = SimDispatcher::default();
}
static SIM_LINK_BUF_SIZE: usize = 256;

/// An address for a simulated channel.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Ord,
    PartialOrd,
    Hash
)]
pub struct SimAddr {
    src: Option<Box<ChannelAddr>>,
    /// The address.
    addr: Box<ChannelAddr>,
    /// If source is the client
    client: bool,
}

impl SimAddr {
    /// Creates a new SimAddr.
    #[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `SimNetError`.
    /// Creates a new SimAddr without a source to be served
    pub fn new(addr: ChannelAddr) -> Result<Self, SimNetError> {
        Self::new_impl(None, addr, false)
    }

    /// Creates a new directional SimAddr meant to convey a channel between two addresses.
    #[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `SimNetError`.
    pub fn new_with_src(src: ChannelAddr, addr: ChannelAddr) -> Result<Self, SimNetError> {
        Self::new_impl(Some(Box::new(src)), addr, false)
    }

    /// Creates a new directional SimAddr meant to convey a channel between two addresses.
    #[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `SimNetError`.
    fn new_with_client_src(src: ChannelAddr, addr: ChannelAddr) -> Result<Self, SimNetError> {
        Self::new_impl(Some(Box::new(src)), addr, true)
    }

    #[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `SimNetError`.
    fn new_impl(
        src: Option<Box<ChannelAddr>>,
        addr: ChannelAddr,
        client: bool,
    ) -> Result<Self, SimNetError> {
        if let ChannelAddr::Sim(_) = &addr {
            return Err(SimNetError::InvalidArg(format!(
                "addr cannot be a sim address, found {}",
                addr
            )));
        }
        Ok(Self {
            src,
            addr: Box::new(addr),
            client,
        })
    }

    /// Returns the address.
    pub fn addr(&self) -> &ChannelAddr {
        &self.addr
    }

    /// Returns the source address
    pub fn src(&self) -> &Option<Box<ChannelAddr>> {
        &self.src
    }

    /// The underlying transport we are simulating
    pub fn transport(&self) -> ChannelTransport {
        self.addr.transport()
    }
}

impl fmt::Display for SimAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.src {
            None => write!(f, "{}", self.addr),
            Some(src) => write!(f, "{},{}", src, self.addr),
        }
    }
}

/// Message Event that can be passed around in the simnet.
#[derive(Debug)]
pub(crate) struct MessageDeliveryEvent {
    dest_addr: ChannelAddr,
    data: Serialized,
    latency: tokio::time::Duration,
}

impl MessageDeliveryEvent {
    /// Creates a new MessageDeliveryEvent.
    pub fn new(dest_addr: ChannelAddr, data: Serialized, latency: tokio::time::Duration) -> Self {
        Self {
            dest_addr,
            data,
            latency,
        }
    }
}

#[async_trait]
impl Event for MessageDeliveryEvent {
    async fn handle(&self) -> Result<(), SimNetError> {
        // Send the message to the correct receiver.
        SENDER
            .send(self.dest_addr.clone(), self.data.clone())
            .await?;
        Ok(())
    }

    fn duration(&self) -> tokio::time::Duration {
        self.latency
    }

    fn summary(&self) -> String {
        format!("Sending message to {}", self.dest_addr.clone())
    }
}

/// Bind a channel address to the simnet. It will register the address as a node in simnet,
/// and configure default latencies between this node and all other existing nodes.
pub async fn bind(addr: ChannelAddr) -> anyhow::Result<(), SimNetError> {
    simnet_handle()?.bind(addr)
}

/// Returns a simulated channel address that is bound to "any" channel address.
pub(crate) fn any(transport: ChannelTransport) -> ChannelAddr {
    ChannelAddr::Sim(SimAddr {
        src: None,
        addr: Box::new(ChannelAddr::any(transport)),
        client: false,
    })
}

/// Parse the sim channel address. It should have two non-sim channel addresses separated by a comma.
#[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `ChannelError`.
pub fn parse(addr_string: &str) -> Result<ChannelAddr, ChannelError> {
    let re = Regex::new(r"([^,]+)(,([^,]+))?$").map_err(|err| {
        ChannelError::InvalidAddress(format!("invalid sim address regex: {}", err))
    })?;

    let result = re.captures(addr_string);
    if let Some(caps) = result {
        let parts = caps
            .iter()
            .skip(1)
            .map(|cap| cap.map_or("", |m| m.as_str()))
            .filter(|m| !m.is_empty())
            .collect::<Vec<_>>();

        if parts.iter().any(|part| part.starts_with("sim!")) {
            return Err(ChannelError::InvalidAddress(addr_string.to_string()));
        }

        match parts.len() {
            1 => {
                let addr = parts[0].parse::<ChannelAddr>()?;

                Ok(ChannelAddr::Sim(SimAddr::new(addr)?))
            }
            3 => {
                let src_addr = parts[0].parse::<ChannelAddr>()?;
                let addr = parts[2].parse::<ChannelAddr>()?;
                Ok(ChannelAddr::Sim(if parts[0] == "client" {
                    SimAddr::new_with_client_src(src_addr, addr)
                } else {
                    SimAddr::new_with_src(src_addr, addr)
                }?))
            }
            _ => Err(ChannelError::InvalidAddress(addr_string.to_string())),
        }
    } else {
        Err(ChannelError::InvalidAddress(addr_string.to_string()))
    }
}

impl<M: RemoteMessage> Drop for SimRx<M> {
    fn drop(&mut self) {
        // Remove the sender from the dispatchers.
        SENDER.dispatchers.remove(&self.addr);
    }
}

/// Primarily used for dispatching messages to the correct sender.
#[derive(Debug)]
pub struct SimDispatcher {
    dispatchers: DashMap<ChannelAddr, mpsc::Sender<Serialized>>,
    sender_cache: DashMap<ChannelAddr, Arc<dyn Tx<MessageEnvelope> + Send + Sync>>,
}

fn create_egress_sender(
    addr: ChannelAddr,
) -> anyhow::Result<Arc<dyn Tx<MessageEnvelope> + Send + Sync>> {
    let tx = channel::dial(addr, "sim-egress".to_string())?;
    Ok(Arc::new(tx))
}

#[async_trait]
impl Dispatcher<ChannelAddr> for SimDispatcher {
    async fn send(&self, addr: ChannelAddr, data: Serialized) -> Result<(), SimNetError> {
        self.dispatchers
            .get(&addr)
            .ok_or_else(|| {
                SimNetError::InvalidNode(addr.to_string(), anyhow::anyhow!("no dispatcher found"))
            })?
            .send(data)
            .await
            .map_err(|err| SimNetError::InvalidNode(addr.to_string(), err.into()))
    }
}

impl Default for SimDispatcher {
    fn default() -> Self {
        Self {
            dispatchers: DashMap::new(),
            sender_cache: DashMap::new(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct SimTx<M: RemoteMessage> {
    src_addr: Option<ChannelAddr>,
    dst_addr: ChannelAddr,
    status: watch::Receiver<TxStatus>, // Default impl. Always reports `Active`.
    client: bool,
    _phantom: PhantomData<M>,
}

#[derive(Debug)]
pub(crate) struct SimRx<M: RemoteMessage> {
    /// The destination address, not the full SimAddr.
    addr: ChannelAddr,
    rx: mpsc::Receiver<Serialized>,
    _phantom: PhantomData<M>,
}

#[async_trait]
impl<M: RemoteMessage + Any> Tx<M> for SimTx<M> {
    fn try_post(&self, message: M, _return_handle: oneshot::Sender<M>) -> Result<(), SendError<M>> {
        let data = match Serialized::serialize(&message) {
            Ok(data) => data,
            Err(err) => return Err(SendError(err.into(), message)),
        };

        let envelope = (&message as &dyn Any)
            .downcast_ref::<MessageEnvelope>()
            .expect("RemoteMessage should always be a MessageEnvelope");

        let (sender, dest) = (envelope.sender().clone(), envelope.dest().0.clone());

        match simnet_handle() {
            Ok(handle) => {
                let event = Box::new(MessageDeliveryEvent::new(
                    self.dst_addr.clone(),
                    data,
                    handle.sample_latency(sender.proc_id(), dest.proc_id()),
                ));

                match &self.src_addr {
                    Some(_) if self.client => handle.send_scheduled_event(ScheduledEvent {
                        event,
                        time: RealClock.now(),
                    }),
                    _ => handle.send_event(event),
                }
            }
            .map_err(|err: SimNetError| SendError(ChannelError::from(err), message)),
            Err(err) => Err(SendError(ChannelError::from(err), message)),
        }
    }

    fn addr(&self) -> ChannelAddr {
        self.dst_addr.clone()
    }

    fn status(&self) -> &watch::Receiver<TxStatus> {
        &self.status
    }
}

/// Dial a peer and return a transmitter. The transmitter can retrieve from the
/// network the link latency.
#[allow(clippy::result_large_err)] // TODO: Consider reducing the size of `ChannelError`.
pub(crate) fn dial<M: RemoteMessage>(addr: SimAddr) -> Result<SimTx<M>, ChannelError> {
    // This watch channel always reports active. The sender is
    // dropped.
    let (_, status) = watch::channel(TxStatus::Active);
    let dialer = addr.src().clone().map(|src| *src);

    Ok(SimTx {
        src_addr: dialer,
        dst_addr: addr.addr().clone(),
        status,
        client: addr.client,
        _phantom: PhantomData,
    })
}

/// Serve a sim channel. Set up the right simulated sender and receivers
/// The mpsc tx will be used to dispatch messages when it's time while
/// the mpsc rx will be used by the above applications to handle received messages
/// like any other channel.
/// A sim address has a dst and optional src. Dispatchers are only indexed by dst address.
pub(crate) fn serve<M: RemoteMessage>(
    sim_addr: SimAddr,
) -> anyhow::Result<(ChannelAddr, SimRx<M>)> {
    let (tx, rx) = mpsc::channel::<Serialized>(SIM_LINK_BUF_SIZE);
    // Add tx to sender dispatch.
    SENDER.dispatchers.insert(*sim_addr.addr.clone(), tx);
    // Return the sender.
    Ok((
        ChannelAddr::Sim(sim_addr.clone()),
        SimRx {
            addr: *sim_addr.addr.clone(),
            rx,
            _phantom: PhantomData,
        },
    ))
}

#[async_trait]
impl<M: RemoteMessage> Rx<M> for SimRx<M> {
    async fn recv(&mut self) -> Result<M, ChannelError> {
        let data = self.rx.recv().await.ok_or(ChannelError::Closed)?;
        data.deserialized().map_err(ChannelError::from)
    }

    fn addr(&self) -> ChannelAddr {
        self.addr.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::iter::zip;

    use ndslice::extent;

    use super::*;
    use crate::PortId;
    use crate::attrs::Attrs;
    use crate::clock::Clock;
    use crate::clock::RealClock;
    use crate::clock::SimClock;
    use crate::id;
    use crate::simnet;
    use crate::simnet::BetaDistribution;
    use crate::simnet::LatencyConfig;
    use crate::simnet::LatencyDistribution;
    use crate::simnet::start;
    use crate::simnet::start_with_config;

    #[tokio::test]
    async fn test_sim_basic() {
        let dst_ok = vec!["tcp:[::1]:1234", "tcp:127.0.0.1:8080", "local:123"];
        let srcs_ok = vec!["tcp:[::2]:1234", "tcp:127.0.0.2:8080", "local:124"];

        start();
        let handle = simnet_handle().unwrap();

        // TODO: New NodeAdd event should do this for you..
        for addr in dst_ok.iter().chain(srcs_ok.iter()) {
            // Add to network along with its edges.
            simnet_handle()
                .unwrap()
                .bind(addr.parse::<ChannelAddr>().unwrap())
                .unwrap();
        }
        for (src_addr, dst_addr) in zip(srcs_ok, dst_ok) {
            let dst_addr = SimAddr::new_with_src(
                src_addr.parse::<ChannelAddr>().unwrap(),
                dst_addr.parse::<ChannelAddr>().unwrap(),
            )
            .unwrap();

            let (_, mut rx) = sim::serve::<MessageEnvelope>(dst_addr.clone()).unwrap();
            let tx = sim::dial::<MessageEnvelope>(dst_addr).unwrap();
            let data = Serialized::serialize(&456).unwrap();
            let sender = id!(world[0].hello);
            let dest = id!(world[1].hello);
            let ext = extent!(region = 1, dc = 1, rack = 4, host = 4, gpu = 8);
            handle.register_proc(
                sender.proc_id().clone(),
                ext.point(vec![0, 0, 0, 0, 0]).unwrap(),
            );
            handle.register_proc(
                dest.proc_id().clone(),
                ext.point(vec![0, 0, 0, 1, 0]).unwrap(),
            );

            let msg = MessageEnvelope::new(sender, PortId(dest, 0), data.clone(), Attrs::new());
            tx.try_post(msg, oneshot::channel().0).unwrap();
            assert_eq!(*rx.recv().await.unwrap().data(), data);
        }

        let records = sim::simnet_handle().unwrap().close().await.unwrap();
        eprintln!("records: {:#?}", records);
    }

    #[tokio::test]
    async fn test_invalid_sim_addr() {
        let src = "sim!src";
        let dst = "sim!dst";
        let sim_addr = format!("{},{}", src, dst);
        let result = parse(&sim_addr);
        assert!(matches!(result, Err(ChannelError::InvalidAddress(_))));
    }

    #[tokio::test]
    async fn test_parse_sim_addr() {
        let sim_addr = "sim!unix:@dst";
        let result = sim_addr.parse();
        assert!(result.is_ok());
        let ChannelAddr::Sim(sim_addr) = result.unwrap() else {
            panic!("Expected a sim address");
        };
        assert!(sim_addr.src().is_none());
        assert_eq!(sim_addr.addr().to_string(), "unix:@dst");

        let sim_addr = "sim!unix:@src,unix:@dst";
        let result = sim_addr.parse();
        assert!(result.is_ok());
        let ChannelAddr::Sim(sim_addr) = result.unwrap() else {
            panic!("Expected a sim address");
        };
        assert!(sim_addr.src().is_some());
        assert_eq!(sim_addr.addr().to_string(), "unix:@dst");
    }

    #[tokio::test]
    async fn test_realtime_frontier() {
        tokio::time::pause();
        // 1 second of latency
        start_with_config(LatencyConfig {
            inter_zone_distribution: LatencyDistribution::Beta(
                BetaDistribution::new(
                    tokio::time::Duration::from_millis(100),
                    tokio::time::Duration::from_millis(100),
                    1.0,
                    1.0,
                )
                .unwrap(),
            ),
            ..Default::default()
        });

        let sim_addr = SimAddr::new("unix:@dst".parse::<ChannelAddr>().unwrap()).unwrap();
        let sim_addr_with_src = SimAddr::new_with_src(
            "unix:@src".parse::<ChannelAddr>().unwrap(),
            "unix:@dst".parse::<ChannelAddr>().unwrap(),
        )
        .unwrap();
        let (_, mut rx) = sim::serve::<MessageEnvelope>(sim_addr.clone()).unwrap();
        let tx = sim::dial::<MessageEnvelope>(sim_addr_with_src).unwrap();

        let controller = id!(world[0].controller);
        let dest = id!(world[1].dest);
        let handle = simnet::simnet_handle().unwrap();

        let ext = extent!(region = 1, dc = 1, zone = 2, rack = 4, host = 4, gpu = 8);
        handle.register_proc(
            controller.proc_id().clone(),
            ext.point(vec![0, 0, 1, 0, 0, 0]).unwrap(),
        );
        handle.register_proc(
            dest.proc_id().clone(),
            ext.point(vec![0, 0, 0, 0, 0, 0]).unwrap(),
        );

        // This message will be delievered at simulator time = 100 seconds
        tx.try_post(
            MessageEnvelope::new(
                controller,
                PortId(dest, 0),
                Serialized::serialize(&456).unwrap(),
                Attrs::new(),
            ),
            oneshot::channel().0,
        )
        .unwrap();
        {
            // Allow simnet to run
            tokio::task::yield_now().await;
            // Messages have not been receive since 10 seconds have not elapsed
            assert!(rx.rx.try_recv().is_err());
        }
        // Advance "real" time by 100 seconds
        tokio::time::advance(tokio::time::Duration::from_secs(100)).await;
        {
            // Allow some time for simnet to run
            tokio::task::yield_now().await;
            // Messages are received
            assert!(rx.rx.try_recv().is_ok());
        }
    }

    #[tokio::test]
    async fn test_client_message_scheduled_realtime() {
        tokio::time::pause();
        // 1 second of latency
        start_with_config(LatencyConfig {
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
        });

        let controller_to_dst = SimAddr::new_with_src(
            "unix:@controller".parse::<ChannelAddr>().unwrap(),
            "unix:@dst".parse::<ChannelAddr>().unwrap(),
        )
        .unwrap();

        let controller_tx = sim::dial::<MessageEnvelope>(controller_to_dst.clone()).unwrap();

        let client_to_dst = SimAddr::new_with_client_src(
            "unix:@client".parse::<ChannelAddr>().unwrap(),
            "unix:@dst".parse::<ChannelAddr>().unwrap(),
        )
        .unwrap();
        let client_tx = sim::dial::<MessageEnvelope>(client_to_dst).unwrap();

        let controller = id!(world[0].controller);
        let dest = id!(world[1].dest);
        let client = id!(world[2].client);

        let handle = simnet::simnet_handle().unwrap();
        let ext = extent!(region = 1, dc = 1, zone = 2, rack = 4, host = 4, gpu = 8);
        handle.register_proc(
            controller.proc_id().clone(),
            ext.point(vec![0, 0, 1, 0, 0, 0]).unwrap(),
        );
        handle.register_proc(
            client.proc_id().clone(),
            ext.point(vec![0, 0, 0, 0, 0, 0]).unwrap(),
        );
        handle.register_proc(
            dest.proc_id().clone(),
            ext.point(vec![0, 0, 0, 0, 1, 0]).unwrap(),
        );

        assert_eq!(
            SimClock.duration_since_start(RealClock.now()),
            tokio::time::Duration::ZERO
        );
        // Fast forward real time to 5 seconds
        tokio::time::advance(tokio::time::Duration::from_secs(5)).await;
        {
            // Send client message
            client_tx
                .try_post(
                    MessageEnvelope::new(
                        client.clone(),
                        PortId(dest.clone(), 0),
                        Serialized::serialize(&456).unwrap(),
                        Attrs::new(),
                    ),
                    oneshot::channel().0,
                )
                .unwrap();
            // Send system message
            controller_tx
                .try_post(
                    MessageEnvelope::new(
                        controller.clone(),
                        PortId(dest.clone(), 0),
                        Serialized::serialize(&456).unwrap(),
                        Attrs::new(),
                    ),
                    oneshot::channel().0,
                )
                .unwrap();
            // Allow some time for simnet to run
            RealClock.sleep(tokio::time::Duration::from_secs(1)).await;
        }
        let recs = simnet::simnet_handle().unwrap().close().await.unwrap();
        assert_eq!(recs.len(), 2);
        let end_times = recs.iter().map(|rec| rec.end_at).collect::<Vec<_>>();
        // client message was delivered at "real" time = 5 seconds
        assert!(end_times.contains(&5000));
        // system message was delivered at simulated time = 1 second
        assert!(end_times.contains(&1000));
    }
}
