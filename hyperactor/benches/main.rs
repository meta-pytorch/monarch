/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![allow(clippy::disallowed_methods)] // tokio::time::sleep

use std::time::Duration;
use std::time::Instant;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use criterion::criterion_group;
use criterion::criterion_main;
use futures::future::join_all;
use hyperactor::Named;
use hyperactor::channel;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::ChannelTransport;
use hyperactor::channel::Rx;
use hyperactor::channel::TcpMode;
use hyperactor::channel::Tx;
use hyperactor::channel::dial;
use hyperactor::channel::serve;
use hyperactor::mailbox::Mailbox;
use hyperactor::mailbox::PortSender;
use hyperactor::mailbox::monitored_return_handle;
use hyperactor::reference::ActorId;
use hyperactor::reference::ProcId;
use hyperactor::reference::WorldId;
use serde::Deserialize;
use serde::Serialize;
use serde_multipart::Part;
use tokio::runtime;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;

fn new_runtime() -> Runtime {
    runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

#[derive(Debug, Clone, Serialize, Deserialize, Named, PartialEq)]
struct Message {
    id: u64,
    #[serde(with = "serde_bytes")]
    payload: Vec<u8>,
}

impl Message {
    fn new(id: u64, size: usize) -> Self {
        Self {
            id,
            payload: vec![0; size],
        }
    }
}

// CHANNEL
// Benchmark message sizes
fn bench_message_sizes(c: &mut Criterion) {
    let transports = vec![
        ("local", ChannelTransport::Local),
        ("tcp", ChannelTransport::Tcp(TcpMode::Hostname)),
        ("unix", ChannelTransport::Unix),
    ];

    for (transport_name, transport) in &transports {
        for size in [10_000, 1_000_000_000] {
            let mut group = c.benchmark_group(format!("send_receive/{}", transport_name));
            let transport = transport.clone();
            group.throughput(Throughput::Bytes(size as u64));
            group.sampling_mode(criterion::SamplingMode::Flat);
            group.sample_size(10);
            group.bench_function(BenchmarkId::from_parameter(size), move |b| {
                let mut b = b.to_async(new_runtime());
                let tt = &transport;
                b.iter_custom(|iters| async move {
                    let addr = ChannelAddr::any(tt.clone());
                    if let ChannelAddr::Tcp(socket_addr) = addr {
                        assert!(!socket_addr.ip().is_loopback());
                    }

                    let (listen_addr, mut rx) = serve::<Message>(addr, "bench").unwrap();
                    let tx = dial::<Message>(listen_addr).unwrap();
                    let msg = Message::new(0, size);
                    let start = Instant::now();
                    for _ in 0..iters {
                        tx.post(msg.clone());
                        rx.recv().await.unwrap();
                    }
                    start.elapsed()
                });
            });
            group.finish();
        }
    }
}

// Benchmark message rates with a single client
fn bench_message_rates(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_rates");

    let transports = vec![
        ("local", ChannelTransport::Local),
        ("tcp", ChannelTransport::Tcp(TcpMode::Hostname)),
        ("unix", ChannelTransport::Unix),
        //TODO Add TLS once it is able to run in Sandcastle
    ];

    let rates = vec![100, 5000];

    let payload_size = 1024; // 1KB payload

    for rate in &rates {
        for (transport_name, transport) in &transports {
            let rate = *rate;

            group.bench_function(format!("rate_{}_{}mps", transport_name, rate), move |b| {
                let mut b = b.to_async(new_runtime());
                b.iter_custom(|iters| async move {
                    let total_msgs = iters * rate;
                    let addr = ChannelAddr::any(transport.clone());
                    let (listen_addr, mut rx) = serve::<Message>(addr, "bench").unwrap();
                    tokio::spawn(async move {
                        let mut received_count = 0;

                        while received_count < total_msgs {
                            match rx.recv().await {
                                Ok(_) => received_count += 1,
                                Err(e) => {
                                    panic!("Error receiving message: {}", e);
                                }
                            }
                        }
                    });

                    let tx = dial::<Message>(listen_addr).unwrap();
                    let message = Message::new(0, payload_size);
                    let start = Instant::now();

                    for _ in 0..iters {
                        let mut response_handlers: Vec<tokio::task::JoinHandle<()>> =
                            Vec::with_capacity(rate as usize);
                        for _ in 0..rate {
                            let (return_sender, return_receiver) = oneshot::channel();
                            if let Err(e) = tx.try_post(message.clone(), return_sender) {
                                panic!("Failed to send message: {:?}", e);
                            }

                            let handle = tokio::spawn(async move {
                                _ = tokio::time::timeout(
                                    Duration::from_millis(5000),
                                    return_receiver,
                                )
                                .await
                                .unwrap();
                            });

                            response_handlers.push(handle);

                            let delay_ms = if rate > 0 { 1000 / rate } else { 0 };
                            let elapsed = start.elapsed().as_millis();
                            let effective_delay = (delay_ms as u128).saturating_sub(elapsed);
                            if effective_delay > 0 {
                                tokio::time::sleep(Duration::from_millis(effective_delay as u64))
                                    .await;
                            }
                        }
                        join_all(response_handlers).await;
                    }

                    start.elapsed()
                });
            });
        }
    }

    group.finish();
}

// Try to replicate https://www.internalfb.com/phabricator/paste/view/P1903314366
fn bench_channel_ping_pong(c: &mut Criterion) {
    let transport = ChannelTransport::Unix;

    for size in [1usize, 1_000_000usize] {
        let mut group = c.benchmark_group("channel_ping_pong".to_string());
        let transport = transport.clone();
        group.throughput(Throughput::Bytes((size * 2) as u64)); // send and receive
        group.sampling_mode(criterion::SamplingMode::Flat);
        group.sample_size(100);
        group.bench_function(BenchmarkId::from_parameter(size), move |b| {
            let mut b = b.to_async(new_runtime());
            b.iter_custom(|iters| channel_ping_pong(transport.clone(), size, iters as usize));
        });
        group.finish();
    }
}

async fn channel_ping_pong(
    transport: ChannelTransport,
    message_size: usize,
    num_iter: usize,
) -> Duration {
    #[derive(Clone, Debug, Named, Serialize, Deserialize)]
    struct Message(Part);

    let (client_addr, mut client_rx) =
        channel::serve::<Message>(ChannelAddr::any(transport.clone()), "ping_pong_client").unwrap();
    let (server_addr, mut server_rx) =
        channel::serve::<Message>(ChannelAddr::any(transport.clone()), "ping_pong_server").unwrap();

    let _server_handle: tokio::task::JoinHandle<Result<(), anyhow::Error>> =
        tokio::spawn(async move {
            let client_tx = channel::dial(client_addr)?;
            loop {
                let message = server_rx.recv().await?;
                client_tx.post(message);
            }
        });

    let client_handle: tokio::task::JoinHandle<Result<(), anyhow::Error>> =
        tokio::spawn(async move {
            let server_tx = channel::dial(server_addr)?;
            let message = Message(Part::from(vec![0u8; message_size]));
            for _ in 0..num_iter {
                server_tx.post(message.clone() /*cheap */);
                client_rx.recv().await?;
            }
            Ok(())
        });

    let start = Instant::now();
    let _ = client_handle.await.unwrap().unwrap();
    start.elapsed()
}

// MAILBOX

fn bench_mailbox_message_sizes(c: &mut Criterion) {
    let sizes: Vec<usize> = vec![10_000, 1_000_000_000];

    for size in sizes {
        let mut group = c.benchmark_group("mailbox_send_receive".to_string());
        group.throughput(Throughput::Bytes(size as u64));
        group.sampling_mode(criterion::SamplingMode::Flat);
        group.sample_size(10);
        group.bench_function(BenchmarkId::from_parameter(size), move |b| {
            let mut b = b.to_async(Runtime::new().unwrap());
            b.iter_custom(|iters| async move {
                let proc_id = ProcId::Ranked(WorldId("world".to_string()), 0);
                let actor_id = ActorId(proc_id, "actor".to_string(), 0);
                let mbox = Mailbox::new_detached(actor_id);
                let (port, mut receiver) = mbox.open_port::<Message>();
                let port = port.bind();

                let msg = Message::new(0, size);
                let start = Instant::now();
                for _ in 0..iters {
                    mbox.serialize_and_send(&port, msg.clone(), monitored_return_handle())
                        .unwrap();
                    receiver.recv().await.unwrap();
                }
                start.elapsed()
            });
        });
        group.finish();
    }
}

// Benchmark message rates for mailbox
fn bench_mailbox_message_rates(c: &mut Criterion) {
    let mut group = c.benchmark_group("mailbox_message_rates");
    let rates = vec![100, 5000];
    let payload_size = 1024; // 1KB payload

    for rate in &rates {
        let rate = *rate;
        group.bench_function(format!("rate_{}mps", rate), move |b| {
            let mut b = b.to_async(Runtime::new().unwrap());
            b.iter_custom(|iters| async move {
                let proc_id = ProcId::Ranked(WorldId("world".to_string()), 0);
                let actor_id = ActorId(proc_id, "actor".to_string(), 0);
                let mbox = Mailbox::new_detached(actor_id);
                let (port, mut receiver) = mbox.open_port::<Message>();
                let port = port.bind();

                // Spawn a task to receive messages
                let total_msgs = iters * rate;
                let receiver_task = tokio::spawn(async move {
                    let mut received_count = 0;
                    while received_count < total_msgs {
                        match receiver.recv().await {
                            Ok(_) => received_count += 1,
                            Err(e) => {
                                panic!("Error receiving message: {}", e);
                            }
                        }
                    }
                });

                let message = Message::new(0, payload_size);
                let start = Instant::now();

                for _ in 0..iters {
                    let mut response_handlers: Vec<tokio::task::JoinHandle<()>> =
                        Vec::with_capacity(rate as usize);

                    for _ in 0..rate {
                        let (return_sender, return_receiver) = oneshot::channel();
                        let msg_clone = message.clone();
                        let port_clone = port.clone();
                        let mbox_clone = mbox.clone();

                        let handle = tokio::spawn(async move {
                            mbox_clone
                                .serialize_and_send(
                                    &port_clone,
                                    msg_clone,
                                    monitored_return_handle(),
                                )
                                .unwrap();
                            let _ = return_sender.send(());

                            let _ =
                                tokio::time::timeout(Duration::from_millis(5000), return_receiver)
                                    .await
                                    .expect("Timed out waiting for return message");
                        });

                        response_handlers.push(handle);

                        let delay_ms = if rate > 0 { 1000 / rate } else { 0 };
                        let elapsed = start.elapsed().as_millis();
                        let effective_delay = (delay_ms as u128).saturating_sub(elapsed);
                        if effective_delay > 0 {
                            tokio::time::sleep(Duration::from_millis(effective_delay as u64)).await;
                        }
                    }
                    join_all(response_handlers).await;
                }

                receiver_task.await.unwrap();
                start.elapsed()
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_message_sizes,
    bench_message_rates,
    bench_mailbox_message_sizes,
    bench_mailbox_message_rates,
    bench_channel_ping_pong,
);

criterion_main!(benches);
