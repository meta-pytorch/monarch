/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::hint::black_box;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use criterion::criterion_group;
use criterion::criterion_main;
use futures::future::join_all;
use hyperactor::PortAddr;
use hyperactor::Proc;
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
use hyperactor::ordering::SeqKey;
use hyperactor::ordering::Sequencer;
use hyperactor::port::Port;
use hyperactor::testing::ids::test_actor_id;
use serde::Deserialize;
use serde::Serialize;
use serde_multipart::Part;
use tokio::runtime;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use typeuri::Named;

fn new_runtime() -> Runtime {
    runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn new_benchmark_sequencer() -> Sequencer {
    let runtime = new_runtime();
    let _guard = runtime.enter();
    let proc = Proc::direct(
        ChannelAddr::any(ChannelTransport::Local),
        "sequencer_bench".to_string(),
    )
    .unwrap();

    proc.client("sequencer_bench").sequencer().clone()
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

                    let (listen_addr, mut rx) = serve::<Message>(addr).unwrap();
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
                    let (listen_addr, mut rx) = serve::<Message>(addr).unwrap();
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
                            let receipt = tx.try_post(message.clone());

                            let handle = tokio::spawn(async move {
                                _ = tokio::time::timeout(Duration::from_millis(5000), receipt)
                                    .await
                                    .unwrap();
                            });

                            response_handlers.push(handle);

                            let delay_ms = 1000_u64.checked_div(rate).unwrap_or(0);
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
        channel::serve::<Message>(ChannelAddr::any(transport.clone())).unwrap();
    let (server_addr, mut server_rx) =
        channel::serve::<Message>(ChannelAddr::any(transport.clone())).unwrap();

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
    client_handle.await.unwrap().unwrap();
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
                let actor_id = test_actor_id("world_0", "actor");
                let mbox = Mailbox::new(actor_id);
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
                let actor_id = test_actor_id("world_0", "actor");
                let mbox = Mailbox::new(actor_id);
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

                        let delay_ms = 1000_u64.checked_div(rate).unwrap_or(0);
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

/// Compare single-key and batch sequence assignment across cast fanout sizes.
fn bench_cast_seq_assignment(c: &mut Criterion) {
    let mut group = c.benchmark_group("cast_seq_assignment");
    for n in [1usize, 8, 64, 512, 4096, 8192, 16384, 32768] {
        let ports: Vec<PortAddr> = (0..n)
            .map(|i| {
                test_actor_id(&format!("worker_{i}"), "worker").port_addr(Port::handler_id(0, None))
            })
            .collect();
        let keys: Vec<SeqKey> = ports
            .iter()
            .map(|port| SeqKey::for_handler(&port.actor_addr()))
            .collect();

        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(
            BenchmarkId::new("per_dest_assign_seq", n),
            &ports,
            |b, ports| {
                let seq = new_benchmark_sequencer();
                for p in ports {
                    black_box(seq.assign_seq(p));
                }
                b.iter(|| {
                    for p in ports {
                        black_box(seq.assign_seq(black_box(p)));
                    }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("batch_assign_seqs", n),
            &keys,
            |b, keys| {
                let seq = new_benchmark_sequencer();
                black_box(seq.assign_seqs(keys));
                b.iter(|| {
                    black_box(seq.assign_seqs(black_box(keys)));
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("batch_assign_seqs_divergent", n),
            &keys,
            |b, keys| {
                let seq = new_benchmark_sequencer();
                let advanced_keys: Vec<_> = keys.iter().step_by(2).cloned().collect();
                black_box(seq.assign_seqs(&advanced_keys));
                b.iter(|| {
                    black_box(seq.assign_seqs(black_box(keys)));
                });
            },
        );
    }
    group.finish();
}

const CONTENTION_FANOUT: usize = 4096;
const CONTENTION_WORKERS: usize = 4;
const MIXED_CONTENTION_FANOUTS: [usize; 5] = [1, 8, 64, 512, 4096];

#[derive(Clone, Copy)]
enum AssignmentMode {
    PerDestination,
    Batch,
}

impl AssignmentMode {
    fn assign(self, sequencer: &Sequencer, ports: &[PortAddr], keys: &[SeqKey]) {
        match self {
            Self::PerDestination => {
                for port in ports {
                    black_box(sequencer.assign_seq(black_box(port)));
                }
            }
            Self::Batch => {
                black_box(sequencer.assign_seqs(black_box(keys)));
            }
        }
    }
}

fn contention_inputs(worker_count: usize, fanout: usize) -> Vec<(Vec<PortAddr>, Vec<SeqKey>)> {
    (0..worker_count)
        .map(|worker| {
            let ports: Vec<_> = (0..fanout)
                .map(|rank| {
                    test_actor_id(&format!("worker_{worker}_{rank}"), "worker")
                        .port_addr(Port::handler_id(0, None))
                })
                .collect();
            let keys = ports
                .iter()
                .map(|port| SeqKey::for_handler(&port.actor_addr()))
                .collect();
            (ports, keys)
        })
        .collect()
}

fn run_concurrent_assignments(
    iterations: u64,
    inputs: &[(Vec<PortAddr>, Vec<SeqKey>)],
    mode: AssignmentMode,
) -> Duration {
    let sequencer = new_benchmark_sequencer();
    for (_, keys) in inputs {
        black_box(sequencer.assign_seqs(keys));
    }

    let barrier = Arc::new(Barrier::new(inputs.len() + 1));
    let mut start = None;
    thread::scope(|scope| {
        for (ports, keys) in inputs {
            let sequencer = sequencer.clone();
            let barrier = barrier.clone();
            scope.spawn(move || {
                barrier.wait();
                for _ in 0..iterations {
                    mode.assign(&sequencer, ports, keys);
                }
            });
        }

        start = Some(Instant::now());
        barrier.wait();
    });
    start
        .expect("timer must start before workers run")
        .elapsed()
}

fn run_mixed_assignments(
    iterations: u64,
    ports: &[PortAddr],
    keys: &[SeqKey],
    scalar_port: &PortAddr,
    mode: AssignmentMode,
) -> Duration {
    let sequencer = new_benchmark_sequencer();
    black_box(sequencer.assign_seqs(keys));
    black_box(sequencer.assign_seq(scalar_port));

    let started = AtomicBool::new(false);
    let stop = AtomicBool::new(false);
    let mut elapsed = None;
    thread::scope(|scope| {
        let background_sequencer = sequencer.clone();
        let background_started = &started;
        let background_stop = &stop;
        scope.spawn(move || {
            mode.assign(&background_sequencer, ports, keys);
            background_started.store(true, Ordering::Release);
            while !background_stop.load(Ordering::Acquire) {
                mode.assign(&background_sequencer, ports, keys);
            }
        });

        while !started.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }

        let start = Instant::now();
        for _ in 0..iterations {
            black_box(sequencer.assign_seq(black_box(scalar_port)));
        }
        elapsed = Some(start.elapsed());
        stop.store(true, Ordering::Release);
    });
    elapsed.expect("scalar assignments must complete")
}

/// Compare aggregate sequence-assignment throughput from concurrent casts.
fn bench_cast_seq_concurrent_contention(c: &mut Criterion) {
    let inputs = contention_inputs(CONTENTION_WORKERS, CONTENTION_FANOUT);
    let mut group = c.benchmark_group("cast_seq_concurrent_contention");
    group.throughput(Throughput::Elements(
        (CONTENTION_WORKERS * CONTENTION_FANOUT) as u64,
    ));

    for (name, mode) in [
        ("per_dest_assign_seq", AssignmentMode::PerDestination),
        ("batch_assign_seqs", AssignmentMode::Batch),
    ] {
        group.bench_function(
            BenchmarkId::new(name, format!("{CONTENTION_WORKERS}x{CONTENTION_FANOUT}")),
            |b| b.iter_custom(|iterations| run_concurrent_assignments(iterations, &inputs, mode)),
        );
    }
    group.finish();
}

/// Compare scalar assignment latency while a concurrent cast uses the sequencer.
fn bench_cast_seq_mixed_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("cast_seq_mixed_contention");

    for fanout in MIXED_CONTENTION_FANOUTS {
        let mut inputs = contention_inputs(1, fanout);
        let (ports, keys) = inputs.pop().expect("one contention input must exist");
        let scalar_port =
            test_actor_id("scalar_worker", "worker").port_addr(Port::handler_id(0, None));

        for (name, mode) in [
            ("per_dest_cast", AssignmentMode::PerDestination),
            ("batch_cast", AssignmentMode::Batch),
        ] {
            group.bench_function(BenchmarkId::new(name, fanout), |b| {
                b.iter_custom(|iterations| {
                    run_mixed_assignments(iterations, &ports, &keys, &scalar_port, mode)
                })
            });
        }
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().without_plots();
    targets = bench_message_sizes,
    bench_message_rates,
    bench_mailbox_message_sizes,
    bench_mailbox_message_rates,
    bench_channel_ping_pong,
    bench_cast_seq_assignment,
    bench_cast_seq_concurrent_contention,
    bench_cast_seq_mixed_contention,
}

criterion_main!(benches);
