/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::time::Instant;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use criterion::criterion_group;
use criterion::criterion_main;
use hyperactor::context::Mailbox;
use hyperactor_mesh::ActorMesh;
use hyperactor_mesh::global_root_client;
use hyperactor_mesh::test_utils;
use ndslice::extent;
use ndslice::view::Ranked as _;
use tokio::time::Duration;

mod bench_actor;
use bench_actor::BenchActor;
use bench_actor::BenchMessage;
use tokio::runtime::Runtime;

/// Single process-wide Runtime shared across all benchmark functions.
///
/// `global_root_client()` is a `OnceLock` singleton whose background
/// tasks (mailbox server, actor run loop) are spawned on whatever
/// tokio Runtime is active at first call. If each `bench_function`
/// creates its own `Runtime::new()`, the singleton's tasks die when
/// the first Runtime is dropped, causing intermittent spawn timeouts
/// in subsequent benchmarks. Sharing one Runtime avoids this.
fn shared_runtime() -> &'static Runtime {
    static RT: std::sync::OnceLock<Runtime> = std::sync::OnceLock::new();
    RT.get_or_init(|| Runtime::new().unwrap())
}

// Benchmark how long does it take to process 1KB message on 1, 10, 100, 1K hosts with 8 GPUs each
fn bench_actor_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("actor_scaling");
    let host_counts = vec![1, 10, 100];
    let gpus = 1;
    let message_size = 1024; // Fixed message size (1KB)

    for host_count in host_counts {
        group.bench_function(BenchmarkId::from_parameter(host_count), |b| {
            let mut b = b.to_async(shared_runtime());
            b.iter_custom(|iters| async move {
                let instance = global_root_client();
                let mut host_mesh = test_utils::local_host_mesh(host_count).await;
                let mut proc_mesh = host_mesh
                    .spawn(instance, "bench", extent!(gpus = gpus))
                    .await
                    .unwrap();
                let actor_mesh: ActorMesh<BenchActor> = proc_mesh
                    .spawn(instance, "bench", &(Duration::from_millis(0)))
                    .await
                    .unwrap();
                let num_actors = actor_mesh.region().num_ranks();

                let start = Instant::now();
                for i in 0..iters {
                    let (tx, mut rx) = instance.mailbox().open_port();
                    let payload = vec![0u8; message_size];

                    actor_mesh
                        .cast(
                            instance,
                            BenchMessage {
                                step: i as usize,
                                reply: tx.bind(),
                                payload,
                            },
                        )
                        .unwrap();

                    let mut msg_rcv = 0;
                    while msg_rcv < num_actors {
                        #[allow(clippy::disallowed_methods)]
                        let _ = tokio::time::timeout(Duration::from_secs(10), rx.recv())
                            .await
                            .unwrap();

                        msg_rcv += 1;
                    }
                }

                let elapsed = start.elapsed();
                println!("Elapsed: {:?} on iters {}", elapsed, iters);
                proc_mesh
                    .stop(instance, "benchmark complete".to_string())
                    .await
                    .expect("Failed to stop mesh");
                let _ = host_mesh.shutdown(instance).await;
                elapsed
            })
        });
    }

    group.finish();
}

fn format_size(size: usize) -> String {
    if size >= 1_000_000_000 {
        format!("{}GB", size / 1_000_000_000)
    } else if size >= 1_000_000 {
        format!("{}MB", size / 1_000_000)
    } else if size >= 1_000 {
        format!("{}KB", size / 1_000)
    } else {
        format!("{}B", size)
    }
}

// Benchmark how long it takes to send a message of size X to an actor mesh of 10 actors
fn bench_actor_mesh_message_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("actor_mesh_message_sizes");
    group.sample_size(10);
    let actor_counts = vec![1, 10];
    let message_sizes: Vec<usize> = vec![
        10_000,
        100_000,
        1_000_000,
        10_000_000,
        100_000_000,
        1_000_000_000,
    ];

    for message_size in message_sizes {
        for &actor_count in &actor_counts {
            group.throughput(Throughput::Bytes((message_size * actor_count) as u64));
            group.sampling_mode(criterion::SamplingMode::Flat);
            group.sample_size(10);
            group.bench_function(
                format!("actors/{}/size/{}", actor_count, format_size(message_size)),
                |b| {
                    let mut b = b.to_async(shared_runtime());
                    b.iter_custom(|iters| async move {
                        let instance = global_root_client();
                        let mut host_mesh = test_utils::local_host_mesh(1).await;
                        let mut proc_mesh = host_mesh
                            .spawn(instance, "bench", extent!(gpus = actor_count))
                            .await
                            .unwrap();
                        let actor_mesh: ActorMesh<BenchActor> = proc_mesh
                            .spawn(instance, "bench", &(Duration::from_millis(0)))
                            .await
                            .unwrap();

                        let num_actors = actor_mesh.region().num_ranks();

                        // Scale timeout with payload size: 30s base + 10s per 100MB.
                        let recv_timeout =
                            Duration::from_secs(30 + (message_size as u64 / 100_000_000) * 10);

                        let start = Instant::now();
                        for i in 0..iters {
                            let (tx, mut rx) = instance.mailbox().open_port();
                            let payload = vec![0u8; message_size];

                            actor_mesh
                                .cast(
                                    instance,
                                    BenchMessage {
                                        step: i as usize,
                                        reply: tx.bind(),
                                        payload,
                                    },
                                )
                                .unwrap();

                            let mut msg_rcv = 0;
                            while msg_rcv < num_actors {
                                #[allow(clippy::disallowed_methods)]
                                let _ =
                                    tokio::time::timeout(recv_timeout, rx.recv()).await.unwrap();
                                msg_rcv += 1;
                            }
                        }
                        let elapsed = start.elapsed();
                        proc_mesh
                            .stop(instance, "benchmark complete".to_string())
                            .await
                            .expect("Failed to stop mesh");
                        let _ = host_mesh.shutdown(instance).await;
                        elapsed
                    });
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_actor_scaling, bench_actor_mesh_message_sizes);
criterion_main!(benches);
