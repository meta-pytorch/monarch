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
use hyperactor::channel::ChannelTransport;
use hyperactor::context::Mailbox;
use hyperactor_mesh::ActorMesh;
use hyperactor_mesh::ProcMesh;
use hyperactor_mesh::alloc::AllocSpec;
use hyperactor_mesh::alloc::Allocator;
use hyperactor_mesh::alloc::LocalAllocator;
use hyperactor_mesh::global_root_client;
use ndslice::extent;
use ndslice::View;
use tokio::time::Duration;

mod bench_actor;
use bench_actor::BenchActor;
use bench_actor::BenchMessage;
use tokio::runtime::Runtime;

// Benchmark how long does it take to process 1KB message on 1, 10, 100, 1K hosts with 8 GPUs each
fn bench_actor_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("actor_scaling");
    let host_counts = vec![1, 10, 100];
    let gpus = 1;
    let message_size = 1024; // Fixed message size (1KB)

    for host_count in host_counts {
        group.bench_function(BenchmarkId::from_parameter(host_count), |b| {
            let mut b = b.to_async(Runtime::new().unwrap());
            b.iter_custom(|iters| async move {
                let alloc = LocalAllocator
                    .allocate(AllocSpec {
                        extent: extent!(hosts = host_count, gpus = gpus),
                        constraints: Default::default(),
                        proc_name: None,
                        transport: ChannelTransport::Local,
                        proc_allocation_mode: Default::default(),
                    })
                    .await
                    .unwrap();

                let instance = global_root_client();
                let mut proc_mesh = ProcMesh::allocate(instance, Box::new(alloc), "bench")
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
                    let mut b = b.to_async(Runtime::new().unwrap());
                    b.iter_custom(|iters| async move {
                        let alloc = LocalAllocator
                            .allocate(AllocSpec {
                                extent: extent!(gpus = actor_count),
                                constraints: Default::default(),
                                proc_name: None,
                                transport: ChannelTransport::Local,
                                proc_allocation_mode: Default::default(),
                            })
                            .await
                            .unwrap();

                        let instance = global_root_client();
                        let mut proc_mesh = ProcMesh::allocate(instance, Box::new(alloc), "bench")
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
                        proc_mesh
                            .stop(instance, "benchmark complete".to_string())
                            .await
                            .expect("Failed to stop mesh");
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
