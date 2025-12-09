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
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

fn bench_thread_to_task(c: &mut Criterion) {
    let mut group = c.benchmark_group("mpsc_thread_to_task");

    for buffer_size in [1, 10, 1000, 100_000].iter() {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("buffer_size", buffer_size),
            buffer_size,
            |b, &buffer_size| {
                b.iter_custom(|iters| {
                    let rt = Runtime::new().unwrap();
                    let (tx, mut rx) = mpsc::channel::<Instant>(buffer_size);

                    let total_latency = rt.spawn(async move {
                        let mut total_latency = std::time::Duration::ZERO;

                        while let Some(sent_at) = rx.recv().await {
                            total_latency += sent_at.elapsed();
                        }

                        total_latency
                    });

                    for _ in 0..iters {
                        tx.blocking_send(Instant::now()).unwrap();
                    }
                    drop(tx);

                    rt.block_on(total_latency).unwrap()
                });
            },
        );
    }

    group.finish();
}

fn bench_thread_to_task_to_thread(c: &mut Criterion) {
    let mut group = c.benchmark_group("mpsc_thread_to_task_to_thread");

    for buffer_size in [10, 100, 1000, 100_000].iter() {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("buffer_size", buffer_size),
            buffer_size,
            |b, &buffer_size| {
                b.iter_custom(|iters| {
                    let rt = Runtime::new().unwrap();
                    let (tx_req, mut rx_req) = mpsc::channel::<Instant>(buffer_size);
                    let (tx_resp, mut rx_resp) = mpsc::channel::<Instant>(buffer_size);

                    rt.spawn(async move {
                        while let Some(sent_at) = rx_req.recv().await {
                            let _ = tx_resp.send(sent_at).await;
                        }
                    });

                    let mut total_duration = std::time::Duration::ZERO;

                    for _ in 0..iters {
                        tx_req.blocking_send(Instant::now()).unwrap();
                        total_duration += rx_resp.blocking_recv().unwrap().elapsed();
                    }

                    total_duration
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_thread_to_task,
    bench_thread_to_task_to_thread,
);
criterion_main!(benches);
