/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fmt::Display;
use std::time::Duration;

use hyperactor::ActorRef;
use hyperactor_mesh::Mesh;
use hyperactor_mesh::ProcMesh;
use hyperactor_mesh::alloc::AllocSpec;
use hyperactor_mesh::alloc::Allocator;
use hyperactor_mesh::alloc::ProcessAllocator;
use monarch_rdma::IbverbsConfig;
use monarch_rdma::RdmaDevice;
use monarch_rdma::RdmaManagerActor;
use monarch_rdma::bench_utils::BenchmarkConfig;
use monarch_rdma::bench_utils::RdmaBufferBenchmarkActor;
use monarch_rdma::bench_utils::RdmaBufferBenchmarkMessageClient;
use ndslice::extent;
use tokio::process::Command;

#[derive(Debug, Clone)]
pub struct BenchmarkStatistics {
    pub total_operations: usize,
    pub avg_latency_ms: f64,
    pub min_latency_ms: f64,
    pub max_latency_ms: f64,
    pub p50_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub throughput_mb_s: f64,
    pub buffer_size_mb: f64,
}

impl Display for BenchmarkStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "=== Benchmark Statistics ===")?;
        writeln!(f, "Total operations: {}", self.total_operations)?;
        writeln!(f, "Buffer size: {:.2} MB", self.buffer_size_mb)?;
        writeln!(f, "Average latency: {:.3} ms", self.avg_latency_ms)?;
        writeln!(f, "Min latency: {:.3} ms", self.min_latency_ms)?;
        writeln!(f, "Max latency: {:.3} ms", self.max_latency_ms)?;
        writeln!(f, "P50 latency: {:.3} ms", self.p50_latency_ms)?;
        writeln!(f, "P95 latency: {:.3} ms", self.p95_latency_ms)?;
        writeln!(f, "P99 latency: {:.3} ms", self.p99_latency_ms)?;
        writeln!(f, "Throughput: {:.3} MB/s", self.throughput_mb_s)?;
        Ok(())
    }
}

/// Calculate percentile from sorted latencies
fn calculate_percentile(sorted_latencies: &[Duration], percentile: f64) -> f64 {
    if sorted_latencies.is_empty() {
        return 0.0;
    }
    let index = ((percentile / 100.0) * (sorted_latencies.len() - 1) as f64).round() as usize;
    let index = index.min(sorted_latencies.len() - 1);
    sorted_latencies[index].as_millis() as f64
}

/// Calculate benchmark statistics from latencies and config
fn calculate_statistics(latencies: Vec<Duration>, config: &BenchmarkConfig) -> BenchmarkStatistics {
    if latencies.is_empty() {
        return BenchmarkStatistics {
            total_operations: 0,
            avg_latency_ms: 0.0,
            min_latency_ms: 0.0,
            max_latency_ms: 0.0,
            p50_latency_ms: 0.0,
            p95_latency_ms: 0.0,
            p99_latency_ms: 0.0,
            throughput_mb_s: 0.0,
            buffer_size_mb: 0.0,
        };
    }

    let mut sorted_latencies = latencies.clone();
    sorted_latencies.sort();

    let total_operations = latencies.len();
    let avg_latency_ms =
        latencies.iter().map(|d| d.as_millis() as f64).sum::<f64>() / total_operations as f64;
    let min_latency_ms = sorted_latencies.first().unwrap().as_millis() as f64;
    let max_latency_ms = sorted_latencies.last().unwrap().as_millis() as f64;
    let p50_latency_ms = calculate_percentile(&sorted_latencies, 50.0);
    let p95_latency_ms = calculate_percentile(&sorted_latencies, 95.0);
    let p99_latency_ms = calculate_percentile(&sorted_latencies, 99.0);

    let buffer_size_mb = config.buffer_size as f64 / (1024.0 * 1024.0);
    let avg_latency_sec = avg_latency_ms / 1000.0;
    let throughput_mb_s = buffer_size_mb / avg_latency_sec;

    BenchmarkStatistics {
        total_operations,
        avg_latency_ms,
        min_latency_ms,
        max_latency_ms,
        p50_latency_ms,
        p95_latency_ms,
        p99_latency_ms,
        throughput_mb_s,
        buffer_size_mb,
    }
}

/// Run warmup iterations to stabilize performance
async fn run_warmup(
    bench_actor: &ActorRef<RdmaBufferBenchmarkActor>,
    client: &hyperactor::Mailbox,
    config: &BenchmarkConfig,
    warmup_iterations: usize,
) -> anyhow::Result<()> {
    println!("Running warmup with {} iterations...", warmup_iterations);

    let warmup_config = BenchmarkConfig {
        buffer_size: config.buffer_size,
        num_buffers: config.num_buffers.min(10), // Use fewer buffers for warmup
        num_iterations: warmup_iterations,
    };

    let start_time = std::time::Instant::now();
    let result = bench_actor.do_benchmark(client, warmup_config).await?;
    let warmup_duration = start_time.elapsed();

    println!(
        "Warmup completed: {} operations in {:.2}s (avg: {:.3}ms per operation)",
        result.latencies.len(),
        warmup_duration.as_secs_f64(),
        result
            .latencies
            .iter()
            .map(|d| d.as_millis() as f64)
            .sum::<f64>()
            / result.latencies.len() as f64
    );

    Ok(())
}

/// Run benchmark and return statistics
async fn run_benchmark_and_get_statistics(
    bench_actor: &ActorRef<RdmaBufferBenchmarkActor>,
    client: &hyperactor::Mailbox,
    config: BenchmarkConfig,
) -> anyhow::Result<BenchmarkStatistics> {
    let result = bench_actor.do_benchmark(client, config.clone()).await?;
    let statistics = calculate_statistics(result.latencies, &config);
    Ok(statistics)
}

/// Run complete benchmark with warmup
async fn run_complete_benchmark(
    bench_actor: &ActorRef<RdmaBufferBenchmarkActor>,
    client: &hyperactor::Mailbox,
    config: BenchmarkConfig,
    warmup_iterations: Option<usize>,
) -> anyhow::Result<BenchmarkStatistics> {
    // Run warmup if requested
    if let Some(warmup_iters) = warmup_iterations {
        run_warmup(bench_actor, client, &config, warmup_iters).await?;
        println!("Starting actual benchmark...\n");
    }

    // Run the actual benchmark
    run_benchmark_and_get_statistics(bench_actor, client, config).await
}

/// Format buffer size in human readable format
fn format_buffer_size(size_bytes: usize) -> String {
    if size_bytes >= 1024 * 1024 {
        format!("{}M", size_bytes / (1024 * 1024))
    } else if size_bytes >= 1024 {
        format!("{}K", size_bytes / 1024)
    } else {
        format!("{}B", size_bytes)
    }
}

/// Print table header
fn print_table_header() {
    println!("{:-<120}", "");
    println!(
        "| {:>8} | {:>8} | {:>10} | {:>8} | {:>8} | {:>8} | {:>8} | {:>8} | {:>12} |",
        "Size",
        "Ops",
        "Avg (ms)",
        "Min (ms)",
        "Max (ms)",
        "P50 (ms)",
        "P95 (ms)",
        "P99 (ms)",
        "Throughput"
    );
    println!(
        "| {:>8} | {:>8} | {:>10} | {:>8} | {:>8} | {:>8} | {:>8} | {:>8} | {:>12} |",
        "", "", "", "", "", "", "", "", "(MB/s)"
    );
    println!("{:-<120}", "");
}

/// Print table row for benchmark statistics
fn print_table_row(stats: &BenchmarkStatistics) {
    println!(
        "| {:>8} | {:>8} | {:>10.3} | {:>8.3} | {:>8.3} | {:>8.3} | {:>8.3} | {:>8.3} | {:>12.2} |",
        format_buffer_size(stats.buffer_size_mb as usize * 1024 * 1024),
        stats.total_operations,
        stats.avg_latency_ms,
        stats.min_latency_ms,
        stats.max_latency_ms,
        stats.p50_latency_ms,
        stats.p95_latency_ms,
        stats.p99_latency_ms,
        stats.throughput_mb_s
    );
}

/// Run multi-size benchmark suite and collect all results
async fn run_multi_size_benchmark(
    bench_actor: &ActorRef<RdmaBufferBenchmarkActor>,
    client: &hyperactor::Mailbox,
    buffer_sizes: &[usize],
    num_buffers: usize,
    num_iterations: usize,
    warmup_iterations: Option<usize>,
) -> anyhow::Result<()> {
    println!("🚀 Starting RDMA Buffer Size Benchmark Suite");
    println!(
        "📊 Testing {} buffer sizes with {} buffers and {} iterations each",
        buffer_sizes.len(),
        num_buffers,
        num_iterations
    );
    if let Some(warmup) = warmup_iterations {
        println!("🔥 Using {} warmup iterations per size", warmup);
    }
    println!();

    // Collect all results first
    let mut results = Vec::new();

    for (i, &buffer_size) in buffer_sizes.iter().enumerate() {
        println!(
            "⏳ Testing buffer size: {} ({}/{})...",
            format_buffer_size(buffer_size),
            i + 1,
            buffer_sizes.len()
        );

        let config = BenchmarkConfig {
            buffer_size,
            num_buffers,
            num_iterations,
        };

        let stats = run_complete_benchmark(bench_actor, client, config, warmup_iterations).await?;

        println!(
            "✅ Completed {} - Avg: {:.3}ms, Throughput: {:.2} MB/s",
            format_buffer_size(buffer_size),
            stats.avg_latency_ms,
            stats.throughput_mb_s
        );

        results.push(stats);
    }

    println!();
    println!("📊 All benchmarks completed! Printing results...");
    println!();

    // Print the complete results table
    print_results_table(&results);

    Ok(())
}

/// Print the complete results table
fn print_results_table(results: &[BenchmarkStatistics]) {
    print_table_header();

    for stats in results {
        print_table_row(stats);
    }

    println!("{:-<120}", "");

    // Print summary statistics
    if !results.is_empty() {
        println!("📈 Summary:");
        println!(
            "   ✅ Successful tests: {}/{}",
            results.len(),
            results.len()
        );

        let avg_throughput =
            results.iter().map(|s| s.throughput_mb_s).sum::<f64>() / results.len() as f64;

        let min_latency = results
            .iter()
            .map(|s| s.avg_latency_ms)
            .fold(f64::INFINITY, f64::min);

        let max_latency = results.iter().map(|s| s.avg_latency_ms).fold(0.0, f64::max);

        println!("   📊 Average throughput: {:.2} MB/s", avg_throughput);
        println!("   ⚡ Fastest average latency: {:.3} ms", min_latency);
        println!("   🐌 Slowest average latency: {:.3} ms", max_latency);
    }

    println!("✅ Benchmark suite completed!");
}

fn create_ibv_config() -> IbverbsConfig {
    IbverbsConfig {
        device: RdmaDevice::default(),
        cq_entries: 32,
        port_num: 1,
        gid_index: 0, // Changed from 3 to 0
        max_send_wr: 16,
        max_recv_wr: 16,
        max_send_sge: 4,
        max_recv_sge: 4,
        path_mtu: rdmaxcel_sys::IBV_MTU_1024,
        retry_cnt: 7,
        rnr_retry: 7,
        qp_timeout: 14,
        min_rnr_timer: 12,
        max_dest_rd_atomic: 1,
        max_rd_atomic: 1,
        pkey_index: 0,
        psn: rand::random::<u32>() & 0xffffff,
        use_gpu_direct: false, // Disable GPU direct for local testing
    }
}

async fn run() -> anyhow::Result<()> {
    let devices = monarch_rdma::get_all_devices();
    let ibv_config_1 = create_ibv_config();
    let ibv_config_2 = IbverbsConfig {
        device: devices.clone().into_iter().nth(3).unwrap(),
        ..create_ibv_config()
    };

    let mut alloc = ProcessAllocator::new(Command::new(
        buck_resources::get("monarch/monarch_rdma/bench/bootstrap").unwrap(),
    ));

    let proc_mesh_1 = ProcMesh::allocate(
        alloc
            .allocate(AllocSpec {
                extent: extent! {replica=1, host=1, gpu=1},
                constraints: Default::default(),
                proc_name: None,
            })
            .await?,
    )
    .await?;

    let rdma_manager_1: ActorRef<RdmaManagerActor> = proc_mesh_1
        .spawn("rdma_manager_1", &ibv_config_1)
        .await
        .unwrap()
        .get(0)
        .unwrap();

    let bench_actor_1: ActorRef<RdmaBufferBenchmarkActor> = proc_mesh_1
        .spawn("bench_1", &(rdma_manager_1, None))
        .await
        .unwrap()
        .get(0)
        .unwrap();

    let proc_mesh_2 = ProcMesh::allocate(
        alloc
            .allocate(AllocSpec {
                extent: extent! {replica=1, host=1, gpu=1},
                constraints: Default::default(),
                proc_name: None,
            })
            .await?,
    )
    .await?;

    let rdma_manager_2: ActorRef<RdmaManagerActor> = proc_mesh_2
        .spawn("rdma_manager_2", &ibv_config_2)
        .await
        .unwrap()
        .get(0)
        .unwrap();

    let bench_actor_2: ActorRef<RdmaBufferBenchmarkActor> = proc_mesh_2
        .spawn("bench_2", &(rdma_manager_2, Some(bench_actor_1)))
        .await
        .unwrap()
        .get(0)
        .unwrap();

    // Define buffer sizes to test: 1M, 4M, 8M, 16M, 32M, 128M, 256M, 512M
    let buffer_sizes = vec![
        1024 * 1024,        // 1M
        4 * 1024 * 1024,    // 4M
        8 * 1024 * 1024,    // 8M
        16 * 1024 * 1024,   // 16M
        32 * 1024 * 1024,   // 32M
        64 * 1024 * 1024,   // 64M
        128 * 1024 * 1024,  // 128M
        256 * 1024 * 1024,  // 256M
        512 * 1024 * 1024,  // 512M
        1024 * 1024 * 1024, // 1G
    ];

    // Run multi-size benchmark with the specified parameters
    match run_multi_size_benchmark(
        &bench_actor_2,
        proc_mesh_2.client(),
        &buffer_sizes,
        10,      // num_buffers: at least 10 as requested
        100,     // num_iterations: at least 100 as requested
        Some(3), // warmup iterations (using 3 to be faster with more tests)
    )
    .await
    {
        Ok(()) => {
            println!("🎉 All benchmarks completed successfully!");
        }
        Err(e) => {
            println!("❌ Multi-size benchmark failed: {:?}", e);
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    run().await
}
