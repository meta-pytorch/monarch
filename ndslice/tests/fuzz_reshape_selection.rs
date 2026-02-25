/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::BTreeSet;
use std::time::Duration;
use std::time::Instant;

use anyhow::Result;
use clap::Parser;
use ndslice::Slice;
use ndslice::reshape::Limit;
use ndslice::reshape::ReshapeSliceExt;
use ndslice::reshape::reshape_selection;
use ndslice::selection::EvalOpts;
use ndslice::selection::Selection;
use ndslice::selection::dsl::all;
use ndslice::selection::dsl::false_;
use ndslice::selection::dsl::first;
use ndslice::selection::dsl::intersection;
use ndslice::selection::dsl::range;
use ndslice::selection::dsl::true_;
use ndslice::selection::dsl::union;
use ndslice::shape::Range;
use ndslice::shape::Shape;
use rand::Rng;

#[derive(Default)]
struct BenchmarkStats {
    total_reshape_selection_time: Duration,
    total_new_eval_time: Duration,
    total_new_string_len: usize,
    total_of_ranks_time: Duration,
    total_old_eval_time: Duration,
    total_old_string_len: usize,
    count: usize,
}

#[derive(Default)]
struct NewOnlyBenchmarkStats {
    total_reshape_selection_time: Duration,
    total_new_eval_time: Duration,
    total_new_string_len: usize,
    count: usize,
}

impl BenchmarkStats {
    fn add_result(
        &mut self,
        reshape_selection_time: Duration,
        new_eval_time: Duration,
        new_string_len: usize,
        of_ranks_time: Duration,
        old_eval_time: Duration,
        old_string_len: usize,
    ) {
        self.total_reshape_selection_time += reshape_selection_time;
        self.total_new_eval_time += new_eval_time;
        self.total_new_string_len += new_string_len;
        self.total_of_ranks_time += of_ranks_time;
        self.total_old_eval_time += old_eval_time;
        self.total_old_string_len += old_string_len;
        self.count += 1;
    }

    fn print_averages(&self) {
        if self.count == 0 {
            println!("No benchmark data to average.");
            return;
        }

        let avg_reshape_selection_time = self.total_reshape_selection_time / self.count as u32;
        let avg_new_eval_time = self.total_new_eval_time / self.count as u32;
        let avg_new_string_len = self.total_new_string_len as f64 / self.count as f64;
        let avg_of_ranks_time = self.total_of_ranks_time / self.count as u32;
        let avg_old_eval_time = self.total_old_eval_time / self.count as u32;
        let avg_old_string_len = self.total_old_string_len as f64 / self.count as f64;

        let avg_total_new_time = avg_reshape_selection_time + avg_new_eval_time;
        let avg_total_old_time = avg_of_ranks_time + avg_old_eval_time;
        let avg_total_speedup =
            avg_total_old_time.as_nanos() as f64 / avg_total_new_time.as_nanos() as f64;
        let avg_reshape_speedup = if avg_reshape_selection_time.as_nanos() > 0 {
            avg_of_ranks_time.as_nanos() as f64 / avg_reshape_selection_time.as_nanos() as f64
        } else {
            0.0
        };
        let avg_eval_speedup = if avg_new_eval_time.as_nanos() > 0 {
            avg_old_eval_time.as_nanos() as f64 / avg_new_eval_time.as_nanos() as f64
        } else {
            0.0
        };
        let avg_compression = if avg_new_string_len > 0.0 {
            avg_old_string_len / avg_new_string_len
        } else {
            0.0
        };

        println!(
            "=== BENCHMARK AVERAGES (over {} iterations) ===",
            self.count
        );
        println!(
            "  Average new approach (reshape_selection): {:?}",
            avg_reshape_selection_time
        );
        println!("  Average new approach eval: {:?}", avg_new_eval_time);
        println!(
            "  Average new selection string length: {:.1}",
            avg_new_string_len
        );
        println!(
            "  Average old approach (Selection::of_ranks): {:?}",
            avg_of_ranks_time
        );
        println!("  Average old approach eval: {:?}", avg_old_eval_time);
        println!(
            "  Average old selection string length: {:.1}",
            avg_old_string_len
        );
        println!(
            "  Average total new time (Reshape + Eval): {:?}",
            avg_total_new_time
        );
        println!(
            "  Average total old time (Reshape + Eval): {:?}",
            avg_total_old_time
        );
        println!(
            "  Average reshape speedup (reshape_selection vs Selection::of_ranks): {:.2}x",
            avg_reshape_speedup
        );
        println!(
            "  Average eval speedup (new eval vs old eval): {:.2}x",
            avg_eval_speedup
        );
        println!(
            "  Average total speedup (Reshape + Eval): {:.2}x",
            avg_total_speedup
        );
        println!("  Average compression: {:.2}x", avg_compression);
        println!("========================================");
    }
}

impl NewOnlyBenchmarkStats {
    fn add_result(
        &mut self,
        reshape_selection_time: Duration,
        new_eval_time: Duration,
        new_string_len: usize,
    ) {
        self.total_reshape_selection_time += reshape_selection_time;
        self.total_new_eval_time += new_eval_time;
        self.total_new_string_len += new_string_len;
        self.count += 1;
    }

    fn print_averages(&self) {
        if self.count == 0 {
            println!("No benchmark data to average.");
            return;
        }

        let avg_reshape_selection_time = self.total_reshape_selection_time / self.count as u32;
        let avg_new_eval_time = self.total_new_eval_time / self.count as u32;
        let avg_new_string_len = self.total_new_string_len as f64 / self.count as f64;
        let avg_total_new_time = avg_reshape_selection_time + avg_new_eval_time;

        println!(
            "=== NEW APPROACH BENCHMARK AVERAGES (over {} iterations) ===",
            self.count
        );
        println!(
            "  Average reshape_selection time: {:?}",
            avg_reshape_selection_time
        );
        println!("  Average eval time: {:?}", avg_new_eval_time);
        println!(
            "  Average selection string length: {:.1}",
            avg_new_string_len
        );
        println!(
            "  Average total time (Reshape + Eval): {:?}",
            avg_total_new_time
        );
        println!("============================================================");
    }
}

#[derive(Parser)]
struct Cli {
    /// Max dimension of original shape
    #[arg(long, default_value_t = 3)]
    max_dimensions: usize,

    /// Max width of a single dimension
    #[arg(long, default_value_t = 128)]
    max_dimension_size: usize,

    /// Fanout limit
    #[arg(long, default_value_t = 8)]
    fanout_limit: usize,

    /// Number of test iterations
    #[arg(long, default_value_t = 100)]
    iterations: usize,

    /// Enable debug printing of shape, selection, and reshape information
    #[arg(long, default_value_t = false)]
    debug: bool,

    /// Enable benchmarking comparison between new reshape_selection and old Selection::of_ranks methods
    #[arg(long, default_value_t = false)]
    bench: bool,

    /// Enable benchmarking of new reshape_selection approach only (faster, no comparison)
    #[arg(long, default_value_t = false)]
    bench_new_only: bool,
}

fn generate_random_shape(max_dimensions: usize, max_dimension_size: usize) -> Shape {
    let mut rng = rand::rng();

    let num_dimensions = rng.random_range(1..=max_dimensions);

    Shape::new(
        (0..num_dimensions)
            .enumerate()
            .map(|(i, _)| format!("dim{}", i))
            .collect::<Vec<_>>(),
        Slice::new_row_major(
            (0..num_dimensions)
                .map(|_| rng.random_range(1..=max_dimension_size))
                .collect::<Vec<_>>(),
        ),
    )
    .unwrap()
}

fn generate_random_selection(shape: &Shape) -> Selection {
    let mut rng = rand::rng();

    generate_selection_for_dimensions(shape.slice().sizes(), 0, &mut rng)
}

fn generate_selection_for_dimensions<R: Rng>(
    sizes: &[usize],
    dim_index: usize,
    rng: &mut R,
) -> Selection {
    if dim_index >= sizes.len() {
        return if rng.random_bool(0.8) {
            true_()
        } else {
            false_()
        };
    }

    let current_size = sizes[dim_index];

    match rng.random_range(0..=8) {
        0..=4 => {
            let start = rng.random_range(0..current_size);
            let end = if rng.random_bool(0.5) {
                Some(rng.random_range(start + 1..=current_size))
            } else {
                None
            };
            let step = rng.random_range(1..=current_size);
            range(
                Range(start, end, step),
                generate_selection_for_dimensions(sizes, dim_index + 1, rng),
            )
        }
        5 => all(generate_selection_for_dimensions(sizes, dim_index + 1, rng)),
        6 => union(
            generate_selection_for_dimensions(sizes, dim_index, rng),
            generate_selection_for_dimensions(sizes, dim_index, rng),
        ),
        7 => intersection(
            generate_selection_for_dimensions(sizes, dim_index, rng),
            generate_selection_for_dimensions(sizes, dim_index, rng),
        ),
        8 => first(generate_selection_for_dimensions(sizes, dim_index + 1, rng)),
        _ => unreachable!(),
    }
}

fn test_reshape_selection_correctness(
    shape: &Shape,
    selection: Selection,
    fanout_limit: Limit,
    debug: bool,
    bench: bool,
    bench_new_only: bool,
    stats: &mut Option<BenchmarkStats>,
    new_only_stats: &mut Option<NewOnlyBenchmarkStats>,
) -> Result<()> {
    let original_selected_ranks = selection
        .eval(&EvalOpts::strict(), shape.slice())
        .unwrap()
        .collect::<BTreeSet<_>>();

    let reshaped = shape.slice().reshape_with_limit(fanout_limit);
    if debug {
        println!("========================================================");
        println!("Shape: {:?}", shape);
        println!("Selection: {}", selection);
        println!("Reshaped: {:?}", reshaped);
        println!("Selected ranks: {:?}", original_selected_ranks);
    }

    if bench {
        let start = Instant::now();
        let folded = reshape_selection(selection.clone(), shape.slice(), &reshaped)
            .ok()
            .unwrap();
        let reshape_selection_time = start.elapsed();

        let start = Instant::now();
        let folded_selected_ranks = folded
            .eval(&EvalOpts::strict(), &reshaped)?
            .collect::<BTreeSet<_>>();
        let new_eval_time = start.elapsed();

        let new_selection_string_len = format!("{}", folded).len();

        let start = Instant::now();
        let old_selection = Selection::of_ranks(
            &reshaped,
            &selection
                .eval(&EvalOpts::strict(), shape.slice())
                .unwrap()
                .collect::<BTreeSet<_>>(),
        )?;
        let of_ranks_time = start.elapsed();

        let start = Instant::now();
        let old_selected_ranks = old_selection
            .eval(&EvalOpts::strict(), &reshaped)?
            .collect::<BTreeSet<_>>();
        let old_eval_time = start.elapsed();

        let old_selection_string_len = format!("{}", old_selection).len();

        println!("Benchmark results:");
        println!(
            "  New approach (reshape_selection): {:?}",
            reshape_selection_time
        );
        println!("  New approach eval: {:?}", new_eval_time);
        println!(
            "  New selection string length: {}",
            new_selection_string_len
        );
        println!("  Old approach (Selection::of_ranks): {:?}", of_ranks_time);
        println!("  Old approach eval: {:?}", old_eval_time);
        println!(
            "  Old selection string length: {}",
            old_selection_string_len
        );
        println!(
            "  Total new time: {:?}",
            reshape_selection_time + new_eval_time
        );
        println!("  Total old time: {:?}", of_ranks_time + old_eval_time);
        println!(
            "  Speedup: {:.2}x",
            (of_ranks_time + old_eval_time).as_nanos() as f64
                / (reshape_selection_time + new_eval_time).as_nanos() as f64
        );
        println!();

        if let Some(stats) = stats {
            stats.add_result(
                reshape_selection_time,
                new_eval_time,
                new_selection_string_len,
                of_ranks_time,
                old_eval_time,
                old_selection_string_len,
            );
        }

        if folded_selected_ranks != old_selected_ranks {
            anyhow::bail!(
                "Mismatch between new and old approaches: new ranks {:?} != old ranks {:?}",
                folded_selected_ranks,
                old_selected_ranks
            );
        }

        if original_selected_ranks != folded_selected_ranks {
            anyhow::bail!(
                "reshape_selection failed: original ranks {:?} != folded ranks {:?}\n\
                 Original shape: {:?}\n\
                 Selection: {:?}\n\
                 Reshaped: {:?}\n\
                 Folded selection: {:?}",
                original_selected_ranks,
                folded_selected_ranks,
                shape,
                selection,
                reshaped,
                folded
            );
        }
    } else if bench_new_only {
        let start = Instant::now();
        let folded = reshape_selection(selection.clone(), shape.slice(), &reshaped)
            .ok()
            .unwrap();
        let reshape_selection_time = start.elapsed();

        let start = Instant::now();
        let folded_selected_ranks = folded
            .eval(&EvalOpts::strict(), &reshaped)?
            .collect::<BTreeSet<_>>();
        let new_eval_time = start.elapsed();

        let new_selection_string_len = format!("{}", folded).len();

        println!("New approach benchmark:");
        println!("  reshape_selection time: {:?}", reshape_selection_time);
        println!("  eval time: {:?}", new_eval_time);
        println!("  selection string length: {}", new_selection_string_len);
        println!("  total time: {:?}", reshape_selection_time + new_eval_time);
        println!();

        if let Some(new_only_stats) = new_only_stats {
            new_only_stats.add_result(
                reshape_selection_time,
                new_eval_time,
                new_selection_string_len,
            );
        }

        if original_selected_ranks != folded_selected_ranks {
            anyhow::bail!(
                "reshape_selection failed: original ranks {:?} != folded ranks {:?}\n\
                 Original shape: {:?}\n\
                 Selection: {:?}\n\
                 Reshaped: {:?}\n\
                 Folded selection: {:?}",
                original_selected_ranks,
                folded_selected_ranks,
                shape,
                selection,
                reshaped,
                folded
            );
        }
    } else {
        let folded = reshape_selection(selection.clone(), shape.slice(), &reshaped)
            .ok()
            .unwrap();

        let folded_selected_ranks = folded
            .eval(&EvalOpts::strict(), &reshaped)?
            .collect::<BTreeSet<_>>();

        if original_selected_ranks != folded_selected_ranks {
            anyhow::bail!(
                "reshape_selection failed: original ranks {:?} != folded ranks {:?}\n\
                 Original shape: {:?}\n\
                 Selection: {:?}\n\
                 Reshaped: {:?}\n\
                 Folded selection: {:?}",
                original_selected_ranks,
                folded_selected_ranks,
                shape,
                selection,
                reshaped,
                folded
            );
        }
    }

    Ok(())
}

fn main() -> Result<(), anyhow::Error> {
    let cli = Cli::parse();

    println!("Starting reshape_selection fuzzing with parameters:");
    println!("  Max dimensions: {}", cli.max_dimensions);
    println!("  Max dimension size: {}", cli.max_dimension_size);
    println!("  Fanout limit: {}", cli.fanout_limit);
    println!("  Iterations: {}", cli.iterations);
    println!();

    let fanout_limit = Limit::from(cli.fanout_limit);
    let mut successful_tests = 0;
    let mut failed_tests = 0;
    let mut benchmark_stats = if cli.bench {
        Some(BenchmarkStats::default())
    } else {
        None
    };
    let mut new_only_benchmark_stats = if cli.bench_new_only {
        Some(NewOnlyBenchmarkStats::default())
    } else {
        None
    };

    for i in 0..cli.iterations {
        let shape = generate_random_shape(cli.max_dimensions, cli.max_dimension_size);

        let selection = generate_random_selection(&shape);

        match test_reshape_selection_correctness(
            &shape,
            selection,
            fanout_limit,
            cli.debug,
            cli.bench,
            cli.bench_new_only,
            &mut benchmark_stats,
            &mut new_only_benchmark_stats,
        ) {
            Ok(()) => {
                successful_tests += 1;
                if (i + 1) % 100 == 0 {
                    println!(
                        "Completed {} iterations, {} successful, {} failed",
                        i + 1,
                        successful_tests,
                        failed_tests
                    );
                }
            }
            Err(e) => {
                failed_tests += 1;
                eprintln!("\x1b[31mTest {} failed\x1b[0m: {}", i + 1, e);
            }
        }
    }

    println!("\nFuzzing completed:");
    println!("  Total tests: {}", cli.iterations);
    println!("  Successful: {}", successful_tests);
    println!("  Failed: {}", failed_tests);

    if let Some(stats) = &benchmark_stats {
        println!();
        stats.print_averages();
    }

    if let Some(new_only_stats) = &new_only_benchmark_stats {
        println!();
        new_only_stats.print_averages();
    }

    if failed_tests > 0 {
        anyhow::bail!("{} tests failed out of {}", failed_tests, cli.iterations);
    }

    println!("All tests passed!");
    Ok(())
}
