/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! OSS binary for merging per-process `.pftrace` files into a single unified trace.

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use monarch_perfetto_trace::local;
use tracing::Level;
use tracing::info;
use tracing_subscriber::FmtSubscriber;

/// Merge per-process .pftrace files into a single unified trace.
#[derive(Parser, Debug)]
#[clap()]
struct Cli {
    /// Execution ID to use. Uses the latest execution if not specified.
    #[clap(short, long)]
    execution_id: Option<String>,

    /// Root directory containing monarch_traces/. Defaults to /tmp/{user}/.
    #[clap(short, long)]
    trace_dir: Option<PathBuf>,

    /// Output file path. Defaults to ./{execution_id}.pftrace.
    #[clap(short, long)]
    output: Option<PathBuf>,

    /// Verbose output.
    #[clap(short, long)]
    verbose: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let level = if cli.verbose {
        Level::DEBUG
    } else {
        Level::INFO
    };
    let subscriber = FmtSubscriber::builder().with_max_level(level).finish();
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set tracing subscriber");

    let output = match cli.output {
        Some(path) => {
            local::merge_to_file(cli.trace_dir.as_deref(), cli.execution_id.as_deref(), &path)?;
            path
        }
        None => {
            // Write to a temp file first, then rename once we know the execution ID.
            let tmp = PathBuf::from(".expanse_merge.pftrace.tmp");
            let exec_id =
                local::merge_to_file(cli.trace_dir.as_deref(), cli.execution_id.as_deref(), &tmp)?;
            let path = PathBuf::from(format!("{}.pftrace", exec_id));
            std::fs::rename(&tmp, &path)?;
            path
        }
    };

    info!("Merged trace written to {}", output.display());
    println!("{}", output.display());

    Ok(())
}
