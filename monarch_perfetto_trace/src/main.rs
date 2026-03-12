/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Command-line interface for expanse_rs tools.

use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;

use anyhow::Result;
use clap::Parser;
use monarch_perfetto_trace::Sink;
use tracing::Level;
use tracing::info;
use tracing_perfetto_sdk_schema::Trace;
use tracing_perfetto_sdk_schema::TracePacket;
use tracing_subscriber::FmtSubscriber;

/// Command-line arguments for the expanse_rs tool
#[derive(Parser, Debug)]
#[clap()]
struct Cli {
    /// Execution ID to filter events. Uses the latest execution if not specified.
    #[clap(short, long)]
    execution_id: Option<String>,

    /// Root directory containing monarch_traces/. Defaults to /tmp/{user}/.
    #[clap(short, long)]
    trace_dir: Option<PathBuf>,

    /// Verbose output
    #[clap(short, long)]
    verbose: bool,
}

#[derive(Default)]
struct Collector {
    trace: Trace,
    path: String,
}

impl Collector {
    fn new(path: &Path) -> Self {
        Self {
            path: path.to_string_lossy().to_string(),
            trace: Trace::default(),
        }
    }

    fn flush(&mut self) -> Result<()> {
        let tr = std::mem::take(&mut self.trace);
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.path)?;

        let bytes = prost::Message::encode_to_vec(&tr);
        file.write_all(&bytes)?;
        file.flush()?;
        Ok(())
    }
}

impl Sink for Collector {
    fn consume(&mut self, packet: TracePacket) {
        self.trace.packet.push(packet);
        if self.trace.packet.len() > 100 {
            self.flush().unwrap();
        }
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Set up logging
    let level = if cli.verbose {
        Level::DEBUG
    } else {
        Level::INFO
    };
    let subscriber = FmtSubscriber::builder().with_max_level(level).finish();
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set tracing subscriber");

    // Set up temp file for output
    let tf = tempfile::NamedTempFile::new()?;
    let path = tf.path();
    let file = std::fs::File::create(path)?;
    drop(file);

    let mut sink = Collector::new(path);
    tracing::info!("Writing trace to {path:?}");

    let trace_root = cli
        .trace_dir
        .unwrap_or_else(monarch_perfetto_trace::local::default_trace_root);
    let trace_dir = monarch_perfetto_trace::local::trace_dir(&trace_root);
    info!("Using trace directory: {}", trace_dir.display());

    // Determine execution ID
    let exec_id = match cli.execution_id {
        Some(id) => id,
        None => match monarch_perfetto_trace::local::get_latest_execution_id(&trace_dir)? {
            Some(id) => {
                info!("Using latest execution: {}", id);
                id
            }
            None => {
                anyhow::bail!(
                    "No executions found in {}. Run a traced execution first.",
                    trace_dir.display()
                );
            }
        },
    };

    let execution_dir = monarch_perfetto_trace::local::execution_dir(&trace_dir, Some(&exec_id))?;
    info!("Reading from execution: {}", execution_dir.display());

    monarch_perfetto_trace::local::merge_traces_from_dir(&execution_dir, &mut sink)?;
    sink.flush()?;

    let file = std::fs::File::open(path)?;
    tracing::info!(
        "Uploading trace to manifold with size {:.2}mb",
        (file.metadata()?.len() as f64 / 1_000_000.0)
    );
    let url = upload_trace_file(path.to_str().expect("bad tmp file path"))?;
    print_perfetto_ui_url(url.as_str());

    Ok(())
}

const PERFETTO_UIROOT_URL: &str =
    "https://interncache-all.fbcdn.net/manifold/perfetto-artifacts/tree/ui/index.html";
const MANIFOLD_FOLDER: &str = "perfetto_internal_traces/tree/shared_trace";
const DEFAULT_TTLSEC: u64 = 28 * 24 * 60 * 60;

fn upload_trace_file(local_path: &str) -> Result<String> {
    let file_name = Path::new(local_path).file_name().unwrap().to_str().unwrap();
    let manifold_path = format!(
        "{}/{}_{}_{}",
        MANIFOLD_FOLDER,
        std::env::var("USER").unwrap_or_else(|_| "unknown_user".to_string()),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        file_name
    );
    let cmd = vec![
        "manifold".to_string(),
        "put".to_string(),
        local_path.to_string(),
        manifold_path.clone(),
        "--ttl".to_string(),
        DEFAULT_TTLSEC.to_string(),
        "--userData".to_string(),
        "false".to_string(),
    ];
    tracing::debug!("running upload command {cmd:?}");
    let output = Command::new(&cmd[0]).args(&cmd[1..]).output()?;
    if !output.status.success() {
        let err = String::from_utf8_lossy(&output.stderr);
        tracing::error!("upload failed {}", err);
        return Err(anyhow::anyhow!("Unable to upload trace {err}"));
    }
    tracing::debug!("manifold upload complete");
    Ok(manifold_path)
}

fn print_perfetto_ui_url(manifold_path: &str) {
    let url = format!(
        "{}?url=https://interncache-all.fbcdn.net/manifold/{}",
        PERFETTO_UIROOT_URL,
        urlencoding::encode(manifold_path)
    );
    tracing::debug!("generating fburl {url}");
    let output = Command::new("fburl")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut c| {
            use std::io::Write;
            c.stdin.as_mut().unwrap().write_all(url.as_bytes()).unwrap();
            c.wait_with_output()
        });
    match output {
        Ok(output) => println!("trace:\n\t{}", String::from_utf8_lossy(&output.stdout)),
        Err(e) => println!(
            "unable to create fburl {}, here's the raw url instead: {}",
            e, url
        ),
    }
}
