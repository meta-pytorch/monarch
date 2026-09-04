/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

mod admin;
mod benchmark;
mod mast;
mod network_baseline;
mod persist;

use anyhow::Context;
use anyhow::Result;
use clap::Parser;
use clap::Subcommand;

#[derive(Debug, Parser)]
#[command(about = "Run Chrysalis process-mesh scale benchmarks")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Lists, adds, and inspects persistent scale experiments.
    Experiments(admin::ExperimentsArgs),
    /// Lists and inspects persistent scale nodes.
    Nodes(admin::NodeArgs),
    /// Runs one rank inside a MAST task.
    Run(benchmark::RunArgs),
    /// Runs persistent, SQLite-driven experiment nodes inside a MAST task.
    Persist(benchmark::PersistArgs),
    /// Builds, packages, and optionally launches MAST ablation jobs.
    Mast(mast::MastArgs),
    /// Measures a selected transport layer between two MAST tasks.
    NetworkBaseline(network_baseline::NetworkBaselineArgs),
    /// Runs one child node under a scale task supervisor.
    #[command(hide = true)]
    Worker(benchmark::WorkerArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Experiments(args) => admin::experiments(args).await,
        Command::Nodes(args) => admin::nodes(args).await,
        Command::Run(args) => benchmark::run(args).await,
        Command::Persist(args) => benchmark::persist(args).await,
        Command::Mast(args) => tokio::task::spawn_blocking(move || mast::run(args))
            .await
            .context("join blocking MAST launcher")?,
        Command::NetworkBaseline(args) => network_baseline::run(args).await,
        Command::Worker(args) => benchmark::worker(args).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_experiment_commands() {
        Cli::try_parse_from([
            "chrysalis-scale",
            "experiments",
            "udp://127.0.0.1:26600",
            "list",
        ])
        .expect("parse experiment list");
        Cli::try_parse_from([
            "chrysalis-scale",
            "experiments",
            "udp://127.0.0.1:26600",
            "add",
            "one-kib",
            "42424242424242424242424242424242",
            "10",
            "1024",
        ])
        .expect("parse experiment add");
        Cli::try_parse_from([
            "chrysalis-scale",
            "experiments",
            "udp://127.0.0.1:26600",
            "add-targeted",
            "leaf-bandwidth",
            "42424242424242424242424242424242",
            "67108864",
            "43434343434343434343434343434343",
            "44444444444444444444444444444444",
            "--kind",
            "delivery",
        ])
        .expect("parse targeted delivery experiment add");
        Cli::try_parse_from([
            "chrysalis-scale",
            "experiments",
            "udp://127.0.0.1:26600",
            "show",
            "one-kib",
        ])
        .expect("parse experiment show");
    }

    #[test]
    fn parses_node_commands() {
        Cli::try_parse_from(["chrysalis-scale", "nodes", "udp://127.0.0.1:26600", "list"])
            .expect("parse node list");
        Cli::try_parse_from([
            "chrysalis-scale",
            "nodes",
            "udp://127.0.0.1:26600",
            "show",
            "42424242424242424242424242424242",
        ])
        .expect("parse node show");
        Cli::try_parse_from(["chrysalis-scale", "nodes", "mast://scale_job", "list"])
            .expect("parse resolved node list");
    }

    #[test]
    fn parses_scale_topologies() {
        Cli::try_parse_from([
            "chrysalis-scale",
            "run",
            "--nodes",
            "100",
            "--topology",
            "task-head",
        ])
        .expect("parse task-head run");
        Cli::try_parse_from([
            "chrysalis-scale",
            "mast",
            "--nodes",
            "100",
            "--topology",
            "flat",
        ])
        .expect("parse flat MAST job");
        Cli::try_parse_from([
            "chrysalis-scale",
            "network-baseline",
            "--protocol",
            "direct-quic",
            "--bytes",
            "1048576",
            "--connections",
            "4",
        ])
        .expect("parse network baseline");
        Cli::try_parse_from([
            "chrysalis-scale",
            "network-baseline",
            "--protocol",
            "quic-io-ablation",
            "--profile",
            "record",
        ])
        .expect("parse profiled QUIC I/O ablation");
        Cli::try_parse_from([
            "chrysalis-scale",
            "network-baseline",
            "--protocol",
            "quic-write-chunks-ablation",
        ])
        .expect("parse batched QUIC write ablation");
        Cli::try_parse_from([
            "chrysalis-scale",
            "network-baseline",
            "--protocol",
            "quic-send-window-ablation",
        ])
        .expect("parse QUIC send-window ablation");
        Cli::try_parse_from([
            "chrysalis-scale",
            "network-baseline",
            "--protocol",
            "quic-mtu-ablation",
        ])
        .expect("parse QUIC MTU ablation");
        Cli::try_parse_from([
            "chrysalis-scale",
            "network-baseline",
            "--protocol",
            "quic-pacing-ablation",
        ])
        .expect("parse QUIC pacing ablation");
        Cli::try_parse_from([
            "chrysalis-scale",
            "network-baseline",
            "--protocol",
            "quic-gso-ablation",
        ])
        .expect("parse QUIC GSO ablation");
    }

    #[test]
    fn task_head_is_the_default_topology() {
        let cli = Cli::try_parse_from(["chrysalis-scale", "run", "--nodes", "100"])
            .expect("parse default topology");
        let Command::Run(args) = cli.command else {
            panic!("run command must parse as run");
        };
        assert_eq!(args.topology(), benchmark::Topology::TaskHead);
    }

    #[test]
    fn parses_private_unix_worker_command() {
        Cli::try_parse_from([
            "chrysalis-scale",
            "worker",
            "--nodes",
            "3",
            "--rank",
            "2",
            "--level",
            "1",
            "--bind",
            "unix:///tmp/chrysalis-node.sock",
            "--parent",
            "unix:///tmp/chrysalis-head.sock?authority=42424242424242424242424242424242",
            "--ready-file",
            "/tmp/chrysalis-ready",
        ])
        .expect("parse private worker command");
    }
}
