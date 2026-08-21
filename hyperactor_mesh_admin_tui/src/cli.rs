/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::io;

use clap::Parser;

use crate::LangName;
use crate::ThemeName;
use crate::TuiConfig;

/// Command-line arguments for the admin TUI.
#[derive(Debug, Parser)]
#[command(name = "admin-tui", about = "TUI client for hyperactor admin API")]
struct Args {
    /// Admin server address.
    ///
    /// Accepts `host:port` (scheme auto-detected) or an explicit URL
    /// like `https://host:port`. `mast_conda:///<job-name>` handles
    /// are currently disabled (returns an error).
    #[arg(long, short)]
    addr: String,

    /// Admin port override (currently unused — `mast_conda:///`
    /// resolution is disabled).
    #[arg(long)]
    admin_port: Option<u16>,

    /// Refresh interval in milliseconds
    #[arg(long, default_value_t = 2000)]
    refresh_ms: u64,

    /// Color theme
    #[arg(long, default_value_t = ThemeName::Nord, value_enum)]
    theme: ThemeName,

    /// Display language
    #[arg(long, default_value_t = LangName::En, value_enum)]
    lang: LangName,

    /// Path to a PEM CA certificate for TLS server verification.
    #[arg(long)]
    tls_ca: Option<String>,

    /// Path to a PEM client certificate for mutual TLS.
    #[arg(long)]
    tls_cert: Option<String>,

    /// Path to a PEM client private key for mutual TLS.
    #[arg(long)]
    tls_key: Option<String>,

    /// Run diagnostics and print a JSON report to stdout, then exit.
    #[arg(long)]
    diagnose: bool,

    /// Disable TLS and use plain HTTP (for a server with no cert, e.g. public infra).
    #[arg(long, conflicts_with_all = ["tls_ca", "tls_cert", "tls_key"])]
    plaintext: bool,
}

/// Parse command-line arguments and run the TUI.
pub async fn run_cli() -> io::Result<()> {
    let mut args = Args::parse();

    // Resolve the admin address via AdminHandle (handles mast_conda:///,
    // bare host:port scheme inference, and explicit URLs uniformly).
    args.addr = hyperactor_mesh::mesh_admin::AdminHandle::parse(&args.addr)
        .resolve(args.admin_port)
        .await
        .unwrap_or_else(|e| {
            eprintln!("{:#}", e);
            std::process::exit(1);
        });

    let config = TuiConfig {
        addr: args.addr,
        refresh_ms: args.refresh_ms,
        theme: args.theme,
        lang: args.lang,
        tls_ca: args.tls_ca,
        tls_cert: args.tls_cert,
        tls_key: args.tls_key,
        diagnose: args.diagnose,
        plaintext: args.plaintext,
    };

    crate::run(config).await
}
