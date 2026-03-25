/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Thin binary for the Monarch mesh admin TUI.
//!
//! Owns CLI argument parsing and MAST handle resolution, then
//! delegates to [`hyperactor_mesh_admin_tui_lib::run`] for the actual TUI.

use std::io;

use clap::Parser;
use hyperactor_mesh_admin_tui_lib::LangName;
use hyperactor_mesh_admin_tui_lib::ThemeName;
use hyperactor_mesh_admin_tui_lib::TuiConfig;

/// Command-line arguments for the admin TUI.
#[derive(Debug, Parser)]
#[command(name = "admin-tui", about = "TUI client for hyperactor admin API")]
struct Args {
    /// Admin server address.
    ///
    /// Accepts `host:port` (scheme auto-detected), an explicit URL
    /// like `https://host:port`, or a MAST job handle like
    /// `mast_conda:///<job-name>` (Meta-internal only).
    #[arg(long, short)]
    addr: String,

    /// Admin port override for MAST job resolution. When not set,
    /// reads from `MESH_ADMIN_ADDR` config.
    #[arg(long)]
    admin_port: Option<u16>,

    /// MAST resolution strategy: "thrift" (default at Meta) or
    /// "cli" (MR-1).
    #[arg(long)]
    mast_resolver: Option<String>,

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
}

// -- MAST resolution dispatch (MR-1) --
//
// `MastResolver` is defined locally in each binary rather than in a
// shared library, to avoid pulling `fbinit` into the library's
// dependency graph. The CLI implementation lives in
// `hyperactor_mesh::mesh_admin`; the thrift implementation lives in
// `hyperactor_meta::mesh_admin`.

/// Resolution strategy for `mast_conda:///` handles.
enum MastResolver {
    /// Shell out to the `mast` CLI. Works in both Meta and OSS.
    Cli,
    /// Use the MAST Thrift API. Only available in Meta builds.
    #[cfg(fbcode_build)]
    Thrift(fbinit::FacebookInit),
}

impl MastResolver {
    fn new(
        #[allow(unused_variables)] fb: Option<fbinit::FacebookInit>,
        choice: Option<&str>,
    ) -> Self {
        #[cfg(fbcode_build)]
        if choice != Some("cli") {
            if let Some(fb) = fb {
                return MastResolver::Thrift(fb);
            }
        }
        MastResolver::Cli
    }
}

async fn resolve_mast_addr(resolver: &MastResolver, addr: &str, admin_port: Option<u16>) -> String {
    let result = match resolver {
        MastResolver::Cli => {
            hyperactor_mesh::mesh_admin::resolve_mast_handle(addr, admin_port).await
        }
        #[cfg(fbcode_build)]
        MastResolver::Thrift(fb) => {
            hyperactor_meta_lib::mesh_admin::resolve_mast_handle(*fb, addr, admin_port).await
        }
    };
    result.unwrap_or_else(|e| {
        eprintln!("{:#}", e);
        std::process::exit(1);
    })
}

#[cfg(fbcode_build)]
#[fbinit::main]
async fn main(fb: fbinit::FacebookInit) -> io::Result<()> {
    run(Some(fb)).await
}

#[cfg(not(fbcode_build))]
#[tokio::main]
async fn main() -> io::Result<()> {
    run(None).await
}

async fn run(fb: Option<fbinit::FacebookInit>) -> io::Result<()> {
    let mut args = Args::parse();

    // Resolve mast_conda:/// handles to https://fqdn:port before
    // building the HTTP client (MR-1).
    if args.addr.starts_with("mast_conda:///") {
        let resolver = MastResolver::new(fb, args.mast_resolver.as_deref());
        args.addr = resolve_mast_addr(&resolver, &args.addr, args.admin_port).await;
    }

    let config = TuiConfig {
        addr: args.addr,
        refresh_ms: args.refresh_ms,
        theme: args.theme,
        lang: args.lang,
        tls_ca: args.tls_ca,
        tls_cert: args.tls_cert,
        tls_key: args.tls_key,
        diagnose: args.diagnose,
    };

    hyperactor_mesh_admin_tui_lib::run(config).await
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- MastResolver::new() tests (MR-1) --

    // MR-1: no fb, no choice → Cli.
    #[test]
    fn test_mast_resolver_no_fb_defaults_to_cli() {
        let resolver = MastResolver::new(None, None);
        assert!(matches!(resolver, MastResolver::Cli));
    }

    // MR-1: explicit "cli" choice → Cli regardless of fb.
    // fbcode_build only: requires fbinit, and the Thrift variant only
    // exists in Meta builds.
    #[cfg(fbcode_build)]
    #[test]
    fn test_mast_resolver_cli_choice_overrides_fb() {
        // SAFETY: only reachable in fbcode_build tests where main()
        // is annotated #[fbinit::main].
        let fb = unsafe { fbinit::assume_init() };
        let resolver = MastResolver::new(Some(fb), Some("cli"));
        assert!(matches!(resolver, MastResolver::Cli));
    }

    // MR-1: fb present, no choice → Thrift.
    // fbcode_build only: the Thrift variant and fbinit are unavailable
    // in OSS builds.
    #[cfg(fbcode_build)]
    #[test]
    fn test_mast_resolver_fb_defaults_to_thrift() {
        // SAFETY: only reachable in fbcode_build tests where main()
        // is annotated #[fbinit::main].
        let fb = unsafe { fbinit::assume_init() };
        let resolver = MastResolver::new(Some(fb), None);
        assert!(matches!(resolver, MastResolver::Thrift(_)));
    }

    // MR-1: explicit "thrift" choice (or any non-"cli" string)
    // → Thrift when fb is available.
    // fbcode_build only: the Thrift variant and fbinit are unavailable
    // in OSS builds.
    #[cfg(fbcode_build)]
    #[test]
    fn test_mast_resolver_explicit_thrift_choice() {
        // SAFETY: only reachable in fbcode_build tests where main()
        // is annotated #[fbinit::main].
        let fb = unsafe { fbinit::assume_init() };
        let resolver = MastResolver::new(Some(fb), Some("thrift"));
        assert!(matches!(resolver, MastResolver::Thrift(_)));
    }
}
