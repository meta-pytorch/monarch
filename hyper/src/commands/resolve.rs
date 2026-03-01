/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/// CLI arguments for resolving a `mast_conda:///` job handle into a
/// mesh admin base URL.
///
/// `handle` is the MAST job handle to resolve. `port` optionally
/// overrides the port used in the resolved URL; when not provided,
/// the port is derived from `MESH_ADMIN_ADDR`.
#[derive(clap::Args, Debug)]
pub struct ResolveCommand {
    /// MAST job handle (e.g. mast_conda:///monarch-abc123)
    handle: String,

    /// Override the port (default: from MESH_ADMIN_ADDR config)
    #[arg(long)]
    port: Option<u16>,
}

impl ResolveCommand {
    /// Execute the resolve command.
    ///
    /// Resolves the provided `mast_conda:///` handle into an HTTPS
    /// mesh admin base URL using MAST hostname resolution and prints
    /// the result to stdout. The port is taken from `--admin-port`
    /// when provided, otherwise from `MESH_ADMIN_ADDR` configuration.
    pub async fn run(self) -> anyhow::Result<()> {
        // SAFETY: Only reachable from main(), which is annotated
        // #[fbinit::main] — guaranteeing FacebookInit has been
        // performed.
        let fb = unsafe { fbinit::assume_init() };

        let url = hyperactor_meta_lib::mesh_admin::resolve_mast_handle(fb, &self.handle, self.port)
            .await?;

        println!("{}", url);
        Ok(())
    }
}
