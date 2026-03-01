/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use crate::MastResolver;

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
    /// Execute the resolve command (INV-DISPATCH).
    ///
    /// See the module-level comment on `MastResolver` in `main.rs`
    /// for why the dispatch is local to each binary.
    pub async fn run(self, resolver: &MastResolver) -> anyhow::Result<()> {
        let url = match resolver {
            MastResolver::Cli => {
                hyperactor_mesh::mesh_admin::resolve_mast_handle(&self.handle, self.port).await?
            }
            #[cfg(fbcode_build)]
            MastResolver::Thrift(fb) => {
                hyperactor_meta_lib::mesh_admin::resolve_mast_handle(*fb, &self.handle, self.port)
                    .await?
            }
        };
        println!("{}", url);
        Ok(())
    }
}
