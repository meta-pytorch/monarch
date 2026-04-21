/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/// CLI arguments for resolving an admin handle into a mesh admin URL.
///
/// **Currently disabled for `mast_conda:///` handles.** Mesh admin
/// placement has moved to the caller's local proc, making
/// topology-based resolution incorrect. `mast_conda:///` inputs return
/// an explicit error until a publication-based discovery mechanism
/// replaces them. Direct `https://host:port` and bare `host:port`
/// inputs are resolved immediately.
#[derive(clap::Args, Debug)]
pub struct ResolveCommand {
    /// Admin handle: `https://host:port`, `host:port`, or `mast_conda:///job`
    handle: String,

    /// Override the port (intentionally unused — reserved for future use)
    #[arg(long)]
    port: Option<u16>,
}

impl ResolveCommand {
    /// Execute the resolve command via [`AdminHandle`].
    pub async fn run(self) -> anyhow::Result<()> {
        let url = hyperactor_mesh::mesh_admin::AdminHandle::parse(&self.handle)
            .resolve(self.port)
            .await?;
        println!("{}", url);
        Ok(())
    }
}
