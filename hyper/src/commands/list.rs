/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use crate::commands::AdminArgs;

#[derive(clap::Args, Debug)]
pub struct ListCommand {
    #[command(flatten)]
    admin: AdminArgs,

    /// The reference whose children to list, such as a host or proc. Lists
    /// the hosts in the mesh when omitted.
    reference: Option<String>,
}

impl ListCommand {
    pub async fn run(self) -> anyhow::Result<()> {
        let node = self
            .admin
            .get_node(self.reference.as_deref().unwrap_or("root"))
            .await?;
        println!("{}", serde_json::to_string_pretty(&node["children"])?);
        Ok(())
    }
}
