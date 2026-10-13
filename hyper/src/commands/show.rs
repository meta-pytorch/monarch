/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use crate::commands::AdminArgs;

#[derive(clap::Args, Debug)]
pub struct ShowCommand {
    #[command(flatten)]
    admin: AdminArgs,

    /// The reference to show, such as a host, proc, or actor.
    reference: String,
}

impl ShowCommand {
    pub async fn run(self) -> anyhow::Result<()> {
        let node = self.admin.get_node(&self.reference).await?;
        println!("{}", serde_json::to_string_pretty(&node)?);
        Ok(())
    }
}
