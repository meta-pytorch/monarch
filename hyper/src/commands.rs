/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use hyperactor_mesh::mesh_admin::AdminHandle;
use hyperactor_mesh::mesh_admin_client::AdminClientOptions;

pub mod list;
pub mod resolve;
pub mod show;

/// How to reach a mesh admin server.
#[derive(clap::Args, Debug)]
pub struct AdminArgs {
    /// Admin server address: `host:port` (scheme auto-detected) or an
    /// explicit URL like `https://host:port`.
    #[arg(long, short)]
    admin: String,

    /// Path to a PEM CA certificate for TLS server verification.
    #[arg(long)]
    tls_ca: Option<String>,

    /// Path to a PEM client certificate for mutual TLS.
    #[arg(long)]
    tls_cert: Option<String>,

    /// Path to a PEM client private key for mutual TLS.
    #[arg(long)]
    tls_key: Option<String>,

    /// Disable TLS and use plain HTTP.
    #[arg(long, conflicts_with_all = ["tls_ca", "tls_cert", "tls_key"])]
    plaintext: bool,
}

impl AdminArgs {
    /// Fetch the introspection node for `reference` (`root` for the
    /// mesh root) as JSON.
    pub(crate) async fn get_node(&self, reference: &str) -> anyhow::Result<serde_json::Value> {
        let addr = AdminHandle::parse(&self.admin).resolve(None).await?;
        let (base_url, client) =
            hyperactor_mesh::mesh_admin_client::build_client(&AdminClientOptions {
                addr,
                tls_ca: self.tls_ca.clone(),
                tls_cert: self.tls_cert.clone(),
                tls_key: self.tls_key.clone(),
                plaintext: self.plaintext,
            })?;
        let url = format!("{}/v1/{}", base_url, urlencoding::encode(reference));
        let response = client.get(&url).send().await?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            anyhow::bail!("{} returned {}: {}", url, status, body);
        }
        Ok(serde_json::from_str(&body)?)
    }
}
