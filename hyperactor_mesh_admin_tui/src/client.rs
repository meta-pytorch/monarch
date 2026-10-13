/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! TLS-aware `reqwest` client construction for the admin TUI.
//!
//! Admin handle resolution (`mast_conda:///` and bare `host:port`) is
//! performed in the binary via `AdminHandle::parse` before calling
//! this library. By the time [`build_client`] is called, `addr` is a
//! concrete URL: `https://host:port`, or `http://host:port` for an
//! explicit `http` scheme. Transport selection is shared with other
//! mesh admin clients; see [`hyperactor_mesh::mesh_admin_client`].

use std::io;

use hyperactor_mesh::mesh_admin_client::AdminClientOptions;

use crate::TuiConfig;

/// Build a `reqwest` client and a base URL from CLI arguments. See
/// [`hyperactor_mesh::mesh_admin_client::build_client`].
pub(crate) fn build_client(config: &TuiConfig) -> io::Result<(String, reqwest::Client)> {
    hyperactor_mesh::mesh_admin_client::build_client(&AdminClientOptions {
        addr: config.addr.clone(),
        tls_ca: config.tls_ca.clone(),
        tls_cert: config.tls_cert.clone(),
        tls_key: config.tls_key.clone(),
        plaintext: config.plaintext,
    })
}
