/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor::PortRef;
use hyperactor::RemoteSpawn;
use monarch_types::SerializablePyErr;
use pyo3::prelude::*;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::runtime::monarch_with_gil_blocking;

/// Message to trigger module reloading
#[derive(Debug, Clone, Named, Serialize, Deserialize)]
pub struct AutoReloadMessage {
    pub result: PortRef<Result<(), String>>,
}
wirevalue::register_type!(AutoReloadMessage);

/// Parameters for creating an AutoReloadActor
#[derive(Debug, Clone, Named, Serialize, Deserialize)]
pub struct AutoReloadParams {}
wirevalue::register_type!(AutoReloadParams);

/// Simple Rust Actor that wraps the Python AutoReloader class via pyo3
#[derive(Debug)]
#[hyperactor::export(spawn = true, handlers = [AutoReloadMessage])]
pub struct AutoReloadActor {
    state: Result<(Arc<PyObject>, PyObject), SerializablePyErr>,
}

impl Actor for AutoReloadActor {}

#[async_trait]
impl RemoteSpawn for AutoReloadActor {
    type Params = AutoReloadParams;

    async fn new(Self::Params {}: Self::Params) -> Result<Self> {
        AutoReloadActor::new().await
    }
}

impl AutoReloadActor {
    pub(crate) async fn new() -> Result<Self, anyhow::Error> {
        Ok(Self {
            state: tokio::task::spawn_blocking(move || {
                monarch_with_gil_blocking(|py| {
                    Self::create_state(py).map_err(SerializablePyErr::from_fn(py))
                })
            })
            .await?,
        })
    }

    fn create_state(py: Python) -> PyResult<(Arc<PyObject>, PyObject)> {
        // Import the Python AutoReloader class
        let auto_reload_module = py.import("monarch._src.actor.code_sync.auto_reload")?;
        let auto_reloader_class = auto_reload_module.getattr("AutoReloader")?;

        let reloader = auto_reloader_class.call0()?;

        // Install the audit import hook: SysAuditImportHook.install(reloader.import_callback)
        let sys_audit_import_hook_class = auto_reload_module.getattr("SysAuditImportHook")?;
        let import_callback = reloader.getattr("import_callback")?;
        let hook_guard = sys_audit_import_hook_class.call_method1("install", (import_callback,))?;

        Ok((Arc::new(reloader.into()), hook_guard.into()))
    }

    fn reload(py: Python, py_reloader: &PyObject) -> PyResult<()> {
        let reloader = py_reloader.bind(py);
        let changed_modules: Vec<String> = reloader.call_method0("reload_changes")?.extract()?;
        if !changed_modules.is_empty() {
            eprintln!("reloaded modules: {:?}", changed_modules);
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<AutoReloadMessage> for AutoReloadActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        AutoReloadMessage { result }: AutoReloadMessage,
    ) -> Result<()> {
        // Call the Python reloader's reload_changes method
        let res = async {
            let py_reloader: Arc<_> = self.state.as_ref().map_err(Clone::clone)?.0.clone();
            tokio::task::spawn_blocking(move || {
                monarch_with_gil_blocking(|py| {
                    Self::reload(py, py_reloader.as_ref()).map_err(SerializablePyErr::from_fn(py))
                })
            })
            .await??;
            anyhow::Ok(())
        }
        .await;
        result.send(cx, res.map_err(|e| format!("{:#?}", e)))?;
        Ok(())
    }
}
