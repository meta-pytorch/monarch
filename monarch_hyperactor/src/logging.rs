/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![allow(unsafe_op_in_unsafe_fn)]

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::Bind;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::RefClient;
use hyperactor::RemoteSpawn;
use hyperactor::Unbind;
use monarch_types::SerializablePyErr;
use pyo3::prelude::*;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    Named,
    Handler,
    HandleClient,
    RefClient,
    Bind,
    Unbind
)]
pub enum LoggerRuntimeMessage {
    SetLogging { level: u8 },
}

/// Simple Rust actor that invokes python logger APIs. It needs a python runtime.
#[derive(Debug)]
#[hyperactor::export(spawn = true, handlers = [LoggerRuntimeMessage {cast = true}])]
pub struct LoggerRuntimeActor {
    logger: Arc<PyObject>,
}

impl LoggerRuntimeActor {
    fn get_logger(py: Python) -> PyResult<PyObject> {
        // Import the Python AutoReloader class
        let logging_module = py.import("logging")?;
        let logger = logging_module.call_method0("getLogger")?;

        Ok(logger.into())
    }

    fn set_logger_level(py: Python, logger: &PyObject, level: u8) -> PyResult<()> {
        let logger = logger.bind(py);
        logger.call_method1("setLevel", (level,))?;
        Ok(())
    }
}
impl Actor for LoggerRuntimeActor {}

#[async_trait]
impl RemoteSpawn for LoggerRuntimeActor {
    type Params = ();

    async fn new(_: ()) -> Result<Self, anyhow::Error> {
        let logger =
            Python::with_gil(|py| Self::get_logger(py).map_err(SerializablePyErr::from_fn(py)))?;
        Ok(Self {
            logger: Arc::new(logger),
        })
    }
}

#[async_trait]
#[hyperactor::forward(LoggerRuntimeMessage)]
impl LoggerRuntimeMessageHandler for LoggerRuntimeActor {
    async fn set_logging(&mut self, _cx: &Context<Self>, level: u8) -> Result<(), anyhow::Error> {
        let logger: Arc<_> = self.logger.clone();
        Python::with_gil(|py| {
            Self::set_logger_level(py, logger.as_ref(), level)
                .map_err(SerializablePyErr::from_fn(py))
        })?;
        Ok(())
    }
}
