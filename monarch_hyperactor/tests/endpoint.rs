/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use monarch_hyperactor::endpoint::ActorEndpoint;
use monarch_hyperactor::runtime::GilSite;
use monarch_hyperactor::runtime::get_tokio_runtime;
use monarch_hyperactor::runtime::monarch_with_gil;
use monarch_hyperactor::runtime::monarch_with_gil_blocking;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::types::PyTuple;

/// Exercise the public native wrapper against a spawned Python actor, not the
/// module-private dispatch double. The actor acknowledges the request before
/// the native reply future is polled; unpickling reports its driver's thread.
#[test]
#[cfg_attr(not(fbcode_build), ignore)]
fn native_call_one_sends_before_polling_and_decodes_on_its_driver() -> PyResult<()> {
    Python::initialize();
    let (actor_mesh, receiver, reply) = monarch_with_gil_blocking(GilSite::Test, |py| {
        let module = PyModule::from_code(
            py,
            c"
import threading
from monarch.actor import Actor, endpoint
from monarch._src.actor.actor_mesh import Channel
from monarch._src.actor.host_mesh import this_proc

class UnpickleThread:
    def __reduce__(self):
        return threading.get_ident, ()

class ReplyActor(Actor):
    @endpoint
    def reply(self, acknowledged):
        acknowledged.send(1)
        return UnpickleThread()

def create():
    actor = this_proc().spawn('native_reply_probe', ReplyActor)
    sender, receiver = Channel[int].open()
    return actor, actor.reply, sender, receiver
",
            c"native_reply_probe.py",
            c"native_reply_probe",
        )?;
        let (actor_mesh, endpoint, sender, receiver): (
            Py<PyAny>,
            Py<ActorEndpoint>,
            Py<PyAny>,
            Py<PyAny>,
        ) = module.getattr("create")?.call0()?.extract()?;
        let args = PyTuple::new(py, [sender])?;
        let reply = endpoint.borrow(py).call_one_future(py, &args, None)?;
        Ok::<_, PyErr>((actor_mesh, receiver, reply))
    })?;

    // Keep the observation result separate so shutdown runs on failures too.
    let observed = (|| {
        monarch_with_gil_blocking(GilSite::Test, |py| {
            let kwargs = PyDict::new(py);
            kwargs.set_item("timeout", 10)?;
            let acknowledged = receiver
                .bind(py)
                .call_method0("recv")?
                .call_method("get", (), Some(&kwargs))?
                .extract::<u64>()?;
            if acknowledged != 1 {
                return Err(pyo3::exceptions::PyAssertionError::new_err(
                    "the real actor must acknowledge sending before reply polling",
                ));
            }
            Ok(())
        })?;

        get_tokio_runtime().block_on(async move {
            let driver = monarch_with_gil(GilSite::Test, |py| {
                py.import("threading")?
                    .call_method0("get_ident")?
                    .extract::<u64>()
            })
            .await?;
            let value = tokio::time::timeout(std::time::Duration::from_secs(30), reply)
                .await
                .map_err(|_| pyo3::exceptions::PyTimeoutError::new_err("native reply wait"))??;
            let decoded_on = monarch_with_gil(GilSite::Test, |py| value.extract::<u64>(py)).await?;
            Ok::<_, PyErr>((driver, decoded_on))
        })
    })();

    // Stop only this test's actor, not the client's proc. Wait outside Tokio.
    let stopped = monarch_with_gil_blocking(GilSite::Test, |py| {
        let kwargs = PyDict::new(py);
        kwargs.set_item("timeout", 30)?;
        actor_mesh
            .bind(py)
            .call_method0("stop")?
            .call_method("get", (), Some(&kwargs))?;
        Ok::<_, PyErr>(())
    });
    let (driver, decoded_on) = observed?;
    stopped?;
    assert_eq!(
        driver, decoded_on,
        "native reply must decode on its Rust driver"
    );
    Ok(())
}
