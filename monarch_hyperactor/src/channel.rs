/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::str::FromStr;

use hyperactor::channel::ChannelAddr;
use hyperactor::channel::ChannelTransport;
use hyperactor::channel::TlsMode;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

/// Python binding for [`hyperactor::channel::ChannelTransport`]
#[pyclass(
    name = "ChannelTransport",
    module = "monarch._rust_bindings.monarch_hyperactor.channel",
    eq
)]
#[derive(PartialEq, Clone, Copy, Debug)]
pub enum PyChannelTransport {
    Tcp,
    MetaTlsWithHostname,
    MetaTlsWithIpV6,
    Local,
    Unix,
    // Sim(/*transport:*/ ChannelTransport), TODO kiuk@ add support
}

#[pymethods]
impl PyChannelTransport {
    fn get(&self) -> Self {
        self.clone()
    }
}

impl TryFrom<ChannelTransport> for PyChannelTransport {
    type Error = PyErr;

    fn try_from(transport: ChannelTransport) -> PyResult<Self> {
        match transport {
            ChannelTransport::Tcp => Ok(PyChannelTransport::Tcp),
            ChannelTransport::MetaTls(TlsMode::Hostname) => {
                Ok(PyChannelTransport::MetaTlsWithHostname)
            }
            ChannelTransport::MetaTls(TlsMode::IpV6) => Ok(PyChannelTransport::MetaTlsWithIpV6),
            ChannelTransport::Local => Ok(PyChannelTransport::Local),
            ChannelTransport::Unix => Ok(PyChannelTransport::Unix),
            _ => Err(PyValueError::new_err(format!(
                "unsupported transport: {}",
                transport
            ))),
        }
    }
}

#[pyclass(
    name = "ChannelAddr",
    module = "monarch._rust_bindings.monarch_hyperactor.channel"
)]
pub struct PyChannelAddr {
    inner: ChannelAddr,
}

impl FromStr for PyChannelAddr {
    type Err = anyhow::Error;
    fn from_str(addr: &str) -> anyhow::Result<Self> {
        let inner = ChannelAddr::from_str(addr)?;
        Ok(Self { inner })
    }
}

#[pymethods]
impl PyChannelAddr {
    /// Returns an "any" address for the given transport type.
    /// Primarily used to bind servers. Returned string form of the address
    /// is of the format `{transport}!{address}`. For example:
    /// `tcp![::]:0`, `unix!@a0b1c2d3`, `metatls!devgpu001.pci.facebook.com:0`
    #[staticmethod]
    pub fn any(transport: PyChannelTransport) -> PyResult<String> {
        Ok(ChannelAddr::any(transport.into()).to_string())
    }

    #[staticmethod]
    pub fn parse(addr: &str) -> PyResult<Self> {
        Ok(PyChannelAddr::from_str(addr)?)
    }

    /// Returns the port number (if any) of this channel address,
    /// `0` for transports for which unix ports do not apply (e.g. `unix`, `local`)
    pub fn get_port(&self) -> PyResult<u16> {
        match self.inner {
            ChannelAddr::Tcp(socket_addr) => Ok(socket_addr.port()),
            ChannelAddr::MetaTls(_, port) => Ok(port),
            ChannelAddr::Unix(_) | ChannelAddr::Local(_) => Ok(0),
            _ => Err(PyRuntimeError::new_err(format!(
                "unsupported transport: `{:?}` for channel address: `{}`",
                self.inner.transport(),
                self.inner
            ))),
        }
    }

    /// Returns the channel transport of this channel address.
    pub fn get_transport(&self) -> PyResult<PyChannelTransport> {
        let transport = self.inner.transport();
        match transport {
            ChannelTransport::Tcp => Ok(PyChannelTransport::Tcp),
            ChannelTransport::MetaTls(mode) => match mode {
                TlsMode::Hostname => Ok(PyChannelTransport::MetaTlsWithHostname),
                TlsMode::IpV6 => Ok(PyChannelTransport::MetaTlsWithIpV6),
            },
            ChannelTransport::Local => Ok(PyChannelTransport::Local),
            ChannelTransport::Unix => Ok(PyChannelTransport::Unix),
            _ => Err(PyRuntimeError::new_err(format!(
                "unsupported transport: `{:?}` for address: `{}`",
                self.inner.transport(),
                self.inner
            ))),
        }
    }
}

impl From<PyChannelTransport> for ChannelTransport {
    fn from(val: PyChannelTransport) -> Self {
        match val {
            PyChannelTransport::Tcp => ChannelTransport::Tcp,
            PyChannelTransport::MetaTlsWithHostname => ChannelTransport::MetaTls(TlsMode::Hostname),
            PyChannelTransport::MetaTlsWithIpV6 => ChannelTransport::MetaTls(TlsMode::IpV6),
            PyChannelTransport::Local => ChannelTransport::Local,
            PyChannelTransport::Unix => ChannelTransport::Unix,
        }
    }
}

#[pymodule]
pub fn register_python_bindings(hyperactor_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    hyperactor_mod.add_class::<PyChannelTransport>()?;
    hyperactor_mod.add_class::<PyChannelAddr>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel_any_and_parse() -> PyResult<()> {
        // just make sure any() and parse() calls work for all transports
        for transport in [
            PyChannelTransport::Tcp,
            PyChannelTransport::Unix,
            PyChannelTransport::MetaTlsWithHostname,
            PyChannelTransport::MetaTlsWithIpV6,
            PyChannelTransport::Local,
        ] {
            let address = PyChannelAddr::any(transport)?;
            let _ = PyChannelAddr::parse(&address)?;
        }
        Ok(())
    }

    #[test]
    fn test_channel_unsupported_transport() -> PyResult<()> {
        let sim_addr = ChannelAddr::any(ChannelTransport::Sim(Box::new(ChannelTransport::Unix)));
        let addr = PyChannelAddr { inner: sim_addr };

        assert!(addr.get_port().is_err());
        assert!(addr.get_transport().is_err());
        Ok(())
    }

    #[test]
    fn test_channel_addr_get_port() -> PyResult<()> {
        assert_eq!(PyChannelAddr::parse("tcp![::]:26600")?.get_port()?, 26600);
        assert_eq!(
            PyChannelAddr::parse("metatls!devgpu1.pci.facebook.com:26600")?.get_port()?,
            26600
        );
        assert_eq!(PyChannelAddr::parse("local!12345")?.get_port()?, 0);
        assert_eq!(PyChannelAddr::parse("unix!@1a2b3c")?.get_port()?, 0);
        Ok(())
    }

    #[test]
    fn test_channel_addr_get_transport() -> PyResult<()> {
        assert_eq!(
            PyChannelAddr::parse("tcp![::]:26600")?.get_transport()?,
            PyChannelTransport::Tcp
        );
        assert_eq!(
            PyChannelAddr::parse("metatls!devgpu001.pci.facebook.com:26600")?.get_transport()?,
            PyChannelTransport::MetaTlsWithHostname
        );
        assert_eq!(
            PyChannelAddr::parse("metatls!::1:26600")?.get_transport()?,
            PyChannelTransport::MetaTlsWithIpV6
        );
        assert_eq!(
            // IpV4 will fallback to hostname
            PyChannelAddr::parse("metatls!127.0.0.1:26600")?.get_transport()?,
            PyChannelTransport::MetaTlsWithHostname
        );
        assert_eq!(
            PyChannelAddr::parse("local!12345")?.get_transport()?,
            PyChannelTransport::Local
        );
        assert_eq!(
            PyChannelAddr::parse("unix!@1a2b3c")?.get_transport()?,
            PyChannelTransport::Unix
        );
        Ok(())
    }
}
