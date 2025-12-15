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
use hyperactor::channel::MetaTlsAddr;
use hyperactor::channel::TcpMode;
use hyperactor::channel::TlsMode;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyTypeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

/// Python binding for [`hyperactor::channel::ChannelTransport`]
///
/// This enum represents the basic transport types that can be represented
/// as simple enum variants. For more complex transports like `Explicit`,
/// use string-based configuration via `PyChannelTransportConfig`.
#[pyclass(
    name = "ChannelTransport",
    module = "monarch._rust_bindings.monarch_hyperactor.channel",
    eq
)]
#[derive(PartialEq, Clone, Copy, Debug)]
pub enum PyChannelTransport {
    TcpWithLocalhost,
    TcpWithHostname,
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
            ChannelTransport::Tcp(TcpMode::Localhost) => Ok(PyChannelTransport::TcpWithLocalhost),
            ChannelTransport::Tcp(TcpMode::Hostname) => Ok(PyChannelTransport::TcpWithHostname),
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

/// A wrapper for ChannelTransport that can be created from either a
/// PyChannelTransport enum or a string (for explicit transport).
///
/// We need this wrapper because Python's enum type does not support attaching
/// data to enum variants like Rust does.
#[pyclass(
    name = "ChannelTransportConfig",
    module = "monarch._rust_bindings.monarch_hyperactor.channel"
)]
#[derive(Clone, Debug, PartialEq)]
pub struct PyChannelTransportConfig {
    inner: ChannelTransport,
}

#[pymethods]
impl PyChannelTransportConfig {
    /// Create a new PyChannelTransportConfig from either a ChannelTransport enum
    /// or a string representation.
    ///
    /// Examples:
    ///     PyChannelTransportConfig(ChannelTransport.Unix)
    ///     PyChannelTransportConfig("explicit:tcp://127.0.0.1:8080")
    #[new]
    pub fn new(transport: &Bound<'_, PyAny>) -> PyResult<Self> {
        // First try to extract as PyChannelTransportConfig (for when passing an existing config)
        if let Ok(config) = transport.extract::<PyChannelTransportConfig>() {
            return Ok(config);
        }

        // Then try to extract as PyChannelTransport enum
        if let Ok(py_transport) = transport.extract::<PyChannelTransport>() {
            return Ok(PyChannelTransportConfig {
                inner: py_transport.into(),
            });
        }

        // Then try to extract as a string and parse it
        if let Ok(transport_str) = transport.extract::<String>() {
            if !transport_str.starts_with("explicit:") {
                return Err(PyValueError::new_err(format!(
                    "string argument only supports explicit transport with \
                    address in the zmq url format (e.g., 'explicit:tcp://127.0.0.1:8080'); \
                    but got: {}",
                    transport_str,
                )));
            }
            let addr_str = transport_str.strip_prefix("explicit:").unwrap();
            let addr = ChannelAddr::from_zmq_url(addr_str).map_err(|e| {
                PyValueError::new_err(format!(
                    "invalid address string used for explicit transport '{}': {}",
                    addr_str, e
                ))
            })?;
            return Ok(PyChannelTransportConfig {
                inner: ChannelTransport::Explicit(addr),
            });
        }

        Err(PyTypeError::new_err(
            "expected ChannelTransport enum, ChannelTransportConfig, or str",
        ))
    }

    fn __str__(&self) -> String {
        self.inner.to_string()
    }

    fn __repr__(&self) -> String {
        format!("PyChannelTransportConfig({:?})", self.inner)
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl From<PyChannelTransportConfig> for ChannelTransport {
    fn from(config: PyChannelTransportConfig) -> Self {
        config.inner
    }
}

impl From<ChannelTransport> for PyChannelTransportConfig {
    fn from(transport: ChannelTransport) -> Self {
        PyChannelTransportConfig { inner: transport }
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
            ChannelAddr::Tcp(socket_addr)
            | ChannelAddr::MetaTls(MetaTlsAddr::Socket(socket_addr)) => Ok(socket_addr.port()),
            ChannelAddr::MetaTls(MetaTlsAddr::Host { port, .. }) => Ok(port),
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
            ChannelTransport::Tcp(mode) => match mode {
                TcpMode::Localhost => Ok(PyChannelTransport::TcpWithLocalhost),
                TcpMode::Hostname => Ok(PyChannelTransport::TcpWithHostname),
            },
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
            PyChannelTransport::TcpWithLocalhost => ChannelTransport::Tcp(TcpMode::Localhost),
            PyChannelTransport::TcpWithHostname => ChannelTransport::Tcp(TcpMode::Hostname),
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
    hyperactor_mod.add_class::<PyChannelTransportConfig>()?;
    hyperactor_mod.add_class::<PyChannelAddr>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // TODO: OSS: failed to retrieve ipv6 address
    #[cfg_attr(not(fbcode_build), ignore)]
    fn test_channel_any_and_parse() -> PyResult<()> {
        // just make sure any() and parse() calls work for all transports
        for transport in [
            PyChannelTransport::TcpWithLocalhost,
            PyChannelTransport::TcpWithHostname,
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
            PyChannelAddr::parse("tcp![::1]:26600")?.get_transport()?,
            PyChannelTransport::TcpWithLocalhost,
        );
        assert_eq!(
            PyChannelAddr::parse("tcp![::]:26600")?.get_transport()?,
            PyChannelTransport::TcpWithHostname,
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
