/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Rust-native TLS receiver for remotemount.
//!
//! Accepts parallel TLS connections and writes received blocks directly
//! into a caller-provided anonymous mmap buffer — no intermediate file,
//! no Python SSL overhead.

use std::io::BufReader;
use std::io::Read;
use std::net::TcpListener;
use std::sync::Arc;
use std::thread;

use pyo3::exceptions::PyRuntimeError;
use pyo3::ffi;
use pyo3::prelude::*;

const DEFAULT_CERT_PATH: &str = "/var/facebook/x509_identities/server.pem";

/// Build a `rustls::ServerConfig` matching the Python `_make_server_ssl_context`.
fn make_server_tls_config(cert_path: &str) -> Result<Arc<rustls::ServerConfig>, String> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cert_pem = std::fs::read(cert_path).map_err(|e| format!("read {cert_path} failed: {e}"))?;

    let certs = rustls_pemfile::certs(&mut BufReader::new(&cert_pem[..]))
        .filter_map(Result::ok)
        .collect::<Vec<_>>();

    let key = {
        let mut reader = BufReader::new(&cert_pem[..]);
        loop {
            match rustls_pemfile::read_one(&mut reader) {
                Ok(Some(rustls_pemfile::Item::Pkcs1Key(k))) => {
                    break rustls::pki_types::PrivateKeyDer::Pkcs1(k);
                }
                Ok(Some(rustls_pemfile::Item::Pkcs8Key(k))) => {
                    break rustls::pki_types::PrivateKeyDer::Pkcs8(k);
                }
                Ok(Some(rustls_pemfile::Item::Sec1Key(k))) => {
                    break rustls::pki_types::PrivateKeyDer::Sec1(k);
                }
                Ok(Some(_)) => continue,
                Ok(None) => return Err(format!("no private key found in {cert_path}")),
                Err(e) => return Err(format!("parse {cert_path} failed: {e}")),
            }
        }
    };

    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| format!("server cert failed: {e}"))?;

    Ok(Arc::new(config))
}

/// Return a hostname that the TLS certificate is valid for.
///
/// `hostname -f` can return a name that doesn't match the cert's SAN
/// (e.g. `.fbinfra.net` vs `.facebook.com` on Sandcastle).  We use
/// `openssl x509 -text` to dump the cert and extract the first DNS
/// SAN entry.  Falls back to `hostname -f` if extraction fails.
fn get_tls_hostname(cert_path: &str) -> Result<String, String> {
    // `openssl x509 -text` is supported since OpenSSL 0.9.x (unlike
    // `-ext subjectAltName` which requires 1.1.1+).  The SAN section
    // looks like:
    //     X509v3 Subject Alternative Name:
    //         DNS:host.facebook.com, IP Address:..., ...
    if let Ok(output) = std::process::Command::new("openssl")
        .args(["x509", "-in", cert_path, "-noout", "-text"])
        .output()
    {
        if output.status.success() {
            let text = String::from_utf8_lossy(&output.stdout);
            let mut in_san = false;
            for line in text.lines() {
                let trimmed = line.trim();
                if trimmed.contains("Subject Alternative Name") {
                    in_san = true;
                    continue;
                }
                if in_san {
                    // This line has the SAN values, comma-separated.
                    for part in trimmed.split(',') {
                        let part = part.trim();
                        if let Some(dns) = part.strip_prefix("DNS:") {
                            return Ok(dns.trim().to_string());
                        }
                    }
                    break; // Only check the line immediately after the header.
                }
            }
        }
    }

    // Fallback: hostname -f.
    let output = std::process::Command::new("hostname")
        .arg("-f")
        .output()
        .map_err(|e| format!("hostname -f failed: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "hostname -f returned {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Return the FQDN of this host (for TCP connect address).
fn get_fqdn() -> Result<String, String> {
    let output = std::process::Command::new("hostname")
        .arg("-f")
        .output()
        .map_err(|e| format!("hostname -f failed: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "hostname -f returned {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Rust TLS receiver that writes directly into a caller-provided buffer.
#[pyclass(module = "monarch._rust_bindings.monarch_extension.tls_receiver")]
struct TlsReceiver {
    /// "host:port" for the Rust sender to connect to.
    #[pyo3(get)]
    addr: String,
    /// Hostname from the cert's SAN for TLS verification (may differ
    /// from the connect hostname when DNS names diverge, e.g.
    /// `.fbinfra.net` vs `.facebook.com`).
    #[pyo3(get)]
    tls_hostname: String,
    listener: Option<TcpListener>,
    tls_config: Arc<rustls::ServerConfig>,
    num_streams: usize,
}

#[pymethods]
impl TlsReceiver {
    #[new]
    #[pyo3(signature = (num_streams=1, cert_path=None, port=0))]
    fn new(num_streams: usize, cert_path: Option<&str>, port: u16) -> PyResult<Self> {
        let cert = cert_path.unwrap_or(DEFAULT_CERT_PATH);
        let tls_config = make_server_tls_config(cert).map_err(PyRuntimeError::new_err)?;

        let listener = TcpListener::bind(format!("[::]:{port}"))
            .map_err(|e| PyRuntimeError::new_err(format!("bind: {e}")))?;
        let port = listener
            .local_addr()
            .map_err(|e| PyRuntimeError::new_err(format!("addr: {e}")))?
            .port();

        let tls_hostname = get_tls_hostname(cert).map_err(PyRuntimeError::new_err)?;
        let connect_hostname = get_fqdn().map_err(PyRuntimeError::new_err)?;

        Ok(TlsReceiver {
            addr: format!("{connect_hostname}:{port}"),
            tls_hostname,
            listener: Some(listener),
            tls_config,
            num_streams,
        })
    }

    /// Accept connections and receive blocks into `buffer`.
    ///
    /// Blocks until all `num_streams` connections have completed.
    /// The buffer must be a writable object supporting the buffer protocol
    /// (e.g. a memoryview over an anonymous mmap).
    fn wait(&mut self, py: Python<'_>, buffer: &Bound<'_, PyAny>) -> PyResult<()> {
        // Extract raw pointer and length from the Python buffer protocol.
        // SAFETY: Py_buffer is a POD struct; zeroing is valid initialization.
        let mut buf_view: ffi::Py_buffer = unsafe { std::mem::zeroed() };
        // SAFETY: buffer.as_ptr() is a valid Python object; we check rc != 0.
        let rc = unsafe {
            ffi::PyObject_GetBuffer(
                buffer.as_ptr(),
                &mut buf_view,
                ffi::PyBUF_WRITABLE | ffi::PyBUF_SIMPLE,
            )
        };
        if rc != 0 {
            return Err(PyRuntimeError::new_err(
                "buffer is not writable or does not support the buffer protocol",
            ));
        }
        let buf_ptr = buf_view.buf as usize;
        let buf_len = buf_view.len as usize;
        // SAFETY: buf_view was successfully initialized by PyObject_GetBuffer.
        unsafe { ffi::PyBuffer_Release(&mut buf_view) };

        let listener = self
            .listener
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("wait() already called"))?;
        let config = Arc::clone(&self.tls_config);
        let num_streams = self.num_streams;

        py.detach(move || {
            thread::scope(|s| {
                let mut handles = Vec::with_capacity(num_streams);
                let listener_ref = &listener;

                for _ in 0..num_streams {
                    let cfg = Arc::clone(&config);

                    handles.push(s.spawn(move || -> Result<usize, String> {
                        let (tcp, _) = listener_ref.accept().map_err(|e| format!("accept: {e}"))?;
                        tcp.set_nodelay(true).ok();

                        // 4 MB receive buffer for high-bandwidth transfers.
                        #[cfg(unix)]
                        {
                            use std::os::unix::io::AsRawFd;
                            let bufsize: libc::c_int = 4 * 1024 * 1024;
                            // SAFETY: setsockopt with SOL_SOCKET/SO_RCVBUF is safe;
                            // bufsize is a valid c_int on the stack.
                            unsafe {
                                libc::setsockopt(
                                    tcp.as_raw_fd(),
                                    libc::SOL_SOCKET,
                                    libc::SO_RCVBUF,
                                    &bufsize as *const _ as *const libc::c_void,
                                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                                );
                            }
                        }

                        let conn = rustls::ServerConnection::new(cfg)
                            .map_err(|e| format!("TLS accept: {e}"))?;
                        let mut tls = rustls::StreamOwned::new(conn, tcp);

                        // Read header: cache_path_len(u32) + cache_path + total_size(u64).
                        // We read and discard cache_path (not needed for anonymous mmap).
                        let mut hdr = [0u8; 4];
                        tls.read_exact(&mut hdr)
                            .map_err(|e| format!("read path_len: {e}"))?;
                        let path_len = u32::from_be_bytes(hdr) as usize;
                        let mut path_buf = vec![0u8; path_len];
                        tls.read_exact(&mut path_buf)
                            .map_err(|e| format!("read path: {e}"))?;
                        let mut size_buf = [0u8; 8];
                        tls.read_exact(&mut size_buf)
                            .map_err(|e| format!("read size: {e}"))?;
                        let total_size = u64::from_be_bytes(size_buf) as usize;
                        if total_size != buf_len {
                            return Err(format!(
                                "protocol error: sender total_size={total_size} != \
                                 receiver buf_len={buf_len}"
                            ));
                        }

                        // Read blocks: offset(u64) + size(u64) + data.
                        let mut blocks = 0usize;
                        loop {
                            let mut block_hdr = [0u8; 16];
                            match tls.read_exact(&mut block_hdr) {
                                Ok(()) => {}
                                // EOF before any header byte means the sender is done
                                // (can happen if sender closes after sentinel).
                                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                                    break;
                                }
                                Err(e) => {
                                    return Err(format!("read block header: {e}"));
                                }
                            }
                            let offset =
                                u64::from_be_bytes(block_hdr[..8].try_into().unwrap()) as usize;
                            let size =
                                u64::from_be_bytes(block_hdr[8..].try_into().unwrap()) as usize;
                            if size == 0 {
                                break; // sentinel
                            }

                            let end = offset.checked_add(size).ok_or_else(|| {
                                format!("block offset {offset} + size {size} overflows usize")
                            })?;
                            if end > buf_len {
                                return Err(format!(
                                    "block at offset {offset} size {size} \
                                     exceeds buffer length {buf_len}"
                                ));
                            }

                            // SAFETY: offset + size <= buf_len, buffer is valid.
                            let dst = unsafe {
                                std::slice::from_raw_parts_mut((buf_ptr + offset) as *mut u8, size)
                            };
                            tls.read_exact(dst)
                                .map_err(|e| format!("read block data: {e}"))?;
                            blocks += 1;
                        }
                        Ok(blocks)
                    }));
                }

                let mut errors = Vec::new();
                for h in handles {
                    if let Err(e) = h.join().expect("receiver thread panicked") {
                        errors.push(e);
                    }
                }
                if errors.is_empty() {
                    Ok(())
                } else {
                    Err(PyRuntimeError::new_err(errors.join("; ")))
                }
            })
        })
    }
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<TlsReceiver>()?;
    Ok(())
}
