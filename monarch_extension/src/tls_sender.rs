/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Rust-native TLS sender for remotemount.
//!
//! Packs files into an anonymous mmap, computes block hashes, then sends
//! dirty blocks over parallel TLS connections directly from the buffer —
//! no `/tmp` file, no Python sender actor processes.

use std::io::BufReader;
use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;
use std::thread;

use pyo3::exceptions::PyOSError;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use rustls::pki_types::ServerName;

use crate::fast_pack::FileInfo;
use crate::fast_pack::HASH_BLOCK_SIZE;
use crate::fast_pack::compute_block_hashes;
use crate::fast_pack::mmap_anonymous;
use crate::fast_pack::pack_files_into;

// Default paths used by the Python caller (monarch.remotemount).
// Kept here as documentation; actual values are passed via make_tls_config().
#[allow(dead_code)]
const DEFAULT_CA_PATH: &str = "/var/facebook/rootcanal/ca.pem";
#[allow(dead_code)]
const DEFAULT_CERT_PATH: &str = "/var/facebook/x509_identities/server.pem";

/// A `ServerCertVerifier` that accepts any server certificate.
///
/// Used when `ca_path` is not provided (e.g. self-signed certs in K8s pods).
#[derive(Debug)]
struct AcceptAnyCert;

impl rustls::client::danger::ServerCertVerifier for AcceptAnyCert {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Build a `rustls::ClientConfig`.
///
/// If `ca_path` is provided, server certs are verified against that CA.
/// If `ca_path` is `None`, any server cert is accepted (for self-signed certs).
/// If `cert_path` is provided, it's used for client auth; otherwise no client auth.
fn make_tls_config(
    cert_path: Option<&str>,
    ca_path: Option<&str>,
) -> Result<Arc<rustls::ClientConfig>, String> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let builder = rustls::ClientConfig::builder();

    let builder = match ca_path {
        Some(ca) => {
            let ca_pem = std::fs::read(ca).map_err(|e| format!("read {ca} failed: {e}"))?;
            let mut root_store = rustls::RootCertStore::empty();
            let ca_certs = rustls_pemfile::certs(&mut BufReader::new(&ca_pem[..]))
                .filter_map(Result::ok)
                .collect::<Vec<_>>();
            root_store.add_parsable_certificates(ca_certs);
            builder.with_root_certificates(root_store)
        }
        None => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyCert)),
    };

    let config = match cert_path {
        Some(cp) => {
            let cert_pem = std::fs::read(cp).map_err(|e| format!("read {cp} failed: {e}"))?;
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
                        Ok(None) => return Err(format!("no private key found in {cp}")),
                        Err(e) => return Err(format!("parse {cp} failed: {e}")),
                    }
                }
            };
            builder
                .with_client_auth_cert(certs, key)
                .map_err(|e| format!("client auth cert failed: {e}"))?
        }
        None => builder.with_no_client_auth(),
    };

    Ok(Arc::new(config))
}

/// RAII wrapper for an anonymous mmap buffer with precomputed block hashes.
#[pyclass(module = "monarch._rust_bindings.monarch_extension.tls_sender")]
struct PackedBuffer {
    ptr: *mut libc::c_void,
    size: usize,
    #[pyo3(get)]
    hashes: Vec<String>,
}

// SAFETY: the mmap region is process-global memory accessible from any thread.
// No thread-local state; all access is via raw pointer arithmetic on a
// process-wide anonymous mapping that outlives any thread.
unsafe impl Send for PackedBuffer {}
// SAFETY: PackedBuffer is read-only after construction (ptr/size are immutable).
// Concurrent readers access disjoint slices via offset-based indexing.
unsafe impl Sync for PackedBuffer {}

impl Drop for PackedBuffer {
    fn drop(&mut self) {
        if self.size > 0 {
            // SAFETY: self.ptr was returned by mmap_anonymous(self.size) and
            // has not been munmapped yet. Drop runs at most once.
            unsafe {
                libc::munmap(self.ptr, self.size);
            }
        }
    }
}

/// Send dirty blocks from a buffer over parallel TLS connections.
///
/// Each address in `addresses` gets its own TCP+TLS stream. Blocks
/// are distributed round-robin across streams.
fn send_blocks_impl(
    buf_ptr: usize,
    total_size: usize,
    dirty_blocks: &[usize],
    addresses: &[String],
    cache_path: &str,
    block_size: usize,
    tls_hostname: Option<&str>,
    cert_path: Option<&str>,
    ca_path: Option<&str>,
) -> PyResult<()> {
    let tls_config = make_tls_config(cert_path, ca_path).map_err(PyRuntimeError::new_err)?;

    let num_streams = addresses.len();

    // Partition blocks across streams round-robin.
    let mut per_stream: Vec<Vec<(usize, usize)>> = vec![Vec::new(); num_streams];
    for (i, &bi) in dirty_blocks.iter().enumerate() {
        let offset = bi.checked_mul(block_size).ok_or_else(|| {
            PyRuntimeError::new_err(format!("block {bi} * {block_size} overflows"))
        })?;
        if offset >= total_size {
            return Err(PyRuntimeError::new_err(format!(
                "block {bi} offset {offset} >= total_size {total_size}"
            )));
        }
        let size = std::cmp::min(block_size, total_size - offset);
        per_stream[i % num_streams].push((offset, size));
    }

    let cache_path_bytes = cache_path.as_bytes();

    thread::scope(|s| {
        let mut handles = Vec::with_capacity(num_streams);

        for (stream_idx, blocks) in per_stream.into_iter().enumerate() {
            let addr = &addresses[stream_idx];
            let config = Arc::clone(&tls_config);
            let cp_bytes = cache_path_bytes;
            let tls_name = tls_hostname;

            handles.push(s.spawn(move || -> Result<(), String> {
                let (host, port_str) = addr
                    .rsplit_once(':')
                    .ok_or_else(|| format!("invalid address: {addr}"))?;
                let port: u16 = port_str
                    .parse()
                    .map_err(|e| format!("invalid port in {addr}: {e}"))?;

                let tcp =
                    TcpStream::connect((host, port)).map_err(|e| format!("connect {addr}: {e}"))?;
                tcp.set_nodelay(true)
                    .map_err(|e| format!("set_nodelay: {e}"))?;

                // 4 MB send buffer for high-bandwidth transfers.
                #[cfg(unix)]
                {
                    use std::os::unix::io::AsRawFd;
                    let bufsize: libc::c_int = 4 * 1024 * 1024;
                    // SAFETY: setsockopt with SOL_SOCKET/SO_SNDBUF is safe;
                    // bufsize is a valid c_int on the stack.
                    unsafe {
                        libc::setsockopt(
                            tcp.as_raw_fd(),
                            libc::SOL_SOCKET,
                            libc::SO_SNDBUF,
                            &bufsize as *const _ as *const libc::c_void,
                            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                        );
                    }
                }

                let sni_host = tls_name.unwrap_or(host);
                let server_name = ServerName::try_from(sni_host.to_string())
                    .unwrap_or_else(|_| ServerName::try_from("localhost".to_string()).unwrap());

                let conn = rustls::ClientConnection::new(config, server_name)
                    .map_err(|e| format!("TLS handshake: {e}"))?;
                let mut tls = rustls::StreamOwned::new(conn, tcp);

                // Protocol header: cache_path_len(u32 BE) + cache_path + total_size(u64 BE)
                let mut header = Vec::with_capacity(4 + cp_bytes.len() + 8);
                header.extend_from_slice(&(cp_bytes.len() as u32).to_be_bytes());
                header.extend_from_slice(cp_bytes);
                header.extend_from_slice(&(total_size as u64).to_be_bytes());
                tls.write_all(&header)
                    .map_err(|e| format!("write header: {e}"))?;

                // Send blocks: offset(u64 BE) + size(u64 BE) + data
                for (offset, size) in &blocks {
                    let mut block_header = [0u8; 16];
                    block_header[..8].copy_from_slice(&(*offset as u64).to_be_bytes());
                    block_header[8..].copy_from_slice(&(*size as u64).to_be_bytes());
                    tls.write_all(&block_header)
                        .map_err(|e| format!("write block header: {e}"))?;

                    // SAFETY: offset + size <= total_size, buffer is valid.
                    let data = unsafe {
                        std::slice::from_raw_parts((buf_ptr + offset) as *const u8, *size)
                    };
                    tls.write_all(data)
                        .map_err(|e| format!("write block data: {e}"))?;
                }

                // Done sentinel: offset=0, size=0
                tls.write_all(&[0u8; 16])
                    .map_err(|e| format!("write sentinel: {e}"))?;
                // Flush all buffered TLS application data before closing.
                tls.flush().map_err(|e| format!("flush: {e}"))?;
                tls.conn.send_close_notify();
                // Drain the close_notify record to TCP.
                while tls.conn.wants_write() {
                    tls.conn
                        .write_tls(&mut tls.sock)
                        .map_err(|e| format!("close_notify: {e}"))?;
                }
                // Shut down the write half of TCP to send FIN cleanly.
                tls.sock
                    .shutdown(std::net::Shutdown::Write)
                    .map_err(|e| format!("tcp shutdown: {e}"))?;

                // Drain the receive buffer (e.g. TLS 1.3 NewSessionTicket
                // messages the server sent after the handshake). If unread
                // data remains when close() is called, the kernel sends
                // TCP RST instead of FIN, which can truncate in-flight
                // application data at the receiver.
                tls.sock
                    .set_read_timeout(Some(std::time::Duration::from_millis(200)))
                    .ok();
                let mut drain = [0u8; 4096];
                loop {
                    match tls.sock.read(&mut drain) {
                        Ok(0) => break,
                        Ok(_) => continue,
                        Err(_) => break,
                    }
                }

                Ok(())
            }));
        }

        let mut errors = Vec::new();
        for h in handles {
            if let Err(e) = h.join().expect("sender thread panicked") {
                errors.push(e);
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(PyRuntimeError::new_err(errors.join("; ")))
        }
    })
}

#[pymethods]
impl PackedBuffer {
    /// Send dirty blocks over parallel TLS connections.
    ///
    /// Each address in `addresses` gets its own TCP+TLS stream. Blocks
    /// are distributed round-robin across streams. The wire protocol
    /// matches the Python `SenderShardActor` exactly.
    #[pyo3(signature = (dirty_blocks, addresses, cache_path, hash_block_size=None, tls_hostname=None, cert_path=None, ca_path=None))]
    fn send_blocks(
        &self,
        py: Python<'_>,
        dirty_blocks: Vec<usize>,
        addresses: Vec<String>,
        cache_path: String,
        hash_block_size: Option<usize>,
        tls_hostname: Option<String>,
        cert_path: Option<String>,
        ca_path: Option<String>,
    ) -> PyResult<()> {
        let block_size = hash_block_size.unwrap_or(HASH_BLOCK_SIZE);
        let buf_ptr = self.ptr as usize;
        let total_size = self.size;

        py.detach(move || {
            send_blocks_impl(
                buf_ptr,
                total_size,
                &dirty_blocks,
                &addresses,
                &cache_path,
                block_size,
                tls_hostname.as_deref(),
                cert_path.as_deref(),
                ca_path.as_deref(),
            )
        })
    }
}

/// Send dirty blocks from an existing buffer over parallel TLS connections.
///
/// Like `PackedBuffer.send_blocks()` but operates on any buffer (e.g. a
/// memoryview from `pack_directory_chunked`), avoiding a second pack step.
#[pyfunction]
#[pyo3(signature = (buffer, total_size, dirty_blocks, addresses, cache_path, hash_block_size=None, tls_hostname=None, cert_path=None, ca_path=None))]
fn send_blocks_from_buffer(
    py: Python<'_>,
    buffer: pyo3::buffer::PyBuffer<u8>,
    total_size: usize,
    dirty_blocks: Vec<usize>,
    addresses: Vec<String>,
    cache_path: String,
    hash_block_size: Option<usize>,
    tls_hostname: Option<String>,
    cert_path: Option<String>,
    ca_path: Option<String>,
) -> PyResult<()> {
    let block_size = hash_block_size.unwrap_or(HASH_BLOCK_SIZE);
    let buf_ptr = buffer.buf_ptr() as usize;

    py.detach(move || {
        send_blocks_impl(
            buf_ptr,
            total_size,
            &dirty_blocks,
            &addresses,
            &cache_path,
            block_size,
            tls_hostname.as_deref(),
            cert_path.as_deref(),
            ca_path.as_deref(),
        )
    })
}

/// Pack files into anonymous mmap and compute block hashes.
///
/// Returns a `PackedBuffer` whose `.hashes` attribute holds hex-encoded
/// xxh64 digests for each 100 MB block.
#[pyfunction]
#[pyo3(signature = (file_list, total_size, hash_block_size=None))]
fn pack_and_hash(
    py: Python<'_>,
    file_list: Vec<(String, usize, usize)>,
    total_size: usize,
    hash_block_size: Option<usize>,
) -> PyResult<PackedBuffer> {
    let block_size = hash_block_size.unwrap_or(HASH_BLOCK_SIZE);

    if file_list.is_empty() || total_size == 0 {
        return Ok(PackedBuffer {
            ptr: std::ptr::null_mut(),
            size: 0,
            hashes: Vec::new(),
        });
    }

    let files: Vec<FileInfo> = file_list
        .into_iter()
        .map(|(path, offset, size)| FileInfo { path, offset, size })
        .collect();

    let buffer =
        mmap_anonymous(total_size).map_err(|e| PyOSError::new_err(format!("mmap failed: {e}")))?;

    let buf_ptr = buffer as usize;

    let hashes = py.detach(move || {
        let nthreads = std::thread::available_parallelism()
            .map(|n| n.get().min(16))
            .unwrap_or(1);
        pack_files_into(buf_ptr, &files, nthreads);
        compute_block_hashes(buf_ptr, total_size, block_size, nthreads)
    });

    Ok(PackedBuffer {
        ptr: buffer,
        size: total_size,
        hashes,
    })
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PackedBuffer>()?;
    let f = wrap_pyfunction!(pack_and_hash, module)?;
    f.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_extension.tls_sender",
    )?;
    module.add_function(f)?;
    let f2 = wrap_pyfunction!(send_blocks_from_buffer, module)?;
    f2.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_extension.tls_sender",
    )?;
    module.add_function(f2)?;
    Ok(())
}
