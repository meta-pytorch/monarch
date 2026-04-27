/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use flate2::Compression;
use flate2::write::GzEncoder;
use sha2::Digest;
use sha2::Sha256;
use tempfile::TempDir;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tool_fetch::ArtifactFormat;
use tool_fetch::HashAlgorithm;
use tool_fetch::Platform;
use tool_fetch::PlatformEntry;
use tool_fetch::Provider;
use tool_fetch::ProvisionError;
use tool_fetch::ToolCache;
use tool_fetch::ToolSpec;
use tool_fetch::current_platform;

fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn spec_for(
    format: ArtifactFormat,
    bytes: &[u8],
    executable_path: Option<&str>,
    url: String,
) -> ToolSpec {
    ToolSpec {
        name: "demo-tool".to_string(),
        version: "1.0.0".to_string(),
        platforms: HashMap::from([(
            Platform::MacosAarch64,
            PlatformEntry {
                size: bytes.len() as u64,
                hash_algorithm: HashAlgorithm::Sha256,
                digest: digest(bytes),
                format,
                executable_path: executable_path.map(Into::into),
                providers: vec![Provider::Http { url }],
            },
        )]),
    }
}

async fn serve_bytes(bytes: Vec<u8>) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let requests = Arc::new(AtomicUsize::new(0));
    let request_count = requests.clone();

    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            request_count.fetch_add(1, Ordering::SeqCst);
            let bytes = bytes.clone();
            tokio::spawn(async move {
                let mut buffer = [0_u8; 1024];
                let _ = socket.read(&mut buffer).await;
                let headers = format!(
                    "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    bytes.len()
                );
                socket.write_all(headers.as_bytes()).await.unwrap();
                socket.write_all(&bytes).await.unwrap();
            });
        }
    });

    (format!("http://{addr}/artifact"), requests)
}

fn tar_gz(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let mut tar_bytes = Vec::new();
    {
        let mut builder = tar::Builder::new(&mut tar_bytes);
        for (path, data) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_path(path).unwrap();
            header.set_size(data.len() as u64);
            header.set_mode(0o755);
            header.set_cksum();
            builder.append(&header, *data).unwrap();
        }
        builder.finish().unwrap();
    }

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&tar_bytes).unwrap();
    encoder.finish().unwrap()
}

fn malicious_tar_symlink() -> Vec<u8> {
    let mut tar_bytes = Vec::new();
    {
        let mut builder = tar::Builder::new(&mut tar_bytes);
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Symlink);
        header.set_path("link").unwrap();
        header.set_link_name("../outside").unwrap();
        header.set_size(0);
        header.set_cksum();
        builder.append(&header, std::io::empty()).unwrap();
        builder.finish().unwrap();
    }

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&tar_bytes).unwrap();
    encoder.finish().unwrap()
}

fn raw_tar_gz(path: &str, data: &[u8]) -> Vec<u8> {
    let mut tar_bytes = Vec::new();
    let mut header = [0_u8; 512];
    let path_bytes = path.as_bytes();
    header[..path_bytes.len()].copy_from_slice(path_bytes);
    header[100..108].copy_from_slice(b"0000755\0");
    header[108..116].copy_from_slice(b"0000000\0");
    header[116..124].copy_from_slice(b"0000000\0");
    let size = format!("{:011o}\0", data.len());
    header[124..136].copy_from_slice(size.as_bytes());
    header[136..148].copy_from_slice(b"00000000000\0");
    header[148..156].fill(b' ');
    header[156] = b'0';
    header[257..263].copy_from_slice(b"ustar\0");
    header[263..265].copy_from_slice(b"00");
    let checksum: u32 = header.iter().map(|b| *b as u32).sum();
    let checksum = format!("{:06o}\0 ", checksum);
    header[148..156].copy_from_slice(checksum.as_bytes());

    tar_bytes.extend_from_slice(&header);
    tar_bytes.extend_from_slice(data);
    let padding = (512 - (data.len() % 512)) % 512;
    tar_bytes.extend(std::iter::repeat_n(0, padding));
    tar_bytes.extend([0_u8; 1024]);

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&tar_bytes).unwrap();
    encoder.finish().unwrap()
}

fn zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let cursor = std::io::Cursor::new(Vec::new());
    let mut writer = zip::ZipWriter::new(cursor);
    let options = zip::write::SimpleFileOptions::default().unix_permissions(0o755);
    for (path, data) in entries {
        writer.start_file(path, options).unwrap();
        writer.write_all(data).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

fn assert_executable(path: &Path) {
    assert!(path.is_file());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_ne!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o111,
            0
        );
    }
}

#[tokio::test]
async fn parses_bundled_pyspy_spec() {
    // TF-SPEC-1, TF-PLAT-2: bundled specs deserialize into normalized
    // platform keys used by provision().
    let spec: ToolSpec = serde_json::from_str(include_str!("../specs/py-spy.json")).unwrap();
    assert_eq!(spec.name, "py-spy");
    assert_eq!(spec.version, "0.4.1");
    assert!(spec.platforms.contains_key(&Platform::MacosAarch64));
    assert!(spec.platforms.contains_key(&Platform::LinuxX86_64));
}

#[tokio::test]
async fn provision_tar_gz_extracts_and_resolves() {
    // TF-FETCH-1, TF-CACHE-2, TF-EXTRACT-1, TF-EXTRACT-4.
    let artifact = tar_gz(&[("bin/demo-tool", b"#!/bin/sh\n")]);
    let (url, _) = serve_bytes(artifact.clone()).await;
    let spec = spec_for(ArtifactFormat::TarGz, &artifact, Some("bin/demo-tool"), url);
    let temp = TempDir::new().unwrap();
    let cache = ToolCache::new(temp.path());

    let executable = cache
        .provision(&spec, Platform::MacosAarch64)
        .await
        .unwrap();

    assert!(executable.ends_with("bin/demo-tool"));
    assert_executable(&executable);
}

#[tokio::test]
async fn provision_zip_extracts_and_resolves() {
    // TF-FETCH-1, TF-CACHE-2, TF-EXTRACT-1, TF-EXTRACT-3, TF-EXTRACT-4.
    let artifact = zip(&[("bundle/bin/demo-tool", b"#!/bin/sh\n")]);
    let (url, _) = serve_bytes(artifact.clone()).await;
    let spec = spec_for(
        ArtifactFormat::Zip,
        &artifact,
        Some("bundle/bin/demo-tool"),
        url,
    );
    let temp = TempDir::new().unwrap();
    let cache = ToolCache::new(temp.path());

    let executable = cache
        .provision(&spec, Platform::MacosAarch64)
        .await
        .unwrap();

    assert!(executable.ends_with("bundle/bin/demo-tool"));
    assert_executable(&executable);
}

#[tokio::test]
async fn provision_plain_installs_to_bin() {
    // TF-SPEC-3, TF-CACHE-2, TF-EXTRACT-4: plain artifacts install to
    // extracted/.../bin/{name}; blobs are not returned as executables.
    let artifact = b"#!/bin/sh\n".to_vec();
    let (url, _) = serve_bytes(artifact.clone()).await;
    let spec = spec_for(ArtifactFormat::Plain, &artifact, None, url);
    let temp = TempDir::new().unwrap();
    let cache = ToolCache::new(temp.path());

    let executable = cache
        .provision(&spec, Platform::MacosAarch64)
        .await
        .unwrap();

    assert!(executable.ends_with("bin/demo-tool"));
    assert_executable(&executable);
}

#[tokio::test]
async fn provision_plain_managed_executable_runs() {
    // TF-CACHE-2, TF-CACHE-7, TF-EXTRACT-4: the returned path is the
    // managed executable under extracted/ and can be executed directly.
    let artifact = b"#!/bin/sh\nprintf 'managed:%s\\n' \"$1\"\n".to_vec();
    let (url, _) = serve_bytes(artifact.clone()).await;
    let spec = spec_for(ArtifactFormat::Plain, &artifact, None, url);
    let temp = TempDir::new().unwrap();
    let cache = ToolCache::new(temp.path());

    let executable = cache
        .provision(&spec, Platform::MacosAarch64)
        .await
        .unwrap();
    let output = tokio::process::Command::new(&executable)
        .arg("ok")
        .output()
        .await
        .unwrap();

    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout), "managed:ok\n");
}

#[tokio::test]
async fn cache_hit_does_not_redownload() {
    // TF-CACHE-3: second provision resolves from metadata/extracted
    // state without contacting the provider.
    let artifact = zip(&[("bin/demo-tool", b"#!/bin/sh\n")]);
    let (url, requests) = serve_bytes(artifact.clone()).await;
    let spec = spec_for(ArtifactFormat::Zip, &artifact, Some("bin/demo-tool"), url);
    let temp = TempDir::new().unwrap();
    let cache = ToolCache::new(temp.path());

    let first = cache
        .provision(&spec, Platform::MacosAarch64)
        .await
        .unwrap();
    let second = cache
        .provision(&spec, Platform::MacosAarch64)
        .await
        .unwrap();

    assert_eq!(first, second);
    assert_eq!(requests.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn hash_mismatch_rejects_download() {
    // TF-FETCH-1, TF-FETCH-2: same-sized wrong bytes fail digest
    // verification before install/extract.
    let artifact = b"AAAA".to_vec();
    let (url, _) = serve_bytes(artifact).await;
    let declared = b"BBBB".to_vec();
    let spec = spec_for(ArtifactFormat::Plain, &declared, None, url);
    let temp = TempDir::new().unwrap();
    let cache = ToolCache::new(temp.path());

    let error = cache
        .provision(&spec, Platform::MacosAarch64)
        .await
        .unwrap_err();

    assert!(matches!(error, ProvisionError::HashMismatch { .. }));
}

#[tokio::test]
async fn missing_executable_path_errors() {
    // TF-ERR-1: archive succeeds but missing executable maps to the
    // structured ExecutableMissing variant.
    let artifact = zip(&[("bin/other", b"#!/bin/sh\n")]);
    let (url, _) = serve_bytes(artifact.clone()).await;
    let spec = spec_for(ArtifactFormat::Zip, &artifact, Some("bin/demo-tool"), url);
    let temp = TempDir::new().unwrap();
    let cache = ToolCache::new(temp.path());

    let error = cache
        .provision(&spec, Platform::MacosAarch64)
        .await
        .unwrap_err();

    assert!(matches!(error, ProvisionError::ExecutableMissing { .. }));
}

#[tokio::test]
async fn malicious_tar_absolute_path_rejected() {
    // TF-EXTRACT-1: absolute tar paths cannot escape the extraction root.
    let artifact = raw_tar_gz("/tmp/evil", b"bad");
    let (url, _) = serve_bytes(artifact.clone()).await;
    let spec = spec_for(ArtifactFormat::TarGz, &artifact, Some("tmp/evil"), url);
    let temp = TempDir::new().unwrap();
    let cache = ToolCache::new(temp.path());

    let error = cache
        .provision(&spec, Platform::MacosAarch64)
        .await
        .unwrap_err();

    assert!(matches!(error, ProvisionError::UnsafeArchiveEntry { .. }));
}

#[tokio::test]
async fn malicious_tar_parent_traversal_rejected() {
    // TF-EXTRACT-1: parent traversal is rejected before writing.
    let artifact = raw_tar_gz("../evil", b"bad");
    let (url, _) = serve_bytes(artifact.clone()).await;
    let spec = spec_for(ArtifactFormat::TarGz, &artifact, Some("evil"), url);
    let temp = TempDir::new().unwrap();
    let cache = ToolCache::new(temp.path());

    let error = cache
        .provision(&spec, Platform::MacosAarch64)
        .await
        .unwrap_err();

    assert!(matches!(error, ProvisionError::UnsafeArchiveEntry { .. }));
}

#[tokio::test]
async fn malicious_tar_symlink_rejected() {
    // TF-EXTRACT-2: tar links are rejected instead of resolved.
    let artifact = malicious_tar_symlink();
    let (url, _) = serve_bytes(artifact.clone()).await;
    let spec = spec_for(ArtifactFormat::TarGz, &artifact, Some("link"), url);
    let temp = TempDir::new().unwrap();
    let cache = ToolCache::new(temp.path());

    let error = cache
        .provision(&spec, Platform::MacosAarch64)
        .await
        .unwrap_err();

    assert!(matches!(error, ProvisionError::UnsafeArchiveEntry { .. }));
}

#[tokio::test]
async fn malicious_zip_parent_traversal_rejected() {
    // TF-EXTRACT-1, TF-EXTRACT-3: zip paths are constrained to the
    // extraction root.
    let artifact = zip(&[("../evil", b"bad")]);
    let (url, _) = serve_bytes(artifact.clone()).await;
    let spec = spec_for(ArtifactFormat::Zip, &artifact, Some("evil"), url);
    let temp = TempDir::new().unwrap();
    let cache = ToolCache::new(temp.path());

    let error = cache
        .provision(&spec, Platform::MacosAarch64)
        .await
        .unwrap_err();

    assert!(matches!(error, ProvisionError::UnsafeArchiveEntry { .. }));
}

#[tokio::test]
async fn metadata_sidecar_and_scan_work() {
    // TF-CACHE-4: scan reports only metadata-backed installs with an
    // existing executable.
    let artifact = zip(&[("bin/demo-tool", b"#!/bin/sh\n")]);
    let (url, _) = serve_bytes(artifact.clone()).await;
    let spec = spec_for(ArtifactFormat::Zip, &artifact, Some("bin/demo-tool"), url);
    let temp = TempDir::new().unwrap();
    let cache = ToolCache::new(temp.path());

    let executable = cache
        .provision(&spec, Platform::MacosAarch64)
        .await
        .unwrap();
    let artifacts = cache.scan();

    assert_eq!(artifacts.len(), 1);
    assert_eq!(artifacts[0].name, "demo-tool");
    assert_eq!(artifacts[0].version, "1.0.0");
    assert_eq!(artifacts[0].platform, Platform::MacosAarch64);
    assert_eq!(artifacts[0].executable, executable);
}

#[tokio::test]
async fn metadata_executable_path_must_stay_inside_extracted_tree() {
    // TF-CACHE-6: metadata cannot redirect lookup/scan to an
    // executable outside the managed extraction directory.
    let artifact = zip(&[("bin/demo-tool", b"#!/bin/sh\n")]);
    let (url, _) = serve_bytes(artifact.clone()).await;
    let spec = spec_for(ArtifactFormat::Zip, &artifact, Some("bin/demo-tool"), url);
    let entry = spec.platforms.get(&Platform::MacosAarch64).unwrap();
    let temp = TempDir::new().unwrap();
    let cache = ToolCache::new(temp.path());

    cache
        .provision(&spec, Platform::MacosAarch64)
        .await
        .unwrap();
    let metadata_path = temp
        .path()
        .join("extracted")
        .join(&entry.digest[..2])
        .join(&entry.digest)
        .join(".tool-fetch-metadata.json");
    let mut metadata: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&metadata_path).unwrap()).unwrap();
    metadata["executable_path"] = serde_json::Value::String("/bin/sh".to_string());
    std::fs::write(
        &metadata_path,
        serde_json::to_vec_pretty(&metadata).unwrap(),
    )
    .unwrap();

    assert_eq!(cache.lookup(entry), None);
    assert!(cache.scan().is_empty());
}

#[tokio::test]
#[ignore = "downloads the real py-spy wheel for the current platform"]
async fn provision_real_pyspy_and_run_version() {
    // End-to-end capstone for TF-FETCH-1, TF-CACHE-2, TF-EXTRACT-3,
    // and TF-EXTRACT-4 against the real py-spy wheel for this host.
    let spec: ToolSpec = serde_json::from_str(include_str!("../specs/py-spy.json")).unwrap();
    let platform = current_platform().unwrap();
    if !spec.platforms.contains_key(&platform) {
        eprintln!("skipping: bundled py-spy spec has no entry for {platform:?}");
        return;
    }

    let temp = TempDir::new().unwrap();
    let cache = ToolCache::new(temp.path());
    let executable = cache.provision(&spec, platform).await.unwrap();
    let output = tokio::process::Command::new(&executable)
        .arg("--version")
        .output()
        .await
        .unwrap();

    assert!(
        output.status.success(),
        "py-spy --version failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("py-spy 0.4.1"), "stdout was {stdout:?}");
}
