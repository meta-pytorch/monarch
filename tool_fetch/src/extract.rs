//! Safe archive extraction and plain-artifact installation.
//!
//! This module owns the filesystem safety boundary for archive
//! contents. Fetching verifies artifact bytes; extraction ensures those
//! bytes cannot write outside the intended install tree.
//!
//! # Invariants
//!
//! - **TF-EXTRACT-1 (archive-contained):** Every archive path is joined
//!   through `safe_join`, rejecting absolute paths, roots/prefixes, and
//!   `..`.
//! - **TF-EXTRACT-2 (tar-links-rejected):** Tar symlinks and hardlinks
//!   are rejected rather than followed.
//! - **TF-EXTRACT-3 (zip-regular-only):** Zip extraction writes only
//!   regular files and directories; symlink-like entries are rejected.
//! - **TF-EXTRACT-4 (unix-executable):** Installed/resolved executables
//!   get executable bits on Unix.

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fs;
use std::io;
use std::path::Component;
use std::path::Path;
use std::path::PathBuf;

use crate::ProvisionError;

fn safe_join(root: &Path, path: &Path) -> Result<PathBuf, ProvisionError> {
    if path.is_absolute() {
        return Err(ProvisionError::UnsafeArchiveEntry {
            path: path.display().to_string(),
            reason: "absolute paths are not allowed".to_string(),
        });
    }

    let mut out = root.to_path_buf();
    for component in path.components() {
        match component {
            Component::Normal(part) => out.push(part),
            Component::CurDir => {}
            Component::ParentDir => {
                return Err(ProvisionError::UnsafeArchiveEntry {
                    path: path.display().to_string(),
                    reason: "parent directory traversal is not allowed".to_string(),
                });
            }
            Component::Prefix(_) | Component::RootDir => {
                return Err(ProvisionError::UnsafeArchiveEntry {
                    path: path.display().to_string(),
                    reason: "root or prefix components are not allowed".to_string(),
                });
            }
        }
    }
    Ok(out)
}

/// Extract a gzip-compressed tar archive into `destination`.
///
/// Enforces TF-EXTRACT-1 and TF-EXTRACT-2.
pub(crate) fn extract_tar_gz(blob: &Path, destination: &Path) -> Result<(), ProvisionError> {
    let file = fs::File::open(blob)?;
    let decoder = flate2::read::GzDecoder::new(file);
    let mut archive = tar::Archive::new(decoder);

    for entry in archive
        .entries()
        .map_err(|e| ProvisionError::ExtractionFailed {
            error: e.to_string(),
        })?
    {
        let mut entry = entry.map_err(|e| ProvisionError::ExtractionFailed {
            error: e.to_string(),
        })?;
        let entry_type = entry.header().entry_type();
        let path = entry.path().map_err(|e| ProvisionError::ExtractionFailed {
            error: e.to_string(),
        })?;
        let path = path.as_ref().to_path_buf();

        if entry_type.is_symlink() || entry_type.is_hard_link() {
            return Err(ProvisionError::UnsafeArchiveEntry {
                path: path.display().to_string(),
                reason: "links are not allowed".to_string(),
            });
        }

        let out = safe_join(destination, &path)?;
        if entry_type.is_dir() {
            fs::create_dir_all(&out)?;
        } else if entry_type.is_file() {
            if let Some(parent) = out.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut output = fs::File::create(&out)?;
            io::copy(&mut entry, &mut output).map_err(|e| ProvisionError::ExtractionFailed {
                error: e.to_string(),
            })?;
        } else {
            return Err(ProvisionError::UnsafeArchiveEntry {
                path: path.display().to_string(),
                reason: format!("unsupported tar entry type {:?}", entry_type),
            });
        }
    }

    Ok(())
}

/// Extract a zip archive into `destination`.
///
/// Enforces TF-EXTRACT-1 and TF-EXTRACT-3.
pub(crate) fn extract_zip(blob: &Path, destination: &Path) -> Result<(), ProvisionError> {
    let file = fs::File::open(blob)?;
    let mut archive = zip::ZipArchive::new(file).map_err(|e| ProvisionError::ExtractionFailed {
        error: e.to_string(),
    })?;

    for i in 0..archive.len() {
        let mut entry = archive
            .by_index(i)
            .map_err(|e| ProvisionError::ExtractionFailed {
                error: e.to_string(),
            })?;
        let Some(enclosed_name) = entry.enclosed_name() else {
            return Err(ProvisionError::UnsafeArchiveEntry {
                path: entry.name().to_string(),
                reason: "zip entry escapes extraction root".to_string(),
            });
        };
        let out = safe_join(destination, &enclosed_name)?;

        if is_zip_symlink(&entry) {
            return Err(ProvisionError::UnsafeArchiveEntry {
                path: entry.name().to_string(),
                reason: "zip symlinks are not allowed".to_string(),
            });
        }

        if entry.is_dir() {
            fs::create_dir_all(&out)?;
        } else if entry.is_file() {
            if let Some(parent) = out.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut output = fs::File::create(&out)?;
            io::copy(&mut entry, &mut output).map_err(|e| ProvisionError::ExtractionFailed {
                error: e.to_string(),
            })?;
        } else {
            return Err(ProvisionError::UnsafeArchiveEntry {
                path: entry.name().to_string(),
                reason: "zip entry is not a regular file or directory".to_string(),
            });
        }
    }

    Ok(())
}

fn is_zip_symlink(entry: &zip::read::ZipFile<'_>) -> bool {
    entry
        .unix_mode()
        .is_some_and(|mode| (mode & 0o170000) == 0o120000)
}

/// Install a plain downloaded artifact as `bin/{name}`.
///
/// Enforces TF-CACHE-2 and TF-EXTRACT-4 for non-archive artifacts.
pub(crate) fn install_plain(
    blob: &Path,
    destination: &Path,
    name: &str,
) -> Result<PathBuf, ProvisionError> {
    let bin_dir = destination.join("bin");
    fs::create_dir_all(&bin_dir)?;
    let executable = bin_dir.join(name);
    fs::copy(blob, &executable)?;
    make_executable(&executable)?;
    Ok(executable)
}

/// Mark `path` executable on Unix.
///
/// No-op on non-Unix platforms. Enforces TF-EXTRACT-4.
pub(crate) fn make_executable(path: &Path) -> Result<(), ProvisionError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = fs::metadata(path)?.permissions();
        permissions.set_mode(permissions.mode() | 0o111);
        fs::set_permissions(path, permissions)?;
    }
    Ok(())
}
