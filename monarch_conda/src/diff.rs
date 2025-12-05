/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::path::Path;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use anyhow::Result;
use anyhow::ensure;
use digest::Digest;
use digest::Output;
use serde::Deserialize;
use serde::Serialize;
use sha2::Sha256;
use tokio::fs;

use crate::hash_utils;
use crate::pack_meta_history::History;
use crate::pack_meta_history::Offsets;

/// Fingerprint of the conda-meta directory, used by `CondaFingerprint` below.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CondaMetaFingerprint {
    // TODO(agallagher): It might be worth storing more information of installed
    // packages, so that we could print better error messages when we detect two
    // envs are not equivalent.
    hash: Output<Sha256>,
}

impl CondaMetaFingerprint {
    async fn from_env(path: &Path) -> Result<Self> {
        let mut hasher = Sha256::new();
        hash_utils::hash_directory_tree(&path.join("conda-meta"), &mut hasher).await?;
        Ok(Self {
            hash: hasher.finalize(),
        })
    }
}

/// Fingerprint of the pack-meta directory, used by `CondaFingerprint` below.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PackMetaFingerprint {
    offsets: Output<Sha256>,
    pub history: History,
}

impl PackMetaFingerprint {
    async fn from_env(path: &Path) -> Result<Self> {
        let pack_meta = path.join("pack-meta");

        // Read the fulle history.jsonl file.
        let contents = fs::read_to_string(pack_meta.join("history.jsonl")).await?;
        let history = History::from_contents(&contents)?;

        // Read entire offsets.jsonl file, but avoid hashing the offsets, which can change.
        let mut hasher = Sha256::new();
        let contents = fs::read_to_string(pack_meta.join("offsets.jsonl")).await?;
        let offsets = Offsets::from_contents(&contents)?;
        for ent in offsets.entries {
            let contents = bincode::serialize(&(ent.path, ent.mode, ent.offsets.len()))?;
            hasher.update(contents.len().to_le_bytes());
            hasher.update(&contents);
        }
        let offsets = hasher.finalize();

        Ok(Self { history, offsets })
    }
}

/// A fingerprint of a conda environment, used to detect if two envs are similar enough to
/// facilitate mtime-based conda syncing.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CondaFingerprint {
    pub conda_meta: CondaMetaFingerprint,
    pub pack_meta: PackMetaFingerprint,
}

impl CondaFingerprint {
    pub async fn from_env(path: &Path) -> Result<Self> {
        Ok(Self {
            conda_meta: CondaMetaFingerprint::from_env(path).await?,
            pack_meta: PackMetaFingerprint::from_env(path).await?,
        })
    }

    /// Create a comparator to compare the mtimes of files from two "equivalent" conda envs.
    /// In particular, thie comparator will be aware of spuriuos mtime changes that occurs from
    /// prefix replacement (via `meta-pack`), and will filter them out.
    pub fn mtime_comparator(
        a: &Self,
        b: &Self,
    ) -> Result<Box<dyn Fn(&SystemTime, &SystemTime) -> std::cmp::Ordering + Send + Sync>> {
        let (a_prefix, a_base) = a.pack_meta.history.first()?;
        let (b_prefix, b_base) = b.pack_meta.history.first()?;
        ensure!(a_prefix == b_prefix);

        // NOTE(agallagher): There appears to be some mtime drift on some files after fbpkg creation,
        // so acccount for that here.
        let slop = Duration::from_secs(5 * 60);

        // We load the timestamp from the first history entry, and use this to see if any
        // files have been updated since the env was created.
        let a_base = UNIX_EPOCH + Duration::from_secs(a_base) + slop;
        let b_base = UNIX_EPOCH + Duration::from_secs(b_base) + slop;

        // We also load the last prefix update window for each, as any mtimes from this window
        // should be ignored.
        let a_window = a
            .pack_meta
            .history
            .prefix_and_last_update_window()?
            .1
            .map(|(s, e)| {
                (
                    UNIX_EPOCH + Duration::from_secs(s),
                    UNIX_EPOCH + Duration::from_secs(e + 1),
                )
            });
        let b_window = b
            .pack_meta
            .history
            .prefix_and_last_update_window()?
            .1
            .map(|(s, e)| {
                (
                    UNIX_EPOCH + Duration::from_secs(s),
                    UNIX_EPOCH + Duration::from_secs(e + 1),
                )
            });

        Ok(Box::new(move |a: &SystemTime, b: &SystemTime| {
            match (
                *a > a_base && a_window.is_none_or(|(s, e)| *a < s || *a > e),
                *b > b_base && b_window.is_none_or(|(s, e)| *b < s || *b > e),
            ) {
                (true, false) => std::cmp::Ordering::Greater,
                (false, true) => std::cmp::Ordering::Less,
                (false, false) => std::cmp::Ordering::Equal,
                (true, true) => a.cmp(b),
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::SystemTime;

    use anyhow::Result;
    use rattler_conda_types::package::FileMode;
    use tempfile::TempDir;
    use tokio::fs;

    use super::*;
    use crate::pack_meta_history::HistoryRecord;
    use crate::pack_meta_history::Offset;
    use crate::pack_meta_history::OffsetRecord;

    /// Helper function to create a conda environment with configurable packages and files
    async fn setup_conda_env_with_config(
        temp_dir: &TempDir,
        env_name: &str,
        base_time: SystemTime,
        prefix: &str,
        packages: &[(&str, &str, &str)], // (name, version, build)
        include_update_window: bool,
    ) -> Result<PathBuf> {
        let env_path = temp_dir.path().join(env_name);
        fs::create_dir_all(&env_path).await?;

        // Create conda-meta directory with package files
        let conda_meta_path = env_path.join("conda-meta");
        fs::create_dir_all(&conda_meta_path).await?;

        // Create conda package metadata files for each package
        for (name, version, build) in packages {
            let package_json = format!(
                r#"{{
                    "name": "{}",
                    "version": "{}",
                    "build": "{}",
                    "build_number": 0,
                    "paths_data": {{
                        "paths": [
                            {{
                                "path": "lib/{}.so",
                                "path_type": "hardlink",
                                "size_in_bytes": 1024,
                                "mode": "binary"
                            }}
                        ]
                    }},
                    "repodata_record": {{
                        "package_record": {{
                            "timestamp": {}
                        }}
                    }}
                }}"#,
                name,
                version,
                build,
                name,
                base_time.duration_since(UNIX_EPOCH)?.as_secs()
            );

            fs::write(
                conda_meta_path.join(format!("{}-{}-{}.json", name, version, build)),
                package_json,
            )
            .await?;
        }

        // Create pack-meta directory and files
        let pack_meta_path = env_path.join("pack-meta");
        fs::create_dir_all(&pack_meta_path).await?;

        // Create history.jsonl file
        create_history_file(&pack_meta_path, prefix, base_time, include_update_window).await?;

        // Create offsets.jsonl file
        create_offsets_file(&pack_meta_path, packages).await?;

        Ok(env_path)
    }

    /// Helper function to create a simple conda environment
    async fn create_dummy_conda_env(temp_dir: &TempDir, env_name: &str) -> Result<PathBuf> {
        let base_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1640995200);
        let default_packages = &[
            ("python", "3.9.0", "h12debd9_1"),
            ("numpy", "1.21.0", "py39h7a9d4c0_0"),
        ];
        setup_conda_env_with_config(
            temp_dir,
            env_name,
            base_time,
            "/opt/conda",
            default_packages,
            false,
        )
        .await
    }

    /// Helper function to create a history.jsonl file
    async fn create_history_file(
        pack_meta_path: &Path,
        prefix: &str,
        base_time: SystemTime,
        include_update_window: bool,
    ) -> Result<()> {
        let base_timestamp = base_time.duration_since(UNIX_EPOCH)?.as_secs();
        let mut history = History { entries: vec![] };

        // Add initial history entry
        history.entries.push(HistoryRecord {
            timestamp: base_timestamp,
            prefix: PathBuf::from(prefix),
            finished: true,
        });

        // Optionally add an update window (start and end entries)
        if include_update_window {
            history.entries.push(HistoryRecord {
                timestamp: base_timestamp + 3600, // 1 hour later
                prefix: PathBuf::from(prefix),
                finished: false,
            });
            history.entries.push(HistoryRecord {
                timestamp: base_timestamp + 3660, // 1 minute after start
                prefix: PathBuf::from(prefix),
                finished: true,
            });
        }

        fs::write(pack_meta_path.join("history.jsonl"), history.to_str()?).await?;
        Ok(())
    }

    /// Helper function to create an offsets.jsonl file
    async fn create_offsets_file(
        pack_meta_path: &Path,
        packages: &[(&str, &str, &str)],
    ) -> Result<()> {
        let mut offset_entries = Vec::new();

        // Add default entries for common files
        offset_entries.push(OffsetRecord {
            path: PathBuf::from("bin/python"),
            mode: FileMode::Binary,
            offsets: vec![Offset {
                start: 0,
                len: 1024,
                contents: None,
            }],
        });

        // Add entries for each package
        for (name, _, _) in packages {
            offset_entries.push(OffsetRecord {
                path: PathBuf::from(format!("lib/{}.so", name)),
                mode: FileMode::Binary,
                offsets: vec![Offset {
                    start: 0,
                    len: 1024,
                    contents: None,
                }],
            });
        }

        let offsets = Offsets {
            entries: offset_entries,
        };
        fs::write(pack_meta_path.join("offsets.jsonl"), offsets.to_str()?).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_conda_fingerprint_creation() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let env_path = create_dummy_conda_env(&temp_dir, "test_env").await?;

        // Create fingerprint
        let fingerprint = CondaFingerprint::from_env(&env_path).await?;

        // Verify that the fingerprint was created successfully
        assert_eq!(fingerprint.pack_meta.history.entries.len(), 1);
        assert_eq!(
            fingerprint.pack_meta.history.entries[0].timestamp,
            1640995200
        );
        assert_eq!(
            fingerprint.pack_meta.history.entries[0].prefix,
            PathBuf::from("/opt/conda")
        );
        assert!(fingerprint.pack_meta.history.entries[0].finished);

        Ok(())
    }

    #[tokio::test]
    async fn test_conda_fingerprint_equality() -> Result<()> {
        let temp_dir = TempDir::new()?;

        // Create two identical environments
        let env1_path = create_dummy_conda_env(&temp_dir, "env1").await?;
        let env2_path = create_dummy_conda_env(&temp_dir, "env2").await?;

        let base_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1640995200);
        let default_packages = &[
            ("python", "3.9.0", "h12debd9_1"),
            ("numpy", "1.21.0", "py39h7a9d4c0_0"),
        ];
        for env_path in [&env1_path, &env2_path] {
            let pack_meta_path = env_path.join("pack-meta");
            create_history_file(&pack_meta_path, "/opt/conda", base_time, false).await?;
            create_offsets_file(&pack_meta_path, default_packages).await?;
        }

        // Create fingerprints
        let fingerprint1 = CondaFingerprint::from_env(&env1_path).await?;
        let fingerprint2 = CondaFingerprint::from_env(&env2_path).await?;

        // They should be equal since they have identical content
        assert_eq!(fingerprint1, fingerprint2);

        Ok(())
    }

    #[tokio::test]
    async fn test_conda_fingerprint_inequality_different_packages() -> Result<()> {
        let temp_dir = TempDir::new()?;

        // Create two environments with different packages
        let env1_path = create_dummy_conda_env(&temp_dir, "env1").await?;
        let env2_path = create_dummy_conda_env(&temp_dir, "env2").await?;

        // Add an extra package to env2
        let conda_meta2_path = env2_path.join("conda-meta");
        fs::write(
            conda_meta2_path.join("scipy-1.7.0-py39h7a9d4c0_0.json"),
            r#"{"name": "scipy", "version": "1.7.0", "build": "py39h7a9d4c0_0"}"#,
        )
        .await?;

        let base_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1640995200);
        let default_packages = &[
            ("python", "3.9.0", "h12debd9_1"),
            ("numpy", "1.21.0", "py39h7a9d4c0_0"),
        ];
        for env_path in [&env1_path, &env2_path] {
            let pack_meta_path = env_path.join("pack-meta");
            create_history_file(&pack_meta_path, "/opt/conda", base_time, false).await?;
            create_offsets_file(&pack_meta_path, default_packages).await?;
        }

        // Create fingerprints
        let fingerprint1 = CondaFingerprint::from_env(&env1_path).await?;
        let fingerprint2 = CondaFingerprint::from_env(&env2_path).await?;

        // They should be different due to different packages
        assert_ne!(fingerprint1, fingerprint2);

        Ok(())
    }

    #[tokio::test]
    async fn test_mtime_comparator_basic() -> Result<()> {
        let temp_dir = TempDir::new()?;

        // Create two identical environments
        let env1_path = create_dummy_conda_env(&temp_dir, "env1").await?;
        let env2_path = create_dummy_conda_env(&temp_dir, "env2").await?;

        let base_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1640995200);
        let default_packages = &[
            ("python", "3.9.0", "h12debd9_1"),
            ("numpy", "1.21.0", "py39h7a9d4c0_0"),
        ];
        for env_path in [&env1_path, &env2_path] {
            let pack_meta_path = env_path.join("pack-meta");
            create_history_file(&pack_meta_path, "/opt/conda", base_time, false).await?;
            create_offsets_file(&pack_meta_path, default_packages).await?;
        }

        // Create fingerprints
        let fingerprint1 = CondaFingerprint::from_env(&env1_path).await?;
        let fingerprint2 = CondaFingerprint::from_env(&env2_path).await?;

        // Create mtime comparator
        let comparator = CondaFingerprint::mtime_comparator(&fingerprint1, &fingerprint2)?;

        // Test various mtime scenarios
        let base_timestamp = 1640995200;
        let test_base_time = UNIX_EPOCH + Duration::from_secs(base_timestamp);
        let old_time = test_base_time - Duration::from_hours(1); // 1 hour before base
        let new_time = test_base_time + Duration::from_secs(7200); // 2 hours after base (beyond slop)

        // Files older than base should be considered equal
        assert_eq!(comparator(&old_time, &old_time), std::cmp::Ordering::Equal);

        // File newer than base vs old file
        assert_eq!(
            comparator(&new_time, &old_time),
            std::cmp::Ordering::Greater
        );
        assert_eq!(comparator(&old_time, &new_time), std::cmp::Ordering::Less);

        // Both files newer than base should compare normally
        let newer_time = new_time + Duration::from_mins(30);
        assert_eq!(comparator(&new_time, &newer_time), std::cmp::Ordering::Less);
        assert_eq!(
            comparator(&newer_time, &new_time),
            std::cmp::Ordering::Greater
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_mtime_comparator_with_update_window() -> Result<()> {
        let temp_dir = TempDir::new()?;

        // Create two identical environments
        let env1_path = create_dummy_conda_env(&temp_dir, "env1").await?;
        let env2_path = create_dummy_conda_env(&temp_dir, "env2").await?;

        let base_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1640995200);
        let default_packages = &[
            ("python", "3.9.0", "h12debd9_1"),
            ("numpy", "1.21.0", "py39h7a9d4c0_0"),
        ];
        for env_path in [&env1_path, &env2_path] {
            let pack_meta_path = env_path.join("pack-meta");
            // Include update window this time
            create_history_file(&pack_meta_path, "/opt/conda", base_time, true).await?;
            create_offsets_file(&pack_meta_path, default_packages).await?;
        }

        // Create fingerprints
        let fingerprint1 = CondaFingerprint::from_env(&env1_path).await?;
        let fingerprint2 = CondaFingerprint::from_env(&env2_path).await?;

        // Create mtime comparator
        let comparator = CondaFingerprint::mtime_comparator(&fingerprint1, &fingerprint2)?;

        let base_timestamp = 1640995200;
        let test_base_time = UNIX_EPOCH + Duration::from_secs(base_timestamp);
        let update_start = test_base_time + Duration::from_hours(1); // Update window start
        let update_end = test_base_time + Duration::from_mins(61); // Update window end
        let in_window_time = update_start + Duration::from_secs(30); // Inside update window
        let after_window_time = update_end + Duration::from_hours(1); // After update window

        // Files with mtimes in the update window should be ignored (treated as equal to old files)
        let old_time = base_time - Duration::from_hours(1);
        assert_eq!(
            comparator(&in_window_time, &old_time),
            std::cmp::Ordering::Equal
        );
        assert_eq!(
            comparator(&old_time, &in_window_time),
            std::cmp::Ordering::Equal
        );

        // Files after the update window should be considered newer
        assert_eq!(
            comparator(&after_window_time, &old_time),
            std::cmp::Ordering::Greater
        );
        assert_eq!(
            comparator(&old_time, &after_window_time),
            std::cmp::Ordering::Less
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_mtime_comparator_different_prefixes_fails() -> Result<()> {
        let temp_dir = TempDir::new()?;

        // Create two environments with different prefixes
        let env1_path = create_dummy_conda_env(&temp_dir, "env1").await?;
        let env2_path = create_dummy_conda_env(&temp_dir, "env2").await?;

        let base_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1640995200);
        let default_packages = &[
            ("python", "3.9.0", "h12debd9_1"),
            ("numpy", "1.21.0", "py39h7a9d4c0_0"),
        ];

        // Different prefixes
        create_history_file(
            &env1_path.join("pack-meta"),
            "/opt/conda1",
            base_time,
            false,
        )
        .await?;
        create_history_file(
            &env2_path.join("pack-meta"),
            "/opt/conda2",
            base_time,
            false,
        )
        .await?;

        for env_path in [&env1_path, &env2_path] {
            create_offsets_file(&env_path.join("pack-meta"), default_packages).await?;
        }

        // Create fingerprints
        let fingerprint1 = CondaFingerprint::from_env(&env1_path).await?;
        let fingerprint2 = CondaFingerprint::from_env(&env2_path).await?;

        // mtime_comparator should fail due to different prefixes
        let result = CondaFingerprint::mtime_comparator(&fingerprint1, &fingerprint2);
        assert!(result.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_pack_meta_fingerprint_offsets_hashing() -> Result<()> {
        let temp_dir = TempDir::new()?;

        // Create two environments with identical structure but different offset values
        let env1_path = create_dummy_conda_env(&temp_dir, "env1").await?;
        let env2_path = create_dummy_conda_env(&temp_dir, "env2").await?;

        let base_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1640995200);
        for env_path in [&env1_path, &env2_path] {
            let pack_meta_path = env_path.join("pack-meta");
            create_history_file(&pack_meta_path, "/opt/conda", base_time, false).await?;
        }

        // Create identical offset files
        let default_packages = &[
            ("python", "3.9.0", "h12debd9_1"),
            ("numpy", "1.21.0", "py39h7a9d4c0_0"),
        ];
        for env_path in [&env1_path, &env2_path] {
            create_offsets_file(&env_path.join("pack-meta"), default_packages).await?;
        }

        // Create fingerprints
        let fingerprint1 = CondaFingerprint::from_env(&env1_path).await?;
        let fingerprint2 = CondaFingerprint::from_env(&env2_path).await?;

        // Pack meta fingerprints should be identical for identical offset structures
        assert_eq!(fingerprint1.pack_meta, fingerprint2.pack_meta);

        // Now create env3 with different offset structure (different number of offsets)
        let env3_path = create_dummy_conda_env(&temp_dir, "env3").await?;
        let pack_meta3_path = env3_path.join("pack-meta");
        create_history_file(&pack_meta3_path, "/opt/conda", base_time, false).await?;

        // Create offsets with different structure
        let different_offsets = Offsets {
            entries: vec![OffsetRecord {
                path: PathBuf::from("bin/python"),
                mode: FileMode::Binary,
                offsets: vec![
                    Offset {
                        start: 0,
                        len: 1024,
                        contents: None,
                    },
                    Offset {
                        start: 2048,
                        len: 512,
                        contents: None,
                    }, // Extra offset
                ],
            }],
        };
        fs::write(
            pack_meta3_path.join("offsets.jsonl"),
            different_offsets.to_str()?,
        )
        .await?;

        let fingerprint3 = CondaFingerprint::from_env(&env3_path).await?;

        // Pack meta fingerprints should be different due to different offset structure
        assert_ne!(fingerprint1.pack_meta, fingerprint3.pack_meta);

        Ok(())
    }

    #[tokio::test]
    async fn test_mtime_comparator_complex_scenarios() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let base_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1640995200);

        // Create environments with multiple update windows
        let packages = &[
            ("python", "3.9.0", "h12debd9_1"),
            ("numpy", "1.21.0", "py39h7a9d4c0_0"),
            ("scipy", "1.7.0", "py39h0123456_0"),
        ];

        let env1_path = setup_conda_env_with_config(
            &temp_dir,
            "env1",
            base_time,
            "/opt/conda",
            packages,
            true, // Include update window
        )
        .await?;

        let env2_path = setup_conda_env_with_config(
            &temp_dir,
            "env2",
            base_time,
            "/opt/conda",
            packages,
            true, // Include update window
        )
        .await?;

        // Create fingerprints
        let fingerprint1 = CondaFingerprint::from_env(&env1_path).await?;
        let fingerprint2 = CondaFingerprint::from_env(&env2_path).await?;

        // Create mtime comparator
        let comparator = CondaFingerprint::mtime_comparator(&fingerprint1, &fingerprint2)?;

        // Test edge cases around the slop period (5 minutes)
        let base_timestamp = base_time.duration_since(UNIX_EPOCH)?.as_secs();
        let slop = Duration::from_secs(5 * 60);
        let base_plus_slop = UNIX_EPOCH + Duration::from_secs(base_timestamp) + slop;

        // Times just before and after the slop period
        let just_before_slop = base_plus_slop - Duration::from_secs(30);
        let just_after_slop = base_plus_slop + Duration::from_secs(30);

        // Files just before slop should be equal
        assert_eq!(
            comparator(&just_before_slop, &just_before_slop),
            std::cmp::Ordering::Equal
        );

        // Files after slop should be considered newer
        assert_eq!(
            comparator(&just_after_slop, &just_before_slop),
            std::cmp::Ordering::Greater
        );
        assert_eq!(
            comparator(&just_before_slop, &just_after_slop),
            std::cmp::Ordering::Less
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_mtime_comparator_mixed_update_windows() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let base_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1640995200);

        // Create one environment with update window and one without
        let packages = &[("python", "3.9.0", "h12debd9_1")];

        let env1_path = setup_conda_env_with_config(
            &temp_dir,
            "env1",
            base_time,
            "/opt/conda",
            packages,
            true, // Has update window
        )
        .await?;

        let env2_path = setup_conda_env_with_config(
            &temp_dir,
            "env2",
            base_time,
            "/opt/conda",
            packages,
            false, // No update window
        )
        .await?;

        // Create fingerprints
        let fingerprint1 = CondaFingerprint::from_env(&env1_path).await?;
        let fingerprint2 = CondaFingerprint::from_env(&env2_path).await?;

        // Create mtime comparator
        let comparator = CondaFingerprint::mtime_comparator(&fingerprint1, &fingerprint2)?;

        let base_timestamp = base_time.duration_since(UNIX_EPOCH)?.as_secs();
        let update_window_time = UNIX_EPOCH + Duration::from_secs(base_timestamp + 3630); // In the middle of update window
        let old_time = base_time - Duration::from_hours(1);
        let new_time = base_time + Duration::from_secs(7200);

        // When update_window_time is the first arg (env1 context with update window),
        // it should be treated as equal to old files
        assert_eq!(
            comparator(&update_window_time, &old_time),
            std::cmp::Ordering::Equal
        );

        // When update_window_time is the second arg (env2 context with NO update window),
        // it should be considered newer than old files
        assert_eq!(
            comparator(&old_time, &update_window_time),
            std::cmp::Ordering::Less
        );

        // But new files should still be greater than both
        assert_eq!(
            comparator(&new_time, &old_time),
            std::cmp::Ordering::Greater
        );
        assert_eq!(
            comparator(&new_time, &update_window_time),
            std::cmp::Ordering::Greater
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_mtime_comparator_version_differences() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let base_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1640995200);

        // Create environments with different package versions
        let env1_packages = &[
            ("python", "3.9.0", "h12debd9_1"),
            ("numpy", "1.21.0", "py39h7a9d4c0_0"),
        ];

        let env2_packages = &[
            ("python", "3.9.0", "h12debd9_1"),
            ("numpy", "1.22.0", "py39h7a9d4c0_0"), // Different version
        ];

        let env1_path = setup_conda_env_with_config(
            &temp_dir,
            "env1",
            base_time,
            "/opt/conda",
            env1_packages,
            false,
        )
        .await?;

        let env2_path = setup_conda_env_with_config(
            &temp_dir,
            "env2",
            base_time,
            "/opt/conda",
            env2_packages,
            false,
        )
        .await?;

        // Create fingerprints
        let fingerprint1 = CondaFingerprint::from_env(&env1_path).await?;
        let fingerprint2 = CondaFingerprint::from_env(&env2_path).await?;

        // Environments should have different fingerprints due to package differences
        assert_ne!(fingerprint1, fingerprint2);

        // But the mtime comparator should still work since the core history is the same
        let comparator = CondaFingerprint::mtime_comparator(&fingerprint1, &fingerprint2)?;

        let old_time = base_time - Duration::from_hours(1);
        let new_time = base_time + Duration::from_secs(7200);

        // Basic mtime comparison should still work
        assert_eq!(comparator(&old_time, &old_time), std::cmp::Ordering::Equal);
        assert_eq!(
            comparator(&new_time, &old_time),
            std::cmp::Ordering::Greater
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_mtime_comparator_empty_environments() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let base_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1640995200);

        // Create environments with minimal packages
        let minimal_packages = &[("python", "3.9.0", "h12debd9_1")];

        let env1_path = setup_conda_env_with_config(
            &temp_dir,
            "env1",
            base_time,
            "/opt/conda",
            minimal_packages,
            false,
        )
        .await?;

        let env2_path = setup_conda_env_with_config(
            &temp_dir,
            "env2",
            base_time,
            "/opt/conda",
            minimal_packages,
            false,
        )
        .await?;

        // Create fingerprints
        let fingerprint1 = CondaFingerprint::from_env(&env1_path).await?;
        let fingerprint2 = CondaFingerprint::from_env(&env2_path).await?;

        // Create mtime comparator
        let comparator = CondaFingerprint::mtime_comparator(&fingerprint1, &fingerprint2)?;

        // Test with identical times
        let test_time = base_time + Duration::from_mins(30);
        assert_eq!(
            comparator(&test_time, &test_time),
            std::cmp::Ordering::Equal
        );

        // Test with very old times (should be equal)
        let very_old_time = UNIX_EPOCH + Duration::from_secs(1000);
        assert_eq!(
            comparator(&very_old_time, &very_old_time),
            std::cmp::Ordering::Equal
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_conda_fingerprint_with_large_environments() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let base_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1640995200);

        // Create environments with many packages
        let large_package_set = &[
            ("python", "3.9.0", "h12debd9_1"),
            ("numpy", "1.21.0", "py39h7a9d4c0_0"),
            ("scipy", "1.7.0", "py39h0123456_0"),
            ("pandas", "1.3.0", "py39h0abcdef_0"),
            ("matplotlib", "3.4.0", "py39h0fedcba_0"),
            ("scikit-learn", "0.24.0", "py39h0987654_0"),
            ("tensorflow", "2.6.0", "py39h0321654_0"),
            ("pytorch", "1.9.0", "py39h0456789_0"),
            ("jupyter", "1.0.0", "py39h0111111_0"),
            ("ipython", "7.25.0", "py39h0222222_0"),
        ];

        let env1_path = setup_conda_env_with_config(
            &temp_dir,
            "env1",
            base_time,
            "/opt/conda",
            large_package_set,
            true,
        )
        .await?;

        let env2_path = setup_conda_env_with_config(
            &temp_dir,
            "env2",
            base_time,
            "/opt/conda",
            large_package_set,
            true,
        )
        .await?;

        // Create fingerprints
        let fingerprint1 = CondaFingerprint::from_env(&env1_path).await?;
        let fingerprint2 = CondaFingerprint::from_env(&env2_path).await?;

        // Fingerprints should be identical for identical large environments
        assert_eq!(fingerprint1, fingerprint2);

        // Create mtime comparator and verify it works with large environments
        let comparator = CondaFingerprint::mtime_comparator(&fingerprint1, &fingerprint2)?;

        let old_time = base_time - Duration::from_hours(1);
        let new_time = base_time + Duration::from_secs(7200);

        assert_eq!(comparator(&old_time, &old_time), std::cmp::Ordering::Equal);
        assert_eq!(
            comparator(&new_time, &old_time),
            std::cmp::Ordering::Greater
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_mtime_comparator_boundary_conditions() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let base_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1640995200);
        let packages = &[("python", "3.9.0", "h12debd9_1")];

        let env1_path = setup_conda_env_with_config(
            &temp_dir,
            "env1",
            base_time,
            "/opt/conda",
            packages,
            true, // Include update window
        )
        .await?;

        let env2_path = setup_conda_env_with_config(
            &temp_dir,
            "env2",
            base_time,
            "/opt/conda",
            packages,
            true, // Include update window
        )
        .await?;

        // Create fingerprints
        let fingerprint1 = CondaFingerprint::from_env(&env1_path).await?;
        let fingerprint2 = CondaFingerprint::from_env(&env2_path).await?;

        // Create mtime comparator
        let comparator = CondaFingerprint::mtime_comparator(&fingerprint1, &fingerprint2)?;

        let base_timestamp = base_time.duration_since(UNIX_EPOCH)?.as_secs();
        let slop = Duration::from_secs(5 * 60);

        // Test exact boundary conditions
        let _exact_base_plus_slop = UNIX_EPOCH + Duration::from_secs(base_timestamp) + slop;
        let update_window_start = UNIX_EPOCH + Duration::from_secs(base_timestamp + 3600);
        let update_window_end = UNIX_EPOCH + Duration::from_secs(base_timestamp + 3661); // +1 sec for window end

        // Test exactly at the boundary points
        let one_sec_before_window = update_window_start - Duration::from_secs(1);
        let one_sec_after_window = update_window_end + Duration::from_secs(1);
        let old_time = base_time - Duration::from_hours(1);

        // Just before update window should be newer than old files
        assert_eq!(
            comparator(&one_sec_before_window, &old_time),
            std::cmp::Ordering::Greater
        );

        // Just after update window should be newer than old files
        assert_eq!(
            comparator(&one_sec_after_window, &old_time),
            std::cmp::Ordering::Greater
        );

        // Exactly at window start should be equal to old files (in window)
        assert_eq!(
            comparator(&update_window_start, &old_time),
            std::cmp::Ordering::Equal
        );

        // Test extreme time values
        let very_far_future = UNIX_EPOCH + Duration::from_secs(u32::MAX as u64);
        assert_eq!(
            comparator(&very_far_future, &old_time),
            std::cmp::Ordering::Greater
        );

        Ok(())
    }
}
