/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::path::Path;
use std::path::PathBuf;

use aho_corasick::AhoCorasick;
use aho_corasick::AhoCorasickBuilder;
use aho_corasick::MatchKind;
use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use anyhow::ensure;
use itertools::Itertools;

pub struct Replacer<'a> {
    /// Paths and their replacements, in order of longest to shortest (the order in which
    /// we should perform replacements so that longest prefixes are matched first).
    sorted_paths: Vec<(&'a Path, &'a Path)>,
    /// Above paths as bytestrings to be replaced, ordered in a vec for use with
    /// `AhoCorasick` matcher.
    needles: Vec<&'a [u8]>,
    replacements: Vec<&'a [u8]>,
    /// `AhoCorasick` matcher.
    matcher: AhoCorasick,
}

pub struct ReplacerBuilder<'a> {
    map: HashMap<&'a Path, &'a Path>,
}

impl<'a> Default for ReplacerBuilder<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> ReplacerBuilder<'a> {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn build_if_non_empty(self) -> Result<Option<Replacer<'a>>> {
        Ok(if self.map.is_empty() {
            None
        } else {
            Some(Replacer::from(self.map)?)
        })
    }

    pub fn build(self) -> Result<Replacer<'a>> {
        Replacer::from(self.map)
    }

    pub fn add(&mut self, path: &'a Path, repl: &'a Path) -> Result<()> {
        match self.map.entry(path) {
            Entry::Occupied(o) => {
                if *o.get() != repl {
                    bail!(
                        "conflicting replacements for {}: {} != {}",
                        path.display(),
                        o.get().display(),
                        repl.display()
                    )
                }
            }
            Entry::Vacant(v) => {
                v.insert(repl);
            }
        }
        Ok(())
    }
}

fn replace_bytestring(vec: &mut Vec<u8>, from: &[u8], to: &[u8]) {
    let mut i = 0;
    while from.len() + i <= vec.len() {
        if &vec[i..i + from.len()] == from {
            vec.splice(i..i + from.len(), to.iter().cloned());
            i += to.len(); // Skip past the inserted section
        } else {
            i += 1;
        }
    }
}

impl<'a> Replacer<'a> {
    pub fn from(paths: HashMap<&'a Path, &'a Path>) -> Result<Self> {
        let sorted_paths = paths
            .iter()
            .sorted_by_key(|(s, _)| 0 - (s.as_os_str().as_encoded_bytes().len() as isize))
            .map(|(s, d)| (*s, *d))
            .collect::<Vec<_>>();
        let (needles, replacements) = paths
            .iter()
            .map(|(s, d)| {
                (
                    s.as_os_str().as_encoded_bytes(),
                    d.as_os_str().as_encoded_bytes(),
                )
            })
            .sorted_by_key(|(s, _)| 0 - (s.len() as isize))
            .collect::<(Vec<_>, Vec<_>)>();

        // Build AC automaton over all source prefixes.  Use leftmost-longest to
        // avoid a shorter key stealing a longer one that shares a prefix.
        //let needles: Vec<&[u8]> = bytes.keys().copied().collect();
        let matcher = AhoCorasickBuilder::new()
            .match_kind(MatchKind::LeftmostLongest)
            .build(&needles)?;

        Ok(Replacer {
            sorted_paths,
            needles,
            replacements,
            matcher,
        })
    }

    /// Perform in-place replacements, where the replacement is padded with nul
    /// characters to match the length of the replacee.  Fails if the replacement
    /// is longer than the replacee.
    pub fn replace_inplace_padded(&self, buf: &mut [u8]) -> Result<()> {
        if self.needles.is_empty() {
            return Ok(());
        }

        let mut offset = 0;
        while let Some(m) = self.matcher.find(&buf[offset..]) {
            let buf = &mut buf[offset..];

            let start = m.start();
            let end = m.end(); // end is exclusive
            let pat_idx = m.pattern();

            // Check trailing byte condition: `/`, `\0`, or EOF
            let trailing_ok = match buf.get(end) {
                None => true, // EOF
                Some(b) => *b == b'/' || *b == 0,
            };
            if !trailing_ok {
                offset = end + 1;
                continue;
            }

            // Copy in the replacement over the original path, making sure that it's not too big.
            let pat = self.needles[pat_idx];
            let repl = self.replacements[pat_idx];
            ensure!(
                repl.len() <= pat.len(),
                "Input is longer than target length"
            );
            buf[start..start + repl.len()].copy_from_slice(repl);
            // Find where the nul byte is in the original path, after any path suffixing the prefix
            // we're replacing.
            let nul_idx = end + buf[end..].iter().position(|b| *b == 0u8).context("nul")?;
            // Shift the path suffix over to meet the replacment (in the case where the replacement
            // is shorter than the original path).
            buf.copy_within(end..nul_idx, start + repl.len());
            // Pad the remaining space with nul bytes (in the case where the replacement is shorter
            // than the original path).
            buf[(nul_idx - (pat.len() - repl.len()))..nul_idx].fill(0);

            // Safety: lengths are equal by construction
            offset = nul_idx + 1;
        }

        Ok(())
    }

    /// Perform in-place replacements, which may modify the size of the
    /// bytestring.
    pub fn replace_inplace(&self, buf: &mut Vec<u8>) {
        for (src, dst) in self.needles.iter().zip(self.replacements.iter()) {
            replace_bytestring(buf, src, dst);
        }
    }

    /// Replace any matching prefix of the given path.
    pub fn replace_path(&self, path: PathBuf) -> PathBuf {
        for (pattern, repl) in self.sorted_paths.iter() {
            if let Ok(suffix) = path.strip_prefix(pattern) {
                return repl.join(suffix);
            }
        }
        path
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[test]
    fn test_replace_inplace_padded() -> Result<()> {
        // Create a replacer that replaces "/old/path" with "/new"
        let mut builder = ReplacerBuilder::new();
        builder.add(Path::new("/old/path"), Path::new("/new"))?;
        let replacer = builder.build()?;

        // Test 1: Basic replacement with trailing null byte
        let mut buf = b"/old/path\0some other data".to_vec();
        replacer.replace_inplace_padded(&mut buf)?;
        let expected: &[u8] = b"/new\0\0\0\0\0\0some other data";
        assert_eq!(buf, expected);

        // Test 2: Replacement with trailing slash
        let mut buf = b"/old/path/subdir\0".to_vec();
        replacer.replace_inplace_padded(&mut buf)?;
        let expected: &[u8] = b"/new/subdir\0\0\0\0\0\0";
        assert_eq!(buf, expected);

        // Test 3: Replacement at end of buffer (EOF condition)
        let mut buf = b"/old/path\0".to_vec();
        replacer.replace_inplace_padded(&mut buf)?;
        let expected: &[u8] = b"/new\0\0\0\0\0\0";
        assert_eq!(buf, expected);

        // Test 4: No replacement when trailing byte condition is not met
        let mut buf = b"/old/pathX".to_vec();
        replacer.replace_inplace_padded(&mut buf)?;
        let expected: &[u8] = b"/old/pathX";
        assert_eq!(buf, expected);

        // Test 5: Multiple replacements in same buffer
        let mut buf = b"/old/path\0/old/path/subdir\0".to_vec();
        replacer.replace_inplace_padded(&mut buf)?;
        let expected: &[u8] = b"/new\0\0\0\0\0\0/new/subdir\0\0\0\0\0\0";
        assert_eq!(buf, expected);

        // Test 6: Empty buffer
        let mut buf: Vec<u8> = vec![];
        replacer.replace_inplace_padded(&mut buf)?;
        let expected: Vec<u8> = vec![];
        assert_eq!(buf, expected);

        // Test 7: Buffer without any matches
        let mut buf = b"no matches here".to_vec();
        replacer.replace_inplace_padded(&mut buf)?;
        let expected: &[u8] = b"no matches here";
        assert_eq!(buf, expected);

        Ok(())
    }

    #[test]
    fn test_replace_inplace_padded_empty_replacer() -> Result<()> {
        // Test with empty replacer (no paths to replace)
        let builder = ReplacerBuilder::new();
        let replacer = builder.build()?;

        let mut buf = b"/some/path".to_vec();
        replacer.replace_inplace_padded(&mut buf)?;
        let expected: &[u8] = b"/some/path";
        assert_eq!(buf, expected);

        Ok(())
    }

    #[test]
    fn test_replace_inplace_padded_replacement_too_long() -> Result<()> {
        // Test that replacement fails when replacement is longer than original
        let mut builder = ReplacerBuilder::new();
        builder.add(Path::new("/a"), Path::new("/very/long/path"))?;

        // This should fail during padding since "/very/long/path" is longer than "/a"
        let replacer = builder.build()?;
        let mut buf = b"/a\0".to_vec();
        let result = replacer.replace_inplace_padded(&mut buf);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_replace_inplace_padded_multiple_patterns() -> Result<()> {
        // Test with multiple replacement patterns
        let mut builder = ReplacerBuilder::new();
        builder.add(Path::new("/old"), Path::new("/new"))?;
        builder.add(Path::new("/temp"), Path::new("/tmp"))?;
        let replacer = builder.build()?;

        let mut buf = b"/old/file\0/temp/data\0".to_vec();
        replacer.replace_inplace_padded(&mut buf)?;
        let expected: &[u8] = b"/new/file\0/tmp/data\0\0";
        assert_eq!(buf, expected);

        Ok(())
    }

    #[test]
    fn test_replace_inplace() -> Result<()> {
        // Test replace_inplace which resizes the buffer
        let mut builder = ReplacerBuilder::new();
        builder.add(Path::new("/old/path"), Path::new("/new"))?;
        builder.add(Path::new("/usr/local"), Path::new("/usr"))?;
        let replacer = builder.build()?;

        // Test 1: Replacement makes buffer smaller
        let mut buf = b"/old/path/file.txt and /usr/local/bin/prog".to_vec();
        replacer.replace_inplace(&mut buf);
        let expected: &[u8] = b"/new/file.txt and /usr/bin/prog";
        assert_eq!(buf, expected);

        // Test 2: Replacement makes buffer larger
        let mut builder = ReplacerBuilder::new();
        builder.add(Path::new("/a"), Path::new("/very/long/path"))?;
        let replacer = builder.build()?;

        let mut buf = b"/a/file".to_vec();
        replacer.replace_inplace(&mut buf);
        let expected: &[u8] = b"/very/long/path/file";
        assert_eq!(buf, expected);

        // Test 3: Multiple replacements of different sizes
        let mut builder = ReplacerBuilder::new();
        builder.add(Path::new("/short"), Path::new("/very/long/replacement"))?;
        builder.add(Path::new("/long/path/here"), Path::new("/x"))?;
        let replacer = builder.build()?;

        let mut buf = b"/short and /long/path/here".to_vec();
        replacer.replace_inplace(&mut buf);
        let expected: &[u8] = b"/very/long/replacement and /x";
        assert_eq!(buf, expected);

        // Test 4: Empty buffer
        let mut buf: Vec<u8> = vec![];
        replacer.replace_inplace(&mut buf);
        assert!(buf.is_empty());

        // Test 5: No matches
        let mut buf = b"no matches in this text".to_vec();
        replacer.replace_inplace(&mut buf);
        let expected: &[u8] = b"no matches in this text";
        assert_eq!(buf, expected);

        Ok(())
    }

    #[test]
    fn test_prefix_priority_longer_before_shorter() -> Result<()> {
        // Test that longer prefixes are matched and replaced before shorter ones
        let mut builder = ReplacerBuilder::new();
        builder.add(Path::new("/foo"), Path::new("/short"))?;
        builder.add(Path::new("/foo/bar"), Path::new("/long"))?;
        let replacer = builder.build()?;

        // Test 1: Longer prefix should be matched first with replace_inplace
        let mut buf = b"/foo/bar/file.txt".to_vec();
        replacer.replace_inplace(&mut buf);
        let expected: &[u8] = b"/long/file.txt";
        assert_eq!(buf, expected);

        // Test 2: Shorter prefix should match when longer doesn't
        let mut buf = b"/foo/baz/file.txt".to_vec();
        replacer.replace_inplace(&mut buf);
        let expected: &[u8] = b"/short/baz/file.txt";
        assert_eq!(buf, expected);

        // Test 3: With replace_inplace_padded
        let mut buf = b"/foo/bar\0".to_vec();
        replacer.replace_inplace_padded(&mut buf)?;
        let expected: &[u8] = b"/long\0\0\0\0";
        assert_eq!(buf, expected);

        Ok(())
    }

    #[test]
    fn test_prefix_priority_complex() -> Result<()> {
        // Test with multiple overlapping prefixes of different lengths
        let mut builder = ReplacerBuilder::new();
        builder.add(Path::new("/a"), Path::new("/1"))?;
        builder.add(Path::new("/a/b"), Path::new("/2"))?;
        builder.add(Path::new("/a/b/c"), Path::new("/3"))?;
        builder.add(Path::new("/a/b/c/d"), Path::new("/4"))?;
        let replacer = builder.build()?;

        // Test that the longest matching prefix wins
        let mut buf = b"/a/b/c/d/e/file.txt".to_vec();
        replacer.replace_inplace(&mut buf);
        let expected: &[u8] = b"/4/e/file.txt";
        assert_eq!(buf, expected);

        let mut buf = b"/a/b/c/x/file.txt".to_vec();
        replacer.replace_inplace(&mut buf);
        let expected: &[u8] = b"/3/x/file.txt";
        assert_eq!(buf, expected);

        let mut buf = b"/a/b/x/file.txt".to_vec();
        replacer.replace_inplace(&mut buf);
        let expected: &[u8] = b"/2/x/file.txt";
        assert_eq!(buf, expected);

        let mut buf = b"/a/x/file.txt".to_vec();
        replacer.replace_inplace(&mut buf);
        let expected: &[u8] = b"/1/x/file.txt";
        assert_eq!(buf, expected);

        Ok(())
    }

    #[test]
    fn test_prefix_priority_with_path_method() -> Result<()> {
        // Test that the replace_path method also respects prefix priority
        let mut builder = ReplacerBuilder::new();
        builder.add(Path::new("/usr"), Path::new("/system"))?;
        builder.add(Path::new("/usr/local"), Path::new("/local"))?;
        builder.add(Path::new("/usr/local/bin"), Path::new("/bin"))?;
        let replacer = builder.build()?;

        // Longest matching prefix should win
        let path = PathBuf::from("/usr/local/bin/python");
        let result = replacer.replace_path(path);
        assert_eq!(result, PathBuf::from("/bin/python"));

        let path = PathBuf::from("/usr/local/share/data");
        let result = replacer.replace_path(path);
        assert_eq!(result, PathBuf::from("/local/share/data"));

        let path = PathBuf::from("/usr/share/data");
        let result = replacer.replace_path(path);
        assert_eq!(result, PathBuf::from("/system/share/data"));

        let path = PathBuf::from("/opt/data");
        let result = replacer.replace_path(path);
        assert_eq!(result, PathBuf::from("/opt/data")); // No replacement

        Ok(())
    }
}
