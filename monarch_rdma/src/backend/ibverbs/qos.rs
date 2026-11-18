/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Quality-of-service settings for RoCE queue pairs: the traffic class and
//! service level carried in each queue pair's address vector.
//!
//! On a RoCE fabric, switches put each packet in a queue chosen by the DSCP
//! bits of its IP header, which come from `grh.traffic_class`. Only one queue
//! is lossless (protected by priority flow control); everything else is
//! best-effort and tail-drops under congestion. A queue pair that leaves
//! `traffic_class` at 0 gets the NIC's default ToS, which on our fabrics lands
//! in a lossy queue. RDMA in a lossy queue performs badly, since every drop
//! costs a full retransmit timeout, and with heavy fan-in it can fill the
//! switch's shared buffer and hurt other jobs' lossless traffic.
//!
//! Which traffic class reaches the lossless queue is switch-side policy that
//! differs between fabrics, and no host API reports it. Every RDMA host already
//! tells NCCL, though, so we use the `NCCL_IB_TC` and `NCCL_IB_SL` NCCL would
//! use, resolved by a transliteration of NCCL's own code (NCCL 2.32):
//!
//! - Which files, and their precedence: `initEnvFunc` and `setEnvFile` in
//!   `src/misc/param.cc`. NCCL loads `$NCCL_CONF_FILE` if set and non-empty,
//!   else `$HOME/.nccl.conf`, then `/etc/nccl.conf`, into the environment with
//!   `setenv(.., 0)`. So the environment wins, then the first file to assign a
//!   key. RCCL is the same code reading `.rccl.conf` and `/etc/rccl.conf`,
//!   which ROCm builds read instead. Unlike NCCL we do not modify the
//!   environment; we only reproduce the lookup.
//! - How a value parses: `ncclParamIbTc` / `ncclParamIbSl` in
//!   `src/transport/net_ib/connect.cc`, an `ncclParamCombo` of
//!   `ncclParamBounded(0, 0xFF)` (or `(0, 15)` for the SL) with the options
//!   `AUTO` and `-1`. An option or an invalid value means NCCL's default,
//!   `AUTO`, which for a queue pair outside any communicator is 0.
//!
//! [`RDMA_TRAFFIC_CLASS`] and [`RDMA_SERVICE_LEVEL`] override the result.
//!
//! [`RDMA_TRAFFIC_CLASS`]: crate::config::RDMA_TRAFFIC_CLASS
//! [`RDMA_SERVICE_LEVEL`]: crate::config::RDMA_SERVICE_LEVEL

use std::ffi::OsStr;
use std::fmt;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::PoisonError;

const NCCL_IB_TC: &str = "NCCL_IB_TC";
const NCCL_IB_SL: &str = "NCCL_IB_SL";
const NCCL_CONF_FILE: &str = "NCCL_CONF_FILE";

/// The conf file name NCCL reads; RCCL reads `rccl.conf` with the same code.
#[cfg(not(feature = "rocm"))]
const CONF_NAME: &str = "nccl.conf";
#[cfg(feature = "rocm")]
const CONF_NAME: &str = "rccl.conf";

/// Upper bounds NCCL enforces: "per IB spec, TC value can be 0-0xFF" and "SL
/// value can be 0-15".
const TC_MAX: i64 = 0xff;
const SL_MAX: i64 = 15;

/// `fgets` buffer size in `setEnvFile`.
const LINE_BUF: usize = 4096;
/// `envVar` / `envValue` buffer size in `setEnvFile`.
const ENV_BUF: usize = 1024;

/// The traffic class and service level a queue pair should use.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Qos {
    /// The GRH traffic class: the full IP ToS byte, `DSCP << 2 | ECN`.
    pub(crate) traffic_class: u8,
    /// The address-vector service level.
    pub(crate) service_level: u8,
}

/// The [`Qos`] for queue pairs created in this process: the config overrides
/// if set, NCCL's settings otherwise, and 0 for anything neither names.
///
/// Logs the values in effect, and where each came from, whenever they change.
pub(crate) fn configured() -> Qos {
    let (traffic_class, tc_origin) = effective(
        hyperactor_config::global::get(crate::config::RDMA_TRAFFIC_CLASS),
        "MONARCH_RDMA_TRAFFIC_CLASS",
        NCCL_IB_TC,
        || nccl().traffic_class.clone(),
    );
    let (service_level, sl_origin) = effective(
        hyperactor_config::global::get(crate::config::RDMA_SERVICE_LEVEL),
        "MONARCH_RDMA_SERVICE_LEVEL",
        NCCL_IB_SL,
        || nccl().service_level.clone(),
    );

    let message = format!(
        "RDMA queue pairs use traffic class {traffic_class} ({tc_origin}) \
         and service level {service_level} ({sl_origin})"
    );
    static LOGGED: Mutex<Option<String>> = Mutex::new(None);
    let mut logged = LOGGED.lock().unwrap_or_else(PoisonError::into_inner);
    if logged.as_deref() != Some(message.as_str()) {
        tracing::info!("{message}");
        *logged = Some(message);
    }

    Qos {
        traffic_class,
        service_level,
    }
}

/// The value in effect and where it came from: `override_value` if set (it is
/// the config attribute `override_name`), else `nccl()`, NCCL's `nccl_key`,
/// else 0. `nccl` is called only when there is no override.
fn effective(
    override_value: Option<u8>,
    override_name: &str,
    nccl_key: &str,
    nccl: impl FnOnce() -> Option<(u8, Source)>,
) -> (u8, String) {
    if let Some(value) = override_value {
        return (value, override_name.to_string());
    }
    match nccl() {
        Some((value, source)) => (value, format!("{nccl_key} from {source}")),
        None => (0, format!("{nccl_key} unset")),
    }
}

/// Where a setting came from, for logging.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Source {
    Env,
    File(PathBuf),
}

impl fmt::Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Env => f.write_str("env"),
            Self::File(path) => write!(f, "{}", path.display()),
        }
    }
}

/// NCCL's `NCCL_IB_TC` and `NCCL_IB_SL` on this host, with their sources;
/// `None` where NCCL would use its `AUTO` default.
struct NcclSettings {
    traffic_class: Option<(u8, Source)>,
    service_level: Option<(u8, Source)>,
}

/// NCCL's settings on this host, read once.
///
/// Uses blocking `std::fs`: this runs from `IbvConfig::default()`, a sync
/// context, and reads at most two small local files once per process.
fn nccl() -> &'static NcclSettings {
    static NCCL: OnceLock<NcclSettings> = OnceLock::new();
    NCCL.get_or_init(|| {
        let conf_file = std::env::var_os(NCCL_CONF_FILE);
        let home = std::env::var_os("HOME");
        // `setEnvFile` returns silently when `fopen` fails.
        let files: Vec<(PathBuf, Vec<u8>)> = conf_files(conf_file.as_deref(), home.as_deref())
            .into_iter()
            .filter_map(|path| Some((path.clone(), std::fs::read(&path).ok()?)))
            .collect();
        let lookup = |key: &str, max: i64| -> Option<(u8, Source)> {
            let env = std::env::var_os(key);
            let (raw, source) = resolve(
                key,
                env.as_deref().map(OsStrExt::as_bytes),
                files.iter().map(|(p, t)| (p, t.as_slice())),
            )?;
            Some((parse_param(&raw, max)?, source))
        };
        NcclSettings {
            traffic_class: lookup(NCCL_IB_TC, TC_MAX),
            service_level: lookup(NCCL_IB_SL, SL_MAX),
        }
    })
}

/// The files `initEnvFunc` loads, in order: `$NCCL_CONF_FILE` if set and
/// non-empty, else `$HOME/.nccl.conf` if `HOME` is set, then `/etc/nccl.conf`.
fn conf_files(conf_file: Option<&OsStr>, home: Option<&OsStr>) -> Vec<PathBuf> {
    let user = match conf_file.filter(|f| !f.is_empty()) {
        Some(file) => Some(PathBuf::from(file)),
        None => home.map(|home| {
            let mut path = home.to_os_string();
            path.push(format!("/.{CONF_NAME}"));
            PathBuf::from(path)
        }),
    };
    user.into_iter()
        .chain(std::iter::once(PathBuf::from(format!("/etc/{CONF_NAME}"))))
        .collect()
}

/// The raw value NCCL would see for `key`, and where it came from. Since
/// `setEnvFile` never overwrites (`setenv(.., 0)`), a set environment variable
/// wins even when empty or invalid, and otherwise the first assignment in the
/// first file to make one.
fn resolve<'a>(
    key: &str,
    env: Option<&[u8]>,
    files: impl IntoIterator<Item = (&'a PathBuf, &'a [u8])>,
) -> Option<(Vec<u8>, Source)> {
    if let Some(value) = env {
        return Some((value.to_vec(), Source::Env));
    }
    files.into_iter().find_map(|(path, text)| {
        let (_, value) = env_file_assignments(text).find(|(k, _)| *k == key.as_bytes())?;
        Some((value.to_vec(), Source::File(path.clone())))
    })
}

/// The `(name, value)` pairs `setEnvFile` passes to `setenv`, in file order.
fn env_file_assignments(text: &[u8]) -> impl Iterator<Item = (&[u8], &[u8])> {
    fgets_lines(text).filter_map(|line| {
        // The buffer is a C string: `strlen` stops at the first NUL.
        let mut line = &line[..line.iter().position(|&b| b == 0).unwrap_or(line.len())];
        if let Some(rest) = line.strip_suffix(b"\n") {
            line = rest;
        }
        if let Some(rest) = line.strip_suffix(b"\r") {
            line = rest;
        }
        if line.first() == Some(&b'#') {
            return None;
        }
        let s = line.iter().position(|&b| b == b'=')?;
        let name = &line[..s.min(ENV_BUF - 1)];
        let value = &line[s + 1..];
        let value = &value[..value.len().min(ENV_BUF - 1)];
        // `setenv` rejects an empty name with EINVAL.
        (!name.is_empty()).then_some((name, value))
    })
}

/// The successive buffers `fgets(line, LINE_BUF, file)` fills: up to and
/// including the next newline, or `LINE_BUF - 1` bytes, whichever is shorter.
fn fgets_lines(text: &[u8]) -> impl Iterator<Item = &[u8]> {
    let mut rest = text;
    std::iter::from_fn(move || {
        if rest.is_empty() {
            return None;
        }
        let max = rest.len().min(LINE_BUF - 1);
        let len = rest[..max]
            .iter()
            .position(|&b| b == b'\n')
            .map_or(max, |nl| nl + 1);
        let (line, tail) = rest.split_at(len);
        rest = tail;
        Some(line)
    })
}

/// NCCL's parse of an `int32_t` param declared as
/// `ncclParamCombo(ncclParamBounded(0, max), {AUTO, "-1"})`: `None` for the
/// `AUTO` options and for anything the parser or bound rejects, both of which
/// leave the param at its `AUTO` default.
fn parse_param(value: &[u8], max: i64) -> Option<u8> {
    // `oneOfResolve`: options match the trimmed value, case-insensitively.
    let token = c_trim(value);
    if token.eq_ignore_ascii_case(b"AUTO") || token == b"-1" {
        return None;
    }
    // `ncclIntegerParser<int32_t>`, then `boundedValidate`.
    let parsed = c_strtoll_whole(value)?;
    i32::try_from(parsed).ok()?;
    if !(0..=max).contains(&parsed) {
        return None;
    }
    u8::try_from(parsed).ok()
}

/// `std::strtoll(input, &end, 10)` accepted only if it consumes all of
/// `input`, as `ncclIntegerParser::resolve` requires: optional leading
/// whitespace, an optional sign, and at least one digit, with no trailing
/// characters and no overflow.
fn c_strtoll_whole(input: &[u8]) -> Option<i64> {
    let s = &input[input.iter().position(|&b| !c_isspace(b))?..];
    let (negative, digits) = match s.split_first()? {
        (b'-', rest) => (true, rest),
        (b'+', rest) => (false, rest),
        _ => (false, s),
    };
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    // Accumulate negatively so `i64::MIN` parses, as it does in `strtoll`.
    let magnitude = digits.iter().try_fold(0i64, |acc, &d| {
        acc.checked_mul(10)?.checked_sub(i64::from(d - b'0'))
    })?;
    if negative {
        Some(magnitude)
    } else {
        magnitude.checked_neg()
    }
}

/// `nccl::param::utils::trim`: strip C-locale `isspace` from both ends.
fn c_trim(s: &[u8]) -> &[u8] {
    let start = s.iter().position(|&b| !c_isspace(b)).unwrap_or(s.len());
    let end = s
        .iter()
        .rposition(|&b| !c_isspace(b))
        .map_or(start, |i| i + 1);
    &s[start..end]
}

/// C-locale `isspace`, which unlike `u8::is_ascii_whitespace` includes `\v`.
fn c_isspace(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assignments(text: &str) -> Vec<(String, String)> {
        env_file_assignments(text.as_bytes())
            .map(|(k, v)| {
                (
                    String::from_utf8(k.to_vec()).unwrap(),
                    String::from_utf8(v.to_vec()).unwrap(),
                )
            })
            .collect()
    }

    fn pair(k: &str, v: &str) -> (String, String) {
        (k.to_string(), v.to_string())
    }

    #[test]
    fn env_file_matches_set_env_file() {
        let text = "\
# NCCL_IB_TC=96
NCCL_IB_GID_INDEX=3\r
NCCL_IB_TC=224
 NCCL_IB_SL = 3 # not a comment
export NCCL_IB_TIMEOUT=7
no equals sign

=orphan
NCCL_SOCKET_IFNAME=eth1=x";
        assert_eq!(
            assignments(text),
            vec![
                pair("NCCL_IB_GID_INDEX", "3"),
                pair("NCCL_IB_TC", "224"),
                // Only a leading `#` is a comment, and nothing is trimmed.
                pair(" NCCL_IB_SL ", " 3 # not a comment"),
                // `export` is part of the name.
                pair("export NCCL_IB_TIMEOUT", "7"),
                // Split at the first `=`; the last line needs no newline.
                pair("NCCL_SOCKET_IFNAME", "eth1=x"),
            ],
            "comments, lines without `=`, and empty names are skipped"
        );
    }

    #[test]
    fn env_file_truncates_like_fixed_buffers() {
        // Names and values are cut at 1023 bytes.
        let long = "v".repeat(2000);
        let got = assignments(&format!("K={long}\n"));
        assert_eq!(got, vec![pair("K", &long[..ENV_BUF - 1])]);

        // A line longer than the 4095-byte fgets buffer continues as a new line.
        let line = format!("{}NCCL_IB_TC=41\n", "x".repeat(LINE_BUF - 1));
        assert_eq!(assignments(&line), vec![pair("NCCL_IB_TC", "41")]);

        // Bytes after a NUL are invisible to strlen.
        assert_eq!(
            assignments("A=1\0junk\nB=2\n"),
            vec![pair("A", "1"), pair("B", "2")]
        );
    }

    #[test]
    fn conf_files_follow_init_env_func() {
        let etc = PathBuf::from(format!("/etc/{CONF_NAME}"));
        let home_conf = PathBuf::from(format!("/home/u/.{CONF_NAME}"));
        assert_eq!(
            conf_files(Some(OsStr::new("/c.conf")), Some(OsStr::new("/home/u"))),
            vec![PathBuf::from("/c.conf"), etc.clone()],
            "$NCCL_CONF_FILE replaces the home file, not /etc"
        );
        assert_eq!(
            conf_files(Some(OsStr::new("")), Some(OsStr::new("/home/u"))),
            vec![home_conf, etc.clone()],
            "an empty $NCCL_CONF_FILE counts as unset"
        );
        assert_eq!(conf_files(None, None), vec![etc]);
    }

    #[test]
    fn resolve_never_overwrites() {
        let user = PathBuf::from("/c.conf");
        let etc = PathBuf::from("/etc/nccl.conf");
        let files = [
            (&user, b"NCCL_IB_SL=1\n".as_slice()),
            (
                &etc,
                b"NCCL_IB_TC=224\nNCCL_IB_TC=96\nNCCL_IB_SL=3\n".as_slice(),
            ),
        ];
        assert_eq!(
            resolve("NCCL_IB_TC", Some(b"41"), files),
            Some((b"41".to_vec(), Source::Env))
        );
        // A set but invalid environment value still wins, as with setenv(.., 0).
        assert_eq!(
            resolve("NCCL_IB_TC", Some(b""), files),
            Some((Vec::new(), Source::Env))
        );
        assert_eq!(
            resolve("NCCL_IB_TC", None, files),
            Some((b"224".to_vec(), Source::File(etc.clone()))),
            "the first assignment in a file wins"
        );
        assert_eq!(
            resolve("NCCL_IB_SL", None, files),
            Some((b"1".to_vec(), Source::File(user.clone()))),
            "the earlier file wins"
        );
        assert_eq!(resolve("NCCL_IB_TIMEOUT", None, files), None);
    }

    #[test]
    fn parse_param_matches_nccl_combo_bounded() {
        assert_eq!(parse_param(b"224", TC_MAX), Some(224));
        assert_eq!(parse_param(b"0", TC_MAX), Some(0));
        assert_eq!(parse_param(b"255", TC_MAX), Some(255));
        // strtoll skips leading whitespace and takes a sign.
        assert_eq!(parse_param(b" \t+96", TC_MAX), Some(96));
        assert_eq!(parse_param(b"-0", TC_MAX), Some(0));
        assert_eq!(
            parse_param(b"0224", TC_MAX),
            Some(224),
            "base 10, not octal"
        );

        // AUTO options, after trimming, case-insensitively.
        for auto in [b"AUTO".as_slice(), b" auto ", b"-1", b" -1\n"] {
            assert_eq!(parse_param(auto, TC_MAX), None, "{auto:?} is AUTO");
        }
        // Rejected by the integer parser or the bound: NCCL's AUTO default.
        for bad in [
            b"".as_slice(),
            b"   ",
            b"224 ",
            b"224abc",
            b"0xe0",
            b"\"224\"",
            b"256",
            b"-2",
            b"99999999999999999999",
        ] {
            assert_eq!(parse_param(bad, TC_MAX), None, "{bad:?} is rejected");
        }
        assert_eq!(parse_param(b"15", SL_MAX), Some(15));
        assert_eq!(parse_param(b"16", SL_MAX), None, "SL is bounded to 0..=15");
    }

    #[test]
    fn effective_prefers_override_and_names_source() {
        let etc = Source::File(PathBuf::from("/etc/nccl.conf"));
        assert_eq!(
            effective(Some(96), "MONARCH_RDMA_TRAFFIC_CLASS", NCCL_IB_TC, || {
                panic!("NCCL settings are not read when overridden")
            }),
            (96, "MONARCH_RDMA_TRAFFIC_CLASS".to_string())
        );
        assert_eq!(
            effective(None, "MONARCH_RDMA_TRAFFIC_CLASS", NCCL_IB_TC, || Some((
                224,
                etc.clone()
            ))),
            (224, "NCCL_IB_TC from /etc/nccl.conf".to_string())
        );
        assert_eq!(
            effective(None, "MONARCH_RDMA_TRAFFIC_CLASS", NCCL_IB_TC, || None),
            (0, "NCCL_IB_TC unset".to_string())
        );
    }

    #[test]
    fn strtoll_handles_extremes() {
        assert_eq!(c_strtoll_whole(b"-9223372036854775808"), Some(i64::MIN));
        assert_eq!(c_strtoll_whole(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(c_strtoll_whole(b"9223372036854775808"), None);
        assert_eq!(c_strtoll_whole(b"+"), None);
        assert_eq!(c_strtoll_whole(b"\x0b7"), Some(7), "\\v is C whitespace");
    }
}
