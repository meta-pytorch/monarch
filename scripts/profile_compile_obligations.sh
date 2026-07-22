#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#
# profile_compile_obligations.sh — profile and reduce rustc trait-solving
# (`evaluate_obligation`) compile cost for crates in the monarch workspace.
#
# WHY THIS EXISTS
# ---------------
# For actor-heavy crates, `evaluate_obligation` (the rustc query that proves
# trait bounds, dominated here by `Send`/`Sync` auto-trait checking) can be the
# single largest slice of compile time. The standard trait solver proves
# `Send`/`Sync` *structurally*: to show `T: Send` it recurses through every
# field of `T`. For the actor/proc/gateway state — built on deep
# `DashMap`/`RwLock`/`Arc` towers — that recursion is deep and is redone for
# every monomorphization and every async-fn future that captures the state.
#
# Two independent fixes, both validated on hyperactor_mesh (2m34s -> ~1m00s):
#   1. `-Z next-solver=globally` — the next-gen trait solver caches the
#      structural proofs; drives evaluate_obligation to ~0. One flag, no unsafe.
#   2. `unsafe impl Send/Sync` on the few concrete types that own the deep
#      towers (ProcState, GatewayState, InstanceCellState, Mailbox) — makes the
#      checker match a one-step impl instead of recursing. Same win on the
#      standard solver; pair with a cfg-gated auto-verify guard (see VERIFY).
#
# METHODOLOGY NOTE (important): incremental compilation caches query results,
# so a no-op rebuild reports ~0 for evaluate_obligation and hides the work.
# Every run here sets CARGO_INCREMENTAL=0 so trait solving runs from scratch.
#
# USAGE
#   scripts/profile_compile_obligations.sh profile  <crate> [extra rustc flags…]
#   scripts/profile_compile_obligations.sh nextsolver <crate>   # A/B vs baseline
#   scripts/profile_compile_obligations.sh attribute <crate>    # self-time by trait
#   scripts/profile_compile_obligations.sh diff <profА> <profB>
# Examples
#   scripts/profile_compile_obligations.sh profile hyperactor_mesh
#   scripts/profile_compile_obligations.sh attribute hyperactor
#
# ENV
#   PROFILES_DIR   where *.mm_profdata land (default: $TMPDIR/monarch_compile_profiles)
#   MEASUREME_DIR  path to a measureme checkout for the attribution tool's
#                  `analyzeme` (default: fetch `analyzeme` from git). Needed only
#                  for `attribute`; pin this if the profile format mismatches.
set -euo pipefail

MONARCH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILES_DIR="${PROFILES_DIR:-${TMPDIR:-/tmp}/monarch_compile_profiles}"
mkdir -p "$PROFILES_DIR"
cd "$MONARCH"

# Rust builds here link against Python (PyO3); activate the venv if present.
# shellcheck disable=SC1091
if [ -f .venv/bin/activate ]; then
  source .venv/bin/activate 2>/dev/null || true
fi

crate_root() { # echo the lib.rs path for a workspace crate (dir usually == name)
  local c="$1"
  if [ -f "$c/src/lib.rs" ]; then echo "$c/src/lib.rs"; else echo ""; fi
}

# Force a clean, NON-incremental reprofile of one crate's lib into the profiles
# dir, then print the summarize table. Extra args after the crate are passed
# verbatim to rustc (e.g. `-Z next-solver=globally`).
reprofile() {
  local crate="$1" name="$2"; shift 2
  local root; root="$(crate_root "$crate")"
  [ -n "$root" ] || { echo "no $crate/src/lib.rs" >&2; return 1; }
  CARGO_INCREMENTAL=0 cargo build -p "$crate" --lib >/dev/null    # warm deps
  touch "$root"                                                   # force recompile of just this crate
  CARGO_INCREMENTAL=0 RUSTC_BOOTSTRAP=1 cargo rustc -p "$crate" --lib -- \
    -Z self-profile -Z self-profile-events="${SELF_PROFILE_EVENTS:-default}" "$@"
  local pf="" candidate
  for candidate in "$crate"-*.mm_profdata; do
    [ -e "$candidate" ] || continue
    if [ -z "$pf" ] || [ "$candidate" -nt "$pf" ]; then
      pf="$candidate"
    fi
  done
  [ -n "$pf" ] || { echo "no profdata produced" >&2; return 1; }
  mv "$pf" "$PROFILES_DIR/$name.mm_profdata"
  echo "== summarize $name =="
  summarize summarize "$PROFILES_DIR/$name.mm_profdata" | head -16
  echo "saved: $PROFILES_DIR/$name.mm_profdata"
}

# Build the per-predicate attribution helper (self-time bucketed by trait).
build_attrib_tool() {
  local dir="$PROFILES_DIR/attrib_tool"
  mkdir -p "$dir"
  if [ -n "${MEASUREME_DIR:-}" ]; then
    local dep="analyzeme = { path = \"$MEASUREME_DIR/analyzeme\" }"
  else
    local dep="analyzeme = { git = \"https://github.com/rust-lang/measureme\" }"
  fi
  cat > "$dir/Cargo.toml" <<TOML
[package]
name = "obl_attrib"
version = "0.0.0"
edition = "2021"
[dependencies]
$dep
[[bin]]
name = "obl_attrib"
path = "main.rs"
TOML
  cat > "$dir/main.rs" <<'RUST'
// Replicates summarize's stack-based self-time algorithm but buckets
// evaluate_obligation self-time by the trait being proven (parsed from the
// query key recorded with `-Z self-profile-events=default,query-keys`).
use std::collections::HashMap;
use analyzeme::{ProfilingData, EventPayload, Timestamp};
struct Frame { start: u64, end: u64, bucket: Option<String> }
fn main() {
    let path = std::env::args().nth(1).expect("profdata path");
    let data = ProfilingData::new(std::path::Path::new(&path)).expect("open profdata");
    let mut by_trait: HashMap<String,(u128,u64)> = HashMap::new();
    let mut threads: HashMap<u32, Vec<Frame>> = HashMap::new();
    let mut total: u128 = 0;
    for ev in data.iter_full().rev() {
        let (s,e) = match ev.payload {
            EventPayload::Timestamp(Timestamp::Interval{start,end}) => (
                start.duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64,
                end.duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64),
            _ => continue,
        };
        let dur = (e - s) as i128;
        let st = threads.entry(ev.thread_id).or_default();
        while let Some(t) = st.last() { if t.start<=s && e<=t.end { break } st.pop(); }
        if let Some(t) = st.last() { if let Some(k)=&t.bucket {
            let x=by_trait.entry(k.clone()).or_insert((0,0)); x.0=(x.0 as i128-dur).max(0) as u128; } }
        let bucket = if ev.label.as_ref()=="evaluate_obligation" && !ev.additional_data.is_empty() {
            let k = ev.additional_data.iter().map(|c|c.as_ref()).collect::<Vec<_>>().join(" ");
            let tr = last_trait(&k);
            let x=by_trait.entry(tr.clone()).or_insert((0,0)); x.0+=dur as u128; x.1+=1;
            total += dur as u128; Some(tr)
        } else { None };
        st.push(Frame{start:s,end:e,bucket});
    }
    println!("evaluate_obligation self-time {}ms", total/1_000_000);
    let mut v: Vec<_> = by_trait.into_iter().collect();
    v.sort_by(|a,b| b.1.0.cmp(&a.1.0));
    println!("{:>9} {:>9} {:>6}  TRAIT", "self_ms","count","%");
    for (k,(ns,c)) in v.iter().take(25) {
        println!("{:>9} {:>9} {:>5.1}%  {}", ns/1_000_000, c, 100.0*(*ns as f64)/(total.max(1) as f64), k);
    }
}
fn last_trait(k:&str)->String{ if let Some(p)=k.rfind(" as "){ let a=&k[p+4..];
    let e=a.find(|c|c=='<'||c=='>'||c==','||c==' ').unwrap_or(a.len());
    let pre = if k.contains("ProjectionPredicate") {"Projection:"} else {""};
    format!("{pre}{}", a[..e].rsplit("::").next().unwrap_or(&a[..e])) } else { "<noas>".into() } }
RUST
  ( cd "$dir" && cargo build --release >/dev/null 2>&1 ) || {
    echo "attribution tool build failed; set MEASUREME_DIR to a measureme checkout" >&2; return 1; }
  echo "$dir/target/release/obl_attrib"
}

cmd="${1:-profile}"; shift || true
case "$cmd" in
  profile)
    crate="${1:-hyperactor_mesh}"; shift || true
    reprofile "$crate" "${crate}_baseline" "$@"
    ;;
  nextsolver)
    crate="${1:-hyperactor_mesh}"
    reprofile "$crate" "${crate}_baseline"
    reprofile "$crate" "${crate}_nextsolver" -Z next-solver=globally
    echo "== diff baseline -> next-solver =="
    summarize diff "$PROFILES_DIR/${crate}_baseline.mm_profdata" \
                   "$PROFILES_DIR/${crate}_nextsolver.mm_profdata" | head -12
    ;;
  attribute)
    crate="${1:-hyperactor_mesh}"
    SELF_PROFILE_EVENTS="default,query-keys" reprofile "$crate" "${crate}_keys" >/dev/null
    tool="$(build_attrib_tool)"
    "$tool" "$PROFILES_DIR/${crate}_keys.mm_profdata"
    ;;
  diff)
    summarize diff "$1" "$2"
    ;;
  *)
    echo "usage: $0 {profile|nextsolver|attribute <crate>|diff <a> <b>}" >&2; exit 2
    ;;
esac
