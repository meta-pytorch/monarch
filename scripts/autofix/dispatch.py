#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Ralph Wiggum auto-fix loop for Monarch CI failures.

A local, resumable loop that watches a target CI workflow on
`meta-pytorch/monarch` for test failures, dispatches a Claude or Codex
subagent to produce a minimal fix in an isolated git worktree, has a
second subagent review the fix as a quality gate, and only then opens a
PR (with the `autofix` label and the current GH user as assignee) from
your fork against upstream.

Designed for a personal MacBook with NO CI budget on the fork. All
verification is local. Upstream CI on the resulting PR is the
authoritative re-test; if it goes red, the loop routes back through the
evaluator in `ci_signal` mode and the fixer in `MODE=amend`, then
force-pushes.

ARCHITECTURE — MONARCH ACTORS

  Two actor classes live in the dispatcher's mesh, both spawned on
  `this_host()`:

  * `TestDataActor` — one instance, started at dispatcher startup.
    Owns all GitHub-side polling (gh run list, JUnit artifact download,
    XML parse) and a per-test history of pass/fail observations across
    SHAs, persisted to <state_dir>/test_history.json. Endpoints:
    `discover` (new failures on the latest decided commit, plus
    side-recorded pass observations from the same XMLs),
    `refresh_history` (scan recent commits regardless of conclusion to
    catch pass-after-fail), `status_summary` (per-test history).

  * `AgentActor` — one instance per fixer or evaluator invocation, on
    a fresh proc. Isolates the agent CLI's process tree (cargo, conda,
    nextest, the local LLM bash executor) so we can tear it down
    cleanly: when `procs.stop()` runs — or when the dispatcher process
    disconnects unexpectedly (Ctrl-C, crash) — Monarch fires
    `__cleanup__`, which SIGTERMs every subprocess the agent spawned.
    The AgentActor receives a TestDataActor handle at construction so
    agents can query history without re-fetching from GitHub.

  No manual subprocess bookkeeping in the dispatcher: the actors own
  their own children and Monarch owns the actor lifecycle.

  When the dispatcher polls again while a fixer is mid-investigation
  and sees new pass/fail data on a newer commit (test passed → maybe
  fixed upstream, maybe flaky; test failed again → confirmed real), it
  appends those observations to <worktree>/AUTO_FIX_CONTEXT_UPDATES.md.
  The fixer's prompt instructs it to re-read this file each iteration
  and weigh the new data without abandoning the investigation.

  Run via `uv run` so the dispatcher and the spawned actor procs all
  pick up the in-tree Monarch build:

      uv run python scripts/auto_fix/dispatch.py

WHAT EACH CYCLE DOES
  1. Reconcile already-open auto-fix PRs (merged? closed? CI red?).
     If a PR's upstream CI failed on a new head, run the evaluator in
     `ci_signal` mode; if it requests changes, re-invoke the fixer in
     amend mode and force-push.
  2. Discover new failures:
       - `gh run list` the most recent commit on main of --upstream.
       - Filter strictly to --workflow (default is OS-aware: 'CI
         (macOS)' on Darwin, 'CI' on Linux — we can only repro
         same-OS failures locally).
       - Skip in-progress runs; on the first all-decided commit, take
         its failed runs and stop. Older commits' failures are assumed
         already fixed.
       - Only consider runs that uploaded JUnit XML artifacts (named
         test-result*/junit*/pytest-result*/nextest-result*). Without
         XML we can't reliably name failing tests, so we treat the
         run as if it passed.
  3. Dedup against existing PRs:
       - Skip if our state file marks this issue terminal
         (gave_up / pr_merged / pr_closed / dup_of_existing_pr).
       - Search upstream + origin for a PR whose title contains
         `[auto-fix:<issue_key>]`. Open => track CI; merged => done;
         closed => give up.
       - Free-text search upstream for any open PR mentioning the
         failing test name verbatim. If someone is already on it, mark
         dup_of_existing_pr and move on.
  4. Dispatch a fixer subagent (one per cycle by default) in a fresh
     worktree. The fixer reproduces, minimizes within the failing
     pytest split group (or nextest crate), commits a fix, and writes
     AUTO_FIX_REPORT.md. It does NOT push and does NOT open a PR.
  5. Run an evaluator subagent on the same worktree. Verdicts are
     submit / request_changes / reject, with a confidence score.
       - submit (and confidence ≥ --min-confidence)  =>  push branch
         to origin; `gh pr create` against upstream with the autofix
         label and you as assignee. Big AUTO_FIX_REPORT.md gets posted
         as a follow-up PR comment.
       - request_changes  =>  feedback saved; fixer is re-invoked next
         cycle (subject to MAX_ATTEMPTS=3).
       - reject  =>  state goes terminal-gave_up.

STATE & RESUMABILITY
  State lives at <state-dir>/state.json (default ~/.monarch-auto-fix/).
  Worktrees at <state-dir>/worktrees/<issue-key>/ persist across
  restarts. Per-issue context notes accumulate at
  <state-dir>/contexts/<issue-key>/notes.md and are fed back to the
  fixer on retries; key facts also end up in the PR body / comments.
  Agent stdouts are captured at <state-dir>/logs/. Ctrl-C is safe at
  any point — recovery on next start re-classifies in-flight entries.

PARALLELISM POLICY
  Memory is NOT capped (capping memory can mask real failures). CPU
  parallelism IS capped, dynamically: each in-flight subagent gets
  `cores_per_agent = max(1, (cpu_count - --reserved-cores) /
  in_flight_count)` cores. Defaults reserve 4 cores for the human and
  their browser; lower with --reserved-cores 0 if running unattended
  overnight. The cap is exposed to subagents via MAKEFLAGS,
  CARGO_BUILD_JOBS, CMAKE_BUILD_PARALLEL_LEVEL, RAYON_NUM_THREADS,
  AUTO_FIX_BUILD_JOBS, AUTO_FIX_TEST_THREADS.

PR HYGIENE
  PRs are throttled with --max-open-prs (default 3) so the loop won't
  pepper humans with a flood of bots-PRs. The --autofix label is
  applied so reviewers can filter is:pr label:autofix vs
  is:pr -label:autofix. PR titles are `[auto-fix:<issue_key>] …`; PR
  bodies include `<!-- auto-fix-key: <issue_key> -->` so future runs
  can find them reliably.

PREREQUISITES
  - `gh` (logged in: `gh auth status`)
  - For --agent claude / codex: that CLI on PATH and authenticated.
  - For --agent local: any OpenAI-compatible chat-completions server
    (see LOCAL BACKENDS below).
  - `git` with the upstream and origin remotes configured.
  - JUnit XML upload from the workflow you point --workflow at
    (otherwise the loop has nothing to do).

LOCAL BACKENDS (for --agent local)

  The dispatcher hits `<api-url>/chat/completions` with an OpenAI-style
  request and supports tool use via a single `<bash>...</bash>` protocol
  it injects in the system prompt. ANY server that speaks that API will
  work.

  MODEL CHOICE MATTERS A LOT. The protocol is text-tagged tool use
  (no native function-calling), so models that have been fine-tuned
  for agentic / tool-use workflows perform dramatically better than
  general-purpose chat models — even at the same parameter count. In
  practice:

    Recommended  : Qwen2.5-Coder 32B+, DeepSeek-Coder-V2 16B+,
                   Llama-3.1 70B Instruct, or any "coder" / "agent"
                   variant trained to follow XML-style tool tags.
    Workable     : Mistral Large, Mixtral 8x22B, generic chat models
                   ≥ 70B.
    Will struggle: General-purpose chat models < 32B (Gemma, Llama 8B,
                   Phi, etc.) — they tend to predict tool output
                   instead of waiting for it, and to lose protocol
                   discipline after a handful of turns.

  Streaming is required (we use SSE chunks); every server below
  supports it natively. We also stream-cancel on `</bash>` to force a
  turn boundary on each command — that prevents weaker models from
  hallucinating tool results and rolling forward in one giant turn.

  Streaming is required (we use SSE chunks); every server below supports
  it natively. Tools/function-calling support is NOT required — we use
  text-tagged commands, so even tool-less models work.

  Recipes:

    Ollama
      ollama pull qwen2.5-coder:32b      # one-time
      ollama serve &                     # auto-started by Ollama.app
      dispatch.py --agent local \\
                  --api-url http://localhost:11434/v1 \\
                  --model qwen2.5-coder:32b

    llama.cpp's llama-server
      llama-server -m /path/to/Qwen2.5-Coder-32B-Instruct-Q4_K_M.gguf \\
                   -c 32768 --host 0.0.0.0 --port 8080
      dispatch.py --agent local \\
                  --api-url http://localhost:8080/v1 \\
                  --model qwen2.5-coder:32b

    LM Studio (Server tab; the model name shown in the UI is what to pass)
      dispatch.py --agent local \\
                  --api-url http://localhost:1234/v1 \\
                  --model "lmstudio-community/Qwen2.5-Coder-32B-Instruct"

    vLLM
      vllm serve Qwen/Qwen2.5-Coder-32B-Instruct \\
                 --host 0.0.0.0 --port 8000 --max-model-len 32768
      dispatch.py --agent local \\
                  --api-url http://localhost:8000/v1 \\
                  --model Qwen/Qwen2.5-Coder-32B-Instruct

    Hosted OpenAI-compatible providers (Together, DeepInfra, Groq,
    OpenRouter, etc.) work too — point --api-url at the provider's
    base URL and pass --api-key.

  When --agent local is selected, --max-per-cycle is automatically
  clamped to 1 (single shared backend; parallel agents only queue on
  the same model). Build parallelism (--reserved-cores) is unaffected.
  Local LLM iterations can take many minutes on Apple Silicon — the
  dispatcher streams tokens to the per-issue fixer log so progress is
  visible at <state-dir>/logs/<key>.fixer.*.log.

SECURITY — READ THIS BEFORE YOU LEAVE IT RUNNING

  The subagent runs with the agent CLI's permission system bypassed
  (`--dangerously-skip-permissions` for claude / codex; the local
  `<bash>`-only protocol for ollama). It has the same authority as the
  user invoking this script:

    * full filesystem read AND write (NOT scoped to the worktree)
    * network access (required for `cargo`, `pip`/`uv`, and `conda` to
      fetch build dependencies; can't be turned off without breaking
      fix-and-verify)
    * any credential stores accessible to that user — gh tokens, ssh
      keys, ~/.aws, browser cookies, etc.

  In addition, when the evaluator approves a fix, the dispatcher will:

    * `git push` a new branch `auto-fix/<key>` to your --origin remote.
      Anyone with access to that repo can see it.
    * Open a PR against --upstream (visible to whoever can see that
      repo — usually publicly).
    * Force-push amends with `--force-with-lease` if upstream CI goes
      red and the evaluator requests changes.

  All three side effects are suppressed by --dry-run, which still runs
  the fixer and evaluator locally (so you can inspect the worktree)
  but pushes nothing and opens no PR.

  Mitigations to consider:
    * --dry-run first run, to see what the fixer + evaluator produce
      locally before any visible upstream churn.
    * Run on a dedicated VM or in a container so the unsandboxed
      filesystem reach is naturally bounded.
    * Use a short-lived `gh auth` token scoped to just this repo, or a
      separate machine user.
    * On macOS you can wrap the dispatcher in `sandbox-exec` with a
      profile that allows writes only under --state-dir; on Linux,
      bubblewrap / firejail are the equivalent. None of this is built
      in — it's your call.

  This script does not apply a filesystem sandbox on top of the agent
  CLI. If filesystem isolation matters more than iteration speed,
  run the whole thing inside a VM you control.

USAGE
  Always invoke through `uv run` so the dispatcher and the actor procs
  it spawns share the in-tree Monarch build.

  # Continuous loop, default settings:
  uv run python scripts/auto_fix/dispatch.py

  # Single cycle, full local fix attempt, no push and no PR:
  uv run python scripts/auto_fix/dispatch.py --once --dry-run

  # Use a specific test name (skips upstream discovery entirely):
  uv run python scripts/auto_fix/dispatch.py --once \\
      --manual-failure 'python/tests/test_x.py::test_y@4'

  # Run unattended overnight, use all but one core, cap concurrency:
  uv run python scripts/auto_fix/dispatch.py --reserved-cores 1 \\
      --max-per-cycle 2 --poll-seconds 900

  # Try Codex instead of Claude:
  uv run python scripts/auto_fix/dispatch.py --agent codex

INSPECTING WHAT THE LOOP DID
  ~/.monarch-auto-fix/state.json            -- one entry per issue_key
  ~/.monarch-auto-fix/worktrees/<key>/      -- branch, fix, AUTO_FIX_REPORT.md
  ~/.monarch-auto-fix/contexts/<key>/       -- accumulated context notes
  ~/.monarch-auto-fix/logs/                 -- agent stdouts (fixer + evaluator)

  To force a retry: edit state.json and remove that entry's status (or
  set to "new"), then restart. Worktrees and context notes are reused.

LIMITATIONS
  - Only the OS-matching workflow is acted on (we can't repro Linux
    failures on a Mac).
  - Only the most-recent decided commit on main is mined; agents that
    need historical CI data fetch it themselves with `gh`.
  - Sequential by default (--max-per-cycle=1). Parallel dispatch is
    possible but expensive in CPU + agent-cost; raise carefully.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import hashlib
import json
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import traceback
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Iterable

import monarch.actor
from monarch._src.actor.supervision import MeshFailure
from monarch._rust_bindings.monarch_hyperactor.supervision import SupervisionError
from monarch.actor import Actor, endpoint, this_host

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_UPSTREAM_REMOTE = "upstream"
DEFAULT_ORIGIN_REMOTE = "origin"
DEFAULT_AGENT = "claude"
AGENT_CHOICES = ["claude", "codex", "local"]

REPO_ROOT = Path(__file__).resolve().parents[2]
PROMPT_DIR = Path(__file__).resolve().parent
FIXER_PROMPT_FILE = PROMPT_DIR / "agent_prompt.md"
EVALUATOR_PROMPT_FILE = PROMPT_DIR / "evaluator_prompt.md"

DEFAULT_STATE_DIR = Path.home() / ".monarch-auto-fix"

# These are reassigned in main() once --state-dir is known.
STATE_HOME = DEFAULT_STATE_DIR
STATE_FILE = STATE_HOME / "state.json"
WORKTREE_DIR = STATE_HOME / "worktrees"
LOG_DIR = STATE_HOME / "logs"
ARTIFACT_CACHE = STATE_HOME / "artifacts"
LOCK_FILE = STATE_HOME / "dispatch.lock"
CONTEXTS_DIR = STATE_HOME / "contexts"

DEFAULT_POLL_SECONDS = 10 * 60
RUN_LIST_LIMIT = 10


def default_target_workflow() -> str:
    """Workflow whose failures we'll try to fix.

    Tests can only be reproduced on the OS we're running on, so a Mac
    can only fix things that fail in the macOS workflow, and a Linux box
    only fixes things from the Linux workflow. Docs, CodeQL, etc. are
    explicitly out of scope — we focus on the main "CI" job.
    """
    return "CI (macOS)" if platform.system() == "Darwin" else "CI"
PR_TITLE_PREFIX = "[auto-fix:"
PR_BODY_MARKER_FMT = "<!-- auto-fix-key: {key} -->"
MAX_ATTEMPTS = 3
FIXER_TIMEOUT_SECONDS = 35 * 60
EVALUATOR_TIMEOUT_SECONDS = 10 * 60
DEFAULT_MAX_OPEN_PRS = 3
DEFAULT_MIN_CONFIDENCE = 75
DEFAULT_MAX_PER_CYCLE = 1  # only one fixer per cycle to avoid CPU contention
DEFAULT_RESERVED_CORES = 4  # save this many cores for the human + their browser
DEFAULT_PRUNE_RETENTION_DAYS = 7  # how long to keep terminal-status fixes

# Local OpenAI-compatible server defaults (ollama 0.x exposes /v1 at 11434;
# `llama-server` from llama.cpp typically uses 8080). User points us at
# whichever they're already running.
# Default to ollama's port (11434) since it's the most common local
# OpenAI-compatible server. See LOCAL BACKENDS in --help for recipes for
# llama.cpp, LM Studio, vLLM, and hosted providers.
DEFAULT_LOCAL_API_URL = "http://localhost:11434/v1"
DEFAULT_LOCAL_MODEL = "qwen2.5-coder:32b"
LOCAL_AGENT_MAX_ITERATIONS = 60
LOCAL_AGENT_BASH_TIMEOUT_S = 600
LOCAL_AGENT_OUTPUT_TRUNCATE = 6000


def _truncate_output(out: str, limit: int = LOCAL_AGENT_OUTPUT_TRUNCATE) -> str:
    """Keep head + tail when output is too long.

    For build/test output the LAST lines (test result, error message) are
    usually the most informative; truncating from the start would hide
    them. We keep the first 25% and last 75% of the budget, with a
    machine-readable elision marker in the middle.
    """
    if not out:
        return ""
    if len(out) <= limit:
        return out
    head_len = max(200, limit // 4)
    tail_len = max(200, limit - head_len - 100)
    head = out[:head_len]
    tail = out[-tail_len:]
    elided = len(out) - head_len - tail_len
    return (head
            + f"\n... ({elided} chars elided; original was {len(out)} chars) ...\n"
            + tail)

# Extra args passed to the agent CLI invocation. Set via --agent-extra-args
# for environments that need additional flags (e.g. corporate sandboxes).
_extra_agent_args: list[str] = []

# Local OpenAI-compatible endpoint config. Populated from CLI args in main().
_local_api_url: str = DEFAULT_LOCAL_API_URL
_local_model: str = DEFAULT_LOCAL_MODEL
_local_api_key: str = "ollama"
PR_BODY_MAX_CHARS = 60000  # GitHub's hard limit is 65536; leave headroom


def cores_per_agent(num_concurrent: int, reserved_cores: int) -> int:
    """Decide how many cores each in-flight subagent may use for builds/tests.

    total = os.cpu_count()
    available = max(1, total - reserved_cores)
    per_agent = max(1, available // max(1, num_concurrent))
    """
    total = os.cpu_count() or 8
    available = max(1, total - max(0, reserved_cores))
    return max(1, available // max(1, num_concurrent))


def agent_env(num_concurrent: int = 1,
              reserved_cores: int = DEFAULT_RESERVED_CORES,
              extra: dict[str, str] | None = None) -> dict[str, str]:
    """Build a subprocess env that caps build/test parallelism.

    We DO NOT cap memory — constraining memory can cause real test
    failures to manifest as oom artifacts. We only cap CPU parallelism
    for builds and Rust tests, and leave `reserved_cores` free for the
    human (default 4 — set to 0 if running overnight unattended).
    """
    env = os.environ.copy()
    j = str(cores_per_agent(num_concurrent, reserved_cores))
    env.setdefault("MAKEFLAGS", f"-j{j}")
    env.setdefault("CARGO_BUILD_JOBS", j)
    env.setdefault("CMAKE_BUILD_PARALLEL_LEVEL", j)
    env.setdefault("RAYON_NUM_THREADS", j)
    # Deliberately NOT setting OMP_NUM_THREADS / MKL_NUM_THREADS — they
    # can change pytorch behavior and mask or unmask races.
    env.setdefault("AUTO_FIX_BUILD_JOBS", j)
    env.setdefault("AUTO_FIX_TEST_THREADS", j)
    if extra:
        env.update(extra)
    return env

# Status values for state.fixes[<key>].status
STATUS_NEW = "new"                  # discovered, not yet dispatched
STATUS_FIXING = "fixing"            # fixer subagent is running
STATUS_EVALUATING = "evaluating"    # evaluator subagent is running
STATUS_GAVE_UP = "gave_up"          # fixer or evaluator rejected; not retrying
STATUS_PR_OPEN = "pr_open"          # PR opened upstream
STATUS_PR_FAILED_CI = "pr_failed_ci"  # PR CI red; will re-evaluate
STATUS_PR_MERGED = "pr_merged"      # PR merged
STATUS_PR_CLOSED = "pr_closed"      # PR closed without merge
STATUS_DUP_OF_EXISTING_PR = "dup_of_existing_pr"  # someone else already fixed

TERMINAL_STATUSES = {
    STATUS_GAVE_UP,
    STATUS_PR_MERGED,
    STATUS_PR_CLOSED,
    STATUS_DUP_OF_EXISTING_PR,
}

# ---------------------------------------------------------------------------
# Small utilities
# ---------------------------------------------------------------------------


def log(msg: str) -> None:
    ts = dt.datetime.now().isoformat(timespec="seconds")
    print(f"[{ts}] {msg}", flush=True)


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def run(
    cmd: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    timeout: int | None = None,
    check: bool = True,
    capture: bool = True,
    input_text: str | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a subprocess, returning a CompletedProcess. Logs on failure."""
    res = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        env=env,
        timeout=timeout,
        check=False,
        text=True,
        input=input_text,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    )
    if check and res.returncode != 0:
        log(f"command failed: {' '.join(cmd)} (exit={res.returncode})")
        if capture:
            sys.stderr.write(res.stderr or "")
        raise subprocess.CalledProcessError(res.returncode, cmd, res.stdout, res.stderr)
    return res


def gh_json(args: list[str]) -> Any:
    """Run gh and parse stdout as JSON."""
    res = run(["gh", *args])
    if not res.stdout.strip():
        return None
    return json.loads(res.stdout)


def safe_slug(name: str, max_len: int = 50) -> str:
    """Make a stable, branch-safe slug from an arbitrary test name."""
    s = re.sub(r"[^A-Za-z0-9]+", "-", name).strip("-").lower()
    return s[:max_len] or "test"


def issue_key_for(test_name: str) -> str:
    """Stable, short key derived from the test name only.

    Same flake => same key across restarts and across CI runs.
    """
    h = hashlib.sha1(test_name.encode("utf-8")).hexdigest()[:8]
    return f"{safe_slug(test_name, 40)}-{h}"


_REMOTE_URL_RE = re.compile(
    r"(?:[:/])([^:/\s]+/[^:/\s]+?)(?:\.git)?/?$"
)


def resolve_remote_repo(remote_name: str) -> str:
    """Translate a git remote name (e.g. 'upstream') into an owner/repo
    string suitable for `gh --repo owner/repo`.

    Recognises both SSH (`git@github.com:owner/repo.git`) and HTTPS
    (`https://github.com/owner/repo[.git]`) URLs. Raises if the remote
    is not configured or the URL doesn't parse.
    """
    res = run(
        ["git", "remote", "get-url", remote_name],
        cwd=REPO_ROOT, check=False,
    )
    if res.returncode != 0:
        raise SystemExit(
            f"git remote {remote_name!r} is not configured. "
            f"Add it (e.g. `git remote add {remote_name} <url>`) or pass "
            f"--{remote_name} explicitly."
        )
    url = (res.stdout or "").strip()
    m = _REMOTE_URL_RE.search(url)
    if not m:
        raise SystemExit(
            f"could not parse owner/repo from `git remote get-url "
            f"{remote_name}`: {url!r}"
        )
    return m.group(1)


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------


def _ensure_dirs() -> None:
    for p in (STATE_HOME, WORKTREE_DIR, LOG_DIR, ARTIFACT_CACHE, CONTEXTS_DIR):
        p.mkdir(parents=True, exist_ok=True)


def configure_paths(state_dir: Path) -> None:
    """Reassign module-level path globals so they all live under state_dir."""
    global STATE_HOME, STATE_FILE, WORKTREE_DIR, LOG_DIR, ARTIFACT_CACHE, LOCK_FILE, CONTEXTS_DIR
    STATE_HOME = state_dir
    STATE_FILE = state_dir / "state.json"
    WORKTREE_DIR = state_dir / "worktrees"
    LOG_DIR = state_dir / "logs"
    ARTIFACT_CACHE = state_dir / "artifacts"
    LOCK_FILE = state_dir / "dispatch.lock"
    CONTEXTS_DIR = state_dir / "contexts"
    _ensure_dirs()


def context_dir_for(issue_key: str) -> Path:
    p = CONTEXTS_DIR / issue_key
    p.mkdir(parents=True, exist_ok=True)
    return p


def append_context_note(issue_key: str, header: str, body: str) -> None:
    notes = context_dir_for(issue_key) / "notes.md"
    with notes.open("a") as f:
        f.write(f"\n\n## {header}  ({utc_now_iso()})\n\n{body}\n")


def read_context_notes(issue_key: str) -> str:
    notes = context_dir_for(issue_key) / "notes.md"
    if notes.exists():
        return notes.read_text()[-12000:]  # cap so prompt doesn't blow up
    return ""


def load_state() -> dict[str, Any]:
    _ensure_dirs()
    if not STATE_FILE.exists():
        return {"version": 1, "fixes": {}, "last_polled_at": None}
    try:
        with STATE_FILE.open() as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        backup = STATE_FILE.with_suffix(f".broken-{int(time.time())}.json")
        log(f"state file unreadable ({e}); moving to {backup} and starting fresh")
        STATE_FILE.rename(backup)
        return {"version": 1, "fixes": {}, "last_polled_at": None}


def save_state(state: dict[str, Any]) -> None:
    _ensure_dirs()
    tmp = STATE_FILE.with_suffix(".tmp")
    with tmp.open("w") as f:
        json.dump(state, f, indent=2, sort_keys=True)
    tmp.replace(STATE_FILE)


def upsert_fix(state: dict[str, Any], key: str, **fields: Any) -> dict[str, Any]:
    fix = state["fixes"].setdefault(key, {"issue_key": key, "attempts": 0})
    fix.update(fields)
    fix["updated_at"] = utc_now_iso()
    save_state(state)
    return fix


# ---------------------------------------------------------------------------
# CI failure discovery
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class FailedTest:
    test_name: str          # e.g. "python/tests/test_foo.py::test_bar"
    test_kind: str          # "pytest" or "nextest"
    group_number: int | None  # pytest split group (1..total) or None
    total_groups: int | None
    run_id: str             # gh run database id
    run_url: str
    log_excerpt: str        # ~200 lines of relevant log
    workflow_name: str
    head_sha: str

    @property
    def issue_key(self) -> str:
        return issue_key_for(self.test_name)


def list_recent_failed_runs(repo: str, target_workflow: str,
                            limit: int = RUN_LIST_LIMIT,
                            n_recent_commits: int = 1) -> list[dict[str, Any]]:
    """Get failed runs of `target_workflow` from the N most-recent commits
    on main.

    Only the workflow named `target_workflow` is considered — docs,
    CodeQL, dependency-graph, etc. are intentionally ignored. The OS-
    specific default (set by `default_target_workflow()`) means a Mac
    only sees failures that can be reproduced on a Mac.

    Default is the SINGLE most-recent commit. Historical runs are
    intentionally excluded; if the failure has since been fixed upstream,
    we don't want to spend an agent on it. A commit is "decided" once
    `target_workflow`'s run on that commit has completed. Skip commits
    where it's still pending (try them next cycle). On the first
    decided-with-failures commit, take that commit's failed run(s) and
    stop. On the first decided-all-green commit, stop entirely.
    """
    rows = gh_json([
        "run", "list",
        "--repo", repo,
        "--branch", "main",
        "--limit", str(limit),
        "--json", "databaseId,workflowName,headSha,headBranch,"
                  "createdAt,event,url,conclusion,status",
    ]) or []
    # Filter strictly to the target workflow.
    rows = [r for r in rows if r.get("workflowName") == target_workflow]
    target_shas: list[str] = []
    seen: set[str] = set()
    for r in rows:
        sha = r.get("headSha")
        if not sha or sha in seen:
            continue
        seen.add(sha)
        runs_for_sha = [x for x in rows if x.get("headSha") == sha]
        all_done = all(x.get("status") == "completed" for x in runs_for_sha)
        if not all_done:
            continue
        any_failed = any(x.get("conclusion") == "failure" for x in runs_for_sha)
        if any_failed:
            target_shas.append(sha)
            if len(target_shas) >= n_recent_commits:
                break
        else:
            break
    if not target_shas:
        return []
    return [
        r for r in rows
        if r.get("headSha") in target_shas
        and r.get("status") == "completed"
        and r.get("conclusion") == "failure"
    ]


def list_recent_completed_runs(repo: str, target_workflow: str,
                               n_recent_commits: int = 3,
                               limit: int = 30) -> list[dict[str, Any]]:
    """Get completed runs of `target_workflow` from the N most-recent
    commits on main, regardless of conclusion.

    Used by the TestDataActor's history refresh to detect cases where a
    previously-failing test is now passing on a newer all-green commit
    (which `list_recent_failed_runs` would skip).
    """
    rows = gh_json([
        "run", "list",
        "--repo", repo,
        "--branch", "main",
        "--limit", str(limit),
        "--json", "databaseId,workflowName,headSha,headBranch,"
                  "createdAt,event,url,conclusion,status",
    ]) or []
    rows = [r for r in rows if r.get("workflowName") == target_workflow
            and r.get("status") == "completed"]
    target_shas: list[str] = []
    seen: set[str] = set()
    for r in rows:
        sha = r.get("headSha")
        if not sha or sha in seen:
            continue
        seen.add(sha)
        target_shas.append(sha)
        if len(target_shas) >= n_recent_commits:
            break
    return [r for r in rows if r.get("headSha") in target_shas]


def list_failed_jobs(repo: str, run_id: str) -> list[dict[str, Any]]:
    data = gh_json([
        "run", "view", run_id,
        "--repo", repo,
        "--json", "jobs",
    ]) or {}
    return [
        j for j in data.get("jobs", [])
        if j.get("conclusion") == "failure"
    ]


def list_run_artifacts(repo: str, run_id: str) -> list[dict[str, Any]]:
    """Use the underlying API; gh has no JSON artifacts subcommand."""
    res = run([
        "gh", "api",
        f"repos/{repo}/actions/runs/{run_id}/artifacts",
        "--paginate",
    ], check=False)
    if res.returncode != 0:
        return []
    try:
        data = json.loads(res.stdout or "{}")
    except json.JSONDecodeError:
        return []
    return data.get("artifacts", []) or []


def download_artifact(repo: str, run_id: str, artifact_name: str, dest: Path) -> bool:
    dest.mkdir(parents=True, exist_ok=True)
    res = run(
        ["gh", "run", "download", run_id,
         "--repo", repo,
         "--name", artifact_name,
         "-D", str(dest)],
        check=False,
    )
    if res.returncode != 0:
        log(f"  artifact download failed for {artifact_name}: {res.stderr.strip()}")
        return False
    return True


def parse_junit_xml(path: Path) -> list[tuple[str, str, str]]:
    """Return list of (classname.name, kind, message) for failures/errors."""
    try:
        tree = ET.parse(path)
    except ET.ParseError as e:
        log(f"  could not parse junit xml {path}: {e}")
        return []
    out: list[tuple[str, str, str]] = []
    for tc in tree.iter("testcase"):
        for child in tc:
            tag = child.tag.lower()
            if tag in ("failure", "error"):
                classname = tc.get("classname", "")
                name = tc.get("name", "")
                full = f"{classname}::{name}" if classname else name
                msg = (child.get("message") or child.text or "")[:5000]
                out.append((full, tag, msg))
                break
    return out


def parse_junit_xml_all(path: Path) -> list[tuple[str, str, str]]:
    """Return (classname.name, status, message) for every testcase.

    `status` is one of ``"passed"``, ``"failure"``, ``"error"``, or
    ``"skipped"``. Used to populate per-test history with both passes
    and failures so the dispatcher can spot a previously-failing test
    that's now passing on a new commit.
    """
    try:
        tree = ET.parse(path)
    except ET.ParseError as e:
        log(f"  could not parse junit xml {path}: {e}")
        return []
    out: list[tuple[str, str, str]] = []
    for tc in tree.iter("testcase"):
        classname = tc.get("classname", "")
        name = tc.get("name", "")
        full = f"{classname}::{name}" if classname else name
        status = "passed"
        msg = ""
        for child in tc:
            tag = child.tag.lower()
            if tag in ("failure", "error"):
                status = tag
                msg = (child.get("message") or child.text or "")[:5000]
                break
            if tag == "skipped":
                status = "skipped"
                break
        out.append((full, status, msg))
    return out


def junit_pytest_nodeid(classname: str, name: str) -> str:
    """pytest junit emits classname like 'python.tests.test_foo' for
    'python/tests/test_foo.py::test_bar'. Convert back to a nodeId so the
    fixer can run `pytest <nodeid>` directly."""
    if classname:
        path = classname.replace(".", "/") + ".py"
        return f"{path}::{name}"
    return name


# Log scraping has been intentionally removed. JUnit XML is the source
# of truth for which tests failed. Agents needing log context fetch it
# themselves via `gh run view <run_id> --log-failed --job <job_id>` —
# the prompt teaches them how, and they only run this command when
# they need it. This keeps the dispatcher fast and keeps log-format
# fragility out of the discovery loop.


def fetch_specific_run(repo: str, run_id: str) -> list[dict[str, Any]]:
    """Fetch a single run by ID and return it in list_recent_failed_runs's
    shape. Used by --from-run to bypass the latest-commit filter and
    target a specific run for testing."""
    res = gh_json([
        "run", "view", run_id,
        "--repo", repo,
        "--json", "databaseId,workflowName,headSha,headBranch,"
                  "createdAt,event,url,conclusion,status",
    ]) or {}
    if not res:
        return []
    return [res]


# Top-level `discover_failures` was deleted: discovery now lives on
# the TestDataActor's `discover` endpoint, which additionally records
# pass observations (not just failures) into per-test history. The
# helpers it composed — `list_recent_failed_runs`, `list_run_artifacts`,
# `download_artifact`, `parse_junit_xml_all` — are still used directly
# from the actor.


# ---------------------------------------------------------------------------
# Existing-PR dedup
# ---------------------------------------------------------------------------


def existing_pr_for(issue_key: str, repos: list[str]) -> dict[str, Any] | None:
    """Search both repos for any PR whose title contains [auto-fix:<key>].
    Returns the first match (open OR closed OR merged) or None.
    """
    needle = f"[auto-fix:{issue_key}]"
    for repo in repos:
        try:
            prs = gh_json([
                "pr", "list",
                "--repo", repo,
                "--state", "all",
                "--search", needle,
                "--limit", "5",
                "--json", "number,state,url,title,headRefName,mergedAt",
            ]) or []
        except subprocess.CalledProcessError:
            continue
        for pr in prs:
            if needle in (pr.get("title") or ""):
                pr["repo"] = repo
                return pr
    return None


def existing_pr_by_test_name(test_name: str, repos: list[str]) -> dict[str, Any] | None:
    """Looser dedup: if anyone has an open PR mentioning this test name in
    the title or body, treat as already-being-handled."""
    for repo in repos:
        try:
            prs = gh_json([
                "pr", "list",
                "--repo", repo,
                "--state", "open",
                "--search", f'"{test_name}" in:title,body',
                "--limit", "5",
                "--json", "number,state,url,title,headRefName",
            ]) or []
        except subprocess.CalledProcessError:
            continue
        for pr in prs:
            pr["repo"] = repo
            return pr
    return None


# ---------------------------------------------------------------------------
# Worktree management
# ---------------------------------------------------------------------------


def ensure_worktree(issue_key: str, base_sha: str,
                    origin_remote: str = "origin") -> Path:
    path = WORKTREE_DIR / issue_key
    branch = f"auto-fix/{issue_key}"
    if path.exists() and (path / ".git").exists():
        log(f"  reusing existing worktree at {path}")
        # Make sure branch is checked out and base is correct
        run(["git", "fetch", origin_remote], cwd=path, check=False)
        return path
    path.mkdir(parents=True, exist_ok=True)
    # Branch may already exist on origin from a prior run — try to base off it.
    fetch = run(
        ["git", "fetch", origin_remote, f"{branch}:{branch}"],
        cwd=REPO_ROOT, check=False,
    )
    if fetch.returncode == 0:
        log(f"  branch {branch} already exists on origin; checking it out")
        run(["git", "worktree", "add", str(path), branch], cwd=REPO_ROOT)
    else:
        run(["git", "worktree", "add", "-b", branch, str(path), base_sha], cwd=REPO_ROOT)
    return path


def remove_worktree(issue_key: str) -> None:
    path = WORKTREE_DIR / issue_key
    if not path.exists():
        return
    run(["git", "worktree", "remove", "--force", str(path)],
        cwd=REPO_ROOT, check=False)
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)


def _reap_artifacts(key: str) -> None:
    """Delete every on-disk trace of an issue key: worktree dir, local
    branch, context notes, agent log files. Idempotent; never touches
    remote branches on origin."""
    remove_worktree(key)
    run(["git", "branch", "-D", f"auto-fix/{key}"],
        cwd=REPO_ROOT, check=False)
    cdir = CONTEXTS_DIR / key
    if cdir.exists():
        shutil.rmtree(cdir, ignore_errors=True)
    if LOG_DIR.exists():
        for logfile in LOG_DIR.glob(f"{key}.*.log"):
            logfile.unlink(missing_ok=True)


def prune_stale_artifacts(
    state: dict[str, Any],
    retention_days: int,
    *,
    dry_run: bool = False,
) -> None:
    """Sweep the state dir for stale worktrees, branches, logs, and
    state entries.

    Three kinds of cruft are reaped each cycle:
      1. Worktree records whose on-disk paths have vanished (e.g. the
         user moved the source repo). `git worktree prune` handles it.
      2. Fix entries in a terminal status (merged / closed / gave_up /
         dup) whose `updated_at` is older than `retention_days`. Their
         worktree, local branch, context dir, log files, and state
         entry are all removed.
      3. Orphans: worktree dirs and local `auto-fix/*` branches with
         no matching live state entry. Accumulate when state.json is
         wiped while branches/dirs persist.

    Remote branches on the user's fork (origin) are never touched.
    """
    log("prune: scanning for stale artifacts")
    run(["git", "worktree", "prune"], cwd=REPO_ROOT, check=False)

    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=retention_days)
    reaped = 0

    for key, fix in list(state.get("fixes", {}).items()):
        if fix.get("status") not in TERMINAL_STATUSES:
            continue
        ua = fix.get("updated_at")
        try:
            ts = dt.datetime.fromisoformat((ua or "").replace("Z", "+00:00"))
        except ValueError:
            ts = None
        if ts is None or ts > cutoff:
            continue
        log(f"  [{key}] reaping (status={fix.get('status')}, "
            f"updated_at={ua}, retention={retention_days}d)")
        if dry_run:
            log("    --dry-run: would reap")
            continue
        _reap_artifacts(key)
        state["fixes"].pop(key, None)
        reaped += 1

    live_keys = set(state.get("fixes", {}).keys())
    if WORKTREE_DIR.exists():
        for entry in WORKTREE_DIR.iterdir():
            if not entry.is_dir() or entry.name in live_keys:
                continue
            log(f"  orphan worktree dir: {entry.name}")
            if dry_run:
                log("    --dry-run: would remove")
                continue
            _reap_artifacts(entry.name)
            reaped += 1

    res = run(
        ["git", "branch", "--list", "auto-fix/*",
         "--format=%(refname:short)"],
        cwd=REPO_ROOT, check=False,
    )
    for line in (res.stdout or "").splitlines():
        branch = line.strip()
        if not branch.startswith("auto-fix/"):
            continue
        key = branch[len("auto-fix/"):]
        if key in live_keys or (WORKTREE_DIR / key).exists():
            continue
        log(f"  orphan branch: {branch}")
        if dry_run:
            log("    --dry-run: would delete")
            continue
        run(["git", "branch", "-D", branch], cwd=REPO_ROOT, check=False)
        reaped += 1

    if reaped and not dry_run:
        save_state(state)
    log(f"prune: reaped {reaped} stale artifact(s)")


# ---------------------------------------------------------------------------
# Agent invocation
# ---------------------------------------------------------------------------


def render_template(template: str, fields: dict[str, str]) -> str:
    out = template
    for k, v in fields.items():
        out = out.replace("{" + k + "}", v if v is not None else "")
    return out


# --------- Process-group bookkeeping for clean Ctrl-C ---------------------

_active_subprocs: list[subprocess.Popen] = []


def _kill_all_subprocs(sig: int = signal.SIGTERM) -> None:
    """Send `sig` to the entire process group of every tracked subprocess.

    Children are spawned with `start_new_session=True` so each child is the
    leader of its own group. Killing the group cascades to grandchildren
    (cargo, nextest, conda, etc.).
    """
    for proc in list(_active_subprocs):
        if proc.poll() is not None:
            continue
        try:
            os.killpg(proc.pid, sig)
        except (ProcessLookupError, PermissionError):
            pass


def _shutdown_handler(signum, frame):  # type: ignore[no-untyped-def]
    log(f"received signal {signum}; terminating {len(_active_subprocs)} subprocess group(s)")
    _kill_all_subprocs(signal.SIGTERM)
    # Give them a couple of seconds to exit cleanly, then escalate.
    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline:
        if all(p.poll() is not None for p in _active_subprocs):
            break
        time.sleep(0.1)
    _kill_all_subprocs(signal.SIGKILL)
    release_lock()
    sys.exit(128 + signum)


def _spawn_tracked(cmd: list[str], cwd: Path, env: dict[str, str],
                   stdin=subprocess.PIPE) -> subprocess.Popen:
    """Spawn a subprocess in a new session (its own pgrp) and track it for
    Ctrl-C cleanup. The caller is responsible for calling _untrack() once
    the process has exited."""
    proc = subprocess.Popen(
        cmd, cwd=str(cwd), env=env,
        stdin=stdin, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, text=True, bufsize=1,
        start_new_session=True,
    )
    _active_subprocs.append(proc)
    return proc


def _untrack(proc: subprocess.Popen) -> None:
    try:
        _active_subprocs.remove(proc)
    except ValueError:
        pass


# --------- CLI-driven agents (Claude / Codex) -----------------------------


def invoke_cli_agent(
    agent: str,
    prompt: str,
    cwd: Path,
    *,
    timeout: int,
    log_path: Path,
    num_concurrent: int = 1,
    reserved_cores: int = DEFAULT_RESERVED_CORES,
) -> tuple[int, str]:
    """Run the agent CLI (Claude Code or Codex) headlessly with permission
    checks bypassed. Returns (exit_code, stdout)."""
    if agent == "claude":
        cmd = [
            "claude", "-p",
            "--dangerously-skip-permissions",
            "--output-format", "text",
            *_extra_agent_args,
        ]
    elif agent == "codex":
        cmd = [
            "codex", "exec",
            "--skip-git-repo-check",
            *_extra_agent_args,
            "-",
        ]
    else:
        raise ValueError(f"unknown CLI agent: {agent!r}")

    log(f"  invoking {agent} in {cwd} (timeout {timeout}s); log -> {log_path}")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w") as logf:
        logf.write(f"# CMD: {' '.join(cmd)}\n# CWD: {cwd}\n\n# PROMPT\n{prompt}\n\n# OUTPUT\n")
        logf.flush()
        try:
            proc = _spawn_tracked(
                cmd, cwd,
                env=agent_env(num_concurrent=num_concurrent,
                              reserved_cores=reserved_cores),
            )
        except FileNotFoundError as e:
            log(f"  agent CLI not found: {e}")
            return 127, ""

        assert proc.stdin and proc.stdout
        try:
            proc.stdin.write(prompt)
            proc.stdin.close()
        except BrokenPipeError:
            pass

        captured: list[str] = []
        deadline = time.monotonic() + timeout
        try:
            while True:
                if time.monotonic() > deadline:
                    os.killpg(proc.pid, signal.SIGTERM)
                    log(f"  agent timed out after {timeout}s")
                    break
                line = proc.stdout.readline()
                if not line:
                    break
                logf.write(line)
                logf.flush()
                captured.append(line)
            proc.wait(timeout=30)
        finally:
            _untrack(proc)
    return proc.returncode or 0, "".join(captured)


# --------- OpenAI-compatible local agent (ollama, llama.cpp) ---------------


# Commands the local-agent executor will refuse to run. The model gets
# the refusal as <output exit_code="126"> back so it can adjust. We are
# deliberately conservative — the goal is to stop runaway "I'll just sudo
# this whole machine into shape" behaviors, not to be a real sandbox.
_DANGEROUS_PATTERNS = [
    # System-level / destructive
    (re.compile(r"\bsudo\b"),
     "sudo is not allowed; the dispatcher runs as your unprivileged user"),
    (re.compile(r"\brm\s+-[a-z]*r[a-z]*f?\s+/(?!tmp/|var/folders/|var/tmp/)"),
     "recursive rm at the filesystem root"),
    (re.compile(r":\(\)\{\s*:"),
     "fork-bomb-shaped command"),
    (re.compile(r"\bmkfs\b|\bdd\s+if=|>/dev/(?:disk|sda|nvme)"),
     "raw-disk write"),
    (re.compile(r"\bchmod\s+-R\s+[0-7]+\s+/(?!tmp)"),
     "recursive chmod outside /tmp"),
    (re.compile(r"\bchown\s+-R\b.*\s+/(?!tmp/|Users/)"),
     "recursive chown outside /tmp or /Users"),
    # Off-policy package managers — agent must use uv + cargo only.
    (re.compile(r"\b(?:brew|apt(?:-get)?|yum|dnf|port|pacman)\b"),
     "system package managers are off-policy; use uv (Python) and cargo (Rust) only"),
    (re.compile(r"\b(?:conda|miniconda|anaconda|mamba|micromamba|pyenv)\b"),
     "conda/pyenv are off-policy; use `uv venv` for Python environments"),
    (re.compile(r"\bpip\s+install\b(?!\s+--help)"),
     "raw `pip install` is off-policy; use `uv pip install` instead"),
    (re.compile(r"curl\s+[^|]*\|\s*(?:sh|bash)\b|wget\s+[^|]*\|\s*(?:sh|bash)\b"),
     "curl|sh / wget|sh installers are off-policy; you must not bootstrap "
     "system tooling"),
    (re.compile(r"Miniconda3-.*\.sh|Anaconda3-.*\.sh|install\.python-poetry\.org"),
     "off-policy installer URL"),
]


def _refuse_dangerous(cmd: str) -> str | None:
    """Return a refusal message if `cmd` matches any dangerous pattern,
    else None."""
    for pat, why in _DANGEROUS_PATTERNS:
        if pat.search(cmd):
            return (f"REFUSED: {why}. (Pattern: {pat.pattern!r}.) "
                    f"Adjust your plan. Tooling policy: use ONLY "
                    f"`uv` (for Python) and `cargo` (for Rust). Both "
                    f"are already installed.")
    return None


# Accept either XML-style `<bash>...</bash>` or markdown fenced bash
# (```bash\n...\n```). Coder-tuned models default heavily to markdown
# regardless of system-prompt instructions, so we allow both.
_BASH_BLOCK_RE = re.compile(
    r"<bash>(.*?)</bash>"
    r"|```(?:bash|sh|shell)?[ \t]*\n(.*?)\n```",
    re.DOTALL,
)
# A "complete" bash block, used to detect a turn boundary in the stream.
_COMPLETE_BASH_BLOCK_RE = re.compile(
    r"</bash>"
    r"|```(?:bash|sh|shell)?[ \t]*\n.*?\n```",
    re.DOTALL,
)
_DONE_RE = re.compile(r"<done\s*/>", re.IGNORECASE)


def _extract_bash_commands(text: str) -> list[str]:
    """Pull bash command strings out of either XML or markdown blocks."""
    out: list[str] = []
    for m in _BASH_BLOCK_RE.finditer(text):
        cmd = m.group(1) or m.group(2) or ""
        cmd = cmd.strip()
        if cmd:
            out.append(cmd)
    return out


_LOCAL_AGENT_SYSTEM = """\
You are an autonomous coding agent operating inside a git worktree. You have
exactly ONE tool: a bash block. Wrap commands in EITHER:

  <bash>your command here</bash>

OR a markdown fence:

  ```bash
  your command here
  ```

Either form works; pick one and stay with it. The executor will run the
contents of the FIRST bash block in your turn and start your next turn
with a real `<output exit_code="N">...</output>` block.

CRITICAL — TURN PROTOCOL
- Each turn you emit EXACTLY ONE bash block, then STOP.
- DO NOT predict, simulate, or write `<output>` blocks yourself. They
  are produced ONLY by the executor.
- DO NOT emit a second bash block in the same turn — generation will be
  cut off as soon as you close the first one.
- When the WHOLE task is complete, emit `<done/>` on its own (no bash).

Other rules:
- Output is truncated at {trunc} chars per command, keeping head AND
  tail (so end-of-log lines like test results survive). If you still
  need to see more, narrow the command (e.g. add `2>&1 | tail -100` or
  `grep ...`). The test PASS/FAIL line will appear at the END of the
  output for cargo/pytest, so look there first.
- Decide and act — there is no human. Do not output questions.
- If you cannot complete the task, write your reasoning into
  AUTO_FIX_REPORT.md (with `verdict: give_up` in the frontmatter) using
  `<bash>cat > AUTO_FIX_REPORT.md <<'EOF' ... EOF</bash>` semantics, THEN
  emit `<done/>`.

A long task description follows in the user message. Read it carefully.
""".format(trunc=LOCAL_AGENT_OUTPUT_TRUNCATE)


def _stream_chat_completion(url: str, body: dict, headers: dict,
                            *, timeout: int, log_writer=None,
                            stop_on_complete_bash_block: bool = False) -> str:
    """POST a streaming chat completion and return the assembled assistant
    content. Tokens are appended to `log_writer` as they arrive.

    If `stop_on_complete_bash_block` is true, abort the read as soon as
    the accumulated content contains a complete bash block (either
    `</bash>` or a closing ``` after an opening ```bash). This forces a
    turn boundary on each command so smaller models can't hallucinate
    tool output and roll forward in one giant turn.
    """
    import urllib.request
    body = dict(body, stream=True)
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    chunks: list[str] = []
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        try:
            for raw in resp:
                line = raw.decode("utf-8", errors="replace").strip()
                if not line.startswith("data:"):
                    continue
                payload = line[5:].strip()
                if payload == "[DONE]":
                    break
                try:
                    chunk = json.loads(payload)
                except json.JSONDecodeError:
                    continue
                try:
                    delta = chunk["choices"][0]["delta"].get("content")
                except (KeyError, IndexError, TypeError):
                    continue
                if delta:
                    chunks.append(delta)
                    if log_writer is not None:
                        log_writer.write(delta)
                        log_writer.flush()
                    if stop_on_complete_bash_block:
                        # Cheap-ish: only re-scan when the most recent
                        # tokens could close a block.
                        recent = "".join(chunks[-32:])
                        if "</bash>" in recent or recent.endswith("```\n") or recent.endswith("```"):
                            full = "".join(chunks)
                            if _COMPLETE_BASH_BLOCK_RE.search(full):
                                break
        finally:
            try:
                resp.close()
            except Exception:
                pass
    return "".join(chunks)


def invoke_local_agent(
    prompt: str,
    cwd: Path,
    *,
    api_url: str,
    model: str,
    api_key: str,
    timeout: int,
    log_path: Path,
    num_concurrent: int = 1,
    reserved_cores: int = DEFAULT_RESERVED_CORES,
    max_iterations: int = LOCAL_AGENT_MAX_ITERATIONS,
) -> tuple[int, str]:
    """Drive a local OpenAI-compatible chat completions endpoint (ollama,
    llama.cpp's `llama-server`, vLLM, LM Studio, etc.) in a minimal ReAct
    loop using a single `<bash>` tool.

    Note: capabilities depend heavily on the model. A 7B–14B model will
    struggle to complete a real fix; a 32B+ coder model is the floor for
    useful behavior. Don't expect parity with hosted Claude/Codex.
    """
    env = agent_env(num_concurrent, reserved_cores)
    messages: list[dict] = [
        {"role": "system", "content": _LOCAL_AGENT_SYSTEM},
        {"role": "user", "content": prompt},
    ]
    deadline = time.monotonic() + timeout
    full_assistant_text: list[str] = []

    log(f"  invoking local agent (api={api_url} model={model}) in {cwd} "
        f"(timeout {timeout}s, max_iter {max_iterations})")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    headers = {"Content-Type": "application/json",
               "Authorization": f"Bearer {api_key}"}
    chat_url = api_url.rstrip("/") + "/chat/completions"

    with log_path.open("w") as logf:
        logf.write(f"# api={api_url}\n# model={model}\n# cwd={cwd}\n\n"
                   f"# PROMPT\n{prompt}\n\n# CONVERSATION\n")
        logf.flush()
        for it in range(1, max_iterations + 1):
            if time.monotonic() > deadline:
                logf.write(f"\n# TIMEOUT after {timeout}s at iteration {it}\n")
                return 124, "\n".join(full_assistant_text)

            iter_start = time.monotonic()
            logf.write(f"\n--- iter {it} assistant (streaming) ---\n")
            logf.flush()
            # The HTTP read deadline resets on each chunk, so a 1-hour
            # timeout here really means "no chunks for an hour", not "must
            # finish within an hour". Plenty for slow local models even on
            # cold prompts.
            try:
                content = _stream_chat_completion(
                    chat_url,
                    {"model": model, "messages": messages},
                    headers, timeout=3600, log_writer=logf,
                    stop_on_complete_bash_block=True,
                )
            except Exception as e:
                logf.write(f"\n# API error at iteration {it}: {e}\n")
                return 1, "\n".join(full_assistant_text)

            if not content:
                logf.write("\n# empty response from model; bailing\n")
                return 1, "\n".join(full_assistant_text)

            messages.append({"role": "assistant", "content": content})
            full_assistant_text.append(content)
            elapsed = time.monotonic() - iter_start
            logf.write(f"\n--- iter {it} done in {elapsed:.1f}s ---\n")
            logf.flush()

            if _DONE_RE.search(content):
                logf.write("\n# <done/> received; ending\n")
                return 0, "\n".join(full_assistant_text)

            commands = _extract_bash_commands(content)
            if not commands:
                logf.write("\n# no <bash> commands and no <done/>; bailing\n")
                return 1, "\n".join(full_assistant_text)

            outputs: list[str] = []
            for cmd in commands:
                cmd = cmd.strip()
                logf.write(f"\n--- iter {it} bash ---\n$ {cmd}\n")
                logf.flush()
                refusal = _refuse_dangerous(cmd)
                if refusal:
                    logf.write(refusal + "\n")
                    logf.flush()
                    outputs.append(f'<output exit_code="126">\n{refusal}\n</output>')
                    continue
                try:
                    bash_proc = _spawn_tracked(
                        ["bash", "-c", cmd], cwd, env, stdin=subprocess.DEVNULL,
                    )
                    try:
                        out, _ = bash_proc.communicate(
                            timeout=LOCAL_AGENT_BASH_TIMEOUT_S)
                    except subprocess.TimeoutExpired:
                        os.killpg(bash_proc.pid, signal.SIGTERM)
                        try:
                            out, _ = bash_proc.communicate(timeout=10)
                        except subprocess.TimeoutExpired:
                            os.killpg(bash_proc.pid, signal.SIGKILL)
                            out = "(killed after timeout)\n"
                        rc = 124
                    else:
                        rc = bash_proc.returncode
                    finally:
                        _untrack(bash_proc)
                except Exception as e:
                    out = f"executor error: {e}\n"
                    rc = 1
                truncated = _truncate_output(out or "")
                outputs.append(f'<output exit_code="{rc}">\n{truncated}\n</output>')
                logf.write(f"{truncated}\n")
                logf.flush()
            messages.append({"role": "user", "content": "\n".join(outputs)})

        logf.write(f"\n# hit max_iterations={max_iterations} without <done/>\n")
    return 1, "\n".join(full_assistant_text)


# --------- Monarch actor wrapper: one proc per agent invocation -----------


class AgentActor(Actor):
    """One fixer or evaluator agent invocation, hosted on its own proc.

    Each AgentActor instance is spawned on a fresh proc allocated from
    `this_host()` — one proc per invocation, so the agent CLI's
    grandchildren (cargo, nextest, conda, the local LLM bash executor)
    are isolated to a single tear-downable process tree. The proc lives
    only for the duration of one fixer/evaluator call: the dispatcher
    calls `procs.stop()` afterwards, which fires `__cleanup__` and
    SIGTERMs anything still running. If the dispatcher process dies
    unexpectedly (Ctrl-C, crash) the same `__cleanup__` runs on client
    disconnect, so subprocesses don't leak.

    Inside the actor's process the existing module-level
    `_active_subprocs` / `_spawn_tracked` / `_kill_all_subprocs` helpers
    do the heavy lifting; they're scoped to the actor's own process and
    don't conflict with anything in the dispatcher's main process.
    """

    def __init__(
        self,
        agent: str,
        extra_agent_args: list[str],
        local_api_url: str,
        local_model: str,
        local_api_key: str,
        test_data_actor: Any,
    ) -> None:
        self.agent = agent
        # Handle to the shared TestDataActor so this agent can query
        # latest pass/fail history without re-fetching from gh. The
        # dispatcher also uses it; both can fan in through one cache
        # and one set of GitHub API calls.
        self.test_data_actor = test_data_actor
        # The actor proc is a freshly-imported instance of dispatch.py;
        # propagate the dispatcher's CLI-arg-derived globals into it so
        # `invoke_cli_agent` / `invoke_local_agent` see the right values.
        global _extra_agent_args, _local_api_url, _local_model, _local_api_key
        _extra_agent_args = list(extra_agent_args)
        _local_api_url = local_api_url
        _local_model = local_model
        _local_api_key = local_api_key

    @endpoint
    def invoke(
        self,
        prompt: str,
        cwd: str,
        timeout: int,
        log_path: str,
        num_concurrent: int,
        reserved_cores: int,
    ) -> tuple[int, str]:
        cwd_p = Path(cwd)
        log_p = Path(log_path)
        if self.agent in ("claude", "codex"):
            return invoke_cli_agent(
                self.agent, prompt, cwd_p,
                timeout=timeout, log_path=log_p,
                num_concurrent=num_concurrent,
                reserved_cores=reserved_cores,
            )
        if self.agent == "local":
            return invoke_local_agent(
                prompt, cwd_p,
                api_url=_local_api_url, model=_local_model,
                api_key=_local_api_key,
                timeout=timeout, log_path=log_p,
                num_concurrent=num_concurrent,
                reserved_cores=reserved_cores,
            )
        raise ValueError(f"unknown agent: {self.agent!r}")

    @endpoint
    def status_summary(self, test_name: str) -> dict:
        """Pass-through to the shared TestDataActor's history query.

        Provided so the dispatcher can ask any AgentActor for the
        current pass/fail history of a test without keeping a separate
        TestDataActor reference, and so future intra-agent code can
        consult history without depending on the dispatcher.
        """
        return self.test_data_actor.status_summary.call_one(test_name).get()

    def __cleanup__(self, exc: Exception | None) -> None:
        """Called by Monarch when the proc stops or the client disconnects.

        SIGTERMs every tracked subprocess group, waits up to 3s, then
        SIGKILLs anything still alive. Each subprocess was started with
        `start_new_session=True` so killpg() cascades to grandchildren.
        """
        if not _active_subprocs:
            return
        log(f"AgentActor.__cleanup__: terminating "
            f"{len(_active_subprocs)} subprocess group(s) (exc={exc!r})")
        # Tight budget: Monarch's CLEANUP_TIMEOUT defaults to 3 seconds
        # (hyperactor::config::CLEANUP_TIMEOUT); blowing it surfaces as
        # `cleanup error: deadline has elapsed` on the operator side. Use
        # a 1s SIGTERM grace and a 0.5s SIGKILL polling tail, comfortably
        # under that ceiling.
        _kill_all_subprocs(signal.SIGTERM)
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            if all(p.poll() is not None for p in _active_subprocs):
                break
            time.sleep(0.05)
        _kill_all_subprocs(signal.SIGKILL)
        # Brief drain after SIGKILL so stale entries don't pile up across
        # successive cleanups in a long-running dispatcher.
        kill_deadline = time.monotonic() + 0.5
        while time.monotonic() < kill_deadline:
            if all(p.poll() is not None for p in _active_subprocs):
                break
            time.sleep(0.05)


# --------- Monarch actor: shared CI/test-history fetcher --------------------


class TestDataActor(Actor):
    """Centralized poller and history-keeper for CI test results.

    One instance is spawned by the dispatcher at startup on its own
    proc and is shared between the dispatcher and every AgentActor: the
    dispatcher uses it to find new failures each cycle, and the
    AgentActors hold a handle so they can query the latest pass/fail
    history for the test under investigation.

    What it stores:
      ``self.history[test_name]`` is a list of observation dicts —
      one per (sha, run_id) pair — recording status (``passed`` /
      ``failure`` / ``error`` / ``skipped``), the run URL, the
      polling timestamp, and (for failures) a JUnit message excerpt.
      This lets the dispatcher notice when an in-flight test has
      flipped state on a newer commit and push that observation into
      the agent's worktree as additional context.

    Persistence: history is written to
    ``<state_dir>/test_history.json`` after every endpoint that
    mutates it, so a dispatcher restart picks up where we left off.
    """

    def __init__(self, repo: str, artifact_cache_dir: str,
                 history_path: str) -> None:
        self.repo = repo
        # Configure the actor proc's module global so the existing
        # download_artifact / cache-key helpers work unmodified inside
        # this proc.
        global ARTIFACT_CACHE
        ARTIFACT_CACHE = Path(artifact_cache_dir)
        ARTIFACT_CACHE.mkdir(parents=True, exist_ok=True)
        self.history_path = Path(history_path)
        self.history: dict[str, list[dict]] = self._load_history()

    def _load_history(self) -> dict[str, list[dict]]:
        if not self.history_path.exists():
            return {}
        try:
            with self.history_path.open() as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
        except (json.JSONDecodeError, OSError) as e:
            log(f"TestDataActor: history at {self.history_path} unreadable ({e}); starting fresh")
        return {}

    def _save_history(self) -> None:
        self.history_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.history_path.with_suffix(".tmp")
        with tmp.open("w") as f:
            json.dump(self.history, f, indent=2, sort_keys=True)
        tmp.replace(self.history_path)

    def _record_observation(self, test_name: str, sha: str, status: str,
                            run_id: str, run_url: str, workflow_name: str,
                            log_excerpt: str) -> bool:
        """Append an observation. Returns True if newly recorded, False
        if a duplicate (same sha + run_id) was already present."""
        if "::" not in test_name:
            return False
        existing = self.history.setdefault(test_name, [])
        for obs in existing:
            if obs.get("sha") == sha and obs.get("run_id") == run_id:
                return False
        existing.append({
            "sha": sha, "status": status, "run_id": run_id,
            "run_url": run_url, "workflow_name": workflow_name,
            "polled_at": utc_now_iso(),
            "log_excerpt": (log_excerpt[:4000]
                            if status in ("failure", "error") else ""),
        })
        return True

    def _ingest_run(self, r: dict, target_workflow: str) -> int:
        """Download + parse a run's JUnit XMLs and record every
        testcase observation. Returns the count of newly-recorded
        observations."""
        run_id = str(r["databaseId"])
        sha = r.get("headSha", "")
        run_url = r.get("url", "")
        workflow_name = r.get("workflowName", target_workflow)
        artifacts = list_run_artifacts(self.repo, run_id)
        junit_artifacts = [
            a for a in artifacts
            if any(token in a["name"].lower()
                   for token in ("test-result", "junit",
                                 "pytest-result", "nextest-result"))
        ]
        if not junit_artifacts:
            return 0
        new_count = 0
        for art in junit_artifacts:
            dest = ARTIFACT_CACHE / self.repo.replace("/", "_") / run_id / art["name"]
            if not (dest.exists() and any(dest.rglob("*.xml"))):
                if not download_artifact(self.repo, run_id, art["name"], dest):
                    continue
            for xml_path in dest.rglob("*.xml"):
                for test_name, status, msg in parse_junit_xml_all(xml_path):
                    if self._record_observation(
                        test_name, sha=sha, status=status,
                        run_id=run_id, run_url=run_url,
                        workflow_name=workflow_name, log_excerpt=msg,
                    ):
                        new_count += 1
        return new_count

    @endpoint
    def discover(self, target_workflow: str,
                 from_run: str | None = None) -> list[FailedTest]:
        """Find failures on the most-recent decided commit AND record
        every testcase from those XMLs (passes too) in history.

        Returns the failures the dispatcher should act on; pass info is
        a side effect, queryable via :meth:`status_summary`.
        """
        if from_run:
            runs = fetch_specific_run(self.repo, from_run)
            if not runs:
                log(f"TestDataActor.discover: --from-run {from_run} found no run")
            else:
                log(f"TestDataActor.discover: --from-run forcing run {from_run}")
        else:
            runs = list_recent_failed_runs(self.repo, target_workflow)
            log(f"TestDataActor.discover: found {len(runs)} failed runs of {target_workflow!r}")

        failures: dict[str, FailedTest] = {}
        for r in runs:
            run_id = str(r["databaseId"])
            sha = r.get("headSha", "")
            run_url = r.get("url", "")
            workflow_name = r.get("workflowName", target_workflow)
            conclusion = r.get("conclusion", "")
            failures_before = len(failures)
            artifacts = list_run_artifacts(self.repo, run_id)
            junit_artifacts = [
                a for a in artifacts
                if any(token in a["name"].lower()
                       for token in ("test-result", "junit",
                                     "pytest-result", "nextest-result"))
            ]
            if not junit_artifacts:
                if conclusion == "failure":
                    log(f"WARNING: run {run_id} ({run_url}) finished with "
                        f"conclusion=failure but uploaded 0 junit artifacts. "
                        f"At least one job failed without recording test "
                        f"results — check the failed jobs and ensure each "
                        f"runs a test step that uploads junit XML.")
                continue
            for art in junit_artifacts:
                dest = ARTIFACT_CACHE / self.repo.replace("/", "_") / run_id / art["name"]
                if not (dest.exists() and any(dest.rglob("*.xml"))):
                    if not download_artifact(self.repo, run_id, art["name"], dest):
                        continue
                for xml_path in dest.rglob("*.xml"):
                    fname = xml_path.name
                    m = re.search(r"(?:group|results)-?(\d+)", fname)
                    group_number = int(m.group(1)) if m else None
                    for test_name, status, msg in parse_junit_xml_all(xml_path):
                        if "::" not in test_name:
                            continue
                        self._record_observation(
                            test_name, sha=sha, status=status,
                            run_id=run_id, run_url=run_url,
                            workflow_name=workflow_name, log_excerpt=msg,
                        )
                        if status not in ("failure", "error"):
                            continue
                        is_pytest = ("/" in test_name
                                     or test_name.endswith(".py")
                                     or "tests/" in test_name)
                        test_kind = "pytest" if is_pytest else "nextest"
                        key = issue_key_for(test_name)
                        if key in failures:
                            continue
                        failures[key] = FailedTest(
                            test_name=test_name, test_kind=test_kind,
                            group_number=group_number,
                            total_groups=10 if group_number else None,
                            run_id=run_id, run_url=run_url,
                            log_excerpt=msg[:4000],
                            workflow_name=workflow_name,
                            head_sha=sha,
                        )
            # If the run failed but every uploaded junit XML showed only
            # passes/skips, a job that ran no tests (or dropped its junit)
            # is hiding the real failure. Surface it so the user can fix
            # the upload — silent zero is the bug we're closing here.
            if conclusion == "failure" and len(failures) == failures_before:
                log(f"WARNING: run {run_id} ({run_url}) finished with "
                    f"conclusion=failure but parsed 0 failing testcases "
                    f"from {len(junit_artifacts)} junit artifact(s). "
                    f"A failing job likely didn't upload its junit XML "
                    f"(e.g. nextest's junit.xml was missing at upload "
                    f"time, or a workflow stage failed before tests ran).")
        self._save_history()
        log(f"TestDataActor.discover: {len(failures)} unique failures")
        return list(failures.values())

    @endpoint
    def refresh_history(self, target_workflow: str,
                        n_commits: int = 3) -> int:
        """Scan the N most-recent decided commits' runs of
        `target_workflow` regardless of conclusion and record every
        testcase observation. Detects pass-after-fail signals that
        :meth:`discover` would miss when the latest commit is all-green.
        Returns the number of newly-recorded observations.
        """
        runs = list_recent_completed_runs(
            self.repo, target_workflow, n_recent_commits=n_commits,
        )
        log(f"TestDataActor.refresh_history: scanning {len(runs)} runs across "
            f"{n_commits} recent decided commits")
        new_count = 0
        for r in runs:
            new_count += self._ingest_run(r, target_workflow)
        if new_count:
            self._save_history()
        return new_count

    @endpoint
    def status_summary(self, test_name: str) -> dict:
        """Return a structured snapshot of one test's history. Sorted
        by polling time so the most recent observation is last.
        """
        observations = sorted(
            list(self.history.get(test_name, [])),
            key=lambda o: o.get("polled_at") or "",
        )
        failed = [o for o in observations
                  if o.get("status") in ("failure", "error")]
        passed = [o for o in observations if o.get("status") == "passed"]
        latest = observations[-1] if observations else None
        return {
            "test_name": test_name,
            "failed_at": [
                {"sha": o["sha"], "run_url": o["run_url"],
                 "polled_at": o["polled_at"]}
                for o in failed
            ],
            "passed_at": [
                {"sha": o["sha"], "run_url": o["run_url"],
                 "polled_at": o["polled_at"]}
                for o in passed
            ],
            "latest_status": latest.get("status") if latest else "unknown",
            "latest_sha": latest.get("sha") if latest else None,
        }

    def __cleanup__(self, exc: Exception | None) -> None:
        try:
            self._save_history()
        except Exception as e:
            log(f"TestDataActor.__cleanup__: history save failed: {e!r}")


class _Result:
    # Mimics the future returned by ActorEndpoint.call_one — the dispatcher's
    # call sites use `.call_one(...).get()`, so we hand back a value already
    # resolved by `_TestDataHandle._call`.
    def __init__(self, value: Any) -> None:
        self._value = value

    def get(self) -> Any:
        return self._value


class _EndpointProxy:
    def __init__(self, handle: "TestDataHandle", name: str) -> None:
        self._handle = handle
        self._name = name

    def call_one(self, *args: Any, **kwargs: Any) -> _Result:
        return _Result(self._handle._call(self._name, *args, **kwargs))


class TestDataHandle:
    """Long-lived TestDataActor wrapper that respawns the underlying mesh
    when its proc dies — most notably after a laptop wake-from-sleep,
    which invalidates the AF_UNIX sockets the mesh runs on and orphans
    every actor on it.

    Drop-in for the bare ActorMesh in dispatcher-process call sites
    (`.discover.call_one(...).get()` etc.). To pass into another actor's
    constructor — which needs a serialisable Monarch handle, not this
    Python proxy — use `.actor` to get the live underlying mesh.
    """

    def __init__(self, host: Any, upstream: str,
                 cache_path: str, history_path: str) -> None:
        self._host = host
        self._upstream = upstream
        self._cache_path = cache_path
        self._history_path = history_path
        self._procs: Any = None
        self._actor: Any = None
        self._spawn()

    def _spawn(self) -> None:
        self._procs = self._host.spawn_procs(name="test-data")
        self._actor = self._procs.spawn(
            "test_data", TestDataActor,
            self._upstream, self._cache_path, self._history_path,
        )

    def _respawn(self) -> None:
        log("TestDataHandle: respawning TestDataActor "
            "(mesh failed; likely sleep/IPC reset)")
        try:
            self._procs.stop().get()
        except Exception as e:
            log(f"TestDataHandle: stop on dead procs raised {e!r} (expected)")
        self._spawn()
        log("TestDataHandle: respawn complete")

    def _call(self, endpoint_name: str, *args: Any, **kwargs: Any) -> Any:
        try:
            return getattr(self._actor, endpoint_name).call_one(
                *args, **kwargs).get()
        except SupervisionError as e:
            log(f"TestDataHandle: {endpoint_name} hit {e!r}; "
                "respawning and retrying once")
            self._respawn()
            return getattr(self._actor, endpoint_name).call_one(
                *args, **kwargs).get()

    @property
    def actor(self) -> Any:
        return self._actor

    @property
    def discover(self) -> _EndpointProxy:
        return _EndpointProxy(self, "discover")

    @property
    def refresh_history(self) -> _EndpointProxy:
        return _EndpointProxy(self, "refresh_history")

    @property
    def status_summary(self) -> _EndpointProxy:
        return _EndpointProxy(self, "status_summary")

    def stop(self) -> None:
        try:
            self._procs.stop().get()
        except Exception as e:
            log(f"TestDataHandle.stop: {e!r}")


def _install_lenient_fault_hook() -> None:
    # The default unhandled_fault_hook calls sys.exit(1), which kills the
    # whole dispatcher when a child mesh dies — including the parallel
    # supervision-event delivery path that fires alongside the
    # SupervisionError raised at the call site. Override it to log only;
    # TestDataHandle handles the actual respawn.
    def lenient_hook(failure: MeshFailure) -> None:
        log(f"unhandled mesh failure (suppressed; handled at call site): "
            f"{failure.report()}")
    monarch.actor.unhandled_fault_hook = lenient_hook


# Monarch builds an AF_UNIX socket path from the proc-mesh `name=`,
# embedded into `/tmp/hyperactor_<pid>_<rng>/<name>-<rank><uid>`. macOS's
# `sun_path` is 104 bytes (103 usable), so the label has to leave room
# for the rest of the path. Cap labels conservatively at 28 chars; longer
# labels are truncated with a 6-char content hash appended so different
# inputs still produce different socket paths. `enable_transport()` does
# NOT help here — proc-to-proc bootstrap hardcodes AF_UNIX in
# `hyperactor_mesh::bootstrap::bootstrap` regardless of the configured
# channel transport. Remove this workaround once the upstream fix lands.
_MAX_PROC_NAME_LEN = 28


def _short_proc_name(label: str) -> str:
    if len(label) <= _MAX_PROC_NAME_LEN:
        return label
    h = hashlib.sha1(label.encode("utf-8")).hexdigest()[:6]
    return label[: _MAX_PROC_NAME_LEN - 7] + "-" + h


# Marker lines that `invoke_cli_agent` and `invoke_local_agent` write
# above their respective agent-output sections in the per-invocation
# log file. Anything before the first one of these is prompt header
# we don't want to echo to the operator's terminal.
_AGENT_LOG_OUTPUT_MARKERS = ("# OUTPUT", "# CONVERSATION")


def _stream_agent_log_to_stdout(
    log_path: Path, prefix: str, stop: threading.Event,
) -> None:
    """Tail-follow the per-invocation agent log file and echo new
    output to stdout, so the operator sees progress while a fixer or
    evaluator runs (instead of staring at an idle terminal).

    Skips the prompt header at the top of the log; only echoes lines
    after the agent's `# OUTPUT` / `# CONVERSATION` marker. Exits when
    `stop` is set, after draining any remaining tail.
    """
    f = None
    try:
        # Wait for the actor proc to create the file. Cap the wait so a
        # mis-spawned actor doesn't keep this thread alive forever.
        deadline = time.monotonic() + 60.0
        while not log_path.exists():
            if stop.is_set() or time.monotonic() > deadline:
                return
            time.sleep(0.1)
        f = log_path.open()
        printing = False
        while True:
            line = f.readline()
            if line:
                if not printing:
                    if any(line.startswith(m) for m in _AGENT_LOG_OUTPUT_MARKERS):
                        printing = True
                    continue
                # Strip trailing newline; print() re-adds one. Output
                # is line-atomic with flush=True so it interleaves
                # cleanly with the dispatcher's own log() lines.
                print(f"{prefix} {line.rstrip()}", flush=True)
            elif stop.is_set():
                rest = f.read()
                if rest and printing:
                    for tail_line in rest.splitlines():
                        if tail_line:
                            print(f"{prefix} {tail_line}", flush=True)
                return
            else:
                time.sleep(0.1)
    finally:
        if f is not None:
            f.close()


def invoke_agent(
    agent: str,
    prompt: str,
    cwd: Path,
    *,
    timeout: int,
    log_path: Path,
    test_data_actor: Any,
    num_concurrent: int = 1,
    reserved_cores: int = DEFAULT_RESERVED_CORES,
    proc_name: str | None = None,
) -> tuple[int, str]:
    """Run an agent in a fresh Monarch proc on the local host.

    A `ProcMesh` of one is spawned via `this_host().spawn_procs()`, an
    `AgentActor` is spawned on it (with a handle to the shared
    TestDataActor so it can query test history), the actor's `invoke`
    endpoint runs the agent CLI (or local OpenAI-compatible endpoint)
    to completion, and we then stop the proc — which fires
    `__cleanup__` on the actor and SIGTERMs anything still running.
    If the dispatcher dies before `procs.stop()` runs, Monarch fires
    the same `__cleanup__` on client disconnect, so subprocesses don't
    leak.

    Returns (exit_code, captured_stdout) from the agent invocation, with
    streaming logs already written to `log_path` from inside the actor
    (the worktree and log dir are on the dispatcher's local filesystem,
    which the actor proc shares).
    """
    if agent not in ("claude", "codex", "local"):
        raise ValueError(f"unknown agent: {agent!r}")

    host = this_host()
    name = _short_proc_name(
        proc_name or f"agent-{int(time.time() * 1000)}",
    )
    procs = host.spawn_procs(name=name)
    # Echo the agent's streaming log output to stdout so the operator
    # has live visibility instead of watching an idle terminal for the
    # 30 minutes the fixer is allowed.
    stop_tail = threading.Event()
    tail_thread = threading.Thread(
        target=_stream_agent_log_to_stdout,
        args=(log_path, f"[{name}]", stop_tail),
        daemon=True,
    )
    tail_thread.start()
    try:
        actor = procs.spawn(
            "agent", AgentActor,
            agent, list(_extra_agent_args),
            _local_api_url, _local_model, _local_api_key,
            # AgentActor lives in another proc; pass the live ActorMesh,
            # not the dispatcher-local TestDataHandle proxy. If the
            # TestDataActor is respawned mid-fix this AgentActor's handle
            # goes stale, but agent invocations are short-lived and the
            # cycle gets a fresh handle on the next dispatch.
            test_data_actor.actor if isinstance(
                test_data_actor, TestDataHandle) else test_data_actor,
        )
        return actor.invoke.call_one(
            prompt, str(cwd), timeout, str(log_path),
            num_concurrent, reserved_cores,
        ).get()
    finally:
        # Stop the proc; this triggers AgentActor.__cleanup__ and waits
        # for it to finish. Errors here are logged but not propagated:
        # the agent's result is what the caller cares about, and a hung
        # cleanup shouldn't fail an otherwise-successful invocation.
        try:
            procs.stop().get()
        except Exception as e:
            log(f"warning: procs.stop() raised {e!r}")
        stop_tail.set()
        tail_thread.join(timeout=5)


def build_fixer_prompt(failure: FailedTest, mode: str, base_sha: str,
                       worktree: Path, upstream: str, origin: str,
                       pr_url: str = "", eval_feedback: str = "") -> str:
    template = FIXER_PROMPT_FILE.read_text()
    has_group = failure.group_number is not None
    return render_template(template, {
        "MODE": mode,
        "ISSUE_KEY": failure.issue_key,
        "TEST_NAME": failure.test_name,
        "TEST_KIND": failure.test_kind,
        "GROUP_NUMBER": (str(failure.group_number) if has_group
                        else "(no pytest split group; not applicable)"),
        "TOTAL_GROUPS": str(failure.total_groups) if failure.total_groups else "10",
        "GROUP_KNOWN": "yes" if has_group else "no",
        "PRIMARY_KIND_HEADER": ("This is a Rust/nextest test — read 'How to "
                                "reproduce — nextest (Rust)' below. The "
                                "pytest section does not apply."
                                if failure.test_kind == "nextest"
                                else "This is a Python/pytest test — read "
                                     "'How to reproduce — pytest' below."),
        "RUN_URL": failure.run_url,
        "ORIGIN_REPO": origin,
        "UPSTREAM_REPO": upstream,
        "WORKTREE_PATH": str(worktree),
        "BASE_SHA": base_sha,
        "PR_URL": pr_url or "(none)",
        "EVAL_FEEDBACK": eval_feedback or "(none)",
        "LOG_EXCERPT": failure.log_excerpt[-8000:],
        "CONTEXT_NOTES": read_context_notes(failure.issue_key) or "(none yet)",
        "CONTEXT_DIR": str(context_dir_for(failure.issue_key)),
    })


def build_evaluator_prompt(failure: FailedTest, mode: str, base_sha: str,
                           worktree: Path, upstream: str, pr_url: str = "") -> str:
    template = EVALUATOR_PROMPT_FILE.read_text()
    return render_template(template, {
        "MODE": mode,
        "ISSUE_KEY": failure.issue_key,
        "TEST_NAME": failure.test_name,
        "WORKTREE_PATH": str(worktree),
        "BASE_SHA": base_sha,
        "UPSTREAM_REPO": upstream,
        "PR_URL": pr_url or "(none)",
        "LOG_EXCERPT": failure.log_excerpt[-8000:],
    })


def parse_evaluator_output(stdout: str) -> dict[str, Any] | None:
    """Find a JSON object in the agent's stdout."""
    matches = list(re.finditer(r"\{[^{}]*?\"verdict\"[^{}]*?\}", stdout, re.DOTALL))
    if not matches:
        # Try a more permissive brace-matching pass
        depth = 0
        start = -1
        candidates: list[str] = []
        for i, ch in enumerate(stdout):
            if ch == "{":
                if depth == 0:
                    start = i
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0 and start >= 0:
                    candidates.append(stdout[start:i + 1])
        for c in reversed(candidates):
            if "\"verdict\"" in c:
                try:
                    return json.loads(c)
                except json.JSONDecodeError:
                    continue
        return None
    for m in reversed(matches):
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            continue
    return None


# ---------------------------------------------------------------------------
# PR submission
# ---------------------------------------------------------------------------


_cached_gh_user: str | None = None


def current_gh_user() -> str:
    """Best-effort GitHub login of whoever's running this script.

    Tries in order:
      1. `gh api user -q .login` — authoritative if `gh auth` is set up.
      2. `git config user.email` — if it's a noreply (`<id>+<login>@users.noreply.github.com`
         or `<login>@users.noreply.github.com`), extract the login.
      3. Empty string — caller skips `--assignee` in that case.
    """
    global _cached_gh_user
    if _cached_gh_user is not None:
        return _cached_gh_user
    try:
        res = run(["gh", "api", "user", "-q", ".login"], check=False)
        if res.returncode == 0 and res.stdout.strip():
            _cached_gh_user = res.stdout.strip()
            return _cached_gh_user
    except FileNotFoundError:
        pass
    try:
        email_res = run(["git", "config", "user.email"], check=False)
        email = (email_res.stdout or "").strip()
        m = re.match(r"(?:\d+\+)?([^@]+)@users\.noreply\.github\.com$", email)
        if m:
            _cached_gh_user = m.group(1)
            return _cached_gh_user
    except FileNotFoundError:
        pass
    _cached_gh_user = ""
    return ""


def push_branch(worktree: Path, branch: str, force: bool = False,
                origin_remote: str = "origin") -> None:
    cmd = ["git", "push", "-u", origin_remote, branch]
    if force:
        cmd.append("--force-with-lease")
    run(cmd, cwd=worktree)


def read_report(worktree: Path) -> tuple[dict[str, Any], str]:
    """Parse AUTO_FIX_REPORT.md frontmatter + body."""
    p = worktree / "AUTO_FIX_REPORT.md"
    if not p.exists():
        return {}, ""
    text = p.read_text()
    if text.startswith("---"):
        end = text.find("---", 3)
        if end > 0:
            front = text[3:end].strip()
            body = text[end + 3:].strip()
            meta: dict[str, Any] = {}
            for line in front.splitlines():
                if ":" in line and not line.startswith(" "):
                    k, _, v = line.partition(":")
                    meta[k.strip()] = v.strip()
            return meta, body
    return {}, text


def open_pr(worktree: Path, failure: FailedTest, upstream: str, origin: str,
            issue_key: str) -> str:
    branch = f"auto-fix/{issue_key}"
    meta, body = read_report(worktree)
    summary = meta.get("test_name", failure.test_name).rsplit("::", 1)[-1]
    title = f"[auto-fix:{issue_key}] fix flaky {summary}"
    marker = PR_BODY_MARKER_FMT.format(key=issue_key)
    notes = read_context_notes(issue_key)
    preamble = (
        f"{marker}\n\n"
        f"**Failing test:** `{failure.test_name}`\n"
        f"**Source CI run:** {failure.run_url}\n"
        f"**Issue key:** `{issue_key}`\n\n"
        f"This PR was produced by the auto-fix dispatcher on a local "
        f"MacBook. A separate evaluator subagent reviewed the diff before "
        f"submission and approved with the confidence threshold above the "
        f"configured minimum. Upstream CI on this PR is the authoritative "
        f"re-test; if it goes red, the dispatcher will amend with "
        f"force-with-lease.\n\n"
        f"---\n\n"
    )
    full_body = preamble + body
    overflow_comment: str | None = None
    if len(full_body) > PR_BODY_MAX_CHARS:
        keep = PR_BODY_MAX_CHARS - len(preamble) - 200
        truncated = body[:keep]
        full_body = preamble + truncated + "\n\n*(report truncated; full report posted as a comment below)*"
        overflow_comment = "## Full AUTO_FIX_REPORT\n\n" + body
    head = f"{origin.split('/')[0]}:{branch}"
    assignee = current_gh_user()
    pr_create_cmd = [
        "gh", "pr", "create",
        "--repo", upstream,
        "--base", "main",
        "--head", head,
        "--title", title,
        "--body", full_body,
        "--label", "autofix",
    ]
    if assignee:
        pr_create_cmd.extend(["--assignee", assignee])
    res = run(pr_create_cmd, cwd=worktree, check=False)
    if res.returncode != 0:
        log(f"  gh pr create failed: {res.stderr}")
        raise RuntimeError("gh pr create failed")
    url = (res.stdout or "").strip().splitlines()[-1]
    log(f"  opened PR: {url}")
    if overflow_comment:
        run(["gh", "pr", "comment", url, "--body", overflow_comment[:65000]],
            check=False)
    if notes:
        run(["gh", "pr", "comment", url,
             "--body", "## Dispatcher context notes\n\n" + notes[:60000]],
            check=False)
    return url


def get_pr_status(repo: str, pr_url: str) -> dict[str, Any]:
    return gh_json([
        "pr", "view", pr_url,
        "--repo", repo,
        "--json", "number,state,url,statusCheckRollup,headRefName,headRefOid,mergedAt",
    ]) or {}


def latest_failed_check_runs(repo: str, pr_url: str) -> list[str]:
    """Return a list of run IDs whose status check is FAILURE on the PR head.

    The agent itself fetches log/junit content from these runs; we don't
    pre-scrape logs here.
    """
    pr = get_pr_status(repo, pr_url)
    rollup = pr.get("statusCheckRollup", []) or []
    seen: list[str] = []
    for chk in rollup:
        if chk.get("conclusion") == "FAILURE" and chk.get("detailsUrl"):
            m = re.search(r"/runs/(\d+)", chk["detailsUrl"])
            if m and m.group(1) not in seen:
                seen.append(m.group(1))
    return seen


# ---------------------------------------------------------------------------
# Restart recovery
# ---------------------------------------------------------------------------


def recover_in_flight(state: dict[str, Any]) -> None:
    """On startup, reconcile any non-terminal in-progress entries against
    the disk so the previous cycle's interruption doesn't leave us stuck.
    """
    log("recovery scan: examining in-flight entries from previous run")
    changed = False
    for key, fix in list(state.get("fixes", {}).items()):
        status = fix.get("status")
        worktree = WORKTREE_DIR / key
        if status == STATUS_FIXING:
            if worktree.exists() and (worktree / "AUTO_FIX_REPORT.md").exists():
                log(f"  [{key}] fixer left a report; resume at evaluator stage")
                fix["status"] = STATUS_EVALUATING
                changed = True
                append_context_note(key, "recovery",
                                    "Process restarted while in STATUS_FIXING; report present, advancing to evaluator.")
            elif worktree.exists():
                log(f"  [{key}] fixer left a worktree but no report; will retry as new")
                fix["status"] = STATUS_NEW
                changed = True
                append_context_note(key, "recovery",
                                    "Process restarted while in STATUS_FIXING; no AUTO_FIX_REPORT.md found. Retrying.")
            else:
                log(f"  [{key}] no worktree; resetting to new")
                fix["status"] = STATUS_NEW
                changed = True
        elif status == STATUS_EVALUATING:
            if worktree.exists() and (worktree / "AUTO_FIX_REPORT.md").exists():
                log(f"  [{key}] evaluator was running; will re-run on next cycle (idempotent)")
            else:
                log(f"  [{key}] evaluating but worktree/report missing; resetting")
                fix["status"] = STATUS_NEW
                changed = True
        elif status == STATUS_PR_OPEN:
            # Will be reconciled by reconcile_open_prs in the first cycle.
            pass
    if changed:
        save_state(state)
    log("recovery scan complete")


def count_open_autofix_prs(state: dict[str, Any]) -> int:
    return sum(1 for f in state.get("fixes", {}).values()
               if f.get("status") == STATUS_PR_OPEN)


def count_in_flight_subagents(state: dict[str, Any]) -> int:
    """How many fixers/evaluators are concurrently spending CPU.

    Used to scale per-subagent build/test parallelism via cores_per_agent().
    Today this is always 1 (we dispatch sequentially per cycle), but the
    function exists so future parallel-dispatch can plug in cleanly.
    """
    return sum(1 for f in state.get("fixes", {}).values()
               if f.get("status") in (STATUS_FIXING, STATUS_EVALUATING))


# ---------------------------------------------------------------------------
# Single-failure pipeline
# ---------------------------------------------------------------------------


def handle_failure(
    failure: FailedTest,
    state: dict[str, Any],
    *,
    upstream: str,
    origin: str,
    upstream_remote: str,
    origin_remote: str,
    agent: str,
    test_data_actor: Any,
    dry_run: bool = False,
    min_confidence: int = DEFAULT_MIN_CONFIDENCE,
    max_open_prs: int = DEFAULT_MAX_OPEN_PRS,
) -> None:
    key = failure.issue_key
    fix = state["fixes"].get(key, {})
    status = fix.get("status", STATUS_NEW)

    if status in TERMINAL_STATUSES:
        log(f"[{key}] status={status}, skipping")
        return

    log(f"[{key}] processing failure: {failure.test_name}  status={status}")

    # Dedup: existing PR keyed by issue_key
    pr = existing_pr_for(key, [upstream, origin])
    if pr:
        if pr["state"] == "MERGED":
            log(f"[{key}] already merged in {pr['repo']} #{pr['number']} ({pr['url']})")
            upsert_fix(state, key,
                       status=STATUS_PR_MERGED,
                       pr_url=pr["url"], pr_number=pr["number"],
                       test_name=failure.test_name)
            return
        if pr["state"] == "CLOSED":
            log(f"[{key}] previously closed without merge in {pr['repo']} #{pr['number']}; not retrying automatically")
            upsert_fix(state, key,
                       status=STATUS_PR_CLOSED,
                       pr_url=pr["url"], pr_number=pr["number"],
                       test_name=failure.test_name)
            return
        if pr["state"] == "OPEN":
            log(f"[{key}] open PR exists at {pr['url']}; will track CI status separately")
            upsert_fix(state, key,
                       status=STATUS_PR_OPEN,
                       pr_url=pr["url"], pr_number=pr["number"],
                       test_name=failure.test_name)
            return

    # Looser dedup: anyone else has an open PR mentioning this test
    other_pr = existing_pr_by_test_name(failure.test_name, [upstream])
    if other_pr:
        log(f"[{key}] another open PR mentions this test: {other_pr['url']}; skipping")
        upsert_fix(state, key,
                   status=STATUS_DUP_OF_EXISTING_PR,
                   pr_url=other_pr["url"],
                   test_name=failure.test_name)
        return

    if fix.get("attempts", 0) >= MAX_ATTEMPTS:
        log(f"[{key}] reached MAX_ATTEMPTS={MAX_ATTEMPTS}; giving up")
        upsert_fix(state, key, status=STATUS_GAVE_UP, test_name=failure.test_name)
        return

    open_count = count_open_autofix_prs(state)
    if open_count >= max_open_prs:
        log(f"[{key}] {open_count} open auto-fix PRs already (limit {max_open_prs}); deferring")
        return

    upsert_fix(state, key,
               status=STATUS_FIXING,
               test_name=failure.test_name,
               run_url=failure.run_url)

    base_sha = failure.head_sha or "main"
    if base_sha == "main":
        # Resolve to a concrete sha so the worktree is stable.
        run(["git", "fetch", upstream_remote], cwd=REPO_ROOT, check=False)
        sha_res = run(
            ["git", "rev-parse", f"{upstream_remote}/main"],
            cwd=REPO_ROOT, check=False,
        )
        if sha_res.returncode == 0:
            base_sha = sha_res.stdout.strip()
    upsert_fix(state, key, base_sha=base_sha)

    worktree = ensure_worktree(key, base_sha, origin_remote=origin_remote)

    fixer_prompt = build_fixer_prompt(
        failure, mode="fresh", base_sha=base_sha,
        worktree=worktree, upstream=upstream, origin=origin,
    )
    fixer_log = LOG_DIR / f"{key}.fixer.{int(time.time())}.log"
    fixer_rc, _ = invoke_agent(
        agent, fixer_prompt, cwd=worktree,
        timeout=FIXER_TIMEOUT_SECONDS, log_path=fixer_log,
        test_data_actor=test_data_actor,
        num_concurrent=max(1, count_in_flight_subagents(state)),
        proc_name=f"fixer-{key.rsplit('-', 1)[-1]}",
    )
    # Bump attempts only after the fixer has actually run to completion
    # (whether successfully or not). Earlier failures — spawn errors,
    # missing CLI, infrastructure exceptions — re-raise from invoke_agent
    # without consuming the retry budget, so transient infra issues
    # don't burn through MAX_ATTEMPTS.
    upsert_fix(state, key, attempts=fix.get("attempts", 0) + 1)
    if fixer_rc != 0:
        log(f"[{key}] fixer exited {fixer_rc}; will retry next cycle")
        upsert_fix(state, key, status=STATUS_NEW, last_error=f"fixer rc={fixer_rc}")
        return

    meta, _ = read_report(worktree)
    if meta.get("verdict") == "give_up":
        log(f"[{key}] fixer gave up: {worktree}/AUTO_FIX_REPORT.md")
        upsert_fix(state, key, status=STATUS_GAVE_UP)
        return

    upsert_fix(state, key, status=STATUS_EVALUATING)

    eval_prompt = build_evaluator_prompt(
        failure, mode="pre_submit", base_sha=base_sha,
        worktree=worktree, upstream=upstream,
    )
    eval_log = LOG_DIR / f"{key}.evaluator.{int(time.time())}.log"
    eval_rc, eval_stdout = invoke_agent(
        agent, eval_prompt, cwd=worktree,
        timeout=EVALUATOR_TIMEOUT_SECONDS, log_path=eval_log,
        test_data_actor=test_data_actor,
        num_concurrent=max(1, count_in_flight_subagents(state)),
        proc_name=f"evaluator-{key.rsplit('-', 1)[-1]}",
    )
    verdict = parse_evaluator_output(eval_stdout) or {}
    log(f"[{key}] evaluator verdict: {verdict.get('verdict')!r} confidence={verdict.get('confidence')}")

    confidence = int(verdict.get("confidence") or 0)
    if verdict.get("verdict") == "submit" and confidence < min_confidence:
        log(f"[{key}] evaluator says submit but confidence={confidence} < {min_confidence}; treating as request_changes")
        verdict["verdict"] = "request_changes"
        verdict.setdefault("feedback_for_fixer",
                           f"Evaluator confidence ({confidence}) below required threshold "
                           f"({min_confidence}). Strengthen verification + diagnosis.")

    if verdict.get("verdict") == "submit":
        if dry_run:
            log(f"[{key}] --dry-run: fix verified locally; skipping push to "
                f"{origin_remote!r} and `gh pr create`. Inspect the worktree "
                f"at {worktree} (branch auto-fix/{key}) and AUTO_FIX_REPORT.md.")
            append_context_note(key, "dry-run submit",
                                f"Evaluator approved (confidence={confidence}); "
                                f"push and PR skipped under --dry-run.")
            upsert_fix(state, key, status=STATUS_PR_OPEN,
                       pr_url=f"(dry-run: not pushed) auto-fix/{key}")
            return
        push_branch(worktree, f"auto-fix/{key}", force=False,
                    origin_remote=origin_remote)
        append_context_note(key, "branch pushed",
                            f"Branch auto-fix/{key} pushed to {origin}; evaluator confidence={confidence}.")
        try:
            pr_url = open_pr(worktree, failure, upstream, origin, key)
        except Exception as e:
            log(f"[{key}] open_pr failed: {e}")
            upsert_fix(state, key, status=STATUS_NEW, last_error=str(e))
            return
        upsert_fix(state, key, status=STATUS_PR_OPEN, pr_url=pr_url)
        append_context_note(key, "PR opened",
                            f"PR opened at {pr_url}. Branch will be amended if CI rejects.")
    elif verdict.get("verdict") == "request_changes":
        feedback = verdict.get("feedback_for_fixer", "")
        log(f"[{key}] evaluator requested changes; feedback saved for next cycle")
        append_context_note(key, "evaluator requested changes", feedback or "(empty feedback)")
        upsert_fix(state, key, status=STATUS_NEW, last_eval_feedback=feedback)
    else:
        log(f"[{key}] evaluator rejected; giving up")
        append_context_note(key, "evaluator rejected",
                            json.dumps(verdict.get("reasons") or [], indent=2))
        upsert_fix(state, key, status=STATUS_GAVE_UP, last_eval_feedback=verdict.get("reasons"))


# ---------------------------------------------------------------------------
# CI-signal pipeline (re-evaluate open PRs)
# ---------------------------------------------------------------------------


def reconcile_open_prs(
    state: dict[str, Any],
    *,
    upstream: str,
    origin: str,
    upstream_remote: str,
    origin_remote: str,
    agent: str,
    test_data_actor: Any,
    dry_run: bool = False,
) -> None:
    open_keys = [k for k, f in state["fixes"].items() if f.get("status") == STATUS_PR_OPEN]
    for key in open_keys:
        fix = state["fixes"][key]
        pr_url = fix.get("pr_url")
        if not pr_url:
            continue
        pr = get_pr_status(upstream, pr_url)
        if not pr:
            continue
        if pr.get("state") == "MERGED":
            log(f"[{key}] PR merged: {pr_url}")
            upsert_fix(state, key, status=STATUS_PR_MERGED)
            remove_worktree(key)
            continue
        if pr.get("state") == "CLOSED":
            log(f"[{key}] PR closed: {pr_url}")
            upsert_fix(state, key, status=STATUS_PR_CLOSED)
            remove_worktree(key)
            continue

        rollup = pr.get("statusCheckRollup") or []
        any_failed = any(c.get("conclusion") == "FAILURE" for c in rollup)
        all_success = rollup and all(
            c.get("conclusion") in ("SUCCESS", "NEUTRAL", "SKIPPED") for c in rollup
        )
        if all_success:
            log(f"[{key}] PR CI green; awaiting merge")
            continue
        if not any_failed:
            log(f"[{key}] PR CI still pending")
            continue
        if fix.get("pr_head_sha") == pr.get("headRefOid"):
            log(f"[{key}] CI red but head unchanged since last reconcile; waiting")
            continue
        log(f"[{key}] PR CI is RED; running evaluator in ci_signal mode"
            + (" (--dry-run: amend will not be force-pushed)" if dry_run else ""))
        failed_run_ids = latest_failed_check_runs(upstream, pr_url)
        worktree = WORKTREE_DIR / key
        if not worktree.exists():
            log(f"[{key}] worktree gone; recreating")
            sha = pr.get("headRefOid", "main")
            ensure_worktree(key, sha, origin_remote=origin_remote)

        # The evaluator and fixer fetch logs/junit themselves via gh.
        # We just hand them the run IDs and the test name.
        run_id_hint = ", ".join(failed_run_ids[:5]) if failed_run_ids else "(none)"
        synth = FailedTest(
            test_name=fix.get("test_name", "(unknown)"),
            test_kind="pytest",
            group_number=None, total_groups=None,
            run_id=failed_run_ids[0] if failed_run_ids else "",
            run_url=pr_url,
            log_excerpt=(f"Upstream CI on this PR went red. Failing run IDs: "
                         f"{run_id_hint}. Use\n"
                         f"  gh run view <id> --repo {upstream} --log-failed\n"
                         f"to fetch logs as needed."),
            workflow_name="", head_sha=pr.get("headRefOid", ""),
        )
        eval_prompt = build_evaluator_prompt(
            synth, mode="ci_signal",
            base_sha=pr.get("headRefOid", "main"),
            worktree=worktree, upstream=upstream, pr_url=pr_url,
        )
        eval_log = LOG_DIR / f"{key}.evaluator.ci.{int(time.time())}.log"
        _, eval_stdout = invoke_agent(
            agent, eval_prompt, cwd=worktree,
            timeout=EVALUATOR_TIMEOUT_SECONDS, log_path=eval_log,
            test_data_actor=test_data_actor,
            num_concurrent=max(1, count_in_flight_subagents(state)),
            proc_name=f"evaluator-ci-{key.rsplit('-', 1)[-1]}",
        )
        verdict = parse_evaluator_output(eval_stdout) or {}
        log(f"[{key}] ci_signal verdict: {verdict.get('verdict')!r}")

        if verdict.get("verdict") == "request_changes":
            if fix.get("attempts", 0) >= MAX_ATTEMPTS:
                log(f"[{key}] amend attempts exhausted; giving up")
                upsert_fix(state, key, status=STATUS_GAVE_UP)
                continue
            feedback = verdict.get("feedback_for_fixer", "")
            fixer_prompt = build_fixer_prompt(
                synth, mode="amend",
                base_sha=pr.get("headRefOid", "main"),
                worktree=worktree, upstream=upstream, origin=origin,
                pr_url=pr_url, eval_feedback=feedback,
            )
            fixer_log = LOG_DIR / f"{key}.fixer.amend.{int(time.time())}.log"
            fixer_rc, _ = invoke_agent(
                agent, fixer_prompt, cwd=worktree,
                timeout=FIXER_TIMEOUT_SECONDS, log_path=fixer_log,
                test_data_actor=test_data_actor,
                proc_name=f"fixer-amend-{key.rsplit('-', 1)[-1]}",
            )
            if fixer_rc != 0:
                log(f"[{key}] amend fixer exited {fixer_rc}; will retry")
                upsert_fix(state, key, status=STATUS_PR_FAILED_CI,
                           pr_head_sha=pr.get("headRefOid"),
                           attempts=fix.get("attempts", 0) + 1)
                continue
            if dry_run:
                log(f"[{key}] --dry-run: amend prepared on auto-fix/{key} in "
                    f"{worktree}; skipping force-push to {origin_remote!r}.")
                upsert_fix(state, key,
                           pr_head_sha=pr.get("headRefOid"),
                           attempts=fix.get("attempts", 0) + 1)
            else:
                push_branch(worktree, f"auto-fix/{key}", force=True,
                            origin_remote=origin_remote)
                upsert_fix(state, key, status=STATUS_PR_OPEN,
                           pr_head_sha=pr.get("headRefOid"),
                           attempts=fix.get("attempts", 0) + 1)
        elif verdict.get("verdict") == "submit":
            log(f"[{key}] evaluator considers the PR fine; CI failure unrelated. Marking head_sha to suppress reruns.")
            upsert_fix(state, key, pr_head_sha=pr.get("headRefOid"))
        else:
            log(f"[{key}] evaluator rejected; giving up")
            upsert_fix(state, key, status=STATUS_GAVE_UP)


# ---------------------------------------------------------------------------
# Locking + main loop
# ---------------------------------------------------------------------------


def acquire_lock() -> None:
    _ensure_dirs()
    if LOCK_FILE.exists():
        try:
            pid = int(LOCK_FILE.read_text().strip())
            os.kill(pid, 0)
            log(f"another dispatcher is running (pid {pid}). Exiting.")
            sys.exit(2)
        except (ProcessLookupError, ValueError):
            log("stale lock found; clearing")
    LOCK_FILE.write_text(str(os.getpid()))


def release_lock() -> None:
    try:
        LOCK_FILE.unlink()
    except FileNotFoundError:
        pass


def parse_manual_failures(specs: list[str]) -> list[FailedTest]:
    """Turn `--manual-failure TEST` (optionally `TEST@GROUP`) into FailedTest
    objects so the loop can be exercised without real GitHub artifacts."""
    out: list[FailedTest] = []
    for spec in specs or []:
        if "@" in spec:
            test, _, grp = spec.rpartition("@")
            try:
                group = int(grp)
            except ValueError:
                test = spec
                group = None
        else:
            test, group = spec, None
        kind = "nextest" if "::" in test and "/" not in test else "pytest"
        out.append(FailedTest(
            test_name=test, test_kind=kind,
            group_number=group, total_groups=10 if group else None,
            run_id="manual", run_url="(manual)",
            log_excerpt=f"(manual injection — no log; run repro yourself)",
            workflow_name="manual", head_sha="HEAD",
        ))
    return out


def write_context_update_if_new(
    key: str, fix: dict[str, Any], summary: dict[str, Any],
    base_sha: str,
) -> bool:
    """If the test-data actor has observations for this in-flight test
    that haven't been delivered yet, write them into the worktree as
    AUTO_FIX_CONTEXT_UPDATES.md and record what we delivered.

    Returns True if a new update was written.
    """
    delivered = set(fix.get("delivered_observations") or [])
    new_failed = [o for o in summary.get("failed_at", [])
                  if f"{o['sha']}:{o['run_url']}" not in delivered]
    new_passed = [o for o in summary.get("passed_at", [])
                  if f"{o['sha']}:{o['run_url']}" not in delivered]
    # Only deliver observations on commits other than the one the agent
    # was originally dispatched against — those are by definition new
    # information to the agent.
    new_failed = [o for o in new_failed if o["sha"] != base_sha]
    new_passed = [o for o in new_passed if o["sha"] != base_sha]
    if not (new_failed or new_passed):
        return False
    worktree = WORKTREE_DIR / key
    if not worktree.exists():
        return False

    test_name = summary.get("test_name", fix.get("test_name", "(unknown)"))
    lines: list[str] = [
        "# Auto-fix dispatcher: new test status observations",
        "",
        f"Updated: {utc_now_iso()}",
        "",
        "The dispatcher polled CI again while you were working and observed",
        f"more recent results for `{test_name}`. Treat this as additional",
        "context. **Do NOT abandon your investigation** — instead, weigh the",
        "new data points against your hypothesis.",
        "",
        f"## Test: `{test_name}`",
        "",
        f"Originally dispatched against base SHA: `{base_sha[:12] or '(unknown)'}`",
        "",
    ]
    if new_failed:
        lines.append("### Newly observed FAILURES")
        for o in new_failed:
            lines.append(
                f"- `{o['sha'][:12]}` ({o['run_url']}) — observed {o['polled_at']}"
            )
        lines.append("")
    if new_passed:
        lines.append("### Newly observed PASSES")
        for o in new_passed:
            lines.append(
                f"- `{o['sha'][:12]}` ({o['run_url']}) — observed {o['polled_at']}"
            )
        lines.append("")
    lines.extend([
        "### Interpretation hints",
        "- Both passes AND failures recorded → likely a **flaky** test.",
        "  Your fix should target the source of the flake (timing, "
        "isolation, ordering), and the AUTO_FIX_REPORT.md should call "
        "out the flaky-vs-real distinction so the reviewer can judge.",
        "- Only passes after your base, no further failures → the test "
        "may have been **fixed upstream** by another commit. Compare "
        "the upstream commits between your base SHA and the latest "
        "passing SHA; if your fix duplicates one that's already landed, "
        "say so in AUTO_FIX_REPORT.md and set `verdict: give_up` with "
        "a reason of `already_fixed_upstream`.",
        "- Only further failures → your fix is justified; keep going.",
        "",
        "Continue your investigation; this file may be appended to in",
        "future cycles as more data arrives.",
        "",
    ])
    update_path = worktree / "AUTO_FIX_CONTEXT_UPDATES.md"
    # Append rather than overwrite, so accumulated context survives
    # multiple polling cycles.
    mode = "a" if update_path.exists() else "w"
    with update_path.open(mode) as f:
        if mode == "a":
            f.write("\n\n---\n\n")
        f.write("\n".join(lines))
    delivered.update(f"{o['sha']}:{o['run_url']}" for o in new_failed + new_passed)
    fix["delivered_observations"] = sorted(delivered)
    log(f"[{key}] wrote {len(new_failed)} new fail + {len(new_passed)} new pass observation(s) "
        f"to {update_path.name}")
    return True


def push_status_updates_to_in_flight(
    state: dict[str, Any], test_data_actor: Any,
) -> None:
    """For each test currently being fixed/evaluated, query the
    TestDataActor for the latest history snapshot and append any new
    pass/fail observations into the worktree's AUTO_FIX_CONTEXT_UPDATES.md.
    """
    in_flight_keys = [
        k for k, f in state["fixes"].items()
        if f.get("status") in (STATUS_FIXING, STATUS_EVALUATING, STATUS_NEW)
        and f.get("test_name")
    ]
    if not in_flight_keys:
        return
    any_written = False
    for key in in_flight_keys:
        fix = state["fixes"][key]
        test_name = fix["test_name"]
        try:
            summary = test_data_actor.status_summary.call_one(test_name).get()
        except Exception as e:
            log(f"[{key}] status_summary failed: {e!r}")
            continue
        base_sha = fix.get("base_sha") or fix.get("run_url", "")
        if write_context_update_if_new(key, fix, summary,
                                       base_sha=fix.get("base_sha", "")):
            any_written = True
    if any_written:
        save_state(state)


def cycle(args: argparse.Namespace, state: dict[str, Any],
          test_data_actor: Any) -> None:
    log("--- cycle start ---")
    # Sweep stale worktrees, branches, logs, and state entries from
    # closed/merged/abandoned fixes so they don't pile up forever.
    try:
        prune_stale_artifacts(state, args.prune_retention_days,
                              dry_run=args.dry_run)
    except Exception:
        log("prune: failed; continuing")
        traceback.print_exc()
    # Reconcile any open auto-fix PRs first (cheaper, may clear keys).
    reconcile_open_prs(state, upstream=args.upstream, origin=args.origin,
                       upstream_remote=args.upstream_remote,
                       origin_remote=args.origin_remote,
                       agent=args.agent, dry_run=args.dry_run,
                       test_data_actor=test_data_actor)

    # Refresh history for in-flight tests so we can detect a previously-
    # failing test that's now passing on a newer all-green commit (which
    # the failure-only `discover` would skip).
    has_in_flight = any(
        f.get("status") in (STATUS_FIXING, STATUS_EVALUATING, STATUS_NEW)
        for f in state["fixes"].values()
    )
    if has_in_flight and not args.manual_failure and not args.from_run:
        try:
            test_data_actor.refresh_history.call_one(args.workflow, 3).get()
        except Exception as e:
            log(f"refresh_history failed: {e!r}")

    # Discover new failures.
    if args.manual_failure:
        failures = parse_manual_failures(args.manual_failure)
        log(f"manual failures injected: {[f.test_name for f in failures]}")
        # `--manual-failure` is explicit human intent: reset any prior
        # terminal verdict for these keys so the pipeline re-runs from
        # scratch instead of skipping. Auto-discovered failures still
        # hit the skip in handle_failure to avoid burning agent compute
        # on the same give-ups every cycle.
        cleared = 0
        for f in failures:
            prior = state["fixes"].get(f.issue_key, {}).get("status")
            if prior in TERMINAL_STATUSES:
                log(f"[{f.issue_key}] manual override: clearing prior "
                    f"{prior!r} attempt and starting fresh")
                _reap_artifacts(f.issue_key)
                state["fixes"].pop(f.issue_key, None)
                cleared += 1
        if cleared:
            save_state(state)
    else:
        failures = test_data_actor.discover.call_one(
            args.workflow, args.from_run,
        ).get()
    state["last_polled_at"] = utc_now_iso()
    save_state(state)

    # Push any newly-observed pass/fail data points into the worktrees
    # of in-flight fixers as AUTO_FIX_CONTEXT_UPDATES.md.
    push_status_updates_to_in_flight(state, test_data_actor)

    dispatched = 0
    for failure in failures:
        if dispatched >= args.max_per_cycle and args.max_per_cycle > 0:
            log(f"hit --max-per-cycle={args.max_per_cycle}; remaining failures deferred")
            break
        before_status = state["fixes"].get(failure.issue_key, {}).get("status")
        try:
            handle_failure(failure, state,
                           upstream=args.upstream, origin=args.origin,
                           upstream_remote=args.upstream_remote,
                           origin_remote=args.origin_remote,
                           agent=args.agent, dry_run=args.dry_run,
                           min_confidence=args.min_confidence,
                           max_open_prs=args.max_open_prs,
                           test_data_actor=test_data_actor)
        except KeyboardInterrupt:
            raise
        except Exception:
            log(f"[{failure.issue_key}] unhandled exception:")
            traceback.print_exc()
            upsert_fix(state, failure.issue_key,
                       status=STATUS_NEW,
                       last_error=traceback.format_exc()[:2000])
        after_status = state["fixes"].get(failure.issue_key, {}).get("status")
        # Only count a "real" dispatch (status moved through fixing/evaluating).
        if before_status != after_status and after_status not in {STATUS_DUP_OF_EXISTING_PR, STATUS_PR_MERGED, STATUS_PR_CLOSED}:
            dispatched += 1
    log(f"--- cycle end (dispatched={dispatched}) ---")


def main() -> int:
    global DEFAULT_RESERVED_CORES
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--upstream", default=DEFAULT_UPSTREAM_REMOTE, metavar="REMOTE",
        help=(f"Git remote name for the upstream repo whose CI we mine "
              f"for failures and to which PRs are opened. Resolved to "
              f"owner/repo via `git remote get-url`. (default: "
              f"{DEFAULT_UPSTREAM_REMOTE!r})"))
    parser.add_argument(
        "--origin", default=DEFAULT_ORIGIN_REMOTE, metavar="REMOTE",
        help=(f"Git remote name for your fork — branches "
              f"`auto-fix/<key>` are pushed here. Resolved to owner/repo "
              f"via `git remote get-url`. (default: "
              f"{DEFAULT_ORIGIN_REMOTE!r})"))
    parser.add_argument(
        "--workflow", default=default_target_workflow(), metavar="NAME",
        help=(f"Exact workflow name to monitor (per gh `workflowName`). "
              f"Strict equality — only this workflow is acted on. Other "
              f"workflows (docs, CodeQL, dependency-graph) are ignored. "
              f"OS-aware default: 'CI (macOS)' on Darwin, 'CI' on Linux, "
              f"because we can only reproduce same-OS failures locally. "
              f"This machine's default: {default_target_workflow()!r}"))
    parser.add_argument(
        "--agent", default=DEFAULT_AGENT, choices=AGENT_CHOICES,
        help=(f"Which backend drives the fixer + evaluator subagents. "
              f"`claude` and `codex` are hosted CLI agents; `local` talks "
              f"to any OpenAI-compatible chat-completions endpoint you "
              f"point --api-url at. See LOCAL BACKENDS section below for "
              f"recipes. (default: {DEFAULT_AGENT})"))
    parser.add_argument(
        "--api-url", default=DEFAULT_LOCAL_API_URL, metavar="URL",
        help=(f"Base URL of the OpenAI-compatible chat completions endpoint "
              f"used when --agent local. Path '/chat/completions' is "
              f"appended. (default: {DEFAULT_LOCAL_API_URL})"))
    parser.add_argument(
        "--model", default=DEFAULT_LOCAL_MODEL, metavar="NAME",
        help=(f"Model name to send in the chat completions request when "
              f"--agent local. Must already be loaded by the server. "
              f"(default: {DEFAULT_LOCAL_MODEL})"))
    parser.add_argument(
        "--api-key", default=os.environ.get("AUTO_FIX_API_KEY", "ollama"),
        metavar="KEY",
        help=("Bearer token sent with --agent local requests. Most local "
              "servers ignore it; some require a non-empty value. Hosted "
              "OpenAI-compatible providers want a real key. Falls back to "
              "$AUTO_FIX_API_KEY env var if set."))
    parser.add_argument(
        "--agent-extra-args", default="", metavar="STR",
        help=("Whitespace-separated extra flags to pass through to the "
              "claude/codex CLI invocation. Useful for corporate variants "
              "that require additional flags."))
    parser.add_argument(
        "--state-dir", type=Path, default=DEFAULT_STATE_DIR, metavar="PATH",
        help=(f"Where state, worktrees, agent logs and per-issue context "
              f"notes live. Survives Ctrl-C; restart resumes from here. "
              f"(default: {DEFAULT_STATE_DIR})"))
    parser.add_argument(
        "--poll-seconds", type=int, default=DEFAULT_POLL_SECONDS, metavar="N",
        help=(f"Sleep between cycles in continuous mode. Each cycle "
              f"reconciles open PRs and (if no fixer ran) discovers new "
              f"failures. (default: {DEFAULT_POLL_SECONDS}s)"))
    parser.add_argument(
        "--max-per-cycle", type=int, default=DEFAULT_MAX_PER_CYCLE, metavar="N",
        help=(f"Maximum new fixer dispatches per cycle. Default 1 keeps "
              f"CPU contention predictable; 0 means unlimited. (default: "
              f"{DEFAULT_MAX_PER_CYCLE})"))
    parser.add_argument(
        "--max-open-prs", type=int, default=DEFAULT_MAX_OPEN_PRS, metavar="N",
        help=(f"Throttle: stop opening new auto-fix PRs once this many are "
              f"already open upstream — keeps reviewer load sane. The loop "
              f"keeps reconciling existing ones. (default: "
              f"{DEFAULT_MAX_OPEN_PRS})"))
    parser.add_argument(
        "--prune-retention-days", type=int,
        default=DEFAULT_PRUNE_RETENTION_DAYS, metavar="N",
        help=(f"At the start of each cycle, reap worktrees, local "
              f"branches, contexts, agent logs, and state entries for "
              f"fixes that have been in a terminal status (merged / "
              f"closed / gave_up / dup) longer than this many days. Also "
              f"drops orphan worktree dirs and `auto-fix/*` branches that "
              f"no longer have a matching state entry — handy after the "
              f"state file is wiped or the source repo is moved. Set to "
              f"0 to reap terminal entries on the next cycle. Remote "
              f"branches on --origin are never touched. (default: "
              f"{DEFAULT_PRUNE_RETENTION_DAYS})"))
    parser.add_argument(
        "--min-confidence", type=int, default=DEFAULT_MIN_CONFIDENCE, metavar="0-100",
        help=(f"Evaluator confidence threshold for `submit`. Lower-confidence "
              f"submissions are demoted to request_changes and the fixer "
              f"runs again next cycle. (default: {DEFAULT_MIN_CONFIDENCE})"))
    parser.add_argument(
        "--reserved-cores", type=int, default=DEFAULT_RESERVED_CORES, metavar="N",
        help=(f"Cores held back from each subagent's build/test parallelism, "
              f"so the human's browser stays responsive. Set to 0 if "
              f"running unattended overnight. Memory is intentionally NOT "
              f"capped. (default: {DEFAULT_RESERVED_CORES}; this machine "
              f"reports {os.cpu_count()} cores)"))
    parser.add_argument(
        "--once", action="store_true",
        help=("Run a single cycle and exit. Combine with --dry-run for "
              "a no-side-effects local fix attempt."))
    parser.add_argument(
        "--dry-run", action="store_true",
        help=("Run the full local pipeline (fixer + evaluator, including "
              "real agent calls) but DO NOT push the fix branch to "
              "--origin and DO NOT open or amend a PR upstream. The "
              "worktree at ~/.monarch-auto-fix/worktrees/<key>/ is left "
              "with the local commit and AUTO_FIX_REPORT.md for "
              "inspection. Useful for re-verifying a test that may "
              "already be passing, or for a confidence run before "
              "going live."))
    parser.add_argument(
        "--from-run", default=None, metavar="RUN_ID",
        help=("Bypass the latest-commit/workflow filter and process a "
              "specific GitHub Actions run by ID (the trailing number in "
              "the run URL). Useful for testing XML parsing against a "
              "known artifact set. Honours --dry-run."))
    parser.add_argument(
        "--manual-failure", action="append", default=[], metavar="TEST[@GROUP]",
        help=("Skip GitHub discovery for one cycle and inject a failure by "
              "name. Repeatable. TEST is a pytest nodeId (e.g. "
              "'python/tests/test_x.py::test_y') or a nextest path (e.g. "
              "'crate::module::test'). Optional `@GROUP` is the pytest-"
              "split group number. Useful for testing the dispatcher "
              "against a known local repro without an upstream junit "
              "artifact."))
    args = parser.parse_args()

    configure_paths(args.state_dir.expanduser().resolve())
    DEFAULT_RESERVED_CORES = args.reserved_cores
    global _extra_agent_args, _local_api_url, _local_model, _local_api_key
    _extra_agent_args = args.agent_extra_args.split() if args.agent_extra_args else []
    _local_api_url = args.api_url
    _local_model = args.model
    _local_api_key = args.api_key

    # Resolve git remote names ("upstream", "origin") to owner/repo so
    # the rest of the dispatcher (and gh) can use them directly. We
    # rebind args.upstream / args.origin in-place to the resolved
    # owner/repo (used by `gh --repo`), and stash the original remote
    # names on args.upstream_remote / args.origin_remote (used by
    # `git fetch <remote>` and `git push -u <remote>`).
    args.upstream_remote = args.upstream
    args.origin_remote = args.origin
    args.upstream = resolve_remote_repo(args.upstream_remote)
    args.origin = resolve_remote_repo(args.origin_remote)
    log(f"state dir: {STATE_HOME}")
    log(f"upstream: remote={args.upstream_remote!r} -> {args.upstream}")
    log(f"origin:   remote={args.origin_remote!r} -> {args.origin}")
    log(f"agent: {args.agent}"
        + (f"  api_url={args.api_url}  model={args.model}"
           if args.agent == "local" else ""))
    log(f"reserved cores: {args.reserved_cores}; total cores: {os.cpu_count()}; "
        f"per-agent build jobs at M=1: {cores_per_agent(1, args.reserved_cores)}")

    # Local LLM endpoints serve one request at a time per loaded model;
    # spinning up multiple concurrent subagents just queues them on the
    # same model and doubles wall-clock with no throughput gain. Force
    # sequential dispatch in ollama mode, but leave per-subagent build
    # parallelism (cores) untouched.
    if args.agent == "local" and args.max_per_cycle != 1:
        log(f"--agent local: clamping --max-per-cycle "
            f"{args.max_per_cycle} -> 1 (one shared local model, "
            f"parallel agents would queue on the same backend)")
        args.max_per_cycle = 1

    acquire_lock()
    try:
        # Catch Ctrl-C / SIGTERM and tear down any running subagent process
        # group(s). Each subagent is spawned with start_new_session=True so
        # killpg() cascades to grandchildren (cargo, conda, nextest, etc.).
        signal.signal(signal.SIGINT, _shutdown_handler)
        signal.signal(signal.SIGTERM, _shutdown_handler)

        # Spawn the shared TestDataActor on its own proc. Lives for the
        # whole dispatcher run; both the dispatcher and every AgentActor
        # talk to it for discovery + history queries. Cleanup on Ctrl-C
        # happens via __cleanup__ when the proc disconnects.
        # The TestDataHandle wrapper transparently respawns the actor if
        # its mesh dies (e.g. AF_UNIX sockets reset by wake-from-sleep).
        _install_lenient_fault_hook()
        host = this_host()
        test_data_actor = TestDataHandle(
            host, args.upstream,
            str(ARTIFACT_CACHE),
            str(STATE_HOME / "test_history.json"),
        )

        try:
            first = True
            while True:
                state = load_state()
                if first:
                    recover_in_flight(state)
                    first = False
                try:
                    cycle(args, state, test_data_actor)
                finally:
                    save_state(state)
                if args.once:
                    return 0
                log(f"sleeping {args.poll_seconds}s")
                time.sleep(args.poll_seconds)
        finally:
            test_data_actor.stop()
    finally:
        release_lock()


if __name__ == "__main__":
    sys.exit(main())
