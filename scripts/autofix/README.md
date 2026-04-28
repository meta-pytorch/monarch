# Auto-fix loop

A Ralph Wiggum-style local loop that watches `meta-pytorch/monarch` CI for
test failures and dispatches Claude (or Codex) subagents to produce
minimal fixes — guarded by a separate evaluator subagent, deduped against
existing PRs, and resumable across Ctrl-C.

```
+--------------+       +-----------+       +-----------+       +----------+
|  poll gh     |  -->  |  fixer    |  -->  | evaluator |  -->  |  gh pr   |
|  for CI fail |       |  subagent |       |  subagent |       |  create  |
+--------------+       +-----------+       +-----------+       +----------+
        ^                                                            |
        |                                                            v
        +-----<---------- amend if upstream CI goes red <-------------+
```

## Architecture: Monarch actors

Two kinds of actor live in the dispatcher's mesh, both spawned on
`this_host()`:

**`TestDataActor`** — one instance, started at dispatcher startup.
Owns all GitHub-side polling: `gh run list`, JUnit artifact download,
XML parse. Maintains a per-test history of pass / fail / skipped
observations across SHAs, persisted to
`<state_dir>/test_history.json`. Endpoints:

- `discover(target_workflow, from_run)` — find new failures on the
  most-recent decided commit (the dispatcher's discovery path), and
  side-record every testcase observation from the same XMLs.
- `refresh_history(target_workflow, n_commits=3)` — scan the N
  most-recent decided commits regardless of conclusion. Catches
  pass-after-fail signals on all-green commits that `discover` skips.
- `status_summary(test_name)` — return `{failed_at: [...], passed_at:
  [...], latest_status, latest_sha}` for a single test.

The dispatcher uses it for discovery; every `AgentActor` receives a
handle to it at construction time so agents (or future code in the
agent's actor proc) can query history without re-fetching from GitHub.

**`AgentActor`** — one instance per fixer or evaluator invocation,
spawned on a fresh proc. One proc per invocation isolates the agent
CLI's process tree — cargo, nextest, conda, the local LLM's bash
executor — so it can be torn down cleanly:

- After each invocation, the dispatcher calls `procs.stop()`. Monarch
  fires `__cleanup__` on the `AgentActor`, which SIGTERMs every
  subprocess group the agent spawned (and SIGKILLs anything still
  alive after a 3s grace period).
- If the dispatcher dies unexpectedly (Ctrl-C, crash), Monarch sees
  the client disconnect and fires the same `__cleanup__` on every
  lingering AgentActor — so subprocesses don't leak even when we
  never reach the explicit stop call.

There is no manual subprocess bookkeeping in the dispatcher: the
AgentActor owns its own children and Monarch owns the actor lifecycle.
The dispatcher itself is plain Python; it talks to the actors through
their endpoints.

The dispatcher MUST be started under `uv run` so it and the actor
procs it spawns share the in-tree Monarch build:

```bash
uv run python scripts/auto_fix/dispatch.py
```

`python3 scripts/auto_fix/dispatch.py` will fail at import — the script
imports `monarch.actor` at module load.

### Telling an in-flight subagent the test status changed

If the dispatcher polls again while a fixer is mid-investigation and
sees new pass/fail data on a newer commit (test passed → maybe fixed
upstream, maybe flaky; test failed again → confirmed real), it appends
that observation to `<worktree>/AUTO_FIX_CONTEXT_UPDATES.md`. The
fixer's prompt instructs it to re-read this file at the start of each
iteration and weigh the new data points against its hypothesis,
**without** abandoning the investigation. The file accumulates across
polling cycles, with timestamps and run URLs for each pass/fail
observation, so the agent can trace the history.

This is a one-way dispatcher → worktree-file channel rather than an
actor message because the agent CLI (Claude/Codex) is a black-box
subprocess that doesn't speak Monarch directly. The `AgentActor`
*does* hold a TestDataActor handle for future expansion (e.g., the
evaluator could query history before approving a PR).

## Pieces

| Path | Role |
|---|---|
| `dispatch.py` | The main loop. Polls, dedups, dispatches via Monarch actors, persists state. |
| `dispatch.py:TestDataActor` | Owns CI polling + per-test pass/fail history. Persists to `<state_dir>/test_history.json`. |
| `dispatch.py:AgentActor` | The Monarch `Actor` wrapping one fixer or evaluator agent invocation; `__cleanup__` tears down its subprocess tree on stop or client disconnect. |
| `agent_prompt.md` | Prompt template for the **fixer** subagent. Two modes: `fresh` and `amend`. |
| `evaluator_prompt.md` | Prompt template for the **evaluator** subagent. Three modes: `pre_submit`, `pre_amend`, `ci_signal`. |

## Where state lives

```
~/.monarch-auto-fix/
  state.json          <- one entry per failing test (issue_key)
  test_history.json   <- TestDataActor's per-test pass/fail observations
  worktrees/<key>/    <- one git worktree per fix in flight
    AUTO_FIX_CONTEXT_UPDATES.md  <- written by the dispatcher when new
                                    pass/fail data arrives mid-fix
  logs/               <- per-invocation agent stdout
  artifacts/          <- cached JUnit XML downloads
  dispatch.lock       <- pid file; prevents concurrent loops
```

State is updated after every meaningful step, so killing the script with
`Ctrl-C` (or a crash) and restarting it picks up where it left off:

- Tests with `status=pr_open` are reconciled against `gh pr view` —
  merged/closed PRs are recorded, red CI triggers an amend.
- Tests with `status=fixing`/`evaluating` are reset to `new` for retry.
- `gave_up`, `pr_merged`, `pr_closed`, `dup_of_existing_pr` are terminal
  and ignored on subsequent cycles.

## Dedup rules

Before dispatching anything for an issue key, the loop checks:

1. **Our own state:** terminal-status entries are skipped.
2. **Title-prefix search across upstream + origin:** any PR whose title
   contains `[auto-fix:<issue_key>]`. Open => track its CI; merged =>
   record as done; closed => give up (a human declined a previous attempt).
3. **Free-text search across upstream:** open PRs whose title or body
   mentions the failing test name verbatim. If someone else is already
   on it, we mark `dup_of_existing_pr` and move on.

PRs we open contain a machine-readable marker
`<!-- auto-fix-key: <key> -->` in the body and the issue key in the
title — so future runs (or other tools) can find them reliably.

## Fixer / evaluator handoff

The fixer **never** pushes branches and **never** runs `gh pr`. It works
entirely in a private worktree. It commits, writes a structured report
in `AUTO_FIX_REPORT.md`, and exits. The dispatcher then runs the
evaluator subagent in the same worktree. Only on a `submit` verdict does
the dispatcher push and call `gh pr create`.

This split exists so we can keep the fixer narrowly focused on
"reproduce + minimize + fix" while the evaluator owns the gating policy
(scope, no junk band-aids, no asserts deleted, etc.). It also means a
weak fix is caught locally — without burning upstream CI cycles.

## How verification works without fork CI

There is no CI budget on the fork. The loop never pushes test branches
to the fork "just to see". The fixer verifies locally on the MacBook:

- Minimal repro (an explicit pair/triple of pytest nodeIds)
- Full failing pytest split group
- Or, for Rust failures, the corresponding nextest invocation

Only after that do we push to origin and `gh pr create` against
upstream. Upstream CI on the PR is the authoritative re-test. If it
goes red, the loop sees the failure on the next cycle and routes back
through the evaluator (`mode=ci_signal`) → fixer (`mode=amend`) →
force-push.

## Usage

```bash
# Make sure you have these on $PATH:
which claude codex gh   # only the chosen agent is required

# Run the loop continuously (default poll interval: 10 min):
uv run python scripts/auto_fix/dispatch.py

# Single cycle for testing:
uv run python scripts/auto_fix/dispatch.py --once

# Run the full local pipeline (fixer + evaluator) but don't push the
# branch and don't open a PR — inspect the worktree afterwards:
uv run python scripts/auto_fix/dispatch.py --once --dry-run

# Same, against a specific test name (skips GitHub discovery entirely):
uv run python scripts/auto_fix/dispatch.py --once --dry-run \
    --manual-failure 'python/tests/test_x.py::test_y'

# Use codex instead of claude:
uv run python scripts/auto_fix/dispatch.py --agent codex
```

`--dry-run` runs the fixer and evaluator for real (real agent calls,
real local builds and test runs) and only suppresses the side effects
that would be visible upstream: the branch push to `--origin` and the
`gh pr create` against `--upstream`. After a dry-run, the committed
fix is sitting on `auto-fix/<key>` in `~/.monarch-auto-fix/worktrees/<key>/`
along with `AUTO_FIX_REPORT.md` for inspection.

The `uv run` prefix is required: the dispatcher imports `monarch.actor`,
and the actor procs it spawns inherit the same interpreter / sys.path,
so both ends need the in-tree Monarch build that `uv sync` produces in
this repo's `.venv`.

You can leave it running in a `tmux` / `screen` session. To stop it
cleanly, send `SIGINT` (Ctrl-C) — state is saved after each step so
restart is safe. Any in-flight fixer/evaluator subprocess is torn down
by `AgentActor.__cleanup__` on the disconnect.

## Tuning

Constants worth knowing in `dispatch.py`:

- `MAX_ATTEMPTS = 3` — give up on a single failure after three rounds
  of fixer-then-amend.
- `FIXER_TIMEOUT_SECONDS`, `EVALUATOR_TIMEOUT_SECONDS` — agent budgets.
- `RUN_LIST_LIMIT = 30` — how many recent upstream runs to scan per
  cycle.
- `LOG_EXCERPT_LINES = 200` — tail of failed-job log shown to agents.

If a particular failure is being mishandled, inspect:
- `~/.monarch-auto-fix/logs/<issue_key>.fixer.*.log`
- `~/.monarch-auto-fix/logs/<issue_key>.evaluator.*.log`
- `~/.monarch-auto-fix/worktrees/<issue_key>/AUTO_FIX_REPORT.md`

To force-retry one issue, edit `~/.monarch-auto-fix/state.json` and
remove that entry (or set its `status` back to `new`).

## What this loop does NOT do

- It does not respond to PR review comments. Human review still owns
  the merge button.
- It does not file `DISABLED <test>` GitHub issues automatically — the
  fixer is told to suggest disabling only as a last resort, and a human
  should be the one to file the issue.
- It does not parallelize fixer invocations across multiple worktrees.
  Sequential is simpler and cheaper given typical agent-call costs;
  parallelism can be added later by lifting the per-cycle for-loop.

## Sandboxing the agents (future work)

The `AgentActor`'s subprocess inherits the dispatcher's full user
authority — filesystem, network, credentials. The proc-per-invocation
model gives a clean wrapping point (the per-actor bootstrap command) but
the loop ships with no sandbox today. On Linux this is what cgroups v2
and Docker / podman / bubblewrap are for. On macOS, where this loop is
primarily run, the options are messier; future contributors evaluating
this should know what's been considered:

- **Linux VM via [Lima](https://lima-vm.io/) or
  [OrbStack](https://orbstack.dev/).** Run the dispatcher inside a
  Linux VM and use real cgroups / Docker inside it. Strongest
  isolation, well-supported tooling. Tradeoff: defeats the loop's
  purpose if you're trying to reproduce *macOS-CI* failures, since the
  VM is Linux. Useful only for the Linux workflow.
- **[Tart](https://tart.run/).** macOS-on-macOS VMs via Apple's
  Virtualization.framework. Lets you keep same-OS repro for the macOS
  workflow while putting the agent in a disposable VM. Heaviest
  setup; fast on Apple Silicon.
- **Separate low-privilege macOS user account.** Create a dedicated
  user with no admin rights, run the dispatcher under it via `sudo
  -u`. macOS file ACLs keep the agent out of your real home, and you
  can layer per-user firewall rules. Cheapest meaningful improvement
  over the status quo; doesn't restrict network or per-process
  resource use.
- **Per-process firewall (e.g.
  [LuLu](https://objective-see.org/products/lulu.html), Little
  Snitch).** Allowlist outbound network per binary. Fine-grained but
  manual to maintain — `cargo`, `uv`, `gh`, `claude`, and the local
  LLM backend all need outbound 443.
- **`sandbox-exec` / Seatbelt.** Apple-deprecated and undocumented; we
  are deliberately *not* building on it. Listed here so future
  contributors don't rediscover and adopt it without realizing.

A reasonable next step would be: separate macOS user + per-user
network allowlist, plus a Linux-only Lima path for the Linux workflow.
The wiring point in code is `invoke_agent()`'s `host.spawn_procs(name=...)`
call — Monarch's `HostMesh.with_python_executable()` (and the
underlying `BootstrapCommand`) can route the actor proc launch through
an arbitrary wrapper command (`sudo -u sandboxed`, `lima nerdctl run`,
etc.) without changing the rest of the dispatcher.
