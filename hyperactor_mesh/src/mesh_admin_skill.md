# Mesh Admin API (Introspection)

Base URL: `{base}`

This server exposes a reference-walking introspection API for a mesh.
Start at `root`, resolve it, then follow `children` to traverse topology.

## TLS

In Meta environments, all endpoints require mutual TLS. Every
request needs:
```
--cacert /var/facebook/rootcanal/ca.pem --cert /var/facebook/x509_identities/server.pem --key /var/facebook/x509_identities/server.pem
```

The base URL may show `http://` but the server listens on
`https://`. Always use `https://` with the TLS flags above.

## Contract

The authoritative API contract is machine-readable:

- `GET {base}/v1/openapi.json` — OpenAPI 3.1 spec
- `GET {base}/v1/schema` — JSON Schema for `NodePayload` responses
- `GET {base}/v1/schema/admin` — JSON Schema for `AdminInfo` responses
- `GET {base}/v1/schema/error` — JSON Schema for error responses

Schema is authoritative over prose in this document. Fetch schema
first when building against this API.

## Error handling

Errors return an `ApiErrorEnvelope` JSON body (see error schema).
The `error.code` field is authoritative for programmatic decisions,
not the HTTP status code. Stable codes: `not_found`, `bad_request`,
`gateway_timeout`, `service_unavailable`, `pyspy_failed`,
`internal_error`.

Retry a capacity-related `service_unavailable` with backoff. The same
code can also mean a required tool is unavailable on the target.
`gateway_timeout` means a downstream host did not respond in time;
the node may still exist.

## Schema-first workflow

1. Fetch schema: `GET {base}/v1/schema`
2. Fetch root: `GET {base}/v1/root`
3. Follow `children` references via `GET {base}/v1/{url_encode(reference)}`
   (references must be percent-encoded in the URL path)
4. On error: match on `error.code`, not HTTP status
5. References are opaque — round-trip values exactly as received,
   but percent-encode when placing in URL paths

All requests require the TLS flags above.

## One-shot diagnostic (recommended starting point)

Run this first to verify reachability and topology of the full mesh
(root → hosts → service proc actors → every user proc → user actors):
for each node, the report confirms it returns a valid payload within
budget. Exit code 0 = all reachable, 1 = any failure.

```
cargo run -p hyperactor_mesh --bin hyperactor_mesh_admin_tui -- \
  --addr {base} --diagnose
```

Each entry in `checks[]` includes `reference` (the exact ref that
failed), `note` (role), `phase` (AdminInfra or Mesh), and `outcome`
(Pass/Slow/Fail with `elapsed_ms` and `error`). Use failing
`reference` values to probe further with the endpoints below.

`--diagnose` covers reachability and topology — can we reach each
node, does each return a valid payload — and does NOT inspect
application-level state such as actor reorder buffers. A
`--diagnose` PASS does not rule out a stalled actor; for ordering
stalls see "Diagnose ordering stalls" and "Find any stalled actor
in the mesh" below.

## Endpoints

Most endpoints are read-only (`GET`). Three endpoints accept `POST`:
`/v1/query` (SQL queries), `/v1/pyspy_dump/{proc_reference}`
(dump-and-store), and `/v1/pyspy_profile_svg/{proc_reference}`
(profile → SVG). All endpoints return `application/json` except
`/SKILL.md` (`text/markdown`) and
`/v1/pyspy_profile_svg/{proc_reference}` (`image/svg+xml`).

- `GET {base}/v1/admin`
  Admin self-identification: returns `AdminInfo` with `actor_id`,
  `proc_id`, `host`, and `url`. Use to verify placement and discover
  the admin's identity.

- `GET {base}/v1/schema`
  JSON Schema for `NodePayload` (authoritative contract).

- `GET {base}/v1/schema/admin`
  JSON Schema for `AdminInfo`.

- `GET {base}/v1/schema/error`
  JSON Schema for error envelope.

- `GET {base}/v1/openapi.json`
  OpenAPI 3.1 spec (embeds JSON Schemas, full response mapping).

- `GET {base}/v1/root`
  Returns the synthetic root `NodePayload`.

- `GET {base}/v1/{reference}`
  Resolves `{reference}` to a JSON `NodePayload`.

- `GET {base}/v1/tree`
  Human-readable ASCII topology dump (convenience endpoint).

- `GET {base}/v1/pyspy/{proc_reference}`
  Requests a py-spy stack dump from the process hosting
  `{proc_reference}`. The reference must be a valid ProcAddr
  (percent-encoded in the URL path). Requires py-spy in the
  target environment and ptrace permissions.

  Success returns a `PySpyResult` JSON variant:
  - `{"Ok": {"pid": N, "binary": "...", "capture_mode": "native_all", "stack_traces": [...], "warnings": [...]}}` — structured stack dump
  - `{"BinaryNotFound": {"searched": [...]}}` — py-spy not available
  - `{"Failed": {"pid": N, "binary": "...", "exit_code": N, "stderr": "..."}}` — py-spy error

  Native frames are requested server-side and are best-effort: on
  failure the server retries python-only and still returns `Ok`.
  `capture_mode` records whether the successful attempt used `--native-all`,
  `--native`, or neither flag. `warnings` explains any fallback. See
  "py-spy: missing native frames".

  The endpoint supports worker procs and the service proc. A
  proc supports py-spy iff its stable handler actor is
  reachable: the service proc requires `host_agent`; non-service
  procs require `proc_agent[0]`. On worker procs, the request is
  handled by ProcAgent. On the service proc (which hosts
  HostAgent instead of ProcAgent), the bridge automatically
  routes to HostAgent. If the target agent is not reachable, an
  immediate `not_found` error is returned instead of waiting for
  the full bridge timeout. If the probe send itself fails (a
  bridge-side infrastructure problem), `internal_error` is
  returned.

  Timeout returns the standard `gateway_timeout` error envelope.

- `POST {base}/v1/pyspy_profile_svg/{proc_reference}`
  Profiles the process for a requested duration and returns an SVG
  flamegraph. POST body is JSON `PySpyProfileOpts`:
  `{"duration_s": 5, "rate_hz": 100, "native": false, "threads": false, "nonblocking": false}`

  Returns `image/svg+xml` on success. Long-running — timeout scales
  with `duration_s`. Max duration is configurable (default 300s).

  `native` is caller-controlled for profiles. Unlike stack dumps, a
  profile requested with `native: true` does not fall back to
  Python-only sampling when native capture fails; it returns
  `profile_failed`. Use `native: false` when a Python-only flamegraph
  is acceptable, or configure a py-spy binary that can capture native
  frames.

  Error responses:
  - 400 — invalid `duration_s` or `rate_hz`
  - 404 — proc not found or handler not reachable
  - 503 — py-spy not available on target host
  - 504 — py-spy record subprocess timed out

  Agent note: `{encoded_proc_ref}` is the percent-encoded ProcAddr
  string for the target process. If you save the
  returned SVG on a remote host for browser viewing, tell the user
  the remote file path, the serving port, the exact `ssh -L`
  tunnel command, and the browser URL.

  Example (adapt ports if already in use):
  `curl {TLS} -X POST -H 'Content-Type: application/json' -d '{"duration_s":5,"rate_hz":100,"native":false,"threads":false,"nonblocking":false}' '{base}/v1/pyspy_profile_svg/{encoded_proc_ref}' -o /tmp/profile.svg`
  `cd /tmp && python3 -m http.server 8888 --bind 127.0.0.1`
  User tunnel: `ssh -L <local_port>:127.0.0.1:8888 {host}`
  Browser: `http://localhost:<local_port>/profile.svg`

- `GET {base}/v1/config/{proc_reference}`
  Returns the effective CONFIG-marked configuration entries from the
  process hosting `{proc_reference}`. The reference must be a valid
  ProcAddr (percent-encoded in the URL path).

  Success returns a `ConfigDumpResult` JSON object:
  ```json
  {
    "entries": [
      {
        "name": "hyperactor::config::codec_max_frame_length",
        "value": "1048576",
        "default_value": "1048576",
        "source": "Default",
        "changed_from_default": false,
        "env_var": "HYPERACTOR_CODEC_MAX_FRAME_LENGTH"
      }
    ]
  }
  ```

  Each entry contains:
  - `name` — fully-qualified config key (module_path::key_name)
  - `value` — current resolved value (display string)
  - `default_value` — declared default (null if none)
  - `source` — which layer provided the value: Default,
    ClientOverride, File, Env, Runtime, or TestOverride
  - `changed_from_default` — true when value differs from default
  - `env_var` — environment variable name (null if not env-backed)

  Entries are sorted by `name`. Only CONFIG-marked keys are
  included (not INTROSPECT keys).

  The endpoint supports worker procs and the service proc. Same
  routing as py-spy: ProcAgent for worker procs, HostAgent for the
  service proc. If the target agent is not reachable, an immediate
  `not_found` error is returned. Timeout returns `gateway_timeout`.

  Automated integration test:
  ```
  buck2 test fbcode//monarch/hyperactor_mesh:config_integration_test
  ```

- `POST {base}/v1/query`
  Execute a SQL query to distributed telemetry DataFusion engine.
  Requires `telemetry_url` to be configured.

  Request body (`QueryRequest`):
  ```json
  {"sql": "SELECT * FROM actors LIMIT 10"}
  ```

  Success returns a `QueryResponse`:
  ```json
  {"rows": [ ... ]}
  ```

  `rows` contains the DataFusion result set as a JSON array. On
  invalid SQL or query failure, a non-200 status is returned with
  the dashboard's error message.

  Discover tables with: `SELECT table_name FROM information_schema.tables`.

- `POST {base}/v1/pyspy_dump/{proc_reference}`
  Captures a py-spy stack dump from the process hosting
  `{proc_reference}` and persists the result in the telemetry
  store. The reference must be a valid ProcAddr (percent-encoded
  in the URL path). Requires `telemetry_url` to be configured.

  The endpoint performs two steps:
  1. Sends a `PySpyDump` message to the target proc's agent
     (same routing as `GET /v1/pyspy/{proc_reference}`).
  2. Stores a successful result in DataFusion via the dashboard, keyed
     by a generated UUID.

  Success returns a `PyspyDumpAndStoreResponse`:
  ```json
  {"dump_id": "550e8400-e29b-41d4-a716-446655440000"}
  ```

  Use `dump_id` to retrieve the stored dump via `/v1/query`:
  ```json
  {"sql": "SELECT * FROM pyspy_dumps WHERE dump_id = '550e8400-...'"}
  ```

  `pyspy_dumps.capture_mode` records the successful py-spy invocation mode as
  a queryable string. `pyspy_dumps.warnings_json` preserves the result's
  warnings as a JSON string array with fallback details.

  A failed capture returns `pyspy_failed`, and a missing py-spy binary
  returns `service_unavailable`. Neither response includes a `dump_id`.
  An unreachable target returns `not_found`; a bridge timeout returns
  `gateway_timeout`.

  Runs the same native-frame capture as `GET /v1/pyspy`, with the
  same best-effort fallback. See "py-spy: missing native frames".

- `GET {base}/SKILL.md`
  This document.

### py-spy: missing native frames

**If every frame in a dump is a `.py` file, check `capture_mode` before
drawing any conclusion about the process.** An all-Python stack is
ambiguous when the successful attempt used a native flag;
`capture_mode: "python_only"` confirms that it used neither native flag. Read
`warnings` for the reason.

This matters because a proc blocked in a collective, an allocator, or a
hyperactor C++ path shows nothing useful in Python-only frames -- one
opaque call into an extension module, the real state invisible. Reading
"idle" or "stuck in `<some .py line>`" off a degraded dump is the
mistake this prevents.

`GET /v1/pyspy` and `POST /v1/pyspy_dump` request native frames
(`--native --native-all`) server-side, which can make a dump interleave
interpreter and extension frames with the Python ones:

```
0x7f237a20189a       libpython3.12.so.1.0:0
<your frame>         your_module.py:118
task_step_impl       _asyncio.cpython-312-x86_64-linux-gnu.so:0
```

#### Recognizing the loss

For stack dumps, native capture is best-effort: on failure the server
retries python-only rather than erroring, so you get `200`, an `Ok`
result, and no error anywhere. Check `capture_mode` first;
`python_only` definitively identifies an attempt that used neither native
flag. `warnings` explains why:

- `native capture failed; fell back to python-only frames. py-spy
  (<resolution>) could not unwind native frames …` — Python-only. Do not
  treat the stack as complete.
- `--native-all unsupported by py-spy (<resolution>); fell back to
  --native` — the successful attempt used `--native`. Inspect the returned
  frames to determine whether native frames appeared.

`<resolution>` is how py-spy was found: `PYSPY_BIN=/path` if set, else
`py-spy on PATH`. A `PYSPY_BIN=…` prefix means the variable is already
set to a build that cannot unwind — repoint it, do not set it.

In the admin TUI the warning renders under the `pid`/`binary` header,
above the first thread.

#### Recovering native frames

py-spy 0.4.0 fails with `UNW_EINVAL` on fbcode Python binaries; 0.4.1
unwinds them. Install 0.4.1 from PyPI (wheel sha256
`6a80ec05eb8a6883863a367c6a4d4f2d57de68466f7956b6367d4edd5c61bb29`):

```
mkdir -p /tmp/pyspy041 && cd /tmp/pyspy041
curl -sSfL -o py_spy.whl https://files.pythonhosted.org/packages/68/fb/bc7f639aed026bca6e7beb1e33f6951e16b7d315594e7635a4f7d21d63f4/py_spy-0.4.1-py2.py3-none-manylinux_2_5_x86_64.manylinux1_x86_64.whl
echo '6a80ec05eb8a6883863a367c6a4d4f2d57de68466f7956b6367d4edd5c61bb29  py_spy.whl' | sha256sum -c -
python3 -c "import zipfile,os; z=zipfile.ZipFile('py_spy.whl'); open('py-spy','wb').write(z.read('py_spy-0.4.1.data/scripts/py-spy')); os.chmod('py-spy',0o755)"
```

Point `PYSPY_BIN` at it **in the environment of the proc being dumped** --
resolution happens there, at dump time, so it must precede that proc
starting:

```
PYSPY_BIN=/tmp/pyspy041/py-spy <workload command>
```

`monarch.config.configure(pyspy_bin=...)` also works and reaches procs
spawned afterwards. Neither route affects a running proc. 0.4.1 has no
`--native-all`, so expect that warning; the frames are still native.

## Response: NodePayload

Successful resolves return a JSON object:

- `identity` — the resolved reference string (opaque; round-trip it exactly)
- `properties` — externally-tagged variant, one of:
  `{"Root": {...}}`, `{"Host": {...}}`, `{"Proc": {...}}`,
  `{"Actor": {...}}`, `{"Error": {...}}`
- `children` — list of reference strings to resolve next
- `parent` — optional parent reference (navigation context)
- `as_of` — ISO 8601 timestamp of when this data was captured

Each child reference can be resolved via `/v1/{reference}` (URL-encode first).
Clients should treat reference strings as opaque tokens.

## Key fields

**`actor_status`** (Actor variant): lifecycle state of the actor.
Values: `running` (processing messages), `idle` (waiting for
messages), `stopped` / `stopped: <reason>`, `failed` /
`failed: <reason>`.

**`system_children`** (Root, Host, Proc variants): infrastructure
actors that are part of the mesh framework (proc_agent, comm,
logger, etc.), not user workloads. When debugging user actors,
filter `children` to exclude entries that also appear in
`system_children`.

**`flight_recorder`** (Actor variant): JSON-encoded string
containing recent trace spans. Can be large (tens of KB).
Exclude it when summarizing topology. Parse as JSON if
trace-level debugging is needed. Filter with:
`jq '{identity, properties: {Actor: (.properties.Actor | del(.flight_recorder))}, children}'`

**`failure_info`** (Actor variant): present only when
`actor_status` starts with `failed`. Contains `error_message`,
`root_cause_actor`, `occurred_at`, and `is_propagated`.

## Diagnose ordering stalls

`GET /v1/{actor}`. Look at `inbound_ordering`.

- `inbound_ordering == null` — the actor doesn't go through the ordered work-queue path (IO-1: structural absence; e.g., an instance not built through `Instance::new`).
- `inbound_ordering.enabled == false` — reorder buffering is off; `sessions` is empty regardless of traffic.
- `inbound_ordering.snapshot_complete == false` — the aggregate sequencing lock was busy, so session state is unavailable. Ignore `sessions`, `skipped_session_count`, `known_session_count`, and every `returned_*` rollup, then refetch with bounded backoff. Do not alert on unavailability alone.
- `inbound_ordering.snapshot_complete == true && inbound_ordering.returned_buffered_message_count == 0` — no stalls.
- Otherwise: filter `sessions` for `buffered_count > 0`. Each stalled session has:
  - `sender` — a string `ActorAddr` of the **session owner** (the actor whose `Sequencer` assigned the SEQ_INFO for this session). For direct sends and casts, the session owner IS the logical sender.
  - `expected_next_seq` — the seq the next contiguous send must carry to unblock the buffer.
  - `oldest_buffered_seq` / `newest_buffered_seq` — the buffered seq range.
- Diagnosis template: "`{actor}` is waiting for seq `{expected_next_seq}` from session owner `{sender}`; `{buffered_count}` messages buffered from seq `{oldest_buffered_seq}` to `{newest_buffered_seq}`." The waiting happens at the receiver: the session owner has already done its part (its seqs are in the buffer); the receiver is blocked until the missing seq arrives.
- For a complete snapshot, `known_session_count == sessions.len()` and every `returned_*` rollup is exact. `known_session_count` includes idle / control-plane sessions (e.g., a `client.local` session from a bootstrap call) that have `buffered_count == 0`; those show up in the total but never as stalls. Always filter `sessions` by `buffered_count > 0` before applying the diagnosis template above.
- `queue_depth` and `inbound_ordering.returned_buffered_message_count` are **independent diagnostics with different scopes** (IO-3). `queue_depth` is accepted handler work; `returned_buffered_message_count` is the reorder-buffer total from a complete snapshot. No arithmetic or ordering relationship between the two is part of the API contract; don't derive one from the other.

## Find any stalled actor in the mesh

When you don't know which actor to look at, walk top-down and flag
anything blocked. Reference-shape-agnostic: no name matching, no
opaque-ref parsing beyond percent-encoding for the URL path.

1. `GET {base}/v1/root`. The `children` are host references.
2. For each host, `GET {base}/v1/{host_ref}`. The `children` are
   proc references.
3. For each proc, `GET {base}/v1/{proc_ref}`. The `children` are
   actor references; user actors are those NOT also present in the
   proc's `system_children` (see "Key fields" above).
4. For each user actor, `GET {base}/v1/{actor_ref}` and read
   `properties.Actor.inbound_ordering`.
5. Flag any actor where `inbound_ordering` is non-null AND
   `snapshot_complete == true` AND
   `returned_buffered_message_count > 0`. If `snapshot_complete ==
   false`, ignore all session-derived fields and refetch.
6. For each flagged actor, filter `sessions` by `buffered_count > 0`
   and apply the diagnosis template from "Diagnose ordering stalls"
   above to each remaining session.

## Navigation algorithm

1. Fetch root:
   `curl --cacert /var/facebook/rootcanal/ca.pem --cert /var/facebook/x509_identities/server.pem --key /var/facebook/x509_identities/server.pem '{base}/v1/root'`

2. Select a child reference:
   `curl --cacert /var/facebook/rootcanal/ca.pem --cert /var/facebook/x509_identities/server.pem --key /var/facebook/x509_identities/server.pem '{base}/v1/{child_reference}'`

3. Repeat. Each node describes its next traversal step.

Notes:

- References must be URL-encoded when used in paths.
- The topology is eventually consistent; nodes may disappear between requests.
  Treat HTTP 404 as "node not found" and continue traversal.

## Reference strings

References are opaque identifiers. Clients must not parse them.
Always round-trip references exactly as returned in `children`.

Common examples include:

- `root` — synthetic entrypoint
- Actor references (`ActorAddr`)
- Proc references (`ProcAddr`)

## Examples

List root children:

`curl --cacert /var/facebook/rootcanal/ca.pem --cert /var/facebook/x509_identities/server.pem --key /var/facebook/x509_identities/server.pem '{base}/v1/root' | jq -r '.children[]'`

Resolve a child (URL-encoded):

`curl --cacert /var/facebook/rootcanal/ca.pem --cert /var/facebook/x509_identities/server.pem --key /var/facebook/x509_identities/server.pem '{base}/v1/'$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1], safe=''))" '{example_ref}')`

Actor details without flight recorder noise:

`curl --cacert /var/facebook/rootcanal/ca.pem --cert /var/facebook/x509_identities/server.pem --key /var/facebook/x509_identities/server.pem '{base}/v1/{reference}' | jq '{identity, properties: {Actor: (.properties.Actor | del(.flight_recorder))}, children}'`

## Agent self-test

Run these checks against a live server to verify correct API
usage. Each check is binary (1 = pass, 0 = fail). Report total
score and per-check results. Track scores across sessions to
measure improvement or regression.

**Contract discovery (2 pts)**

1. `GET {base}/v1/schema` returns 200 with valid JSON containing
   a `$id` field with "v1" in the value.
2. `GET {base}/v1/schema/error` returns 200 with valid JSON
   containing a `$id` field.

**Navigation (3 pts)**

3. `GET {base}/v1/root` returns 200 with `properties` key ==
   "Root" and non-empty `children`.
4. Pick first child from root. URL-encode it. `GET
   {base}/v1/{encoded_child}` returns 200 with `properties`
   key in {Root, Host, Proc, Actor, Error}.
5. The child response has `parent` set (non-null).

**Schema conformance (2 pts)**

6. The root response from check 3 validates against the schema
   from check 1 (validate response structure against the schema
   using any available JSON Schema validator).
7. The child response from check 4 also validates against the
   schema from check 1.

**Error handling (2 pts)**

8. `GET {base}/v1/nonexistent_ref_xxxx` returns a JSON body
   with top-level `error` object containing `code` field.
9. The `error.code` value is exactly `"not_found"`.

**Security boundary (1 pt)**

10. Across all responses collected in checks 3, 4, 8: no
    response body contains substrings `PortId`, `PortRef`,
    `MailboxSender`, or `ChannelTransport`.

### Scoring

- **10/10:** Full competence. Agent discovers, navigates,
  validates, and handles errors correctly.
- **8-9/10:** Functional with gaps. Review failing checks.
- **<8/10:** Re-read this document from the top.

### Tracking

Record: `{timestamp, score, failing_checks[], server_version}`.
Compare across sessions. A score regression after a SKILL.md
change means the edit made the document harder to follow. A
score regression after a server change means the API or schema
drifted. Use the schema `$id` to correlate.
