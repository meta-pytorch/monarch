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
- `GET {base}/v1/schema/error` — JSON Schema for error responses

Schema is authoritative over prose in this document. Fetch schema
first when building against this API.

## Error handling

Errors return an `ApiErrorEnvelope` JSON body (see error schema).
The `error.code` field is authoritative for programmatic decisions,
not the HTTP status code. Stable codes: `not_found`, `bad_request`,
`gateway_timeout`, `service_unavailable`, `internal_error`.

`service_unavailable` is transient (server at capacity) — retry
with backoff. `gateway_timeout` means a downstream host did not
respond in time; the node may still exist.

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

Run this first to get a structured JSON health report of the full mesh
(root → hosts → service proc actors → every user proc → user actors).
Exit code 0 = healthy, 1 = any failure.

```
cargo run -p hyperactor_mesh --bin hyperactor_mesh_admin_tui -- \
  --addr {base} --diagnose
```

Each entry in `checks[]` includes `reference` (the exact ref that
failed), `note` (role), `phase` (AdminInfra or Mesh), and `outcome`
(Pass/Slow/Fail with `elapsed_ms` and `error`). Use failing
`reference` values to probe further with the endpoints below.

## Endpoints

All endpoints are read-only (`GET`). All return `application/json`
except `/SKILL.md` (`text/markdown`).

- `GET {base}/v1/schema`
  JSON Schema for `NodePayload` (authoritative contract).

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
  `{proc_reference}`. The reference must be a valid ProcId
  (percent-encoded in the URL path). Requires py-spy in the
  target environment and ptrace permissions.

  Success returns a `PySpyResult` JSON variant:
  - `{"Ok": {"pid": N, "binary": "...", "stack_traces": [...], "warnings": [...]}}` — structured stack dump
  - `{"BinaryNotFound": {"searched": [...]}}` — py-spy not available
  - `{"Failed": {"pid": N, "binary": "...", "exit_code": N, "stderr": "..."}}` — py-spy error

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

- `GET {base}/SKILL.md`
  This document.

## Response: NodePayload

Successful resolves return a JSON object:

- `identity` — the resolved reference string
- `properties` — externally-tagged variant, one of:
  `{"Root": {...}}`, `{"Host": {...}}`, `{"Proc": {...}}`,
  `{"Actor": {...}}`, `{"Error": {...}}`
- `children` — list of reference strings to resolve next
- `parent` — optional parent reference (navigation context)
- `as_of` — ISO 8601 timestamp of when this data was captured

Each child reference can be resolved via `/v1/{reference}`.

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
- Actor references (`ActorId`)
- Proc references (`ProcId`)

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
   from check 1 (parse schema as JSON Schema Draft 2020-12,
   validate response structure).
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

## py-spy validation

Automated integration test (runs all three modes — cpu, block,
mixed — sequentially):

```
buck2 test fbcode//monarch/hyperactor_mesh:pyspy_integration_test
```

Manual verification against a live mesh:

1. Start the py-spy workload:

```
buck2 run fbcode//monarch/python/examples:pyspy_workload -- \
  --mode cpu --work-ms 500 --concurrency 3
```

2. Run the verification script (exit codes: 0 PASS, 1 FAIL,
   2 SKIP when py-spy is unavailable):

```
buck2 run fbcode//monarch/python/examples:verify_pyspy -- \
  --admin-url <url> --mode cpu --samples 10 \
  --cacert /var/facebook/rootcanal/ca.pem \
  --cert /var/facebook/x509_identities/server.pem \
  --key /var/facebook/x509_identities/server.pem
```

Modes: `cpu` (iterative CPU burn), `block` (blocking sleep),
`mixed` (alternating CPU + async). The verifier checks for
mode-specific evidence frames in py-spy stacks.
