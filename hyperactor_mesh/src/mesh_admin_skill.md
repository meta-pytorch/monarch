# Mesh Admin API (Introspection)

Base URL: `{base}`

This server exposes a reference-walking introspection API for a mesh.
Start at `root`, resolve it, then follow `children` to traverse topology.

## Endpoints

- `GET {base}/v1/root`
  Returns the synthetic root `NodePayload`.

- `GET {base}/v1/{reference}`
  Resolves `{reference}` to a JSON `NodePayload`.

- `GET {base}/v1/tree`
  Human-readable ASCII topology dump (convenience endpoint).

- `GET {base}/SKILL.md`
  This document.

## Response: NodePayload

Successful resolves return a JSON object:

- `identity` — the resolved reference string
- `properties` — one of `Root | Host | Proc | Actor | Error`
- `children` — list of reference strings to resolve next
- `parent` — optional parent reference (navigation context)

Each child reference can be resolved via `/v1/{reference}`.

## Navigation algorithm

1. Fetch root:
   `curl '{base}/v1/root'`

2. Select a child reference:
   `curl '{base}/v1/{child_reference}'`

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

`curl '{base}/v1/root' | jq -r '.children[]'`

Resolve a child (URL-encoded):

`curl '{base}/v1/'$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1], safe=''))" '{example_ref}')`

Actor nodes include a `flight_recorder` field with recent trace events.
To focus on structure and stats, filter it out:

`curl '{base}/v1/{reference}' | jq '{identity, properties, children}'`
