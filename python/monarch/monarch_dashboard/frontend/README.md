# Frontend

## Dependency locks

This frontend currently supports two locked dependency install paths:

- npm-based package builds use `package-lock.json` and install with `npm ci`.
- Buck/Yarn-based builds use `yarn.lock` and install with Yarn.

Keep both lockfiles in sync when changing `package.json`. Do not delete either
lockfile until every supported build path uses the same package manager.

## Tests

Run `npm test` from this directory. The runner discovers files directly under
`tests/` named `*.test.ts` or `*.test.tsx`, in sorted order. Keep fixtures and
support helpers outside that pattern. A test failure or no matching test files
returns a nonzero exit code.

DOM tests use React Testing Library and user-event. Import `./dom` in each DOM
test file to unmount rendered roots after every test. A CommonJS preload installs
JSDOM before test imports and closes its document after the file finishes. Each
test file runs in its own process, with React and Testing Library bundled together.
JSDOM stays external; no native canvas dependency is needed.

Run `npm run typecheck` to check both application and test TypeScript. Test types
use the existing TypeScript 4.9 compiler; Node types are pinned for compatibility.
