# Frontend Dependency Locks

This frontend currently supports two locked dependency install paths:

- npm-based package builds use `package-lock.json` and install with `npm ci`.
- Buck/Yarn-based builds use `yarn.lock` and install with Yarn.

Keep both lockfiles in sync when changing `package.json`. Do not delete either
lockfile until every supported build path uses the same package manager.
