/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import { build } from "esbuild";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

const directory = await mkdtemp(join(tmpdir(), "monarch-dashboard-tests-"));
try {
  const names = ["actorHealth"];
  await build({ entryPoints: names.map(name => `tests/${name}.test.ts`), outdir: directory,
    outExtension: { ".js": ".cjs" }, bundle: true, platform: "node", format: "cjs" });
  const result = spawnSync(process.execPath, ["--test", ...names.map(name => join(directory, `${name}.test.cjs`))], { stdio: "inherit" });
  if (result.error) throw result.error;
  process.exitCode = result.status ?? 1;
} finally {
  await rm(directory, { recursive: true, force: true });
}
