/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import { build } from "esbuild";
import { mkdtemp, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawnSync } from "node:child_process";

const files = (await readdir("tests", { withFileTypes: true }))
  .filter(file => file.isFile() && /\.test\.tsx?$/.test(file.name))
  .map(file => file.name)
  .sort();
if (files.length === 0) throw new Error("No frontend test files found");

const directory = await mkdtemp(join(tmpdir(), "monarch-dashboard-tests-"));
try {
  await build({ entryPoints: Object.fromEntries(files.map(file => [file, `tests/${file}`])), outdir: directory,
    outExtension: { ".js": ".cjs" }, bundle: true, platform: "node", format: "cjs",
    jsx: "automatic", external: ["jsdom", "canvas"] });
  const result = spawnSync(process.execPath, ["--test", "--require", resolve("tests/dom.cjs"),
    ...files.map(file => join(directory, `${file}.cjs`))], {
    stdio: "inherit", env: { ...process.env, NODE_PATH: resolve("node_modules") },
  });
  if (result.error) throw result.error;
  process.exitCode = result.status ?? 1;
} finally {
  await rm(directory, { recursive: true, force: true });
}
