/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import assert from "node:assert/strict";
import { test } from "node:test";
import { useState } from "react";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Drawer } from "../src/components/common/ui";
import "./dom";

function DrawerExample() {
  const [open, setOpen] = useState(false);
  return <>
    <button onClick={() => setOpen(true)}>Open details</button>
    {open && <Drawer title="Worker" onClose={() => setOpen(false)}>
      Worker details
    </Drawer>}
  </>;
}

test("Drawer opens with its content and closes through the Close button", async () => {
  const user = userEvent.setup();
  render(<DrawerExample />);
  assert.equal(screen.queryByRole("dialog"), null);

  await user.click(screen.getByRole("button", { name: "Open details" }));
  const drawer = screen.getByRole("dialog", { name: "Worker" });
  assert.ok(within(drawer).getByText("Worker"));
  assert.ok(within(drawer).getByText("Worker details"));

  await user.click(within(drawer).getByRole("button", { name: "Close" }));
  assert.equal(screen.queryByRole("dialog"), null);
});
