/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React from "react";
import { render, screen } from "@testing-library/react";
import { StatusBadge } from "../components/StatusBadge";

describe("StatusBadge", () => {
  test("renders status text", () => {
    render(<StatusBadge status="idle" />);
    expect(screen.getByText("idle")).toBeInTheDocument();
  });

  test("renders with data-status attribute", () => {
    const { container } = render(<StatusBadge status="failed" />);
    const badge = container.querySelector(".status-badge");
    expect(badge).toHaveAttribute("data-status", "failed");
  });

  test("renders dot element", () => {
    const { container } = render(<StatusBadge status="processing" />);
    expect(container.querySelector(".status-dot")).toBeInTheDocument();
  });

  test("handles null status", () => {
    render(<StatusBadge status={null} />);
    expect(screen.getByText("unknown")).toBeInTheDocument();
  });

  test("handles undefined status", () => {
    render(<StatusBadge status={undefined} />);
    expect(screen.getByText("unknown")).toBeInTheDocument();
  });

  test("lowercases status", () => {
    render(<StatusBadge status="IDLE" />);
    expect(screen.getByText("idle")).toBeInTheDocument();
  });
});
