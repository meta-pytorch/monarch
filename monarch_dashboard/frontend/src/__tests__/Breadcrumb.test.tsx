/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React from "react";
import { render, screen, fireEvent } from "@testing-library/react";
import { Breadcrumb } from "../components/Breadcrumb";
import { NavItem } from "../types";

const ITEMS: NavItem[] = [
  { label: "Host Meshes", level: "hosts" },
  { label: "host_mesh_0", level: "procs", meshId: 1 },
  { label: "proc_mesh_0_0", level: "actor_meshes", meshId: 3 },
];

describe("Breadcrumb", () => {
  test("renders all labels", () => {
    render(<Breadcrumb items={ITEMS} onNavigate={jest.fn()} />);
    expect(screen.getByText("Host Meshes")).toBeInTheDocument();
    expect(screen.getByText("host_mesh_0")).toBeInTheDocument();
    expect(screen.getByText("proc_mesh_0_0")).toBeInTheDocument();
  });

  test("last item is not a button", () => {
    render(<Breadcrumb items={ITEMS} onNavigate={jest.fn()} />);
    const current = screen.getByText("proc_mesh_0_0");
    expect(current.tagName).not.toBe("BUTTON");
    expect(current).toHaveClass("breadcrumb-current");
  });

  test("earlier items are clickable buttons", () => {
    render(<Breadcrumb items={ITEMS} onNavigate={jest.fn()} />);
    const link = screen.getByText("Host Meshes");
    expect(link.tagName).toBe("BUTTON");
  });

  test("clicking earlier item calls onNavigate with index", () => {
    const onNavigate = jest.fn();
    render(<Breadcrumb items={ITEMS} onNavigate={onNavigate} />);
    fireEvent.click(screen.getByText("host_mesh_0"));
    expect(onNavigate).toHaveBeenCalledWith(1);
  });

  test("renders separators between items", () => {
    const { container } = render(
      <Breadcrumb items={ITEMS} onNavigate={jest.fn()} />
    );
    const seps = container.querySelectorAll(".breadcrumb-sep");
    expect(seps.length).toBe(2);
  });

  test("single item has no separators or buttons", () => {
    const single: NavItem[] = [{ label: "Root", level: "hosts" }];
    const { container } = render(
      <Breadcrumb items={single} onNavigate={jest.fn()} />
    );
    expect(container.querySelectorAll(".breadcrumb-sep").length).toBe(0);
    expect(container.querySelectorAll("button").length).toBe(0);
  });
});
