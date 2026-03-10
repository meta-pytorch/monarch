/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React from "react";
import { render, screen, fireEvent } from "@testing-library/react";
import { Header } from "../components/Header";

const TABS = [
  { id: "hierarchy", label: "Hierarchy" },
  { id: "dag", label: "DAG" },
];

describe("Header", () => {
  test("renders brand name", () => {
    render(<Header tabs={TABS} activeTab="hierarchy" onTabChange={jest.fn()} />);
    expect(screen.getByText("Monarch")).toBeInTheDocument();
    expect(screen.getByText("dashboard")).toBeInTheDocument();
  });

  test("renders all tabs", () => {
    render(<Header tabs={TABS} activeTab="hierarchy" onTabChange={jest.fn()} />);
    expect(screen.getByText("Hierarchy")).toBeInTheDocument();
    expect(screen.getByText("DAG")).toBeInTheDocument();
  });

  test("marks active tab", () => {
    render(<Header tabs={TABS} activeTab="hierarchy" onTabChange={jest.fn()} />);
    const activeTab = screen.getByText("Hierarchy");
    expect(activeTab).toHaveClass("active");
    expect(activeTab).toHaveAttribute("aria-selected", "true");
  });

  test("marks inactive tab", () => {
    render(<Header tabs={TABS} activeTab="hierarchy" onTabChange={jest.fn()} />);
    const dagTab = screen.getByText("DAG");
    expect(dagTab).not.toHaveClass("active");
    expect(dagTab).toHaveAttribute("aria-selected", "false");
  });

  test("clicking tab calls onTabChange", () => {
    const onTabChange = jest.fn();
    render(<Header tabs={TABS} activeTab="hierarchy" onTabChange={onTabChange} />);
    fireEvent.click(screen.getByText("DAG"));
    expect(onTabChange).toHaveBeenCalledWith("dag");
  });

  test("renders butterfly logo SVG", () => {
    const { container } = render(
      <Header tabs={TABS} activeTab="hierarchy" onTabChange={jest.fn()} />
    );
    expect(container.querySelector(".header-logo")).toBeInTheDocument();
  });
});
