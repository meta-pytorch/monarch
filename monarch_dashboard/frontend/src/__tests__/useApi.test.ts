/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import { renderHook, waitFor } from "@testing-library/react";
import { useApi } from "../hooks/useApi";

describe("useApi", () => {
  const MOCK_DATA = [{ id: 1, name: "test" }];

  beforeEach(() => {
    jest.restoreAllMocks();
  });

  test("starts in loading state", () => {
    jest.spyOn(global, "fetch").mockImplementation(
      () => new Promise(() => {}) // never resolves
    );
    const { result } = renderHook(() => useApi("/meshes"));
    expect(result.current.loading).toBe(true);
    expect(result.current.data).toBeNull();
    expect(result.current.error).toBeNull();
  });

  test("resolves with data on success", async () => {
    jest.spyOn(global, "fetch").mockResolvedValue({
      ok: true,
      json: async () => MOCK_DATA,
    } as Response);

    const { result } = renderHook(() => useApi("/meshes"));

    await waitFor(() => {
      expect(result.current.loading).toBe(false);
    });

    expect(result.current.data).toEqual(MOCK_DATA);
    expect(result.current.error).toBeNull();
  });

  test("sets error on HTTP error", async () => {
    jest.spyOn(global, "fetch").mockResolvedValue({
      ok: false,
      status: 500,
    } as Response);

    const { result } = renderHook(() => useApi("/meshes"));

    await waitFor(() => {
      expect(result.current.loading).toBe(false);
    });

    expect(result.current.data).toBeNull();
    expect(result.current.error).toBe("HTTP 500");
  });

  test("sets error on network failure", async () => {
    jest.spyOn(global, "fetch").mockRejectedValue(new Error("Network error"));

    const { result } = renderHook(() => useApi("/meshes"));

    await waitFor(() => {
      expect(result.current.loading).toBe(false);
    });

    expect(result.current.data).toBeNull();
    expect(result.current.error).toBe("Network error");
  });

  test("refetch triggers new fetch", async () => {
    const fetchMock = jest.spyOn(global, "fetch").mockResolvedValue({
      ok: true,
      json: async () => MOCK_DATA,
    } as Response);

    const { result } = renderHook(() => useApi("/meshes"));

    await waitFor(() => {
      expect(result.current.loading).toBe(false);
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);

    // Trigger refetch
    result.current.refetch();

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledTimes(2);
    });
  });

  test("calls correct URL", async () => {
    const fetchMock = jest.spyOn(global, "fetch").mockResolvedValue({
      ok: true,
      json: async () => [],
    } as Response);

    renderHook(() => useApi("/actors?mesh_id=3"));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith("/api/actors?mesh_id=3");
    });
  });
});
