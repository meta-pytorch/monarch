/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React, { useEffect, useRef } from "react";
import * as echarts from "echarts/core";
import { GaugeChart, PieChart, LineChart, BarChart } from "echarts/charts";
import {
  GridComponent,
  TooltipComponent,
  GraphicComponent,
} from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";

// Register only what we use to keep the esbuild bundle lean.
echarts.use([
  GaugeChart,
  PieChart,
  LineChart,
  BarChart,
  GridComponent,
  TooltipComponent,
  GraphicComponent,
  CanvasRenderer,
]);

interface EChartProps {
  option: echarts.EChartsCoreOption;
  height?: number | string;
  className?: string;
}

/** Thin declarative wrapper: init once, update option, auto-resize, dispose. */
export function EChart({ option, height = 200, className }: EChartProps) {
  const ref = useRef<HTMLDivElement>(null);
  const chart = useRef<echarts.ECharts | null>(null);

  useEffect(() => {
    if (!ref.current) return;
    chart.current = echarts.init(ref.current, undefined, {
      renderer: "canvas",
    });
    const ro = new ResizeObserver(() => chart.current?.resize());
    ro.observe(ref.current);
    return () => {
      ro.disconnect();
      chart.current?.dispose();
      chart.current = null;
    };
  }, []);

  useEffect(() => {
    chart.current?.setOption(option, true);
  }, [option]);

  return (
    <div
      ref={ref}
      className={className}
      style={{ width: "100%", height }}
    />
  );
}
