/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React from "react";
import { EChart } from "../common/echart";

export interface ResourceSeries {
  name: string;
  data: Array<[number, number | null]>;
}

export function ResourceChart({ title, unit, series, start, end, max }: {
  title: string; unit: string; series: ResourceSeries[]; start: number; end: number; max?: number;
}) {
  const css = getComputedStyle(document.documentElement);
  const colors = [1, 2, 3, 4].map(i => css.getPropertyValue(`--chart-${i}`).trim());
  const text = css.getPropertyValue("--text-2").trim();
  const border = css.getPropertyValue("--border").trim();
  const hasData = series.some(s => s.data.some(([, v]) => v != null));
  return (
    <section className="panel resource-chart" aria-label={title}>
      <div className="panel-head"><h3>{title}</h3><span className="sub">{unit}</span></div>
      <div className="panel-body">
        {!hasData ? <div className="resource-chart-empty">No readings available</div> : (
          <div role="img" aria-label={`${title}. ${series.map(s => `${s.name}: ${s.data.filter(([, v]) => v != null).length} readings`).join("; ")}.`}>
            <EChart height={170} option={{
              animation: false,
              grid: { left: 45, right: 12, top: 8, bottom: 25 },
              tooltip: { trigger: "axis", backgroundColor: css.getPropertyValue("--surface-2").trim(), borderColor: border, textStyle: { color: text }, valueFormatter: (v: number | null) => v == null ? "Unavailable" : `${Number(v).toFixed(2)} ${unit}` },
              xAxis: { type: "time", min: start / 1000, max: end / 1000, axisLabel: { color: text, fontSize: 10 }, axisLine: { lineStyle: { color: border } }, splitLine: { show: false } },
              yAxis: { type: "value", min: 0, max, axisLabel: { color: text, fontSize: 10 }, splitLine: { lineStyle: { color: border } } },
              series: series.map((s, i) => ({ name: s.name, type: "line", data: s.data, showSymbol: false, connectNulls: false, lineStyle: { width: 2, color: colors[i % colors.length], type: i < colors.length ? "solid" : "dashed" }, itemStyle: { color: colors[i % colors.length] } })),
            }} />
          </div>
        )}
        <div className="resource-legend">{series.map((s, i) => <span key={s.name}><i style={{ borderTop: `3px ${i < colors.length ? "solid" : "dashed"} ${colors[i % colors.length]}` }} />{s.name}</span>)}</div>
      </div>
    </section>
  );
}
