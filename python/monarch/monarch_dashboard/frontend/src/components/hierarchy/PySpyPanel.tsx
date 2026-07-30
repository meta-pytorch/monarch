/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React, { useState } from "react";
import { Loading } from "../common/ui";
import { IconAlert, IconPyspy } from "../common/icons";

/* PySpyResult mirrors hyperactor_mesh's externally-tagged serde enum. */
interface PySpyFrame {
  name: string;
  filename: string;
  module?: string | null;
  short_filename?: string | null;
  line: number;
  is_entry: boolean;
}
interface PySpyThread {
  pid: number;
  thread_id: number;
  thread_name?: string | null;
  os_thread_id?: number | null;
  active: boolean;
  owns_gil: boolean;
  frames: PySpyFrame[];
}
type PySpyResult =
  | { Ok: { pid: number; binary: string; stack_traces: PySpyThread[]; warnings: string[] } }
  | { BinaryNotFound: { searched: string[] } }
  | { Failed: { pid: number; binary: string; exit_code?: number | null; stderr: string } };

type Phase = "idle" | "loading" | "done" | "error";

/**
 * On-demand py-spy stack dump for a proc. py-spy is proc-level, so this
 * profiles the whole Python process (every actor sharing it), not one actor.
 */
export function PySpyPanel({ procRef, procLabel }: { procRef: string; procLabel: string }) {
  const [phase, setPhase] = useState<Phase>("idle");
  const [result, setResult] = useState<PySpyResult | null>(null);
  const [err, setErr] = useState<string | null>(null);

  const run = async () => {
    setPhase("loading");
    setErr(null);
    setResult(null);
    try {
      const res = await fetch("/api/pyspy/capture", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ proc_ref: procRef }),
      });
      const j = await res.json();
      if (!res.ok || j.error) {
        setErr(j.error ?? `HTTP ${res.status}`);
        setPhase("error");
        return;
      }
      setResult(j.result as PySpyResult);
      setPhase("done");
    } catch (e) {
      setErr(String((e as Error)?.message ?? e));
      setPhase("error");
    }
  };

  return (
    <section className="drawer-section">
      <div className="eyebrow" style={{ display: "flex", alignItems: "center", gap: "var(--s2)" }}>
        Py-Spy
        <div className="spacer" />
        <button className="btn sm pyspy-btn" onClick={run} disabled={phase === "loading"}>
          <IconPyspy size={13} /> {phase === "loading" ? "Capturing…" : phase === "done" ? "Re-capture" : "PySpy Dump"}
        </button>
      </div>
      <div className="pyspy-note">
        Samples the whole proc <code>{procLabel}</code> — every actor sharing this Python process.
      </div>
      {phase === "loading" && <Loading label="Running py-spy…" />}
      {phase === "error" && (
        <div className="pyspy-banner err">
          <IconAlert size={14} /> Couldn’t reach the profiler — {err}
        </div>
      )}
      {phase === "done" && result && <ResultView result={result} />}
    </section>
  );
}

function ResultView({ result }: { result: PySpyResult }) {
  if ("Ok" in result) {
    const { pid, binary, stack_traces, warnings } = result.Ok;
    // Surface the interesting threads first: GIL holder, then active.
    const threads = [...stack_traces].sort(
      (a, b) => Number(b.owns_gil) - Number(a.owns_gil) || Number(b.active) - Number(a.active)
    );
    return (
      <div className="pyspy-result">
        <div className="pyspy-meta">
          <span className="pyspy-chip">pid <b>{pid}</b></span>
          <span className="pyspy-chip">{threads.length} thread{threads.length === 1 ? "" : "s"}</span>
          <span className="pyspy-chip mono">{binary}</span>
        </div>
        {warnings.length > 0 && (
          <div className="pyspy-banner warn">
            <IconAlert size={13} /> {warnings.join(" · ")}
          </div>
        )}
        {threads.length === 0 ? (
          <div className="state">No Python threads captured</div>
        ) : (
          threads.map((t) => <ThreadCard key={`${t.thread_id}-${t.os_thread_id}`} t={t} />)
        )}
      </div>
    );
  }
  if ("Failed" in result) {
    const { pid, binary, exit_code, stderr } = result.Failed;
    return (
      <div className="pyspy-result">
        <div className="pyspy-banner err">
          <IconAlert size={14} /> py-spy failed{exit_code != null ? ` (exit ${exit_code})` : ""} on pid {pid}
        </div>
        <div className="pyspy-meta"><span className="pyspy-chip mono">{binary}</span></div>
        {stderr && <pre className="pyspy-stderr">{stderr.trim()}</pre>}
      </div>
    );
  }
  // BinaryNotFound
  const { searched } = result.BinaryNotFound;
  return (
    <div className="pyspy-result">
      <div className="pyspy-banner warn">
        <IconAlert size={14} /> py-spy binary not found
      </div>
      {searched.length > 0 && (
        <pre className="pyspy-stderr">{["Searched:", ...searched].join("\n")}</pre>
      )}
    </div>
  );
}

function ThreadCard({ t }: { t: PySpyThread }) {
  const [open, setOpen] = useState(t.owns_gil || t.active);
  const title = t.thread_name || `Thread ${t.thread_id}`;
  return (
    <div className={`pyspy-thread${t.active ? " active" : ""}`}>
      <button className="pyspy-thread-head" onClick={() => setOpen((o) => !o)}>
        <span className={`pyspy-caret${open ? " open" : ""}`}>▸</span>
        <span className="pyspy-thread-name">{title}</span>
        {t.owns_gil && <span className="pyspy-badge gil">GIL</span>}
        <span className={`pyspy-badge ${t.active ? "on" : "off"}`}>{t.active ? "active" : "idle"}</span>
        <span className="spacer" />
        <span className="pyspy-frame-count">{t.frames.length} frames</span>
      </button>
      {open && (
        <ol className="pyspy-frames">
          {t.frames.map((f, i) => {
            const file = f.short_filename || f.filename.split("/").pop() || f.filename;
            const qual = f.module ? `${f.module}.${f.name}` : f.name;
            return (
              <li key={i} className={`pyspy-frame${f.is_entry ? " entry" : ""}${i === 0 ? " top" : ""}`}>
                <span className="pyspy-fn" title={qual}>{qual}</span>
                <span className="pyspy-loc mono" title={f.filename}>{file}:{f.line}</span>
              </li>
            );
          })}
        </ol>
      )}
    </div>
  );
}
