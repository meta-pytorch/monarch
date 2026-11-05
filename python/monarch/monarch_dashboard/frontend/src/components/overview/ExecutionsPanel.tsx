/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React, { useEffect, useState } from "react";
import type { Execution, ExecutionDetail, ExecutionPage } from "../../types";
import { useApi } from "../../lib/useApi";
import { formatDateTime } from "../../lib/format";
import { executionDuration, EXECUTION_STATUS } from "../../lib/executions";
import { Drawer, ErrorState, Loading } from "../common/ui";

function ExecutionBadge({ execution }: { execution: Execution }) {
  const meta = EXECUTION_STATUS[execution.status] ?? EXECUTION_STATUS.unknown;
  return (
    <span
      className={`execution-badge ${execution.status}`}
      style={{ "--execution-color": meta.color } as React.CSSProperties}
    >
      <i aria-hidden="true" />
      {meta.label}
    </span>
  );
}

function ExecutionRow({
  execution,
  onSelect,
}: {
  execution: Execution;
  onSelect: (id: string) => void;
}) {
  return (
    <button
      className="execution-row"
      onClick={() => onSelect(execution.id)}
    >
      <ExecutionBadge execution={execution} />
      <span className="execution-command mono" title={execution.command}>
        {execution.command}
        {(execution.runtime_faults ?? 0) > 0 && <span className="execution-runtime-fault" title="Command runtime actor failure"> · Runtime fault</span>}
      </span>
      <time
        className="execution-time"
        dateTime={new Date(execution.started_us / 1000).toISOString()}
      >
        {formatDateTime(execution.started_us)}
      </time>
      <span className="execution-duration mono">
        {executionDuration(execution)}
      </span>
      <span className="execution-exit mono">
        {execution.returncode == null ? "—" : `exit ${execution.returncode}`}
      </span>
      <span className="execution-arrow" aria-hidden="true">
        ›
      </span>
    </button>
  );
}

function ExecutionDetails({
  id,
  onClose,
}: {
  id: string;
  onClose: () => void;
}) {
  const [offset, setOffset] = useState(0);
  const { data, error, loading } = useApi<ExecutionDetail>(
    `/executions/${id}?rank_offset=${offset}`,
  );
  return (
    <Drawer
      title="Execution details"
      subtitle={data?.command}
      onClose={onClose}
    >
      {error ? (
        <ErrorState message={error} />
      ) : loading && !data ? (
        <Loading />
      ) : (
        data && (
          <div className="execution-detail">
            <div className="execution-detail-summary">
              <ExecutionBadge execution={data} />
              <span className="mono">{executionDuration(data)}</span>
              {data.returncode != null && (
                <span className="mono">exit {data.returncode}</span>
              )}
            </div>
            <pre className="execution-full-command">{data.command}</pre>
            <dl className="execution-metadata">
              <dt>Started</dt>
              <dd>{formatDateTime(data.started_us)}</dd>
              {data.finished_us != null && (
                <>
                  <dt>Finished</dt>
                  <dd>{formatDateTime(data.finished_us)}</dd>
                </>
              )}
              {data.workdir && (
                <>
                  <dt>Working directory</dt>
                  <dd className="mono">{data.workdir}</dd>
                </>
              )}
              {data.output_dir && (
                <>
                  <dt>Worker output directory</dt>
                  <dd className="mono">{data.output_dir}</dd>
                </>
              )}
            </dl>
            {data.error && (
              <div className="execution-error" role="status">
                {data.error}
              </div>
            )}
            {(data.runtime_faults ?? 0) > 0 && <div className="execution-error">
              <strong>{data.runtime_faults} runtime actor failure{data.runtime_faults === 1 ? "" : "s"}</strong>
              <p>The command outcome above records its actual result. Runtime faults remain visible here.</p>
              {(data.runtime_actor_failures ?? []).map((fault) => <p key={fault.actor_id} className="mono">{fault.full_name}: {fault.reason ?? "Actor failed"}</p>)}
            </div>}
            <h4>
              Rank outcomes <span className="count-tag">{data.rank_count}</span>
            </h4>
            {data.rank_count === 0 && (
              <p className="execution-note">
                {data.status === "running"
                  ? "Rank results appear as execution completes."
                  : "No rank outcomes were reported. Check the command output and controller logs."}
              </p>
            )}
            {data.ranks.map((rank) => (
              <div className="execution-rank" key={rank.seq}>
                <div className="execution-rank-head">
                  <span className="mono">
                    {rank.mesh} · {rank.rank}
                  </span>
                  <span
                    className={`execution-rank-exit mono${rank.returncode ? " failed" : ""}`}
                  >
                    exit {rank.returncode}
                  </span>
                </div>

              </div>
            ))}
            {data.rank_count > 100 && (
              <div className="execution-pagination">
                <button
                  className="btn sm"
                  disabled={offset === 0}
                  onClick={() => setOffset(offset - 100)}
                >
                  Previous ranks
                </button>
                <span>
                  {offset + 1}–{offset + data.ranks.length} of {data.rank_count}
                </span>
                <button
                  className="btn sm"
                  disabled={offset + 100 >= data.rank_count}
                  onClick={() => setOffset(offset + 100)}
                >
                  Next ranks
                </button>
              </div>
            )}
            {data.output_dir && (
              <p className="execution-note">
                Redirected stdout and stderr are written to the worker output
                directory shown above. If a GatherMount covers that directory,
                use it to access gathered output.
              </p>
            )}
            {!data.output_dir && (
              <p className="execution-note">
                stdout and stderr were returned to the CLI session.
              </p>
            )}
          </div>
        )
      )}
    </Drawer>
  );
}

/** Independent command outcomes alongside fleet health, with bounded history. */
export function ExecutionsPanel({
  compact = false,
  onViewAll,
}: {
  compact?: boolean;
  onViewAll?: () => void;
}) {
  const [selected, setSelected] = useState<string | null>(null);
  const [status, setStatus] = useState("");
  const [search, setSearch] = useState("");
  const [query, setQuery] = useState("");
  const [cursors, setCursors] = useState<string[]>([]);
  useEffect(() => {
    if (search === query) return;
    const timer = setTimeout(() => {
      setQuery(search);
      setCursors([]);
    }, 250);
    return () => clearTimeout(timer);
  }, [search, query]);
  const before = cursors[cursors.length - 1];
  const params = new URLSearchParams({ limit: compact ? "3" : "10" });
  if (status) params.set("status", status);
  if (query) params.set("search", query);
  if (before != null) params.set("before", String(before));
  const { data, error, loading } = useApi<ExecutionPage>(
    `/executions?${params}`,
  );
  const running = data?.counts.running ?? 0;
  const retainedFailures = data?.retained_failures ?? 0;
  return (
    <section
      className={`panel execution-panel${compact ? " compact" : ""}`}
      aria-label="Executions"
    >
      <div className="panel-head">
        <h3>Executions</h3>
        <span className="sub">commands &amp; jobs</span>
        <div className="spacer" />
        {compact && onViewAll && (
          <button className="btn sm" onClick={onViewAll}>
            View history <span aria-hidden="true">↗</span>
          </button>
        )}
      </div>
      {!compact && data?.available !== false && (
        <div className="execution-toolbar">
          <div
            className="execution-filters"
            role="group"
            aria-label="Filter execution history"
          >
            {[
              "",
              "failed",
              "succeeded",
              "interrupted",
              "unknown",
            ].map((value) => (
              <button
                key={value}
                className={`btn sm${status === value ? " active" : ""}`}
                aria-pressed={status === value}
                onClick={() => {
                  setStatus(value);
                  setCursors([]);
                }}
              >
                {value
                  ? EXECUTION_STATUS[value as Execution["status"]]
                      .label
                  : "All outcomes"}
              </button>
            ))}
          </div>
          <label className="execution-search">
            <span className="sr-only">Search commands</span>
            <input
              type="search"
              value={search}
              placeholder="Search commands…"
              onChange={(event) => setSearch(event.target.value)}
            />
          </label>
        </div>
      )}
      {error ? (
        <ErrorState message={error} />
      ) : loading && !data ? (
        <Loading label="Loading executions…" />
      ) : (
        data && (
          <>
            <div className="execution-summary" aria-live="polite">
              <span>
                <i className="execution-running-dot" aria-hidden="true" />
                <strong>{running}</strong> active
              </span>
              <span className={retainedFailures ? "has-failures" : ""}>
                <strong>{retainedFailures}</strong> failed in retained history
              </span>
              {(["unknown", "interrupted"] as const).map((state) => (data.counts[state] ?? 0) > 0 && <span key={state} className="needs-attention"><strong>{data.counts[state]}</strong> {state}</span>)}
              {(data.runtime_faulted ?? 0) > 0 && <span className="has-failures"><strong>{data.runtime_faulted}</strong> with runtime faults</span>}
              <span className="execution-summary-note">
                Execution outcomes are separate from fleet health.
              </span>
            </div>
            {!data.available ? (
              <div className="execution-empty">
                Execution recording is unavailable for this dashboard.
              </div>
            ) : (
              <>
                {running > 0 && (
                  <div className="execution-active">
                    <div className="execution-section-title">
                      Active executions
                    </div>
                    {(compact ? data.active.slice(0, 3) : data.active).map(
                      (execution) => (
                        <ExecutionRow
                          key={execution.id}
                          execution={execution}
                          onSelect={setSelected}
                        />
                      ),
                    )}
                    {running > (compact ? 3 : 100) && (
                      <p className="execution-note">
                        Showing {compact ? 3 : 100} of {running} active
                        executions.
                      </p>
                    )}
                  </div>
                )}
                <div className="execution-section-title">
                  {compact ? "Recent commands" : "Execution history"}
                </div>
                {data.history.length ? (
                  <div className="execution-history">
                    {data.history.map((execution) => (
                      <ExecutionRow
                        key={execution.id}
                        execution={execution}
                        onSelect={setSelected}
                      />
                    ))}
                  </div>
                ) : (
                  <div className="execution-empty">
                    {query || status
                      ? "No executions match these filters."
                      : "No completed executions yet. Commands will appear here when they finish."}
                  </div>
                )}
                {!compact && (
                  <div className="execution-pagination">
                    <button
                      className="btn sm"
                      disabled={!cursors.length}
                      onClick={() => setCursors(cursors.slice(0, -1))}
                    >
                      Newer
                    </button>
                    <span>
                      Page {cursors.length + 1} ·{" "}
                      {Object.values(data.counts).reduce(
                        (sum, count) => sum + (count ?? 0),
                        0,
                      )}{" "}
                      executions recorded
                    </span>
                    <button
                      className="btn sm"
                      disabled={data.next_before == null}
                      onClick={() =>
                        data.next_before != null &&
                        setCursors([...cursors, data.next_before])
                      }
                    >
                      Older
                    </button>
                  </div>
                )}
              </>
            )}
          </>
        )
      )}
      {selected && (
        <ExecutionDetails
          key={selected}
          id={selected}
          onClose={() => setSelected(null)}
        />
      )}
    </section>
  );
}
