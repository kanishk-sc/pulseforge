import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  DashboardData,
  Incident,
  IncidentDetail,
  Explanation,
  Region,
  explainIncident,
  loadDashboard,
  loadIncident,
} from "./api";

type View =
  | "overview"
  | "payments"
  | "revenue"
  | "shipments"
  | "refunds"
  | "incidents"
  | "status";
const views: Array<{ id: View; label: string }> = [
  { id: "overview", label: "Overview" },
  { id: "payments", label: "Payment health" },
  { id: "revenue", label: "Revenue" },
  { id: "shipments", label: "Shipments" },
  { id: "refunds", label: "Refund requests" },
  { id: "incidents", label: "Incidents" },
  { id: "status", label: "Analytics status" },
];

const money = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" });
const integer = new Intl.NumberFormat("en-US");
const utc = new Intl.DateTimeFormat("en-US", {
  timeZone: "UTC",
  month: "short",
  day: "numeric",
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
});

function sum<T>(values: T[], select: (value: T) => number): number {
  return values.reduce((total, value) => total + select(value), 0);
}

function Empty({ children }: { children: string }) {
  return (
    <div className="empty" role="status">
      {children}
    </div>
  );
}

function MetricCard({ label, value, detail }: { label: string; value: string; detail: string }) {
  return (
    <article className="metric-card">
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{detail}</small>
    </article>
  );
}

interface Column {
  label: string;
  value: (row: Record<string, unknown>) => string;
}

function DataTable({
  title,
  definition,
  columns,
  rows,
}: {
  title: string;
  definition: string;
  columns: Column[];
  rows: Array<Record<string, unknown>>;
}) {
  const headingId = `${title.toLowerCase().replace(/[^a-z0-9]+/g, "-")}-heading`;
  return (
    <section className="panel" aria-labelledby={headingId}>
      <header>
        <div>
          <h2 id={headingId}>{title}</h2>
          <p>{definition}</p>
        </div>
        <span className="utc-label">All times UTC</span>
      </header>
      {rows.length === 0 ? (
        <Empty>No trustworthy rows exist for this selection.</Empty>
      ) : (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>{columns.map((column) => <th scope="col" key={column.label}>{column.label}</th>)}</tr>
            </thead>
            <tbody>
              {rows.map((row, index) => (
                <tr key={`${String(row.metric_hour_utc)}-${String(row.region_code)}-${index}`}>
                  {columns.map((column) => <td key={column.label}>{column.value(row)}</td>)}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

function IncidentList({
  incidents,
  onSelect,
}: {
  incidents: Incident[];
  onSelect: (incident: Incident) => void;
}) {
  if (!incidents.length) return <Empty>No detector incidents match this region.</Empty>;
  return (
    <div className="incident-list">
      {incidents.map((incident) => (
        <button
          className="incident-row"
          key={incident.incident_id}
          onClick={() => onSelect(incident)}
        >
          <span className={`severity ${incident.severity}`}>{incident.severity}</span>
          <strong>{incident.detector_name.replaceAll("_", " ")}</strong>
          <span>{incident.region_code}</span>
          <span>{utc.format(new Date(incident.detected_at))} UTC</span>
          <small>{incident.summary}</small>
        </button>
      ))}
    </div>
  );
}

function ExplanationPanel({ explanation }: { explanation: Explanation }) {
  const citationIndex = new Map(explanation.citations.map((item, index) => [item.citation_id, index + 1]));
  const sections = [
    ["Observed facts", explanation.facts],
    ["Supported interpretation", explanation.interpretations],
    ["Hypotheses to investigate", explanation.hypotheses],
    ["Diagnostic steps", explanation.diagnostic_steps],
    ["Missing or stale evidence", explanation.limitations],
  ] as const;
  return (
    <div className="explanation" aria-live="polite">
      <div className="explanation-heading"><h4>Incident explanation</h4><span className="offline-label">{explanation.label}</span></div>
      <p className="explanation-meta">Original build <code>{explanation.analytics_build_id}</code> · Published {explanation.original_build_published_at ? `${utc.format(new Date(explanation.original_build_published_at))} UTC` : "unavailable"} · Retrieved by {explanation.retrieval_mode.replaceAll("_", " ")} · Generated {utc.format(new Date(explanation.generated_at))} UTC</p>
      <p className="explanation-meta">Current build: {explanation.current_build_id ? <><code>{explanation.current_build_id}</code> · Published {explanation.current_build_published_at ? `${utc.format(new Date(explanation.current_build_published_at))} UTC` : "unavailable"}</> : "unavailable"} · Source references shown: {explanation.source_references_included} of {explanation.source_reference_count}</p>
      {sections.map(([title, statements]) => (
        <section key={title} aria-label={title}>
          <h5>{title}</h5>
          {statements.length ? <ul>{statements.map((item, index) => <li key={`${title}-${index}`}>{item.text}{" "}{item.citation_ids.map((id) => <a key={id} href={`#assistant-source-${citationIndex.get(id)}`} aria-label={`Source ${citationIndex.get(id)}`}>[{citationIndex.get(id)}]</a>)}</li>)}</ul> : <p>No supported {title.toLowerCase()} to report.</p>}
        </section>
      ))}
      <section aria-label="Explanation sources"><h5>Sources supplied to this explanation</h5><ol className="explanation-sources">{explanation.citations.map((item, index) => <li id={`assistant-source-${index + 1}`} key={item.citation_id}><strong>{item.label}</strong> <code>{item.citation_id}</code>{item.source_path && <span> · {item.source_path}{item.section_id ? ` # ${item.section_id}` : ""}</span>}{item.document_version && <small>Document SHA-256: {item.document_version}</small>}{item.excerpt && <blockquote>{item.excerpt}</blockquote>}</li>)}</ol></section>
    </div>
  );
}

export default function App() {
  const [view, setView] = useState<View>("overview");
  const [region, setRegion] = useState<Region | "all">("all");
  const [hours, setHours] = useState(24);
  const [data, setData] = useState<DashboardData | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const [revision, setRevision] = useState(0);
  const [selected, setSelected] = useState<IncidentDetail | null>(null);
  const detailController = useRef<AbortController | null>(null);
  const explainController = useRef<AbortController | null>(null);
  const [explanation, setExplanation] = useState<Explanation | null>(null);
  const [explainState, setExplainState] = useState<"idle" | "loading" | "error">("idle");
  const [explainError, setExplainError] = useState("");

  useEffect(() => {
    const controller = new AbortController();
    let timer: number | undefined;
    let delay = 30_000;
    setLoading(true);
    setData(null);
    setSelected(null);
    detailController.current?.abort();
    explainController.current?.abort();
    setExplanation(null);
    setExplainState("idle");
    const refresh = async () => {
      try {
        const result = await loadDashboard(hours, region, controller.signal);
        setData(result);
        setError("");
        setLoading(false);
        delay = 30_000;
      } catch (reason) {
        if ((reason as Error).name === "AbortError") return;
        setError((reason as Error).message);
        setLoading(false);
        delay = Math.min(delay * 2, 120_000);
      }
      if (!controller.signal.aborted) timer = window.setTimeout(refresh, delay);
    };
    void refresh();
    return () => {
      controller.abort();
      if (timer) window.clearTimeout(timer);
    };
  }, [hours, region, revision]);

  const selectIncident = useCallback(async (incident: Incident) => {
    detailController.current?.abort();
    explainController.current?.abort();
    setExplanation(null);
    setExplainState("idle");
    const controller = new AbortController();
    detailController.current = controller;
    setView("incidents");
    setSelected(null);
    try {
      setSelected(await loadIncident(incident.incident_id, controller.signal));
    } catch (reason) {
      if ((reason as Error).name === "AbortError") return;
      setError((reason as Error).message);
    }
  }, []);

  useEffect(() => () => detailController.current?.abort(), []);
  useEffect(() => () => explainController.current?.abort(), []);
  useEffect(() => {
    if (view !== "incidents") {
      explainController.current?.abort();
      setExplainState("idle");
    }
  }, [view]);

  const cancelExplanation = useCallback(() => {
    explainController.current?.abort();
    setExplainState("idle");
  }, []);

  const requestExplanation = useCallback(async () => {
    if (!selected) return;
    explainController.current?.abort();
    const controller = new AbortController();
    explainController.current = controller;
    setExplanation(null);
    setExplainError("");
    setExplainState("loading");
    try {
      const result = await explainIncident(selected.incident_id, controller.signal);
      if (result.incident_id !== selected.incident_id || result.analytics_build_id !== selected.analytics_build_id) {
        throw new Error("explanation evidence build mismatch");
      }
      setExplanation(result);
      setExplainState("idle");
    } catch (reason) {
      if (controller.signal.aborted || (reason as Error).name === "AbortError") return;
      setExplainError((reason as Error).message);
      setExplainState("error");
    }
  }, [selected]);

  const totals = useMemo(
    () =>
      data
        ? {
            revenue: sum(data.revenue.points, (point) => Number(point.revenue_amount)),
            payments: sum(data.payments.points, (point) => point.payment_attempt_count),
            failed: sum(data.payments.points, (point) => point.failed_payment_count),
            shipments: sum(data.shipments.points, (point) => point.shipment_created_count),
            delayed: sum(
              data.shipments.points.filter((point) => point.cohort_mature),
              (point) => point.delayed_shipment_count,
            ),
          }
        : null,
    [data],
  );

  const commonHour: Column = {
    label: "Hour",
    value: (row) => `${utc.format(new Date(String(row.metric_hour_utc)))} UTC`,
  };
  const commonRegion: Column = { label: "Region", value: (row) => String(row.region_code) };

  return (
    <div className="app-shell">
      <a className="skip-link" href="#main-content">Skip to main content</a>
      <aside>
        <div className="brand">
          <span className="brand-mark">PF</span>
          <div><strong>PulseForge</strong><small>Operations product</small></div>
        </div>
        <nav aria-label="Product views">
          {views.map((item) => (
            <button
              key={item.id}
              aria-current={view === item.id ? "page" : undefined}
              onClick={() => setView(item.id)}
            >
              {item.label}
            </button>
          ))}
        </nav>
        <div className="synthetic"><span /> Synthetic commerce data</div>
      </aside>

      <main className="workspace" id="main-content" tabIndex={-1}>
        <header className="topbar">
          <div><span className="eyebrow">Build-aware analytics</span><h1>{views.find((item) => item.id === view)?.label}</h1></div>
          <div className="filters">
            <label>Region
              <select value={region} onChange={(event) => setRegion(event.target.value as Region | "all")}>
                <option value="all">All regions</option><option>us-east</option><option>us-west</option><option>eu-west</option><option>ap-south</option>
              </select>
            </label>
            <label>Window
              <select value={hours} onChange={(event) => setHours(Number(event.target.value))}>
                <option value={6}>6 hours</option><option value={24}>24 hours</option><option value={168}>7 days</option>
              </select>
            </label>
            <button className="refresh" onClick={() => setRevision((value) => value + 1)}>Refresh</button>
          </div>
        </header>

        {data?.quality.state === "stale" && (
          <div className="banner stale" role="alert">
            <strong>Stale analytics.</strong> The last successful build remains visible; detectors will not evaluate this snapshot.
          </div>
        )}
        {error && (
          <div className="banner error" role="alert">
            <strong>Data unavailable.</strong> {error}. Any retained data remains labeled with its build.
            <button onClick={() => setRevision((value) => value + 1)}>Retry</button>
          </div>
        )}

        {loading && !data ? (
          <div className="loading" role="status"><span />Loading published analytics…</div>
        ) : data && totals ? (
          <>
            {view === "overview" && (
              <>
                <section className="metrics" aria-label="Selected interval totals">
                  <MetricCard label="Successful revenue" value={money.format(totals.revenue)} detail="Successful payments only" />
                  <MetricCard label="Payment failure rate" value={totals.payments ? `${((totals.failed / totals.payments) * 100).toFixed(1)}%` : "—"} detail={totals.payments ? `${integer.format(totals.failed)} of ${integer.format(totals.payments)} attempts` : "Denominator unavailable"} />
                  <MetricCard label="Mature delayed shipments" value={integer.format(totals.delayed)} detail={`${integer.format(totals.shipments)} creation cohorts`} />
                  <MetricCard label="Open incidents" value={integer.format(data.incidents.filter((item) => item.status === "open").length)} detail="Deterministic detector findings" />
                </section>
                <section className="overview-grid">
                  <div className="panel"><header><div><h2>Trust context</h2><p>The dashboard reads one immutable successful build.</p></div></header><dl>
                    <div><dt>State</dt><dd className={`state ${data.quality.state}`}>{data.quality.state}</dd></div>
                    <div><dt>Build</dt><dd><code>{data.quality.build?.build_id.slice(0, 8) ?? "none"}</code></dd></div>
                    <div><dt>Published</dt><dd>{data.quality.build ? `${utc.format(new Date(data.quality.build.published_at))} UTC` : "Unavailable"}</dd></div>
                    <div><dt>Source events</dt><dd>{integer.format(data.quality.build?.source_event_count ?? 0)}</dd></div>
                  </dl></div>
                  <div className="panel"><header><div><h2>Recent incidents</h2><p>Open evidence-backed findings, newest first.</p></div></header><IncidentList incidents={data.incidents.slice(0, 4)} onSelect={selectIncident} /></div>
                </section>
              </>
            )}
            {view === "payments" && <DataTable title="Payment health" definition={data.payments.metric_definition} rows={data.payments.points as unknown as Array<Record<string, unknown>>} columns={[commonHour, commonRegion, { label: "Attempts", value: (row) => integer.format(Number(row.payment_attempt_count)) }, { label: "Failures", value: (row) => integer.format(Number(row.failed_payment_count)) }, { label: "Failure rate", value: (row) => row.failure_rate == null ? "— (no denominator)" : `${(Number(row.failure_rate) * 100).toFixed(1)}%` }, { label: "Attempted", value: (row) => money.format(Number(row.attempted_amount)) }]} />}
            {view === "revenue" && <DataTable title="Revenue" definition={data.revenue.metric_definition} rows={data.revenue.points as unknown as Array<Record<string, unknown>>} columns={[commonHour, commonRegion, { label: "Successful payments", value: (row) => integer.format(Number(row.successful_payment_count)) }, { label: "Revenue", value: (row) => money.format(Number(row.revenue_amount)) }]} />}
            {view === "shipments" && <DataTable title="Shipment health" definition={data.shipments.metric_definition} rows={data.shipments.points as unknown as Array<Record<string, unknown>>} columns={[{ ...commonHour, label: "Creation cohort" }, commonRegion, { label: "Created", value: (row) => integer.format(Number(row.shipment_created_count)) }, { label: "Delayed", value: (row) => integer.format(Number(row.delayed_shipment_count)) }, { label: "Rate", value: (row) => row.delayed_shipment_rate == null ? "—" : `${(Number(row.delayed_shipment_rate) * 100).toFixed(1)}%` }, { label: "Maturity", value: (row) => row.cohort_mature ? "Mature" : "Immature — may change" }]} />}
            {view === "refunds" && <DataTable title="Refund requests" definition={data.refunds.metric_definition} rows={data.refunds.points as unknown as Array<Record<string, unknown>>} columns={[commonHour, commonRegion, { label: "Requests", value: (row) => integer.format(Number(row.refund_request_count)) }, { label: "Requested amount", value: (row) => money.format(Number(row.requested_amount)) }]} />}
            {view === "incidents" && (
              <section className="incident-layout">
                <div className="panel"><header><div><h2>Detector incidents</h2><p>Stale or incomplete builds are never classified as business anomalies.</p></div></header><IncidentList incidents={data.incidents} onSelect={selectIncident} /></div>
                <div className="panel detail"><header><div><h2>Evidence detail</h2><p>Source lineage retained with every finding.</p></div></header>
                  {selected ? <><h3>{selected.summary}</h3><dl><div><dt>Observed</dt><dd>{selected.observed_metric}{selected.observed_denominator ? ` / ${selected.observed_denominator}` : ""}</dd></div><div><dt>Baseline</dt><dd>{selected.baseline_metric}</dd></div><div><dt>Threshold</dt><dd>{selected.threshold}</dd></div><div><dt>Build</dt><dd><code>{selected.analytics_build_id.slice(0, 8)}</code></dd></div></dl><h4>Source events</h4>{selected.evidence.length ? <ul className="evidence">{selected.evidence.map((item) => <li key={`${item.source_event_id}-${item.evidence_role}`}><code>{item.source_event_id}</code><span>{item.evidence_role}</span><time>{utc.format(new Date(item.event_ts))} UTC</time></li>)}</ul> : <Empty>No source events were eligible for this finding.</Empty>}<div className="explain-action"><button onClick={() => void requestExplanation()} disabled={explainState === "loading"}>Explain this incident</button>{explainState === "loading" && <><span role="status">Building offline evidence summary…</span><button onClick={cancelExplanation}>Cancel explanation</button></>}</div>{explainState === "error" && <div className="banner error" role="alert">Explanation unavailable: {explainError}. <button onClick={() => void requestExplanation()}>Retry explanation</button></div>}{explanation && <ExplanationPanel explanation={explanation} />}</> : <Empty>Select an incident to inspect its threshold and source events.</Empty>}
                </div>
              </section>
            )}
            {view === "status" && (
              <section className="panel status-panel"><header><div><h2>Analytics publication</h2><p>{data.quality.note}</p></div><span className={`state ${data.quality.state}`}>{data.quality.state}</span></header><dl>
                <div><dt>Successful build</dt><dd><code>{data.quality.build?.build_id ?? "Unavailable"}</code></dd></div>
                <div><dt>dbt invocation</dt><dd><code>{data.quality.build?.dbt_invocation_id ?? "Unavailable"}</code></dd></div>
                <div><dt>Published</dt><dd>{data.quality.build ? `${utc.format(new Date(data.quality.build.published_at))} UTC` : "Unavailable"}</dd></div>
                <div><dt>Source watermark</dt><dd>{data.quality.build?.source_max_event_ts ? `${utc.format(new Date(data.quality.build.source_max_event_ts))} UTC` : "No source events"}</dd></div>
                <div><dt>Running build</dt><dd>{data.quality.running_build_started_at ? `${utc.format(new Date(data.quality.running_build_started_at))} UTC (not served)` : "None"}</dd></div>
                <div><dt>Latest failed build</dt><dd>{data.quality.latest_failed_build_at ? `${utc.format(new Date(data.quality.latest_failed_build_at))} UTC — ${data.quality.latest_failure_reason ?? "No reason recorded"}` : "None"}</dd></div>
              </dl><div className="honesty"><strong>Pipeline status boundary</strong><p>Spark health is intentionally not inferred from mart timestamps. This product reports publication state only.</p></div></section>
            )}
          </>
        ) : null}
        <footer>UTC intervals use <code>[start,end)</code>. Build identity is included in every metric response and cache key.</footer>
      </main>
    </div>
  );
}
