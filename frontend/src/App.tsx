import { useEffect, useState } from "react";
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import {
  AnalyticsOverview,
  OperationsPoint,
  PaymentPoint,
  PipelineStatus,
  QualityPoint,
  RefundPoint,
  RevenuePoint,
  ShipmentPoint,
  loadDashboard,
} from "./api";

type DashboardData = { overview: AnalyticsOverview; pipeline: PipelineStatus };

const money = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  maximumFractionDigits: 0,
});

function latest<T>(values: T[]): T | undefined {
  return values.at(-1);
}

function hour(value: string): string {
  return new Date(value).toLocaleTimeString([], { hour: "numeric" });
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

interface ChartProps {
  title: string;
  description: string;
  data: Array<
    RevenuePoint | PaymentPoint | ShipmentPoint | RefundPoint | OperationsPoint | QualityPoint
  >;
  lines: Array<{ key: string; label: string; color: string }>;
  percent?: boolean;
}

function MetricChart({ title, description, data, lines, percent = false }: ChartProps) {
  return (
    <article className="chart-card">
      <header>
        <div>
          <h2>{title}</h2>
          <p>{description}</p>
        </div>
        <div className="legend">
          {lines.map((line) => (
            <span key={line.key} style={{ "--legend": line.color } as React.CSSProperties}>
              {line.label}
            </span>
          ))}
        </div>
      </header>
      {data.length === 0 ? (
        <div className="empty">No modeled rows in this 24-hour window.</div>
      ) : (
        <div className="chart">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={data} margin={{ top: 12, right: 12, left: 0, bottom: 0 }}>
              <CartesianGrid stroke="#18304a" vertical={false} />
              <XAxis dataKey="metric_hour" tickFormatter={hour} stroke="#8194aa" />
              <YAxis
                stroke="#8194aa"
                tickFormatter={(value) => (percent ? `${Math.round(value * 100)}%` : value)}
              />
              <Tooltip
                labelFormatter={(value) => new Date(String(value)).toLocaleString()}
                contentStyle={{ background: "#0c1a2b", border: "1px solid #27415d" }}
              />
              {lines.map((line) => (
                <Line
                  key={line.key}
                  type="monotone"
                  dataKey={line.key}
                  name={line.label}
                  stroke={line.color}
                  strokeWidth={2.5}
                  dot={false}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}
    </article>
  );
}

export default function App() {
  const [data, setData] = useState<DashboardData | null>(null);
  const [error, setError] = useState("");
  const [reload, setReload] = useState(0);

  useEffect(() => {
    const controller = new AbortController();
    setError("");
    loadDashboard(controller.signal)
      .then(setData)
      .catch((reason: Error) => {
        if (reason.name !== "AbortError") setError(reason.message);
      });
    return () => controller.abort();
  }, [reload]);

  if (error) {
    return (
      <main className="centered-state">
        <span className="eyebrow">PulseForge operations</span>
        <h1>Warehouse data is unavailable</h1>
        <p>{error}. Confirm dbt has built the analytics marts, then retry.</p>
        <button onClick={() => setReload((value) => value + 1)}>Retry</button>
      </main>
    );
  }

  if (!data) {
    return (
      <main className="centered-state">
        <div className="pulse" />
        <p>Loading measured warehouse signals…</p>
      </main>
    );
  }

  const { overview, pipeline } = data;
  const revenue = latest(overview.revenue);
  const payment = latest(overview.payments);
  const shipment = latest(overview.shipments);
  const operations = latest(overview.operations);
  const quality = latest(overview.quality);

  return (
    <main className="shell">
      <header className="topbar">
        <div>
          <span className="eyebrow">Synthetic commerce telemetry</span>
          <h1>Operations control room</h1>
          <p>Measured Spark and dbt outputs. No generated dashboard values.</p>
        </div>
        <div className={`status ${pipeline.status}`}>
          <span /> Warehouse {pipeline.status}
          {pipeline.minutes_since_update !== null && (
            <small>{pipeline.minutes_since_update.toFixed(1)}m since update</small>
          )}
        </div>
      </header>

      <section className="metrics" aria-label="Latest metrics">
        <MetricCard
          label="Successful revenue"
          value={money.format(revenue?.successful_revenue ?? 0)}
          detail={`${revenue?.successful_payment_count ?? 0} successful payments`}
        />
        <MetricCard
          label="Payment failure rate"
          value={`${((payment?.payment_failure_rate ?? 0) * 100).toFixed(1)}%`}
          detail={`${payment?.failed_payment_count ?? 0} of ${payment?.payment_attempt_count ?? 0} attempts`}
        />
        <MetricCard
          label="Shipment delay signals"
          value={String(shipment?.shipment_delay_signal_count ?? 0)}
          detail={`${shipment?.shipment_created_count ?? 0} created shipments`}
        />
        <MetricCard
          label="Event throughput"
          value={String(operations?.event_count ?? 0)}
          detail="events in latest modeled hour"
        />
      </section>

      <section className="charts">
        <MetricChart
          title="Revenue trend"
          description="Successful payment amounts only"
          data={overview.revenue}
          lines={[{ key: "successful_revenue", label: "Revenue", color: "#43d9ad" }]}
        />
        <MetricChart
          title="Payment health"
          description="Failures divided by payment attempts"
          data={overview.payments}
          lines={[{ key: "payment_failure_rate", label: "Failure rate", color: "#ff8066" }]}
          percent
        />
        <MetricChart
          title="Shipment health"
          description="Created shipments and observed delay signals"
          data={overview.shipments}
          lines={[
            { key: "shipment_created_count", label: "Created", color: "#64a8ff" },
            { key: "shipment_delay_signal_count", label: "Delay signals", color: "#ffbd59" },
          ]}
        />
        <MetricChart
          title="Refund activity"
          description="Requests are not represented as settled refunds"
          data={overview.refunds}
          lines={[{ key: "refund_request_count", label: "Requests", color: "#d88cff" }]}
        />
        <MetricChart
          title="Event throughput"
          description="Validated events modeled by hour"
          data={overview.operations}
          lines={[{ key: "event_count", label: "Events", color: "#43d9ad" }]}
        />
        <MetricChart
          title="Data quality"
          description="Orphan signals retained from deliberate corruption"
          data={overview.quality}
          lines={[
            { key: "orphan_payment_attempt_count", label: "Payments", color: "#ff8066" },
            { key: "orphan_shipment_event_count", label: "Shipments", color: "#ffbd59" },
          ]}
        />
      </section>

      <footer>
        Generated from the last {overview.hours} hours at {new Date(overview.generated_at).toLocaleString()}.
        {quality && " Quality findings remain visible rather than filtered."}
      </footer>
    </main>
  );
}
