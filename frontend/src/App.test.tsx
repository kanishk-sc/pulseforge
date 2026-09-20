import { fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import App from "./App";

const build = {
  build_id: "00000000-0000-0000-0000-000000000001",
  dbt_invocation_id: "dbt-test",
  published_at: "2026-09-20T12:00:00Z",
  source_max_event_ts: "2026-09-20T11:58:00Z",
  source_max_ingested_at: "2026-09-20T11:59:00Z",
  source_event_count: 42,
  age_seconds: 60,
  state: "fresh",
};

function response(points: object[] = [], state = "fresh") {
  return {
    state,
    interval_start: "2026-09-19T12:00:00Z",
    interval_end: "2026-09-20T12:00:00Z",
    interval_semantics: "[start,end)",
    build: { ...build, state },
    metric_definition: "Measured definition",
    points,
  };
}

function mockDashboard(state = "fresh") {
  const payloads = [
    response(
      [
        {
          metric_hour_utc: "2026-09-20T11:00:00Z",
          region_code: "us-east",
          payment_attempt_count: 20,
          successful_payment_count: 15,
          failed_payment_count: 5,
          failure_rate: "0.25",
          attempted_amount: "100.00",
          successful_payment_amount: "75.00",
          failed_payment_amount: "25.00",
        },
      ],
      state,
    ),
    response(
      [
        {
          metric_hour_utc: "2026-09-20T11:00:00Z",
          region_code: "us-east",
          successful_payment_count: 15,
          revenue_amount: "75.00",
        },
      ],
      state,
    ),
    response([], state),
    response([], state),
    response([], state),
    {
      state,
      build: { ...build, state },
      running_build_started_at: null,
      latest_failed_build_at: null,
      latest_failure_reason: null,
      note: "Serving the latest successful immutable publication.",
    },
    { items: [] },
  ];
  let index = 0;
  vi.stubGlobal(
    "fetch",
    vi.fn(() =>
      Promise.resolve({
        ok: true,
        json: () => Promise.resolve(payloads[index++ % payloads.length]),
      }),
    ),
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("operations dashboard", () => {
  it("shows loading, measured values, and honest empty states", async () => {
    mockDashboard();
    render(<App />);
    expect(screen.getByText("Loading published analytics…")).toBeInTheDocument();
    expect(await screen.findByText("$75.00")).toBeInTheDocument();
    expect(screen.getByText("25.0%")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Shipments" }));
    expect(screen.getByText("No trustworthy rows exist for this selection.")).toBeInTheDocument();
  });

  it("labels stale successful data", async () => {
    mockDashboard("stale");
    render(<App />);
    expect(await screen.findByText("Stale analytics.")).toBeInTheDocument();
    expect(screen.getByText(/last successful build remains visible/i)).toBeInTheDocument();
  });

  it("renders an unavailable state with retry", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve({
          ok: false,
          status: 503,
          json: () => Promise.resolve({ detail: "analytics_publication_unavailable" }),
        }),
      ),
    );
    render(<App />);
    expect(await screen.findByText("Data unavailable.")).toBeInTheDocument();
    expect(screen.getByText(/analytics publication unavailable/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
  });

  it("opens evidence when an overview incident is selected", async () => {
    const incident = {
      incident_id: "00000000-0000-0000-0000-000000000099",
      detector_name: "payment_failure_rate_increase",
      detector_version: "1.0.0",
      region_code: "ap-south",
      evaluation_start: "2026-09-20T11:00:00Z",
      evaluation_end: "2026-09-20T12:00:00Z",
      observed_metric: "1.000000",
      observed_denominator: 20,
      baseline_metric: "0.000000",
      threshold: "0.100000",
      severity: "critical",
      summary: "Payment failures exceeded the deterministic threshold",
      analytics_build_id: build.build_id,
      detected_at: "2026-09-20T12:01:00Z",
      status: "open",
      resolved_at: null,
    } as const;
    vi.stubGlobal(
      "fetch",
      vi.fn((request: string | URL | Request) => {
        const url = String(request);
        const payload = url.includes(`/incidents/${incident.incident_id}`)
          ? {
              ...incident,
              evidence: [
                {
                  source_event_id: "00000000-0000-0000-0000-000000000123",
                  evidence_role: "payment_attempt",
                  event_ts: "2026-09-20T11:05:00Z",
                },
              ],
            }
          : url.includes("/incidents?")
            ? { items: [incident] }
            : url.includes("/analytics/status")
              ? {
                  state: "fresh",
                  build,
                  running_build_started_at: null,
                  latest_failed_build_at: null,
                  latest_failure_reason: null,
                  note: "Published",
                }
              : response();
        return Promise.resolve({ ok: true, json: () => Promise.resolve(payload) });
      }),
    );
    render(<App />);
    const finding = await screen.findByRole("button", {
      name: /payment failure rate increase/i,
    });
    fireEvent.click(finding);
    expect(await screen.findByText("Evidence detail")).toBeInTheDocument();
    expect(screen.getByText("payment_attempt")).toBeInTheDocument();
  });
});
