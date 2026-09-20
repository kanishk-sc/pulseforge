export type DataState = "fresh" | "stale" | "empty";
export type Region = "us-east" | "us-west" | "eu-west" | "ap-south";

export interface BuildContext {
  build_id: string;
  dbt_invocation_id: string;
  published_at: string;
  source_max_event_ts: string | null;
  source_max_ingested_at: string | null;
  source_event_count: number;
  age_seconds: number;
  state: DataState;
}

export interface MetricResponse<T extends object> {
  state: DataState;
  interval_start: string;
  interval_end: string;
  interval_semantics: "[start,end)";
  build: BuildContext;
  metric_definition: string;
  points: T[];
}

export interface PaymentPoint {
  metric_hour_utc: string;
  region_code: Region;
  payment_attempt_count: number;
  successful_payment_count: number;
  failed_payment_count: number;
  failure_rate: string | null;
  attempted_amount: string;
  successful_payment_amount: string;
  failed_payment_amount: string;
}

export interface RevenuePoint {
  metric_hour_utc: string;
  region_code: Region;
  successful_payment_count: number;
  revenue_amount: string;
}

export interface ShipmentPoint {
  metric_hour_utc: string;
  region_code: Region;
  shipment_created_count: number;
  delayed_shipment_count: number;
  shipment_delay_event_count: number;
  delayed_shipment_rate: string | null;
  cohort_mature: boolean;
}

export interface RefundPoint {
  metric_hour_utc: string;
  region_code: Region;
  refund_request_count: number;
  requested_amount: string;
}

export interface OperationsPoint {
  metric_hour_utc: string;
  region_code: Region;
  order_count: number;
  payment_attempt_count: number;
  failed_payment_count: number;
  payment_failure_rate: string | null;
  shipment_created_count: number;
  delayed_shipment_count: number;
  delayed_shipment_rate: string | null;
  refund_request_count: number;
  refund_requests_per_order: string | null;
}

export interface QualityStatus {
  state: DataState | "unavailable";
  build: BuildContext | null;
  running_build_started_at: string | null;
  latest_failed_build_at: string | null;
  latest_failure_reason: string | null;
  note: string;
}

export interface Incident {
  incident_id: string;
  detector_name: string;
  detector_version: string;
  region_code: Region;
  evaluation_start: string;
  evaluation_end: string;
  observed_metric: string;
  observed_denominator: number | null;
  baseline_metric: string;
  threshold: string;
  severity: "warning" | "critical";
  summary: string;
  analytics_build_id: string;
  detected_at: string;
  status: "open" | "resolved";
  resolved_at: string | null;
}

export interface IncidentDetail extends Incident {
  evidence: Array<{ source_event_id: string; evidence_role: string; event_ts: string }>;
}

export interface DashboardData {
  payments: MetricResponse<PaymentPoint>;
  revenue: MetricResponse<RevenuePoint>;
  shipments: MetricResponse<ShipmentPoint>;
  refunds: MetricResponse<RefundPoint>;
  operations: MetricResponse<OperationsPoint>;
  quality: QualityStatus;
  incidents: Incident[];
}

async function getJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(path, { signal, headers: { Accept: "application/json" } });
  if (!response.ok) {
    let reason = `request failed (${response.status})`;
    try {
      const body = (await response.json()) as { detail?: string };
      if (body.detail) reason = body.detail.replaceAll("_", " ");
    } catch {
      // Preserve the HTTP status when an upstream proxy returns non-JSON.
    }
    throw new Error(reason);
  }
  return response.json() as Promise<T>;
}

export function metricQuery(hours: number, region: Region | "all"): string {
  const end = new Date();
  const start = new Date(end.getTime() - hours * 60 * 60 * 1000);
  const params = new URLSearchParams({ start: start.toISOString(), end: end.toISOString() });
  if (region !== "all") params.set("region", region);
  return params.toString();
}

export async function loadDashboard(
  hours: number,
  region: Region | "all",
  signal?: AbortSignal,
): Promise<DashboardData> {
  const query = metricQuery(hours, region);
  const incidentParams = new URLSearchParams({ status: "open" });
  if (region !== "all") incidentParams.set("region", region);
  const [payments, revenue, shipments, refunds, operations, quality, incidentList] =
    await Promise.all([
      getJson<MetricResponse<PaymentPoint>>(`/api/v1/payment-health?${query}`, signal),
      getJson<MetricResponse<RevenuePoint>>(`/api/v1/revenue?${query}`, signal),
      getJson<MetricResponse<ShipmentPoint>>(`/api/v1/shipment-health?${query}`, signal),
      getJson<MetricResponse<RefundPoint>>(`/api/v1/refund-requests?${query}`, signal),
      getJson<MetricResponse<OperationsPoint>>(`/api/v1/operations-health?${query}`, signal),
      getJson<QualityStatus>("/api/v1/analytics/status", signal),
      getJson<{ items: Incident[] }>(`/api/v1/incidents?${incidentParams}`, signal),
    ]);
  return {
    payments,
    revenue,
    shipments,
    refunds,
    operations,
    quality,
    incidents: incidentList.items,
  };
}

export function loadIncident(incidentId: string, signal?: AbortSignal): Promise<IncidentDetail> {
  return getJson<IncidentDetail>(`/api/v1/incidents/${incidentId}`, signal);
}
