export interface RevenuePoint {
  metric_hour: string;
  successful_payment_count: number;
  successful_revenue: number;
}

export interface PaymentPoint {
  metric_hour: string;
  payment_attempt_count: number;
  failed_payment_count: number;
  payment_failure_rate: number;
}

export interface ShipmentPoint {
  metric_hour: string;
  shipment_created_count: number;
  shipment_delay_signal_count: number;
}

export interface RefundPoint {
  metric_hour: string;
  refund_request_count: number;
  requested_refund_amount: number;
}

export interface OperationsPoint {
  metric_hour: string;
  event_count: number;
  average_processing_latency_ms: number | null;
  warehouse_updated_at: string;
}

export interface QualityPoint {
  metric_hour: string;
  orphan_payment_attempt_count: number;
  orphan_shipment_event_count: number;
}

export interface AnalyticsOverview {
  generated_at: string;
  hours: number;
  revenue: RevenuePoint[];
  payments: PaymentPoint[];
  shipments: ShipmentPoint[];
  refunds: RefundPoint[];
  operations: OperationsPoint[];
  quality: QualityPoint[];
}

export interface PipelineStatus {
  status: "fresh" | "stale" | "empty";
  warehouse_updated_at: string | null;
  minutes_since_update: number | null;
}

async function getJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(path, { signal });
  if (!response.ok) throw new Error(`Request failed (${response.status})`);
  return response.json() as Promise<T>;
}

export async function loadDashboard(signal?: AbortSignal) {
  const [overview, pipeline] = await Promise.all([
    getJson<AnalyticsOverview>("/api/metrics/overview?hours=24", signal),
    getJson<PipelineStatus>("/api/pipeline/status", signal),
  ]);
  return { overview, pipeline };
}
