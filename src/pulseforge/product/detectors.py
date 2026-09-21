import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from statistics import mean
from uuid import UUID, uuid4

import psycopg

DETECTOR_VERSION = "1.0.0"
COOLDOWN = timedelta(hours=6)


@dataclass(frozen=True)
class Candidate:
    detector_name: str
    region_code: str
    evaluation_start: datetime
    evaluation_end: datetime
    observed_metric: Decimal
    observed_denominator: int | None
    baseline_metric: Decimal
    threshold: Decimal
    severity: str
    summary: str
    evidence: tuple[tuple[UUID, datetime, str], ...] = ()

    def uniqueness_key(self, build_id: UUID) -> str:
        value = "|".join(
            (
                self.detector_name,
                DETECTOR_VERSION,
                self.region_code,
                self.evaluation_start.isoformat(),
                self.evaluation_end.isoformat(),
                str(build_id),
            )
        )
        return hashlib.sha256(value.encode()).hexdigest()


def rate_increase_candidate(
    detector_name: str,
    region: str,
    start: datetime,
    observed_numerator: int,
    observed_denominator: int,
    baseline_rates: list[Decimal],
    *,
    min_denominator: int,
    min_baseline_windows: int,
    absolute_increase: Decimal,
    multiplier: Decimal,
) -> Candidate | None:
    if observed_denominator < min_denominator or len(baseline_rates) < min_baseline_windows:
        return None
    observed = Decimal(observed_numerator) / Decimal(observed_denominator)
    baseline = sum(baseline_rates) / Decimal(len(baseline_rates))
    threshold = max(baseline + absolute_increase, baseline * multiplier)
    if observed < threshold:
        return None
    return Candidate(
        detector_name=detector_name,
        region_code=region,
        evaluation_start=start,
        evaluation_end=start + timedelta(hours=1),
        observed_metric=observed,
        observed_denominator=observed_denominator,
        baseline_metric=baseline,
        threshold=threshold,
        severity="critical" if observed >= threshold + absolute_increase else "warning",
        summary=f"{detector_name} observed {observed:.3f} versus baseline {baseline:.3f}",
    )


def count_spike_candidate(
    detector_name: str,
    region: str,
    start: datetime,
    observed: int,
    denominator: int,
    baseline_rates: list[Decimal],
    *,
    min_denominator: int = 20,
    min_baseline_windows: int = 6,
) -> Candidate | None:
    if denominator < min_denominator or len(baseline_rates) < min_baseline_windows:
        return None
    rate = Decimal(observed) / Decimal(denominator)
    baseline = sum(baseline_rates) / Decimal(len(baseline_rates))
    threshold = max(baseline * Decimal("2"), baseline + Decimal("0.05"))
    if rate < threshold:
        return None
    return Candidate(
        detector_name=detector_name,
        region_code=region,
        evaluation_start=start,
        evaluation_end=start + timedelta(hours=1),
        observed_metric=rate,
        observed_denominator=denominator,
        baseline_metric=baseline,
        threshold=threshold,
        severity="critical" if rate >= threshold * 2 else "warning",
        summary=f"Refund requests per order reached {rate:.3f} from baseline {baseline:.3f}",
    )


def volume_drop_candidate(
    region: str, start: datetime, observed: int, baseline_counts: list[int]
) -> Candidate | None:
    if len(baseline_counts) < 12:
        return None
    baseline = Decimal(str(mean(baseline_counts)))
    if baseline < 20:
        return None
    threshold = baseline * Decimal("0.5")
    if Decimal(observed) > threshold:
        return None
    return Candidate(
        detector_name="order_volume_drop",
        region_code=region,
        evaluation_start=start,
        evaluation_end=start + timedelta(hours=1),
        observed_metric=Decimal(observed),
        observed_denominator=None,
        baseline_metric=baseline,
        threshold=threshold,
        severity="critical" if Decimal(observed) <= baseline * Decimal("0.25") else "warning",
        summary=f"Order volume fell to {observed} from hourly baseline {baseline:.2f}",
    )


def order_volume_candidate(
    region: str,
    start: datetime,
    current: dict | None,
    historical: list[dict],
) -> Candidate | None:
    """Evaluate a missing current row as zero orders, not as missing detector input."""
    observed = current["order_count"] if current else 0
    return volume_drop_candidate(
        region,
        start,
        observed,
        [row["order_count"] for row in historical],
    )


def _rows(connection: psycopg.Connection, query: str, params: tuple) -> list[dict]:
    with connection.cursor(row_factory=psycopg.rows.dict_row) as cursor:
        cursor.execute(query, params)
        return list(cursor.fetchall())


def _evidence(
    connection: psycopg.Connection,
    build_id: UUID,
    evidence_kinds: tuple[str, ...],
    region: str,
    start: datetime,
    end: datetime,
) -> tuple[tuple[UUID, datetime, str], ...]:
    rows = _rows(
        connection,
        """SELECT source_event_id, event_ts, evidence_kind FROM product.analytics_evidence
           WHERE build_id=%s AND evidence_kind=ANY(%s) AND region_code=%s
             AND evaluation_ts>=%s AND evaluation_ts<%s
           ORDER BY event_ts, source_event_id, evidence_kind LIMIT 25""",
        (build_id, list(evidence_kinds), region, start, end),
    )
    return tuple((row["source_event_id"], row["event_ts"], row["evidence_kind"]) for row in rows)


def evaluate(
    connection: psycopg.Connection,
    now: datetime | None = None,
    stale_after_seconds: int = 7200,
) -> list[UUID]:
    now = (now or datetime.now(UTC)).astimezone(UTC)
    build = connection.execute(
        """SELECT build_id, published_at FROM product.analytics_builds
           WHERE status='succeeded' ORDER BY published_at DESC LIMIT 1"""
    ).fetchone()
    if build is None or now - build[1] > timedelta(seconds=stale_after_seconds):
        return []
    build_id = build[0]
    evaluation_start = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    candidates: list[Candidate] = []
    for region in ("us-east", "us-west", "eu-west", "ap-south"):
        payment = _rows(
            connection,
            """SELECT metric_hour_utc, failed_payment_count, payment_attempt_count, failure_rate
               FROM product.payment_health_hourly WHERE build_id=%s AND region_code=%s
                 AND metric_hour_utc >= %s AND metric_hour_utc < %s
               ORDER BY metric_hour_utc""",
            (
                build_id,
                region,
                evaluation_start - timedelta(hours=24),
                evaluation_start + timedelta(hours=1),
            ),
        )
        current = next((row for row in payment if row["metric_hour_utc"] == evaluation_start), None)
        baseline_rates = [
            row["failure_rate"]
            for row in payment
            if row["metric_hour_utc"] < evaluation_start and row["failure_rate"] is not None
        ]
        if current:
            candidate = rate_increase_candidate(
                "payment_failure_rate_increase",
                region,
                evaluation_start,
                current["failed_payment_count"],
                current["payment_attempt_count"],
                baseline_rates,
                min_denominator=20,
                min_baseline_windows=6,
                absolute_increase=Decimal("0.10"),
                multiplier=Decimal("1.5"),
            )
            if candidate:
                candidates.append(
                    Candidate(
                        **{
                            **candidate.__dict__,
                            "evidence": _evidence(
                                connection,
                                build_id,
                                ("payment_attempt",),
                                region,
                                evaluation_start,
                                evaluation_start + timedelta(hours=1),
                            ),
                        }
                    )
                )

        mature_start = evaluation_start - timedelta(hours=24)
        shipments = _rows(
            connection,
            """SELECT metric_hour_utc, delayed_shipment_count, shipment_created_count,
                      delayed_shipment_rate
               FROM product.shipment_health_hourly WHERE build_id=%s AND region_code=%s
                 AND metric_hour_utc >= %s AND metric_hour_utc < %s ORDER BY metric_hour_utc""",
            (
                build_id,
                region,
                mature_start - timedelta(hours=24),
                mature_start + timedelta(hours=1),
            ),
        )
        current_shipment = next(
            (row for row in shipments if row["metric_hour_utc"] == mature_start), None
        )
        shipment_baseline = [
            row["delayed_shipment_rate"]
            for row in shipments
            if row["metric_hour_utc"] < mature_start and row["delayed_shipment_rate"] is not None
        ]
        if current_shipment:
            candidate = rate_increase_candidate(
                "shipment_delay_rate_increase",
                region,
                mature_start,
                current_shipment["delayed_shipment_count"],
                current_shipment["shipment_created_count"],
                shipment_baseline,
                min_denominator=10,
                min_baseline_windows=6,
                absolute_increase=Decimal("0.15"),
                multiplier=Decimal("1.5"),
            )
            if candidate:
                candidates.append(
                    Candidate(
                        **{
                            **candidate.__dict__,
                            "evidence": _evidence(
                                connection,
                                build_id,
                                ("shipment_cohort", "shipment_delay"),
                                region,
                                mature_start,
                                mature_start + timedelta(hours=1),
                            ),
                        }
                    )
                )

        operations = _rows(
            connection,
            """SELECT metric_hour_utc, order_count, refund_request_count, refund_requests_per_order
               FROM product.operations_health_hourly WHERE build_id=%s AND region_code=%s
                 AND metric_hour_utc >= %s AND metric_hour_utc < %s ORDER BY metric_hour_utc""",
            (
                build_id,
                region,
                evaluation_start - timedelta(hours=24),
                evaluation_start + timedelta(hours=1),
            ),
        )
        current_ops = next(
            (row for row in operations if row["metric_hour_utc"] == evaluation_start), None
        )
        historical = [row for row in operations if row["metric_hour_utc"] < evaluation_start]
        if current_ops:
            refund_baseline = [
                row["refund_requests_per_order"]
                for row in historical
                if row["refund_requests_per_order"] is not None
            ]
            candidate = count_spike_candidate(
                "refund_request_spike",
                region,
                evaluation_start,
                current_ops["refund_request_count"],
                current_ops["order_count"],
                refund_baseline,
            )
            if candidate:
                candidates.append(
                    Candidate(
                        **{
                            **candidate.__dict__,
                            "evidence": _evidence(
                                connection,
                                build_id,
                                ("refund_request",),
                                region,
                                evaluation_start,
                                evaluation_start + timedelta(hours=1),
                            ),
                        }
                    )
                )
        candidate = order_volume_candidate(region, evaluation_start, current_ops, historical)
        if candidate:
            candidates.append(
                Candidate(
                    **{
                        **candidate.__dict__,
                        "evidence": _evidence(
                            connection,
                            build_id,
                            ("order",),
                            region,
                            evaluation_start,
                            evaluation_start + timedelta(hours=1),
                        ),
                    }
                )
            )

    created: list[UUID] = []
    with connection.transaction():
        for candidate in candidates:
            recent = connection.execute(
                """SELECT 1 FROM product.incidents WHERE detector_name=%s AND region_code=%s
                   AND status='open' AND detected_at >= %s LIMIT 1""",
                (candidate.detector_name, candidate.region_code, now - COOLDOWN),
            ).fetchone()
            if recent:
                continue
            incident_id = uuid4()
            result = connection.execute(
                """INSERT INTO product.incidents(
                   incident_id, uniqueness_key, detector_name, detector_version, region_code,
                   evaluation_start, evaluation_end, observed_metric, observed_denominator,
                   baseline_metric, threshold, severity, summary, analytics_build_id, detected_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (uniqueness_key) DO NOTHING RETURNING incident_id""",
                (
                    incident_id,
                    candidate.uniqueness_key(build_id),
                    candidate.detector_name,
                    DETECTOR_VERSION,
                    candidate.region_code,
                    candidate.evaluation_start,
                    candidate.evaluation_end,
                    candidate.observed_metric,
                    candidate.observed_denominator,
                    candidate.baseline_metric,
                    candidate.threshold,
                    candidate.severity,
                    candidate.summary,
                    build_id,
                    now,
                ),
            ).fetchone()
            if not result:
                continue
            created.append(incident_id)
            with connection.cursor() as cursor:
                cursor.executemany(
                    """INSERT INTO product.incident_evidence(
                       incident_id, source_event_id, event_ts, evidence_role)
                       VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                    [
                        (incident_id, event_id, event_ts, role)
                        for event_id, event_ts, role in candidate.evidence
                    ],
                )
    return created
