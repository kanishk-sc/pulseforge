from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from pulseforge.product.detectors import (
    order_volume_candidate,
    rate_increase_candidate,
    volume_drop_candidate,
)

START = datetime(2026, 9, 20, 12, tzinfo=UTC)


def test_payment_rate_threshold_and_minimum_sample_size():
    assert (
        rate_increase_candidate(
            "payment_failure_rate_increase",
            "us-east",
            START,
            9,
            19,
            [Decimal("0.10")] * 6,
            min_denominator=20,
            min_baseline_windows=6,
            absolute_increase=Decimal("0.10"),
            multiplier=Decimal("1.5"),
        )
        is None
    )
    candidate = rate_increase_candidate(
        "payment_failure_rate_increase",
        "us-east",
        START,
        8,
        20,
        [Decimal("0.10")] * 6,
        min_denominator=20,
        min_baseline_windows=6,
        absolute_increase=Decimal("0.10"),
        multiplier=Decimal("1.5"),
    )
    assert candidate is not None
    assert candidate.observed_metric == Decimal("0.4")
    assert candidate.threshold == Decimal("0.20")


def test_rate_baseline_requires_complete_historical_windows():
    candidate = rate_increase_candidate(
        "shipment_delay_rate_increase",
        "eu-west",
        START,
        5,
        10,
        [Decimal("0.10")] * 5,
        min_denominator=10,
        min_baseline_windows=6,
        absolute_increase=Decimal("0.15"),
        multiplier=Decimal("1.5"),
    )
    assert candidate is None


def test_volume_drop_is_suppressed_without_sufficient_history():
    assert volume_drop_candidate("us-west", START, 0, [30] * 11) is None
    assert volume_drop_candidate("us-west", START, 0, [10] * 12) is None


def test_volume_drop_uses_history_only_and_has_stable_key():
    candidate = volume_drop_candidate("us-west", START, 10, [40] * 12)
    assert candidate is not None
    assert candidate.baseline_metric == Decimal("40")
    assert candidate.threshold == Decimal("20.0")
    build_id = UUID("00000000-0000-0000-0000-000000000001")
    assert candidate.uniqueness_key(build_id) == candidate.uniqueness_key(build_id)


def test_volume_drop_treats_missing_current_hour_as_zero_activity():
    historical = [{"order_count": 40} for _ in range(12)]
    candidate = order_volume_candidate("us-east", START, None, historical)

    assert candidate is not None
    assert candidate.observed_metric == Decimal("0")
    assert candidate.severity == "critical"
