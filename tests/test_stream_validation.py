import json
from datetime import UTC, datetime, timedelta

import pytest

from pulseforge.generator import EventGenerator
from pulseforge.streaming.validation import validate_payload
from pulseforge.streaming.warehouse import candidate_query, checked_table, stage_name


def test_valid_record_normalizes_without_losing_money():
    generator = EventGenerator()
    generator.next_event()
    event = generator.next_event()
    result = validate_payload(event.model_dump_json().encode(), datetime.now(UTC))
    assert result["error_code"] is None
    assert result["event"]["amount"] == event.amount
    assert result["event"]["event_id"] == str(event.event_id)


@pytest.mark.parametrize(
    "payload,code",
    [
        (b"{broken", "malformed_json"),
        (b"\xff", "malformed_json"),
        (None, "other_validation"),
        (b"[]", "other_validation"),
        (b"{}", "missing_event_id"),
        (b'{"metadata":{"bad":NaN}}', "malformed_json"),
        (b'{"event_id":"some-id","event_type":"bad"}', "invalid_event_type"),
        (b'{"event_id":"some-id","event_type":[]}', "invalid_event_type"),
    ],
)
def test_safe_rejection_codes(payload, code):
    result = validate_payload(payload, datetime.now(UTC))
    assert result["error_code"] == code
    assert result["event"] is None
    assert "some-id" not in result["error_detail"]


@pytest.mark.parametrize(
    "field,value,code",
    [
        ("schema_version", 99, "schema_validation"),
        ("event_id", "private-untrusted-value", "schema_validation"),
        ("status", "private-untrusted-value", "invalid_business_fields"),
    ],
)
def test_contract_failure_classification(field, value, code):
    record = EventGenerator().next_event().model_dump(mode="json")
    record[field] = value
    result = validate_payload(json.dumps(record).encode(), datetime.now(UTC))
    assert result["error_code"] == code
    assert "private-untrusted-value" not in result["error_detail"]


def test_validation_uses_archived_clock_on_replay():
    record = EventGenerator().next_event().model_dump(mode="json")
    archive_time = datetime.now(UTC) - timedelta(hours=1)
    result = validate_payload(json.dumps(record).encode(), archive_time)
    assert result["error_code"] == "invalid_business_fields"


@pytest.mark.parametrize("name", ["events; DROP TABLE events", "stage_x", "", "public.stage_123"])
def test_reject_unsafe_table_name(name):
    with pytest.raises(ValueError):
        checked_table(name)
    with pytest.raises(ValueError):
        candidate_query(name)


def test_staging_identity_is_stable_and_generation_specific():
    assert stage_name("a", 0) == stage_name("a", 0)
    assert stage_name("a", 0) != stage_name("b", 0)
    assert stage_name("a", 0) != stage_name("a", 1)
    assert len(stage_name("a", 0)) < 63
