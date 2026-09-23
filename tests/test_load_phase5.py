import importlib.util
from pathlib import Path
from types import SimpleNamespace


def load_module():
    path = Path(__file__).resolve().parents[1] / "scripts/load_phase5.py"
    spec = importlib.util.spec_from_file_location("load_phase5", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_percentile_interpolates_and_handles_no_measurements():
    percentile = load_module().percentile
    assert percentile([], 0.95) is None
    assert percentile([1, 2, 3, 4, 5], 0.5) == 3
    assert percentile([1, 2, 3, 4, 5], 0.95) == 4.8


def test_clean_ingestion_environment_overrides_default_anomaly_rate(monkeypatch):
    monkeypatch.setenv("ANOMALY_RATE", "0.7")
    environment = load_module().producer_environment(10, 123)
    assert environment["EVENTS_PER_SECOND"] == "10"
    assert environment["GENERATOR_SEED"] == "123"
    assert environment["ANOMALY_RATE"] == "0"


def test_expected_ingestion_ids_are_deterministic_and_unique():
    ids = load_module().expected_event_ids(123, 20)
    assert len(ids) == len(set(ids)) == 20
    assert ids == load_module().expected_event_ids(123, 20)
    assert ids != load_module().expected_event_ids(124, 20)


def test_ingestion_does_not_accept_other_traffic_as_its_own(monkeypatch):
    module = load_module()
    row_counts = iter((10, 13))
    times = iter((0, 1, 2, 93, 94))
    monkeypatch.setattr(module, "warehouse_rows", lambda: next(row_counts))
    monkeypatch.setattr(module, "expected_event_ids", lambda _seed, _count: ["event-a"])
    monkeypatch.setattr(module, "warehouse_rows_for_event_ids", lambda _ids: 0)
    monkeypatch.setattr(
        module.subprocess, "run", lambda *_args, **_kwargs: SimpleNamespace(returncode=0)
    )
    monkeypatch.setattr(module.time, "monotonic", lambda: next(times))
    outcome = module.ingestion_load(SimpleNamespace(events=1, rate=10))
    assert outcome["measured"]["new_committed_rows"] == 3
    assert outcome["measured"]["matching_run_event_rows"] == 0
    assert not outcome["measured"]["all_events_observed"]
    assert not module.acceptance_ok(outcome)


def test_scenario_acceptance_rejects_partial_results():
    acceptance_ok = load_module().acceptance_ok
    assert acceptance_ok(
        {"scenario": "warm", "measured": {"error_count": 0, "scenario_condition_met": True}}
    )
    assert not acceptance_ok(
        {"scenario": "fallback", "measured": {"error_count": 6, "scenario_condition_met": False}}
    )
    assert acceptance_ok({"scenario": "ingestion", "measured": {"all_events_observed": True}})
    assert not acceptance_ok({"scenario": "ingestion", "measured": {"all_events_observed": False}})
