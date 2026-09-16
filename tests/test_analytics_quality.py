import json

import pytest

from scripts.summarize_dbt_run import summarize


def write_results(tmp_path, statuses):
    path = tmp_path / "run_results.json"
    path.write_text(
        json.dumps(
            {
                "metadata": {"invocation_id": "test-invocation"},
                "results": [{"status": status} for status in statuses],
            }
        ),
        encoding="utf-8",
    )
    return path


def test_summarize_dbt_results_counts_statuses(tmp_path):
    summary = summarize(write_results(tmp_path, ["success", "pass", "pass"]))
    assert summary == {
        "invocation_id": "test-invocation",
        "result_count": 3,
        "statuses": {"pass": 2, "success": 1},
        "failed": 0,
    }


def test_summarize_dbt_results_counts_failures(tmp_path):
    summary = summarize(write_results(tmp_path, ["success", "fail", "error"]))
    assert summary["failed"] == 2


def test_summarize_rejects_missing_results(tmp_path):
    path = tmp_path / "run_results.json"
    path.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="results list"):
        summarize(path)


@pytest.mark.parametrize("statuses", [[], ["skipped"], ["warn"], ["unknown"]])
def test_summary_does_not_report_incomplete_build_as_success(tmp_path, statuses):
    path = write_results(tmp_path, statuses)
    if not statuses:
        with pytest.raises(ValueError, match="nonempty"):
            summarize(path)
    else:
        assert summarize(path)["failed"] == 1


@pytest.mark.parametrize("payload", [[], {"results": [None]}])
def test_summary_rejects_malformed_results(tmp_path, payload):
    path = tmp_path / "run_results.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="object"):
        summarize(path)
