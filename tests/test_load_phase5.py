import importlib.util
from pathlib import Path


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
