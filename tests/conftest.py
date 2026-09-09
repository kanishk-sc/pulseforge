import pytest


def pytest_addoption(parser):
    parser.addoption("--run-integration", action="store_true", default=False)


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-integration"):
        return
    skip = pytest.mark.skip(reason="start Compose and pass --run-integration")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)
