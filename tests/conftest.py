import pytest


def pytest_addoption(parser):
    parser.addoption("--run-integration", action="store_true", default=False)
    parser.addoption("--run-streaming", action="store_true", default=False)
    parser.addoption("--run-analytics", action="store_true", default=False)
    parser.addoption("--run-product", action="store_true", default=False)


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "streaming" in item.keywords and not config.getoption("--run-streaming"):
            item.add_marker(
                pytest.mark.skip(reason="start streaming profile and pass --run-streaming")
            )
        if "analytics" in item.keywords and not config.getoption("--run-analytics"):
            item.add_marker(
                pytest.mark.skip(reason="build analytics image and pass --run-analytics")
            )
        if "product" in item.keywords and not config.getoption("--run-product"):
            item.add_marker(pytest.mark.skip(reason="start product profile and pass --run-product"))
    if config.getoption("--run-integration"):
        return
    skip = pytest.mark.skip(reason="start Compose and pass --run-integration")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)
