"""Shared pytest fixtures for nexusflow test suite."""

import random
from collections.abc import Generator

import pytest
from starlette.testclient import TestClient

from nexusflow.api.app import create_app


@pytest.fixture(autouse=True)
def set_deterministic_seed() -> None:
    """Ensure deterministic random seed across test runs."""
    random.seed(42)


@pytest.fixture
def test_app():
    """Create test FastAPI application instance with worker disabled for deterministic tests."""
    return create_app(enable_worker=False, aging_window_seconds=2.0)


@pytest.fixture
def client(test_app) -> Generator[TestClient, None, None]:
    """TestClient fixture with app lifespan context."""
    with TestClient(test_app) as c:
        yield c
