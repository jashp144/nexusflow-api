"""Tests for ASGI entrypoint."""

from nexusflow.main import app


def test_main_app_instance():
    """Verify ASGI app instance is created and configured."""
    assert app is not None
    assert app.title == "NexusFlow API"
