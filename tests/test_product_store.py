import base64

import pytest

from pulseforge.product.store import decode_cursor


def test_cursor_timestamp_requires_timezone():
    raw = b"2026-09-20T12:00:00|00000000-0000-0000-0000-000000000001"
    cursor = base64.urlsafe_b64encode(raw).decode().rstrip("=")

    with pytest.raises(ValueError, match="invalid cursor"):
        decode_cursor(cursor)
