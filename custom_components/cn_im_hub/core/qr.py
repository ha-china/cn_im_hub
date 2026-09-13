"""Shared QR code helpers for provider setup flows."""

from __future__ import annotations

import base64
import io

import segno


def build_qr_data_url(text: str) -> str:
    """Render ``text`` as a PNG data URL for config-flow placeholders."""
    out = io.BytesIO()
    segno.make(text).save(out, kind="png", scale=6, border=2)
    return "data:image/png;base64," + base64.b64encode(out.getvalue()).decode("ascii")
