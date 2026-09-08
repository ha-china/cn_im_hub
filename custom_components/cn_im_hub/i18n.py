"""Runtime translations for user-facing IM messages.

HA's translation system covers config flows and services only. Messages we
send *into* the IM channels (timeouts, welcome texts, fallback replies) need
their own lookup: a `runtime` section in translations/{lang}.json with an
English fallback baked in below.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from homeassistant.core import HomeAssistant

_TRANSLATIONS_DIR = Path(__file__).parent / "translations"
_CACHE: dict[str, dict[str, str]] = {}

_DEFAULTS: dict[str, str] = {
    "wecom_welcome": "Home Assistant is connected. Send me questions or control commands directly.",
    "wecom_command_timeout": "Command timed out (6 minutes). Please simplify the command and try again.",
    "wecom_command_failed": "Execution failed: {error}",
    "wechat_already_connected": "This bot is already connected. No need to scan again.",
    "xiaoyi_task_empty_reply": "Task finished without any output. Please try again.",
    "xiaoyi_task_failed": "Task failed, please try again later.",
    "dingtalk_command_timeout": "Command timed out. Please simplify the command and try again.",
}


def _load_blocking(lang: str) -> dict[str, str]:
    for candidate in (lang, "en"):
        path = _TRANSLATIONS_DIR / f"{candidate}.json"
        if not path.is_file():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        section = data.get("runtime")
        if isinstance(section, dict):
            return {str(k): str(v) for k, v in section.items()}
    return {}


async def async_get_runtime_strings(hass: HomeAssistant, lang: str) -> dict[str, str]:
    """Return the `runtime` string map for the given language (cached)."""
    if lang in _CACHE:
        return _CACHE[lang]
    strings = await hass.async_add_executor_job(_load_blocking, lang)
    _CACHE[lang] = strings
    return strings


def runtime_string(strings: dict[str, str], key: str, **kwargs: Any) -> str:
    """Look up a runtime string; falls back to the built-in English default."""
    template = strings.get(key) or _DEFAULTS.get(key) or key
    if kwargs:
        try:
            return template.format(**kwargs)
        except (KeyError, IndexError, ValueError):
            return template
    return template
