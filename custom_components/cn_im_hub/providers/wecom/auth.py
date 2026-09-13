"""WeCom QR-scan bot binding.

Mirrors @wecom/wecom-openclaw-cli 1.1.1 ``dist/utils/qrcode.js``:

  GET https://work.weixin.qq.com/ai/qc/generate?source=...&plat=...
      -> {data: {scode, auth_url}}            (auth_url is the QR content)
  GET https://work.weixin.qq.com/ai/qc/query_result?scode=...
      -> {data: {status: "success", bot_info: {botid, secret}}}

The server side keeps a session for about 5 minutes; the CLI polls every 3s.
Here the config flow polls once per submit instead, so no background task
is needed. The returned botid / secret are the same credentials the manual
setup path stores.
"""

from __future__ import annotations

import logging
import sys
import time
from dataclasses import dataclass, field
from typing import Any

import aiohttp
from homeassistant.helpers.aiohttp_client import async_get_clientsession

_LOGGER = logging.getLogger(__name__)

_QR_GENERATE_URL = "https://work.weixin.qq.com/ai/qc/generate"
_QR_QUERY_URL = "https://work.weixin.qq.com/ai/qc/query_result?scode="
_QR_CODE_PAGE = "https://work.weixin.qq.com/ai/qc/gen?source=cn-im-hub&scode="
_QR_SOURCE = "cn-im-hub"
_QR_TTL_SECONDS = 300
_REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30)

# Poll outcomes, aligned with the other QR flows.
STATUS_PENDING = "pending"
STATUS_OK = "ok"
STATUS_EXPIRED = "expired"


@dataclass
class WecomQrSession:
    """A pending QR-scan binding session."""

    scode: str
    auth_url: str
    started_at: float = field(default_factory=time.monotonic)

    @property
    def expired(self) -> bool:
        return time.monotonic() - self.started_at >= _QR_TTL_SECONDS

    @property
    def page_url(self) -> str:
        return _QR_CODE_PAGE + self.scode


def _plat_code() -> int:
    return {"darwin": 1, "win32": 2, "linux": 3}.get(sys.platform, 0)


async def async_start_wecom_qr(hass: Any) -> WecomQrSession:
    """Request a QR code; the QR encodes ``auth_url``."""
    session = async_get_clientsession(hass)
    url = f"{_QR_GENERATE_URL}?source={_QR_SOURCE}&plat={_plat_code()}"
    async with session.get(url, timeout=_REQUEST_TIMEOUT) as resp:
        data = await resp.json(content_type=None)
    payload = data.get("data") if isinstance(data, dict) else None
    scode = str((payload or {}).get("scode") or "").strip()
    auth_url = str((payload or {}).get("auth_url") or "").strip()
    if not scode or not auth_url:
        raise ValueError(f"unexpected QR generate response: {data}")
    return WecomQrSession(scode=scode, auth_url=auth_url)


async def async_poll_wecom_qr(hass: Any, qr: WecomQrSession) -> tuple[str, str, str]:
    """Poll once. Returns ``(status, bot_id, secret)``."""
    session = async_get_clientsession(hass)
    async with session.get(_QR_QUERY_URL + qr.scode, timeout=_REQUEST_TIMEOUT) as resp:
        data = await resp.json(content_type=None)
    payload = data.get("data") if isinstance(data, dict) else None
    payload = payload or {}
    if str(payload.get("status") or "") != "success":
        return STATUS_PENDING, "", ""
    bot_info = payload.get("bot_info") or {}
    bot_id = str(bot_info.get("botid") or "").strip()
    secret = str(bot_info.get("secret") or "").strip()
    if not bot_id or not secret:
        _LOGGER.warning("WeCom QR success without bot_info: %s", data)
        return STATUS_PENDING, "", ""
    return STATUS_OK, bot_id, secret
