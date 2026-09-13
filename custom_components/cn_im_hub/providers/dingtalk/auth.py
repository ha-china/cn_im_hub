"""DingTalk one-click bot registration (QR scan).

Mirrors @dingtalk-real-ai/dingtalk-connector 0.8.26 ``src/device-auth.ts``:
an OAuth-style device registration against the DingTalk Open Platform.

  POST {base}/app/registration/init   {source}              -> nonce
  POST {base}/app/registration/begin  {nonce}              -> device_code,
      verification_uri_complete, expires_in (default 7200), interval (default 3)
  POST {base}/app/registration/poll   {device_code}        -> status
      WAITING | SUCCESS | FAIL | EXPIRED (+ client_id / client_secret /
      fail_reason)

On SUCCESS the returned client_id / client_secret are the same credentials
the manual setup path stores.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

import aiohttp
from homeassistant.helpers.aiohttp_client import async_get_clientsession

_LOGGER = logging.getLogger(__name__)

_REGISTRATION_BASE_URL = "https://oapi.dingtalk.com"
_REGISTRATION_SOURCE = "DING_DWS_CLAW"
_REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30)

# Poll outcomes, aligned with the Feishu flow's vocabulary.
STATUS_PENDING = "pending"
STATUS_OK = "ok"
STATUS_DENIED = "denied"
STATUS_EXPIRED = "expired"
STATUS_FAILED = "failed"


@dataclass
class DingtalkRegistration:
    """A pending one-click bot registration session."""

    device_code: str
    qr_url: str
    expire_in: int = 7200
    interval: int = 3
    started_at: float = field(default_factory=time.monotonic)

    @property
    def expired(self) -> bool:
        return time.monotonic() - self.started_at >= self.expire_in


async def _post_registration(hass: Any, path: str, payload: dict[str, Any]) -> dict[str, Any]:
    session = async_get_clientsession(hass)
    url = f"{_REGISTRATION_BASE_URL}/app/registration/{path}"
    async with session.post(url, json=payload, timeout=_REQUEST_TIMEOUT) as resp:
        data = await resp.json(content_type=None)
    if not isinstance(data, dict) or data.get("errcode") != 0:
        errmsg = data.get("errmsg") if isinstance(data, dict) else None
        raise RuntimeError(f"registration/{path} failed: {errmsg or data} (errcode={data.get('errcode') if isinstance(data, dict) else 'N/A'})")
    return data


async def async_begin_dingtalk_registration(hass: Any) -> DingtalkRegistration:
    """Start a registration and return the QR session handle."""
    init = await _post_registration(hass, "init", {"source": _REGISTRATION_SOURCE})
    nonce = str(init.get("nonce") or "").strip()
    if not nonce:
        raise ValueError("registration/init missing nonce")

    begin = await _post_registration(hass, "begin", {"nonce": nonce})
    device_code = str(begin.get("device_code") or "").strip()
    qr_url = str(begin.get("verification_uri_complete") or "").strip()
    if not device_code or not qr_url:
        raise ValueError("registration/begin missing device_code or verification_uri_complete")

    expire_in = int(begin.get("expires_in") or 7200)
    interval = int(begin.get("interval") or 3)
    return DingtalkRegistration(
        device_code=device_code,
        qr_url=qr_url,
        expire_in=expire_in if expire_in > 0 else 7200,
        interval=interval if interval > 0 else 3,
    )


async def async_poll_dingtalk_registration(hass: Any, reg: DingtalkRegistration) -> tuple[str, str, str]:
    """Poll once. Returns ``(status, client_id, client_secret)``."""
    data = await _post_registration(hass, "poll", {"device_code": reg.device_code})
    status_raw = str(data.get("status") or "").strip().upper()
    client_id = str(data.get("client_id") or "").strip()
    client_secret = str(data.get("client_secret") or "").strip()
    if status_raw == "SUCCESS":
        if not client_id or not client_secret:
            _LOGGER.warning("DingTalk registration SUCCESS without credentials")
            return STATUS_FAILED, "", ""
        return STATUS_OK, client_id, client_secret
    if status_raw == "WAITING":
        return STATUS_PENDING, "", ""
    if status_raw == "FAIL":
        _LOGGER.info("DingTalk registration FAIL: %s", data.get("fail_reason"))
        return STATUS_DENIED, "", ""
    if status_raw == "EXPIRED":
        return STATUS_EXPIRED, "", ""
    return STATUS_FAILED, "", ""
