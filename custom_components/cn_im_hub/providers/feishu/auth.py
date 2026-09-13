"""Feishu one-click app registration (QR scan).

Wraps the official ``lark_oapi.aregister_app`` device-flow registration
(RFC 8628, ``POST {accounts}/oauth/v1/app/registration``) so the config
flow can show a QR code and poll for completion. The platform base
template already presets the bot capability, message scopes, the
``im.message.receive_v1`` event and WebSocket delivery, so no ``addons``
are requested here.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any

_LOGGER = logging.getLogger(__name__)

_REGISTRATION_SOURCE = "cn-im-hub"
_QR_READY_TIMEOUT_SECONDS = 15.0

# Outcome of waiting for the registration task.
OUTCOME_OK = "ok"
OUTCOME_PENDING = "pending"
OUTCOME_DENIED = "denied"
OUTCOME_EXPIRED = "expired"
OUTCOME_FAILED = "failed"


@dataclass
class FeishuRegistration:
    """A pending one-click app registration session."""

    task: asyncio.Task[Any] | None = None
    qr_ready: asyncio.Event = field(default_factory=asyncio.Event)
    qr_url: str = ""
    expire_in: int = 600
    started_at: float = field(default_factory=time.monotonic)

    @property
    def expired(self) -> bool:
        return time.monotonic() - self.started_at >= self.expire_in


async def async_start_feishu_registration() -> FeishuRegistration:
    """Kick off an async registration and return the session handle.

    The QR URL arrives via ``on_qr_code`` while the task keeps polling in
    the background until the user confirms, rejects, or the code expires.
    """
    from lark_oapi import aregister_app

    registration = FeishuRegistration()

    def _on_qr_code(info: dict[str, Any]) -> None:
        registration.qr_url = str(info.get("url") or "")
        registration.expire_in = int(info.get("expire_in") or 600)
        registration.started_at = time.monotonic()
        registration.qr_ready.set()

    def _on_status_change(info: dict[str, Any]) -> None:
        status = str(info.get("status") or "")
        if status == "slow_down":
            _LOGGER.debug(
                "Feishu registration slowed down (interval=%s)", info.get("interval")
            )
        elif status == "domain_switched":
            _LOGGER.info("Feishu registration switched to Lark domain")

    registration.task = asyncio.create_task(
        aregister_app(
            on_qr_code=_on_qr_code,
            on_status_change=_on_status_change,
            source=_REGISTRATION_SOURCE,
        ),
        name="cn_im_hub_feishu_register",
    )
    return registration


async def async_wait_for_qr(registration: FeishuRegistration) -> str:
    """Return the QR URL once it is ready."""
    await asyncio.wait_for(registration.qr_ready.wait(), _QR_READY_TIMEOUT_SECONDS)
    return registration.qr_url


async def async_wait_feishu_registration(
    registration: FeishuRegistration, timeout: float
) -> tuple[str, dict[str, Any]]:
    """Wait up to ``timeout`` for the registration to finish.

    Returns ``(outcome, result)``; ``result`` carries ``client_id`` /
    ``client_secret`` / ``user_info`` only for ``OUTCOME_OK``.
    """
    if registration.task is None:
        return OUTCOME_FAILED, {}
    done, _ = await asyncio.wait({registration.task}, timeout=timeout)
    if not done:
        return OUTCOME_PENDING, {}
    try:
        err = registration.task.exception()
    except asyncio.CancelledError:
        return OUTCOME_FAILED, {}
    if err is None:
        return OUTCOME_OK, dict(registration.task.result() or {})
    _LOGGER.debug("Feishu registration failed: %s", err)
    name = type(err).__name__
    if name == "AppAccessDeniedError":
        return OUTCOME_DENIED, {}
    if name == "AppExpiredError":
        return OUTCOME_EXPIRED, {}
    return OUTCOME_FAILED, {}


def cancel_registration(registration: FeishuRegistration | None) -> None:
    """Stop a still-pending registration task."""
    if registration is not None and registration.task is not None and not registration.task.done():
        registration.task.cancel()
