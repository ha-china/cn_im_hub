"""WeCom subentry flow: QR-scan bot binding or manual credentials."""

from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import ConfigSubentryFlow, SubentryFlowResult

from ...const import CONF_WECOM_BOT_ID, CONF_WECOM_SECRET
from ...core.qr import build_qr_data_url
from ...provider_flow import (
    _MAX_INSTANCES_PER_PROVIDER,
    _complete,
    _current_data,
    _existing_count,
    _set_options,
)
from .auth import (
    STATUS_OK,
    STATUS_PENDING,
    WecomQrSession,
    async_poll_wecom_qr,
    async_start_wecom_qr,
)

_LOGGER = logging.getLogger(__name__)


class WecomProviderSubentryFlow(ConfigSubentryFlow):
    """QR-first setup flow for the WeCom channel.

    Mirrors the WeChat flow: the QR shows on the first screen. The menu
    buttons below it only offer the manual-credentials fallback.
    """

    _provider_spec: Any
    _current: dict[str, Any]
    _session: WecomQrSession | None = None
    _qr_data_url: str = ""

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> SubentryFlowResult:
        spec = self._provider_spec
        count = _existing_count(self, spec)
        if not spec.allow_multiple and count > 0:
            return self.async_abort(reason="already_configured")
        if spec.allow_multiple and count >= _MAX_INSTANCES_PER_PROVIDER:
            return self.async_abort(reason="max_instances_reached")
        try:
            await self._async_ensure_qr()
        except Exception as err:
            _LOGGER.warning("WeCom QR unavailable: %s", err)
            # No QR: land directly on the manual form with the reason shown.
            return self.async_show_form(
                step_id="set_options",
                data_schema=self._provider_spec.schema_builder({}),
                errors={"base": "registration_failed"},
            )
        return self.async_show_menu(
            step_id="user",
            menu_options=["qr", "manual"],
            description_placeholders=self._qr_placeholders(),
        )

    async def async_step_qr(
        self, user_input: dict[str, Any] | None = None
    ) -> SubentryFlowResult:
        return await self.async_step_qr_wait(user_input)

    async def async_step_manual(
        self, user_input: dict[str, Any] | None = None
    ) -> SubentryFlowResult:
        self._current = {}
        return await _set_options(self, self._provider_spec, None)

    async def async_step_set_options(
        self, user_input: dict[str, Any] | None = None
    ) -> SubentryFlowResult:
        return await _set_options(self, self._provider_spec, user_input)

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> SubentryFlowResult:
        self._current = _current_data(self)
        return await _set_options(self, self._provider_spec, user_input)

    async def async_step_qr_wait(
        self, user_input: dict[str, Any] | None = None
    ) -> SubentryFlowResult:
        session = self._session
        if user_input is None:
            # First entry from the menu: reuse the QR already rendered there.
            if session is not None and not session.expired:
                return self.async_show_form(
                    step_id="qr_wait",
                    data_schema=vol.Schema({}),
                    description_placeholders=self._qr_placeholders(),
                )
            return await self._async_render_fresh_qr(
                "qr_expired" if session is not None else None
            )
        if session.expired:
            return await self._async_render_fresh_qr("qr_expired")

        try:
            status, bot_id, secret = await async_poll_wecom_qr(self.hass, session)
        except Exception as err:
            _LOGGER.warning("WeCom QR poll failed: %s", err)
            return self.async_show_form(
                step_id="qr_wait",
                data_schema=vol.Schema({}),
                errors={"base": "auth_not_confirmed"},
                description_placeholders=self._qr_placeholders(),
            )

        if status == STATUS_PENDING:
            return self.async_show_form(
                step_id="qr_wait",
                data_schema=vol.Schema({}),
                errors={"base": "auth_not_confirmed"},
                description_placeholders=self._qr_placeholders(),
            )

        data = {CONF_WECOM_BOT_ID: bot_id, CONF_WECOM_SECRET: secret}
        try:
            await self._provider_spec.validate_config(self.hass, data)
        except Exception as err:
            _LOGGER.warning("WeCom QR credential validation failed: %s", err)
            return self.async_show_form(
                step_id="qr_wait",
                data_schema=vol.Schema({}),
                errors={"base": "cannot_connect"},
                description_placeholders=self._qr_placeholders(),
            )
        return await _complete(self, self._provider_spec, data)

    async def _async_ensure_qr(self) -> None:
        if self._session is None or self._session.expired:
            self._session = await async_start_wecom_qr(self.hass)
            self._qr_data_url = build_qr_data_url(self._session.auth_url)

    async def _async_render_fresh_qr(self, error_key: str | None) -> SubentryFlowResult:
        """Request a new QR code and render the QR form."""
        try:
            self._session = await async_start_wecom_qr(self.hass)
        except Exception as err:
            _LOGGER.warning("WeCom QR request failed: %s", err)
            self._session = None
            return self.async_show_form(
                step_id="qr_wait",
                data_schema=vol.Schema({}),
                errors={"base": "registration_failed"},
            )
        self._qr_data_url = build_qr_data_url(self._session.auth_url)
        return self.async_show_form(
            step_id="qr_wait",
            data_schema=vol.Schema({}),
            errors={"base": error_key} if error_key else None,
            description_placeholders=self._qr_placeholders(),
        )

    def _qr_placeholders(self) -> dict[str, str]:
        # The QR encodes auth_url; the gen page is the fallback link like the
        # upstream CLI shows ("也可打开二维码链接扫码").
        page_url = self._session.page_url if self._session else ""
        return {
            "qr_markdown": f"![WeCom QR]({self._qr_data_url})" if self._qr_data_url else "",
            "qr_url": page_url,
        }
