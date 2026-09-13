"""DingTalk subentry flow: QR one-click bot creation or manual credentials."""

from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import ConfigSubentryFlow, SubentryFlowResult

from ...const import CONF_DINGTALK_CLIENT_ID, CONF_DINGTALK_CLIENT_SECRET
from ...core.qr import build_qr_data_url
from ...provider_flow import (
    _MAX_INSTANCES_PER_PROVIDER,
    _complete,
    _current_data,
    _existing_count,
    _set_options,
)
from .auth import (
    STATUS_DENIED,
    STATUS_EXPIRED,
    STATUS_FAILED,
    STATUS_OK,
    STATUS_PENDING,
    DingtalkRegistration,
    async_begin_dingtalk_registration,
    async_poll_dingtalk_registration,
)

_LOGGER = logging.getLogger(__name__)


class DingtalkProviderSubentryFlow(ConfigSubentryFlow):
    """QR-first setup flow for the DingTalk channel.

    Mirrors the WeChat flow: the QR shows on the first screen. The menu
    buttons below it only offer the manual-credentials fallback.
    """

    _provider_spec: Any
    _current: dict[str, Any]
    _registration: DingtalkRegistration | None = None
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
            _LOGGER.warning("DingTalk QR unavailable: %s", err)
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
        registration = self._registration
        if user_input is None:
            # First entry from the menu: reuse the QR already rendered there.
            if registration is not None and not registration.expired:
                return self.async_show_form(
                    step_id="qr_wait",
                    data_schema=vol.Schema({}),
                    description_placeholders=self._qr_placeholders(),
                )
            return await self._async_render_fresh_qr(
                "qr_expired" if registration is not None else None
            )
        if registration.expired:
            return await self._async_render_fresh_qr("qr_expired")

        try:
            status, client_id, client_secret = await async_poll_dingtalk_registration(
                self.hass, registration
            )
        except Exception as err:
            _LOGGER.warning("DingTalk QR poll failed: %s", err)
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
        if status == STATUS_EXPIRED:
            return await self._async_render_fresh_qr("qr_expired")
        if status in (STATUS_DENIED, STATUS_FAILED):
            if status == STATUS_FAILED:
                _LOGGER.warning("DingTalk QR registration failed")
            return await self._async_render_fresh_qr(
                "auth_denied" if status == STATUS_DENIED else "registration_failed"
            )

        data = {CONF_DINGTALK_CLIENT_ID: client_id, CONF_DINGTALK_CLIENT_SECRET: client_secret}
        try:
            await self._provider_spec.validate_config(self.hass, data)
        except Exception as err:
            _LOGGER.warning("DingTalk QR credential validation failed: %s", err)
            return self.async_show_form(
                step_id="qr_wait",
                data_schema=vol.Schema({}),
                errors={"base": "cannot_connect"},
                description_placeholders=self._qr_placeholders(),
            )
        return await _complete(self, self._provider_spec, data)

    async def _async_ensure_qr(self) -> None:
        if self._registration is None or self._registration.expired:
            self._registration = await async_begin_dingtalk_registration(self.hass)
            self._qr_data_url = build_qr_data_url(self._registration.qr_url)

    async def _async_render_fresh_qr(self, error_key: str | None) -> SubentryFlowResult:
        """Begin a new registration and render the QR form."""
        try:
            self._registration = await async_begin_dingtalk_registration(self.hass)
        except Exception as err:
            _LOGGER.warning("DingTalk registration failed to start: %s", err)
            self._registration = None
            return self.async_show_form(
                step_id="qr_wait",
                data_schema=vol.Schema({}),
                errors={"base": "registration_failed"},
            )
        self._qr_data_url = build_qr_data_url(self._registration.qr_url)
        return self.async_show_form(
            step_id="qr_wait",
            data_schema=vol.Schema({}),
            errors={"base": error_key} if error_key else None,
            description_placeholders=self._qr_placeholders(),
        )

    def _qr_placeholders(self) -> dict[str, str]:
        qr_url = self._registration.qr_url if self._registration else ""
        return {
            "qr_markdown": f"![DingTalk QR]({self._qr_data_url})" if self._qr_data_url else "",
            "qr_url": qr_url,
        }
