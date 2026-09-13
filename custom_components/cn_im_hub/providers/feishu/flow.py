"""Feishu subentry flow: QR one-click app creation or manual credentials."""

from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import ConfigSubentryFlow, SubentryFlowResult

from ...core.qr import build_qr_data_url
from ...provider_flow import (
    _MAX_INSTANCES_PER_PROVIDER,
    _complete,
    _current_data,
    _existing_count,
    _set_options,
)
from .auth import (
    OUTCOME_DENIED,
    OUTCOME_EXPIRED,
    OUTCOME_FAILED,
    OUTCOME_OK,
    OUTCOME_PENDING,
    FeishuRegistration,
    async_start_feishu_registration,
    async_wait_feishu_registration,
    async_wait_for_qr,
    cancel_registration,
)

_LOGGER = logging.getLogger(__name__)
_CONFIRM_POLL_SECONDS = 30.0


class FeishuProviderSubentryFlow(ConfigSubentryFlow):
    """QR-first setup flow for the Feishu channel.

    Mirrors the WeChat flow: the QR shows on the first screen. The menu
    buttons below it only offer the manual-credentials fallback.
    """

    _provider_spec: Any
    _current: dict[str, Any]
    _registration: FeishuRegistration | None = None
    _qr_data_url: str = ""
    _pending_error: str = ""

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
            registration = self._registration
            if registration is None or registration.expired:
                if registration is not None:
                    cancel_registration(registration)
                await self._async_start_registration()
                registration = self._registration
            qr_url = await async_wait_for_qr(registration)
            self._qr_data_url = build_qr_data_url(qr_url)
        except Exception as err:
            _LOGGER.warning("Feishu QR unavailable: %s", err)
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
            return await self._async_show_qr_form()

        if registration is None:
            await self._async_start_registration()
            registration = self._registration
            if registration is None:
                return self.async_show_form(
                    step_id="qr_wait",
                    data_schema=vol.Schema({}),
                    errors={"base": "registration_failed"},
                )

        if registration.expired:
            return await self._async_restart("qr_expired")

        outcome, result = await async_wait_feishu_registration(
            registration, _CONFIRM_POLL_SECONDS
        )
        if outcome == OUTCOME_PENDING:
            return self.async_show_form(
                step_id="qr_wait",
                data_schema=vol.Schema({}),
                errors={"base": "auth_not_confirmed"},
                description_placeholders=self._qr_placeholders(),
            )
        if outcome != OUTCOME_OK:
            error_key = {
                OUTCOME_EXPIRED: "qr_expired",
                OUTCOME_DENIED: "auth_denied",
            }.get(outcome, "registration_failed")
            _LOGGER.warning("Feishu QR registration ended: %s", outcome)
            return await self._async_restart(error_key)

        app_id = str(result.get("client_id") or "").strip()
        app_secret = str(result.get("client_secret") or "").strip()
        if not app_id or not app_secret:
            return await self._async_restart("registration_failed")
        cancel_registration(registration)
        data = {"app_id": app_id, "app_secret": app_secret}
        try:
            await self._provider_spec.validate_config(self.hass, data)
        except Exception as err:
            _LOGGER.warning("Feishu QR credential validation failed: %s", err)
            return self.async_show_form(
                step_id="qr_wait",
                data_schema=vol.Schema({}),
                errors={"base": "cannot_connect"},
                description_placeholders=self._qr_placeholders(),
            )
        return await self._async_complete(data)

    async def _async_show_qr_form(self) -> SubentryFlowResult:
        """Render the QR form, carrying any pending error from a restart."""
        registration = self._registration
        errors: dict[str, str] = {}
        if self._pending_error:
            errors = {"base": self._pending_error}
            self._pending_error = ""
        try:
            qr_url = await async_wait_for_qr(registration)
        except Exception as err:
            _LOGGER.warning("Feishu registration QR unavailable: %s", err)
            return self.async_show_form(
                step_id="qr_wait",
                data_schema=vol.Schema({}),
                errors=errors or {"base": "registration_failed"},
                description_placeholders=self._qr_placeholders(),
            )
        if not self._qr_data_url:
            self._qr_data_url = build_qr_data_url(qr_url)
        return self.async_show_form(
            step_id="qr_wait",
            data_schema=vol.Schema({}),
            errors=errors or None,
            description_placeholders=self._qr_placeholders(),
        )

    async def _async_start_registration(self) -> None:
        try:
            self._registration = await async_start_feishu_registration()
        except Exception as err:
            _LOGGER.warning("Feishu registration failed to start: %s", err)
            self._registration = None
        self._qr_data_url = ""

    async def _async_restart(self, error_key: str) -> SubentryFlowResult:
        """Swap in a fresh registration, then re-render the QR with the error."""
        cancel_registration(self._registration)
        await self._async_start_registration()
        if self._registration is None:
            return self.async_show_form(
                step_id="qr_wait",
                data_schema=vol.Schema({}),
                errors={"base": "registration_failed"},
            )
        self._pending_error = error_key
        return await self.async_step_qr_wait(None)

    async def _async_complete(self, data: dict[str, Any]) -> SubentryFlowResult:
        cancel_registration(self._registration)
        return await _complete(self, self._provider_spec, data)

    def _qr_placeholders(self) -> dict[str, str]:
        qr_url = self._registration.qr_url if self._registration else ""
        if qr_url and not self._qr_data_url:
            self._qr_data_url = build_qr_data_url(qr_url)
        return {
            "qr_markdown": f"![Feishu QR]({self._qr_data_url})" if self._qr_data_url else "",
            "qr_url": qr_url,
        }
