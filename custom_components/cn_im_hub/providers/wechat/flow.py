"""Weixin QR login flow."""

from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import ConfigSubentryFlow, SubentryFlowResult
from homeassistant.helpers.storage import Store

from ...const import CONF_WECHAT_ACCOUNT_ID, CONF_WECHAT_BASE_URL, CONF_WECHAT_TOKEN, CONF_WECHAT_USER_ID, PROVIDER_WECHAT, WECHAT_DEFAULT_BASE_URL
from ...provider_flow import _load_channel_titles
from .auth import async_start_weixin_login, async_wait_weixin_login

_LOGGER = logging.getLogger(__name__)
_ACCOUNT_INDEX_STORE_VERSION = 2
_ACCOUNT_INDEX_STORE_KEY = "cn_im_hub_wechat_accounts"
_CONF_WECHAT_SHOW_LIVE_PROGRESS = "wechat_show_live_progress"

class _WeixinAccountIndexStore(Store[dict[str, dict[str, str]]]):
    """Store for the Weixin account index."""

    async def _async_migrate_func(
        self,
        old_major_version: int,
        old_minor_version: int,
        old_data: dict[str, dict[str, str]],
    ) -> dict[str, dict[str, str]]:
        """Migrate old account index data."""
        if old_major_version == 1:
            return old_data

        raise ValueError(
            f"Unsupported storage version: "
            f"{old_major_version}.{old_minor_version}"
        )

class WeixinProviderSubentryFlow(ConfigSubentryFlow):
    """QR login based setup flow for Weixin channel."""

    _provider_spec: Any
    _current: dict[str, Any]

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> SubentryFlowResult:
        self._current = {CONF_WECHAT_BASE_URL: WECHAT_DEFAULT_BASE_URL}
        await self._async_prepare_qr()
        return await self.async_step_auth_wait(None)

    async def async_step_auth_wait(
        self, user_input: dict[str, Any] | None = None
    ) -> SubentryFlowResult:
        placeholders = {
            "qr_markdown": f"![Weixin QR]({self._current.get('wechat_qr_data_url', '')})"
            if self._current.get("wechat_qr_data_url")
            else "",
            "qr_url": str(self._current.get("wechat_qr_url", "")),
        }
        if user_input is None:
            return self.async_show_form(
                step_id="auth_wait",
                data_schema=vol.Schema({}),
                description_placeholders=placeholders,
            )
        try:
            result = await async_wait_weixin_login(
                self.hass,
                login=self._current["wechat_login_session"],
                base_url=str(self._current.get(CONF_WECHAT_BASE_URL, WECHAT_DEFAULT_BASE_URL)),
            )
        except Exception as err:
            _LOGGER.warning("Weixin QR login wait failed: %s", err)
            return self.async_show_form(
                step_id="auth_wait",
                data_schema=vol.Schema({}),
                errors={"base": "auth_not_confirmed"},
                description_placeholders=placeholders,
            )

        if result.already_connected:
            return self.async_abort(reason="already_connected", description_placeholders={"message": result.message})

        data = {
            CONF_WECHAT_TOKEN: result.token,
            CONF_WECHAT_ACCOUNT_ID: result.account_id,
            CONF_WECHAT_USER_ID: result.user_id,
            CONF_WECHAT_BASE_URL: result.base_url or str(self._current.get(CONF_WECHAT_BASE_URL, WECHAT_DEFAULT_BASE_URL)),
        }
        await self._async_update_account_index(data)
        return self.async_create_entry(
            title=await self._build_entry_title(self.hass.config.language),
            data=data,
        )

    async def _build_entry_title(self, language: str) -> str:
        titles = await _load_channel_titles(self.hass, language)
        return titles.get(PROVIDER_WECHAT, PROVIDER_WECHAT)

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> SubentryFlowResult:
        subentry = self._get_reconfigure_subentry()
        current_data = dict(subentry.data)
        errors: dict[str, str] = {}

        if user_input is not None:
            new_data = {
                **current_data,
                _CONF_WECHAT_SHOW_LIVE_PROGRESS: user_input.get(_CONF_WECHAT_SHOW_LIVE_PROGRESS, False),
            }
            entry = self._get_entry()
            result = self.async_update_and_abort(entry, subentry, data=new_data)
            
            async def _reload() -> None:
                await self.hass.config_entries.async_reload(entry.entry_id)
            
            self.hass.async_create_task(_reload(), "cn_im_hub_wechat_reload")
            return result

        schema = vol.Schema(
            {
                vol.Optional(
                    _CONF_WECHAT_SHOW_LIVE_PROGRESS,
                    default=current_data.get(_CONF_WECHAT_SHOW_LIVE_PROGRESS, False),
                ): bool,
            }
        )
        return self.async_show_form(
            step_id="reconfigure",
            data_schema=schema,
            errors=errors,
        )

    async def _async_prepare_qr(self) -> None:
        result = await async_start_weixin_login(
            self.hass,
            base_url=str(self._current.get(CONF_WECHAT_BASE_URL, WECHAT_DEFAULT_BASE_URL)),
            local_token_list=await self._async_load_local_token_list(),
        )
        self._current["wechat_login_session"] = result
        self._current["wechat_qr_url"] = result.qrcode_url
        self._current["wechat_qr_data_url"] = result.qrcode_data_url

    async def _async_load_local_token_list(self) -> list[str]:
        """Return up to 10 most-recent bot tokens from the account index.

        Mirrors openclaw-weixin 2.3.1+ getLocalBotTokenList: lets the server
        recognize an already-bound bot during QR scan and short-circuit with
        ``binded_redirect`` instead of issuing new credentials.
        """
        store = _WeixinAccountIndexStore(
            self.hass,
            _ACCOUNT_INDEX_STORE_VERSION,
            _ACCOUNT_INDEX_STORE_KEY,
        )
        current = await store.async_load() or {}
        return [
            token
            for value in current.values()
            if (token := str(value.get(CONF_WECHAT_TOKEN, "")).strip())
        ][-10:]

    async def _async_update_account_index(self, data: dict[str, str]) -> None:
        store = _WeixinAccountIndexStore(
            self.hass,
            _ACCOUNT_INDEX_STORE_VERSION,
            _ACCOUNT_INDEX_STORE_KEY,
        )
        current = await store.async_load() or {}
        user_id = str(data.get(CONF_WECHAT_USER_ID, "")).strip()
        account_id = str(data.get(CONF_WECHAT_ACCOUNT_ID, "")).strip()
        if user_id:
            stale_keys = [key for key, value in current.items() if key != account_id and value.get(CONF_WECHAT_USER_ID) == user_id]
            for key in stale_keys:
                current.pop(key, None)
        if account_id:
            current[account_id] = {
                CONF_WECHAT_USER_ID: user_id,
                CONF_WECHAT_TOKEN: str(data.get(CONF_WECHAT_TOKEN, "")),
                CONF_WECHAT_BASE_URL: str(data.get(CONF_WECHAT_BASE_URL, WECHAT_DEFAULT_BASE_URL)),
            }
        await store.async_save(current)
