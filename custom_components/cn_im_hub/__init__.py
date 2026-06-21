from __future__ import annotations

import asyncio
import logging
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers import device_registry as dr

from .const import CONF_AGENT_ID, DOMAIN, SERVICE_SEND_MESSAGE
from .core.service import register_services
from .models import HubRuntime
from .providers.registry import get_provider_specs

_LOGGER = logging.getLogger(__name__)
PLATFORMS = [Platform.SENSOR, Platform.SELECT]
CONFIG_SCHEMA = cv.config_entry_only_config_schema(DOMAIN)

MAX_RETRY_ATTEMPTS = 8
RETRY_DELAY_SECONDS = 5

_BOOL_MAP = {"true": True, "false": False}


def _remove_feishu_callback_config(hass: HomeAssistant, subentry_id: str) -> None:
    feishu_configs = hass.data.get(DOMAIN, {}).get("feishu_callback_configs", {})
    feishu_configs.pop(subentry_id, None)


def _normalize_stored_value(value: Any) -> Any:
    return (
        {k: _normalize_stored_value(v) for k, v in value.items()} if isinstance(value, dict)
        else [_normalize_stored_value(v) for v in value] if isinstance(value, list)
        else _BOOL_MAP.get(value.strip().lower(), value.strip()) if isinstance(value, str)
        else value
    )


async def async_setup(hass: HomeAssistant, config: dict[str, Any]) -> bool:
    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN].setdefault("feishu_callback_configs", {})
    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    agent_id = str(dict(entry.options).get(CONF_AGENT_ID, "")).strip()
    specs = get_provider_specs()
    runtimes = {}
    failed_subentries: list[str] = []

    for sub in entry.subentries.values():
        spec = specs.get(sub.subentry_type)
        if spec is None:
            _LOGGER.warning("Unknown provider in subentry: %s", sub.subentry_type)
            continue

        rt = None
        last_error: Exception | None = None
        for attempt in range(1, MAX_RETRY_ATTEMPTS + 1):
            try:
                rt = await spec.setup_provider(
                    hass, _normalize_stored_value(dict(sub.data)), agent_id=agent_id, subentry_id=sub.subentry_id,
                )
                rt.title = sub.title
                break
            except Exception as err:
                last_error = err
                _LOGGER.warning(
                    "Provider %s setup failed (attempt %d/%d): %s",
                    sub.subentry_type, attempt, MAX_RETRY_ATTEMPTS, err,
                )
                if attempt < MAX_RETRY_ATTEMPTS:
                    await asyncio.sleep(RETRY_DELAY_SECONDS)

        if rt is not None:
            runtimes[f"{sub.subentry_type}:{sub.subentry_id}"] = rt
        else:
            failed_subentries.append(sub.title or sub.subentry_type)
            _LOGGER.error(
                "Provider %s failed after %d attempts, marking as error: %s",
                sub.subentry_type, MAX_RETRY_ATTEMPTS, last_error,
            )

    if failed_subentries:
        _LOGGER.error("The following channels failed to start: %s", ", ".join(failed_subentries))

    entry.runtime_data = HubRuntime(providers=runtimes)

    dev_reg = dr.async_get(hass)
    valid_ids = {(DOMAIN, entry.entry_id, rk) for rk in runtimes}
    for dev in dr.async_entries_for_config_entry(dev_reg, entry.entry_id):
        if not any(ident in valid_ids for ident in dev.identifiers):
            dev_reg.async_remove_device(dev.id)

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    if runtimes:
        register_services(hass)

    from .core.tmp_cleanup import async_setup_tmp_cleanup
    await async_setup_tmp_cleanup(hass)

    from .providers.feishu import FeishuCardCallbackView
    hass.http.register_view(FeishuCardCallbackView(hass))

    entry.async_on_unload(entry.add_update_listener(_async_update_listener))
    return True


async def _async_update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    await hass.config_entries.async_reload(entry.entry_id)


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    for runtime_key, rt in entry.runtime_data.providers.items():
        provider_key, _, subentry_id = runtime_key.partition(":")
        if provider_key == "feishu" and subentry_id:
            _remove_feishu_callback_config(hass, subentry_id)
        await rt.stop()

    has_remaining = any(
        getattr(e, "runtime_data", None) and getattr(e.runtime_data, "providers", None)
        for e in hass.config_entries.async_entries(DOMAIN)
        if e.entry_id != entry.entry_id
    )
    if not has_remaining:
        if hass.services.has_service(DOMAIN, SERVICE_SEND_MESSAGE):
            hass.services.async_remove(DOMAIN, SERVICE_SEND_MESSAGE)
        from .core.tmp_cleanup import async_unload_tmp_cleanup
        await async_unload_tmp_cleanup(hass)

    return unload_ok
