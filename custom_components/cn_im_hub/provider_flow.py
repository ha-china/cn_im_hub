from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from homeassistant.config_entries import ConfigSubentryFlow, SubentryFlowResult
from homeassistant.core import HomeAssistant

from .providers.base import ProviderSpec

_LOGGER = logging.getLogger(__name__)


def _normalize(data: dict[str, Any]) -> dict[str, Any]:
    return {key: value.strip() if isinstance(value, str) else value for key, value in data.items()}


def _existing_count(flow: ConfigSubentryFlow, spec: ProviderSpec) -> int:
    return sum(1 for sub in flow._get_entry().subentries.values() if sub.subentry_type == spec.key)


_TRANSLATIONS_DIR = Path(__file__).parent / "translations"
_TITLE_CACHE: dict[str, dict[str, str]] = {}
_MAX_INSTANCES_PER_PROVIDER = 3


def _read_channel_titles_blocking(lang: str) -> dict[str, str]:
    for candidate in (lang, "en"):
        path = _TRANSLATIONS_DIR / f"{candidate}.json"
        if path.is_file():
            data = json.loads(path.read_text(encoding="utf-8"))
            return {k: v.get("channel_title", k) for k, v in data.get("config_subentries", {}).items()}
    return {}


async def _load_channel_titles(hass: HomeAssistant, lang: str) -> dict[str, str]:
    if lang in _TITLE_CACHE:
        return _TITLE_CACHE[lang]
    titles = await hass.async_add_executor_job(_read_channel_titles_blocking, lang)
    _TITLE_CACHE[lang] = titles
    return titles


async def _next_title(flow: ConfigSubentryFlow, spec: ProviderSpec) -> str:
    titles = await _load_channel_titles(flow.hass, flow.hass.config.language)
    base = titles.get(spec.key, spec.key)
    n = _existing_count(flow, spec)
    return base if n == 0 else f"{base} #{n + 1}"


def _current_data(flow: ConfigSubentryFlow) -> dict[str, Any]:
    return {} if flow.source == "user" else dict(flow._get_reconfigure_subentry().data)


async def _complete(flow: ConfigSubentryFlow, spec: ProviderSpec, data: dict[str, Any]) -> SubentryFlowResult:
    entry = flow._get_entry()
    if flow.source == "user":
        result = flow.async_create_entry(title=await _next_title(flow, spec), data=data)
    else:
        result = flow.async_update_and_abort(entry, flow._get_reconfigure_subentry(), data=data)
    
    async def _delayed_reload() -> None:
        await flow.hass.config_entries.async_reload(entry.entry_id)
    
    flow.hass.async_create_task(_delayed_reload(), "cn_im_hub_reload_after_config")
    return result


async def _set_options(
    flow: ConfigSubentryFlow,
    spec: ProviderSpec,
    user_input: dict[str, Any] | None,
) -> SubentryFlowResult:
    errors: dict[str, str] = {}
    current = getattr(flow, "_current", _current_data(flow))
    if user_input is not None:
        current = _normalize(user_input)
        try:
            await spec.validate_config(flow.hass, current)
            return await _complete(flow, spec, current)
        except Exception as err:
            _LOGGER.warning("Provider validation failed (%s): %s", spec.key, err)
            errors["base"] = "cannot_connect"
    flow._current = current
    return flow.async_show_form(step_id="set_options", data_schema=spec.schema_builder(current), errors=errors)


def build_simple_provider_flow(spec: ProviderSpec) -> type[ConfigSubentryFlow]:
    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> SubentryFlowResult:
        count = _existing_count(self, spec)
        if not spec.allow_multiple and count > 0:
            return self.async_abort(reason="already_configured")
        if spec.allow_multiple and count >= _MAX_INSTANCES_PER_PROVIDER:
            return self.async_abort(reason="max_instances_reached")
        return await async_step_set_options(self, user_input)

    async def async_step_reconfigure(self, user_input: dict[str, Any] | None = None) -> SubentryFlowResult:
        self._current = _current_data(self)
        return await async_step_set_options(self, user_input)

    async def async_step_set_options(self, user_input: dict[str, Any] | None = None) -> SubentryFlowResult:
        return await _set_options(self, spec, user_input)

    return type(
        f"{spec.key.title().replace(' ', '')}ProviderSubentryFlow",
        (ConfigSubentryFlow,),
        {
            "_provider_spec": spec,
            "async_step_user": async_step_user,
            "async_step_reconfigure": async_step_reconfigure,
            "async_step_set_options": async_step_set_options,
        },
    )
