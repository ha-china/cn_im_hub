from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import ConfigEntry, ConfigFlow as HAConfigFlow, ConfigFlowResult, OptionsFlow
from homeassistant.core import callback
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import selector

from .const import CONF_AGENT_ID, DOMAIN
from .providers.registry import get_provider_flow_handlers

_LOGGER = logging.getLogger(__name__)


async def _get_preferred_agent_id(hass) -> str:
    try:
        from homeassistant.components.assist_pipeline.pipeline import async_get_pipeline

        pipeline = async_get_pipeline(hass)
        return pipeline.conversation_engine if isinstance(pipeline.conversation_engine, str) else ""
    except Exception as err:
        _LOGGER.debug("Unable to resolve preferred assist pipeline: %r", err)
        return ""


def _normalize_agent_id_for_storage(hass, agent_id: str) -> str:
    """Store the agent id exactly as chosen.

    HA's conversation layer accepts both forms natively: entity ids (containing
    ".") resolve through the entity component, while bare config entry ids and
    the built-in "conversation.home_assistant" resolve through the agent
    manager (see homeassistant/components/conversation/agent_manager.py
    async_get_agent). Converting an entity id to its config entry id is lossy:
    when one config entry exposes several conversation entities, the reverse
    lookup cannot tell them apart and silently falls back to the first one.
    """
    return agent_id.strip()


def _resolve_agent_id_for_selector(hass, agent_id: str) -> str:
    """Resolve a stored agent id for display in the selector.

    Values already stored as entity ids (or the built-in agent) are passed
    through untouched. Bare config entry ids (older storage format) are mapped
    back to an entity id so the selector can show a matching entry.
    """
    candidate = agent_id.strip()
    if not candidate or candidate == "conversation.home_assistant" or candidate.startswith("conversation."):
        return candidate
    match = next(
        (
            entry.entity_id
            for entry in er.async_get(hass).entities.values()
            if entry.domain == "conversation" and entry.config_entry_id == candidate
        ),
        "",
    )
    return match or candidate


def _agent_selector(hass) -> selector.ConversationAgentSelector:
    return selector.ConversationAgentSelector({"language": hass.config.language})


def _agent_schema(hass, default: str) -> vol.Schema:
    return vol.Schema({vol.Required(CONF_AGENT_ID, default=default): _agent_selector(hass)})


async def _agent_step(
    flow,
    *,
    step_id: str,
    default_agent: str,
    user_input: dict[str, Any] | None,
    submit: Callable[[str], ConfigFlowResult],
) -> ConfigFlowResult:
    agent_id = str((user_input or {}).get(CONF_AGENT_ID, "")).strip()
    errors = {"base": "agent_id_required"} if user_input is not None and not agent_id else {}
    return (
        submit(_normalize_agent_id_for_storage(flow.hass, agent_id))
        if agent_id
        else flow.async_show_form(step_id=step_id, data_schema=_agent_schema(flow.hass, default_agent), errors=errors)
    )


class ConfigFlow(HAConfigFlow, domain=DOMAIN):
    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        preferred_agent = await _get_preferred_agent_id(self.hass)
        return await _agent_step(
            self,
            step_id="user",
            default_agent=_resolve_agent_id_for_selector(self.hass, preferred_agent),
            user_input=user_input,
            submit=lambda agent_id: self.async_create_entry(title="", data={}, options={CONF_AGENT_ID: agent_id}),
        )

    @staticmethod
    def async_get_options_flow(config_entry: ConfigEntry) -> "OptionsFlowHandler":
        return OptionsFlowHandler(config_entry)

    @classmethod
    @callback
    def async_get_supported_subentry_types(cls, config_entry: ConfigEntry) -> dict[str, type]:
        return get_provider_flow_handlers()


class OptionsFlowHandler(OptionsFlow):
    def __init__(self, config_entry: ConfigEntry) -> None:
        self._config_entry = config_entry

    async def async_step_init(self, user_input: dict[str, Any] | None = None):
        preferred_agent = await _get_preferred_agent_id(self.hass)
        current = str(
            self._config_entry.options.get(
                CONF_AGENT_ID,
                self._config_entry.data.get(CONF_AGENT_ID, preferred_agent),
            )
        ).strip()
        current = _resolve_agent_id_for_selector(self.hass, current)
        return await _agent_step(
            self,
            step_id="init",
            default_agent=current,
            user_input=user_input,
            submit=lambda agent_id: self.async_create_entry(title="", data={CONF_AGENT_ID: agent_id}),
        )
