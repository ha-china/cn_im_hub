"""Conservative read-only fast path for queued WeChat messages."""
from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any

from homeassistant.components import conversation
from homeassistant.core import Context, HomeAssistant
from homeassistant.helpers import intent

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class FastQuery:
    intent_type: str
    handler: Any
    slots: dict[str, Any]
    text: str
    language: str


def _speech_handlers(hass: HomeAssistant) -> dict[str, Any]:
    try:
        from homeassistant.components.intent_script.helpers import ScriptIntentHandler
    except ImportError:
        try:
            from homeassistant.components.intent_script import ScriptIntentHandler
        except ImportError:
            return {}
    return {
        h.intent_type: h
        for h in intent.async_get(hass)
        if type(h) is ScriptIntentHandler
        and "speech" in h.config
        and not any(key in h.config for key in ("action", "reprompt", "card"))
    }


async def recognize_fast_query(hass: HomeAssistant, text: str) -> FastQuery | None:
    """Recognize without executing an intent or mutating a chat log."""
    try:
        before = _speech_handlers(hass)
        if not before or not text.strip():
            return None
        agent = conversation.async_get_agent(hass, "conversation.home_assistant")
        recognize = getattr(agent, "async_recognize_intent", None)
        # Avoid a trigger-recognition API whose older implementations may
        # run callbacks. If triggers are configured or the registry layout is
        # unknown, conservatively use the ordinary conversation lane.
        trigger_config = getattr(agent, "_trigger_intents_config", None)
        if not callable(recognize) or not isinstance(trigger_config, dict) or trigger_config.get("intents"):
            return None
        user_input = conversation.ConversationInput(
            text=text, context=Context(), conversation_id=None,
            device_id=None, satellite_id=None, language=hass.config.language,
            agent_id="conversation.home_assistant",
        )
        result = await recognize(user_input, strict_intents_only=True)
        if result is None or result.unmatched_entities:
            return None
        name = result.intent.name
        handler = before.get(name)
        if handler is None or _speech_handlers(hass).get(name) is not handler:
            return None
        slots = {
            item.name: {"value": item.value, "text": item.text or item.value}
            for item in result.entities_list
        }
        return FastQuery(name, handler, slots, text, hass.config.language)
    except Exception:
        _LOGGER.debug("Read-only fast recognition unavailable", exc_info=True)
        return None


async def execute_fast_query(hass: HomeAssistant, query: FastQuery) -> str:
    """Execute only the recognized handler, never retry via an action agent."""
    if _speech_handlers(hass).get(query.intent_type) is not query.handler:
        return "本地查询配置已变更，请重新发送。"
    # async_handle resolves the registered handler before its first await.
    response = await intent.async_handle(
        hass, "conversation", query.intent_type, query.slots,
        query.text, Context(), query.language,
        assistant="conversation", conversation_agent_id="conversation.home_assistant",
    )
    speech = response.speech.get("plain", {}).get("speech")
    if response.response_type == intent.IntentResponseType.ERROR or not isinstance(speech, str) or not speech.strip():
        return "本地查询暂时无法返回有效结果，请稍后重试。"
    return speech.strip()
