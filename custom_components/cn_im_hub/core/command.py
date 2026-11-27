from __future__ import annotations

from homeassistant.core import HomeAssistant
from homeassistant.helpers.network import get_url

from .conversation import ask_home_assistant
from ..models import Command, command_factory


def parse_command(text: str) -> Command | None:
    text = text.strip()
    return command_factory("conversation", text) if text else None


_IM_PREFIXES = {
    "wechat:": "WeChat",
    "feishu:": "Feishu",
    "dingtalk:": "DingTalk",
    "qq:": "QQ",
    "wecom:": "WeCom",
    "xiaoyi:": "XiaoYi",
}


def _resolve_im_base_url(hass: HomeAssistant) -> str:
    try:
        return get_url(hass, allow_internal=True, allow_external=True, prefer_external=False).rstrip("/")
    except Exception:
        return ""


def im_public_url_for_source(hass: HomeAssistant, source: str) -> str:
    candidate = source.strip()
    if candidate.startswith(("http://", "https://")):
        return candidate
    base_url = _resolve_im_base_url(hass)
    if not base_url:
        return candidate
    marker = "/config/www/"
    if marker in candidate:
        return f"{base_url}/local/{candidate.split(marker, 1)[1].lstrip('/')}"
    if candidate.startswith("/local/"):
        return f"{base_url}{candidate}"
    if candidate.startswith("/config/"):
        return f"{base_url}/local/{candidate.removeprefix('/config/www/').lstrip('/')}"
    return candidate


def _resolve_channel_context(
    hass: HomeAssistant,
    conversation_id: str,
    extra_system_prompt: str | None,
) -> str | None:
    for prefix, name in _IM_PREFIXES.items():
        if conversation_id.startswith(prefix):
            base_url = _resolve_im_base_url(hass)
            media_hint = (
                "\n\n## IM Media URL Rules\n"
                "- For IM channels, media tag sources must be network-accessible URLs.\n"
                "- Never output server-local filesystem paths such as `/Users/.../config/www/...` or `/config/www/...` inside `[IMAGE:...]`, `[VIDEO:...]`, `[GIF:...]`, or `[FILE:...]`.\n"
                "- If a generated file is under Home Assistant `config/www/claw_assistant/`, convert it to a full URL before using it in a media tag.\n"
                f"- Correct format: `[IMAGE:{base_url}/local/claw_assistant/file.png]`.\n"
                f"- Example: `/Users/knoopnu/core/config/www/claw_assistant/ha_homepage.png` must become `{base_url}/local/claw_assistant/ha_homepage.png`.\n"
            ) if base_url else (
                "\n\n## IM Media URL Rules\n"
                "- For IM channels, media tag sources must be network-accessible URLs.\n"
                "- Never output server-local filesystem paths such as `/Users/.../config/www/...` or `/config/www/...` inside `[IMAGE:...]`, `[VIDEO:...]`, `[GIF:...]`, or `[FILE:...]`.\n"
                "- Files under Home Assistant `config/www/` must be converted to `/local/...` URLs before use.\n"
            )
            channel_hint = f"Current IM channel: {name} (conversation_id={conversation_id}){media_hint}"
            return f"{channel_hint}\n\n{extra_system_prompt}" if extra_system_prompt else channel_hint
    return extra_system_prompt


async def execute_command(
    hass: HomeAssistant,
    command: Command,
    *,
    conversation_id: str,
    agent_id: str | None,
    extra_system_prompt: str | None = None,
    user_id: str = "",
) -> str:
    if command.kind == "conversation":
        return await ask_home_assistant(
            hass,
            command.target,
            conversation_id=conversation_id,
            agent_id=agent_id,
            extra_system_prompt=_resolve_channel_context(hass, conversation_id, extra_system_prompt),
            retry_after_dispatch=not conversation_id.startswith("wechat:"),
        )
    return "Only natural language conversations are currently supported."
