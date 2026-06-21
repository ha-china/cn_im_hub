from __future__ import annotations

import json as _json
import logging
from pathlib import Path
from typing import Any

from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers import config_validation as cv
import voluptuous as vol

from ..const import (
    ATTR_APPROVAL_ID,
    ATTR_CAMERA_ENTITY,
    ATTR_CHANNEL,
    ATTR_FILE_NAME,
    ATTR_FILE_PATH,
    ATTR_FILE_URL,
    ATTR_GIF_FPS,
    ATTR_LOOKBACK,
    ATTR_MESSAGE,
    ATTR_MESSAGE_FORMAT,
    ATTR_MEDIA_TYPE,
    ATTR_RECORD_DURATION,
    ATTR_TARGET,
    ATTR_TTS_TEXT,
    ATTR_WECHAT_ACCOUNT_ID,
    CHANNEL_FEISHU_CHAT_ID,
    CHANNEL_OPTIONS,
    DEFAULT_GIF_DURATION,
    DEFAULT_VIDEO_RECORD_DURATION,
    DOMAIN,
    PROVIDER_WECHAT,
    SERVICE_SEND_MESSAGE,
)
from ..media.camera import (
    async_capture_camera_gif,
    async_record_camera_clip,
    async_resolve_camera_entity,
    resolve_ha_local_path,
)
from .routing import (
    all_provider_runtimes,
    parse_channel,
    select_provider_runtime,
    select_wechat_runtime,
)

_LOGGER = logging.getLogger(__name__)

_SUFFIX_TYPE_MAP: dict[str, str] = {
    **dict.fromkeys((".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"), "image"),
    **dict.fromkeys((".mp3", ".wav", ".silk", ".ogg", ".amr", ".m4a"), "voice"),
    **dict.fromkeys((".mp4", ".mov", ".mkv", ".webm", ".avi", ".m4v"), "video"),
}

SERVICE_SCHEMA = vol.Schema(
    {
        vol.Required(ATTR_CHANNEL, default=CHANNEL_FEISHU_CHAT_ID): vol.In(CHANNEL_OPTIONS),
        vol.Optional(ATTR_MESSAGE, default=""): cv.string,
        vol.Optional(ATTR_TARGET, default=""): cv.string,
        vol.Optional(ATTR_WECHAT_ACCOUNT_ID, default=""): cv.string,
        vol.Optional(ATTR_CAMERA_ENTITY, default=""): vol.Any(None, "", cv.entity_id),
        vol.Optional(ATTR_FILE_PATH, default=""): cv.string,
        vol.Optional(ATTR_FILE_URL, default=""): cv.string,
        vol.Optional(ATTR_FILE_NAME, default=""): cv.string,
        vol.Optional(ATTR_MEDIA_TYPE, default=""): vol.Any("", vol.In(["image", "gif", "voice", "video", "file"])),
        vol.Optional(ATTR_TTS_TEXT, default=""): cv.string,
        vol.Optional(ATTR_MESSAGE_FORMAT, default=""): vol.Any("", vol.In(["auto", "text", "markdown"])),
        vol.Optional(ATTR_APPROVAL_ID, default=""): cv.string,
        vol.Optional(ATTR_RECORD_DURATION): vol.Coerce(int),
        vol.Optional(ATTR_LOOKBACK, default=0): vol.Coerce(int),
        vol.Optional(ATTR_GIF_FPS, default=2): vol.Coerce(int),
    }
)


def _infer_media_type(file_path: str, file_url: str, explicit: str) -> str:
    if explicit:
        return explicit
    suffix = Path((file_path or file_url).split("?", 1)[0]).suffix.lower() if (file_path or file_url) else ""
    return _SUFFIX_TYPE_MAP.get(suffix, "file")


async def _read_media_source(hass: HomeAssistant, file_path: str, file_url: str) -> tuple[bytes, str]:
    if file_path:
        path = resolve_ha_local_path(hass, file_path)
        if path is None:
            raise ValueError(f"file_path not found: {file_path}")
        return await hass.async_add_executor_job(path.read_bytes), path.name

    if file_url:
        from homeassistant.helpers.aiohttp_client import async_get_clientsession
        session = async_get_clientsession(hass)
        async with session.get(file_url, timeout=60) as resp:
            if resp.status >= 400:
                raise ValueError(f"file_url download failed: {resp.status}")
            return await resp.read(), Path(file_url.split("?", 1)[0]).name or "attachment.bin"

    raise ValueError("file_path or file_url is required")


def _extract_call_data(call: ServiceCall) -> dict[str, Any]:
    d = call.data
    _s = lambda key, default="": str(d.get(key, default)).strip()
    return {
        "channel": str(d.get(ATTR_CHANNEL, CHANNEL_FEISHU_CHAT_ID)),
        "target": _s(ATTR_TARGET),
        "message": _s(ATTR_MESSAGE),
        "camera_entity": _s(ATTR_CAMERA_ENTITY),
        "file_path": _s(ATTR_FILE_PATH),
        "file_url": _s(ATTR_FILE_URL),
        "file_name": _s(ATTR_FILE_NAME),
        "media_type": _s(ATTR_MEDIA_TYPE).lower(),
        "tts_text": _s(ATTR_TTS_TEXT),
        "message_format": _s(ATTR_MESSAGE_FORMAT).lower(),
        "approval_id": _s(ATTR_APPROVAL_ID),
        "record_duration": (int(v) if (v := d.get(ATTR_RECORD_DURATION)) not in (None, "") else None),
        "lookback": int(d.get(ATTR_LOOKBACK, 0) or 0),
        "gif_fps": int(d.get(ATTR_GIF_FPS, 2) or 2),
        "wechat_account_id": _s(ATTR_WECHAT_ACCOUNT_ID),
    }


async def _handle_camera(hass, provider, requested, p, resolved_target, target_type):
    resolved = await async_resolve_camera_entity(hass, p["camera_entity"])
    if resolved is None:
        raise ValueError(f"camera source not found: {p['camera_entity']}")

    handlers = {
        "video": lambda: _camera_video(hass, provider, requested, p, resolved, resolved_target, target_type),
        "gif": lambda: _camera_gif(hass, provider, requested, p, resolved, resolved_target, target_type),
    }
    is_gif = p["media_type"] == "gif" or (p["media_type"] == "image" and p["file_name"].lower().endswith(".gif"))
    handler = handlers.get("gif" if is_gif else p["media_type"])
    if handler:
        await handler()
    else:
        await _camera_snapshot(hass, provider, requested, resolved, resolved_target, target_type)
    if p["message"]:
        await provider.send_text(resolved_target, p["message"], target_type)


async def _camera_video(hass, provider, requested, p, cam, target, ttype):
    if provider.send_media is None:
        raise ValueError(f"Provider '{requested}' does not support video sending")
    video_bytes, name = await async_record_camera_clip(
        hass, cam, duration=p["record_duration"] or DEFAULT_VIDEO_RECORD_DURATION, lookback=p["lookback"],
    )
    await provider.send_media(target, video_bytes, "video", ttype, p["file_name"] or name)


async def _camera_gif(hass, provider, requested, p, cam, target, ttype):
    if provider.send_image is None:
        raise ValueError(f"Provider '{requested}' does not support GIF sending")
    gif_bytes, _ = await async_capture_camera_gif(
        hass, cam, duration=p["record_duration"] or DEFAULT_GIF_DURATION, fps=p["gif_fps"],
    )
    await provider.send_image(target, gif_bytes, ttype)


async def _camera_snapshot(hass, provider, requested, cam, target, ttype):
    if provider.send_image is None:
        raise ValueError(f"Provider '{requested}' does not support camera image sending")
    from homeassistant.components.camera import async_get_image
    image = await async_get_image(hass, cam)
    await provider.send_image(target, image.content, ttype)


async def _handle_file(hass, provider, requested, p, resolved_target, target_type):
    if provider.send_media is None:
        raise ValueError(f"Provider '{requested}' does not support media sending")
    resolved_type = _infer_media_type(p["file_path"], p["file_url"], p["media_type"])
    media_bytes, detected_name = await _read_media_source(hass, p["file_path"], p["file_url"])
    await provider.send_media(resolved_target, media_bytes, resolved_type, target_type, p["file_name"] or detected_name)
    if p["message"]:
        await provider.send_text(resolved_target, p["message"], target_type)


async def _handle_send_message(hass: HomeAssistant, call: ServiceCall) -> None:
    p = _extract_call_data(call)
    has_content = p["message"] or p["camera_entity"] or p["file_path"] or p["file_url"] or p["tts_text"]
    if not has_content:
        return

    requested, target_type = parse_channel(p["channel"])
    resolved_target = p["target"]
    providers = all_provider_runtimes(hass, requested)
    if not providers:
        _LOGGER.error("No matched provider runtime for send_message")
        return

    provider = (
        select_wechat_runtime(providers, wechat_account_id=p["wechat_account_id"], explicit_target=resolved_target)
        if requested == PROVIDER_WECHAT
        else select_provider_runtime(providers, explicit_target=resolved_target)
    )
    if provider is None:
        raise ValueError(f"Provider '{requested}' is ambiguous. Provide a target or ensure only one selector is active.")

    resolved_target = resolved_target or provider.selected_target()
    if not resolved_target:
        raise ValueError("target is required, or select a known target in the provider target selector entity")

    dispatch: list[tuple[bool, Any]] = [
        (bool(p["approval_id"]), lambda: _dispatch_approval(provider, requested, p, resolved_target, target_type)),
        (bool(p["tts_text"]), lambda: _dispatch_tts(provider, requested, p, resolved_target, target_type)),
        (bool(p["camera_entity"]), lambda: _handle_camera(hass, provider, requested, p, resolved_target, target_type)),
        (bool(p["file_path"] or p["file_url"]), lambda: _handle_file(hass, provider, requested, p, resolved_target, target_type)),
    ]

    for condition, handler in dispatch:
        if condition:
            await handler()
            return

    if p["message_format"] and requested == "qq":
        sender = getattr(getattr(provider, "client", None), "send_text_formatted", None)
        if callable(sender):
            await sender(resolved_target, p["message"], target_type, p["message_format"])
            return
    await provider.send_text(resolved_target, p["message"], target_type)


async def _dispatch_approval(provider, requested, p, target, ttype):
    if provider.send_approval is None:
        raise ValueError(f"Provider '{requested}' does not support approval buttons")
    if not p["message"]:
        raise ValueError("message is required when approval_id is provided")
    await provider.send_approval(target, p["message"], ttype, p["approval_id"])


async def _dispatch_tts(provider, requested, p, target, ttype):
    if provider.send_tts is None:
        raise ValueError(f"Provider '{requested}' does not support TTS sending")
    await provider.send_tts(target, p["tts_text"], ttype)
    if p["message"]:
        await provider.send_text(target, p["message"], ttype)


def register_services(hass: HomeAssistant) -> None:
    async def _service_handler(call: ServiceCall) -> None:
        await _handle_send_message(hass, call)

    if hass.services.has_service(DOMAIN, SERVICE_SEND_MESSAGE):
        return
    hass.services.async_register(
        DOMAIN,
        SERVICE_SEND_MESSAGE,
        _service_handler,
        schema=SERVICE_SCHEMA,
    )
