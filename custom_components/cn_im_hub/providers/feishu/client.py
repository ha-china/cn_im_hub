from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any

from homeassistant.core import HomeAssistant, callback
import voluptuous as vol

EVENT_LIVE_PROGRESS = "ha_crack_live_progress"
_LIVE_PROGRESS_SEND_INTERVAL_SECONDS = 2.0

from ...core.command import execute_command, parse_command
from ...core.known_targets import async_get_tracker
from ...const import (
    CONF_FEISHU_APP_ID,
    CONF_FEISHU_APP_SECRET,
    DEFAULT_FEISHU_TARGET_TYPE,
    PROVIDER_FEISHU,
)
from ...media.rich_media import (
    FileSegment,
    GifSegment,
    ImageSegment,
    TextSegment,
    VideoSegment,
    VoiceSegment,
    extract_reply_prefix,
    parse_reply_segments,
)
from .prompt import build_feishu_prompt
from ...models import ProviderRuntime
from ..base import ProviderSpec
from .api import FeishuApiClient
from .ws import FeishuWsClient

_LOGGER = logging.getLogger(__name__)


async def async_validate_config(hass: HomeAssistant, config: dict[str, Any]) -> None:
    app_id, app_secret = _credentials(config)
    if not app_id or not app_secret:
        raise ValueError("app_id and app_secret are required")
    await FeishuApiClient(hass, app_id, app_secret).async_validate_connection()


async def async_setup_provider(
    hass: HomeAssistant,
    config: dict[str, Any],
    *,
    agent_id: str,
    subentry_id: str,
) -> ProviderRuntime:
    app_id, app_secret = _credentials(config)
    show_live_progress = bool(config.get(_CONF_FEISHU_SHOW_LIVE_PROGRESS, False))
    api = FeishuApiClient(hass, app_id, app_secret)
    await api.async_validate_connection()
    tracker = await async_get_tracker(hass, subentry_id)
    ws = FeishuWsClient(
        hass=hass,
        app_id=app_id,
        app_secret=app_secret,
        message_handler=_message_handler_factory(hass, api, tracker, agent_id, show_live_progress),
    )
    await ws.async_start()
    return _runtime_factory(ws, api, tracker, subentry_id, app_id)


def _credentials(config: dict[str, Any]) -> tuple[str, str]:
    return str(config.get(CONF_FEISHU_APP_ID, "")).strip(), str(config.get(CONF_FEISHU_APP_SECRET, "")).strip()


def _format_live_progress(payload: dict[str, Any]) -> str:
    display_text = str(payload.get("display_text") or "").strip()
    if display_text:
        cleaned = display_text.replace("┊", "").replace("*", "").strip()
        return cleaned[:200]
    tool_name = str(payload.get("tool_name") or "").strip()
    if tool_name:
        return f"🔧 {tool_name}"
    return ""


def _message_handler_factory(hass, api, tracker, agent_id, show_live_progress: bool = False):
    async def _run_live_progress_bridge(conversation_id: str, receive_id: str, receive_type: str) -> None:
        if not show_live_progress:
            await asyncio.Future()

        queue: asyncio.Queue[str] = asyncio.Queue()

        @callback
        def _listener(event) -> None:
            payload = event.data or {}
            if payload.get("conversation_id") != conversation_id:
                return
            text = _format_live_progress(payload)
            if text:
                queue.put_nowait(text)

        unsub = hass.bus.async_listen(EVENT_LIVE_PROGRESS, _listener)
        last_sent = ""
        pending_tasks: list[asyncio.Task] = []
        
        async def _fire_and_forget(msg: str) -> None:
            with contextlib.suppress(Exception):
                await _reply(api, receive_id, receive_type, msg)
        
        try:
            while True:
                text = await queue.get()
                if text == last_sent:
                    continue
                task = asyncio.create_task(_fire_and_forget(text))
                pending_tasks.append(task)
                last_sent = text
        except asyncio.CancelledError:
            if pending_tasks:
                await asyncio.gather(*pending_tasks, return_exceptions=True)
        finally:
            unsub()

    async def _handle_message(message: dict[str, str]) -> None:
        chat_id = message.get("chat_id", "")
        user_id = message.get("user_id", "")
        text = message.get("text", "").strip()
        receive_id = chat_id or user_id
        receive_type = "chat_id" if chat_id else "open_id"
        if not receive_id or not text:
            return
        await tracker.async_record(provider=PROVIDER_FEISHU, target=receive_id, target_type=receive_type, display_name=user_id or chat_id)
        try:
            command = parse_command(text)
        except ValueError as err:
            await _reply(api, receive_id, receive_type, f"Invalid command: {err}")
            return
        if command is None:
            return

        conversation_id = f"feishu:{receive_id}"
        progress_task = asyncio.create_task(_run_live_progress_bridge(conversation_id, receive_id, receive_type))

        try:
            result = await execute_command(
                hass,
                command,
                conversation_id=conversation_id,
                agent_id=agent_id,
                extra_system_prompt=build_feishu_prompt(),
                user_id=user_id or receive_id,
            )
        except Exception as err:
            result = f"Execution failed: {type(err).__name__}"
            _LOGGER.exception("Feishu command execution failed: %s", err)
        finally:
            progress_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await progress_task
        reply = str(result)
        if not reply:
            return
        _, reply_body = extract_reply_prefix(reply)
        segments = parse_reply_segments(reply_body)
        for seg in segments:
            if isinstance(seg, TextSegment):
                await _reply(api, receive_id, receive_type, seg.text)
            elif isinstance(seg, ImageSegment):
                try:
                    image_bytes = await _resolve_image(hass, seg.source)
                    if image_bytes:
                        await api.async_send_image_message(
                            receive_id=receive_id,
                            image_bytes=image_bytes,
                            receive_id_type=receive_type,
                        )
                except Exception as err:
                    _LOGGER.warning("Feishu image send failed: %s", err)
                    await _reply(api, receive_id, receive_type, f"Image send failed: {err}")
            elif isinstance(seg, VideoSegment):
                try:
                    result = await _resolve_video(hass, seg.source)
                    if result:
                        video_bytes, file_name = result
                        await api.async_send_video_message(
                            receive_id=receive_id,
                            video_bytes=video_bytes,
                            file_name=file_name,
                            receive_id_type=receive_type,
                        )
                    else:
                        await _reply(api, receive_id, receive_type, f"📎 {seg.source}")
                except Exception as err:
                    _LOGGER.warning("Feishu video send failed: %s", err)
                    await _reply(api, receive_id, receive_type, f"📎 {seg.source}")
            elif isinstance(seg, GifSegment):
                try:
                    result = await _resolve_gif(hass, seg.source)
                    if result:
                        gif_bytes, file_name = result
                        await api.async_send_image_message(
                            receive_id=receive_id,
                            image_bytes=gif_bytes,
                            receive_id_type=receive_type,
                        )
                    else:
                        await _reply(api, receive_id, receive_type, f"📎 {seg.source}")
                except Exception as err:
                    _LOGGER.warning("Feishu gif send failed: %s", err)
                    await _reply(api, receive_id, receive_type, f"📎 {seg.source}")
            elif isinstance(seg, FileSegment):
                try:
                    media_bytes = await _resolve_media(hass, seg.source)
                    if media_bytes:
                        name = seg.source.rsplit("/", 1)[-1] or "file"
                        await api.async_send_file_message(
                            receive_id=receive_id,
                            file_bytes=media_bytes,
                            file_name=name,
                            receive_id_type=receive_type,
                        )
                    else:
                        await _reply(api, receive_id, receive_type, f"📎 {seg.source}")
                except Exception as err:
                    _LOGGER.warning("Feishu file send failed: %s", err)
                    await _reply(api, receive_id, receive_type, f"📎 {seg.source}")
            elif isinstance(seg, VoiceSegment):
                try:
                    from ...media.tts import async_generate_tts_mp3, is_edge_tts_available
                    if is_edge_tts_available():
                        mp3_bytes = await async_generate_tts_mp3(hass, seg.text)
                        await api.async_send_file_message(
                            receive_id=receive_id,
                            file_bytes=mp3_bytes,
                            file_name="voice.mp3",
                            receive_id_type=receive_type,
                        )
                except Exception as err:
                    _LOGGER.warning("Feishu voice send failed: %s", err)
                    await _reply(api, receive_id, receive_type, seg.text)
    return _handle_message


async def _resolve_media(hass: HomeAssistant, source: str) -> bytes | None:
    import os
    from homeassistant.helpers.aiohttp_client import async_get_clientsession
    if source.startswith(("http://", "https://")):
        session = async_get_clientsession(hass)
        async with session.get(source) as resp:
            if resp.status == 200:
                return await resp.read()
    elif await hass.async_add_executor_job(os.path.isfile, source):
        return await hass.async_add_executor_job(_read_file, source)
    return None


def _read_file(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()


async def _resolve_image(hass: HomeAssistant, source: str) -> bytes | None:
    from ...media.camera import async_resolve_camera_entity
    resolved = await async_resolve_camera_entity(hass, source)
    _LOGGER.debug("_resolve_image source=%s resolved=%s", source, resolved)
    if resolved is not None:
        from homeassistant.components.camera import async_get_image
        image = await async_get_image(hass, resolved)
        return image.content
    return await _resolve_media(hass, source)


async def _resolve_video(hass: HomeAssistant, source: str) -> tuple[bytes, str] | None:
    from ...media.camera import async_resolve_camera_entity, async_record_camera_clip
    resolved = await async_resolve_camera_entity(hass, source)
    if resolved is not None:
        return await async_record_camera_clip(hass, resolved)
    data = await _resolve_media(hass, source)
    if data:
        name = source.rsplit("/", 1)[-1] or "video.mp4"
        return data, name
    return None


async def _resolve_gif(hass: HomeAssistant, source: str) -> tuple[bytes, str] | None:
    from ...media.camera import async_resolve_camera_entity, async_capture_camera_gif
    resolved = await async_resolve_camera_entity(hass, source)
    if resolved is not None:
        return await async_capture_camera_gif(hass, resolved)
    data = await _resolve_media(hass, source)
    if data:
        name = source.rsplit("/", 1)[-1] or "image.gif"
        return data, name
    return None


async def _reply(api: FeishuApiClient, receive_id: str, receive_type: str, text: str) -> None:
    await api.async_send_safe_reply(receive_id=receive_id, receive_id_type=receive_type, text=text)


def _runtime_factory(ws, api, tracker, subentry_id: str, app_id: str = "") -> ProviderRuntime:
    async def _send(target: str, message: str, target_type: str) -> None:
        await api.async_send_text_message(receive_id=target, text=message, receive_id_type=target_type or DEFAULT_FEISHU_TARGET_TYPE)

    async def _send_image(target: str, image_bytes: bytes, target_type: str) -> None:
        await api.async_send_image_message(receive_id=target, image_bytes=image_bytes, receive_id_type=target_type or DEFAULT_FEISHU_TARGET_TYPE)

    async def _send_video(target: str, video_bytes: bytes, filename: str, target_type: str) -> None:
        await api.async_send_video_message(receive_id=target, video_bytes=video_bytes, file_name=filename, receive_id_type=target_type or DEFAULT_FEISHU_TARGET_TYPE)

    async def _send_file(target: str, file_bytes: bytes, filename: str, target_type: str) -> None:
        await api.async_send_file_message(receive_id=target, file_bytes=file_bytes, file_name=filename, receive_id_type=target_type or DEFAULT_FEISHU_TARGET_TYPE)

    return ProviderRuntime(
        key=PROVIDER_FEISHU,
        title=PROVIDER_FEISHU,
        subentry_id=subentry_id,
        client=ws,
        stop=ws.async_stop,
        send_text=_send,
        status=lambda: ws.status,
        known_targets=tracker.snapshot,
        selected_target=tracker.selected_target,
        select_target=tracker.async_select_target,
        send_image=_send_image,
        send_video=_send_video,
        send_file=_send_file,
    )


_CONF_FEISHU_SHOW_LIVE_PROGRESS = "feishu_show_live_progress"


def _build_schema(current: dict[str, Any]) -> vol.Schema:
    return vol.Schema(
        {
            vol.Required(CONF_FEISHU_APP_ID, default=current.get(CONF_FEISHU_APP_ID, "")): str,
            vol.Required(CONF_FEISHU_APP_SECRET, default=current.get(CONF_FEISHU_APP_SECRET, "")): str,
            vol.Optional(_CONF_FEISHU_SHOW_LIVE_PROGRESS, default=current.get(_CONF_FEISHU_SHOW_LIVE_PROGRESS, False)): bool,
        }
    )


PROVIDER_SPEC = ProviderSpec(
    key=PROVIDER_FEISHU,
    schema_builder=_build_schema,
    validate_config=async_validate_config,
    setup_provider=async_setup_provider,
    allow_multiple=True,
)
