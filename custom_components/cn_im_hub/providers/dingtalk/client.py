"""DingTalk provider using Stream mode (no HTTP callback)."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any

import contextlib

import aiohttp
import voluptuous as vol
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession

EVENT_LIVE_PROGRESS = "ha_crack_live_progress"
_LIVE_PROGRESS_SEND_INTERVAL_SECONDS = 2.0

from ...core.command import execute_command, im_public_url_for_source, parse_command
from ...const import (
    CONF_DINGTALK_CLIENT_ID,
    CONF_DINGTALK_CLIENT_SECRET,
    DINGTALK_API_BASE,
    DINGTALK_OAUTH_URL,
    DINGTALK_OAPI_BASE,
    PROVIDER_DINGTALK,
)
from ...core.known_targets import async_get_tracker
from ...media.rich_media import (
    FileSegment,
    ImageSegment,
    TextSegment,
    VideoSegment,
    VoiceSegment,
    parse_reply_segments,
)
from ...models import ProviderRuntime
from ..base import ProviderSpec
from .flow import DingtalkProviderSubentryFlow
from .prompt import build_dingtalk_prompt

_LOGGER = logging.getLogger(__name__)
_OAUTH_URL = DINGTALK_OAUTH_URL
_API_BASE = DINGTALK_API_BASE
_OAPI_BASE = DINGTALK_OAPI_BASE
_DINGTALK_MEDIA_MAX_BYTES = 20 * 1024 * 1024
_DINGTALK_MEDIA_TYPE_LIMITS = {"image": 10 * 1024 * 1024, "voice": 2 * 1024 * 1024}


def _extract_stream_text(data: dict[str, Any]) -> str:
    msgtype = str(data.get("msgtype") or "text").strip().lower()
    if msgtype == "text":
        return str(((data.get("text") or {}).get("content") or "")).strip()
    if msgtype == "audio":
        content = data.get("content") if isinstance(data.get("content"), dict) else {}
        recognition = str(content.get("recognition") or (data.get("audio") or {}).get("recognition") or "").strip()
        return recognition
    return ""


def _extract_stream_sender_and_target(data: dict[str, Any]) -> tuple[str, str, str]:
    sender_id = str(data.get("senderStaffId") or data.get("sender_staff_id") or data.get("senderId") or data.get("sender_id") or "").strip()
    conversation_id = str(data.get("conversationId") or data.get("conversation_id") or "").strip()
    display_name = str(data.get("senderNick") or data.get("sender_nick") or sender_id or conversation_id).strip()
    # conversationType: "1" = single chat, "2" = group chat
    conversation_type = str(data.get("conversationType") or "").strip()
    if conversation_type == "2":
        # Group chat: use conversation_id as target
        return conversation_id or "group", "group", display_name
    if sender_id:
        return sender_id, "user", display_name
    return conversation_id or "group", "group", display_name


class DingTalkClient:
    def __init__(self, hass: HomeAssistant, client_id: str, client_secret: str, agent_id: str) -> None:
        self._hass = hass
        self._session = async_get_clientsession(hass)
        self._client_id = client_id
        self._client_secret = client_secret
        self._agent_id = agent_id
        self._status = "disconnected"
        self._task: asyncio.Task[None] | None = None
        self._token = ""
        self._token_expire = 0.0
        self._oapi_token = ""
        self._oapi_token_expire = 0.0

    @property
    def status(self) -> str:
        return self._status

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run_stream())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        self._status = "disconnected"

    async def send_text(self, target: str, text: str, target_type: str, at_user_ids: list[str] | None = None) -> None:
        token = await self._get_token()
        target = target.strip()
        if not target:
            raise ValueError("DingTalk target is required")

        content = text
        if at_user_ids:
            for uid in at_user_ids:
                if f"@{uid}" not in content:
                    content = f"{content} @{uid}"

        msg_param = json.dumps({"content": content}, ensure_ascii=False)

        if target_type == "user":
            path = "/v1.0/robot/oToMessages/batchSend"
            body = {
                "robotCode": self._client_id,
                "userIds": [target],
                "msgKey": "sampleText",
                "msgParam": msg_param,
            }
        else:
            path = "/v1.0/robot/groupMessages/send"
            body = {
                "robotCode": self._client_id,
                "openConversationId": target,
                "msgKey": "sampleText",
                "msgParam": msg_param,
            }

        async with self._session.post(
            f"{_API_BASE}{path}",
            headers={"x-acs-dingtalk-access-token": token},
            json=body,
            timeout=15,
        ) as resp:
            if resp.status >= 400:
                raise RuntimeError(f"DingTalk send failed: {resp.status} {await resp.text()}")
            data = await resp.json(content_type=None)
            # DingTalk returns HTTP 200 even on business failure; check processQueryKey.
            if not data.get("processQueryKey"):
                raise RuntimeError(f"DingTalk send failed (no processQueryKey): {data}")

    async def send_image(self, target: str, image_bytes: bytes, target_type: str) -> None:
        token = await self._get_token()
        media_id = await self._upload_image(image_bytes)
        target = target.strip()
        if not target:
            raise ValueError("DingTalk target is required")

        msg_param = json.dumps({"photoURL": media_id}, ensure_ascii=False)

        if target_type == "user":
            path = "/v1.0/robot/oToMessages/batchSend"
            body = {
                "robotCode": self._client_id,
                "userIds": [target],
                "msgKey": "sampleImageMsg",
                "msgParam": msg_param,
            }
        else:
            path = "/v1.0/robot/groupMessages/send"
            body = {
                "robotCode": self._client_id,
                "openConversationId": target,
                "msgKey": "sampleImageMsg",
                "msgParam": msg_param,
            }

        async with self._session.post(
            f"{_API_BASE}{path}",
            headers={"x-acs-dingtalk-access-token": token},
            json=body,
            timeout=30,
        ) as resp:
            if resp.status >= 400:
                error_text = await resp.text()
                _LOGGER.error("DingTalk image send failed: %s, response: %s", resp.status, error_text)
                raise RuntimeError(f"DingTalk image send failed: {resp.status} {error_text}")

    async def _run_stream(self) -> None:
        """Use official Stream SDK if available, without webhook mode."""
        self._status = "connecting"
        try:
            import dingtalk_stream

            outer = self
            seen_msg_ids: dict[str, None] = {}
            _MAX_SEEN_MSG_IDS = 512

            class _Handler(dingtalk_stream.ChatbotHandler):
                async def process(self, callback):
                    raw_data = callback.data if isinstance(callback.data, dict) else {}
                    incoming = dingtalk_stream.ChatbotMessage.from_dict(callback.data)

                    # Dedup on msgId — DingTalk redelivers if no ack/business reply
                    # arrives within ~60s. We ack immediately to prevent redelivery.
                    msg_id = str(raw_data.get("msgId") or "").strip()
                    if msg_id:
                        if msg_id in seen_msg_ids:
                            return dingtalk_stream.AckMessage.STATUS_OK, "OK"
                        seen_msg_ids[msg_id] = None
                        if len(seen_msg_ids) > _MAX_SEEN_MSG_IDS:
                            seen_msg_ids.pop(next(iter(seen_msg_ids)), None)

                    text = _extract_stream_text(raw_data)
                    if not text:
                        return dingtalk_stream.AckMessage.STATUS_OK, "OK"

                    sender_id, target_type, display_name = _extract_stream_sender_and_target(raw_data)
                    try:
                        command = parse_command(text)
                    except ValueError as err:
                        self.reply_text(f"Invalid command: {err}", incoming)
                        return dingtalk_stream.AckMessage.STATUS_OK, "OK"

                    if command is None:
                        return dingtalk_stream.AckMessage.STATUS_OK, "OK"

                    # Ack immediately, then execute asynchronously so the SDK
                    # thread is not blocked by long-running commands.
                    async def _execute_and_reply() -> None:
                        try:
                            reply = await execute_command(
                                outer._hass,
                                command,
                                conversation_id=f"dingtalk:stream:{sender_id or target_type}",
                                agent_id=outer._agent_id,
                                user_id=display_name or sender_id,
                            )
                        except Exception as err:
                            _LOGGER.warning("DingTalk command execution failed: %s", err)
                            reply = f"Execution failed: {type(err).__name__}"
                        if reply:
                            try:
                                self.reply_text(reply, incoming)
                            except Exception as err:
                                _LOGGER.warning("DingTalk reply_text failed: %s", err)

                    asyncio.run_coroutine_threadsafe(_execute_and_reply(), outer._hass.loop)
                    return dingtalk_stream.AckMessage.STATUS_OK, "OK"

            credential = dingtalk_stream.Credential(self._client_id, self._client_secret)
            client = dingtalk_stream.DingTalkStreamClient(credential)
            client.register_callback_handler(dingtalk_stream.chatbot.ChatbotMessage.TOPIC, _Handler())
            self._status = "connected"
            await asyncio.to_thread(client.start_forever)
        except asyncio.CancelledError:
            raise
        except Exception as err:
            _LOGGER.warning("DingTalk stream not started: %s", err)
            self._status = "error"

    async def _get_token(self) -> str:
        now = asyncio.get_running_loop().time()
        if self._token and now < self._token_expire - 300:
            return self._token

        async with self._session.post(
            _OAUTH_URL,
            json={"appKey": self._client_id, "appSecret": self._client_secret},
            timeout=15,
        ) as resp:
            data = await resp.json(content_type=None)
            if resp.status >= 400:
                raise RuntimeError(f"DingTalk token fetch failed: {resp.status} {data}")

        token = str(data.get("accessToken") or "")
        if not token:
            raise RuntimeError(f"DingTalk accessToken missing: {data}")
        self._token = token
        self._token_expire = now + int(data.get("expireIn") or 7200)
        return token

    async def _get_oapi_token(self) -> str:
        now = asyncio.get_running_loop().time()
        if self._oapi_token and now < self._oapi_token_expire - 300:
            return self._oapi_token

        async with self._session.get(
            f"{_OAPI_BASE}/gettoken",
            params={"appkey": self._client_id, "appsecret": self._client_secret},
            timeout=15,
        ) as resp:
            data = await resp.json(content_type=None)
            if resp.status >= 400 or int(data.get("errcode") or 0) != 0:
                raise RuntimeError(f"DingTalk OAPI token fetch failed: {resp.status} {data}")

        token = str(data.get("access_token") or "")
        if not token:
            raise RuntimeError(f"DingTalk OAPI access_token missing: {data}")
        self._oapi_token = token
        self._oapi_token_expire = now + int(data.get("expires_in") or 7200)
        return token

    async def _upload_image(self, image_bytes: bytes) -> str:
        if not image_bytes:
            raise ValueError("DingTalk image data is empty")
        if len(image_bytes) > _DINGTALK_MEDIA_TYPE_LIMITS["image"]:
            raise ValueError(f"DingTalk image exceeds {_DINGTALK_MEDIA_TYPE_LIMITS['image']} bytes limit")
        token = await self._get_oapi_token()
        form = aiohttp.FormData()
        form.add_field("media", image_bytes, filename="camera.jpg", content_type="image/jpeg")
        async with self._session.post(
            f"{_OAPI_BASE}/media/upload",
            params={"access_token": token, "type": "image"},
            data=form,
            timeout=60,
        ) as resp:
            data = await resp.json(content_type=None)
            if resp.status >= 400:
                raise RuntimeError(f"DingTalk image upload failed: HTTP {resp.status} {data}")
            errcode = int(data.get("errcode") or 0)
            if errcode != 0:
                errmsg = data.get("errmsg", "Unknown error")
                raise RuntimeError(f"DingTalk image upload failed: errcode={errcode}, errmsg={errmsg}")
        media_id = str(data.get("media_id") or "")
        if not media_id:
            raise RuntimeError(f"DingTalk image upload missing media_id: {data}")
        return media_id

    async def _upload_media(self, file_bytes: bytes, media_type: str, filename: str) -> str:
        if not file_bytes:
            raise ValueError(f"DingTalk {media_type} data is empty")
        if len(file_bytes) > _DINGTALK_MEDIA_MAX_BYTES:
            raise ValueError(f"DingTalk {media_type} exceeds {_DINGTALK_MEDIA_MAX_BYTES} bytes limit")
        type_limit = _DINGTALK_MEDIA_TYPE_LIMITS.get(media_type)
        if type_limit is not None and len(file_bytes) > type_limit:
            raise ValueError(f"DingTalk {media_type} exceeds {type_limit} bytes limit")
        token = await self._get_oapi_token()
        content_type = "application/octet-stream"
        if media_type == "voice":
            content_type = "audio/mpeg" if filename.endswith(".mp3") else "audio/amr"
        elif media_type == "video":
            content_type = "video/mp4"
        form = aiohttp.FormData()
        form.add_field("media", file_bytes, filename=filename, content_type=content_type)
        async with self._session.post(
            f"{_OAPI_BASE}/media/upload",
            params={"access_token": token, "type": media_type if media_type != "video" else "file"},
            data=form,
            timeout=120,
        ) as resp:
            data = await resp.json(content_type=None)
            if resp.status >= 400:
                raise RuntimeError(f"DingTalk {media_type} upload failed: HTTP {resp.status} {data}")
            errcode = int(data.get("errcode") or 0)
            if errcode != 0:
                errmsg = data.get("errmsg", "Unknown error")
                raise RuntimeError(f"DingTalk {media_type} upload failed: errcode={errcode}, errmsg={errmsg}")
        media_id = str(data.get("media_id") or "")
        if not media_id:
            raise RuntimeError(f"DingTalk {media_type} upload missing media_id: {data}")
        return media_id

    async def send_file(self, target: str, file_bytes: bytes, filename: str, target_type: str) -> None:
        media_id = await self._upload_media(file_bytes, "file", filename)
        token = await self._get_token()
        target = target.strip()
        if not target:
            raise ValueError("DingTalk target is required")

        msg_param = json.dumps({"mediaId": media_id, "fileName": filename, "fileType": filename.rsplit(".", 1)[-1] if "." in filename else "file"}, ensure_ascii=False)

        if target_type == "user":
            path = "/v1.0/robot/oToMessages/batchSend"
            body = {
                "robotCode": self._client_id,
                "userIds": [target],
                "msgKey": "sampleFile",
                "msgParam": msg_param,
            }
        else:
            path = "/v1.0/robot/groupMessages/send"
            body = {
                "robotCode": self._client_id,
                "openConversationId": target,
                "msgKey": "sampleFile",
                "msgParam": msg_param,
            }

        async with self._session.post(
            f"{_API_BASE}{path}",
            headers={"x-acs-dingtalk-access-token": token},
            json=body,
            timeout=30,
        ) as resp:
            if resp.status >= 400:
                error_text = await resp.text()
                _LOGGER.error("DingTalk file send failed: %s, response: %s", resp.status, error_text)
                raise RuntimeError(f"DingTalk file send failed: {resp.status} {error_text}")

    async def send_video(self, target: str, video_bytes: bytes, filename: str, target_type: str) -> None:
        media_id = await self._upload_media(video_bytes, "video", filename)
        token = await self._get_token()
        target = target.strip()
        if not target:
            raise ValueError("DingTalk target is required")

        msg_param = json.dumps({"videoMediaId": media_id, "videoType": "mp4", "duration": "60000", "picMediaId": ""}, ensure_ascii=False)

        if target_type == "user":
            path = "/v1.0/robot/oToMessages/batchSend"
            body = {
                "robotCode": self._client_id,
                "userIds": [target],
                "msgKey": "sampleVideo",
                "msgParam": msg_param,
            }
        else:
            path = "/v1.0/robot/groupMessages/send"
            body = {
                "robotCode": self._client_id,
                "openConversationId": target,
                "msgKey": "sampleVideo",
                "msgParam": msg_param,
            }

        async with self._session.post(
            f"{_API_BASE}{path}",
            headers={"x-acs-dingtalk-access-token": token},
            json=body,
            timeout=30,
        ) as resp:
            if resp.status >= 400:
                error_text = await resp.text()
                _LOGGER.error("DingTalk video send failed: %s, response: %s", resp.status, error_text)
                raise RuntimeError(f"DingTalk video send failed: {resp.status} {error_text}")

    async def send_voice(self, target: str, voice_bytes: bytes, filename: str, target_type: str) -> None:
        media_id = await self._upload_media(voice_bytes, "voice", filename)
        token = await self._get_token()
        target = target.strip()
        if not target:
            raise ValueError("DingTalk target is required")

        msg_param = json.dumps({"mediaId": media_id, "duration": "60000"}, ensure_ascii=False)

        if target_type == "user":
            path = "/v1.0/robot/oToMessages/batchSend"
            body = {
                "robotCode": self._client_id,
                "userIds": [target],
                "msgKey": "sampleAudio",
                "msgParam": msg_param,
            }
        else:
            path = "/v1.0/robot/groupMessages/send"
            body = {
                "robotCode": self._client_id,
                "openConversationId": target,
                "msgKey": "sampleAudio",
                "msgParam": msg_param,
            }

        async with self._session.post(
            f"{_API_BASE}{path}",
            headers={"x-acs-dingtalk-access-token": token},
            json=body,
            timeout=30,
        ) as resp:
            if resp.status >= 400:
                error_text = await resp.text()
                _LOGGER.error("DingTalk voice send failed: %s, response: %s", resp.status, error_text)
                raise RuntimeError(f"DingTalk voice send failed: {resp.status} {error_text}")


async def _resolve_media(hass: HomeAssistant, source: str) -> bytes | None:
    source = im_public_url_for_source(hass, source)
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


async def async_validate_config(_: HomeAssistant, config: dict[str, Any]) -> None:
    client_id = str(config.get(CONF_DINGTALK_CLIENT_ID, "")).strip()
    client_secret = str(config.get(CONF_DINGTALK_CLIENT_SECRET, "")).strip()
    if not client_id or not client_secret:
        raise ValueError("dingtalk_client_id and dingtalk_client_secret are required")


def _format_live_progress(payload: dict[str, Any]) -> str:
    display_text = str(payload.get("display_text") or "").strip()
    if display_text:
        cleaned = display_text.replace("┊", "").replace("*", "").strip()
        return cleaned[:200]
    tool_name = str(payload.get("tool_name") or "").strip()
    if tool_name:
        return f"🔧 {tool_name}"
    return ""


async def async_setup_provider(
    hass: HomeAssistant,
    config: dict[str, Any],
    *,
    agent_id: str,
    subentry_id: str,
) -> ProviderRuntime:
    client_id = str(config.get(CONF_DINGTALK_CLIENT_ID, "")).strip()
    client_secret = str(config.get(CONF_DINGTALK_CLIENT_SECRET, "")).strip()
    show_live_progress = bool(config.get(_CONF_DINGTALK_SHOW_LIVE_PROGRESS, False))
    client = DingTalkClient(hass, client_id, client_secret, agent_id)
    tracker = await async_get_tracker(hass, subentry_id)

    async def _process_rich_reply(reply: str, target: str, target_type: str, reply_text_func, incoming) -> None:
        segments = parse_reply_segments(reply)
        for seg in segments:
            if isinstance(seg, TextSegment):
                reply_text_func(seg.text, incoming)
            elif isinstance(seg, ImageSegment):
                try:
                    image_bytes = await _resolve_image(hass, seg.source)
                    if image_bytes:
                        await client.send_image(target, image_bytes, target_type)
                except Exception as err:
                    _LOGGER.warning("DingTalk image send failed: %s", err)
                    reply_text_func(f"Image send failed: {err}", incoming)
            elif isinstance(seg, VideoSegment):
                try:
                    result = await _resolve_video(hass, seg.source)
                    if result:
                        video_bytes, file_name = result
                        await client.send_video(target, video_bytes, file_name, target_type)
                    else:
                        reply_text_func(f"📎 {seg.source}", incoming)
                except Exception as err:
                    _LOGGER.warning("DingTalk video send failed: %s", err)
                    reply_text_func(f"📎 {seg.source}", incoming)
            elif isinstance(seg, FileSegment):
                try:
                    media_bytes = await _resolve_media(hass, seg.source)
                    if media_bytes:
                        name = seg.source.rsplit("/", 1)[-1] or "file"
                        await client.send_file(target, media_bytes, name, target_type)
                    else:
                        reply_text_func(f"📎 {seg.source}", incoming)
                except Exception as err:
                    _LOGGER.warning("DingTalk file send failed: %s", err)
                    reply_text_func(f"📎 {seg.source}", incoming)
            elif isinstance(seg, VoiceSegment):
                try:
                    from ...media.tts import async_generate_tts_mp3, is_edge_tts_available
                    if is_edge_tts_available():
                        mp3_bytes = await async_generate_tts_mp3(hass, seg.text)
                        await client.send_voice(target, mp3_bytes, "voice.mp3", target_type)
                except Exception as err:
                    _LOGGER.warning("DingTalk voice send failed: %s", err)
                    reply_text_func(seg.text, incoming)

    async def _run_stream_with_tracking() -> None:
        import dingtalk_stream

        outer = client

        class _RichHandler(dingtalk_stream.ChatbotHandler):
            async def process(self, callback):
                raw_data = callback.data if isinstance(callback.data, dict) else {}
                incoming = dingtalk_stream.ChatbotMessage.from_dict(callback.data)
                target, target_type, display_name = _extract_stream_sender_and_target(raw_data)

                fut = asyncio.run_coroutine_threadsafe(
                    tracker.async_record(
                        provider=PROVIDER_DINGTALK,
                        target=target,
                        target_type=target_type,
                        display_name=display_name,
                    ),
                    outer._hass.loop,
                )
                try:
                    fut.result(timeout=10)
                except Exception as err:
                    _LOGGER.debug("DingTalk tracker record failed: %s", err)

                text = _extract_stream_text(raw_data)
                if not text:
                    return dingtalk_stream.AckMessage.STATUS_OK, "OK"

                try:
                    command = parse_command(text)
                except ValueError as err:
                    self.reply_text(f"Invalid command: {err}", incoming)
                    return dingtalk_stream.AckMessage.STATUS_OK, "OK"

                if command is None:
                    return dingtalk_stream.AckMessage.STATUS_OK, "OK"

                conversation_id = f"dingtalk:{target}"

                async def _run_live_progress_bridge() -> None:
                    if not show_live_progress:
                        await asyncio.Future()

                    queue: asyncio.Queue[str] = asyncio.Queue()

                    @callback
                    def _listener(event) -> None:
                        payload = event.data or {}
                        if payload.get("conversation_id") != conversation_id:
                            return
                        progress_text = _format_live_progress(payload)
                        if progress_text:
                            queue.put_nowait(progress_text)

                    unsub = hass.bus.async_listen(EVENT_LIVE_PROGRESS, _listener)
                    last_sent = ""
                    pending_tasks: list[asyncio.Task] = []
                    
                    async def _fire_and_forget(msg: str) -> None:
                        with contextlib.suppress(Exception):
                            await hass.async_add_executor_job(self.reply_text, msg, incoming)
                    
                    try:
                        while True:
                            progress_text = await queue.get()
                            if progress_text == last_sent:
                                continue
                            task = asyncio.create_task(_fire_and_forget(progress_text))
                            pending_tasks.append(task)
                            last_sent = progress_text
                    except asyncio.CancelledError:
                        if pending_tasks:
                            await asyncio.gather(*pending_tasks, return_exceptions=True)
                    finally:
                        unsub()

                async def _execute_with_progress() -> str:
                    progress_task = asyncio.create_task(_run_live_progress_bridge())
                    try:
                        result = await execute_command(
                            outer._hass,
                            command,
                            conversation_id=conversation_id,
                            agent_id=outer._agent_id,
                            extra_system_prompt=build_dingtalk_prompt(),
                            user_id=display_name or target,
                        )
                        return result
                    finally:
                        progress_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await progress_task

                fut = asyncio.run_coroutine_threadsafe(
                    _execute_with_progress(),
                    outer._hass.loop,
                )
                try:
                    reply = fut.result(timeout=60)
                except Exception as err:
                    _LOGGER.warning("DingTalk command execution failed: %s", err)
                    reply = f"Execution failed: {type(err).__name__}"

                if not reply:
                    return dingtalk_stream.AckMessage.STATUS_OK, "OK"

                fut = asyncio.run_coroutine_threadsafe(
                    _process_rich_reply(reply, target, target_type, self.reply_text, incoming),
                    outer._hass.loop,
                )
                try:
                    fut.result(timeout=60)
                except Exception as err:
                    _LOGGER.warning("DingTalk rich reply failed: %s", err)
                    self.reply_text(reply, incoming)

                return dingtalk_stream.AckMessage.STATUS_OK, "OK"

        max_retries = 8
        retry_count = 0
        while retry_count < max_retries:
            try:
                credential = dingtalk_stream.Credential(outer._client_id, outer._client_secret)
                sdk_client = dingtalk_stream.DingTalkStreamClient(credential)
                sdk_client.register_callback_handler(dingtalk_stream.chatbot.ChatbotMessage.TOPIC, _RichHandler())
                outer._status = "connected"
                retry_count = 0
                await asyncio.to_thread(sdk_client.start_forever)
            except asyncio.CancelledError:
                raise
            except Exception as err:
                retry_count += 1
                _LOGGER.warning("DingTalk stream error (attempt %d/%d): %s", retry_count, max_retries, err)
                if retry_count >= max_retries:
                    _LOGGER.error("DingTalk stream failed after %d attempts, stopping", max_retries)
                    outer._status = "error"
                    break
                outer._status = "reconnecting"
                await asyncio.sleep(5)

    client._run_stream = _run_stream_with_tracking
    await client.start()

    async def _send(target: str, message: str, target_type: str) -> None:
        mode = target_type if target_type in ("group", "user") else "group"
        await client.send_text(target, message, mode)

    async def _send_image(target: str, image_bytes: bytes, target_type: str) -> None:
        mode = target_type if target_type in ("group", "user") else "group"
        await client.send_image(target, image_bytes, mode)

    async def _send_video(target: str, video_bytes: bytes, filename: str, target_type: str) -> None:
        mode = target_type if target_type in ("group", "user") else "group"
        await client.send_video(target, video_bytes, filename, mode)

    async def _send_file(target: str, file_bytes: bytes, filename: str, target_type: str) -> None:
        mode = target_type if target_type in ("group", "user") else "group"
        await client.send_file(target, file_bytes, filename, mode)

    async def _send_voice(target: str, voice_bytes: bytes, filename: str, target_type: str) -> None:
        mode = target_type if target_type in ("group", "user") else "group"
        await client.send_voice(target, voice_bytes, filename, mode)

    return ProviderRuntime(
        key=PROVIDER_DINGTALK,
        title=PROVIDER_DINGTALK,
        subentry_id=subentry_id,
        client=client,
        stop=client.stop,
        send_text=_send,
        status=lambda: client.status,
        known_targets=tracker.snapshot,
        selected_target=tracker.selected_target,
        select_target=tracker.async_select_target,
        send_image=_send_image,
        send_video=_send_video,
        send_file=_send_file,
        send_voice=_send_voice,
    )


_CONF_DINGTALK_SHOW_LIVE_PROGRESS = "dingtalk_show_live_progress"


def _build_schema(current: dict[str, Any]) -> vol.Schema:
    return vol.Schema(
        {
            vol.Required(CONF_DINGTALK_CLIENT_ID, default=current.get(CONF_DINGTALK_CLIENT_ID, "")): str,
            vol.Required(CONF_DINGTALK_CLIENT_SECRET, default=current.get(CONF_DINGTALK_CLIENT_SECRET, "")): str,
            vol.Optional(_CONF_DINGTALK_SHOW_LIVE_PROGRESS, default=current.get(_CONF_DINGTALK_SHOW_LIVE_PROGRESS, False)): bool,
        }
    )


PROVIDER_SPEC = ProviderSpec(
    key=PROVIDER_DINGTALK,
    schema_builder=_build_schema,
    validate_config=async_validate_config,
    setup_provider=async_setup_provider,
    flow_handler=DingtalkProviderSubentryFlow,
    allow_multiple=True,
)
