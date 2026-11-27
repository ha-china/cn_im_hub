"""Weixin provider based on Tencent OpenClaw Weixin plugin protocol."""

from __future__ import annotations

import asyncio
import contextlib
import copy
import logging
import time
from typing import Any

import voluptuous as vol
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.storage import Store

EVENT_LIVE_PROGRESS = "ha_crack_live_progress"

from ...media.camera import (
    async_capture_camera_gif,
    async_record_camera_clip,
    async_record_remote_stream_clip,
    async_resolve_camera_entity,
    resolve_ha_local_path,
)
from ...core.command import execute_command, im_public_url_for_source, parse_command
from ...const import (
    CONF_WECHAT_ACCOUNT_ID,
    CONF_WECHAT_BASE_URL,
    CONF_WECHAT_TOKEN,
    CONF_WECHAT_USER_ID,
    PROVIDER_WECHAT,
    WECHAT_DEFAULT_BASE_URL,
)
from ...core.known_targets import async_get_tracker
from ...core.fast_query import FastQuery, recognize_fast_query, execute_fast_query
from ...models import ProviderRuntime
from ...media.rich_media import (
    FileSegment,
    GifSegment,
    ImageSegment,
    TextSegment,
    VideoSegment,
    VoiceSegment,
    is_camera_entity,
    is_url,
    parse_reply_segments,
)
from ...media.tts import async_generate_tts_mp3, is_edge_tts_available
from .prompt import build_wechat_prompt
from ..base import ProviderSpec
from .auth import (
    SESSION_EXPIRED_ERRCODE,
    async_download_weixin_media,
    async_get_typing_ticket,
    async_get_updates,
    async_notify_start,
    async_notify_stop,
    async_send_typing,
    async_send_weixin_file,
    async_send_weixin_image,
    async_send_weixin_text,
    async_send_weixin_video,
    extract_inbound_media,
    extract_text_body,
)
from .flow import WeixinProviderSubentryFlow

_LOGGER = logging.getLogger(__name__)
_STORE_VERSION = 1
_MAX_PENDING = 100
_MAX_NORMAL_TASKS = 2
_DEDUP_TTL = 86400
_MAX_SEEN = 2048
_QUERY_TIMEOUT = 5
_CONVERSATION_TIMEOUT = 120
_RECONNECT_BASE_DELAY_SECONDS = 5
_MAX_RETRY_DELAY_SECONDS = 300
_SESSION_PAUSE_SECONDS = 3600
_TYPING_TICKET_TTL = 23 * 3600
_CONF_WECHAT_SHOW_LIVE_PROGRESS = "wechat_show_live_progress"
_FILE_TEXT_EXTENSIONS = {".txt", ".md", ".json", ".csv", ".xml", ".yaml", ".yml", ".log", ".py", ".js", ".ts", ".html", ".css"}
_GIF_COMPRESS_THRESHOLD_BYTES = 2 * 1024 * 1024
_GIF_MAX_DIMENSION = 360
_REMOTE_STREAM_SUFFIXES = (".m3u8", ".m3u", ".mpd", ".ts")


def _compress_image(raw: bytes, max_dim: int = 640, target_kb: int = 60) -> bytes:
    from PIL import Image
    from io import BytesIO
    img = Image.open(BytesIO(raw))
    if img.mode not in ("RGB", "L"):
        img = img.convert("RGB")
    w, h = img.size
    if max(w, h) > max_dim:
        scale = max_dim / max(w, h)
        img = img.resize((int(w * scale), int(h * scale)), Image.LANCZOS)
    for quality in (85, 60, 40, 20):
        buf = BytesIO()
        img.save(buf, format="JPEG", quality=quality)
        if buf.tell() <= target_kb * 1024:
            return buf.getvalue()
    buf = BytesIO()
    img.save(buf, format="JPEG", quality=20)
    return buf.getvalue()


def _compress_gif(raw: bytes, max_dim: int = _GIF_MAX_DIMENSION) -> bytes:
    """Shrink an animated GIF: scale frames down if largest dim exceeds max_dim.

    Preserves animation. Returns original bytes if PIL fails or no resize needed.
    """
    try:
        from PIL import Image, ImageSequence
        from io import BytesIO
        img = Image.open(BytesIO(raw))
        w, h = img.size
        if max(w, h) <= max_dim:
            return raw
        scale = max_dim / max(w, h)
        new_size = (max(1, int(w * scale)), max(1, int(h * scale)))
        frames: list[Image.Image] = []
        durations: list[int] = []
        for frame in ImageSequence.Iterator(img):
            frames.append(frame.convert("RGBA").resize(new_size, Image.LANCZOS))
            durations.append(frame.info.get("duration", 80))
        out = BytesIO()
        frames[0].save(
            out,
            format="GIF",
            save_all=True,
            append_images=frames[1:],
            duration=durations,
            loop=img.info.get("loop", 0),
            disposal=2,
            optimize=True,
        )
        return out.getvalue()
    except Exception as err:
        _LOGGER.debug("GIF compress failed (using original): %s", err)
        return raw


def _is_remote_stream_source(source: str) -> bool:
    value = source.lower().strip()
    return value.startswith(("rtsp://", "rtsps://")) or any(part in value for part in _REMOTE_STREAM_SUFFIXES)


def _extract_file_text(raw: bytes, file_name: str) -> str:
    import os
    ext = os.path.splitext(file_name)[1].lower() if file_name else ""
    if ext in _FILE_TEXT_EXTENSIONS or not ext:
        try:
            return raw.decode("utf-8", errors="replace")
        except Exception:
            return ""
    if ext in (".doc", ".docx"):
        try:
            from io import BytesIO
            from zipfile import ZipFile
            import xml.etree.ElementTree as ET
            zf = ZipFile(BytesIO(raw))
            xml_content = zf.read("word/document.xml")
            tree = ET.fromstring(xml_content)
            ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
            return "\n".join(
                "".join(node.text or "" for node in p.iter(f"{{{ns['w']}}}t"))
                for p in tree.iter(f"{{{ns['w']}}}p")
            )
        except Exception:
            return ""
    return ""


def _clean_progress_text(text: str) -> str:
    value = text.strip().replace("\n", " ")
    if value.startswith("┊"):
        value = value[1:].lstrip()
    if value.startswith("*") and value.endswith("*") and len(value) >= 2:
        value = value[1:-1].strip()
    return value


def _format_live_progress(payload: dict[str, Any]) -> str:
    display_text = str(payload.get("display_text") or "").strip()
    if display_text:
        return _clean_progress_text(display_text)[:200]
    phase = str(payload.get("phase") or "").strip()
    text = str(payload.get("text") or "").strip()
    tool_name = str(payload.get("tool_name") or "").strip()
    if phase == "thinking":
        return text[:120]
    if phase == "tool_call" and tool_name:
        return f"tool: {tool_name}"
    return text[:120]


class WeixinClient:
    """Long-poll Weixin client for pure text conversation."""

    def __init__(
        self,
        hass: HomeAssistant,
        *,
        account_id: str,
        token: str,
        base_url: str,
        user_id: str,
        conversation_agent_id: str,
        subentry_id: str,
        show_live_progress: bool,
    ) -> None:
        self._hass = hass
        self._account_id = account_id
        self._token = token
        self._base_url = base_url
        self._user_id = user_id
        self._conversation_agent_id = conversation_agent_id
        self._subentry_id = subentry_id
        self._task: asyncio.Task[None] | None = None
        self._stopping = False
        self._status = "disconnected"
        self._context_tokens: dict[str, str] = {}
        self._store: Store[dict[str, Any]] = Store(hass, _STORE_VERSION, f"cn_im_hub_wechat_{subentry_id}")
        self._sync_buf = ""
        self._pending: list[dict[str, Any]] = []
        self._seen: dict[str, float] = {}
        self._state_lock = asyncio.Lock()
        self._dispatch_event = asyncio.Event()
        self._dispatch_task: asyncio.Task[None] | None = None
        self._normal_tasks: dict[str, asyncio.Task[None]] = {}
        self._pause_until = 0.0
        self._tracker = None
        self._typing_tickets: dict[str, tuple[str, float]] = {}
        self._show_live_progress = show_live_progress

    @property
    def status(self) -> str:
        return self._status

    async def start(self) -> None:
        data = await self._store.async_load() or {}
        self._sync_buf = str(data.get("get_updates_buf") or "")
        tokens = data.get("context_tokens") or {}
        if isinstance(tokens, dict):
            self._context_tokens = {str(key): str(value) for key, value in tokens.items() if value}
        seen = data.get("seen")
        if isinstance(seen, dict):
            self._seen = {str(k): v for k, v in seen.items()
                          if isinstance(v, (int, float))}
        self._prune_seen()
        pending = data.get("pending")
        if isinstance(pending, list):
            for entry in pending:
                if not isinstance(entry, dict) or not isinstance(entry.get("msg"), dict):
                    continue
                if entry.get("state") == "running":
                    # Started before the last restart; upstream semantics:
                    # interrupted tasks are not re-run (would duplicate replies).
                    _LOGGER.warning(
                        "Dropping interrupted Weixin message for account %s (was mid-processing at restart)",
                        self._account_id,
                    )
                    continue
                self._pending.append({"state": "queued", "msg": entry["msg"]})
                self._remember_context(entry["msg"])
        self._stopping = False
        if self._dispatch_task is None:
            self._dispatch_task = asyncio.create_task(self._dispatch_pending())
            self._dispatch_event.set()
        if self._task is None:
            self._task = asyncio.create_task(self._run())
        await self._notify_start()

    async def stop(self) -> None:
        self._stopping = True
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if self._dispatch_task is not None:
            self._dispatch_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._dispatch_task
            self._dispatch_task = None
        tasks = list(self._normal_tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._normal_tasks.clear()
        self._status = "disconnected"
        await self._notify_stop()

    async def _notify_start(self) -> None:
        try:
            await async_notify_start(self._hass, base_url=self._base_url, token=self._token)
        except Exception as err:
            _LOGGER.debug("Weixin notifyStart failed (ignored) account=%s: %s", self._account_id, err)

    async def _notify_stop(self) -> None:
        try:
            await async_notify_stop(self._hass, base_url=self._base_url, token=self._token)
        except Exception as err:
            _LOGGER.debug("Weixin notifyStop failed (ignored) account=%s: %s", self._account_id, err)

    def _check_push_context(self, target: str, context_token: str) -> None:
        """Warn when pushing without a context_token (no recent inbound message).

        The Weixin ilink protocol scopes proactive sends to a session window
        opened by the user's latest message. Without a token the send may be
        rejected by the server.
        """
        if not context_token:
            _LOGGER.warning(
                "Weixin push to %s has no context_token (no recent inbound message) — sending without context; if it fails, have the user message the bot first",
                target,
            )

    async def send_text(self, target: str, text: str, _: str) -> None:
        if self._remaining_pause_seconds() > 0:
            raise RuntimeError("Weixin session is paused due to stale token; please wait for the pause to expire or have the user send a message first")
        target = target.strip()
        if not target:
            raise ValueError("Weixin target user_id is required")
        context_token = self._context_tokens.get(target, "")
        self._check_push_context(target, context_token)
        await async_send_weixin_text(
            self._hass,
            base_url=self._base_url,
            token=self._token,
            to_user_id=target,
            context_token=context_token,
            text=text,
        )

    async def send_image(self, target: str, image_bytes: bytes, _: str) -> None:
        if self._remaining_pause_seconds() > 0:
            raise RuntimeError("Weixin session is paused due to stale token; please wait for the pause to expire or have the user send a message first")
        target = target.strip()
        if not target:
            raise ValueError("Weixin target user_id is required")
        context_token = self._context_tokens.get(target, "")
        self._check_push_context(target, context_token)
        await async_send_weixin_image(
            self._hass,
            base_url=self._base_url,
            token=self._token,
            to_user_id=target,
            context_token=context_token,
            image_bytes=image_bytes,
        )

    async def _resolve_media_source(
        self, source: str, *, default_name: str
    ) -> tuple[bytes, str]:
        """Resolve URL / HA local path to (bytes, file_name)."""
        candidate = im_public_url_for_source(self._hass, source)
        if not candidate:
            raise ValueError("Media source is empty")
        if is_url(candidate):
            from homeassistant.helpers.aiohttp_client import async_get_clientsession
            session = async_get_clientsession(self._hass)
            async with session.get(candidate, timeout=120) as resp:
                if resp.status >= 400:
                    raise RuntimeError(f"download failed: {resp.status}")
                data = await resp.read()
            from pathlib import Path
            remote_name = Path(candidate.split("?", 1)[0]).name
            return data, remote_name or default_name
        local_path = resolve_ha_local_path(self._hass, candidate)
        if local_path is not None:
            data = await self._hass.async_add_executor_job(local_path.read_bytes)
            return data, local_path.name or default_name
        raise ValueError(f"Media source not found: {candidate}")

    @staticmethod
    def _retry_delay(attempt: int) -> int:
        """Exponential backoff for reconnects: 5s, 10s, 20s ... capped at 300s.

        The long-poll loop must never give up permanently: while it is dead,
        inbound messages are lost and cached context_tokens go stale, which
        breaks proactive pushes until the user messages the bot again.
        """
        return min(_RECONNECT_BASE_DELAY_SECONDS * (2 ** min(attempt - 1, 6)), _MAX_RETRY_DELAY_SECONDS)

    async def _run(self) -> None:
        consecutive_failures = 0
        next_timeout_ms = 35_000
        while not self._stopping:
            # Backpressure: leave additional messages on the server, not lost.
            if len(self._pending) >= _MAX_PENDING:
                await asyncio.sleep(0.2)
                continue
            remaining_pause = self._remaining_pause_seconds()
            if remaining_pause > 0:
                self._status = "paused"
                await asyncio.sleep(remaining_pause)
                continue
            self._status = "connected" if consecutive_failures == 0 else "reconnecting"
            try:
                resp = await async_get_updates(
                    self._hass,
                    base_url=self._base_url,
                    token=self._token,
                    get_updates_buf=self._sync_buf,
                    timeout_ms=next_timeout_ms,
                )
                errcode = self._extract_error_code(resp)
                if errcode == SESSION_EXPIRED_ERRCODE:
                    self._pause_session()
                    consecutive_failures = 0
                    _LOGGER.warning(
                        "Weixin session expired for account %s, pausing for %s minutes",
                        self._account_id,
                        _SESSION_PAUSE_SECONDS // 60,
                    )
                    continue
                if self._is_api_error(resp):
                    consecutive_failures += 1
                    delay = self._retry_delay(consecutive_failures)
                    _LOGGER.warning(
                        "Weixin getupdates failed (attempt %d, retry in %ds) account=%s ret=%s errcode=%s errmsg=%s",
                        consecutive_failures,
                        delay,
                        self._account_id,
                        resp.get("ret"),
                        resp.get("errcode"),
                        resp.get("errmsg"),
                    )
                    await asyncio.sleep(delay)
                    continue
                if consecutive_failures:
                    _LOGGER.info(
                        "Weixin connection restored for account %s after %d failures",
                        self._account_id,
                        consecutive_failures,
                    )
                consecutive_failures = 0
                if isinstance(resp.get("longpolling_timeout_ms"), int) and resp["longpolling_timeout_ms"] > 0:
                    next_timeout_ms = int(resp["longpolling_timeout_ms"])
                new_buf = str(resp.get("get_updates_buf") or "")
                buf_changed = bool(new_buf) and new_buf != self._sync_buf
                if buf_changed:
                    self._sync_buf = new_buf
                batch = [m for m in (resp.get("msgs") or []) if isinstance(m, dict)]
                if batch:
                    self._enqueue_batch(batch)
                # Persist cursor and queued messages before dispatch.
                if buf_changed or batch:
                    await self._async_save_state()
                self._dispatch_event.set()
            except asyncio.CancelledError:
                raise
            except Exception as err:
                consecutive_failures += 1
                self._status = "error"
                delay = self._retry_delay(consecutive_failures)
                _LOGGER.warning(
                    "Weixin long-poll error (attempt %d, retry in %ds, account=%s): %s",
                    consecutive_failures,
                    delay,
                    self._account_id,
                    err,
                )
                await asyncio.sleep(delay)
        self._status = "disconnected"

    async def _get_typing_ticket(self, user_id: str, context_token: str) -> str:
        now = asyncio.get_running_loop().time()
        cached = self._typing_tickets.get(user_id)
        if cached and (now - cached[1]) < _TYPING_TICKET_TTL:
            return cached[0]
        ticket = await async_get_typing_ticket(
            self._hass,
            base_url=self._base_url,
            token=self._token,
            ilink_user_id=user_id,
            context_token=context_token,
        )
        if ticket:
            self._typing_tickets[user_id] = (ticket, now)
        return ticket

    async def _set_typing(self, user_id: str, context_token: str, *, status: int = 1) -> None:
        try:
            ticket = await self._get_typing_ticket(user_id, context_token)
            if not ticket:
                return
            await async_send_typing(
                self._hass,
                base_url=self._base_url,
                token=self._token,
                ilink_user_id=user_id,
                typing_ticket=ticket,
                status=status,
            )
        except Exception:
            _LOGGER.debug("Weixin typing indicator failed for %s", user_id)

    async def _typing_keepalive(self, user_id: str, context_token: str) -> None:
        while True:
            await self._set_typing(user_id, context_token, status=1)
            await asyncio.sleep(10)

    async def _run_live_progress_bridge(
        self,
        *,
        conversation_id: str,
        to_user_id: str,
        context_token: str,
    ) -> None:
        if not self._show_live_progress:
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

        unsub = self._hass.bus.async_listen(EVENT_LIVE_PROGRESS, _listener)
        last_sent = ""
        pending_tasks: list[asyncio.Task] = []
        
        async def _fire_and_forget(msg: str) -> None:
            with contextlib.suppress(Exception):
                await async_send_weixin_text(
                    self._hass,
                    base_url=self._base_url,
                    token=self._token,
                    to_user_id=to_user_id,
                    context_token=context_token,
                    text=msg,
                )
        
        try:
            while True:
                text = await queue.get()
                if text == last_sent:
                    continue
                task = asyncio.create_task(_fire_and_forget(text))
                pending_tasks.append(task)
                last_sent = text
        except asyncio.CancelledError:
            while not queue.empty():
                text = queue.get_nowait()
                if text and text != last_sent:
                    task = asyncio.create_task(_fire_and_forget(text))
                    pending_tasks.append(task)
                    last_sent = text
            if pending_tasks:
                await asyncio.gather(*pending_tasks, return_exceptions=True)
        finally:
            unsub()

    async def _resolve_image(self, source: str) -> bytes | None:
        try:
            if is_camera_entity(source):
                from homeassistant.components.camera import async_get_image
                image = await async_get_image(self._hass, source)
                return image.content
            source = im_public_url_for_source(self._hass, source)
            if is_url(source):
                from homeassistant.helpers.aiohttp_client import async_get_clientsession
                session = async_get_clientsession(self._hass)
                async with session.get(source, timeout=30) as resp:
                    if resp.status < 400:
                        return await resp.read()
            local_path = resolve_ha_local_path(self._hass, source)
            if local_path and local_path.is_file():
                return await self._hass.async_add_executor_job(local_path.read_bytes)
        except Exception:
            _LOGGER.warning("Failed to resolve image source: %s", source)
        return None

    async def _process_inbound_media(self, media: Any, text: str) -> str:
        from .auth import InboundMedia
        if not isinstance(media, InboundMedia):
            return text or ""
        try:
            raw = await async_download_weixin_media(
                self._hass,
                encrypt_query_param=media.encrypt_query_param or None,
                aes_key_b64=media.aes_key_b64 or None,
                full_url=media.full_url or None,
                aeskey_hex=media.aeskey_hex or None,
            )
            if media.kind == "image":
                import uuid
                from pathlib import Path
                compressed = await self._hass.async_add_executor_job(
                    _compress_image, raw, 640, 60
                )
                tmp_dir = Path(self._hass.config.path(".storage", "cn_im_hub", "tmp"))
                tmp_dir.mkdir(parents=True, exist_ok=True)
                tmp_path = tmp_dir / f"wx_{uuid.uuid4().hex[:12]}.jpg"
                await self._hass.async_add_executor_job(tmp_path.write_bytes, compressed)
                prefix = text + "\n" if text else ""
                return f"{prefix}[ATTACHMENT:image/jpeg:{tmp_path}]"
            if media.kind == "file":
                file_text = await self._hass.async_add_executor_job(
                    _extract_file_text, raw, media.file_name
                )
                if file_text:
                    prefix = text + "\n" if text else ""
                    name = media.file_name or "file"
                    return f"{prefix}[用户发送了文件 {name}，内容如下：]\n{file_text[:8000]}"
        except Exception:
            _LOGGER.exception("Failed to process inbound media: %s", media.kind)
        return text or ""

    async def _handle_message(self, message: dict[str, Any]) -> None:
        from_user_id = str(message.get("from_user_id") or "").strip()
        if not from_user_id:
            return
        text = extract_text_body(message)
        media = extract_inbound_media(message)
        if not text and not media:
            return
        if media:
            text = await self._process_inbound_media(media, text)
        if self._tracker is not None:
            await self._tracker.async_record(
                provider=PROVIDER_WECHAT,
                target=from_user_id,
                target_type="user_id",
                display_name=from_user_id,
            )
        command = parse_command(text)
        if command is None:
            return
        resolved_context = str(message.get("context_token") or "").strip() or self._context_tokens.get(from_user_id)
        if not resolved_context:
            _LOGGER.warning("Weixin context_token missing for user %s", from_user_id)
            return
        typing_task = asyncio.create_task(
            self._typing_keepalive(from_user_id, resolved_context)
        )
        progress_task = asyncio.create_task(
            self._run_live_progress_bridge(
                conversation_id=f"wechat:{self._account_id}:{from_user_id}",
                to_user_id=from_user_id,
                context_token=resolved_context,
            )
        )
        try:
            try:
                reply = await asyncio.wait_for(
                    execute_command(
                        self._hass,
                        command,
                        conversation_id=f"wechat:{self._account_id}:{from_user_id}",
                        agent_id=self._conversation_agent_id or None,
                        extra_system_prompt=build_wechat_prompt(),
                        user_id=from_user_id,
                    ),
                    timeout=_CONVERSATION_TIMEOUT,
                )
            except TimeoutError:
                # An action may already have happened. Never replay it.
                reply = "本次处理超时，执行结果可能尚未确认，请先核实设备状态。"
                _LOGGER.warning("Weixin conversation timed out")
            if progress_task is not None:
                progress_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await progress_task
                progress_task = None
                await asyncio.sleep(0.3)
            if not reply:
                return
            _LOGGER.info("Reply to parse: %r", reply[:200])
            segments = parse_reply_segments(reply)
            _LOGGER.info("Parsed segments: %s", segments)
            for seg in segments:
                if isinstance(seg, TextSegment):
                    await async_send_weixin_text(
                        self._hass,
                        base_url=self._base_url,
                        token=self._token,
                        to_user_id=from_user_id,
                        context_token=resolved_context,
                        text=seg.text,
                    )
                elif isinstance(seg, ImageSegment):
                    image_bytes = await self._resolve_image(seg.source)
                    if image_bytes:
                        await async_send_weixin_image(
                            self._hass,
                            base_url=self._base_url,
                            token=self._token,
                            to_user_id=from_user_id,
                            context_token=resolved_context,
                            image_bytes=image_bytes,
                        )
                elif isinstance(seg, VideoSegment):
                    try:
                        resolved_camera = await async_resolve_camera_entity(
                            self._hass, seg.source
                        )
                        if resolved_camera is not None:
                            video_bytes, _name = await async_record_camera_clip(
                                self._hass, resolved_camera
                            )
                        elif _is_remote_stream_source(seg.source):
                            video_bytes, _name = await async_record_remote_stream_clip(
                                self._hass, seg.source
                            )
                        else:
                            video_bytes, _name = await self._resolve_media_source(
                                seg.source, default_name="video.mp4"
                            )
                        await async_send_weixin_video(
                            self._hass,
                            base_url=self._base_url,
                            token=self._token,
                            to_user_id=from_user_id,
                            context_token=resolved_context,
                            video_bytes=video_bytes,
                        )
                    except Exception as err:
                        _LOGGER.warning("Weixin video send failed: %s", err)
                        await async_send_weixin_text(
                            self._hass,
                            base_url=self._base_url,
                            token=self._token,
                            to_user_id=from_user_id,
                            context_token=resolved_context,
                            text=f"Video send failed: {type(err).__name__}: {err}",
                        )
                elif isinstance(seg, GifSegment):
                    try:
                        resolved_camera = await async_resolve_camera_entity(
                            self._hass, seg.source
                        )
                        if resolved_camera is not None:
                            gif_bytes, _name = await async_capture_camera_gif(
                                self._hass, resolved_camera
                            )
                        else:
                            gif_bytes, _name = await self._resolve_media_source(
                                seg.source, default_name="animated.gif"
                            )
                        if len(gif_bytes) > _GIF_COMPRESS_THRESHOLD_BYTES:
                            gif_bytes = await self._hass.async_add_executor_job(
                                _compress_gif, gif_bytes
                            )
                        await async_send_weixin_image(
                            self._hass,
                            base_url=self._base_url,
                            token=self._token,
                            to_user_id=from_user_id,
                            context_token=resolved_context,
                            image_bytes=gif_bytes,
                        )
                    except Exception as err:
                        _LOGGER.warning("Weixin gif send failed: %s", err)
                        await async_send_weixin_text(
                            self._hass,
                            base_url=self._base_url,
                            token=self._token,
                            to_user_id=from_user_id,
                            context_token=resolved_context,
                            text=f"GIF send failed: {type(err).__name__}: {err}",
                        )
                elif isinstance(seg, FileSegment):
                    try:
                        file_bytes, file_name = await self._resolve_media_source(
                            seg.source, default_name="attachment.bin"
                        )
                        await async_send_weixin_file(
                            self._hass,
                            base_url=self._base_url,
                            token=self._token,
                            to_user_id=from_user_id,
                            context_token=resolved_context,
                            file_bytes=file_bytes,
                            file_name=file_name,
                        )
                    except Exception as err:
                        _LOGGER.warning("Weixin file send failed: %s", err)
                        await async_send_weixin_text(
                            self._hass,
                            base_url=self._base_url,
                            token=self._token,
                            to_user_id=from_user_id,
                            context_token=resolved_context,
                            text=f"File send failed: {type(err).__name__}: {err}",
                        )
                elif isinstance(seg, VoiceSegment):
                    try:
                        mp3_bytes = await async_generate_tts_mp3(self._hass, seg.text)
                        await async_send_weixin_file(
                            self._hass,
                            base_url=self._base_url,
                            token=self._token,
                            to_user_id=from_user_id,
                            context_token=resolved_context,
                            file_bytes=mp3_bytes,
                            file_name="voice.mp3",
                        )
                    except Exception as err:
                        _LOGGER.warning("Weixin voice send failed: %s", err)
                        await async_send_weixin_text(
                            self._hass,
                            base_url=self._base_url,
                            token=self._token,
                            to_user_id=from_user_id,
                            context_token=resolved_context,
                            text=f"Voice send failed: {type(err).__name__}: {err}",
                        )
        finally:
            if progress_task is not None:
                progress_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await progress_task
            typing_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await typing_task
            await self._set_typing(from_user_id, resolved_context, status=2)

    def _message_key(self, message: dict[str, Any]) -> str | None:
        sender = str(message.get("from_user_id") or "").strip()
        for field in ("message_id", "client_id"):
            value = message.get(field)
            if isinstance(value, (str, int)) and not isinstance(value, bool) and str(value).strip() not in ("", "0"):
                return f"{sender}:{field}:{value}"
        # Never deduplicate by content: repeated queries may be intentional.
        return None

    def _prune_seen(self) -> None:
        cutoff = time.time() - _DEDUP_TTL
        self._seen = dict(sorted(
            ((k, v) for k, v in self._seen.items() if v >= cutoff),
            key=lambda item: item[1],
        )[-_MAX_SEEN:])

    def _remember_context(self, message: dict[str, Any]) -> None:
        sender = str(message.get("from_user_id") or "").strip()
        token = str(message.get("context_token") or "").strip()
        if sender and token:
            self._context_tokens[sender] = token

    def _enqueue_batch(self, batch: list[dict[str, Any]]) -> None:
        self._prune_seen()
        pending_keys = {self._message_key(entry["msg"]) for entry in self._pending}
        for message in batch:
            key = self._message_key(message)
            if key is not None and (key in self._seen or key in pending_keys):
                continue
            self._remember_context(message)
            self._pending.append({"state": "queued", "msg": message})
            if key is not None:
                self._seen[key] = time.time()
                pending_keys.add(key)
        self._prune_seen()

    async def _async_save_state(self) -> None:
        # Store writes must not overtake each other or retain mutable references.
        async with self._state_lock:
            await self._store.async_save(copy.deepcopy({
                "get_updates_buf": self._sync_buf,
                "context_tokens": self._context_tokens,
                "pending": self._pending,
                "seen": self._seen,
            }))

    async def _mark_running(self, entry: dict[str, Any]) -> None:
        entry["state"] = "running"
        try:
            await self._async_save_state()
        except Exception:
            entry["state"] = "queued"
            raise

    async def _finish_entry(self, entry: dict[str, Any]) -> None:
        if not self._stopping:
            self._pending[:] = [item for item in self._pending if item is not entry]
            try:
                await self._async_save_state()
            except Exception:
                _LOGGER.exception("Failed to persist completed Weixin message")
        self._dispatch_event.set()

    async def _run_normal(self, sender: str, entry: dict[str, Any]) -> None:
        try:
            await self._handle_message(entry["msg"])
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOGGER.exception("Weixin message handling failed; not replaying")
        finally:
            self._normal_tasks.pop(sender, None)
            await self._finish_entry(entry)

    async def _try_recognize(self, message: dict[str, Any]) -> FastQuery | None:
        if extract_inbound_media(message):
            return None
        text = extract_text_body(message)
        if not text:
            return None
        try:
            return await asyncio.wait_for(
                recognize_fast_query(self._hass, text), timeout=_QUERY_TIMEOUT
            )
        except TimeoutError:
            return None

    async def _run_fast(self, sender: str, entry: dict[str, Any], query: FastQuery) -> None:
        try:
            token = str(entry["msg"].get("context_token") or "").strip() or self._context_tokens.get(sender)
            if not sender or not token:
                return
            if self._tracker is not None:
                await self._tracker.async_record(
                    provider=PROVIDER_WECHAT, target=sender,
                    target_type="user_id", display_name=sender,
                )
            try:
                reply = await asyncio.wait_for(
                    execute_fast_query(self._hass, query), timeout=_QUERY_TIMEOUT
                )
            except Exception:
                _LOGGER.exception("Weixin local fast query failed")
                reply = "本地查询暂时失败，请稍后重试。"
            # Fast responses are plain text, never interpreted as media tags.
            await async_send_weixin_text(
                self._hass, base_url=self._base_url, token=self._token,
                to_user_id=sender, context_token=token, text=reply,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOGGER.exception("Weixin fast reply failed; not replaying")
        finally:
            await self._finish_entry(entry)

    async def _dispatch_pending(self) -> None:
        """Keep each conversational lane ordered; bypass it only for read-only scripts."""
        while not self._stopping:
            await self._dispatch_event.wait()
            self._dispatch_event.clear()
            try:
                blocked_senders: set[str] = set()
                for entry in list(self._pending):
                    if self._stopping:
                        return
                    if entry.get("state") != "queued":
                        continue
                    message = entry["msg"]
                    sender = str(message.get("from_user_id") or "").strip()
                    if (sender not in blocked_senders and sender not in self._normal_tasks
                            and len(self._normal_tasks) < _MAX_NORMAL_TASKS):
                        await self._mark_running(entry)
                        self._normal_tasks[sender] = asyncio.create_task(self._run_normal(sender, entry))
                        continue
                    # Only bypass an occupied lane when strict read-only recognition succeeds.
                    query = await self._try_recognize(message)
                    if query is not None:
                        await self._mark_running(entry)
                        await self._run_fast(sender, entry, query)
                    else:
                        # A later normal turn must not leapfrog an earlier queued turn
                        # if the active task finishes during recognition.
                        blocked_senders.add(sender)
            except asyncio.CancelledError:
                raise
            except Exception:
                _LOGGER.exception("Weixin dispatch failed; retrying queued messages")
                await asyncio.sleep(1)
                self._dispatch_event.set()

    def _pause_session(self) -> None:
        self._pause_until = asyncio.get_running_loop().time() + _SESSION_PAUSE_SECONDS

    def _remaining_pause_seconds(self) -> float:
        remaining = self._pause_until - asyncio.get_running_loop().time()
        return remaining if remaining > 0 else 0.0

    @staticmethod
    def _extract_error_code(resp: dict[str, Any]) -> int | None:
        for key in ("errcode", "ret"):
            value = resp.get(key)
            if isinstance(value, int):
                return value
        return None

    @staticmethod
    def _is_api_error(resp: dict[str, Any]) -> bool:
        errcode = resp.get("errcode")
        ret = resp.get("ret")
        return (isinstance(errcode, int) and errcode != 0) or (isinstance(ret, int) and ret != 0)


async def async_validate_config(_: HomeAssistant, config: dict[str, Any]) -> None:
    token = str(config.get(CONF_WECHAT_TOKEN, "")).strip()
    account_id = str(config.get(CONF_WECHAT_ACCOUNT_ID, "")).strip()
    if not token or not account_id:
        raise ValueError("wechat_token and wechat_account_id are required")


async def async_setup_provider(
    hass: HomeAssistant,
    config: dict[str, Any],
    *,
    agent_id: str,
    subentry_id: str,
) -> ProviderRuntime:
    account_id = str(config.get(CONF_WECHAT_ACCOUNT_ID, "")).strip()
    client = WeixinClient(
        hass,
        account_id=account_id,
        token=str(config.get(CONF_WECHAT_TOKEN, "")).strip(),
        base_url=str(config.get(CONF_WECHAT_BASE_URL, WECHAT_DEFAULT_BASE_URL)).strip() or WECHAT_DEFAULT_BASE_URL,
        user_id=str(config.get(CONF_WECHAT_USER_ID, "")).strip(),
        conversation_agent_id=agent_id,
        subentry_id=subentry_id,
        show_live_progress=bool(config.get(_CONF_WECHAT_SHOW_LIVE_PROGRESS, False)),
    )
    tracker = await async_get_tracker(hass, subentry_id)
    client._tracker = tracker
    await client.start()

    async def _send(target: str, message: str, target_type: str) -> None:
        await client.send_text(target, message, target_type)

    async def _send_image(target: str, image_bytes: bytes, target_type: str) -> None:
        await client.send_image(target, image_bytes, target_type)

    return ProviderRuntime(
        key=PROVIDER_WECHAT,
        title=PROVIDER_WECHAT,
        subentry_id=subentry_id,
        client=client,
        stop=client.stop,
        send_text=_send,
        status=lambda: client.status,
        known_targets=tracker.snapshot,
        selected_target=tracker.selected_target,
        select_target=tracker.async_select_target,
        send_image=_send_image,
    )


def _build_schema(current: dict[str, Any]) -> vol.Schema:
    return vol.Schema(
        {
            vol.Required(CONF_WECHAT_TOKEN, default=current.get(CONF_WECHAT_TOKEN, "")): str,
            vol.Required(CONF_WECHAT_ACCOUNT_ID, default=current.get(CONF_WECHAT_ACCOUNT_ID, "")): str,
            vol.Optional(CONF_WECHAT_BASE_URL, default=current.get(CONF_WECHAT_BASE_URL, WECHAT_DEFAULT_BASE_URL)): str,
            vol.Optional(_CONF_WECHAT_SHOW_LIVE_PROGRESS, default=current.get(_CONF_WECHAT_SHOW_LIVE_PROGRESS, False)): bool,
        }
    )


PROVIDER_SPEC = ProviderSpec(
    key=PROVIDER_WECHAT,
    schema_builder=_build_schema,
    validate_config=async_validate_config,
    setup_provider=async_setup_provider,
    flow_handler=WeixinProviderSubentryFlow,
    allow_multiple=True,
)
