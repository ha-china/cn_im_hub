"""QQ provider using WebSocket gateway with rich-media support."""

from __future__ import annotations

import asyncio
import base64
import contextlib
from dataclasses import asdict, dataclass
import json
import logging
import mimetypes
from pathlib import Path
import random
import re
import tempfile
from typing import Any
import uuid

import aiohttp
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.storage import Store
import voluptuous as vol

EVENT_LIVE_PROGRESS = "ha_crack_live_progress"

from ...core.command import execute_command, im_public_url_for_source, parse_command
from ...media.camera import (
    async_capture_camera_gif,
    async_record_camera_clip,
    async_record_remote_stream_clip,
    async_resolve_camera_entity,
    resolve_ha_local_path,
)
from ...const import CONF_QQ_APP_ID, CONF_QQ_CLIENT_SECRET, PROVIDER_QQ, QQ_API_BASE, QQ_TOKEN_URL
from ...media.tts import async_generate_tts_mp3
from ...core.known_targets import async_get_tracker
from ...models import ProviderRuntime
from ...media.card import CardButton, CardRow, CardSpec, build_inline_keyboard, parse_card_source
from ...media.rich_media import (
    CardSegment,
    FileSegment,
    GifSegment,
    ImageSegment,
    TextSegment,
    VideoSegment,
    VoiceSegment,
    is_url,
    parse_reply_segments,
)
from .prompt import build_qq_prompt
from ..base import ProviderSpec
from .upload import async_upload_media_chunked

_LOGGER = logging.getLogger(__name__)
_TOKEN_URL = QQ_TOKEN_URL
_API_BASE = QQ_API_BASE
_INTENTS = (1 << 30) | (1 << 12) | (1 << 25)
_STORE_VERSION = 1
_TYPING_INPUT_SECOND = 60
_TYPING_INTERVAL_SECONDS = 50
_MAX_REFERENCE_MESSAGES = 40
_DIRECT_UPLOAD_MAX_BYTES = 8 * 1024 * 1024
_TEXT_FILE_EXTENSIONS = {
    ".txt",
    ".md",
    ".json",
    ".csv",
    ".xml",
    ".yaml",
    ".yml",
    ".log",
    ".py",
    ".js",
    ".ts",
    ".html",
    ".css",
}
_FACE_TAG_RE = re.compile(r'<faceType=\d+,faceId="[^"]*",ext="([^"]*)">')
_MARKDOWN_HINT_RE = re.compile(
    r"(^[#>*-]|\n[#>*-]|\[[^\]]+\]\([^)]+\)|```|\|[^|\n]+\|)",
    re.MULTILINE,
)
_TABLE_ROW_RE = re.compile(r"^\|(.+)\|$")
_TABLE_SEP_RE = re.compile(r"^\|[\s:|-]+\|$")
_HTML_HREF_RE = re.compile(r'href=["\']([^"\']+)["\']', re.IGNORECASE)
_MARKDOWN_LINK_RE = re.compile(r"\[[^\]]*]\((https?://[^)]+)\)")
_PLAIN_URL_RE = re.compile(r"https?://[^\s<>\"]+")
_APPROVAL_EVENT = "cn_im_hub_qq_approval_resolved"
_INTERACTION_EVENT = "cn_im_hub_qq_interaction"
_GROUP_PROACTIVE_EVENT = "cn_im_hub_qq_group_proactive_status"
_GROUP_PROACTIVE_UNKNOWN = "unknown"
_GROUP_PROACTIVE_ACCEPT = "accept"
_GROUP_PROACTIVE_REJECT = "reject"
_REMOTE_STREAM_SUFFIXES = (".m3u8", ".m3u", ".mpd")
_CARD_ACK_PHRASES = [
    "收到啦，马上处理呢~",
    "好嘞，这就给你安排上呀~",
    "嘿嘿，知道啦，稍等一下吧~",
]
_LIVE_PROGRESS_TYPING_IDLE_SECONDS = 12
_LIVE_PROGRESS_SEND_INTERVAL_SECONDS = 2.0
_LIVE_PROGRESS_TYPING_INTERVAL_SECONDS = 4.0
_CONF_QQ_SHOW_LIVE_PROGRESS = "qq_show_live_progress"
_QQ_PASSIVE_REPLY_LIMIT = 5
_QQ_PASSIVE_AUX_REPLY_LIMIT = 2
_QQ_RECONNECT_BASE_DELAY_SECONDS = 1
_QQ_RECONNECT_MAX_DELAY_SECONDS = 60


class _QQPassiveReplyLimitError(RuntimeError):
    """Raised when a message_id has exhausted its passive reply budget."""


class _QQFatalCloseError(RuntimeError):
    """Raised on gateway close codes that mean the bot can no longer run."""


@dataclass(slots=True)
class QQReferenceEntry:
    """Cached inbound message content for quote resolution."""

    msg_idx: str
    text: str
    display_name: str


@dataclass(slots=True)
class QQInboundMessage:
    """Normalized inbound QQ event."""

    text: str
    target: str
    target_kind: str
    target_id: str
    message_id: str
    display_name: str
    ref_msg_idx: str = ""
    msg_idx: str = ""


@dataclass(slots=True)
class QQLiveProgressState:
    conversation_id: str
    last_progress_at: float
    last_sent_text: str = ""
    replies_used: int = 0


def _looks_like_markdown(text: str) -> bool:
    value = text.strip()
    if not value:
        return False
    return bool(_MARKDOWN_HINT_RE.search(value))


_BARE_SEP_RE = re.compile(r"^[\s|:-]+[-]{2,}[\s|:-]*$")


def _fix_markdown_tables(text: str) -> str:
    lines = text.split("\n")
    fixed: list[str] = []
    in_table = False
    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()
        if _TABLE_ROW_RE.match(stripped):
            cols = len(stripped.split("|")) - 2
            if cols > 0 and not in_table:
                j = i + 1
                while j < len(lines) and not lines[j].strip():
                    j += 1
                has_sep = j < len(lines) and (_TABLE_SEP_RE.match(lines[j].strip()) or _BARE_SEP_RE.match(lines[j].strip()))
                has_next_row = j < len(lines) and _TABLE_ROW_RE.match(lines[j].strip())
                if has_sep:
                    fixed.append(line)
                    fixed.append("| " + " | ".join(["---"] * cols) + " |")
                    in_table = True
                    i = j + 1
                    continue
                if has_next_row and j > i + 1:
                    fixed.append(line)
                    fixed.append("| " + " | ".join(["---"] * cols) + " |")
                    in_table = True
                    i = j
                    continue
                fixed.append(line)
                fixed.append("| " + " | ".join(["---"] * cols) + " |")
                in_table = True
                i += 1
                continue
            fixed.append(line)
            i += 1
            continue
        if in_table and not stripped:
            in_table = False
        if _BARE_SEP_RE.match(stripped) and in_table:
            i += 1
            continue
        fixed.append(line)
        i += 1
    return "\n".join(fixed)


_HEADING_RE = re.compile(r"^(#{1,6})\s+(.+)$", re.MULTILINE)


def _fix_qq_markdown(text: str) -> str:
    text = _fix_markdown_tables(text)
    text = _HEADING_RE.sub(lambda m: f"**{m.group(2).strip()}**", text)
    return text


def _build_approval_keyboard(approval_id: str) -> dict[str, Any]:
    def _button(
        button_id: str,
        label: str,
        visited_label: str,
        data: str,
        style: int,
    ) -> dict[str, Any]:
        return {
            "id": button_id,
            "render_data": {
                "label": label,
                "visited_label": visited_label,
                "style": style,
            },
            "action": {
                "type": 1,
                "data": data,
                "permission": {"type": 2},
                "click_limit": 1,
            },
            "group_id": "approval",
        }

    return {
        "content": {
            "rows": [
                {
                    "buttons": [
                        _button("allow", "✅ 允许一次", "已允许", f"approve:{approval_id}:allow-once", 1),
                        _button("always", "⭐ 始终允许", "已始终允许", f"approve:{approval_id}:allow-always", 1),
                        _button("deny", "❌ 拒绝", "已拒绝", f"approve:{approval_id}:deny", 0),
                    ]
                }
            ]
        }
    }


def _split_target(target: str) -> tuple[str, str]:
    if ":" not in target:
        return "", target.strip()
    kind, ident = target.split(":", 1)
    return kind.strip().lower(), ident.strip()


def _normalize_media_source(source: str) -> str:
    value = source.strip()
    if not value:
        return value
    if match := _HTML_HREF_RE.search(value):
        return match.group(1).strip()
    if match := _MARKDOWN_LINK_RE.search(value):
        return match.group(1).strip()
    if match := _PLAIN_URL_RE.search(value):
        return match.group(0).strip()
    return value


def _is_remote_stream_source(source: str) -> bool:
    value = source.lower().strip()
    return value.startswith(("rtsp://", "rtsps://")) or any(part in value for part in _REMOTE_STREAM_SUFFIXES)


def _normalize_outbound_target(target: str, target_type: str) -> str:
    target = target.strip()
    if ":" in target:
        return target
    if target_type in ("user", "group", "channel"):
        return f"{target_type}:{target}"
    return target


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


def _parse_face_tags(text: str) -> str:
    """Replace QQ face markup with readable labels."""

    if not text:
        return text

    def _replace(match: re.Match[str]) -> str:
        try:
            payload = json.loads(base64.b64decode(match.group(1)).decode("utf-8"))
        except Exception:
            return match.group(0)
        face_name = str(payload.get("text") or "").strip()
        return f"【表情: {face_name or '未知表情'}】"

    return _FACE_TAG_RE.sub(_replace, text)


def _parse_reference_indices(data: dict[str, Any]) -> tuple[str, str]:
    ref_msg_idx = ""
    msg_idx = ""
    message_scene = data.get("message_scene") or {}
    ext = message_scene.get("ext") or []
    if isinstance(ext, list):
        for item in ext:
            value = str(item or "")
            if value.startswith("ref_msg_idx="):
                ref_msg_idx = value.removeprefix("ref_msg_idx=")
            elif value.startswith("msg_idx="):
                msg_idx = value.removeprefix("msg_idx=")

    if int(data.get("message_type") or 0) == 103:
        msg_elements = data.get("msg_elements") or []
        if isinstance(msg_elements, list) and msg_elements:
            first = msg_elements[0]
            if isinstance(first, dict):
                ref_msg_idx = str(first.get("msg_idx") or ref_msg_idx or "")

    return ref_msg_idx.strip(), msg_idx.strip()


def _guess_suffix(file_name: str, content_type: str) -> str:
    suffix = Path(file_name).suffix if file_name else ""
    if suffix:
        return suffix
    guessed = mimetypes.guess_extension(content_type or "")
    return guessed or ".bin"


def _guess_image_file_name(image_bytes: bytes) -> str:
    if image_bytes.startswith((b"GIF87a", b"GIF89a")):
        return "image.gif"
    if image_bytes.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image.png"
    if image_bytes.startswith(b"RIFF") and image_bytes[8:12] == b"WEBP":
        return "image.webp"
    return "image.jpg"


def _extract_file_text(raw: bytes, file_name: str) -> str:
    ext = Path(file_name).suffix.lower() if file_name else ""
    if ext in _TEXT_FILE_EXTENSIONS or not ext:
        try:
            return raw.decode("utf-8", errors="replace")
        except Exception:
            return ""
    if ext == ".docx":
        try:
            from io import BytesIO
            import xml.etree.ElementTree as ET
            from zipfile import ZipFile

            with ZipFile(BytesIO(raw)) as zipped:
                xml_content = zipped.read("word/document.xml")
            tree = ET.fromstring(xml_content)
            ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
            return "\n".join(
                "".join(node.text or "" for node in paragraph.iter(f"{{{ns['w']}}}t"))
                for paragraph in tree.iter(f"{{{ns['w']}}}p")
            )
        except Exception:
            return ""
    return ""


def _qq_provider_version() -> str:
    try:
        manifest_path = Path(__file__).resolve().parents[1] / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        return str(manifest.get("version") or "unknown")
    except Exception:
        return "unknown"


class QQClient:
    """QQ bot gateway client."""

    def __init__(
        self,
        hass: HomeAssistant,
        app_id: str,
        client_secret: str,
        agent_id: str,
        *,
        subentry_id: str,
        show_live_progress: bool,
    ) -> None:
        self._hass = hass
        self._session = async_get_clientsession(hass)
        self._app_id = app_id
        self._client_secret = client_secret
        self._agent_id = agent_id
        self._subentry_id = subentry_id
        self._task: asyncio.Task[None] | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._status = "disconnected"
        self._token = ""
        self._token_expire = 0.0
        self._gateway_seq: int | None = None
        self._session_id = ""
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._store: Store[dict[str, Any]] = Store(hass, _STORE_VERSION, f"cn_im_hub_qq_{subentry_id}")
        self._tracker = None
        self._reference_index: dict[str, QQReferenceEntry] = {}
        self._reply_sequences: dict[str, int] = {}
        self._group_proactive_status: dict[str, str] = {}
        self._show_live_progress = show_live_progress
        self._card_futures: dict[str, asyncio.Future[str]] = {}
        self._card_contexts: dict[str, QQInboundMessage] = {}

    @property
    def status(self) -> str:
        return self._status

    def _build_upstream_prompt(self, inbound: QQInboundMessage) -> str | None:
        is_rich = inbound.target_kind in ("user", "group")
        return build_qq_prompt(is_rich=is_rich)

    async def start(self) -> None:
        await self._async_load_state()
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        self._stop_heartbeat()
        if self._ws and not self._ws.closed:
            await self._ws.close()
        self._ws = None
        self._status = "disconnected"

    async def send_text(self, target: str, text: str, target_type: str = "") -> None:
        await self._send_proactive_text_message(
            _normalize_outbound_target(target, target_type),
            text,
            message_format="auto",
        )

    async def send_text_formatted(
        self,
        target: str,
        text: str,
        target_type: str = "",
        message_format: str = "auto",
    ) -> None:
        await self._send_proactive_text_message(
            _normalize_outbound_target(target, target_type),
            text,
            message_format=message_format,
        )

    async def send_image(self, target: str, image_bytes: bytes, target_type: str) -> None:
        await self._send_image_message(
            _normalize_outbound_target(target, target_type),
            image_bytes,
            target_type=target_type,
            reply_to_message_id=None,
            file_name=_guess_image_file_name(image_bytes),
        )

    async def send_media(
        self,
        target: str,
        media_bytes: bytes,
        media_kind: str,
        target_type: str,
        file_name: str | None = None,
    ) -> None:
        await self._send_media_message(
            _normalize_outbound_target(target, target_type),
            media_bytes,
            media_kind=media_kind,
            target_type=target_type,
            reply_to_message_id=None,
            file_name=file_name,
        )

    async def send_tts(self, target: str, text: str, target_type: str) -> None:
        voice_bytes = await async_generate_tts_mp3(self._hass, text)
        await self._send_voice_message(
            _normalize_outbound_target(target, target_type),
            voice_bytes,
            target_type=target_type,
            reply_to_message_id=None,
        )

    async def send_approval(
        self,
        target: str,
        text: str,
        target_type: str,
        approval_id: str,
    ) -> None:
        if target_type not in ("user", "group"):
            raise ValueError("QQ approval buttons only support user and group targets")
        await self._send_proactive_text_message(
            _normalize_outbound_target(target, target_type),
            text,
            message_format="markdown",
            inline_keyboard=_build_approval_keyboard(approval_id),
        )

    async def _run_live_progress_bridge(
        self,
        inbound: QQInboundMessage,
        state: QQLiveProgressState,
    ) -> None:
        queue: asyncio.Queue[str] = asyncio.Queue()

        @callback
        def _listener(event) -> None:
            payload = event.data or {}
            if payload.get("conversation_id") != state.conversation_id:
                return
            state.last_progress_at = asyncio.get_running_loop().time()
            if not self._show_live_progress:
                return
            text = _format_live_progress(payload)
            if not text:
                return
            queue.put_nowait(text)

        unsub = self._hass.bus.async_listen(EVENT_LIVE_PROGRESS, _listener)
        pending_tasks: list[asyncio.Task] = []
        
        async def _fire_and_forget(msg: str) -> None:
            # Live progress shares the same msg_id passive budget as the real
            # answer; cap it so the final reply always has quota left.
            if state.replies_used >= _QQ_PASSIVE_AUX_REPLY_LIMIT:
                return
            with contextlib.suppress(Exception):
                await self._send_text_message(
                    inbound.target,
                    msg,
                    reply_to_message_id=inbound.message_id or None,
                )
                state.replies_used += 1
        
        try:
            while True:
                text = await queue.get()
                if text == state.last_sent_text:
                    continue
                task = asyncio.create_task(_fire_and_forget(text))
                pending_tasks.append(task)
                state.last_sent_text = text
        except asyncio.CancelledError:
            while not queue.empty():
                text = queue.get_nowait()
                if text and text != state.last_sent_text:
                    task = asyncio.create_task(_fire_and_forget(text))
                    pending_tasks.append(task)
                    state.last_sent_text = text
            if pending_tasks:
                await asyncio.gather(*pending_tasks, return_exceptions=True)
        finally:
            unsub()

    async def _typing_keepalive_active(
        self,
        user_openid: str,
        message_id: str,
        state: QQLiveProgressState,
    ) -> None:
        with contextlib.suppress(Exception):
            await self._send_typing_notify(user_openid, message_id)
        while True:
            now = asyncio.get_running_loop().time()
            if (now - state.last_progress_at) <= _LIVE_PROGRESS_TYPING_IDLE_SECONDS:
                with contextlib.suppress(Exception):
                    await self._send_typing_notify(user_openid, message_id)
            await asyncio.sleep(_LIVE_PROGRESS_TYPING_INTERVAL_SECONDS)

    @staticmethod
    def _retry_delay(attempt: int) -> int:
        return min(_QQ_RECONNECT_BASE_DELAY_SECONDS * (2 ** min(attempt - 1, 6)), _QQ_RECONNECT_MAX_DELAY_SECONDS)

    async def _run(self) -> None:
        consecutive_failures = 0
        forced_delay = 0.0
        while True:
            self._status = "connecting"
            try:
                token = await self._get_token()
                gateway = await self._get_gateway(token)
                self._ws = await self._session.ws_connect(gateway, heartbeat=30)
                self._status = "connected"
                consecutive_failures = 0
                forced_delay = 0.0
                async for msg in self._ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    await self._handle_payload(json.loads(msg.data))
            except asyncio.CancelledError:
                raise
            except _QQFatalCloseError as err:
                _LOGGER.error("%s", err)
                self._status = "error"
                return
            except Exception as err:
                consecutive_failures += 1
                _LOGGER.warning(
                    "QQ loop error (attempt %d, retry in %ds): %s",
                    consecutive_failures,
                    self._retry_delay(consecutive_failures),
                    err,
                )
            finally:
                self._stop_heartbeat()
                close_code = self._ws.close_code if self._ws is not None else None
                if self._ws and not self._ws.closed:
                    await self._ws.close()
                self._ws = None
                forced_delay = self._apply_close_code(close_code)
                if self._status not in ("error",):
                    self._status = "disconnected"
            await asyncio.sleep(forced_delay if forced_delay > 0 else self._retry_delay(consecutive_failures))

    def _apply_close_code(self, code: int | None) -> float:
        """Adjust reconnect behavior based on the gateway close code.

        Returns an extra delay in seconds when the server mandates a wait.
        """
        if code is None:
            return 0.0
        if code == 4004:
            _LOGGER.warning("QQ gateway auth failure (close 4004), forcing token refresh")
            self._token = ""
            self._token_expire = 0.0
        elif code == 4008:
            _LOGGER.warning("QQ gateway connect rate limited (close 4008), waiting 60s")
            return 60.0
        elif code in (4006, 4007, 4009) or 4900 <= code <= 4913:
            _LOGGER.warning("QQ gateway session invalid (close %s), dropping session for re-identify", code)
            self._session_id = ""
        elif code in (4914, 4915):
            raise _QQFatalCloseError(f"QQ bot banned or invalid (close {code}), stopping")
        return 0.0

    def _start_heartbeat(self, interval_ms: int) -> None:
        self._stop_heartbeat()
        interval = max(interval_ms / 1000, 5.0)

        async def _loop() -> None:
            while True:
                await asyncio.sleep(interval)
                try:
                    await self._send_heartbeat()
                except Exception as err:
                    _LOGGER.debug("QQ heartbeat send failed: %s", err)
                    return

        self._heartbeat_task = asyncio.create_task(_loop())

    def _stop_heartbeat(self) -> None:
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None

    async def _send_heartbeat(self) -> None:
        if self._ws is not None and not self._ws.closed:
            await self._ws.send_json({"op": 1, "d": self._gateway_seq})

    async def _handle_payload(self, payload: dict[str, Any]) -> None:
        op = payload.get("op")
        if op == 10:
            self._start_heartbeat(int((payload.get("d") or {}).get("heartbeat_interval") or 30_000))
            await self._identify_or_resume()
            return
        if op == 1:
            await self._send_heartbeat()
            return
        if op == 7:
            _LOGGER.info("QQ gateway requested reconnect (op 7)")
            if self._ws is not None and not self._ws.closed:
                await self._ws.close()
            return
        if op == 9:
            _LOGGER.warning("QQ gateway reported invalid session (op 9), re-identifying")
            self._session_id = ""
            if not payload.get("d"):
                await self._identify()
            return
        if op != 0:
            return

        seq = payload.get("s")
        if isinstance(seq, int):
            self._gateway_seq = seq
        event_type = str(payload.get("t") or "")
        data = payload.get("d") or {}
        if event_type == "READY":
            self._session_id = str(data.get("session_id") or "")
            _LOGGER.info("QQ gateway session ready (session_id=%s...)", self._session_id[:8])
            return
        if event_type == "RESUMED":
            _LOGGER.info("QQ gateway session resumed")
            return
        if event_type == "INTERACTION_CREATE":
            await self._handle_interaction(data)
            return
        if event_type == "GROUP_MSG_REJECT":
            await self._handle_group_proactive_status(data, _GROUP_PROACTIVE_REJECT)
            return
        if event_type == "GROUP_MSG_RECEIVE":
            await self._handle_group_proactive_status(data, _GROUP_PROACTIVE_ACCEPT)
            return
        inbound = await self._parse_inbound(event_type, data)
        if inbound is None:
            return
        asyncio.create_task(self._process_inbound(inbound))

    async def _identify_or_resume(self) -> None:
        if self._session_id and self._gateway_seq is not None:
            await self._resume()
        else:
            await self._identify()

    async def _resume(self) -> None:
        token = await self._get_token()
        if self._ws is not None and not self._ws.closed:
            await self._ws.send_json(
                {
                    "op": 6,
                    "d": {
                        "token": f"QQBot {token}",
                        "session_id": self._session_id,
                        "seq": self._gateway_seq,
                    },
                }
            )

    async def _process_inbound(self, inbound: QQInboundMessage) -> None:
        if self._tracker is not None:
            await self._tracker.async_record(
                provider=PROVIDER_QQ,
                target=inbound.target_id,
                target_type=inbound.target_kind,
                display_name=inbound.display_name or inbound.target_id,
            )

        if inbound.msg_idx:
            await self._async_record_reference(
                inbound.msg_idx,
                inbound.text,
                inbound.display_name or inbound.target_id,
            )

        if await self._handle_slash_command(inbound):
            return

        try:
            command = parse_command(inbound.text)
        except ValueError as err:
            await self._send_text_message(
                inbound.target,
                f"Invalid command: {err}",
                reply_to_message_id=inbound.message_id or None,
            )
            return
        if command is None:
            return

        typing_task: asyncio.Task[None] | None = None
        progress_task: asyncio.Task[None] | None = None
        if inbound.target_kind == "user" and inbound.message_id:
            progress_state = QQLiveProgressState(
                conversation_id=f"qq:{inbound.target}",
                last_progress_at=asyncio.get_running_loop().time(),
            )
            typing_task = asyncio.create_task(
                self._typing_keepalive_active(inbound.target_id, inbound.message_id, progress_state)
            )
            progress_task = asyncio.create_task(
                self._run_live_progress_bridge(inbound, progress_state)
            )

        try:
            reply = await execute_command(
                self._hass,
                command,
                conversation_id=f"qq:{inbound.target}",
                agent_id=self._agent_id,
                extra_system_prompt=self._build_upstream_prompt(inbound),
                user_id=inbound.display_name or inbound.target_id,
            )
            if progress_task is not None:
                progress_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await progress_task
                progress_task = None
                await asyncio.sleep(0.3)
            if not reply:
                return
            segments = parse_reply_segments(reply)
            prev_was_media = False
            for segment in segments:
                if prev_was_media:
                    await asyncio.sleep(1.0)
                prev_was_media = False
                try:
                    if isinstance(segment, TextSegment):
                        await self._send_text_message(
                            inbound.target,
                            segment.text,
                            reply_to_message_id=inbound.message_id or None,
                        )
                    elif isinstance(segment, ImageSegment):
                        image_bytes = await self._resolve_image(segment.source)
                        if image_bytes is None:
                            await self._send_text_message(
                                inbound.target,
                                f"Image source unavailable: {segment.source}",
                                reply_to_message_id=inbound.message_id or None,
                            )
                            continue
                        try:
                            await self._send_image_message(
                                inbound.target,
                                image_bytes,
                                target_type=inbound.target_kind,
                                reply_to_message_id=inbound.message_id or None,
                            )
                            prev_was_media = True
                        except ValueError:
                            await self._send_text_message(
                                inbound.target,
                                "当前 QQ 频道暂不支持图片回复。",
                                reply_to_message_id=inbound.message_id or None,
                            )
                        except Exception as err:
                            _LOGGER.warning("QQ image send failed (%s): %s", segment.source, err)
                            await self._send_text_message(
                                inbound.target,
                                f"图片发送失败: {segment.source}",
                                reply_to_message_id=inbound.message_id or None,
                            )
                    elif isinstance(segment, VoiceSegment):
                        try:
                            voice_bytes = await async_generate_tts_mp3(
                                self._hass,
                                segment.text,
                            )
                            await self._send_voice_message(
                                inbound.target,
                                voice_bytes,
                                target_type=inbound.target_kind,
                                reply_to_message_id=inbound.message_id or None,
                            )
                            prev_was_media = True
                        except ValueError:
                            await self._send_text_message(
                                inbound.target,
                                "当前 QQ 频道暂不支持语音回复。",
                                reply_to_message_id=inbound.message_id or None,
                            )
                        except Exception as err:
                            _LOGGER.warning("QQ TTS generation failed: %s", err)
                            await self._send_text_message(
                                inbound.target,
                                segment.text,
                                reply_to_message_id=inbound.message_id or None,
                            )
                    elif isinstance(segment, FileSegment):
                        try:
                            normalized_source = _normalize_media_source(segment.source)
                            file_bytes, file_name = await self._resolve_media_source(
                                normalized_source,
                                default_name="attachment.bin",
                            )
                            await self._send_media_message(
                                inbound.target,
                                file_bytes,
                                media_kind="file",
                                target_type=inbound.target_kind,
                                reply_to_message_id=inbound.message_id or None,
                                file_name=file_name,
                            )
                            prev_was_media = True
                        except Exception as err:
                            _LOGGER.warning("QQ file send failed: %s", err)
                            await self._send_text_message(
                                inbound.target,
                                f"File send failed: {type(err).__name__}: {err}",
                                reply_to_message_id=inbound.message_id or None,
                            )
                    elif isinstance(segment, VideoSegment):
                        try:
                            normalized_source = _normalize_media_source(segment.source)
                            resolved_camera = await async_resolve_camera_entity(self._hass, normalized_source)
                            if resolved_camera is not None:
                                video_bytes, file_name = await async_record_camera_clip(
                                    self._hass,
                                    resolved_camera,
                                )
                            elif _is_remote_stream_source(normalized_source):
                                video_bytes, file_name = await async_record_remote_stream_clip(
                                    self._hass,
                                    normalized_source,
                                )
                            else:
                                video_bytes, file_name = await self._resolve_media_source(
                                    normalized_source,
                                    default_name="video.mp4",
                                )
                            await self._send_media_message(
                                inbound.target,
                                video_bytes,
                                media_kind="video",
                                target_type=inbound.target_kind,
                                reply_to_message_id=inbound.message_id or None,
                                file_name=file_name,
                            )
                            prev_was_media = True
                        except Exception as err:
                            _LOGGER.warning("QQ video send failed: %s", err)
                            await self._send_text_message(
                                inbound.target,
                                f"Video send failed: {type(err).__name__}: {err}",
                                reply_to_message_id=inbound.message_id or None,
                            )
                    elif isinstance(segment, GifSegment):
                        try:
                            normalized_source = _normalize_media_source(segment.source)
                            resolved_camera = await async_resolve_camera_entity(self._hass, normalized_source)
                            if resolved_camera is not None:
                                gif_bytes, _ = await async_capture_camera_gif(
                                    self._hass,
                                    resolved_camera,
                                )
                            else:
                                gif_bytes, _ = await self._resolve_media_source(
                                    normalized_source,
                                    default_name="animated.gif",
                                )
                            await self._send_image_message(
                                inbound.target,
                                gif_bytes,
                                target_type=inbound.target_kind,
                                reply_to_message_id=inbound.message_id or None,
                                file_name="animated.gif",
                            )
                            prev_was_media = True
                        except Exception as err:
                            _LOGGER.warning("QQ gif send failed: %s", err)
                            await self._send_text_message(
                                inbound.target,
                                f"GIF source unavailable: {segment.source}",
                                reply_to_message_id=inbound.message_id or None,
                            )
                    elif isinstance(segment, CardSegment):
                        card_spec = parse_card_source(segment.source)
                        if card_spec is None:
                            await self._send_text_message(
                                inbound.target,
                                f"Invalid card: {segment.source[:100]}",
                                reply_to_message_id=inbound.message_id or None,
                            )
                            continue
                        card_id = card_spec.card_id or uuid.uuid4().hex[:8]
                        card_spec.card_id = card_id
                        keyboard = build_inline_keyboard(card_spec)
                        await self._send_text_message(
                            inbound.target,
                            card_spec.text or " ",
                            reply_to_message_id=inbound.message_id or None,
                            message_format="markdown",
                            inline_keyboard=keyboard,
                        )
                        self._card_contexts[card_id] = inbound
                except _QQPassiveReplyLimitError:
                    _LOGGER.warning(
                        "QQ passive回复配额已用尽 (msg_id=%s), 丢弃剩余分段",
                        inbound.message_id,
                    )
                    break
        except Exception as err:
            _LOGGER.exception("QQ command execution failed: %s", err)
            await self._send_text_message(
                inbound.target,
                f"Execution failed: {type(err).__name__}",
                reply_to_message_id=inbound.message_id or None,
            )
        finally:
            if progress_task is not None:
                progress_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await progress_task
            if typing_task is not None:
                typing_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await typing_task

    async def _handle_interaction(self, data: dict[str, Any]) -> None:
        interaction_id = str(data.get("id") or "").strip()
        if interaction_id:
            await self._acknowledge_interaction(interaction_id)

        resolved = (data.get("data") or {}).get("resolved") or {}
        button_data = str(resolved.get("button_data") or "").strip()
        user_id = str(
            data.get("group_member_openid")
            or data.get("user_openid")
            or resolved.get("user_id")
            or ""
        ).strip()
        group_id = str(data.get("group_openid") or "").strip()
        channel_id = str(data.get("channel_id") or "").strip()
        self._hass.bus.async_fire(
            _INTERACTION_EVENT,
            {
                "provider": PROVIDER_QQ,
                "button_data": button_data,
                "user_id": user_id,
                "group_id": group_id,
                "channel_id": channel_id,
                "raw": data,
            },
        )
        card_match = re.match(r"^card:([^:]*):(.+)$", button_data)
        if card_match:
            card_id, selection = card_match.groups()
            fut = self._card_futures.pop(card_id, None)
            if fut and not fut.done():
                fut.set_result(selection)
            else:
                inbound = self._card_contexts.pop(card_id, None)
                if inbound is not None:
                    asyncio.create_task(
                        self._handle_card_selection(inbound, selection)
                    )
            self._hass.bus.async_fire(
                _INTERACTION_EVENT,
                {
                    "provider": PROVIDER_QQ,
                    "type": "card_selection",
                    "card_id": card_id,
                    "selection": selection,
                    "user_id": user_id,
                    "group_id": group_id,
                },
            )
            return

        match = re.match(r"^approve:([^:]+(?:[:][^:]+)?):(allow-once|allow-always|deny)$", button_data)
        if not match:
            return
        approval_id, decision = match.groups()
        self._hass.bus.async_fire(
            _APPROVAL_EVENT,
            {
                "provider": PROVIDER_QQ,
                "approval_id": approval_id,
                "decision": decision,
                "user_id": user_id,
                "group_id": group_id,
                "channel_id": channel_id,
            },
        )

    async def _handle_card_selection(self, inbound: QQInboundMessage, selection: str) -> None:
        try:
            ack = random.choice(_CARD_ACK_PHRASES)
            await self._send_text_message(
                inbound.target,
                ack,
                reply_to_message_id=inbound.message_id or None,
            )
            cmd = parse_command(f"用户选择了: {selection}")
            if cmd is not None:
                reply = await execute_command(
                    self._hass,
                    cmd,
                    conversation_id=f"qq:{inbound.target}",
                    agent_id=self._agent_id,
                    extra_system_prompt=self._build_upstream_prompt(inbound),
                )
                if reply:
                    await self._send_text_message(
                        inbound.target,
                        reply,
                        reply_to_message_id=inbound.message_id or None,
                    )
        except Exception as err:
            _LOGGER.warning("QQ card selection handling failed: %s", err)

    async def _handle_group_proactive_status(self, data: dict[str, Any], status: str) -> None:
        group_id = str(data.get("group_openid") or "").strip()
        operator_id = str(data.get("op_member_openid") or "").strip()
        if not group_id:
            return
        self._group_proactive_status[group_id] = status
        await self._async_save_state()
        self._hass.bus.async_fire(
            _GROUP_PROACTIVE_EVENT,
            {
                "provider": PROVIDER_QQ,
                "group_id": group_id,
                "status": status,
                "operator_id": operator_id,
            },
        )

    _APPROVE_MODES: dict[str, tuple[str, str]] = {
        "on": ("allowlist", "白名单模式 — 已批准命令自动加入白名单，其余需审批"),
        "off": ("full", "审批已关闭 — 所有命令直接执行"),
        "always": ("always", "严格模式 — 每次执行都需要人工审批"),
    }

    async def _handle_slash_command(self, inbound: QQInboundMessage) -> bool:
        text = inbound.text.strip()
        if not text.startswith("/bot-"):
            return False
        parts = text.split(None, 1)
        cmd = parts[0]
        arg = parts[1].strip().lower() if len(parts) > 1 else ""

        if cmd == "/bot-ping":
            reply = f"pong\nstatus={self._status}\ntarget={inbound.target}"
        elif cmd == "/bot-version":
            reply = f"cn_im_hub qq provider\nversion={_qq_provider_version()}"
        elif cmd == "/bot-approve":
            return await self._handle_approve_command(inbound, arg)
        elif cmd == "/bot-help":
            reply = (
                "QQ commands:\n"
                "/bot-ping — 检测连接\n"
                "/bot-version — 查看版本\n"
                "/bot-approve — 管理命令执行审批\n"
                "/bot-help — 显示帮助"
            )
        else:
            return False
        await self._send_text_message(
            inbound.target,
            reply,
            reply_to_message_id=inbound.message_id or None,
        )
        return True

    async def _handle_approve_command(self, inbound: QQInboundMessage, arg: str) -> bool:
        stored = await self._store.async_load() or {}
        current = str(stored.get("approve_mode", "on"))

        if arg == "status" or not arg:
            mode_info = self._APPROVE_MODES.get(current, ("unknown", "未知模式"))
            status_icon = {"on": "🟡", "off": "🟢", "always": "🔴"}.get(current, "🟡")
            text = f"{status_icon} 当前审批配置: {mode_info[1]}"
            keyboard = build_inline_keyboard(CardSpec(
                text=text,
                card_id="approve_menu",
                rows=[
                    CardRow(buttons=[
                        CardButton(label="🟢 开启审批", data="approve_set:on", style=1),
                        CardButton(label="🔴 关闭审批", data="approve_set:off", style=3),
                    ]),
                    CardRow(buttons=[
                        CardButton(label="🟡 严格模式", data="approve_set:always", style=0),
                        CardButton(label="恢复默认", data="approve_set:reset", style=0),
                    ]),
                ],
            ))
            await self._send_text_message(
                inbound.target,
                text,
                reply_to_message_id=inbound.message_id or None,
                message_format="markdown",
                inline_keyboard=keyboard,
            )
            fut: asyncio.Future[str] = asyncio.get_running_loop().create_future()
            self._card_futures["approve_menu"] = fut
            try:
                selection = await asyncio.wait_for(fut, timeout=120.0)
                m = re.match(r"^approve_set:(\w+)$", selection)
                if m:
                    return await self._apply_approve_mode(inbound, m.group(1))
            except asyncio.TimeoutError:
                self._card_futures.pop("approve_menu", None)
            return True

        if arg == "reset":
            arg = "on"
        if arg in self._APPROVE_MODES:
            return await self._apply_approve_mode(inbound, arg)

        await self._send_text_message(
            inbound.target,
            f"未知参数: {arg}\n用法: /bot-approve [on|off|always|reset|status]",
            reply_to_message_id=inbound.message_id or None,
        )
        return True

    async def _apply_approve_mode(self, inbound: QQInboundMessage, mode: str) -> bool:
        mode_info = self._APPROVE_MODES.get(mode, ("on", "白名单模式"))
        stored = await self._store.async_load() or {}
        stored["approve_mode"] = mode
        await self._store.async_save(stored)
        icon = {"on": "🟢", "off": "🟢", "always": "🔴"}.get(mode, "🟡")
        await self._send_text_message(
            inbound.target,
            f"{icon} 审批配置已更新: {mode_info[1]}",
            reply_to_message_id=inbound.message_id or None,
        )
        return True

    async def _parse_inbound(
        self,
        event_type: str,
        data: dict[str, Any],
    ) -> QQInboundMessage | None:
        target_kind = ""
        target_id = ""
        display_name = ""
        message_id = str(data.get("id") or "").strip()

        if event_type == "C2C_MESSAGE_CREATE":
            target_kind = "user"
            target_id = str((data.get("author") or {}).get("user_openid") or "").strip()
            display_name = str((data.get("author") or {}).get("user_openid") or target_id).strip()
        elif event_type == "GROUP_AT_MESSAGE_CREATE":
            target_kind = "group"
            target_id = str(data.get("group_openid") or "").strip()
            author = data.get("author") or {}
            display_name = str(author.get("username") or author.get("member_openid") or target_id).strip()
        elif event_type in ("AT_MESSAGE_CREATE", "DIRECT_MESSAGE_CREATE"):
            target_kind = "channel"
            target_id = str(data.get("channel_id") or "").strip()
            author = data.get("author") or {}
            display_name = str(author.get("username") or author.get("id") or target_id).strip()
        else:
            return None

        if not target_id:
            return None

        ref_msg_idx, msg_idx = _parse_reference_indices(data)
        text = await self._build_message_text(
            str(data.get("content") or ""),
            data.get("attachments"),
        )

        quote_text = await self._resolve_quote_text(data, ref_msg_idx)
        if quote_text:
            text = (
                f"[引用消息开始]\n{quote_text}\n[引用消息结束]\n{text}".strip()
                if text
                else f"[引用消息开始]\n{quote_text}\n[引用消息结束]"
            )

        if not text:
            return None

        return QQInboundMessage(
            text=text,
            target=f"{target_kind}:{target_id}",
            target_kind=target_kind,
            target_id=target_id,
            message_id=message_id,
            display_name=display_name,
            ref_msg_idx=ref_msg_idx,
            msg_idx=msg_idx,
        )

    async def _build_message_text(
        self,
        content: str,
        attachments: Any,
    ) -> str:
        parts: list[str] = []
        parsed_content = _parse_face_tags(str(content or "").strip())
        if parsed_content:
            parts.append(parsed_content)

        if isinstance(attachments, list):
            for attachment in attachments:
                if not isinstance(attachment, dict):
                    continue
                note, tag = await self._process_attachment(attachment)
                if note:
                    parts.append(note)
                if tag:
                    parts.append(tag)

        return "\n".join(part for part in parts if part).strip()

    async def _process_attachment(self, attachment: dict[str, Any]) -> tuple[str, str]:
        content_type = str(attachment.get("content_type") or "").strip().lower()
        file_name = str(attachment.get("filename") or "").strip()
        file_url = str(attachment.get("url") or "").strip()
        voice_wav_url = str(attachment.get("voice_wav_url") or "").strip()
        asr_text = str(attachment.get("asr_refer_text") or "").strip()
        source_url = voice_wav_url or file_url

        if not source_url:
            if asr_text:
                return (f"[语音消息] {asr_text}", "")
            if file_name:
                return (f"[用户发送了文件 {file_name}]", "")
            return ("", "")

        if content_type.startswith("image/"):
            local_path = await self._download_attachment(source_url, file_name, content_type)
            if local_path:
                return ("", f"[ATTACHMENT:{content_type}:{local_path}]")
            return ("[用户发送了一张图片]", "")

        if (
            content_type == "voice"
            or content_type.startswith("audio/")
            or "silk" in content_type
            or "amr" in content_type
        ):
            mime = "audio/wav" if voice_wav_url else (content_type or "audio/mpeg")
            local_path = await self._download_attachment(source_url, file_name, mime)
            note = f"[语音消息] {asr_text}" if asr_text else "[语音消息]"
            if local_path:
                return (note if asr_text else "", f"[ATTACHMENT:{mime}:{local_path}]")
            return (note, "")

        if content_type.startswith("video/"):
            local_path = await self._download_attachment(source_url, file_name, content_type)
            note = f"[用户发送了视频 {file_name or 'video'}]"
            if local_path:
                return (note, f"[ATTACHMENT:{content_type}:{local_path}]")
            return (note, "")

        mime = content_type or "application/octet-stream"
        local_path = await self._download_attachment(source_url, file_name, mime)
        if not local_path:
            return (f"[用户发送了文件 {file_name or 'file'}]", "")

        extracted = await self._extract_file_preview(local_path, file_name)
        if extracted:
            note = f"[用户发送了文件 {file_name or Path(local_path).name}，内容如下：]\n{extracted[:8000]}"
        else:
            note = f"[用户发送了文件 {file_name or Path(local_path).name}]"
        return (note, f"[ATTACHMENT:{mime}:{local_path}]")

    async def _download_attachment(
        self,
        source_url: str,
        file_name: str,
        content_type: str,
    ) -> str | None:
        url = source_url.strip()
        if not url:
            return None
        if url.startswith("//"):
            url = f"https:{url}"

        tmp_dir = Path(self._hass.config.path(".storage", "cn_im_hub", "tmp"))
        tmp_dir.mkdir(parents=True, exist_ok=True)
        suffix = _guess_suffix(file_name, content_type)
        tmp_path = str(tmp_dir / f"qq_{uuid.uuid4().hex[:12]}{suffix}")

        try:
            chunks: list[bytes] = []
            async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                if resp.status >= 400:
                    raise RuntimeError(f"download failed: {resp.status}")
                async for chunk in resp.content.iter_chunked(64 * 1024):
                    chunks.append(chunk)
            data = b"".join(chunks)
            await self._hass.async_add_executor_job(Path(tmp_path).write_bytes, data)
            return tmp_path
        except Exception as err:
            _LOGGER.warning("QQ attachment download failed (%s): %s", url, err)
            with contextlib.suppress(OSError):
                Path(tmp_path).unlink()
            return None

    async def _extract_file_preview(self, local_path: str, file_name: str) -> str:
        def _read_preview() -> str:
            raw = Path(local_path).read_bytes()
            return _extract_file_text(raw, file_name or Path(local_path).name)

        try:
            return await self._hass.async_add_executor_job(_read_preview)
        except Exception:
            return ""

    async def _resolve_quote_text(self, data: dict[str, Any], ref_msg_idx: str) -> str:
        if ref_msg_idx:
            cached = self._reference_index.get(ref_msg_idx)
            if cached:
                sender = cached.display_name or "引用消息"
                return f"来自 {sender}:\n{cached.text}".strip()

        if int(data.get("message_type") or 0) != 103:
            return ""

        msg_elements = data.get("msg_elements") or []
        if not isinstance(msg_elements, list) or not msg_elements:
            return ""

        first = msg_elements[0]
        if not isinstance(first, dict):
            return ""

        quoted = await self._build_message_text(
            str(first.get("content") or ""),
            first.get("attachments"),
        )
        return f"来自 引用消息:\n{quoted}".strip() if quoted else ""

    async def _async_record_reference(
        self,
        msg_idx: str,
        text: str,
        display_name: str,
    ) -> None:
        value = msg_idx.strip()
        if not value or not text.strip():
            return

        self._reference_index.pop(value, None)
        self._reference_index[value] = QQReferenceEntry(
            msg_idx=value,
            text=text,
            display_name=display_name.strip(),
        )
        while len(self._reference_index) > _MAX_REFERENCE_MESSAGES:
            oldest = next(iter(self._reference_index))
            self._reference_index.pop(oldest, None)
        await self._async_save_state()

    async def _async_load_state(self) -> None:
        data = await self._store.async_load() or {}
        items = data.get("reference_index") or []
        if not isinstance(items, list):
            return
        self._reference_index = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            msg_idx = str(item.get("msg_idx") or "").strip()
            text = str(item.get("text") or "").strip()
            if not msg_idx or not text:
                continue
            self._reference_index[msg_idx] = QQReferenceEntry(
                msg_idx=msg_idx,
                text=text,
                display_name=str(item.get("display_name") or "").strip(),
            )
        proactive_status = data.get("group_proactive_status") or {}
        if isinstance(proactive_status, dict):
            self._group_proactive_status = {
                str(key): str(value)
                for key, value in proactive_status.items()
                if str(value) in (
                    _GROUP_PROACTIVE_ACCEPT,
                    _GROUP_PROACTIVE_REJECT,
                    _GROUP_PROACTIVE_UNKNOWN,
                )
            }

    async def _async_save_state(self) -> None:
        await self._store.async_save(
            {
                "reference_index": [
                    asdict(item)
                    for item in self._reference_index.values()
                ],
                "group_proactive_status": self._group_proactive_status,
            }
        )

    async def _send_text_message(
        self,
        target: str,
        text: str,
        *,
        reply_to_message_id: str | None,
        message_format: str = "auto",
        inline_keyboard: dict[str, Any] | None = None,
    ) -> None:
        token = await self._get_token()
        kind, ident = _split_target(target)
        if kind == "user":
            path = f"/v2/users/{ident}/messages"
            body = self._build_text_body(
                text,
                kind=kind,
                reply_to_message_id=reply_to_message_id,
                message_format=message_format,
                inline_keyboard=inline_keyboard,
            )
            if reply_to_message_id:
                body["msg_id"] = reply_to_message_id
        elif kind == "group":
            path = f"/v2/groups/{ident}/messages"
            body = self._build_text_body(
                text,
                kind=kind,
                reply_to_message_id=reply_to_message_id,
                message_format=message_format,
                inline_keyboard=inline_keyboard,
            )
            if reply_to_message_id:
                body["msg_id"] = reply_to_message_id
        elif kind == "channel":
            path = f"/channels/{ident}/messages"
            body = {"content": text}
            if reply_to_message_id:
                body["message_reference"] = {"message_id": reply_to_message_id}
        else:
            raise ValueError("QQ target must be user:/group:/channel:")

        async with self._session.post(
            f"{_API_BASE}{path}",
            headers={"Authorization": f"QQBot {token}"},
            json=body,
            timeout=15,
        ) as resp:
            if resp.status < 400:
                return
            error_body = await resp.text()
            if resp.status == 401:
                # Token invalidated server-side: refresh once and retry.
                _LOGGER.warning("QQ send rejected with 401, refreshing token and retrying once")
                self._token = ""
                self._token_expire = 0.0
                token = await self._get_token()
                async with self._session.post(
                    f"{_API_BASE}{path}",
                    headers={"Authorization": f"QQBot {token}"},
                    json=body,
                    timeout=15,
                ) as retry_resp:
                    if retry_resp.status < 400:
                        return
                    raise RuntimeError(f"QQ send failed: {retry_resp.status} {await retry_resp.text()}")
            if body.get("msg_type") == 2:
                # Markdown requires special QQ approval; fall back to plain text.
                _LOGGER.warning("QQ markdown send rejected (%s), falling back to plain text: %s", resp.status, error_body[:200])
                plain: dict[str, Any] = {"content": text, "msg_type": 0}
                if reply_to_message_id:
                    plain["msg_id"] = reply_to_message_id
                    plain["msg_seq"] = self._next_msg_seq(reply_to_message_id)
                async with self._session.post(
                    f"{_API_BASE}{path}",
                    headers={"Authorization": f"QQBot {token}"},
                    json=plain,
                    timeout=15,
                ) as plain_resp:
                    if plain_resp.status >= 400:
                        raise RuntimeError(f"QQ send failed: {plain_resp.status} {await plain_resp.text()}")
                return
            raise RuntimeError(f"QQ send failed: {resp.status} {error_body}")

    async def _send_proactive_text_message(
        self,
        target: str,
        text: str,
        *,
        message_format: str = "auto",
        inline_keyboard: dict[str, Any] | None = None,
    ) -> None:
        token = await self._get_token()
        kind, ident = _split_target(target)
        if kind == "channel":
            body = {"content": text}
            async with self._session.post(
                f"{_API_BASE}/channels/{ident}/messages",
                headers={"Authorization": f"QQBot {token}"},
                json=body,
                timeout=30,
            ) as resp:
                if resp.status >= 400:
                    raise RuntimeError(f"QQ channel send failed: {resp.status} {await resp.text()}")
            return

        if kind == "group":
            group_status = self._group_proactive_status.get(ident, _GROUP_PROACTIVE_UNKNOWN)
            if group_status == _GROUP_PROACTIVE_REJECT:
                raise RuntimeError(
                    f"QQ group {ident} has rejected proactive messages. Ask a group admin to re-enable bot proactive delivery."
                )

        body = self._build_text_body(
            text,
            kind=kind,
            reply_to_message_id=None,
            message_format=message_format,
            inline_keyboard=inline_keyboard,
        )
        if kind == "user":
            path = f"/v2/users/{ident}/messages"
        elif kind == "group":
            path = f"/v2/groups/{ident}/messages"
        else:
            raise ValueError("QQ proactive text only supports user/group/channel targets")

        async with self._session.post(
            f"{_API_BASE}{path}",
            headers={"Authorization": f"QQBot {token}"},
            json=body,
            timeout=30,
        ) as resp:
            if resp.status >= 400:
                raise RuntimeError(f"QQ proactive send failed: {resp.status} {await resp.text()}")

    async def _send_image_message(
        self,
        target: str,
        image_bytes: bytes,
        *,
        target_type: str,
        reply_to_message_id: str | None,
        file_name: str | None = None,
    ) -> None:
        token = await self._get_token()
        if not image_bytes:
            raise ValueError("QQ image data is empty")
        kind = target_type.strip().lower() if target_type else _split_target(target)[0]
        ident = target.strip()
        if ":" in ident:
            kind, ident = _split_target(ident)
        file_info = await self._upload_media(
            token,
            ident,
            kind,
            file_type=1,
            file_data=image_bytes,
            file_name=file_name or _guess_image_file_name(image_bytes),
        )
        if kind == "user":
            path = f"/v2/users/{ident}/messages"
            body: dict[str, Any] = {"msg_type": 7, "media": {"file_info": file_info}}
            if reply_to_message_id:
                body["msg_id"] = reply_to_message_id
                body["msg_seq"] = self._next_msg_seq(reply_to_message_id)
        elif kind == "group":
            path = f"/v2/groups/{ident}/messages"
            body = {"msg_type": 7, "media": {"file_info": file_info}}
            if reply_to_message_id:
                body["msg_id"] = reply_to_message_id
                body["msg_seq"] = self._next_msg_seq(reply_to_message_id)
        else:
            raise ValueError("QQ image sending only supports user and group targets")

        async with self._session.post(
            f"{_API_BASE}{path}",
            headers={"Authorization": f"QQBot {token}"},
            json=body,
            timeout=30,
        ) as resp:
            if resp.status >= 400:
                raise RuntimeError(f"QQ image send failed: {resp.status} {await resp.text()}")

    async def _send_voice_message(
        self,
        target: str,
        voice_bytes: bytes,
        *,
        target_type: str,
        reply_to_message_id: str | None,
    ) -> None:
        token = await self._get_token()
        if not voice_bytes:
            raise ValueError("QQ voice data is empty")
        kind = target_type.strip().lower() if target_type else _split_target(target)[0]
        ident = target.strip()
        if ":" in ident:
            kind, ident = _split_target(ident)
        file_info = await self._upload_media(
            token,
            ident,
            kind,
            file_type=3,
            file_data=voice_bytes,
        )
        if kind == "user":
            path = f"/v2/users/{ident}/messages"
            body: dict[str, Any] = {"msg_type": 7, "media": {"file_info": file_info}}
            if reply_to_message_id:
                body["msg_id"] = reply_to_message_id
                body["msg_seq"] = self._next_msg_seq(reply_to_message_id)
        elif kind == "group":
            path = f"/v2/groups/{ident}/messages"
            body = {"msg_type": 7, "media": {"file_info": file_info}}
            if reply_to_message_id:
                body["msg_id"] = reply_to_message_id
                body["msg_seq"] = self._next_msg_seq(reply_to_message_id)
        else:
            raise ValueError("QQ voice sending only supports user and group targets")

        async with self._session.post(
            f"{_API_BASE}{path}",
            headers={"Authorization": f"QQBot {token}"},
            json=body,
            timeout=30,
        ) as resp:
            if resp.status >= 400:
                raise RuntimeError(f"QQ voice send failed: {resp.status} {await resp.text()}")

    async def _send_media_message(
        self,
        target: str,
        media_bytes: bytes,
        *,
        media_kind: str,
        target_type: str,
        reply_to_message_id: str | None,
        file_name: str | None = None,
    ) -> None:
        if media_kind == "image":
            await self._send_image_message(
                target,
                media_bytes,
                target_type=target_type,
                reply_to_message_id=reply_to_message_id,
            )
            return
        if media_kind == "voice":
            await self._send_voice_message(
                target,
                media_bytes,
                target_type=target_type,
                reply_to_message_id=reply_to_message_id,
            )
            return

        file_type_map = {"video": 2, "file": 4}
        file_type = file_type_map.get(media_kind)
        if file_type is None:
            raise ValueError(f"Unsupported QQ media kind: {media_kind}")

        token = await self._get_token()
        kind = target_type.strip().lower() if target_type else _split_target(target)[0]
        ident = target.strip()
        if ":" in ident:
            kind, ident = _split_target(ident)
        file_info = await self._upload_media(
            token,
            ident,
            kind,
            file_type=file_type,
            file_data=media_bytes,
            file_name=file_name,
        )
        if kind == "user":
            path = f"/v2/users/{ident}/messages"
            body: dict[str, Any] = {"msg_type": 7, "media": {"file_info": file_info}}
            if reply_to_message_id:
                body["msg_id"] = reply_to_message_id
                body["msg_seq"] = self._next_msg_seq(reply_to_message_id)
        elif kind == "group":
            path = f"/v2/groups/{ident}/messages"
            body = {"msg_type": 7, "media": {"file_info": file_info}}
            if reply_to_message_id:
                body["msg_id"] = reply_to_message_id
                body["msg_seq"] = self._next_msg_seq(reply_to_message_id)
        else:
            raise ValueError(f"QQ {media_kind} sending only supports user and group targets")

        async with self._session.post(
            f"{_API_BASE}{path}",
            headers={"Authorization": f"QQBot {token}"},
            json=body,
            timeout=60,
        ) as resp:
            if resp.status >= 400:
                raise RuntimeError(f"QQ {media_kind} send failed: {resp.status} {await resp.text()}")

    async def _send_media_url_message(
        self,
        target: str,
        media_url: str,
        *,
        media_kind: str,
        target_type: str,
        reply_to_message_id: str | None,
        file_name: str | None = None,
    ) -> None:
        file_type_map = {"image": 1, "video": 2, "voice": 3, "file": 4}
        file_type = file_type_map.get(media_kind)
        if file_type is None:
            raise ValueError(f"Unsupported QQ media kind: {media_kind}")

        token = await self._get_token()
        kind = target_type.strip().lower() if target_type else _split_target(target)[0]
        ident = target.strip()
        if ":" in ident:
            kind, ident = _split_target(ident)
        if kind not in ("user", "group"):
            raise ValueError(f"QQ {media_kind} sending only supports user and group targets")

        if kind == "user":
            upload_path = f"/v2/users/{ident}/files"
            send_path = f"/v2/users/{ident}/messages"
        else:
            upload_path = f"/v2/groups/{ident}/files"
            send_path = f"/v2/groups/{ident}/messages"

        upload_body: dict[str, Any] = {
            "file_type": file_type,
            "srv_send_msg": False,
            "url": media_url,
        }
        if file_name:
            upload_body["file_name"] = file_name

        async with self._session.post(
            f"{_API_BASE}{upload_path}",
            headers={"Authorization": f"QQBot {token}"},
            json=upload_body,
            timeout=60,
        ) as resp:
            data = await resp.json(content_type=None)
            if resp.status >= 400:
                raise RuntimeError(f"QQ {media_kind} URL upload failed: {resp.status} {data}")
        file_info = str(data.get("file_info") or "")
        if not file_info:
            raise RuntimeError(f"QQ {media_kind} URL upload missing file_info: {data}")

        body: dict[str, Any] = {"msg_type": 7, "media": {"file_info": file_info}}
        if reply_to_message_id:
            body["msg_id"] = reply_to_message_id
            body["msg_seq"] = self._next_msg_seq(reply_to_message_id)
        async with self._session.post(
            f"{_API_BASE}{send_path}",
            headers={"Authorization": f"QQBot {token}"},
            json=body,
            timeout=60,
        ) as resp:
            if resp.status >= 400:
                raise RuntimeError(f"QQ {media_kind} URL send failed: {resp.status} {await resp.text()}")

    async def _upload_media(
        self,
        token: str,
        ident: str,
        kind: str,
        *,
        file_type: int,
        file_data: bytes,
        file_name: str | None = None,
    ) -> str:
        if kind == "user":
            path = f"/v2/users/{ident}/files"
        elif kind == "group":
            path = f"/v2/groups/{ident}/files"
        else:
            raise ValueError("QQ media upload only supports user and group targets")

        resolved_file_name = file_name or {
            1: "image.jpg",
            2: "video.mp4",
            3: "voice.mp3",
            4: "file.bin",
        }.get(file_type, "file.bin")
        if file_type in (2, 4) or len(file_data) > _DIRECT_UPLOAD_MAX_BYTES:
            return await async_upload_media_chunked(
                self._hass,
                self._session,
                token=token,
                api_base=_API_BASE,
                ident=ident,
                kind=kind,
                file_type=file_type,
                file_bytes=file_data,
                file_name=resolved_file_name,
            )

        body: dict[str, Any] = {
            "file_type": file_type,
            "srv_send_msg": False,
            "file_data": base64.b64encode(file_data).decode("ascii"),
        }
        if resolved_file_name:
            body["file_name"] = resolved_file_name

        async with self._session.post(
            f"{_API_BASE}{path}",
            headers={"Authorization": f"QQBot {token}"},
            json=body,
            timeout=60,
        ) as resp:
            data = await resp.json(content_type=None)
            if resp.status >= 400:
                raise RuntimeError(f"QQ media upload failed: {resp.status} {data}")
        file_info = str(data.get("file_info") or "")
        if not file_info:
            raise RuntimeError(f"QQ media upload missing file_info: {data}")
        return file_info

    async def _resolve_image(self, source: str) -> bytes | None:
        try:
            source = im_public_url_for_source(self._hass, _normalize_media_source(source))
            resolved_camera = await async_resolve_camera_entity(self._hass, source)
            if resolved_camera is not None:
                from homeassistant.components.camera import async_get_image

                image = await async_get_image(self._hass, resolved_camera)
                return image.content
            if is_url(source):
                async with self._session.get(source, timeout=30) as resp:
                    if resp.status < 400:
                        return await resp.read()
                return None
            local_path = resolve_ha_local_path(self._hass, source)
            if local_path is not None:
                return await self._hass.async_add_executor_job(local_path.read_bytes)
        except Exception as err:
            _LOGGER.warning("Failed to resolve QQ image source (%s): %s", source, err)
        return None

    def _build_text_body(
        self,
        text: str,
        *,
        kind: str,
        reply_to_message_id: str | None,
        message_format: str,
        inline_keyboard: dict[str, Any] | None,
    ) -> dict[str, Any]:
        use_markdown = False
        if kind in ("user", "group"):
            if message_format == "markdown":
                use_markdown = True
            elif message_format == "auto":
                use_markdown = _looks_like_markdown(text)

        body: dict[str, Any]
        if use_markdown:
            body = {"markdown": {"content": _fix_qq_markdown(text)}, "msg_type": 2}
        else:
            body = {"content": text, "msg_type": 0}

        if reply_to_message_id:
            body["msg_seq"] = self._next_msg_seq(reply_to_message_id)
        if inline_keyboard:
            body["keyboard"] = inline_keyboard
        return body

    async def _acknowledge_interaction(self, interaction_id: str) -> None:
        token = await self._get_token()
        async with self._session.put(
            f"{_API_BASE}/interactions/{interaction_id}",
            headers={"Authorization": f"QQBot {token}"},
            json={"code": 0},
            timeout=15,
        ) as resp:
            if resp.status >= 400:
                raise RuntimeError(f"QQ interaction ack failed: {resp.status} {await resp.text()}")

    async def _resolve_media_source(self, source: str, *, default_name: str) -> tuple[bytes, str]:
        candidate = _normalize_media_source(source)
        if not candidate:
            raise ValueError("Media source is empty")
        if is_url(candidate):
            async with self._session.get(candidate, timeout=60) as resp:
                if resp.status >= 400:
                    raise RuntimeError(f"download failed: {resp.status}")
                data = await resp.read()
            remote_name = Path(candidate.split("?", 1)[0]).name
            return data, remote_name or default_name
        local_path = resolve_ha_local_path(self._hass, candidate)
        if local_path is not None:
            data = await self._hass.async_add_executor_job(local_path.read_bytes)
            return data, local_path.name or default_name
        raise ValueError(f"Media source not found: {candidate}")

    async def _typing_keepalive(self, user_openid: str, message_id: str) -> None:
        with contextlib.suppress(Exception):
            await self._send_typing_notify(user_openid, message_id)
        while True:
            await asyncio.sleep(_TYPING_INTERVAL_SECONDS)
            with contextlib.suppress(Exception):
                await self._send_typing_notify(user_openid, message_id)

    async def _send_typing_notify(self, user_openid: str, message_id: str) -> None:
        token = await self._get_token()
        body = {
            "msg_type": 6,
            "input_notify": {
                "input_type": 1,
                "input_second": _TYPING_INPUT_SECOND,
            },
            "msg_id": message_id,
            "msg_seq": self._next_msg_seq(message_id, auxiliary=True),
        }
        async with self._session.post(
            f"{_API_BASE}/v2/users/{user_openid}/messages",
            headers={"Authorization": f"QQBot {token}"},
            json=body,
            timeout=15,
        ) as resp:
            if resp.status >= 400:
                raise RuntimeError(f"QQ typing notify failed: {resp.status} {await resp.text()}")

    def _next_msg_seq(self, message_id: str, *, auxiliary: bool = False) -> int:
        """Allocate the next passive reply sequence for a message_id.

        QQ allows at most ~5 passive replies per inbound message_id. Typing
        indicators and live-progress updates are auxiliary: they are capped
        lower so the real answer always has budget left. Raises
        _QQPassiveReplyLimitError once the budget is exhausted.
        """
        seq = self._reply_sequences.get(message_id, 0) + 1
        limit = _QQ_PASSIVE_AUX_REPLY_LIMIT if auxiliary else _QQ_PASSIVE_REPLY_LIMIT
        if seq > limit:
            raise _QQPassiveReplyLimitError(f"passive reply budget exhausted for msg_id {message_id}")
        self._reply_sequences[message_id] = seq
        if len(self._reply_sequences) > 200:
            oldest = next(iter(self._reply_sequences))
            self._reply_sequences.pop(oldest, None)
        return seq

    async def _identify(self) -> None:
        token = await self._get_token()
        if self._ws and not self._ws.closed:
            await self._ws.send_json(
                {"op": 2, "d": {"token": f"QQBot {token}", "intents": _INTENTS, "shard": [0, 1]}}
            )

    async def _get_gateway(self, token: str) -> str:
        async with self._session.get(
            f"{_API_BASE}/gateway",
            headers={"Authorization": f"QQBot {token}"},
            timeout=15,
        ) as resp:
            data = await resp.json(content_type=None)
            if resp.status >= 400:
                raise RuntimeError(f"QQ gateway fetch failed: {resp.status} {data}")
        url = str(data.get("url") or "")
        if not url:
            raise RuntimeError("QQ gateway url missing")
        return url

    async def _get_token(self) -> str:
        now = asyncio.get_running_loop().time()
        if self._token and now < self._token_expire - 300:
            return self._token
        async with self._session.post(
            _TOKEN_URL,
            json={"appId": self._app_id, "clientSecret": self._client_secret},
            timeout=15,
        ) as resp:
            data = await resp.json(content_type=None)
            if resp.status >= 400:
                raise RuntimeError(f"QQ token fetch failed: {resp.status} {data}")
        token = str(data.get("access_token") or "")
        if not token:
            raise RuntimeError("QQ access_token missing")
        self._token = token
        self._token_expire = now + int(data.get("expires_in") or 7200)
        return token


async def async_validate_config(_: HomeAssistant, config: dict[str, Any]) -> None:
    app_id = str(config.get(CONF_QQ_APP_ID, "")).strip()
    client_secret = str(config.get(CONF_QQ_CLIENT_SECRET, "")).strip()
    if not app_id or not client_secret:
        raise ValueError("qq_app_id and qq_client_secret are required")


async def async_setup_provider(
    hass: HomeAssistant,
    config: dict[str, Any],
    *,
    agent_id: str,
    subentry_id: str,
) -> ProviderRuntime:
    app_id = str(config.get(CONF_QQ_APP_ID, "")).strip()
    client_secret = str(config.get(CONF_QQ_CLIENT_SECRET, "")).strip()
    client = QQClient(
        hass,
        app_id,
        client_secret,
        agent_id,
        subentry_id=subentry_id,
        show_live_progress=bool(config.get(_CONF_QQ_SHOW_LIVE_PROGRESS, False)),
    )
    tracker = await async_get_tracker(hass, subentry_id)
    client._tracker = tracker
    await client.start()

    async def _send(target: str, message: str, target_type: str) -> None:
        await client.send_text(target, message, target_type)

    async def _send_image(target: str, image_bytes: bytes, target_type: str) -> None:
        await client.send_image(target, image_bytes, target_type)

    async def _send_media(
        target: str,
        media_bytes: bytes,
        media_kind: str,
        target_type: str,
        file_name: str | None,
    ) -> None:
        await client.send_media(target, media_bytes, media_kind, target_type, file_name)

    async def _send_tts(target: str, text: str, target_type: str) -> None:
        await client.send_tts(target, text, target_type)

    async def _send_approval(target: str, message: str, target_type: str, approval_id: str) -> None:
        await client.send_approval(target, message, target_type, approval_id)

    return ProviderRuntime(
        key=PROVIDER_QQ,
        title=PROVIDER_QQ,
        subentry_id=subentry_id,
        client=client,
        stop=client.stop,
        send_text=_send,
        status=lambda: client.status,
        known_targets=tracker.snapshot,
        selected_target=tracker.selected_target,
        select_target=tracker.async_select_target,
        send_image=_send_image,
        send_media=_send_media,
        send_tts=_send_tts,
        send_approval=_send_approval,
    )


def _build_schema(current: dict[str, Any]) -> vol.Schema:
    return vol.Schema(
        {
            vol.Required(CONF_QQ_APP_ID, default=current.get(CONF_QQ_APP_ID, "")): str,
            vol.Required(CONF_QQ_CLIENT_SECRET, default=current.get(CONF_QQ_CLIENT_SECRET, "")): str,
            vol.Optional(_CONF_QQ_SHOW_LIVE_PROGRESS, default=current.get(_CONF_QQ_SHOW_LIVE_PROGRESS, False)): bool,
        }
    )


PROVIDER_SPEC = ProviderSpec(
    key=PROVIDER_QQ,
    schema_builder=_build_schema,
    validate_config=async_validate_config,
    setup_provider=async_setup_provider,
    allow_multiple=True,
)
