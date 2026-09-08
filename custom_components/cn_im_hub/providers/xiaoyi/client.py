"""XiaoYi provider aligned with the npm package behavior."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import json
import logging
import ssl
import time
from base64 import b64encode
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4

import aiohttp
import voluptuous as vol
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession

EVENT_LIVE_PROGRESS = "ha_crack_live_progress"
_LIVE_PROGRESS_SEND_INTERVAL_SECONDS = 2.0

from ...core.command import execute_command, parse_command
from ...const import (
    CONF_XIAOYI_AGENT_ID,
    CONF_XIAOYI_AK,
    CONF_XIAOYI_SK,
    CONF_XIAOYI_WS_URL_1,
    CONF_XIAOYI_WS_URL_2,
    PROVIDER_XIAOYI,
    XIAOYI_DEFAULT_WS_URL_1,
    XIAOYI_DEFAULT_WS_URL_2,
)
from ...core.known_targets import async_get_tracker
from ...models import ProviderRuntime
from ..base import ProviderSpec

_LOGGER = logging.getLogger(__name__)
_SERVER_IDS = ("server1", "server2")
_STABLE_CONNECTION_THRESHOLD = 30.0
_MAX_RECONNECT_ATTEMPTS = 50
_WATCHDOG_INTERVAL = 20.0
_WATCHDOG_TIMEOUT = 90.0
_TASK_TIMEOUT_SECONDS = 360
_MAX_SEEN_TASK_IDS = 512


@dataclass(slots=True)
class _ServerState:
    connected: bool = False
    ready: bool = False
    last_heartbeat: float = 0.0
    reconnect_attempts: int = 0


class XiaoYiClient:
    """Dual-websocket XiaoYi client."""

    def __init__(
        self,
        hass: HomeAssistant,
        *,
        ak: str,
        sk: str,
        xiaoyi_agent_id: str,
        conversation_agent_id: str,
        ws_url_1: str,
        ws_url_2: str,
        show_live_progress: bool = False,
    ) -> None:
        self._hass = hass
        self._session = async_get_clientsession(hass)
        self._ak = ak
        self._sk = sk
        self._xiaoyi_agent_id = xiaoyi_agent_id
        self._conversation_agent_id = conversation_agent_id
        self._urls = {"server1": ws_url_1, "server2": ws_url_2}
        self._states = {server_id: _ServerState() for server_id in _SERVER_IDS}
        self._ws: dict[str, aiohttp.ClientWebSocketResponse | None] = {server_id: None for server_id in _SERVER_IDS}
        self._listen_tasks: dict[str, asyncio.Task[None] | None] = {server_id: None for server_id in _SERVER_IDS}
        self._reconnect_tasks: dict[str, asyncio.Task[None] | None] = {server_id: None for server_id in _SERVER_IDS}
        self._stable_tasks: dict[str, asyncio.Task[None] | None] = {server_id: None for server_id in _SERVER_IDS}
        self._app_heartbeat_task: asyncio.Task[None] | None = None
        self._watchdog_task: asyncio.Task[None] | None = None
        self._active_prompts: dict[str, asyncio.Task[str]] = {}
        self._session_servers: dict[str, str] = {}
        self._seen_task_ids: dict[str, None] = {}
        self._request_ids: dict[str, str] = {}  # task_id -> request id (message.id)
        self._stopping = False
        self._tracker = None
        self._show_live_progress = show_live_progress

    @property
    def status(self) -> str:
        if any(state.ready for state in self._states.values()):
            return "connected"
        if any(state.connected for state in self._states.values()):
            return "connecting"
        return "disconnected"

    async def start(self) -> None:
        self._stopping = False
        results = await asyncio.gather(
            self._connect_to_server("server1"),
            self._connect_to_server("server2"),
            return_exceptions=True,
        )
        if all(isinstance(result, Exception) for result in results):
            raise RuntimeError("Failed to connect to both XiaoYi servers")
        if self._app_heartbeat_task is None:
            self._app_heartbeat_task = asyncio.create_task(self._app_heartbeat_loop())
        if self._watchdog_task is None:
            self._watchdog_task = asyncio.create_task(self._watchdog_loop())

    async def stop(self) -> None:
        self._stopping = True
        for task in self._active_prompts.values():
            task.cancel()
        for task_map in (self._listen_tasks, self._reconnect_tasks, self._stable_tasks):
            for task in task_map.values():
                if task is not None:
                    task.cancel()
        if self._app_heartbeat_task is not None:
            self._app_heartbeat_task.cancel()
        if self._watchdog_task is not None:
            self._watchdog_task.cancel()

        pending = [*self._active_prompts.values()]
        for task_map in (self._listen_tasks, self._reconnect_tasks, self._stable_tasks):
            pending.extend(task for task in task_map.values() if task is not None)
        if self._app_heartbeat_task is not None:
            pending.append(self._app_heartbeat_task)
        if self._watchdog_task is not None:
            pending.append(self._watchdog_task)
        for task in pending:
            with contextlib.suppress(asyncio.CancelledError):
                await task

        for server_id in _SERVER_IDS:
            await self._close_server(server_id)
            self._states[server_id] = _ServerState()
            self._listen_tasks[server_id] = None
            self._reconnect_tasks[server_id] = None
            self._stable_tasks[server_id] = None

        self._active_prompts.clear()
        self._session_servers.clear()
        self._app_heartbeat_task = None
        self._watchdog_task = None

    async def send_text(self, target: str, message: str, target_type: str) -> None:
        if target_type != "session_id":
            raise ValueError("XiaoYi send_message only supports target_type=session_id")
        session_id = target.strip()
        if not session_id:
            raise ValueError("XiaoYi session_id is required")
        server_id = self._session_servers.get(session_id)
        if not server_id:
            raise ValueError("XiaoYi session_id is unknown or no longer active")
        task_id = str(uuid4())
        await self._send_text_chunk(task_id, session_id, message)
        await self._send_final(task_id, session_id)

    async def _connect_to_server(self, server_id: str) -> None:
        url = self._urls[server_id]
        headers = _build_auth_headers(self._ak, self._sk, self._xiaoyi_agent_id)
        ssl_context = _build_ws_ssl_context(url)
        if ssl_context is not True:
            _LOGGER.debug("XiaoYi %s uses insecure TLS fallback for IP endpoint", server_id)

        _LOGGER.info("Connecting XiaoYi %s: %s", server_id, url)
        try:
            ws = await self._session.ws_connect(
                url,
                headers=headers,
                heartbeat=30,
                autoping=True,
                receive_timeout=90,
                timeout=30,
                ssl=ssl_context,
            )
        except Exception:
            self._states[server_id].connected = False
            self._states[server_id].ready = False
            raise

        self._ws[server_id] = ws
        state = self._states[server_id]
        state.connected = True
        state.ready = True
        state.last_heartbeat = time.time()

        await ws.send_json({"msgType": "clawd_bot_init", "agentId": self._xiaoyi_agent_id})
        self._schedule_stable_connection_check(server_id)
        self._listen_tasks[server_id] = asyncio.create_task(self._listen_server(server_id, ws))
        _LOGGER.info("XiaoYi %s connected", server_id)

    async def _listen_server(self, server_id: str, ws: aiohttp.ClientWebSocketResponse) -> None:
        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    self._states[server_id].last_heartbeat = time.time()
                    await self._handle_message(server_id, json.loads(msg.data))
                elif msg.type in (aiohttp.WSMsgType.PING, aiohttp.WSMsgType.PONG):
                    self._states[server_id].last_heartbeat = time.time()
                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break
        except asyncio.CancelledError:
            raise
        except Exception as err:
            _LOGGER.warning("XiaoYi websocket loop error (%s): %s", server_id, err)
        finally:
            close_code = ws.close_code
            close_reason = getattr(ws, "close_reason", None)
            if not self._stopping:
                _LOGGER.warning(
                    "XiaoYi %s connection closed (code=%s reason=%s)",
                    server_id,
                    close_code,
                    close_reason,
                )
            await self._handle_disconnect(server_id)

    async def _handle_disconnect(self, server_id: str) -> None:
        await self._close_server(server_id)
        state = self._states[server_id]
        state.connected = False
        state.ready = False
        stable_task = self._stable_tasks[server_id]
        if stable_task is not None:
            stable_task.cancel()
            self._stable_tasks[server_id] = None
        if not self._stopping:
            self._schedule_reconnect(server_id)

    async def _close_server(self, server_id: str) -> None:
        ws = self._ws.get(server_id)
        if ws is not None and not ws.closed:
            with contextlib.suppress(Exception):
                await ws.close()
        self._ws[server_id] = None

    def _schedule_reconnect(self, server_id: str) -> None:
        current = self._reconnect_tasks.get(server_id)
        if current is not None and not current.done():
            return
        self._reconnect_tasks[server_id] = asyncio.create_task(self._reconnect_server(server_id))

    async def _reconnect_server(self, server_id: str) -> None:
        state = self._states[server_id]
        if _MAX_RECONNECT_ATTEMPTS and state.reconnect_attempts >= _MAX_RECONNECT_ATTEMPTS:
            _LOGGER.error("XiaoYi %s reached max reconnect attempts", server_id)
            return
        delay = min(2 * (2**state.reconnect_attempts), 60)
        state.reconnect_attempts += 1
        await asyncio.sleep(delay)
        if self._stopping:
            return
        try:
            await self._connect_to_server(server_id)
        except Exception as err:
            _LOGGER.warning("XiaoYi reconnect failed (%s): %s", server_id, err)
            self._schedule_reconnect(server_id)

    def _schedule_stable_connection_check(self, server_id: str) -> None:
        current = self._stable_tasks.get(server_id)
        if current is not None and not current.done():
            current.cancel()
        self._stable_tasks[server_id] = asyncio.create_task(self._stable_connection_check(server_id))

    async def _stable_connection_check(self, server_id: str) -> None:
        try:
            await asyncio.sleep(_STABLE_CONNECTION_THRESHOLD)
            if self._states[server_id].connected:
                self._states[server_id].reconnect_attempts = 0
        except asyncio.CancelledError:
            raise

    async def _app_heartbeat_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(20)
                payload = {"msgType": "heartbeat", "agentId": self._xiaoyi_agent_id}
                for server_id in _SERVER_IDS:
                    ws = self._ws.get(server_id)
                    if ws is not None and not ws.closed:
                        try:
                            await ws.send_json(payload)
                        except Exception as err:
                            _LOGGER.debug("XiaoYi heartbeat send failed (%s): %s", server_id, err)
        except asyncio.CancelledError:
            raise

    async def _watchdog_loop(self) -> None:
        if _WATCHDOG_TIMEOUT <= 0:
            return
        try:
            while True:
                await asyncio.sleep(_WATCHDOG_INTERVAL)
                now = time.time()
                for server_id in _SERVER_IDS:
                    ws = self._ws.get(server_id)
                    state = self._states[server_id]
                    if ws is None or ws.closed or not state.connected:
                        continue
                    if now - state.last_heartbeat > _WATCHDOG_TIMEOUT:
                        _LOGGER.warning("XiaoYi %s heartbeat timeout, reconnecting", server_id)
                        with contextlib.suppress(Exception):
                            await ws.close()
        except asyncio.CancelledError:
            raise

    async def _handle_message(self, server_id: str, message: dict[str, Any]) -> None:
        agent_id = str(message.get("agentId") or "")
        if agent_id and agent_id != self._xiaoyi_agent_id:
            _LOGGER.debug("Ignore XiaoYi message for agent_id=%s", agent_id)
            return

        session_id = _extract_session_id(message)
        if session_id:
            self._session_servers[session_id] = server_id

        method = str(message.get("method") or "")
        action = str(message.get("action") or "")
        if method == "clearContext" or action == "clear":
            if session_id:
                await self._send_clear_context_response(str(message.get("id") or uuid4()), session_id)
                # Clean up session state
                self._session_servers.pop(session_id, None)
                # Cancel any active prompt for this session
                for task_id, task in list(self._active_prompts.items()):
                    if self._request_ids.get(task_id) == session_id:
                        task.cancel()
            return
        if method == "tasks/cancel" or action == "tasks/cancel":
            if session_id:
                await self._handle_cancel(message, session_id)
            return
        if method != "message/stream":
            return

        task_id = str(message.get("id") or "")
        # Dedup: dual servers may deliver the same request twice
        if task_id in self._seen_task_ids:
            return
        self._seen_task_ids[task_id] = None
        if len(self._seen_task_ids) > _MAX_SEEN_TASK_IDS:
            self._seen_task_ids.pop(next(iter(self._seen_task_ids)), None)

        text = _extract_inbound_text(message)
        if not task_id or not session_id:
            return

        # Capture the request id (message.id) for responses
        request_id = str(message.get("id") or "")
        self._request_ids[task_id] = request_id

        if self._tracker is not None:
            await self._tracker.async_record(
                provider=PROVIDER_XIAOYI,
                target=session_id,
                target_type="session_id",
                display_name=session_id,
            )
        task = asyncio.create_task(self._process_prompt(task_id, session_id, text))
        self._active_prompts[task_id] = task
        task.add_done_callback(lambda _: self._active_prompts.pop(task_id, None))

    def _format_live_progress(self, payload: dict[str, Any]) -> str:
        display_text = str(payload.get("display_text") or "").strip()
        if display_text:
            cleaned = display_text.replace("┊", "").replace("*", "").strip()
            return cleaned[:200]
        tool_name = str(payload.get("tool_name") or "").strip()
        if tool_name:
            return f"🔧 {tool_name}"
        return ""

    async def _run_live_progress_bridge(self, conversation_id: str, task_id: str, session_id: str) -> None:
        if not self._show_live_progress:
            await asyncio.Future()

        queue: asyncio.Queue[str] = asyncio.Queue()

        @callback
        def _listener(event) -> None:
            payload = event.data or {}
            if payload.get("conversation_id") != conversation_id:
                return
            text = self._format_live_progress(payload)
            if text:
                queue.put_nowait(text)

        unsub = self._hass.bus.async_listen(EVENT_LIVE_PROGRESS, _listener)
        last_sent = ""
        pending_tasks: list[asyncio.Task] = []
        
        async def _fire_and_forget(msg: str) -> None:
            with contextlib.suppress(Exception):
                await self._send_text_chunk(task_id, session_id, msg)
        
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

    async def _process_prompt(self, task_id: str, session_id: str, text: str) -> str:
        if not text:
            await self._send_final(task_id, session_id)
            return ""

        tagged_text = f"[IM:XiaoYi user={session_id}] {text}"
        try:
            command = parse_command(tagged_text)
            reply = ""
            if command is not None:
                conversation_id = f"xiaoyi:{session_id}"
                progress_task = asyncio.create_task(self._run_live_progress_bridge(conversation_id, task_id, session_id))
                try:
                    reply = await asyncio.wait_for(
                        execute_command(
                            self._hass,
                            command,
                            conversation_id=conversation_id,
                            agent_id=self._conversation_agent_id or None,
                        ),
                        timeout=_TASK_TIMEOUT_SECONDS,
                    )
                except asyncio.TimeoutError:
                    _LOGGER.warning("XiaoYi command timed out after %ds", _TASK_TIMEOUT_SECONDS)
                    reply = "Task timed out. Please simplify the command and try again."
                finally:
                    progress_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await progress_task
            if reply:
                await self._send_text_chunk(task_id, session_id, reply)
            else:
                # Empty reply: send a fallback so the user knows the task finished
                await self._send_text_chunk(task_id, session_id, "Task finished without any output. Please try again.")
            await self._send_final(task_id, session_id)
            return reply
        except asyncio.CancelledError:
            await self._send_cancelled(task_id, session_id)
            raise
        except Exception as err:
            _LOGGER.exception("XiaoYi prompt handling failed: %s", err)
            await self._send_error(task_id, session_id, err)
            return ""

    async def _handle_cancel(self, message: dict[str, Any], session_id: str) -> None:
        cancel_task_id = str(message.get("taskId") or message.get("id") or "")
        if cancel_task_id:
            task = self._active_prompts.get(cancel_task_id)
            if task is not None:
                task.cancel()
        await self._send_tasks_cancel_response(str(message.get("id") or uuid4()), session_id)

    async def _send_tasks_cancel_response(self, request_id: str, session_id: str) -> None:
        await self._send_jsonrpc_result(
            request_id=request_id,
            session_id=session_id,
            task_id=request_id,
            result={"id": request_id, "status": {"state": "canceled"}},
        )

    async def _send_clear_context_response(self, request_id: str, session_id: str) -> None:
        await self._send_jsonrpc_result(
            request_id=request_id,
            session_id=session_id,
            task_id=request_id,
            result={"status": {"state": "cleared"}},
        )

    async def _send_text_chunk(self, task_id: str, session_id: str, text: str) -> None:
        request_id = self._request_ids.get(task_id) or str(uuid4())
        await self._send_jsonrpc_result(
            request_id=request_id,
            session_id=session_id,
            task_id=task_id,
            result={
                "taskId": task_id,
                "kind": "artifact-update",
                "append": True,
                "lastChunk": False,
                "final": False,
                "artifact": {
                    "artifactId": f"artifact_{uuid4().hex}",
                    "parts": [{"kind": "text", "text": text}],
                },
            },
        )

    async def _send_final(self, task_id: str, session_id: str) -> None:
        request_id = self._request_ids.get(task_id) or str(uuid4())
        await self._send_jsonrpc_result(
            request_id=request_id,
            session_id=session_id,
            task_id=task_id,
            result={
                "taskId": task_id,
                "kind": "artifact-update",
                "append": True,
                "lastChunk": True,
                "final": True,
                "artifact": {
                    "artifactId": f"artifact_{uuid4().hex}",
                    "parts": [{"kind": "text", "text": ""}],
                },
            },
        )

    async def _send_cancelled(self, task_id: str, session_id: str) -> None:
        request_id = self._request_ids.get(task_id) or str(uuid4())
        await self._send_jsonrpc_result(
            request_id=request_id,
            session_id=session_id,
            task_id=task_id,
            result={
                "taskId": task_id,
                "kind": "status-update",
                "final": True,
                "status": {
                    "message": {"role": "agent", "parts": [{"kind": "text", "text": ""}]},
                    "state": "canceled",
                },
            },
        )

    async def _send_error(self, task_id: str, session_id: str, err: Exception) -> None:
        request_id = self._request_ids.get(task_id) or str(uuid4())
        await self._send_jsonrpc_error(
            request_id=request_id,
            session_id=session_id,
            task_id=task_id,
            code="internal_error",
            message=f"{type(err).__name__}: {err}",
        )

    async def _send_jsonrpc_result(
        self, *, request_id: str, session_id: str, task_id: str, result: dict[str, Any]
    ) -> None:
        await self._send_outbound(
            session_id=session_id,
            task_id=task_id,
            detail={"jsonrpc": "2.0", "id": request_id, "result": result},
        )

    async def _send_jsonrpc_error(
        self, *, request_id: str, session_id: str, task_id: str, code: str, message: str
    ) -> None:
        await self._send_outbound(
            session_id=session_id,
            task_id=task_id,
            detail={"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}},
        )

    async def _send_outbound(self, *, session_id: str, task_id: str, detail: dict[str, Any]) -> None:
        server_id = self._session_servers.get(session_id)
        if not server_id:
            raise RuntimeError(f"XiaoYi session route missing: {session_id}")
        ws = self._ws.get(server_id)
        if ws is None or ws.closed:
            raise RuntimeError(f"XiaoYi websocket unavailable: {server_id}")
        await ws.send_json(
            {
                "msgType": "agent_response",
                "agentId": self._xiaoyi_agent_id,
                "sessionId": session_id,
                "taskId": task_id,
                "msgDetail": json.dumps(detail, ensure_ascii=True),
            }
        )


def _build_auth_headers(ak: str, sk: str, agent_id: str) -> dict[str, str]:
    timestamp = str(int(time.time() * 1000))
    digest = hmac.new(sk.encode("utf-8"), timestamp.encode("utf-8"), hashlib.sha256).digest()
    return {
        "x-access-key": ak,
        "x-sign": b64encode(digest).decode("ascii"),
        "x-ts": timestamp,
        "x-agent-id": agent_id,
    }


def _build_ws_ssl_context(url: str) -> ssl.SSLContext | bool:
    parsed = urlparse(url)
    if parsed.scheme != "wss" or not _is_ip_host(parsed.hostname or ""):
        return True
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context


def _is_ip_host(host: str) -> bool:
    if not host:
        return False
    parts = host.split(".")
    if len(parts) == 4 and all(part.isdigit() for part in parts):
        return all(0 <= int(part) <= 255 for part in parts)
    return ":" in host


def _extract_session_id(message: dict[str, Any]) -> str:
    method = str(message.get("method") or "")
    action = str(message.get("action") or "")
    if method == "message/stream":
        return str((message.get("params") or {}).get("sessionId") or message.get("sessionId") or "")
    if method in {"tasks/cancel", "clearContext"} or action in {"tasks/cancel", "clear"}:
        return str(message.get("sessionId") or "")
    return ""


def _extract_inbound_text(message: dict[str, Any]) -> str:
    parts = (((message.get("params") or {}).get("message") or {}).get("parts") or [])
    chunks: list[str] = []
    for part in parts:
        if isinstance(part, dict) and part.get("kind") == "text":
            value = str(part.get("text") or "").strip()
            if value:
                chunks.append(value)
    return "\n".join(chunks).strip()


async def async_validate_config(_: HomeAssistant, config: dict[str, Any]) -> None:
    ak = str(config.get(CONF_XIAOYI_AK, "")).strip()
    sk = str(config.get(CONF_XIAOYI_SK, "")).strip()
    agent_id = str(config.get(CONF_XIAOYI_AGENT_ID, "")).strip()
    if not ak or not sk or not agent_id:
        raise ValueError("xiaoyi_ak, xiaoyi_sk, and xiaoyi_agent_id are required")


async def async_setup_provider(
    hass: HomeAssistant,
    config: dict[str, Any],
    *,
    agent_id: str,
    subentry_id: str,
) -> ProviderRuntime:
    client = XiaoYiClient(
        hass,
        ak=str(config.get(CONF_XIAOYI_AK, "")).strip(),
        sk=str(config.get(CONF_XIAOYI_SK, "")).strip(),
        xiaoyi_agent_id=str(config.get(CONF_XIAOYI_AGENT_ID, "")).strip(),
        conversation_agent_id=agent_id,
        ws_url_1=str(config.get(CONF_XIAOYI_WS_URL_1, XIAOYI_DEFAULT_WS_URL_1)).strip() or XIAOYI_DEFAULT_WS_URL_1,
        ws_url_2=str(config.get(CONF_XIAOYI_WS_URL_2, XIAOYI_DEFAULT_WS_URL_2)).strip() or XIAOYI_DEFAULT_WS_URL_2,
        show_live_progress=bool(config.get(_CONF_XIAOYI_SHOW_LIVE_PROGRESS, False)),
    )
    tracker = await async_get_tracker(hass, subentry_id)
    client._tracker = tracker
    await client.start()

    async def _send(target: str, message: str, target_type: str) -> None:
        await client.send_text(target, message, target_type)

    return ProviderRuntime(
        key=PROVIDER_XIAOYI,
        title=PROVIDER_XIAOYI,
        subentry_id=subentry_id,
        client=client,
        stop=client.stop,
        send_text=_send,
        status=lambda: client.status,
        known_targets=tracker.snapshot,
        selected_target=tracker.selected_target,
        select_target=tracker.async_select_target,
    )


_CONF_XIAOYI_SHOW_LIVE_PROGRESS = "xiaoyi_show_live_progress"


def _build_schema(current: dict[str, Any]) -> vol.Schema:
    return vol.Schema(
        {
            vol.Required(CONF_XIAOYI_AK, default=current.get(CONF_XIAOYI_AK, "")): str,
            vol.Required(CONF_XIAOYI_SK, default=current.get(CONF_XIAOYI_SK, "")): str,
            vol.Required(CONF_XIAOYI_AGENT_ID, default=current.get(CONF_XIAOYI_AGENT_ID, "")): str,
            vol.Optional(_CONF_XIAOYI_SHOW_LIVE_PROGRESS, default=current.get(_CONF_XIAOYI_SHOW_LIVE_PROGRESS, False)): bool,
        }
    )


PROVIDER_SPEC = ProviderSpec(
    key=PROVIDER_XIAOYI,
    schema_builder=_build_schema,
    validate_config=async_validate_config,
    setup_provider=async_setup_provider,
    allow_multiple=True,
)
