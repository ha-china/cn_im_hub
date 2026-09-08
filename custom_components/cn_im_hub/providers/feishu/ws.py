from __future__ import annotations

import asyncio
import json
import logging
import threading
from collections import OrderedDict
from collections.abc import Awaitable, Callable

from homeassistant.core import HomeAssistant

from .api import import_lark

_LOGGER = logging.getLogger(__name__)


class FeishuWsClient:
    def __init__(
        self,
        *,
        hass: HomeAssistant,
        app_id: str,
        app_secret: str,
        message_handler: Callable[[dict[str, str]], Awaitable[None]],
    ) -> None:
        self._hass = hass
        self._app_id = app_id
        self._app_secret = app_secret
        self._message_handler = message_handler
        self._client: object | None = None
        self._runner_task: asyncio.Task | None = None
        self._worker_thread: threading.Thread | None = None
        self._stop_flag = False
        self._seen_message_ids: OrderedDict[str, float] = OrderedDict()
        self._seen_limit = 512
        self._seen_ttl = 3600.0  # 1 hour
        self._status = "disconnected"

    @property
    def status(self) -> str:
        return self._status

    async def async_start(self) -> None:
        if self._runner_task is not None:
            return
        self._stop_flag = False
        self._runner_task = self._hass.async_create_background_task(
            self._async_run_forever(),
            "cn_im_hub_feishu_ws_runner",
        )

    async def async_stop(self) -> None:
        self._stop_flag = True
        if self._runner_task is not None:
            self._runner_task.cancel()
            try:
                await self._runner_task
            except asyncio.CancelledError:
                pass
            self._runner_task = None
        await self._hass.async_add_executor_job(self._stop_sync)
        self._status = "disconnected"

    async def _async_run_forever(self) -> None:
        self._status = "connecting"
        self._worker_thread = threading.Thread(
            target=self._run_in_thread,
            daemon=True,
            name="feishu_ws_worker",
        )
        self._worker_thread.start()

    def _run_in_thread(self) -> None:
        import time
        retry_count = 0
        max_delay = 300  # 5 minutes
        
        while not self._stop_flag:
            try:
                self._start_sync_isolated()
                retry_count = 0
            except Exception as err:
                if self._stop_flag:
                    return
                retry_count += 1
                delay = min(5 * (2 ** min(retry_count - 1, 6)), max_delay)
                _LOGGER.warning("Feishu websocket error (attempt %d, retry in %ds): %s", retry_count, delay, err)
            if self._stop_flag:
                return
            self._status = "disconnected"
            time.sleep(delay if retry_count > 0 else 5)
            self._status = "connecting"

    def _start_sync_isolated(self) -> None:
        import importlib
        import sys
        
        for mod_name in list(sys.modules.keys()):
            if mod_name.startswith("lark_oapi.ws"):
                del sys.modules[mod_name]
        
        lark, _ = import_lark()
        import lark_oapi.ws.client as lark_ws_client_mod
        
        worker_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(worker_loop)
        lark_ws_client_mod.loop = worker_loop

        event_handler = lark.EventDispatcherHandler.builder("", "").register_p2_customized_event(
            "im.message.receive_v1",
            self._on_custom_message_sync,
        ).register_p2_customized_event(
            "im.message.message_read_v1",
            self._on_ignored_event_sync,
        ).register_p2_customized_event(
            "card.action.trigger",
            self._on_card_action_sync,
        ).build()

        self._client = lark.ws.Client(
            self._app_id,
            self._app_secret,
            event_handler=event_handler,
            log_level=lark.LogLevel.INFO,
        )
        
        self._status = "connected"
        
        try:
            self._client.start()
        finally:
            try:
                worker_loop.close()
            except Exception:
                pass

    def _stop_sync(self) -> None:
        client = self._client
        self._client = None
        stop = getattr(client, "stop", None) if client is not None else None
        if callable(stop):
            stop()

    def _on_custom_message_sync(self, data: object) -> None:
        event = getattr(data, "event", None)
        if not isinstance(event, dict):
            return

        message = event.get("message") or {}
        sender = event.get("sender") or {}
        message_id = str(message.get("message_id") or "")
        if not message_id:
            return

        import time
        now = time.time()
        # Clean up expired entries
        while self._seen_message_ids:
            oldest_id, oldest_time = next(iter(self._seen_message_ids.items()))
            if now - oldest_time > self._seen_ttl:
                self._seen_message_ids.popitem(last=False)
            else:
                break

        if message_id in self._seen_message_ids:
            return

        self._seen_message_ids[message_id] = now
        self._seen_message_ids.move_to_end(message_id)
        if len(self._seen_message_ids) > self._seen_limit:
            self._seen_message_ids.popitem(last=False)

        sender_id = sender.get("sender_id") if isinstance(sender, dict) else None
        future = asyncio.run_coroutine_threadsafe(
            self._message_handler(
                {
                    "message_id": message_id,
                    "text": _extract_text(str(message.get("content") or "")),
                    "chat_id": str(message.get("chat_id") or ""),
                    "user_id": _extract_user_id(sender_id),
                }
            ),
            self._hass.loop,
        )
        future.add_done_callback(_log_future_exception)

    def _on_card_action_sync(self, data: object) -> None:
        event = getattr(data, "event", None)
        if not isinstance(event, dict):
            return
        action = event.get("action") or {}
        operator = event.get("operator") or {}
        value = action.get("value", {})
        user_text = ""
        if isinstance(value, dict):
            user_text = value.get("action", "")
        elif isinstance(value, str):
            user_text = value
        if not user_text:
            return
        operator_id = operator.get("open_id") or ""
        open_chat_id = event.get("context", {}).get("open_chat_id", "") if isinstance(event.get("context"), dict) else ""
        future = asyncio.run_coroutine_threadsafe(
            self._message_handler(
                {
                    "message_id": "",
                    "text": user_text,
                    "chat_id": open_chat_id,
                    "user_id": operator_id,
                }
            ),
            self._hass.loop,
        )
        future.add_done_callback(_log_future_exception)

    def _on_ignored_event_sync(self, _: object) -> None:
        return


def _extract_user_id(sender_id: object) -> str:
    return str(
        sender_id.get("open_id") or sender_id.get("user_id") or sender_id.get("union_id") or ""
    ) if isinstance(sender_id, dict) else ""


def _extract_text(content: str) -> str:
    try:
        payload = json.loads(content)
    except json.JSONDecodeError:
        return content.strip()
    return str(payload.get("text", "")).strip() if isinstance(payload, dict) else ""


def _log_future_exception(future: asyncio.Future) -> None:
    try:
        future.result()
    except Exception:
        _LOGGER.exception("Failed to process Feishu message")
