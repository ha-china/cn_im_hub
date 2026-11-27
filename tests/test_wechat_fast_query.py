"""Read-only recognition and no-replay tests without Home Assistant runtime."""
from __future__ import annotations

import ast
import asyncio
from dataclasses import dataclass
import inspect
import logging
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, Mock, patch

ROOT = Path(__file__).resolve().parents[1] / "custom_components/cn_im_hub/core"


def load_functions(file, names, namespace):
    source = ROOT / file
    parsed = ast.parse(source.read_text(encoding="utf-8"))
    selected = [n for n in parsed.body if getattr(n, "name", None) in names]
    node = ast.fix_missing_locations(ast.Module(body=[
        ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0),
        *selected,
    ], type_ignores=[]))
    exec(compile(node, str(source), "exec"), namespace)
    return namespace


class FastQueryTests(IsolatedAsyncioTestCase):
    def setUp(self):
        self.handler = object()
        self.agent = SimpleNamespace(
            _trigger_intents_config={"intents": {}},
            async_recognize_intent=AsyncMock(return_value=SimpleNamespace(
                intent=SimpleNamespace(name="Query"), unmatched_entities={},
                entities_list=[SimpleNamespace(name="device", value="lamp", text="灯")],
            )),
        )
        self.handlers = Mock(return_value={"Query": self.handler})
        self.execute = AsyncMock(return_value=SimpleNamespace(
            speech={"plain": {"speech": "已关闭"}}, response_type="action_done",
        ))
        self.ns = load_functions("fast_query.py", {
            "FastQuery", "recognize_fast_query", "execute_fast_query",
        }, {
            "__name__": __name__, "dataclass": dataclass, "Any": Any,
            "HomeAssistant": object, "Context": object,
            "conversation": SimpleNamespace(
                async_get_agent=Mock(return_value=self.agent),
                ConversationInput=lambda **kwargs: SimpleNamespace(**kwargs),
            ),
            "intent": SimpleNamespace(async_handle=self.execute,
                                      IntentResponseType=SimpleNamespace(ERROR="error")),
            "_speech_handlers": self.handlers, "_LOGGER": logging.getLogger(__name__),
        })
        self.hass = SimpleNamespace(config=SimpleNamespace(language="zh-CN"))

    async def test_strict_recognition_does_not_execute(self):
        query = await self.ns["recognize_fast_query"](self.hass, "灯亮着吗")
        self.assertIsNotNone(query)
        self.assertEqual(query.slots, {"device": {"value": "lamp", "text": "灯"}})
        self.assertTrue(self.agent.async_recognize_intent.call_args.kwargs["strict_intents_only"])
        self.execute.assert_not_called()
        self.assertEqual(await self.ns["execute_fast_query"](self.hass, query), "已关闭")
        self.execute.assert_awaited_once()

    async def test_trigger_config_fails_closed(self):
        self.agent._trigger_intents_config["intents"]["trigger"] = {}
        self.assertIsNone(await self.ns["recognize_fast_query"](self.hass, "灯亮着吗"))
        self.agent.async_recognize_intent.assert_not_called()
        self.execute.assert_not_called()

    async def test_handler_reload_does_not_execute(self):
        query = await self.ns["recognize_fast_query"](self.hass, "灯亮着吗")
        self.handlers.return_value = {"Query": object()}
        self.assertIn("配置已变更", await self.ns["execute_fast_query"](self.hass, query))
        self.execute.assert_not_called()

    async def test_non_speech_handler_not_recognized(self):
        self.handlers.return_value = {"Other": object()}
        self.assertIsNone(await self.ns["recognize_fast_query"](self.hass, "灯亮着吗"))
        self.execute.assert_not_called()

    async def test_unmatched_slots_not_recognized(self):
        self.agent.async_recognize_intent.return_value.unmatched_entities = {"device": "?"}
        self.assertIsNone(await self.ns["recognize_fast_query"](self.hass, "灯亮着吗"))
        self.execute.assert_not_called()


class NoReplayTests(IsolatedAsyncioTestCase):
    def setUp(self):
        self.service = AsyncMock(return_value={"response": {"speech": {"plain": {"speech": "fallback"}}}})
        self.hass = SimpleNamespace(
            config=SimpleNamespace(language="zh-CN"),
            services=SimpleNamespace(has_service=Mock(return_value=True), async_call=self.service),
        )
        self.ns = load_functions("conversation.py", {
            "extract_speech", "extract_speech_any", "ask_home_assistant",
        }, {"inspect": inspect, "Any": Any, "HomeAssistant": object, "Context": object,
            "_normalize_agent_id_for_runtime": lambda hass, agent_id: agent_id,
            "_LOGGER": logging.getLogger(__name__)})
        self.ha = ModuleType("homeassistant")
        self.components = ModuleType("homeassistant.components")
        self.conv = ModuleType("homeassistant.components.conversation")
        self.ha.components = self.components
        self.components.conversation = self.conv

    async def call(self, agent_func, retry=False):
        self.conv.async_converse = agent_func
        with patch.dict(sys.modules, {
            "homeassistant": self.ha, "homeassistant.components": self.components,
            "homeassistant.components.conversation": self.conv,
        }):
            return await self.ns["ask_home_assistant"](
                self.hass, "test", conversation_id="wechat:a:b",
                agent_id="agent", retry_after_dispatch=retry,
            )

    async def test_attempted_call_exception_is_not_replayed(self):
        async def converse(hass, text, context):
            raise RuntimeError("unknown outcome")
        self.assertIn("核实执行结果", await self.call(converse))
        self.service.assert_not_called()

    async def test_attempted_call_empty_response_is_not_replayed(self):
        async def converse(hass, text, context):
            return {}
        self.assertIn("核实执行结果", await self.call(converse))
        self.service.assert_not_called()

    async def test_other_providers_keep_fallback(self):
        async def converse(hass, text, context):
            return {}
        self.assertEqual(await self.call(converse, retry=True), "fallback")
        self.service.assert_awaited_once()
