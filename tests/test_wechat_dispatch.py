"""Dependency-free regression for Weixin dispatch methods.

Extracts the actual class methods from the source so the async scheduler can
be exercised without HA or live WeChat credentials. Run from repository root:
python -m unittest discover -s tests -p test_wechat_dispatch.py
"""
import ast
import asyncio
import copy
import logging
from pathlib import Path
import time
import unittest
from typing import Any

SOURCE = Path(__file__).resolve().parents[1] / "custom_components/cn_im_hub/providers/wechat/client.py"
tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
original = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "WeixinClient")
names = {"_message_key", "_prune_seen", "_remember_context", "_enqueue_batch", "_async_save_state",
         "_mark_running", "_finish_entry", "_run_normal", "_dispatch_pending"}
methods = [node for node in original.body if isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef) and node.name in names]
cls = ast.ClassDef(name="Dispatch", bases=[], keywords=[], body=methods, decorator_list=[])
module = ast.fix_missing_locations(ast.Module(body=[cls], type_ignores=[]))
ns = dict(asyncio=asyncio, copy=copy, time=time, Any=Any, _LOGGER=logging.getLogger(__name__),
          _DEDUP_TTL=86400, _MAX_SEEN=2048, _MAX_NORMAL_TASKS=2)
exec(compile(module, str(SOURCE), "exec"), ns)
Dispatch = ns["Dispatch"]


class Store:
    def __init__(self):
        self.data = {}
        self.writes = []

    async def async_save(self, data):
        await asyncio.sleep(0)
        self.data = data
        self.writes.append(data)


def msg(i, text, sender="a"):
    return {"message_id": i, "from_user_id": sender, "context_token": str(i), "text": text}


class DispatchTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.d = d = Dispatch()
        d._pending = []
        d._seen = {}
        d._context_tokens = {}
        d._sync_buf = "cursor"
        d._state_lock = asyncio.Lock()
        d._dispatch_event = asyncio.Event()
        d._normal_tasks = {}
        d._stopping = False
        d._store = Store()
        self.release = asyncio.Event()
        self.replies = []
        self.started = []

        async def normal(message):
            self.started.append(message["text"])
            if message["text"] == "slow":
                await self.release.wait()
            self.replies.append(message["text"])

        async def recognize(message):
            await asyncio.sleep(0)
            return message["text"] if message["text"].startswith("fast") else None

        async def fast(sender, entry, query):
            self.replies.append(query)
            await d._finish_entry(entry)

        d._handle_message = normal
        d._try_recognize = recognize
        d._run_fast = fast
        self.task = asyncio.create_task(d._dispatch_pending())

    async def asyncTearDown(self):
        self.d._stopping = True
        self.task.cancel()
        tasks = list(self.d._normal_tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(self.task, *tasks, return_exceptions=True)

    async def enqueue(self, *messages):
        self.d._enqueue_batch(list(messages))
        await self.d._async_save_state()
        self.d._dispatch_event.set()

    async def until(self, predicate):
        async with asyncio.timeout(2):
            while not predicate():
                await asyncio.sleep(0.001)

    async def test_fast_overtakes_slow_and_normal_turns_stay_ordered(self):
        await self.enqueue(msg(1, "slow"), msg(2, "normal2"), msg(3, "fast3"), msg(4, "normal4"))
        await self.until(lambda: "fast3" in self.replies)
        self.assertEqual(self.replies, ["fast3"])
        self.assertEqual(self.started, ["slow"])
        self.release.set()
        await self.until(lambda: len(self.replies) == 4)
        self.assertEqual(self.replies, ["fast3", "slow", "normal2", "normal4"])

    async def test_other_sender_not_blocked(self):
        await self.enqueue(msg(1, "slow"), msg(2, "normalB", "b"))
        await self.until(lambda: "normalB" in self.replies)
        self.assertNotIn("slow", self.replies)
        self.release.set()

    async def test_explicit_id_dedup_and_no_text_dedup(self):
        await self.enqueue(msg(1, "normal"), msg(1, "normal"), msg(2, "normal"))
        await self.until(lambda: len(self.replies) == 2)
        await self.enqueue(msg(1, "normal"))
        self.assertEqual(len(self.d._pending), 0)
        self.assertEqual(self.replies, ["normal", "normal"])

    async def test_running_and_queued_are_persisted(self):
        await self.enqueue(msg(1, "slow"), msg(2, "normal2"))
        await self.until(lambda: self.started == ["slow"])
        self.assertEqual([e["state"] for e in self.d._store.data["pending"]], ["running", "queued"])
        self.assertEqual(self.d._store.data["get_updates_buf"], "cursor")
        self.assertEqual(self.d._store.data["context_tokens"]["a"], "2")

    async def test_queue_order_when_active_finishes_during_recognition(self):
        async def recognize(message):
            if message["text"] == "normal2":
                self.release.set()
                await self.until(lambda: "a" not in self.d._normal_tasks)
            return None
        self.d._try_recognize = recognize
        await self.enqueue(msg(1, "slow"), msg(2, "normal2"), msg(3, "normal3"))
        await self.until(lambda: len(self.replies) == 3)
        self.assertEqual(self.started, ["slow", "normal2", "normal3"])


if __name__ == "__main__":
    unittest.main()
