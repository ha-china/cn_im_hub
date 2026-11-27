"""Dependency-free regression for Weixin dispatch methods.

Extracts the actual class methods from the source so the async scheduler can
be exercised without HA or live WeChat credentials. Run from repository root:
python -m unittest discover -s tests -p test_wechat_dispatch.py
"""
import ast
import asyncio
import copy
import json
import logging
from pathlib import Path
import time
import tempfile
import unittest
from types import SimpleNamespace
from typing import Any

SOURCE = Path(__file__).resolve().parents[1] / "custom_components/cn_im_hub/providers/wechat/client.py"
tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
original = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "WeixinClient")
names = {"start", "stop", "_message_key", "_prune_seen", "_remember_context", "_enqueue_batch",
         "_verify_disk", "_persist_locked", "_async_save_state", "_accept_batch",
         "_mark_running", "_finish_entry", "_run_normal", "_fast_done", "_dispatch_pending"}
methods = [node for node in original.body if isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef) and node.name in names]
cls = ast.ClassDef(name="Dispatch", bases=[], keywords=[], body=methods, decorator_list=[])
module = ast.fix_missing_locations(ast.Module(body=[cls], type_ignores=[]))
ns = dict(asyncio=asyncio, copy=copy, json=json, time=time, Any=Any, _LOGGER=logging.getLogger(__name__),
          _DEDUP_TTL=86400, _MAX_SEEN=2048, _MAX_NORMAL_TASKS=2, _MAX_FAST_TASKS=2,
          _MESSAGE_TIMEOUT=180)
exec(compile(module, str(SOURCE), "exec"), ns)
Dispatch = ns["Dispatch"]


class Store:
    def __init__(self, path):
        self.path = path
        self.data = {}
        self.writes = []
        self.fail = False
        self.entered = asyncio.Event()
        self.release = None

    async def async_load(self):
        return copy.deepcopy(self.data)

    async def async_save(self, data):
        self.entered.set()
        if self.release is not None:
            await self.release.wait()
        await asyncio.sleep(0)
        if self.fail:
            # Real HA Store logs some disk failures without raising.
            return
        self.data = copy.deepcopy(data)
        self.writes.append(self.data)
        Path(self.path).write_text(json.dumps({"data": data}), encoding="utf-8")


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
        d._fast_tasks = set()
        d._stopping = False
        d._task = None
        d._dispatch_task = None
        d._account_id = "test"
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        d._store = Store(str(Path(self.tmp.name) / "state.json"))
        d._hass = SimpleNamespace(async_add_executor_job=asyncio.to_thread)
        async def noop():
            return None
        d._notify_start = d._notify_stop = noop
        d._run = noop
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
        self.task = d._dispatch_task = asyncio.create_task(d._dispatch_pending())

    async def asyncTearDown(self):
        if self.d._store.release is not None:
            self.d._store.release.set()
        await self.d.stop()

    async def enqueue(self, *messages):
        await self.d._accept_batch("cursor", list(messages))

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
        await self.until(lambda: len(self.replies) == 2 and not self.d._pending)
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

    async def test_silent_write_failure_rolls_back_cursor_and_dedup(self):
        await self.d._async_save_state()
        self.d._store.fail = True
        with self.assertRaises(OSError):
            await self.d._accept_batch("new_cursor", [msg(1, "normal")])
        self.assertEqual(self.d._sync_buf, "cursor")
        self.assertEqual(self.d._pending, [])
        self.assertEqual(self.d._seen, {})
        self.assertEqual(self.d._context_tokens, {})
        self.d._store.fail = False
        await self.d._accept_batch("new_cursor", [msg(1, "normal")])
        await self.until(lambda: self.replies == ["normal"])
        self.assertEqual(self.d._sync_buf, "new_cursor")

    async def test_unverified_running_state_cannot_execute(self):
        d = self.d
        d._enqueue_batch([msg(1, "normal")])
        await d._async_save_state()
        d._store.fail = True
        with self.assertRaises(OSError):
            await d._mark_running(d._pending[0])
        self.assertEqual(d._pending[0]["state"], "queued")
        self.assertEqual(d._store.data["pending"][0]["state"], "queued")
        self.assertEqual(self.started, [])

    async def test_cancel_during_commit_joins_write_and_restores_queued(self):
        d = self.d
        d._enqueue_batch([msg(1, "normal")])
        await d._async_save_state()
        d._store.entered.clear()
        d._store.release = asyncio.Event()
        task = asyncio.create_task(d._mark_running(d._pending[0]))
        await d._store.entered.wait()
        task.cancel()
        await asyncio.sleep(0)
        self.assertFalse(task.done())
        d._store.release.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(d._pending[0]["state"], "queued")
        self.assertEqual(d._store.data["pending"][0]["state"], "queued")
        self.assertEqual(self.started, [])

    async def test_stop_joins_worker_finishing_persistence(self):
        d = self.d
        await self.enqueue(msg(1, "slow"))
        await self.until(lambda: self.started == ["slow"])
        d._store.entered.clear()
        d._store.release = asyncio.Event()
        self.release.set()
        await d._store.entered.wait()
        self.assertIn("a", d._normal_tasks)
        stop = asyncio.create_task(d.stop())
        await asyncio.sleep(0)
        self.assertFalse(stop.done())
        d._store.release.set()
        await stop
        self.assertFalse(d._normal_tasks)
        self.assertFalse(d._fast_tasks)
        self.assertIsNone(d._dispatch_task)
        self.assertEqual(d._store.data["pending"], [])

    async def test_slow_fast_send_does_not_block_second_fast_worker(self):
        d = self.d
        gate = asyncio.Event()
        async def fast(sender, entry, query):
            if query == "fast2":
                await gate.wait()
            self.replies.append(query)
            await d._finish_entry(entry)
        d._run_fast = fast
        await self.enqueue(msg(1, "slow"), msg(2, "fast2"), msg(3, "fast3"))
        try:
            await self.until(lambda: "fast3" in self.replies)
            self.assertNotIn("fast2", self.replies)
            self.assertLessEqual(len(d._fast_tasks), 2)
        finally:
            gate.set()
            self.release.set()
        await self.until(lambda: len(self.replies) == 3)

    async def test_restart_drops_running_recovers_queued_and_keeps_latest_token(self):
        d = self.d
        await d.stop()
        d._store.data = {
            "get_updates_buf": "saved",
            "context_tokens": {"a": "latest"},
            "seen": {"a:message_id:1": time.time(), "a:message_id:2": time.time()},
            "pending": [
                {"state": "running", "msg": msg(1, "must_not_replay")},
                {"state": "queued", "msg": msg(2, "recover")},
            ],
        }
        await d.start()
        await self.until(lambda: self.replies == ["recover"] and not d._pending)
        self.assertEqual(d._context_tokens["a"], "latest")
        self.assertEqual(d._sync_buf, "saved")
        await d._accept_batch("saved", [msg(1, "must_not_replay"), msg(2, "recover")])
        self.assertEqual(d._pending, [])

    async def test_outer_deadline_cancels_normal_without_replay(self):
        d = self.d
        old_timeout = ns["_MESSAGE_TIMEOUT"]
        ns["_MESSAGE_TIMEOUT"] = 0.02
        try:
            await self.enqueue(msg(1, "slow"), msg(2, "normal2"))
            await self.until(lambda: self.replies == ["normal2"] and not d._pending)
            self.assertEqual(self.started, ["slow", "normal2"])
            self.assertNotIn("slow", self.replies)
        finally:
            ns["_MESSAGE_TIMEOUT"] = old_timeout


if __name__ == "__main__":
    unittest.main()
