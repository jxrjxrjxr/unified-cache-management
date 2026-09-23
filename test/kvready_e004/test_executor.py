"""Storage completion and ownership contracts, independent of NPU imports."""

import importlib.util
from pathlib import Path
import sys
import threading
import time
import unittest


MODULE = Path(__file__).resolve().parents[2] / "ucm/kvready/executor.py"
spec = importlib.util.spec_from_file_location("kvready_test_executor", MODULE)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
StorageExecutor = module.StorageExecutor


def eventually(predicate):
    deadline = time.monotonic() + 2
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError("background storage operation made no progress")
        time.sleep(0.001)


class Control:
    def __init__(self):
        self.denied = set()
        self.grants, self.releases, self.saved = {}, [], []

    def call(self, operation, **data):
        if operation == "acquire":
            if data["request_id"] in self.denied:
                return {"granted": False}
            self.grants[data["task_id"]] = data
            return {"granted": True}
        if operation == "release":
            self.releases.append(data)
        elif operation == "saved":
            self.saved.append(data)
        return {"ok": True}


class Store:
    def __init__(self):
        self.tasks, self.wait_calls = [], []
        self.check_error = False
        self.lock = threading.Lock()

    def load_data(self, keys, layers, addresses):
        return self.dump_data(keys, layers, addresses, 0)

    def dump_data(self, keys, layers, addresses, event):
        with self.lock:
            task = dict(keys=keys, layers=layers, addresses=addresses, event=event,
                        done=False, error=None)
            self.tasks.append(task)
        return task

    def check(self, task):
        if self.check_error:
            raise RuntimeError("completion observation temporarily failed")
        return task["done"]

    def wait(self, task):
        if not task["done"]:
            raise AssertionError("wait consumed a task before completion")
        if any(previous is task for previous in self.wait_calls):
            raise AssertionError("task wait was called twice")
        self.wait_calls.append(task)
        if task["error"]:
            raise task["error"]


class ExecutorTest(unittest.TestCase):
    def test_chunks_publish_only_after_real_completion(self):
        store, control = Store(), Control()
        control.denied.add("r")
        executor = StorageExecutor(store, control, rank=1, max_task_bytes=8)
        source_addresses = [[10, 11], [20, 21], [30, 31]]
        callbacks = []
        future = executor.submit("r", "p_save", 3, 7, [b"a", b"b", b"c"],
                                 source_addresses, 3, event=42,
                                 on_complete=lambda f: callbacks.append(f))
        source_addresses[0][0] = 999
        control.denied.clear()
        eventually(lambda: len(store.tasks) == 2)
        self.assertEqual(store.tasks[0]["addresses"][0], (10, 11))
        self.assertEqual(store.tasks[0]["event"], 42)
        self.assertFalse(store.wait_calls)
        self.assertFalse(control.saved)
        self.assertFalse(future.cancel())
        store.tasks[1]["done"] = True
        eventually(lambda: len(control.saved) == 1)
        self.assertEqual(control.saved[0]["block_start"], 9)
        self.assertFalse(future.done())
        store.tasks[0]["done"] = True
        result = executor.wait([future], timeout=2)[0]
        executor.close(timeout=2)
        self.assertEqual(len(callbacks), 1)
        self.assertEqual(len(store.wait_calls), 2)
        self.assertEqual(sum(r["nbytes"] for r in result), 9)
        self.assertEqual({(m["block_start"], m["block_end"])
                          for m in control.saved}, {(7, 9), (9, 10)})
        self.assertTrue(executor.quiescent)

    def test_denied_ticket_does_not_block_another_ready_request(self):
        store, control = Store(), Control()
        control.denied.add("later")
        executor = StorageExecutor(store, control, rank=0)
        later = executor.submit("later", "d_prefix", 0, 0, [b"a"], [[1]], 4)
        current = executor.submit("current", "p_load", 0, 0, [b"b"], [[2]], 4)
        eventually(lambda: len(store.tasks) == 1)
        self.assertEqual(store.tasks[0]["keys"], [b"b"])
        store.tasks[0]["done"] = True
        executor.wait([current], timeout=2)
        self.assertFalse(later.done())
        executor.cancel_request("later")
        executor.close(timeout=2)
        self.assertIsInstance(later.exception(), RuntimeError)

    def test_failure_drains_other_submitted_work_before_wait_raises(self):
        store, control = Store(), Control()
        executor = StorageExecutor(store, control, rank=0, max_task_bytes=4)
        future = executor.submit("r", "p_save", 1, 0, [b"a", b"b", b"c"],
                                 [[1], [2], [3]], 4)
        eventually(lambda: len(store.tasks) == 2)
        store.tasks[0]["error"] = RuntimeError("transfer failed")
        store.tasks[0]["done"] = True
        eventually(lambda: len(store.wait_calls) == 1)
        self.assertFalse(executor.quiescent)
        self.assertFalse(future.done())
        self.assertFalse(control.saved)
        self.assertEqual(len(store.tasks), 2)
        store.tasks[1]["done"] = True
        with self.assertRaisesRegex(RuntimeError, "transfer failed"):
            executor.wait([future], timeout=2)
        self.assertTrue(executor.quiescent)
        executor.close(timeout=2)

    def test_observation_failure_and_timeout_keep_inflight_owned(self):
        store, control = Store(), Control()
        executor = StorageExecutor(store, control, rank=0)
        future = executor.submit("r", "d_new", 0, 0, [b"a"], [[1]], 4)
        eventually(lambda: len(store.tasks) == 1)
        store.check_error = True
        with self.assertRaises(TimeoutError):
            executor.wait([future], timeout=0.02)
        with self.assertRaises(TimeoutError):
            executor.drain(timeout=0.01)
        self.assertFalse(executor.quiescent)
        self.assertFalse(store.wait_calls)
        self.assertFalse(control.releases)
        store.check_error = False
        store.tasks[0]["done"] = True
        executor.close(timeout=2)
        self.assertTrue(executor.quiescent)
        self.assertEqual(len(store.wait_calls), 1)
        self.assertIsInstance(future.exception(), Exception)


if __name__ == "__main__":
    unittest.main()
