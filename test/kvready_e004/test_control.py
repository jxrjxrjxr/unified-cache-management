import asyncio
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch
import httpx
import fastapi

# Isolate the Python experiment from UCM's unbuilt native logging extension.
package = types.ModuleType("ucm")
package.__path__ = [str(Path(__file__).resolve().parents[2] / "ucm")]
with patch.dict(sys.modules, {"ucm": package}):
    from ucm.kvready.coordinator import Coordinator
    from ucm.kvready.policy import restore_plan
    from ucm.pd.kvready_proxy import create_app


class ControlTest(unittest.TestCase):
    def broker(self, mode="eager", **kw):
        broker = Coordinator(tp_size=1, num_layers=2, mode=mode, **kw)
        broker.call("register", request_id="a", namespace="test")
        broker.call("allocated", request_id="a", rank=0)
        return broker

    def task(self, broker, task_id="t", **kw):
        values = dict(task_id=task_id, request_id="a", kind="p_save", rank=0,
                      layer=0, block_start=1, block_end=2, nbytes=1024)
        values.update(kw)
        return broker.call("acquire", **values)

    def test_budget_completion_and_manifest(self):
        b = self.broker(max_tasks=1)
        self.assertTrue(self.task(b)["granted"])
        self.assertFalse(self.task(b, "other", layer=1)["granted"])
        with self.assertRaises(ValueError):
            b.call("saved", request_id="a", rank=0, layer=0, block_start=1, block_end=2)
        b.call("release", task_id="t", success=True)
        b.call("release", task_id="t", success=True)
        b.call("saved", request_id="a", rank=0, layer=0, block_start=1, block_end=2)
        self.assertTrue(self.task(b, "other", layer=1)["granted"])
        self.assertEqual(b.inflight_bytes, 1024)
        self.assertFalse(b.pending_tasks)
        self.assertEqual(len(b.request_tasks["a"]), 2)
        self.assertEqual(b.call("state", request_id="a")["completed_bytes"], {"p_save":1024})

    def test_failure_keeps_inflight_credits(self):
        b = self.broker(max_tasks=1)
        self.task(b)
        self.task(b, "pending", layer=1)
        b.call("fail", request_id="a", error="stop")
        self.assertFalse(b.pending_tasks)
        self.assertEqual(b.inflight_bytes, 1024)
        self.assertTrue(self.task(b, "pending", layer=1)["failed"])
        b.call("release", task_id="t", success=True)
        self.assertEqual(b.inflight_bytes, 0)
        with self.assertRaises(ValueError):
            b.call("register", request_id="b", namespace="test")

    def test_cancel_pending_ticket_removes_only_its_reservation(self):
        b = self.broker(max_tasks=1)
        self.task(b)
        self.task(b, "pending", layer=1)
        b.call("release", task_id="pending", success=False, error="cancelled")
        self.assertFalse(b.pending_tasks)
        self.assertEqual(b.inflight_tasks, 1)
        b.call("release", task_id="t", success=True)
        self.assertEqual(b.inflight_tasks, 0)

    def test_late_waits_for_actual_producer(self):
        b = self.broker(mode="late")
        self.assertFalse(self.task(b, kind="d_prefix")["granted"])
        b.call("producer_done", request_id="a", rank=0)
        self.assertTrue(self.task(b, kind="d_prefix")["granted"])

    def test_identity_rejects_mismatch(self):
        b = self.broker()
        b.call("bind", request_id="a", role="d", rank=-1, input_hash="hash", layout={"size":1})
        with self.assertRaises(ValueError):
            b.call("bind", request_id="a", role="p", rank=-1, input_hash="other", layout={"size":1})
        self.assertTrue(b.call("state", request_id="a")["failed"])

    def test_calibrated_joint_and_baseline(self):
        c = dict(storage_bytes_per_ms=10, prefill_tokens_per_ms=10, margin_ms=1)
        kwargs = dict(load_bytes=1000, recompute_tokens=10, new_tokens=10, d_remaining_bytes=1000)
        self.assertEqual(restore_plan("joint", c, **kwargs)["action"], "RECOMPUTE")
        self.assertEqual(restore_plan("progress", c, **kwargs)["action"], "LOAD")
        self.assertEqual(restore_plan("joint", {}, **kwargs)["reason"], "missing_calibration")

    def test_progress_reorders_pending_prefix_with_the_same_budget(self):
        selected = {}
        for mode in ("eager", "progress"):
            with self.subTest(mode=mode):
                b = self.broker(mode=mode, max_tasks=1, max_bytes=1024,
                                clock=lambda: 1.0, calibration=dict(
                                    storage_bytes_per_ms=10, prefill_tokens_per_ms=10, margin_ms=1))
                self.assertTrue(self.task(b, "occupy")["granted"])
                for rid, remaining in (("early", 100), ("urgent", 10)):
                    b.call("register", request_id=rid, namespace="test")
                    b.call("d_plan", request_id=rid, prefix_blocks=1,
                           total_blocks=3, local_blocks=0, block_bytes=100)
                    b.call("p_plan", request_id=rid, p_remaining_ms=remaining)
                    s = b.call("state", request_id=rid)
                    # Two new blocks each need a save and a read: 400 B / 10 B/ms.
                    self.assertEqual(s["producer_remaining_io_ms"], 40)
                    self.assertEqual(s["producer_deadline_ms"],
                                     s["producer_compute_deadline_ms"] + 40)
                    self.assertFalse(self.task(b, rid, request_id=rid, kind="d_prefix",
                                               block_start=0, block_end=1, nbytes=50)["granted"])
                self.assertEqual(b.pending_tasks, {"early", "urgent"})
                b.call("release", task_id="occupy", success=True)
                selected[mode] = next(t["request_id"] for t in b.tasks.values()
                                      if t["status"] == "inflight")
                self.assertEqual(b.inflight_tasks, 1)
                self.assertEqual(b.inflight_bytes, 50)
        self.assertEqual(selected, {"eager":"early", "progress":"urgent"})

    def test_local_prefix_can_exceed_external_prefix(self):
        b = self.broker()
        b.call("d_plan", request_id="a", prefix_blocks=1, total_blocks=4,
               local_blocks=3, block_bytes=100)
        self.assertEqual(b.call("state", request_id="a")["prefix_remaining_bytes"], 0)

    def test_calibration_freezes_before_formal_comparison(self):
        b = Coordinator(tp_size=1, num_layers=2)
        values = dict(storage_bytes_per_ms=10, prefill_tokens_per_ms=10, margin_ms=1)
        b.call("set_calibration", calibration=values)
        b.call("register", request_id="a", namespace="test")
        with self.assertRaises(ValueError):
            b.call("set_calibration", calibration=dict(values, margin_ms=2))


class ProxyTest(unittest.IsolatedAsyncioTestCase):
    async def test_real_request_order_and_output(self):
        b = Coordinator(tp_size=1, num_layers=2)
        events = []
        produced = asyncio.Event()

        class DecodeStream(httpx.AsyncByteStream):
            async def __aiter__(self):
                await produced.wait()
                b.call("ready", request_id="pd", rank=0)
                yield b'data: {"choices":[{"text":"D output"}]}\n\n'
                yield b'data: {"choices":[],"usage":{"completion_tokens":128}}\n\n'
                yield b'data: [DONE]\n\n'

        async def backend(request):
            body = json.loads(request.content)
            meta = body["kv_transfer_params"]["kvready"]
            rid = meta["id"]
            if meta["role"] == "d":
                events.append("D")
                b.call("allocated", request_id=rid, rank=0)
                self.assertTrue(body["stream"])
                return httpx.Response(200, headers={"content-type":"text/event-stream"}, stream=DecodeStream())
            events.append("P")
            self.assertTrue(b.call("state", request_id=rid)["d_allocated"])
            self.assertEqual(body["max_tokens"], 1)
            self.assertNotIn("min_tokens", body)
            self.assertFalse(body["stream"])
            produced.set()
            return httpx.Response(200, json={"choices":[{"text":"P internal"}]})

        app = create_app(coordinator=b, transport=httpx.MockTransport(backend))
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://proxy") as client:
                response = await client.post("/v1/completions", json={
                    "model":"fixture", "prompt":[1,2,3], "max_tokens":128, "min_tokens":128, "stream":True,
                    "kvready":{"namespace":"e004-test", "id":"pd"}})
                self.assertEqual(response.status_code, 200, response.text)
                self.assertIn('"text":"D output"', response.text)
                self.assertIn('"completion_tokens":128', response.text)
                self.assertNotIn("P internal", response.text)
                self.assertEqual(events, ["D", "P"])
                self.assertTrue(b.call("state", request_id="pd")["producer_done"])
                self.assertEqual(app.state.slots._value, 2)
                self.assertEqual(app.state.active_requests, set())

    async def test_reference_stream_keeps_payload_and_releases_active_request(self):
        b = Coordinator(tp_size=1, num_layers=2)
        app = None
        class Stream(httpx.AsyncByteStream):
            async def __aiter__(self):
                self_test.assertIn("ref", app.state.active_requests)
                yield b'data: {"choices":[{"text":"answer"}]}\n\n'
                yield b'data: {"choices":[],"usage":{"completion_tokens":128}}\n\n'
                yield b'data: [DONE]\n\n'
        self_test = self
        async def backend(request):
            body = json.loads(request.content)
            self.assertEqual(body["kv_transfer_params"]["kvready"]["role"], "reference")
            self.assertEqual(body["max_tokens"], 128)
            self.assertEqual(body["min_tokens"], 128)
            self.assertTrue(body["stream"])
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://proxy") as probe:
                self.assertEqual((await probe.post("/e004/reset")).status_code, 409)
            return httpx.Response(200, headers={"content-type":"text/event-stream"}, stream=Stream())
        app = create_app(coordinator=b, transport=httpx.MockTransport(backend))
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://proxy") as client:
                response = await client.post("/e004/reference", json={
                    "model":"fixture", "prompt":[1,2,3], "max_tokens":128, "min_tokens":128,
                    "stream":True, "kvready":{"namespace":"test", "id":"ref"}})
                self.assertEqual(response.status_code, 200, response.text)
                self.assertIn('"completion_tokens":128', response.text)
                self.assertEqual(app.state.active_requests, set())

    async def test_p_failure_freezes_admission(self):
        b = Coordinator(tp_size=1, num_layers=2)
        async def backend(request):
            meta = json.loads(request.content)["kv_transfer_params"]["kvready"]
            if meta["role"] == "d":
                b.call("allocated", request_id=meta["id"], rank=0)
                await asyncio.sleep(5)
                return httpx.Response(200, json={})
            return httpx.Response(500, text="P failed")
        app = create_app(coordinator=b, request_timeout=1, transport=httpx.MockTransport(backend))
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://proxy") as client:
                response = await client.post("/v1/completions", json={"model":"fixture", "prompt":[1],
                                               "kvready":{"namespace":"test", "id":"failed"}})
                self.assertEqual(response.status_code, 502)
                self.assertTrue(b.call("state", request_id="failed")["failed"])
                self.assertEqual(app.state.slots._value, 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
