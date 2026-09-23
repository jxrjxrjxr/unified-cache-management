"""Exercise the production connector contract without a device runtime."""

import importlib.util
from dataclasses import dataclass, field
from pathlib import Path
import sys
import threading
import time
import types
import unittest
from unittest.mock import patch

import numpy as np


@dataclass
class RequestMeta:
    ucm_block_ids: list = field(default_factory=list)
    hbm_hit_block_num: int = 0
    total_hit_block_num: int = 0
    num_token_ids: int = 0
    vllm_block_ids: list = field(default_factory=list)
    token_processed: int = 0


@dataclass
class Dispatch:
    load_block_ids: tuple
    dump_block_ids: tuple


@dataclass
class Metadata:
    request_meta: dict = field(default_factory=dict)


class Layerwise:
    def _get_connector_metadata(self):
        return self._connector_metadata


def load_connector():
    base = types.ModuleType("ucm.integration.vllm.ucm_connector")
    base.KVConnectorRole = types.SimpleNamespace(SCHEDULER="scheduler", WORKER="worker")
    base.RequestMeta, base.RequestDispatchMeta = RequestMeta, Dispatch
    base.UCMConnectorMetadata, base.UCMLayerWiseConnector = Metadata, Layerwise
    control = types.ModuleType("ucm.kvready.coordinator")
    control.ControlClient = object
    executor = types.ModuleType("ucm.kvready.executor")
    executor.StorageExecutor = object
    path = Path(__file__).resolve().parents[2] / "ucm/integration/vllm/kvready_connector.py"
    spec = importlib.util.spec_from_file_location("kvready_connector_contract", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    with patch.dict(sys.modules, {base.__name__: base, control.__name__: control, executor.__name__: executor}):
        spec.loader.exec_module(module)
    return module


MODULE = load_connector()


class Control:
    def __init__(self):
        self.calls = []

    def call(self, op, **data):
        self.calls.append((op, data))
        return {"action": "LOAD"}


def connector(side="d", hit=2):
    c = MODULE.KVReadyConnector.__new__(MODULE.KVReadyConnector)
    c.side, c.block_size, c.num_layers, c.tp_size = side, 4, 2, 2
    c.num_head, c.head_size, c.element_size = 1, 2, 2
    c.signature, c._contexts, c._matches, c._receives = {}, {}, {}, {}
    c.requests_meta, c.control = {}, Control()
    c._allocated = set()
    c._lock, c._d_work, c._connector_metadata = threading.RLock(), {}, None
    c.store = types.SimpleNamespace(lookup_on_prefix=lambda keys: hit - 1)
    c.request_hasher = lambda data: str(data).encode()[:16].ljust(16, b"x")
    c.generate_hash = lambda b, tokens, seed: [bytes([i]) for i in range(len(tokens) // b)]
    return c


def request(role="d", n=17):
    return types.SimpleNamespace(
        request_id="engine-id", kv_transfer_params={"kvready": {"id": "logical-id", "namespace": "run-a", "role": role}},
        mm_features=[], lora_request=None, prompt_token_ids=list(range(n)), num_prompt_tokens=n,
    )


class ConnectorTest(unittest.TestCase):
    def test_async_promise_survives_allocation_retry_and_preserves_tail(self):
        c, req = connector(), request()
        self.assertEqual(c.get_num_new_matched_tokens(req, 4), (12, True))
        self.assertEqual(c.get_num_new_matched_tokens(req, 4), (12, True))
        self.assertEqual(len([x for x in c.control.calls if x[0] == "d_plan"]), 1)
        c.update_state_after_alloc(req, types.SimpleNamespace(get_block_ids=lambda: ([5, 6, 7, 8, 9],)), 12)
        output = types.SimpleNamespace(finished_req_ids=set())
        meta = c.build_connector_meta(output)
        self.assertFalse(meta.request_meta)  # No scheduled model request is needed.
        self.assertEqual(meta.receives["engine-id"]["blocks"], [5, 6, 7, 8])
        self.assertFalse(c.build_connector_meta(output).receives)
        self.assertEqual(c.get_num_new_matched_tokens(req, 16), (0, False))
        c.update_state_after_alloc(req, types.SimpleNamespace(get_block_ids=lambda: ([5, 6, 7, 8, 9],)), 0)
        self.assertFalse(c.build_connector_meta(output).receives)

    def test_complete_hit_is_clipped_to_last_complete_safe_block(self):
        c, req = connector(hit=4), request(n=16)
        self.assertEqual(c.get_num_new_matched_tokens(req, 0), (12, True))
        self.assertEqual(c._contexts[req.request_id]["prefix"], 3)

    def test_chunked_recompute_saves_only_missing_objects_with_absolute_ranges(self):
        c = connector(side="p")
        meta = MODULE.ReadyRequestMeta(
            ucm_block_ids=[b"a", b"b", b"c", b"d"], hbm_hit_block_num=0,
            total_hit_block_num=0, num_token_ids=17, original_hit=2,
            dump_end=2, target=4,
        )
        first = c._generate_dispatch_meta(meta, 4, [10], True)
        second = c._generate_dispatch_meta(meta, 13, [11, 12, 13, 14], False)
        self.assertEqual(first.dump_block_ids, ([], []))
        self.assertEqual(second.dump_block_ids, ([b"c", b"d"], [12, 13]))
        self.assertEqual(second.dump_start, 2)
        self.assertEqual(meta.token_processed, 17)

    def test_prepare_writes_local_cached_prefix_into_new_namespace(self):
        c = connector(side="p", hit=0)
        req = request(role="prepare", n=16)
        self.assertEqual(c.get_num_new_matched_tokens(req, 4), (0, False))
        meta = c.requests_meta[req.request_id]
        dispatch = c._generate_dispatch_meta(meta, 12, [1, 2, 3, 4], True)
        self.assertEqual(dispatch.dump_start, 0)
        self.assertEqual(dispatch.dump_block_ids[1], [1, 2, 3, 4])

    def test_timeout_retains_event_until_executor_is_quiescent(self):
        c = connector(side="p")
        c.timeout, c._p_saves = 1, [object()]
        destroyed = []
        c.device = types.SimpleNamespace(destroy_event_handles=lambda: destroyed.append(True))
        def timeout(*args, **kwargs):
            raise TimeoutError("in flight")
        release = threading.Event()
        draining = threading.Event()
        def drain():
            draining.set()
            release.wait(2)
            c.executor.quiescent = True
        c.executor = types.SimpleNamespace(wait=timeout, quiescent=False, stop=lambda error: None, drain=drain)
        errors = []
        def run():
            try:
                c.wait_for_save()
            except TimeoutError:
                errors.append(True)
        worker = threading.Thread(target=run)
        worker.start()
        self.assertTrue(draining.wait(1))
        self.assertFalse(destroyed)
        self.assertFalse(errors)
        release.set()
        worker.join(2)
        self.assertEqual(destroyed, [True])
        self.assertEqual(errors, [True])

    def test_rank_key_adaptation_does_not_mutate_scheduler_metadata(self):
        c = connector()
        keys = [b"original"]
        c.tp_rank = 1
        c.request_hasher = lambda value: value + b"rank1"
        self.assertEqual(c._rank_keys(keys), [b"originalrank1"])
        self.assertEqual(keys, [b"original"])

    def test_two_rank_receive_waits_for_real_saves_and_last_physical_load(self):
        path = Path(__file__).resolve().parents[2] / "ucm/kvready/executor.py"
        spec = importlib.util.spec_from_file_location("connector_contract_executor", path)
        execution = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = execution
        spec.loader.exec_module(execution)
        memory, stored, manifest, ready = {}, {}, [], set()
        held = threading.Event()
        held.set()

        class Broker:
            def call(self, op, **data):
                if op == "acquire":
                    return {"granted": True}
                if op == "saved":
                    manifest.append(data)
                if op == "state":
                    return {"manifest": list(manifest), "failed": False}
                if op == "ready":
                    ready.add(data["rank"])
                return {"ok": True}

        class Store:
            def __init__(self, rank):
                self.rank = rank

            def dump_data(self, keys, layers, addresses, event):
                for key, layer, row in zip(keys, layers, addresses):
                    stored[key, layer] = memory[int(row[0])]
                return {"waited": False}

            def load_data(self, keys, layers, addresses):
                return {"keys": keys, "layers": layers, "addresses": addresses,
                        "held": self.rank == 1 and layers[0] == 1 and keys[0].startswith(b"c"),
                        "waited": False}

            def check(self, task):
                if task.get("held") and held.is_set():
                    return False
                for key, layer, row in zip(task.get("keys", []), task.get("layers", []), task.get("addresses", [])):
                    memory[int(row[0])] = stored[key, layer]
                return True

            def wait(self, task):
                if task["waited"]:
                    raise AssertionError("duplicate backend wait")
                task["waited"] = True

        def eventually(predicate):
            deadline = time.monotonic() + 3
            while not predicate():
                if time.monotonic() >= deadline:
                    raise AssertionError("receive pipeline did not finish")
                time.sleep(0.005)

        workers, producers = [], []
        keys = [b"a", b"b", b"c", b"d"]
        broker = Broker()
        try:
            for rank in range(2):
                c = connector()
                c.tp_rank, c.control, c.timeout = rank, broker, 5
                c._worker_error, c._stop = None, threading.Event()
                c._d_completed = set()
                c.layer_ids, c.first_layer_id = [0, 1], 0
                c.request_hasher = lambda value: value + b"rank1"
                ptrs = np.arange(100 + rank * 100, 108 + rank * 100).reshape(2, 4, 1)
                c.kv_cache_layout = types.SimpleNamespace(
                    extract_block_addrs=lambda blocks, layer_first, ptrs=ptrs: ptrs,
                    tensor_size_lists=np.array([[4], [4]]),
                )
                c._connector_metadata = MODULE.ReadyMetadata(receives={"engine-id": {
                    "id": "r", "namespace": "ns", "input_hash": "same-input",
                    "keys": keys, "blocks": [1, 2, 3, 4], "local": 0, "prefix": 2, "target": 4,
                }})
                backend = Store(rank)
                c.executor = execution.StorageExecutor(backend, broker, rank)
                producers.append(execution.StorageExecutor(backend, broker, rank))
                for layer in range(2):
                    for index, key in enumerate(c._rank_keys(keys[:2])):
                        stored[key, layer] = rank * 1000 + layer * 10 + index
                c.start_load_kv(None)
                c._receiver = threading.Thread(target=c._receive_loop, daemon=True)
                c._receiver.start()
                workers.append(c)
            eventually(lambda: all(len(c._d_work["engine-id"]["submitted"]) == 4 for c in workers))
            self.assertFalse(ready)
            for rank, producer in enumerate(producers):
                for layer in range(2):
                    addresses = [[500 + rank * 100 + layer * 10 + i] for i in (2, 3)]
                    for i, row in zip((2, 3), addresses):
                        memory[row[0]] = rank * 1000 + layer * 10 + i
                    producer.submit("r", "p_save", layer, 2, workers[rank]._rank_keys(keys[2:]), addresses, 4)
            eventually(lambda: 0 in ready)
            self.assertEqual(workers[0].get_finished(set())[1], {"engine-id"})
            self.assertEqual(workers[1].get_finished(set())[1], set())
            held.clear()
            eventually(lambda: ready == {0, 1})
            self.assertEqual(workers[1].get_finished(set())[1], {"engine-id"})
            self.assertEqual(workers[1].get_finished(set())[1], set())
            for rank in range(2):
                for layer in range(2):
                    for index in range(4):
                        self.assertEqual(memory[100 + rank * 100 + layer * 4 + index], rank * 1000 + layer * 10 + index)
        finally:
            held.clear()
            for c in workers:
                c.shutdown()
            for producer in producers:
                producer.close(timeout=3)


if __name__ == "__main__":
    unittest.main()
