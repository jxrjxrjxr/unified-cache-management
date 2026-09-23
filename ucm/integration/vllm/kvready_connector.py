"""E004: storage-backed PD handoff using the existing layerwise KV layout.

The scheduler owns token promises; workers own storage tasks and device events.
Only the explicit KVReadyConnector selection enables this experiment.
"""

import json
import threading
import time
from functools import wraps
from dataclasses import dataclass, field

import numpy as np

from ucm.integration.vllm.ucm_connector import (
    KVConnectorRole,
    RequestDispatchMeta,
    RequestMeta,
    UCMConnectorMetadata,
    UCMLayerWiseConnector,
)
from ucm.kvready.coordinator import ControlClient
from ucm.kvready.executor import StorageExecutor


def storage_guard(method):
    """A Python exception must not outlive device-buffer ownership."""
    @wraps(method)
    def guarded(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except Exception as error:
            if self.executor is not None:
                self.executor.stop(error)
                self._worker_error = error
                self._notify_failure(error)
                # The HTTP client has a deadline. Physical ownership has none:
                # unknown DMA stays pinned until the backend confirms completion.
                self.executor.drain()
                if self.side == "p":
                    self.device.destroy_event_handles()
            raise
    return guarded


@dataclass
class ReadyRequestMeta(RequestMeta):
    context: dict = field(default_factory=dict)
    original_hit: int = 0
    dump_end: int = 0
    target: int = 0


@dataclass
class ReadyMetadata(UCMConnectorMetadata):
    contexts: dict = field(default_factory=dict)
    receives: dict = field(default_factory=dict)
    finished: set = field(default_factory=set)


class KVReadyConnector(UCMLayerWiseConnector):
    def __init__(self, vllm_config, role, kv_cache_config=None):
        super().__init__(vllm_config, role, kv_cache_config)
        options = self.launch_config.get("kvready", {})
        self.side = options.get("role")
        if self.side not in ("p", "d"):
            raise ValueError("kvready.role must be p or d")
        if self.is_mla or vllm_config.parallel_config.pipeline_parallel_size != 1:
            raise ValueError("E004 requires GQA and pipeline_parallel_size=1")
        if not vllm_config.model_config.enforce_eager:
            raise ValueError("E004 requires --enforce-eager")
        self.timeout = float(options.get("timeout", 120))
        self.control = ControlClient(options.get("control_url", "http://127.0.0.1:8300"))
        self.signature = {
            "model": vllm_config.model_config.model,
            "tokenizer": vllm_config.model_config.tokenizer,
            "dtype": str(vllm_config.model_config.dtype),
            "kv_dtype": str(vllm_config.cache_config.cache_dtype),
            "block_size": self.block_size,
            "layers": self.num_layers,
            "kv_heads_per_rank": self.num_head,
            "head_size": self.head_size,
            "tp": self.tp_size,
            "pp": 1,
            "attention": "gqa",
            "rope": getattr(vllm_config.model_config.hf_text_config, "rope_scaling", None),
        }
        self._contexts = {}
        self._matches = {}
        self._receives = {}
        self._allocated = set()
        self._lock = threading.RLock()
        self._d_work = {}
        self._d_completed = set()
        self._p_loads = {}
        self._p_saves = []
        self._p_data = []
        self._p_dump_data = []
        self._worker_error = None
        self._stop = threading.Event()
        self.executor = None

    def _context(self, request):
        params = (getattr(request, "kv_transfer_params", None) or {}).get("kvready")
        if not isinstance(params, dict) or not all(params.get(k) for k in ("id", "namespace", "role")):
            raise ValueError("E004 requests must arrive through the KVReady proxy")
        expected = {"p", "prepare", "calibrate"} if self.side == "p" else {"d", "reference"}
        if params["role"] not in expected:
            raise ValueError("KVReady request role does not match this service")
        if request.mm_features or request.lora_request or request.prompt_token_ids is None:
            raise ValueError("E004 accepts text prompts with one fixed model")
        return dict(params)

    def _bind(self, context, rank):
        self.control.call(
            "bind", request_id=context["id"], role=self.side, rank=rank,
            input_hash=context["input_hash"], layout=self.signature,
            tp_size=self.tp_size, num_layers=self.num_layers,
        )

    def get_num_new_matched_tokens(self, request, num_computed_tokens):
        context = self._context(request)
        if context["role"] == "reference":
            return 0, False
        if request.request_id in self._contexts:
            # Resumption after the asynchronous receive already has local KV.
            # Preemption is outside the fixed batch resource contract.
            previous = self._contexts[request.request_id]
            if self.side == "d" and num_computed_tokens >= previous["target"] * self.block_size:
                return 0, False
            if num_computed_tokens == previous["local"] * self.block_size:
                return self._matches[request.request_id]
            raise RuntimeError("E004 preemption exceeded the fixed batch resource contract")
        n = request.num_prompt_tokens
        b = self.block_size
        if n < b + 1 or num_computed_tokens % b:
            raise ValueError("E004 requires a full prefix block and a block-aligned local hit")
        context["input_hash"] = self.request_hasher(tuple(request.prompt_token_ids)).hex()
        seed = self.request_hasher(("KVREADY", context["namespace"], json.dumps(self.signature, sort_keys=True)))
        keys = self.generate_hash(b, request.prompt_token_ids, seed)
        self._bind(context, -1)
        h = num_computed_tokens // b
        # A key is only reused after the dedicated preparation request completed.
        r = self.store.lookup_on_prefix(keys) + 1
        if not 0 <= r <= len(keys):
            raise RuntimeError("external prefix lookup returned an invalid range")
        c = (n - 1) // b
        context.update(n=n, local=h, prefix=min(r, c), target=c)
        self._contexts[request.request_id] = context
        if self.side == "d":
            self.control.call(
                "d_plan", request_id=context["id"], prefix_blocks=min(r, c),
                total_blocks=c, local_blocks=h,
                block_bytes=b * self.num_layers * self.num_head * self.head_size * self.element_size * 2 * self.tp_size,
            )
            context["keys"] = keys[:c]
            # No future external data is promised for an already local range.
            self._matches[request.request_id] = (max(0, c * b - num_computed_tokens), h < c)
            return self._matches[request.request_id]

        preparing = context["role"] == "prepare"
        if preparing:
            action = "RECOMPUTE"
            r = 0
            context["target"] = len(keys)
        elif context["role"] == "calibrate":
            action = context.get("action")
            if action not in ("LOAD", "RECOMPUTE"):
                raise ValueError("calibration requires LOAD or RECOMPUTE")
        else:
            decision = self.control.call(
                "p_plan", request_id=context["id"], prefix_blocks=r,
                load_bytes=max(0, r - h) * b * self.num_layers * self.num_head * self.head_size * self.element_size * 2 * self.tp_size,
                recompute_tokens=max(0, r - h) * b,
                new_tokens=n - max(r, h) * b,
            )
            action = decision["action"]
        accepted = max(h, r) if action == "LOAD" else h
        external = max(0, accepted * b - num_computed_tokens)
        if accepted * b == n and external:
            external -= 1
        self.requests_meta[request.request_id] = ReadyRequestMeta(
            ucm_block_ids=keys, hbm_hit_block_num=h, total_hit_block_num=accepted,
            num_token_ids=n, token_processed=num_computed_tokens + external,
            context=context, original_hit=r, dump_end=r, target=context["target"],
        )
        self._matches[request.request_id] = (external, False)
        return self._matches[request.request_id]

    def update_state_after_alloc(self, request, blocks, num_external_tokens):
        if self.side != "d" or request.request_id in self._allocated:
            return
        context = self._contexts.get(request.request_id)
        if context is None:
            return
        ids = blocks.get_block_ids()[0]
        target = context["target"]
        if len(ids) < target:
            raise RuntimeError("D allocation does not cover its external KV promise")
        self._allocated.add(request.request_id)
        self._receives[request.request_id] = {**context, "blocks": list(ids[:target])}

    def _generate_dispatch_meta(self, req_meta, new_tokens, vllm_block_ids, need_load=True):
        req_meta.vllm_block_ids.extend(vllm_block_ids)
        h, accepted = req_meta.hbm_hit_block_num, req_meta.total_hit_block_num
        load = (req_meta.ucm_block_ids[h:accepted], req_meta.vllm_block_ids[h:accepted]) if need_load else ([], [])
        end = min(req_meta.target, (req_meta.token_processed + new_tokens) // self.block_size)
        start = max(req_meta.original_hit, req_meta.dump_end)
        dump = (req_meta.ucm_block_ids[start:end], req_meta.vllm_block_ids[start:end])
        req_meta.dump_end = max(req_meta.dump_end, end)
        req_meta.token_processed += new_tokens
        dispatch = RequestDispatchMeta(load, dump)
        dispatch.dump_start = start
        return dispatch

    def build_connector_meta(self, scheduler_output):
        if self.side == "p":
            meta = super().build_connector_meta(scheduler_output)
        else:
            meta = UCMConnectorMetadata()
        contexts = {rid: self._contexts[rid] for rid in meta.request_meta}
        receives, self._receives = self._receives, {}
        finished = set(scheduler_output.finished_req_ids)
        for rid in finished:
            self._contexts.pop(rid, None)
            self._matches.pop(rid, None)
            self._allocated.discard(rid)
        return ReadyMetadata(meta.request_meta, contexts, receives, finished)

    def register_kv_caches(self, kv_caches):
        super().register_kv_caches(kv_caches)
        if self.layer_ids != list(range(self.num_layers)):
            raise ValueError("E004 requires one contiguous full-model layer group")
        expected = self.block_size * self.num_head * self.head_size * self.element_size * 2
        if any(int(row.sum()) != expected for row in self.kv_cache_layout.tensor_size_lists):
            raise ValueError("actual KV layout differs from the declared GQA layout")
        self.executor = StorageExecutor(self.store, self.control, self.tp_rank % self.tp_size)
        if self.side == "d":
            self._receiver = threading.Thread(target=self._receive_loop, daemon=True, name="kvready-receive")
            self._receiver.start()

    def _rank_keys(self, keys):
        if self.tp_rank % self.tp_size:
            return [self.request_hasher(key) for key in keys]
        return list(keys)

    def _check_worker(self):
        if self._worker_error is not None:
            raise RuntimeError("KVReady worker stopped after a storage/control failure") from self._worker_error

    def _notify_failure(self, error):
        meta = getattr(self, "_connector_metadata", None)
        contexts = list(getattr(meta, "contexts", {}).values())
        with self._lock:
            contexts += [work["context"] for work in self._d_work.values()]
        for rid in {context["id"] for context in contexts}:
            try:
                self.control.call("fail", request_id=rid, error=str(error))
            except Exception:
                pass  # Keep the original fault and continue physical draining.

    @storage_guard
    def start_load_kv(self, forward_context, **kwargs):
        self._check_worker()
        meta = self._get_connector_metadata()
        if not isinstance(meta, ReadyMetadata):
            raise TypeError("KVReady metadata missing")
        if self.side == "d":
            with self._lock:
                for engine_id, context in meta.receives.items():
                    if engine_id in self._d_work:
                        continue
                    self._bind(context, self.tp_rank % self.tp_size)
                    if context["local"] >= context["target"]:
                        self.control.call("allocated", request_id=context["id"], rank=self.tp_rank % self.tp_size)
                        self.control.call("ready", request_id=context["id"], rank=self.tp_rank % self.tp_size)
                        continue
                    self._d_work[engine_id] = {
                        "context": context, "ptrs": self.kv_cache_layout.extract_block_addrs(context["blocks"], layer_first=True),
                        "keys": self._rank_keys(context["keys"]), "submitted": set(),
                        "futures": [], "cancelled": False, "started": time.monotonic(),
                    }
                    self.control.call("allocated", request_id=context["id"], rank=self.tp_rank % self.tp_size)
                for engine_id in meta.finished:
                    if engine_id in self._d_work:
                        self._d_work[engine_id]["cancelled"] = True
            return
        self._p_loads = {}
        self._p_saves = []
        self._p_data = []
        self._p_dump_data = []
        for engine_id, dispatch in meta.request_meta.items():
            context = meta.contexts[engine_id]
            self._bind(context, self.tp_rank % self.tp_size)
            keys, blocks = dispatch.load_block_ids
            if keys:
                self._p_data.append((context, self._rank_keys(keys), self.kv_cache_layout.extract_block_addrs(blocks, layer_first=True)))
            keys, blocks = dispatch.dump_block_ids
            if keys:
                self._p_dump_data.append((context, self._rank_keys(keys), dispatch.dump_start,
                                          self.kv_cache_layout.extract_block_addrs(blocks, layer_first=True)))
        self._load_layer(self.first_layer_id)

    def _load_layer(self, layer):
        row = layer - self.first_layer_id
        futures = []
        for context, keys, ptrs in self._p_data:
            futures.append(self.executor.submit(
                context["id"], "p_load", layer, context["local"], keys,
                np.ascontiguousarray(ptrs[row]), int(self.kv_cache_layout.tensor_size_lists[row].sum()),
            ))
        self._p_loads[layer] = futures

    @storage_guard
    def wait_for_layer_load(self, layer_name):
        if self.side != "p" or not self._connector_metadata:
            return
        layer = self.layer_name_to_id[layer_name]
        self.executor.wait(self._p_loads.pop(layer, []), timeout=self.timeout)
        if layer + 1 in self.layer_ids:
            self._load_layer(layer + 1)

    @storage_guard
    def save_kv_layer(self, layer_name, kv_layer, attn_metadata, **kwargs):
        if self.side != "p" or not self._connector_metadata:
            return
        layer = self.layer_name_to_id[layer_name]
        row = layer - self.first_layer_id
        event = None
        for context, keys, start, ptrs in self._p_dump_data:
            if event is None:
                event = self._get_dump_event_handle()
            future = self.executor.submit(
                context["id"], "p_save", layer, start, keys,
                np.ascontiguousarray(ptrs[row]), int(self.kv_cache_layout.tensor_size_lists[row].sum()), event=event,
            )
            self._p_saves.append(future)

    @storage_guard
    def wait_for_save(self):
        if self.side != "p" or self.executor is None:
            return
        try:
            self.executor.wait(self._p_saves, timeout=self.timeout)
        finally:
            # A timed-out task still owns its source and prerequisite event.
            if self.executor.quiescent:
                self.device.destroy_event_handles()
                self._p_saves = []
                self._p_dump_data = []

    def _submit_receive(self, work, layer, start, end, kind):
        context = work["context"]
        start, end = max(start, context["local"]), min(end, context["target"])
        if start >= end:
            return
        for block in range(start, end):
            if (layer, block) in work["submitted"]:
                raise RuntimeError("duplicate receive coverage")
        row = layer - self.first_layer_id
        future = self.executor.submit(
            context["id"], kind, layer, start, work["keys"][start:end],
            np.ascontiguousarray(work["ptrs"][row, start:end]),
            int(self.kv_cache_layout.tensor_size_lists[row].sum()),
        )
        work["futures"].append(future)
        work["submitted"].update((layer, block) for block in range(start, end))

    def _receive_loop(self):
        while not self._stop.wait(0.005):
            try:
                with self._lock:
                    for engine_id, work in list(self._d_work.items()):
                        if engine_id in self._d_completed:
                            continue
                        context = work["context"]
                        state = self.control.call("state", request_id=context["id"])
                        if state.get("failed"):
                            raise RuntimeError(state.get("error", "producer failed"))
                        if time.monotonic() - work["started"] > self.timeout:
                            raise TimeoutError("D receive exceeded the fixed request deadline")
                        if not work["cancelled"]:
                            if not work.get("prefix_queued"):
                                for layer in self.layer_ids:
                                    self._submit_receive(work, layer, context["local"], context["prefix"], "d_prefix")
                                work["prefix_queued"] = True
                            for item in state.get("manifest", []):
                                if item["rank"] != self.tp_rank % self.tp_size:
                                    continue
                                key = (item["layer"], item["block_start"], item["block_end"])
                                if key not in work.setdefault("manifests", set()):
                                    self._submit_receive(work, item["layer"], max(context["prefix"], item["block_start"]), item["block_end"], "d_new")
                                    work["manifests"].add(key)
                        expected = max(0, context["target"] - context["local"]) * len(self.layer_ids)
                        if (len(work["submitted"]) == expected or work["cancelled"]) and all(f.done() for f in work["futures"]):
                            for future in work["futures"]:
                                future.result()
                            self.control.call("ready", request_id=context["id"], rank=self.tp_rank % self.tp_size)
                            self._d_completed.add(engine_id)
            except Exception as exc:
                self._worker_error = exc
                self.executor.stop(exc)
                self._notify_failure(exc)
                return

    @storage_guard
    def get_finished(self, finished_req_ids):
        self._check_worker()
        if self.side != "d":
            return set(), set()
        with self._lock:
            completed = set(self._d_completed)
            self._d_completed.clear()
            for engine_id in completed:
                self._d_work.pop(engine_id, None)
            return set(), completed

    def request_finished(self, request, block_ids):
        # vLLM itself pins requests cancelled while WAITING_FOR_REMOTE_KVS
        # until finished_recving arrives. P writes finish at wait_for_save.
        return False, None

    def shutdown(self):
        self._stop.set()
        if hasattr(self, "_receiver"):
            self._receiver.join(timeout=self.timeout)
        if self.executor is not None:
            self.executor.close(timeout=self.timeout)
