"""Compare real HCCL and ASU KV paths with bounded representative expert traffic.

Run after stopping the P/D model services, in the existing yellow-zone environment:
  ASCEND_RT_VISIBLE_DEVICES=4,5,6,7 torchrun --standalone --nproc-per-node=4 \
    test/kvready_e004/test_transfer.py --model-config /model/config.json \
    --asu-config /config/ucm.yaml --output results/e004 --services-stopped

Use --describe for a CPU-only size/configuration check, or --self-test for the
specification tests. Each of five cells has 5 warmups and 20 recorded requests.
One layer is released at a time, in model order, with the same TP2 bytes on both
paths. CPU barriers, data preparation, and exact byte validation are outside the
measurement. ASU timing includes save completion, its CPU publication, and load.

On an uncertain submitted operation, all processes retain their buffers and
print QUARANTINED. Stop the ASU backend/transport before terminating this demo;
do not restart model services while demo processes remain. No cancellation or
buffer reclamation is inferred from a timeout. ASU objects use a fresh namespace
per invocation and are reused only after the previous read has completed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import statistics
import subprocess
import threading
import time
import unittest
import uuid
from datetime import timedelta

MIB = 1024 * 1024
WARMUPS, REPEATS, WORLD, TP = 5, 20, 4, 2
CHUNK_BYTES, BUFFER_LIMIT = 8 * MIB, 256 * MIB
COMM_BYTES, COMM_STEPS = 8 * MIB, 8
HCCL_BUFFER_MIB = 16
TRANSPORT_ENV = ("ASCEND_RT_VISIBLE_DEVICES", "ASCEND_HOME_PATH", "HCCL_BUFFSIZE",
                 "HCCL_SOCKET_IFNAME", "HCCL_INTRA_ROCE_ENABLE", "HCCL_INTRA_PCIE_ENABLE",
                 "HCCL_RDMA_TRAFFIC_CLASS", "HCCL_RDMA_SL", "HCCL_IF_BASE_PORT",
                 "HCCL_WHITELIST_DISABLE", "HCCL_ALGO", "HCCL_EXEC_TIMEOUT")
CELLS = ("direct", "store", "comm", "direct_comm", "store_comm")


def make_spec(model, tokens=6144, block_size=128):
    """Uniform dense GQA BF16 geometry, matching the E004 TP2 model services."""
    if model.get("kv_lora_rank") or set(model.get("layer_types") or []) - {"full_attention"}:
        raise ValueError("microdemo requires uniform dense GQA layers")
    layers = int(model["num_hidden_layers"])
    heads = int(model["num_key_value_heads"])
    attention_heads = int(model["num_attention_heads"])
    hidden = int(model["hidden_size"])
    dim = int(model.get("head_dim", hidden // attention_heads))
    if min(layers, heads, dim, tokens, block_size) <= 0 or heads % TP:
        raise ValueError("positive geometry and KV heads divisible by TP2 required")
    if not model.get("head_dim") and hidden % attention_heads:
        raise ValueError("hidden_size must divide num_attention_heads exactly")
    if tokens % block_size:
        raise ValueError("tokens must consist of complete KV blocks")
    one_tensor_block = block_size * (heads // TP) * dim * 2
    block_bytes = 2 * one_tensor_block
    if block_bytes > CHUNK_BYTES:
        raise ValueError("one complete block exceeds the 8 MiB transfer bound")
    layer_bytes = tokens // block_size * block_bytes
    # Two aligned KV allocations, expert input/output, one communicator probe.
    application_bytes = layer_bytes + 2 * 4096 + 2 * COMM_BYTES + 4
    # Two participating HCCL groups; allow separate send/receive buffers in each.
    reserved_bytes = application_bytes + 4 * HCCL_BUFFER_MIB * MIB
    if reserved_bytes > BUFFER_LIMIT:
        raise ValueError("application-owned NPU buffers exceed 256 MiB per device")
    return {"model_type": model.get("model_type", "unknown"),
            "layers": layers, "kv_heads": heads, "head_dim": dim, "tp": TP,
            "dtype": "bfloat16", "tokens": tokens, "block_size": block_size,
            "blocks": tokens // block_size, "tensor_block_bytes": one_tensor_block,
            "block_bytes_per_rank": block_bytes, "layer_bytes_per_rank": layer_bytes,
            "request_bytes": layer_bytes * layers * TP,
            "required_store_bytes": layer_bytes * layers * TP,
            "application_buffer_bytes_per_device": application_bytes,
            "reserved_buffer_bytes_per_device": reserved_bytes,
            "hccl_buffer_mib_per_group": HCCL_BUFFER_MIB,
            "buffer_limit_bytes_per_device": BUFFER_LIMIT,
            "chunk_blocks": CHUNK_BYTES // block_bytes,
            "comm_bytes_per_rank_per_step": COMM_BYTES, "comm_steps_per_layer": COMM_STEPS,
            "comm_steps_per_request": COMM_STEPS * layers,
            "physical_pairs": [[4, 6], [5, 7]], "warmups": WARMUPS, "records": REPEATS}


def read_asu_config(path):
    import yaml
    with Path(path).open(encoding="utf-8") as stream:
        raw = yaml.safe_load(stream)
    entries = raw.get("ucm_connectors", []) if isinstance(raw, dict) else []
    if len(entries) != 1 or entries[0].get("ucm_connector_name") != "UcmPipelineStore":
        raise ValueError("one UcmPipelineStore entry is required in the ASU YAML")
    config = dict(entries[0]["ucm_connector_config"])
    if config.get("store_pipeline") != "ASU":
        raise ValueError("store_pipeline must be ASU")
    if str(config.get("asu_trans_provider_backend", "")).lower() not in ("aiv", "aicpu"):
        raise ValueError("a real ASU AIV/AICPU transfer provider is required")
    if config.get("asu_fake_backend_path") or config.get("asu_fake_backend_complete_immediately"):
        raise ValueError("fake-backend options must be disabled")
    if config.get("asu_config_path"):
        raise ValueError("use inline ASU settings so the actual provider is explicit")
    return config


def write_result(directory, result):
    target = Path(directory)
    target.mkdir(parents=True, exist_ok=True)
    temporary = target / "transfer.json.tmp"
    temporary.write_text(json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n",
                         encoding="utf-8")
    temporary.replace(target / "transfer.json")


class RunFailure(RuntimeError):
    pass


class Demo:
    def __init__(self, args, spec, config, torch, dist):
        self.args, self.spec, self.config = args, spec, config
        self.torch, self.dist = torch, dist
        self.rank = int(os.environ["RANK"])
        self.device = int(os.environ["LOCAL_RANK"])
        self.pair = self.rank % TP
        self.producer = self.rank < TP
        self.unsafe = False
        self.comm_active = False
        self.owned = []  # Submitted ASU descriptors remain strongly owned until wait succeeds.
        self.store = None
        self.records = {cell: [] for cell in CELLS}
        self.namespace = None

    def exchange(self, value):
        result = [None] * WORLD
        self.dist.all_gather_object(result, value)  # Default group is CPU Gloo.
        return result

    def check_stage(self, error=None):
        rows = self.exchange({"rank": self.rank, "error": error, "unsafe": self.unsafe})
        failures = [row for row in rows if row["error"]]
        if failures:
            self.unsafe = any(row["unsafe"] for row in rows)
            raise RunFailure("; ".join(f"rank {r['rank']}: {r['error']}" for r in failures))

    def prepare(self):
        t, d = self.torch, self.dist
        t.npu.set_device(self.device)
        error = None
        try:
            # Allocate only a single layer. Internal transport/runtime reservations
            # are separate from this explicitly bounded application buffer budget.
            count = self.spec["blocks"] * self.spec["tensor_block_bytes"] // 2
            self.kv = []
            self.allocations = []
            for _ in range(2):
                raw = t.empty(count * 2 + 4096, dtype=t.uint8, device=f"npu:{self.device}")
                shift = (-raw.data_ptr()) % 4096
                tensor = raw[shift:shift + count * 2].view(t.bfloat16)
                self.allocations.append(raw)
                self.kv.append(tensor)
            self.expected = [t.empty(count, dtype=t.bfloat16) for _ in range(2)]
            self.comm_in = t.full((COMM_BYTES // 4,), self.rank + 1, dtype=t.float32,
                                  device=f"npu:{self.device}")
            self.comm_out = t.empty_like(self.comm_in)
            self.probe = t.zeros(1, dtype=t.float32, device=f"npu:{self.device}")
            self.kv_stream, self.comm_stream = t.npu.Stream(), t.npu.Stream()
            t.npu.synchronize()
        except Exception as exc:
            error = f"buffer allocation: {exc}"
        self.check_stage(error)
        import torch_npu
        def group(ranks):
            options = torch_npu._C._distributed_c10d.ProcessGroupHCCL.Options()
            options.hccl_config = {"hccl_buffer_size": HCCL_BUFFER_MIB}
            return d.new_group(ranks, backend="hccl", pg_options=options,
                               timeout=timedelta(seconds=self.args.timeout))
        self.expert_group = group(list(range(WORLD)))
        pair_groups = [group([0, 2]), group([1, 3])]
        self.pair_group = pair_groups[self.pair]
        self.unsafe = True
        with t.npu.stream(self.kv_stream):
            d.all_reduce(self.probe, group=self.pair_group)
        self.kv_stream.synchronize()
        self.unsafe = False
        namespace = str(uuid.uuid4()) if self.rank == 0 else None
        self.namespace = self.exchange(namespace)[0]
        self.keys = [hashlib.blake2b(f"e004-demo:{self.namespace}:{self.pair}:{block}".encode(),
                                     digest_size=16).digest() for block in range(self.spec["blocks"])]
        error = None
        try:
            from ucm.store.factory_v1 import UcmConnectorFactoryV1
            cfg = dict(self.config)
            cfg.update(role="worker", device_id=self.device, tensor_layout="gqa",
                       share_buffer_enable=False, local_rank_size=1,
                       unique_id=f"e004-demo-{self.namespace}-{self.rank}",
                       tensor_size_list=[self.spec["tensor_block_bytes"]] * 2,
                       shard_size=self.spec["block_bytes_per_rank"],
                       block_size=self.spec["block_bytes_per_rank"] * self.spec["layers"],
                       gpu_kv_buffer_addrs=[x.data_ptr() for x in self.kv],
                       gpu_kv_buffer_sizes=[x.numel() * x.element_size() for x in self.kv])
            cfg["asu_client_id"] = f"{cfg.get('asu_client_id', 'e004')}-demo-{self.rank}"
            self.store = UcmConnectorFactoryV1.create_connector("UcmPipelineStore", cfg)
            self.actual_config = {
                key: ("<redacted>" if any(word in key.lower() for word in
                                         ("password", "secret", "token", "credential")) else value)
                for key, value in cfg.items() if key != "gpu_kv_buffer_addrs"}
        except Exception as exc:
            error = f"ASU registration: {exc}"
        self.check_stage(error)
        self.rank_configs = self.exchange(self.actual_config)

    def fill(self, layer, sample):
        t = self.torch
        width = self.spec["tensor_block_bytes"] // 2
        for index, cpu in enumerate(self.expected):
            for block, row in enumerate(cpu.view(self.spec["blocks"], width)):
                row.fill_((self.pair * 31 + layer * 7 + block + sample * 11) % 127 + 128 * index)
            if self.producer:
                self.kv[index].copy_(cpu)
            else:
                self.kv[index].zero_()
        self.comm_out.zero_()
        t.npu.synchronize()

    def store_transfer(self, layer, save):
        import numpy as np
        step = self.spec["chunk_blocks"]
        block_bytes = self.spec["tensor_block_bytes"]
        for start in range(0, self.spec["blocks"], step):
            end = min(start + step, self.spec["blocks"])
            keys = self.keys[start:end]
            indexes = [layer] * (end - start)
            addresses = np.array([[buf.data_ptr() + block * block_bytes for buf in self.kv]
                                  for block in range(start, end)], dtype=np.uint64)
            # A submission exception can leave acceptance uncertain. Keep descriptors.
            descriptor = [keys, indexes, addresses, None]
            self.owned.append(descriptor)
            self.unsafe = True
            task = (self.store.dump_data(keys, indexes, addresses, 0) if save else
                    self.store.load_data(keys, indexes, addresses))
            descriptor[3] = task
            deadline = time.monotonic() + self.args.timeout
            while not self.store.check(task):
                if time.monotonic() >= deadline:
                    raise RunFailure("ASU completion deadline; descriptors retained")
                time.sleep(0.001)
            self.store.wait(task)  # Exactly once, and only after check confirms completion.
            self.owned.remove(descriptor)
            self.unsafe = False

    def direct_transfer(self):
        t, d = self.torch, self.dist
        step = self.spec["chunk_blocks"]
        width = self.spec["tensor_block_bytes"] // 2
        peer = self.rank + TP if self.producer else self.rank - TP
        with t.npu.stream(self.kv_stream):
            for start in range(0, self.spec["blocks"], step):
                end = min(start + step, self.spec["blocks"])
                self.unsafe = True
                works = []
                self.owned.append(works)
                for tensor in self.kv:
                    part = tensor[start * width:end * width]
                    work = (d.isend(part, dst=peer, group=self.pair_group) if self.producer else
                            d.irecv(part, src=peer, group=self.pair_group))
                    works.append(work)
                for work in works:
                    work.wait()
                self.kv_stream.synchronize()
                self.owned.remove(works)
                self.unsafe = False

    def expert(self, go, ready, outcome):
        t = self.torch
        try:
            t.npu.set_device(self.device)
            with t.npu.stream(self.comm_stream):
                ready.set()
                go.wait()
                self.comm_active = True
                begin = time.perf_counter()
                for _ in range(COMM_STEPS):
                    work = self.dist.all_to_all_single(self.comm_out, self.comm_in,
                                                       group=self.expert_group, async_op=True)
                    work.wait()
                    self.comm_stream.synchronize()
                outcome["ms"] = (time.perf_counter() - begin) * 1000
                self.comm_active = False
        except Exception as exc:
            outcome["error"] = f"HCCL expert exchange: {exc}"
            outcome["unsafe"] = True
            ready.set()

    def layer(self, cell, layer, sample):
        has_kv, mixed = cell != "comm", cell in ("comm", "direct_comm", "store_comm")
        if has_kv:
            self.fill(layer, sample)
        else:
            self.comm_out.zero_()
            self.torch.npu.synchronize()
        go, ready, outcome = threading.Event(), threading.Event(), {}
        thread = None
        if mixed:
            thread = threading.Thread(target=self.expert, args=(go, ready, outcome), daemon=True)
            thread.start()
            if not ready.wait(self.args.timeout):
                raise RunFailure("expert thread did not become ready")
        self.dist.barrier()  # CPU release barrier is outside both measurements.
        begin = time.perf_counter()
        go.set()
        error = None
        save_ms = load_ms = publication_wait_ms = None
        try:
            if cell.startswith("direct"):
                self.direct_transfer()
            elif cell.startswith("store"):
                if self.producer:
                    self.store_transfer(layer, save=True)
                    save_ms = (time.perf_counter() - begin) * 1000
                # This CPU record is the producer-completion publication. Its cost
                # belongs to the storage handoff, unlike the layer release barrier.
        except Exception as exc:
            error = str(exc)
        if cell.startswith("store"):
            published = self.exchange({"error": error, "saved": self.producer and not error,
                                       "unsafe": self.unsafe})
            if not self.producer:
                publication_wait_ms = (time.perf_counter() - begin) * 1000
            self.unsafe = self.unsafe or any(row["unsafe"] for row in published)
            failures = [row["error"] for row in published if row["error"]]
            if failures:
                error = "; ".join(failures)
            elif not all(published[p]["saved"] for p in range(TP)):
                error = "producer completion publication missing"
            elif not self.producer:
                try:
                    load_begin = time.perf_counter()
                    self.store_transfer(layer, save=False)
                    load_ms = (time.perf_counter() - load_begin) * 1000
                except Exception as exc:
                    error = str(exc)
        kv_ms = (time.perf_counter() - begin) * 1000 if has_kv else None
        if thread:
            thread.join(self.args.timeout)
            if thread.is_alive():
                outcome.update(error="expert completion deadline", unsafe=True)
            error = error or outcome.get("error")
            self.unsafe = self.unsafe or outcome.get("unsafe", False)
        self.check_stage(error)
        # Exact bytes, including every block and both K/V tensors, after transfer.
        error = None
        if has_kv and not self.producer:
            if not all(self.torch.equal(got.cpu().view(self.torch.uint8),
                                        want.view(self.torch.uint8))
                       for got, want in zip(self.kv, self.expected)):
                error = f"KV byte mismatch at layer {layer}"
        if mixed:
            cpu = self.comm_out.cpu().reshape(WORLD, -1)
            if any(not self.torch.all(row == src + 1).item() for src, row in enumerate(cpu)):
                error = error or f"expert exchange mismatch at layer {layer}"
        self.check_stage(error)
        rows = self.exchange({"kv_ms": kv_ms, "comm_ms": outcome.get("ms"),
                              "save_ms": save_ms, "load_ms": load_ms,
                              "publication_wait_ms": publication_wait_ms})
        return {name: max((row[name] for row in rows if row[name] is not None), default=None)
                for name in rows[0]}

    def run(self):
        for cell in CELLS:
            for sample in range(WARMUPS + REPEATS):
                layers = [self.layer(cell, layer, sample) for layer in range(self.spec["layers"])]
                if any(value is not None and (not math.isfinite(value) or value <= 0)
                       for row in layers for value in row.values()):
                    raise RunFailure("nonfinite or nonpositive transfer measurement")
                if sample >= WARMUPS:
                    self.records[cell].append({name: (sum(row[name] for row in layers)
                                                     if layers[0][name] is not None else None)
                                               for name in layers[0]})
            if self.rank == 0:
                print(f"E004 transfer {cell}: {REPEATS} verified records", flush=True)

    def result(self, status, reason=None):
        try:
            commit = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True,
                                    text=True, timeout=5,
                                    cwd=Path(__file__).resolve().parents[2]).stdout.strip()
        except Exception:
            commit = "unknown"
        result = {"status": status, "units": "ms", "reason": reason,
                  "details": {"spec": self.spec, "namespace": self.namespace,
                              "commit": commit or "unknown", "torch_version": self.torch.__version__,
                              "records": self.records, "model_config": str(Path(self.args.model_config).resolve()),
                              "asu_config_file": str(Path(self.args.asu_config).resolve()),
                              "asu_rank_configs": getattr(self, "rank_configs", []),
                              "control_backend": "gloo", "kv_direct_backend": "hccl_p2p",
                              "expert_backend": "hccl_all_to_all_single",
                              "layer_release": "sequential completed layer; identical order and byte geometry",
                              "kv_metric": "sum of per-layer maximum rank completion milliseconds",
                              "comm_metric": "same per-layer sum; fixed eight exchanges per layer",
                              "store_task_inflight_per_rank": 1,
                              "data_check": "exact source/destination bytes on every transferred layer",
                              "buffer_scope": "application tensors plus explicit HCCL communication buffers; runtime/ASU internal workspace is additional",
                              "transport_environment": {key: os.environ[key] for key in TRANSPORT_ENV
                                                         if key in os.environ},
                              "shutdown": "quarantined; stop backend transport before terminating demo" if self.unsafe
                                          else "all submitted work completed"}}
        if status == "valid":
            for name, cell, metric in (("kv_direct", "direct", "kv_ms"), ("kv_store", "store", "kv_ms"),
                                       ("comm_alone", "comm", "comm_ms"),
                                       ("comm_with_direct", "direct_comm", "comm_ms"),
                                       ("comm_with_store", "store_comm", "comm_ms")):
                result[name] = round(statistics.mean(row[metric] for row in self.records[cell]), 3)
            result["details"]["kv_with_direct_comm_ms"] = statistics.mean(r["kv_ms"] for r in self.records["direct_comm"])
            result["details"]["kv_with_store_comm_ms"] = statistics.mean(r["kv_ms"] for r in self.records["store_comm"])
            result["details"]["store_components_ms"] = {
                metric: statistics.mean(r[metric] for r in self.records["store"])
                for metric in ("save_ms", "load_ms", "publication_wait_ms")}
            result["details"]["store_component_note"] = (
                "load_ms separately records D reads of completed objects; publication_wait_ms "
                "includes producer save and CPU completion publication, and overlaps save_ms")
        return result


class SpecificationTest(unittest.TestCase):
    def model(self):
        return {"num_hidden_layers": 48, "num_key_value_heads": 8,
                "num_attention_heads": 40, "hidden_size": 5120, "model_type": "qwen2"}

    def test_real_tp2_geometry_and_capacity(self):
        spec = make_spec(self.model())
        self.assertEqual(spec["layer_bytes_per_rank"], 12 * MIB)
        self.assertEqual(spec["required_store_bytes"], 1152 * MIB)
        self.assertLess(spec["reserved_buffer_bytes_per_device"], BUFFER_LIMIT)
        self.assertEqual(spec["chunk_blocks"], 32)

    def test_only_complete_blocks_and_bounded_buffers(self):
        for tokens in (6143, 262144):
            with self.assertRaises(ValueError):
                make_spec(self.model(), tokens=tokens)

    def test_uniform_tp_layout_required(self):
        model = self.model()
        model["num_key_value_heads"] = 3
        with self.assertRaises(ValueError):
            make_spec(model)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--model-config")
    parser.add_argument("--asu-config")
    parser.add_argument("--output", default="results/kvready-e004")
    parser.add_argument("--tokens", type=int, default=6144)
    parser.add_argument("--block-size", type=int, default=128)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--services-stopped", action="store_true")
    parser.add_argument("--describe", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(SpecificationTest)
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
    rank = int(os.environ.get("RANK", "0"))
    try:
        if not args.model_config or not args.asu_config:
            raise ValueError("--model-config and --asu-config are required")
        if not math.isfinite(args.timeout) or args.timeout <= 0:
            raise ValueError("timeout must be positive and finite")
        with Path(args.model_config).open(encoding="utf-8") as stream:
            spec = make_spec(json.load(stream), args.tokens, args.block_size)
        config = read_asu_config(args.asu_config)
        if args.describe:
            print(json.dumps({"status": "not_run", "reason": "specification_only", "spec": spec}, indent=2))
            return 0
        if not args.services_stopped:
            raise ValueError("stop P/D model services, then pass --services-stopped")
        if os.environ.get("ASCEND_RT_VISIBLE_DEVICES") != "4,5,6,7":
            raise ValueError("ASCEND_RT_VISIBLE_DEVICES must be exactly 4,5,6,7")
        if int(os.environ.get("WORLD_SIZE", "0")) != WORLD or int(os.environ.get("LOCAL_WORLD_SIZE", "0")) != WORLD:
            raise ValueError("use single-host torchrun --nproc-per-node=4")
        if rank not in range(WORLD) or int(os.environ.get("LOCAL_RANK", "-1")) != rank:
            raise ValueError("expected local ranks 0..3 matching global ranks")
        # This standalone process must not inherit the model service's large buffer.
        os.environ["HCCL_BUFFSIZE"] = str(HCCL_BUFFER_MIB)
        import torch
        import torch_npu  # noqa: F401: registers the real NPU/HCCL runtime.
        import torch.distributed as dist
        from ucm.store.factory_v1 import UcmConnectorFactoryV1  # noqa: F401
        if not torch.npu.is_available() or torch.npu.device_count() != WORLD:
            raise RuntimeError("four visible NPU devices are required")
        if not dist.is_gloo_available() or not hasattr(dist, "all_to_all_single"):
            raise RuntimeError("CPU Gloo control and HCCL all_to_all_single support are required")
    except Exception as exc:
        if rank == 0:
            result = {"status": "not_run", "reason": f"preflight: {exc}"}
            write_result(args.output, result)
            print(json.dumps(result), flush=True)
        return 2
    demo = Demo(args, spec, config, torch, dist)
    stage = "prepare"
    try:
        # CPU control is independent of both HCCL data groups and excluded from
        # timing, except the required ASU write-complete publication.
        dist.init_process_group("gloo", timeout=timedelta(seconds=args.timeout))
        demo.prepare()
        stage = "transfer"
        demo.run()
        result = demo.result("valid")
    except Exception as exc:
        demo.unsafe = demo.unsafe or demo.comm_active
        if dist.is_initialized():
            try:
                demo.unsafe = any(demo.exchange(demo.unsafe))
            except Exception:
                # Lost CPU coordination cannot confirm peer operation ownership.
                demo.unsafe = True
        result = demo.result("not_run" if stage == "prepare" and not demo.unsafe else "invalid", str(exc))
    if rank == 0:
        write_result(args.output, result)
        print(json.dumps({k: v for k, v in result.items() if k != "details"}), flush=True)
    if demo.unsafe:
        print(f"QUARANTINED rank={rank}: buffers retained; stop backend transport before terminating demo", flush=True)
        while True:
            time.sleep(60)
    demo.store = None  # Native shutdown precedes release of registered NPU tensors.
    if dist.is_initialized():
        dist.destroy_process_group()
    return 0 if result["status"] == "valid" else 2


if __name__ == "__main__":
    raise SystemExit(main())
