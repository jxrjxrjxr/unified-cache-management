"""Thread-safe metadata broker. It never owns KV addresses or performs I/O."""

import copy
import math
import threading
import time
from collections import Counter

from .policy import KINDS, MODES, restore_plan, task_order, validate_calibration


class ControlClient:
    """Small synchronous JSON client used by scheduler and executor threads."""

    def __init__(self, url, timeout=10.0):
        import httpx
        self.client = httpx.Client(base_url=url.rstrip("/"), timeout=timeout, trust_env=False)

    def call(self, op, **payload):
        response = self.client.post("/control/" + op, json=payload)
        response.raise_for_status()
        return response.json()

    def close(self):
        self.client.close()


class Coordinator:
    def __init__(self, *, tp_size=2, num_layers=0, mode="eager", calibration=None,
                 max_tasks=8, max_bytes=64 * 1024**2, worker_tasks=2,
                 task_bytes=8 * 1024**2, clock=time.monotonic):
        if mode not in MODES or tp_size < 1 or num_layers < 0:
            raise ValueError("invalid coordinator configuration")
        if min(max_tasks, max_bytes, worker_tasks, task_bytes) < 1:
            raise ValueError("budgets must be positive")
        self.config = dict(tp_size=tp_size, num_layers=num_layers, mode=mode,
                           max_tasks=max_tasks, max_bytes=max_bytes,
                           worker_tasks=worker_tasks, task_bytes=task_bytes,
                           calibration=validate_calibration(calibration))
        self.requests = {}
        self.tasks = {}
        self.pending_tasks = set()
        self.request_tasks = {}
        self._lock = threading.RLock()
        self._clock = clock
        self._sequence = 0
        self.inflight_bytes = 0
        self.inflight_tasks = 0
        self.peak_bytes = 0
        self.peak_tasks = 0
        self.workers = Counter()
        self.admission_error = None
        self.calibration_locked = bool(calibration)

    def _now(self):
        return self._clock() * 1000

    def _next(self):
        self._sequence += 1
        return self._sequence

    def call(self, op, **payload):
        operations = {"register", "bind", "d_plan", "p_plan", "allocated", "ready",
                      "progress", "producer_done", "acquire", "release", "saved",
                      "fail", "state", "summary", "config", "set_calibration"}
        if op not in operations:
            raise ValueError("unknown control operation: " + op)
        with self._lock:
            if op == "config":
                return copy.deepcopy(self.config)
            return getattr(self, "_" + op)(**payload)

    def _set_calibration(self, calibration):
        validated = validate_calibration(calibration)
        if not validated:
            raise ValueError("measured calibration is required")
        if validated == self.config["calibration"]:
            return {"ok": True, "calibration": copy.deepcopy(validated)}
        if self.calibration_locked or any(r["role"] == "pd" for r in self.requests.values()):
            raise ValueError("calibration is frozen for formal requests")
        if self.inflight_tasks:
            raise ValueError("wait for calibration storage tasks to complete")
        self.config["calibration"] = validated
        self.calibration_locked = True
        return {"ok": True, "calibration": copy.deepcopy(validated)}

    def _request(self, request_id):
        if request_id not in self.requests:
            raise ValueError("unregistered request: " + request_id)
        return self.requests[request_id]

    def _rank(self, request, rank):
        if not isinstance(rank, int) or not 0 <= rank < request["tp_size"]:
            raise ValueError("rank outside configured TP")

    def _register(self, request_id, namespace, mode=None, tp_size=None,
                  num_layers=None, role="pd", input_hash=None, layout=None, action=None):
        if not request_id or not isinstance(namespace, str) or not namespace:
            raise ValueError("request_id and namespace are required")
        mode = mode or self.config["mode"]
        tp_size = tp_size or self.config["tp_size"]
        num_layers = num_layers or self.config["num_layers"]
        if mode not in MODES or role not in ("pd", "prepare", "reference", "calibrate"):
            raise ValueError("invalid mode or role")
        if action is not None and (role != "calibrate" or action not in ("LOAD", "RECOMPUTE")):
            raise ValueError("forced action belongs to calibration requests only")
        if tp_size != self.config["tp_size"] or num_layers < 0:
            raise ValueError("request TP/layout differs from coordinator")
        if self.config["num_layers"] and num_layers != self.config["num_layers"]:
            raise ValueError("request layer count differs from coordinator")
        if request_id in self.requests:
            old = self.requests[request_id]
            if (old["namespace"], old["mode"], old["role"]) != (namespace, mode, role):
                raise ValueError("request_id reused with different metadata")
            return self._state(request_id)
        if self.admission_error:
            raise ValueError("new admission stopped: " + self.admission_error)
        self.requests[request_id] = dict(
            request_id=request_id, namespace=namespace, mode=mode, role=role,
            tp_size=tp_size, num_layers=num_layers, input_hash=input_hash, layout=layout,
            sequence=self._next(), created_ms=self._now(), failed=False, error=None,
            allocated_ranks=[], ready_ranks=[], producer_done_ranks=[], manifest=[],
            bindings=[], progress={}, p_plan=(dict(action=action, reason="forced_calibration")
                                             if action else None), d_plan=None, prefix_remaining_bytes=0,
            prefix_completed_bytes=0, completed_bytes={}, events=[], producer_deadline_ms=self._now())
        self.request_tasks[request_id] = []
        return self._state(request_id)

    def _bind(self, request_id, role, rank, input_hash, layout,
              tp_size=None, num_layers=None, namespace=None):
        r = self._request(request_id)
        if rank != -1:
            self._rank(r, rank)
        if role not in ("p", "d", "prepare", "reference", "calibrate"):
            raise ValueError("invalid binding role")
        checks = {"input_hash": input_hash, "layout": layout}
        if tp_size is not None:
            checks["tp_size"] = tp_size
        if namespace is not None:
            checks["namespace"] = namespace
        if num_layers is not None:
            checks["num_layers"] = num_layers
        for key, value in checks.items():
            if r[key] is None or (key == "num_layers" and r[key] == 0):
                r[key] = copy.deepcopy(value)
            elif r[key] != value:
                self._fail(request_id, "binding mismatch: " + key)
                raise ValueError("binding mismatch: " + key)
        if not input_hash or not layout or r["num_layers"] < 1:
            self._fail(request_id, "incomplete input/layout binding")
            raise ValueError("complete input hash, layout and layer count required")
        binding = {"role": role, "rank": rank}
        if binding not in r["bindings"]:
            r["bindings"].append(binding)
        return {"ok": True}

    def _d_plan(self, request_id, prefix_blocks, total_blocks, local_blocks, block_bytes):
        r = self._request(request_id)
        if not (0 <= local_blocks <= total_blocks and 0 <= prefix_blocks <= total_blocks) or block_bytes <= 0:
            raise ValueError("invalid D block plan")
        plan = dict(prefix_blocks=prefix_blocks, total_blocks=total_blocks,
                    local_blocks=local_blocks, block_bytes=block_bytes)
        if r["d_plan"] is not None and r["d_plan"] != plan:
            raise ValueError("D plan already committed")
        r["d_plan"] = plan
        r["prefix_remaining_bytes"] = max(
            0, (prefix_blocks - local_blocks) * block_bytes - r["prefix_completed_bytes"])
        return {"ok": True}

    def _p_plan(self, request_id, prefix_blocks=0, load_bytes=0, recompute_tokens=0,
                new_tokens=0, p_remaining_ms=0, d_remaining_ms=0, tail_ms=0):
        r = self._request(request_id)
        if r["p_plan"] is None:
            values = (prefix_blocks, load_bytes, recompute_tokens, new_tokens,
                      p_remaining_ms, d_remaining_ms, tail_ms)
            if any(not math.isfinite(value) or value < 0 for value in values):
                raise ValueError("non-finite or negative restore-plan input")
            r["p_plan"] = restore_plan(
                r["mode"], self.config["calibration"], load_bytes=load_bytes,
                recompute_tokens=recompute_tokens, new_tokens=new_tokens,
                d_remaining_bytes=r["prefix_remaining_bytes"],
                p_remaining_ms=p_remaining_ms, d_remaining_ms=d_remaining_ms, tail_ms=tail_ms,
                new_transfer_ms=self._remaining_new_io_ms(r))
            r["p_plan"]["inputs"] = dict(prefix_blocks=prefix_blocks, load_bytes=load_bytes,
                                         recompute_tokens=recompute_tokens, new_tokens=new_tokens)
            r["producer_initial_ms"] = r["p_plan"].get("p_remaining_ms", 0)
            r["producer_compute_deadline_ms"] = self._now() + r["producer_initial_ms"]
            self._update_producer_deadline(r)
        return copy.deepcopy(r["p_plan"])

    def _remaining_new_io_ms(self, r):
        plan = r["d_plan"]
        bw = self.config["calibration"].get("storage_bytes_per_ms")
        if not plan or not bw:
            return 0.0
        new_bytes = max(0, plan["total_blocks"] - max(plan["prefix_blocks"],
                                                    plan["local_blocks"])) * plan["block_bytes"]
        completed = r["completed_bytes"]
        return (max(0, new_bytes - completed.get("p_save", 0)) +
                max(0, new_bytes - completed.get("d_new", 0))) / bw

    def _update_producer_deadline(self, r):
        # Conservatively serialize remaining storage service after compute.
        # Real overlap is reported by task events, not by this predictor.
        r["producer_remaining_io_ms"] = self._remaining_new_io_ms(r)
        r["producer_deadline_ms"] = max(self._now(), r.get("producer_compute_deadline_ms", self._now())) + r["producer_remaining_io_ms"]

    def _mark(self, request_id, rank, key):
        r = self._request(request_id)
        self._rank(r, rank)
        if rank not in r[key]:
            r[key].append(rank)
            r["events"].append(dict(event=key, rank=rank, at_ms=self._now()))
        self._schedule()
        return {"ok": True}

    def _allocated(self, request_id, rank):
        return self._mark(request_id, rank, "allocated_ranks")

    def _ready(self, request_id, rank):
        return self._mark(request_id, rank, "ready_ranks")

    def _producer_done(self, request_id, rank):
        result = self._mark(request_id, rank, "producer_done_ranks")
        r = self._request(request_id)
        if len(r["producer_done_ranks"]) == r["tp_size"]:
            r["producer_compute_deadline_ms"] = self._now()
            self._update_producer_deadline(r)
        return result

    def _progress(self, request_id, rank, layer, elapsed_ms=None, remaining_ms=None):
        r = self._request(request_id)
        self._rank(r, rank)
        if not 0 <= layer < r["num_layers"]:
            raise ValueError("invalid progress layer")
        r["progress"][str(rank)] = dict(layer=layer, at_ms=self._now(), elapsed_ms=elapsed_ms)
        if remaining_ms is not None:
            if remaining_ms < 0:
                raise ValueError("negative remaining time")
            r["producer_compute_deadline_ms"] = self._now() + remaining_ms
            self._update_producer_deadline(r)
        self._schedule()
        return {"ok": True}

    def _acquire(self, task_id, request_id, kind, rank, layer, block_start,
                 block_end, nbytes, worker_id=None, urgent=True):
        r = self._request(request_id)
        self._rank(r, rank)
        if kind not in KINDS or not 0 <= layer < r["num_layers"]:
            raise ValueError("invalid task kind/layer")
        if not 0 <= block_start < block_end or not 0 < nbytes <= self.config["task_bytes"]:
            raise ValueError("task exceeds legal block/byte range")
        worker_id = worker_id or ("p" if kind.startswith("p_") else "d") + ":" + str(rank)
        fields = dict(task_id=task_id, request_id=request_id, kind=kind, rank=rank,
                      layer=layer, block_start=block_start, block_end=block_end,
                      nbytes=nbytes, worker_id=worker_id, urgent=urgent)
        if task_id not in self.tasks:
            self.tasks[task_id] = dict(fields, status="pending", sequence=self._next(),
                                      queued_ms=self._now(), granted_ms=None)
            self.pending_tasks.add(task_id)
            self.request_tasks[request_id].append(task_id)
            if kind == "d_prefix" and r["d_plan"] is None:
                r["prefix_remaining_bytes"] += nbytes
        elif any(self.tasks[task_id][key] != value for key, value in fields.items()):
            raise ValueError("task_id reused with different metadata")
        self._schedule()
        task = self.tasks[task_id]
        return dict(granted=task["status"] == "inflight",
                    failed=r["failed"] or self.admission_error is not None,
                    error=r["error"] or self.admission_error, status=task["status"])

    def _eligible(self, task):
        r = self.requests[task["request_id"]]
        if r["failed"]:
            return False
        if task["kind"].startswith("p_") and r["role"] == "pd":
            if len(r["allocated_ranks"]) != r["tp_size"]:
                return False
        if task["kind"] == "d_prefix" and r["mode"] == "late":
            return len(r["producer_done_ranks"]) == r["tp_size"]
        if task["kind"] == "d_new":
            return any(m["rank"] == task["rank"] and m["layer"] == task["layer"] and
                       m["block_start"] <= task["block_start"] and
                       m["block_end"] >= task["block_end"] for m in r["manifest"])
        return True

    def _schedule(self):
        if self.admission_error:
            return
        pending = [self.tasks[task_id] for task_id in self.pending_tasks
                   if self._eligible(self.tasks[task_id])]
        pending.sort(key=lambda t: task_order(t, self.requests[t["request_id"]],
                                             self._now(), self.config["calibration"]))
        for task in pending:
            if self.inflight_tasks >= self.config["max_tasks"]:
                break
            if (self.inflight_bytes + task["nbytes"] > self.config["max_bytes"] or
                    self.workers[task["worker_id"]] >= self.config["worker_tasks"]):
                continue
            task["status"] = "inflight"
            self.pending_tasks.discard(task["task_id"])
            task["granted_ms"] = self._now()
            self.inflight_tasks += 1
            self.inflight_bytes += task["nbytes"]
            self.workers[task["worker_id"]] += 1
            self.peak_bytes = max(self.peak_bytes, self.inflight_bytes)
            self.peak_tasks = max(self.peak_tasks, self.inflight_tasks)

    def _release(self, task_id, success, nbytes=None, transfer_ms=None, queue_ms=None, error=None):
        if task_id not in self.tasks:
            raise ValueError("unknown task_id")
        t = self.tasks[task_id]
        r = self.requests[t["request_id"]]
        if t["status"] in ("complete", "cancelled", "failed"):
            return {"ok": True}
        if t["status"] == "pending" and success:
            raise ValueError("ungranted task cannot complete")
        if t["status"] == "inflight":
            self.inflight_tasks -= 1
            self.inflight_bytes -= t["nbytes"]
            self.workers[t["worker_id"]] -= 1
        self.pending_tasks.discard(task_id)
        if success:
            t["status"] = "complete"
            r["completed_bytes"][t["kind"]] = r["completed_bytes"].get(t["kind"], 0) + t["nbytes"]
            if t["kind"] == "d_prefix":
                r["prefix_completed_bytes"] += t["nbytes"]
                r["prefix_remaining_bytes"] = max(0, r["prefix_remaining_bytes"] - t["nbytes"])
        else:
            t["status"] = "failed"
            self._fail(t["request_id"], error or "storage task failed")
        t.update(completed_ms=self._now(), transfer_ms=transfer_ms,
                 queue_ms=queue_ms, error=error)
        if success and t["kind"] in ("p_save", "d_new"):
            self._update_producer_deadline(r)
        self._schedule()
        return {"ok": True}

    def _saved(self, request_id, rank, layer, block_start, block_end):
        r = self._request(request_id)
        if r["failed"]:
            return {"ok": False, "failed": True, "error": r["error"]}
        entry = dict(rank=rank, layer=layer, block_start=block_start, block_end=block_end)
        if not any(t["request_id"] == request_id and t["kind"] == "p_save" and
                   t["status"] == "complete" and all(t[k] == v for k, v in entry.items())
                   for t in (self.tasks[tid] for tid in self.request_tasks[request_id])):
            raise ValueError("manifest requires a completed physical save")
        if entry not in r["manifest"]:
            r["manifest"].append(entry)
        # A layer contributes production progress only after all its new ranges
        # have completed on every rank, never merely when the hook enqueued it.
        d = r["d_plan"]
        if d and d["total_blocks"] > d["prefix_blocks"]:
            completed_layers = 0
            for candidate in range(r["num_layers"]):
                if all(self._covered(r["manifest"], rank_id, candidate,
                                     d["prefix_blocks"], d["total_blocks"])
                       for rank_id in range(r["tp_size"])):
                    completed_layers += 1
                else:
                    break
            remaining = r.get("producer_initial_ms", 0) * (1 - completed_layers / r["num_layers"])
            r["producer_compute_deadline_ms"] = self._now() + remaining
            self._update_producer_deadline(r)
        self._schedule()
        return {"ok": True}

    @staticmethod
    def _covered(manifest, rank, layer, start, end):
        ranges = sorted((m["block_start"], m["block_end"]) for m in manifest
                        if m["rank"] == rank and m["layer"] == layer)
        for lo, hi in ranges:
            if lo > start:
                break
            start = max(start, hi)
            if start >= end:
                return True
        return start >= end

    def _fail(self, request_id, error):
        r = self._request(request_id)
        self.admission_error = self.admission_error or str(error)
        if not r["failed"]:
            r["failed"], r["error"] = True, str(error)
            r["events"].append(dict(event="failed", at_ms=self._now(), error=str(error)))
        for task_id in self.request_tasks[request_id]:
            t = self.tasks[task_id]
            if t["status"] == "pending":
                t["status"] = "cancelled"
                self.pending_tasks.discard(task_id)
        return {"ok": True}

    def _state(self, request_id):
        r = copy.deepcopy(self._request(request_id))
        r.update(d_allocated=len(r["allocated_ranks"]) == r["tp_size"],
                 d_ready=len(r["ready_ranks"]) == r["tp_size"],
                 producer_done=len(r["producer_done_ranks"]) == r["tp_size"])
        tasks = [self.tasks[tid] for tid in self.request_tasks[request_id]]
        r["tasks"] = dict(Counter(t["status"] for t in tasks))
        r["p_tasks"] = dict(Counter(t["status"] for t in tasks if t["kind"].startswith("p_")))
        return r

    def _summary(self, namespace=None):
        selected = [r for r in self.requests.values()
                    if namespace is None or r["namespace"] == namespace]
        ids = [r["request_id"] for r in selected]
        return dict(requests=[self._state(r["request_id"]) for r in selected],
                    tasks=[copy.deepcopy(self.tasks[tid]) for rid in ids for tid in self.request_tasks[rid]],
                    inflight_tasks=self.inflight_tasks, inflight_bytes=self.inflight_bytes,
                    peak_tasks=self.peak_tasks, peak_bytes=self.peak_bytes,
                    admission_error=self.admission_error)
