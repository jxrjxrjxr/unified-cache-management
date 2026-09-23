"""Bounded ASU submission with ownership lasting until confirmed completion."""

from concurrent.futures import Future, wait as wait_futures
from dataclasses import dataclass, field
import threading
import time
import uuid


class _TransferFuture(Future):
    def cancel(self):
        # Cancelling a Python Future cannot cancel a device transfer.
        return False


@dataclass
class _Group:
    future: Future
    remaining: int
    records: list = field(default_factory=list)
    error: Exception | None = None


@dataclass
class _Transfer:
    meta: dict
    keys: tuple
    addresses: object
    event: object
    group: _Group
    queued_at: float
    handle: object = None
    submitted_at: float = 0.0
    unknown: bool = False
    registered: bool = False


class StorageExecutor:
    """The store implements load_data/dump_data/check/wait; control grants tickets.

    submit() never waits for a ticket. A returned Future represents all chunks.
    on_complete receives that Future once. A timeout stops admission but does not
    release device addresses. Call drain() and check quiescent before destroying
    device events or reusing buffers. Store task handles have one exclusive owner.
    """

    def __init__(self, store, control, rank, max_inflight=2,
                 max_task_bytes=8 * 1024 * 1024, max_queued=4096,
                 poll_interval=0.001):
        if min(max_inflight, max_task_bytes, max_queued) <= 0:
            raise ValueError("executor limits must be positive")
        self.store, self.control, self.rank = store, control, rank
        self.max_inflight = max_inflight
        self.max_task_bytes, self.max_queued = max_task_bytes, max_queued
        self.poll_interval = poll_interval
        self._cv = threading.Condition()
        self._pending, self._active = [], []
        self._failed_requests = {}
        self._fault = None
        self._accepting = True
        self._closing = False
        self._thread = threading.Thread(target=self._run, daemon=True,
                                        name=f"kvready-storage-{rank}")
        self._thread.start()

    @property
    def quiescent(self):
        with self._cv:
            return not self._pending and not self._active

    def submit(self, rid, kind, layer, start, keys, addresses, block_bytes,
               event=0, on_complete=None):
        if kind not in {"p_load", "p_save", "d_prefix", "d_new"}:
            raise ValueError(f"unsupported storage operation: {kind}")
        if block_bytes <= 0 or block_bytes > self.max_task_bytes or start < 0:
            raise ValueError("one complete layer/block must fit the task limit")
        keys = tuple(keys)
        if len(addresses) != len(keys):
            raise ValueError("one address row is required for each block")
        # Retain an independent array, or immutable rows for a Python sequence.
        addresses = (addresses.copy() if hasattr(addresses, "shape") else
                     tuple(tuple(row) for row in addresses))
        step = self.max_task_bytes // block_bytes
        chunks = (len(keys) + step - 1) // step
        future = _TransferFuture()
        group = _Group(future, chunks)
        if on_complete is not None:
            future.add_done_callback(on_complete)
        with self._cv:
            if not self._accepting or rid in self._failed_requests:
                raise RuntimeError("storage executor is not accepting this request")
            if len(self._pending) + chunks > self.max_queued:
                raise RuntimeError("local storage queue is full")
            for offset in range(0, len(keys), step):
                end = min(offset + step, len(keys))
                meta = dict(task_id=uuid.uuid4().hex, request_id=rid, kind=kind,
                            rank=self.rank, layer=layer, block_start=start + offset,
                            block_end=start + end, nbytes=(end - offset) * block_bytes)
                self._pending.append(_Transfer(meta, keys[offset:end],
                                               addresses[offset:end], event, group,
                                               time.monotonic()))
            self._cv.notify_all()
        if not chunks:
            future.set_result([])
        return future

    def _finish_group(self, task, record, error=None):
        group = task.group
        group.records.append(record)
        group.error = group.error or error
        group.remaining -= 1
        if group.remaining == 0:
            if group.error is not None:
                group.future.set_exception(group.error)
            else:
                group.future.set_result(group.records)

    def cancel_request(self, request_id, error="request cancelled"):
        error = error if isinstance(error, Exception) else RuntimeError(error)
        with self._cv:
            self._failed_requests[request_id] = error
            self._cv.notify_all()

    def stop(self, error="storage execution stopped"):
        error = error if isinstance(error, Exception) else RuntimeError(error)
        with self._cv:
            self._accepting = False
            self._fault = self._fault or error
            self._cv.notify_all()

    def _request_error(self, task):
        with self._cv:
            return self._fault or self._failed_requests.get(task.meta["request_id"])

    def _release(self, task, success, record, error):
        if not task.registered:
            return
        self.control.call("release", task_id=task.meta["task_id"], success=success,
                          nbytes=task.meta["nbytes"],
                          transfer_ms=record.get("transfer_ms", 0),
                          queue_ms=record.get("queue_ms", 0),
                          error=str(error) if error else None)

    def _complete(self, task, error=None, submitted=True):
        now = time.monotonic()
        error = error or self._request_error(task)
        record = dict(task.meta, queued_at=task.queued_at,
                      submitted_at=task.submitted_at, completed_at=now,
                      queue_ms=1000 * ((task.submitted_at or now) - task.queued_at),
                      transfer_ms=1000 * (now - task.submitted_at) if submitted else 0,
                      success=error is None)
        try:
            self._release(task, error is None, record, error)
            if error is None and task.meta["kind"] == "p_save":
                self.control.call("saved", **{k: task.meta[k] for k in
                                  ("request_id", "rank", "layer", "block_start", "block_end")})
        except Exception as exc:
            error = error or exc
            self.stop(exc)
        record["success"] = error is None
        with self._cv:
            if task in self._active:
                self._active.remove(task)
            self._cv.notify_all()
        self._finish_group(task, record, error)

    @staticmethod
    def _unknown_wait_error(error):
        message = str(error).lower().replace("_", " ")
        return any(x in message for x in ("timeout", "timed out", "task not found"))

    def _poll(self, task):
        if task.unknown:
            return
        try:
            done = self.store.check(task.handle)
        except Exception as exc:
            # A failed observation does not establish that DMA has stopped.
            self.stop(exc)
            return
        if not done:
            return
        try:
            self.store.wait(task.handle)  # Exactly once, after check says done.
        except Exception as exc:
            self.stop(exc)
            if self._unknown_wait_error(exc):
                task.unknown = True
                return
            self._complete(task, exc)
        else:
            self._complete(task)

    def _dispatch(self):
        with self._cv:
            candidates = list(self._pending)
        for task in candidates:
            error = self._request_error(task)
            if error is not None:
                with self._cv:
                    self._pending.remove(task)
                self._complete(task, error, submitted=False)
                continue
            with self._cv:
                if len(self._active) >= self.max_inflight:
                    return
            try:
                task.registered = True
                response = self.control.call("acquire", **task.meta)
                if response.get("failed"):
                    self.cancel_request(task.meta["request_id"],
                                        response.get("error", "producer failed"))
                    continue
                if not response["granted"]:
                    continue
            except Exception as exc:
                self.stop(exc)
                continue
            error = self._request_error(task)
            if error is not None:
                with self._cv:
                    self._pending.remove(task)
                self._complete(task, error, submitted=False)
                continue
            # Make ownership visible before calling the store submission API.
            with self._cv:
                self._pending.remove(task)
                self._active.append(task)
            task.submitted_at = time.monotonic()
            try:
                layers = [task.meta["layer"]] * len(task.keys)
                if task.meta["kind"] == "p_save":
                    task.handle = self.store.dump_data(list(task.keys), layers,
                                                       task.addresses, task.event)
                else:
                    task.handle = self.store.load_data(list(task.keys), layers,
                                                       task.addresses)
            except Exception as exc:
                # ASU submission failure returns no handle and queues no transfer.
                self.stop(exc)
                self._complete(task, exc, submitted=False)

    def _run(self):
        while True:
            with self._cv:
                if self._closing and not self._pending and not self._active:
                    return
                active = list(self._active)
            for task in active:
                self._poll(task)
            self._dispatch()
            with self._cv:
                self._cv.wait(self.poll_interval)

    def drain(self, timeout=None):
        """Wait for physical ownership to end; unknown tasks remain owned."""
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cv:
            while self._pending or self._active:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    error = TimeoutError("storage is not quiescent; buffers remain owned")
                    self._accepting = False
                    self._fault = self._fault or error
                    self._cv.notify_all()
                    raise error
                self._cv.wait(remaining)

    def wait(self, futures, timeout=None):
        futures = list(futures)
        deadline = None if timeout is None else time.monotonic() + timeout
        _, pending = wait_futures(futures, timeout=timeout)
        if pending:
            self.stop(TimeoutError("storage wait exceeded its deadline"))
            raise TimeoutError("storage wait timed out; drain before releasing buffers")
        errors = [f.exception() for f in futures if f.exception() is not None]
        if errors:
            remaining = None if deadline is None else max(0, deadline - time.monotonic())
            self.drain(remaining)
            raise errors[0]
        return [f.result() for f in futures]

    def close(self, timeout=None):
        with self._cv:
            self._accepting = False
            self._closing = True
            self._cv.notify_all()
        self.drain(timeout)
        self._thread.join(timeout)
