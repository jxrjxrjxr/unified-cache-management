"""E004 D-first HTTP proxy and colocated metadata coordinator.

Run with ``python -m ucm.pd.kvready_proxy --help``. Existing UCM proxies and
connectors retain their defaults; this service is explicitly opt-in.
"""

import argparse
import asyncio
import contextlib
import copy
import json
import os
import time
import uuid
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse

from ucm.kvready.coordinator import Coordinator


def create_app(*, prefill_url="http://127.0.0.1:8100",
               decode_url="http://127.0.0.1:8200", coordinator=None,
               request_timeout=120.0, transport=None):
    broker = coordinator or Coordinator()

    @asynccontextmanager
    async def lifespan(app):
        async with httpx.AsyncClient(timeout=request_timeout, transport=transport,
                                    trust_env=False) as client:
            app.state.client = client
            app.state.slots = asyncio.Semaphore(2)
            yield

    app = FastAPI(lifespan=lifespan)
    app.state.coordinator = broker
    app.state.active_requests = set()

    def headers(request_id):
        result = {"X-Request-Id": request_id}
        if os.environ.get("OPENAI_API_KEY"):
            result["Authorization"] = "Bearer " + os.environ["OPENAI_API_KEY"]
        return result

    def request_parts(body, role):
        body = copy.deepcopy(body)
        options = body.pop("kvready", {})
        if not isinstance(options, dict) or not options.get("namespace"):
            raise HTTPException(400, "kvready.namespace is required")
        request_id = options.get("id") or uuid.uuid4().hex
        metadata = dict(id=request_id, namespace=options["namespace"], role=role)
        if "action" in options:
            metadata["action"] = options["action"]
        body["kv_transfer_params"] = {"kvready": metadata}
        return body, options, request_id

    def register(options, request_id, role):
        # IDs identify one real admission, including preparation/calibration.
        if request_id in broker.requests:
            raise HTTPException(409, "kvready.id has already been used")
        try:
            broker.call("register", request_id=request_id, namespace=options["namespace"],
                        mode=options.get("mode"), role=role, action=options.get("action"))
        except (TypeError, ValueError) as exc:
            raise HTTPException(400, str(exc)) from exc

    @app.get("/healthcheck")
    async def healthcheck():
        return {"status": "ok", "service": "kvready_e004"}

    @app.post("/control/{op}")
    def control(op: str, body: dict):
        try:
            return broker.call(op, **body)
        except (KeyError, TypeError, ValueError) as exc:
            raise HTTPException(400, str(exc)) from exc

    @app.get("/e004/config")
    async def config():
        return dict(broker.call("config"), prefill_url=prefill_url, decode_url=decode_url,
                    request_timeout=request_timeout, d_receiving_limit=2)

    @app.get("/e004/summary")
    async def summary(namespace: str = None):
        return broker.call("summary", namespace=namespace)

    @app.post("/e004/calibration")
    async def calibration(body: dict):
        try:
            return broker.call("set_calibration", calibration=body["calibration"])
        except (KeyError, TypeError, ValueError) as exc:
            raise HTTPException(400, str(exc)) from exc

    @app.get("/e004/state/{request_id}")
    async def state(request_id: str):
        try:
            return broker.call("state", request_id=request_id)
        except ValueError as exc:
            raise HTTPException(404, str(exc)) from exc

    @app.post("/e004/reset")
    async def reset():
        # The backend must affirm its real cache-reset API. Reset never claims
        # external storage deletion or invents a successful cleanup operation.
        active = broker.call("summary")
        if app.state.active_requests or active["inflight_tasks"] or any(
                not r["failed"] and r["role"] == "pd" and not r["d_ready"]
                for r in active["requests"]):
            raise HTTPException(409, "wait for active receives and storage operations before reset")
        results = {}
        for role, base in (("p", prefill_url), ("d", decode_url)):
            response = await app.state.client.post(
                base.rstrip("/") + "/reset_prefix_cache",
                params={"reset_running_requests": "false"}, headers=headers("cache-reset"))
            if not response.is_success:
                raise HTTPException(502, f"{role} reset failed: HTTP {response.status_code}")
            if response.text.strip().lower() == "false":
                raise HTTPException(502, f"{role} reset returned false")
            results[role] = {"status_code": response.status_code, "body": response.text}
        return {"ok": True, "p": True, "d": True, "backends": results}

    async def single_stage(request, role):
        body, options, request_id = request_parts(await request.json(), role)
        endpoint = options.get("endpoint", "completions")
        if endpoint not in ("completions", "chat/completions"):
            raise HTTPException(400, "invalid endpoint")
        if role == "calibrate" and options.get("action") not in ("LOAD", "RECOMPUTE"):
            raise HTTPException(400, "calibration requires kvready.action=LOAD or RECOMPUTE")
        register(options, request_id, role)
        app.state.active_requests.add(request_id)
        if role == "reference" and body.get("stream"):
            try:
                response = await app.state.client.send(app.state.client.build_request(
                    "POST", decode_url.rstrip("/") + "/v1/" + endpoint,
                    json=body, headers=headers(request_id)), stream=True)
                response.raise_for_status()
            except Exception as exc:
                app.state.active_requests.discard(request_id)
                broker.call("fail", request_id=request_id, error=str(exc))
                raise HTTPException(502, str(exc)) from exc

            async def reference_stream():
                try:
                    async for chunk in response.aiter_bytes():
                        yield chunk
                except BaseException as exc:
                    broker.call("fail", request_id=request_id, error=str(exc) or "reference cancelled")
                    raise
                finally:
                    await response.aclose()
                    app.state.active_requests.discard(request_id)

            return StreamingResponse(reference_stream(), status_code=response.status_code,
                                     media_type=response.headers.get("content-type", "text/event-stream"),
                                     headers={"X-KVReady-Request-Id": request_id})
        body["stream"] = False
        body.pop("stream_options", None)
        if role != "reference":
            body["max_tokens"] = 1
            body.pop("min_tokens", None)
            body.pop("max_completion_tokens", None)
        base = decode_url if role == "reference" else prefill_url
        started = time.monotonic()
        try:
            response = await app.state.client.post(base.rstrip("/") + "/v1/" + endpoint,
                                                   json=body, headers=headers(request_id))
            response.raise_for_status()
            # vLLM's P response follows its connector wait_for_save. Validate
            # broker tickets too before marking the producer complete.
            if role != "reference":
                s = broker.call("state", request_id=request_id)
                if s["failed"] or s["tasks"].get("pending", 0) or s["tasks"].get("inflight", 0):
                    raise RuntimeError(s["error"] or "P returned with unfinished storage tasks")
                if role == "prepare" and not s["completed_bytes"].get("p_save", 0):
                    raise RuntimeError("preparation returned without a completed storage save")
                for rank in range(s["tp_size"]):
                    broker.call("producer_done", request_id=request_id, rank=rank)
            result = response.json()
            result["kvready"] = dict(id=request_id, elapsed_ms=(time.monotonic()-started)*1000,
                                      state=broker.call("state", request_id=request_id))
            return JSONResponse(result)
        except Exception as exc:
            broker.call("fail", request_id=request_id, error=str(exc))
            raise HTTPException(502, str(exc)) from exc
        finally:
            app.state.active_requests.discard(request_id)

    @app.post("/e004/prepare")
    async def prepare(request: Request):
        return await single_stage(request, "prepare")

    @app.post("/e004/calibrate")
    async def calibrate(request: Request):
        return await single_stage(request, "calibrate")

    @app.post("/e004/reference")
    async def reference(request: Request):
        return await single_stage(request, "reference")

    async def handle(request, endpoint):
        body, options, request_id = request_parts(await request.json(), "d")
        await app.state.slots.acquire()
        released = False

        def release_slot():
            nonlocal released
            if not released:
                released = True
                app.state.slots.release()

        try:
            register(options, request_id, "pd")
            app.state.active_requests.add(request_id)
        except Exception:
            release_slot()
            raise
        chunks = asyncio.Queue(maxsize=4)
        response_info = asyncio.get_running_loop().create_future()
        deadline = time.monotonic() + request_timeout

        async def decode():
            async with app.state.client.stream(
                    "POST", decode_url.rstrip("/") + "/v1/" + endpoint,
                    json=body, headers=headers(request_id)) as response:
                response.raise_for_status()
                response_info.set_result((response.status_code,
                                          response.headers.get("content-type", "application/json")))
                async for chunk in response.aiter_bytes():
                    await chunks.put(chunk)

        d_task = asyncio.create_task(decode())
        p_task = None

        async def wait_flag(flag):
            while True:
                s = broker.call("state", request_id=request_id)
                if s["failed"]:
                    raise RuntimeError(s["error"])
                if s[flag]:
                    return s
                if d_task.done():
                    d_task.result()
                    raise RuntimeError("D response ended before " + flag)
                if time.monotonic() > deadline:
                    raise TimeoutError("request deadline waiting for " + flag)
                await asyncio.sleep(0.01)

        async def produce():
            p_body = copy.deepcopy(body)
            p_body["kv_transfer_params"]["kvready"]["role"] = "p"
            p_body["stream"] = False
            p_body["max_tokens"] = 1
            p_body.pop("min_tokens", None)
            p_body.pop("max_completion_tokens", None)
            p_body.pop("stream_options", None)
            result = await app.state.client.post(prefill_url.rstrip("/") + "/v1/" + endpoint,
                                                 json=p_body, headers=headers(request_id))
            result.raise_for_status()
            s = broker.call("state", request_id=request_id)
            if s["failed"] or s["p_tasks"].get("pending", 0) or s["p_tasks"].get("inflight", 0):
                raise RuntimeError(s["error"] or "P returned before its storage tasks completed")
            for rank in range(s["tp_size"]):
                broker.call("producer_done", request_id=request_id, rank=rank)

        async def watch_ready():
            try:
                await wait_flag("d_ready")
            finally:
                release_slot()

        async def cleanup(failure=None):
            if failure:
                broker.call("fail", request_id=request_id, error=failure)
            for task in (d_task, p_task, ready_task):
                if task is not None and not task.done():
                    task.cancel()
            for task in (d_task, p_task, ready_task):
                if task is not None:
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await task
            release_slot()
            app.state.active_requests.discard(request_id)

        ready_task = None
        try:
            await wait_flag("d_allocated")
            p_task = asyncio.create_task(produce())
            ready_task = asyncio.create_task(watch_ready())
            while not response_info.done():
                for task in (p_task, d_task, ready_task):
                    if task.done():
                        task.result()
                if time.monotonic() > deadline:
                    raise TimeoutError("request deadline waiting for D response")
                await asyncio.sleep(0.01)
            status_code, content_type = response_info.result()
        except BaseException as exc:
            await cleanup(str(exc) or "request cancelled")
            if isinstance(exc, asyncio.CancelledError):
                raise
            raise HTTPException(502, str(exc)) from exc

        async def stream():
            failure = None
            try:
                while not d_task.done() or not chunks.empty():
                    for task in (p_task, d_task, ready_task):
                        if task.done():
                            task.result()
                    if time.monotonic() > deadline:
                        raise TimeoutError("request deadline forwarding D output")
                    try:
                        chunk = await asyncio.wait_for(chunks.get(), timeout=0.05)
                    except asyncio.TimeoutError:
                        continue
                    yield chunk
                await p_task
                await ready_task
                d_task.result()
            except BaseException as exc:
                failure = str(exc) or "client disconnected"
                raise
            finally:
                await cleanup(failure)

        return StreamingResponse(stream(), status_code=status_code,
                                 media_type=content_type,
                                 headers={"X-KVReady-Request-Id": request_id})

    @app.post("/v1/completions")
    async def completions(request: Request):
        return await handle(request, "completions")

    @app.post("/v1/chat/completions")
    async def chat_completions(request: Request):
        return await handle(request, "chat/completions")

    return app


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--prefill-url", default="http://127.0.0.1:8100")
    parser.add_argument("--decode-url", default="http://127.0.0.1:8200")
    parser.add_argument("--tp-size", type=int, default=2)
    parser.add_argument("--num-layers", type=int, required=True)
    parser.add_argument("--mode", choices=("late", "eager", "progress", "joint"), default="eager")
    parser.add_argument("--calibration", help="fixed measured JSON coefficients")
    parser.add_argument("--request-timeout", type=float, default=120)
    args = parser.parse_args()
    calibration = None
    if args.calibration:
        with open(args.calibration, encoding="utf-8") as source:
            calibration = json.load(source)
    broker = Coordinator(tp_size=args.tp_size, num_layers=args.num_layers,
                         mode=args.mode, calibration=calibration)
    import uvicorn
    uvicorn.run(create_app(prefill_url=args.prefill_url, decode_url=args.decode_url,
                           coordinator=broker, request_timeout=args.request_timeout),
                host=args.host, port=args.port)


if __name__ == "__main__":
    main()
