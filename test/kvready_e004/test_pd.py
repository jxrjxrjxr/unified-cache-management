"""Run the approved finite E004 experiment against real P/D HTTP services.

Imports do not contact services. Run this file explicitly; unittest discovers only
the dedicated CPU test files, outside legacy conftest.py.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from importlib import metadata
import os
import statistics
import subprocess
import sys
import time
import uuid
from pathlib import Path

from report import MODES, append_record, json_safe, validate_request, write_report
from workload import answer_matches, make_manifest, required_storage_bytes


class InvalidRun(RuntimeError):
    def __init__(self, stage, message):
        self.stage = stage
        super().__init__(message)


def installed_versions():
    result = {}
    for package in ("vllm", "vllm-ascend", "torch", "torch-npu", "uc-manager"):
        try:
            result[package] = metadata.version(package)
        except metadata.PackageNotFoundError:
            result[package] = "unavailable"
    return result


def save_json(path, value):
    Path(path).write_text(json.dumps(json_safe(value), ensure_ascii=False, indent=2,
                                    allow_nan=False) + "\n", encoding="utf-8")


def completion_payload(config, case, namespace, mode, phase, request_id):
    formal = phase == "performance"
    return {
        "model": config["served_model_name"], "prompt": case["prompt"],
        "max_tokens": 128, "temperature": 0, "seed": 17,
        "ignore_eos": formal, "min_tokens": 128 if formal else 0,
        "stream": True, "stream_options": {"include_usage": True},
        "kvready": {"namespace": namespace, "mode": mode, "id": request_id},
    }


def consume_sse(line):
    if not line.startswith("data:"):
        return None
    data = line[5:].strip()
    if data == "[DONE]":
        return {"done": True}
    if not data:
        return None
    event = json.loads(data)
    if event.get("error"):
        raise InvalidRun("stream_error", str(event["error"]))
    return event


async def request_json(client, method, path, **kwargs):
    response = await client.request(method, path, **kwargs)
    response.raise_for_status()
    data = response.json()
    if isinstance(data, dict) and (data.get("error") or data.get("status") in ("failed", "invalid")):
        raise InvalidRun("control", str(data.get("error") or data))
    return data


def state_complete(state):
    # Exact coordinator state is normalized by the proxy. The runner requires
    # positive completion evidence, not just an HTTP 200 or a prefix lookup hit.
    tasks = state.get("tasks", {})
    terminal = (isinstance(tasks, dict) and
                all(tasks.get(name, 0) == 0 for name in ("pending", "inflight", "failed", "cancelled")))
    if state.get("failed") is not False or not terminal:
        return False
    if state.get("role") in ("prepare", "calibrate"):
        return state.get("producer_done") is True
    return state.get("d_ready") is True and state.get("producer_done") is True


async def snapshot(client, request_id):
    return await request_json(client, "GET", f"/e004/state/{request_id}")


async def stream_request(client, config, case, namespace, mode, phase, batch,
                         planned_at, raw_path, semaphore):
    request_id = f"{namespace}-{case['case_id']}"
    record = {"phase": phase, "batch": batch, "mode": mode,
              "case_id": case["case_id"], "family": case["family"],
              "request_id": request_id, "namespace": namespace,
              "planned_at": planned_at, "status": "failed"}
    try:
        await asyncio.sleep(max(0.0, planned_at - time.perf_counter()))
        deadline = planned_at + 120.0
        # The deadline includes any wait for one of the four client slots.
        async def send():
            async with semaphore:
                record["sent_at"] = time.perf_counter()
                body = completion_payload(config, case, namespace, mode, phase, request_id)
                text_parts = []
                usage = None
                saw_done = False
                async with client.stream("POST", "/v1/completions", json=body) as response:
                    if not response.is_success:
                        detail = (await response.aread()).decode("utf-8", errors="replace")
                        raise InvalidRun("http_response", f"HTTP {response.status_code}: {detail[:2000]}")
                    async for line in response.aiter_lines():
                        event = consume_sse(line)
                        if event is None:
                            continue
                        if event.get("done"):
                            saw_done = True
                            continue
                        if event.get("usage"):
                            usage = event["usage"]
                        for choice in event.get("choices", []):
                            fragment = choice.get("text") or choice.get("delta", {}).get("content") or ""
                            if fragment:
                                record.setdefault("first_at", time.perf_counter())
                                text_parts.append(fragment)
                record["completed_at"] = time.perf_counter()
                record["text"] = "".join(text_parts)
                record["output_tokens"] = usage.get("completion_tokens") if usage else None
                record["usage"] = usage
                if not usage or usage.get("prompt_tokens") != len(case["prompt"]):
                    raise InvalidRun("input_length", "Server usage must confirm the frozen input-token count")
                if not saw_done:
                    raise InvalidRun("stream_incomplete", "No final SSE DONE marker")
                state = await snapshot(client, request_id)
                record["state"] = state
                record["kv_complete"] = state_complete(state)
                record["p_action"] = (state.get("p_plan") or {}).get("action", "unknown")
                plan = state.get("d_plan") or {}
                block_size = config.get("block_size", 128)
                total_blocks = (len(case["prompt"]) - 1) // block_size
                prefix_blocks = min(case["prefix_tokens"] // block_size, total_blocks)
                if (plan.get("prefix_blocks") != prefix_blocks or plan.get("local_blocks") != 0
                        or plan.get("total_blocks") != total_blocks):
                    raise InvalidRun("cache_range", "D plan differs from the prepared external prefix or contains a local cache hit")
                if record["p_action"] not in ("LOAD", "RECOMPUTE"):
                    raise InvalidRun("producer_action", "Missing actual producer action")
                record["status"] = "completed"
                errors = validate_request(record)
                if errors:
                    raise InvalidRun(errors[0], ", ".join(errors))
                if phase == "correctness":
                    record["answer_pass"] = answer_matches(record["text"], case["expected"])
                    if not record["answer_pass"]:
                        raise InvalidRun("answer_mismatch", f"Missing expected answer fields for {case['case_id']}")
                return record
        return await asyncio.wait_for(send(), timeout=max(0.001, deadline - time.perf_counter()))
    except BaseException as error:
        record["status"] = "invalid"
        record["failure_stage"] = getattr(error, "stage", "request_timeout" if isinstance(error, asyncio.TimeoutError) else "request")
        record["error"] = f"{type(error).__name__}: {error}"
        record.setdefault("completed_at", time.perf_counter())
        raise
    finally:
        append_record(raw_path, record)


async def reset_caches(client, raw_path, namespace):
    result = await request_json(client, "POST", "/e004/reset", json={})
    # Both physical services must explicitly acknowledge successful reset.
    if result.get("p") is not True or result.get("d") is not True:
        raise InvalidRun("cache_reset", "P and D reset completion must both be true")
    append_record(raw_path, {"phase": "reset", "namespace": namespace,
                             "status": "completed", "result": result})


async def prepare(client, config, cases, namespace, raw_path):
    for case in cases:
        request_id = f"{namespace}-prepare-{case['case_id']}"
        body = {"model": config["served_model_name"],
                "prompt": case["prompt"][:case["prefix_tokens"]],
                "max_tokens": 1, "temperature": 0, "stream": False,
                "kvready": {"namespace": namespace, "id": request_id}}
        start = time.perf_counter()
        result = await request_json(client, "POST", "/e004/prepare", json=body)
        state = await snapshot(client, request_id)
        if not state_complete(state):
            raise InvalidRun("prepare_completion", f"Storage completion missing for {request_id}")
        append_record(raw_path, {"phase": "prepare", "case_id": case["case_id"],
                                "request_id": request_id, "namespace": namespace,
                                "status": "completed", "elapsed_s": time.perf_counter() - start,
                                "usage": result.get("usage"), "state": state})
    await reset_caches(client, raw_path, namespace)


async def run_batch(client, config, cases, namespace, mode, batch, interval_s, raw_path, phase):
    await prepare(client, config, cases, namespace, raw_path)
    start = time.perf_counter() + 0.1
    semaphore = asyncio.Semaphore(4)
    # There are only 40 queued descriptors. Times never depend on completion.
    tasks = [asyncio.create_task(stream_request(client, config, case, namespace, mode, phase,
                                                batch, start + index * interval_s, raw_path, semaphore))
             for index, case in enumerate(cases)]
    try:
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=600)
    except BaseException:
        for task in tasks:
            task.cancel()
        # Each child retains its failure in finally; finish cancellation before
        # propagating the original failure or timeout to the run report.
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    summary = await request_json(client, "GET", "/e004/summary", params={"namespace": namespace})
    append_record(raw_path, {"phase": "namespace_summary", "namespace": namespace,
                             "status": "completed", "summary": summary,
                             "cleanup": "not_supported_objects_retained"})


async def calibrate(client, config, manifest, prefix, raw_path, references):
    samples = []
    cases = (manifest["cases"][0], manifest["cases"][1])
    for family_index, case in enumerate(cases):
        for action in ("LOAD", "RECOMPUTE"):
            for repeat in range(5):
                namespace = f"{prefix}-cal-{family_index}-{action.lower()}-{repeat}"
                await prepare(client, config, [case], namespace, raw_path)
                request_id = f"{namespace}-produce"
                body = {"model": config["served_model_name"], "prompt": case["prompt"],
                        "max_tokens": 1, "temperature": 0, "stream": False,
                        "kvready": {"namespace": namespace, "id": request_id, "action": action}}
                begin = time.perf_counter()
                response = await request_json(client, "POST", "/e004/calibrate", json=body)
                production_elapsed_s = time.perf_counter() - begin
                state = await snapshot(client, request_id)
                if not state_complete(state):
                    raise InvalidRun("calibration_completion", "P production did not complete")
                summary = await request_json(client, "GET", "/e004/summary", params={"namespace": namespace})
                io_tasks = [t for t in summary.get("tasks", []) if t.get("request_id") == request_id]
                record = {"phase": "calibration", "status": "completed", "action": action,
                          "family": case["family"], "warmup": repeat < 2,
                          "elapsed_s": production_elapsed_s,
                          "input_tokens": len(case["prompt"]), "prefix_tokens": case["prefix_tokens"],
                          "namespace": namespace, "request_id": request_id,
                          "state": state, "io_tasks": io_tasks, "usage": response.get("usage")}
                samples.append(record)
                append_record(raw_path, record)
    timed_tasks = [t for r in samples if not r["warmup"] for t in r["io_tasks"]
                   if isinstance(t.get("nbytes"), (int, float)) and t["nbytes"] > 0
                   and isinstance(t.get("transfer_ms"), (int, float)) and t["transfer_ms"] > 0]
    if not timed_tasks:
        raise InvalidRun("calibration_io_timing", "Real completed storage byte/time samples are required")
    byte_rate = sum(t["nbytes"] for t in timed_tasks) / sum(t["transfer_ms"] for t in timed_tasks)
    token_rate = statistics.median(r["input_tokens"] / r["service_ttft_ms"] for r in references)
    errors = []
    for sample in samples:
        if sample["warmup"]:
            continue
        tokens = sample["input_tokens"] - (sample["prefix_tokens"] if sample["action"] == "LOAD" else 0)
        byte_count = sum(t.get("nbytes", 0) for t in sample["io_tasks"])
        estimate_ms = tokens / token_rate + byte_count / byte_rate
        errors.append(abs(sample["elapsed_s"] * 1000 - estimate_ms))
    coefficients = {"storage_bytes_per_ms": byte_rate, "prefill_tokens_per_ms": token_rate,
                    "margin_ms": max(1.0, statistics.median(errors))}
    calibrated = await request_json(client, "POST", "/e004/calibration", json={"calibration": coefficients})
    interval = max(0.01, statistics.median(r["elapsed_s"] for r in samples if not r["warmup"]) / 2)
    return {"samples": samples, "references": references, "policy": calibrated,
            "coefficients": coefficients, "arrival_interval_s": interval,
            "compute_rate_source": "reference_service_rate",
            "compute_rate_scope": "D no-cache service TTFT includes normal scheduling and first-token computation; not a pure device kernel rate",
            "storage_rate_source": "completed_storage_tasks_sum_bytes_over_sum_transfer_ms"}


async def check_reference(client, config, cases, prefix, raw_path):
    """Four normal D computations confirm the fixture answers before caching."""
    records = []
    for case in cases:
        body = {"model": config["served_model_name"], "prompt": case["prompt"],
                "max_tokens": 128, "temperature": 0, "stream": True,
                "stream_options": {"include_usage": True},
                "ignore_eos": False, "kvready": {"namespace": f"{prefix}-reference"}}
        begin = time.perf_counter()
        first = None
        parts = []
        usage = None
        done = False
        async with client.stream("POST", "/e004/reference", json=body) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                event = consume_sse(line)
                if not event:
                    continue
                if event.get("done"):
                    done = True
                if event.get("usage"):
                    usage = event["usage"]
                for choice in event.get("choices", []):
                    text = choice.get("text", "")
                    if text:
                        if first is None:
                            first = time.perf_counter()
                        parts.append(text)
        answer = "".join(parts)
        passed = answer_matches(answer, case["expected"])
        if (not done or first is None or first <= begin or not usage
                or usage.get("prompt_tokens") != len(case["prompt"])):
            raise InvalidRun("reference_stream", "A complete reference response with usage and first content is required")
        record = {"phase": "reference", "case_id": case["case_id"],
                  "status": "completed" if passed else "invalid", "input_tokens": len(case["prompt"]),
                  "answer_pass": passed, "text": answer, "usage": usage,
                  "service_ttft_ms": (first - begin) * 1000,
                  "failure_stage": None if passed else "reference_answer"}
        append_record(raw_path, record)
        records.append(record)
        if not passed:
            raise InvalidRun("reference_answer", "The reference model did not answer the fixture correctly")
    return records


def validate_config(config, manifest):
    for name in ("proxy_url", "model_path", "served_model_name", "storage_capacity_source"):
        if not isinstance(config.get(name), str) or not config[name].strip():
            raise InvalidRun("configuration", f"Set {name} in the yellow-zone run configuration")
    available = config.get("storage_available_bytes")
    required = required_storage_bytes(manifest["layout"])
    if not isinstance(available, int) or isinstance(available, bool) or available < required:
        raise InvalidRun("storage_capacity", f"A dedicated store needs at least {required} available bytes; declared={available}")
    if config.get("storage_dedicated_to_test") is not True:
        raise InvalidRun("storage_capacity", "Confirm a dedicated test store; no deletion API is assumed")
    if config.get("block_size", 128) != manifest["layout"]["block_size"]:
        raise InvalidRun("configuration", "Block-size mismatch")
    return required


async def execute(config, output):
    import httpx
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    raw_path = output / "requests.jsonl"
    run = {"status": "running", "model": config.get("served_model_name", "unknown"),
           "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
           "run_id": "e004-" + uuid.uuid4().hex[:12], "planned": 320,
           "installed_versions": installed_versions(),
           "cleanup": "not_supported_objects_retained"}
    save_json(output / "run.json", run)
    try:
        manifest = make_manifest(config["model_path"], config.get("block_size", 128))
        run["storage_required_bytes"] = validate_config(config, manifest)
        run["storage_available_bytes"] = config["storage_available_bytes"]
        run["storage_capacity_source"] = config["storage_capacity_source"]
        save_json(output / "workload.json", manifest)
        save_json(output / "run.json", run)
        headers = {}
        if os.environ.get("OPENAI_API_KEY"):
            headers["Authorization"] = "Bearer " + os.environ["OPENAI_API_KEY"]
        async with httpx.AsyncClient(base_url=config["proxy_url"].rstrip("/"),
                                     timeout=125, headers=headers, trust_env=False) as client:
            health = await request_json(client, "GET", "/healthcheck")
            run["health"] = health
            run["proxy_config"] = await request_json(client, "GET", "/e004/config")
            await reset_caches(client, raw_path, run["run_id"])
            references = await check_reference(client, config, manifest["correctness"], run["run_id"], raw_path)
            calibration = await calibrate(client, config, manifest, run["run_id"], raw_path, references)
            save_json(output / "calibration.json", calibration)
            interval = calibration["arrival_interval_s"]
            save_json(output / "arrival_schedule.json", [{"case_id": c["case_id"], "offset_s": i * interval}
                                                         for i, c in enumerate(manifest["cases"])])
            for mode in MODES:
                await run_batch(client, config, manifest["correctness"],
                                f"{run['run_id']}-qa-{mode}", mode, -1, interval, raw_path, "correctness")
            async def run_formal():
                for batch in (0, 1):
                    for mode in MODES if batch == 0 else tuple(reversed(MODES)):
                        await run_batch(client, config, manifest["cases"],
                                        f"{run['run_id']}-b{batch}-{mode}", mode, batch,
                                        interval, raw_path, "performance")
            await asyncio.wait_for(run_formal(), timeout=2400)
            run["status"] = "completed"
    except BaseException as error:
        run["status"] = "invalid"
        run["failure_stage"] = getattr(error, "stage", "execution")
        run["error"] = f"{type(error).__name__}: {error}"
        run["recovery"] = "Stop new admission. Keep services and in-flight buffers until backend completion is confirmed. Do not start another run with unknown task state."
        append_record(raw_path, {"phase": "run_failure", "status": "invalid",
                                "failure_stage": run["failure_stage"], "error": run["error"]})
    finally:
        save_json(output / "run.json", run)
    return write_report(output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    config = json.loads(args.config.read_text(encoding="utf-8"))
    result = asyncio.run(execute(config, args.output))
    return 0 if result["status"] == "valid" else 1


if __name__ == "__main__":
    sys.exit(main())
