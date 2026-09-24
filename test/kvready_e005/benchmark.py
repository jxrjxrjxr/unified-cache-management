"""One fixed, small PD benchmark; standard-library HTTP, local tokenizer only."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
from pathlib import Path
import re
import statistics
import time
import urllib.request
import uuid

INPUT = 1024
OUTPUT = 32


def fetch(url, timeout=5):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(url, timeout=timeout) as r:
        return r.read().decode()


def prompts(tokenizer, count=18):
    nonce = uuid.uuid4().hex
    filler = tokenizer.encode(
        "A library stores books and journals. Readers compare ideas and write clear notes. " * 200,
        add_special_tokens=False)
    result = []
    for i in range(count):
        # Unique early marker separates the first cache block of every request.
        marker = "E005 record " + nonce + " sample " + str(i) + ". "
        sentinel = "E005_CONTEXT_PLACEHOLDER"
        rendered = tokenizer.apply_chat_template(
            [{"role": "user", "content": marker + sentinel +
              "\nSummarize the library activities in a few English sentences."}],
            tokenize=False, add_generation_prompt=True, enable_thinking=False)
        left, right = rendered.split(sentinel)
        head = tokenizer.encode(left, add_special_tokens=False)
        tail = tokenizer.encode(right, add_special_tokens=False)
        n = INPUT - len(head) - len(tail)
        if n <= 0 or n > len(filler):
            raise ValueError("Tokenizer template exceeds bounded prompt budget")
        result.append(head + filler[:n] + tail)
    if len({tuple(p[:128]) for p in result}) != count:
        raise ValueError("Prompt first blocks must be independent")
    return result


def parse_stream(lines, started, now=time.perf_counter):
    first = last = None
    usage = None
    finished = None
    text = []
    done = False
    for raw in lines:
        line = raw.decode("utf-8").strip()
        if not line.startswith("data:"):
            continue
        payload = line[5:].strip()
        if payload == "[DONE]":
            done = True
            break
        item = json.loads(payload)
        if item.get("error"):
            raise ValueError("Server error: " + str(item["error"]))
        if item.get("usage"):
            usage = item["usage"]
        for choice in item.get("choices", []):
            fragment = choice.get("text", "")
            if fragment:
                stamp = now()
                if first is None:
                    first = stamp
                last = stamp
                text.append(fragment)
            if choice.get("finish_reason") is not None:
                finished = choice["finish_reason"]
    if not done or first is None or not "".join(text).strip() or not usage:
        raise ValueError("Incomplete SSE response, empty output, or missing usage")
    if usage.get("prompt_tokens") != INPUT or usage.get("completion_tokens") != OUTPUT or finished != "length":
        raise ValueError("Response token counts/finish reason differ from fixed workload: " + str(usage))
    values = {"ttft_ms": (first - started) * 1000,
              "tpot_ms": (last - first) * 1000 / (OUTPUT - 1),
              "e2e_ms": (now() - started) * 1000}
    if any(not math.isfinite(v) or v < 0 for v in values.values()) or values["e2e_ms"] <= 0:
        raise ValueError("Non-finite or invalid latency")
    return dict(values, text="".join(text), usage=usage, finish_reason=finished)


def request(url, tokens, case):
    payload = {"model": "e005", "prompt": tokens, "max_tokens": OUTPUT,
               "temperature": 0, "ignore_eos": True, "stream": True,
               "stream_options": {"include_usage": True}}
    record = {"case": case, "status": "invalid"}
    started = time.perf_counter()
    try:
        req = urllib.request.Request(url, json.dumps(payload).encode(), {"Content-Type": "application/json"})
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(req, timeout=120) as response:
            record.update(parse_stream(response, started))
        record["status"] = "valid"
    except Exception as exc:
        record["error"] = type(exc).__name__ + ": " + str(exc)
    return record


def metric_workers(text, name):
    output = {}
    pattern = re.compile(r"^" + re.escape("ucm:" + name + "_sum") + r'\{([^}]*)\}\s+(\S+)')
    for line in text.splitlines():
        match = pattern.match(line)
        if not match:
            continue
        worker = re.search(r'worker_id="([^"]+)"', match[1])
        value = float(match[2])
        if worker and math.isfinite(value):
            output[worker[1]] = output.get(worker[1], 0) + value
    return output


def pd_evidence(before, after, minimum):
    deltas = {w: v - before.get(w, 0) for w, v in after.items()}
    workers = {w: v for w, v in deltas.items() if v > 0}
    return len(workers) == 2 and all(v >= minimum for v in workers.values()), workers


def summarize(records, elapsed):
    if not records or any(r["status"] != "valid" for r in records):
        raise ValueError("A failed response invalidates its batch")
    values = {key: statistics.mean(r[key] for r in records)
              for key in ("ttft_ms", "tpot_ms", "e2e_ms")}
    values["output_tokens_per_s"] = len(records) * OUTPUT / elapsed
    if any(not math.isfinite(v) or v < 0 for v in values.values()):
        raise ValueError("Invalid batch statistic")
    return dict(values, completed=len(records), wall_seconds=elapsed)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", required=True)
    a = ap.parse_args()
    c = json.loads(Path(a.config).read_text())
    session = Path(c["session"])
    output = session / "results"
    output.mkdir()  # One run per session; preserve all previous records.
    result = {"status": "invalid", "stage": "workload", "completed": 0, "batches": {}}
    records = []
    try:
        from transformers import AutoTokenizer
        tokenizer = AutoTokenizer.from_pretrained(c["model"], local_files_only=True)
        inputs = prompts(tokenizer)
        (output / "workload.json").write_text(json.dumps(inputs), encoding="utf-8")
        urls = {r: "http://127.0.0.1:" + str(c[r + "_port"]) + "/metrics" for r in ("p", "d")}
        before = {}
        for role in urls:
            before[role] = fetch(urls[role])
            (output / (role + "-before.prom")).write_text(before[role], encoding="utf-8")

        def evidence(count, label):
            deadline = time.monotonic() + 10
            while True:
                flags, details = [], {}
                for role, kind in (("p", "save_blocks_num"), ("d", "load_blocks_num")):
                    raw = fetch(urls[role])
                    (output / (role + "-" + label + ".prom")).write_text(raw, encoding="utf-8")
                    # Direct connector waits for dump/load before recording these metrics.
                    minimum = count * (INPUT // 128)
                    ok, delta = pd_evidence(metric_workers(before[role], kind),
                                            metric_workers(raw, kind), minimum)
                    flags.append(ok)
                    details[role] = delta
                if all(flags):
                    return details
                if time.monotonic() >= deadline:
                    raise ValueError("PD save/load evidence missing on one or more TP ranks: " + str(details))
                time.sleep(1)

        offset = 0
        with (output / "requests.jsonl").open("x", encoding="utf-8") as raw:
            for name, count, concurrency in (("warmup", 2, 1), ("c1", 8, 1), ("c2", 8, 2)):
                result["stage"] = name
                batch = []
                start = time.perf_counter()
                with ThreadPoolExecutor(max_workers=concurrency) as pool:
                    for wave in range(0, count, concurrency):
                        futures = [pool.submit(request,
                                   "http://127.0.0.1:" + str(c["proxy_port"]) + "/v1/completions",
                                   inputs[offset + i], name + "-" + str(i))
                                   for i in range(wave, min(wave + concurrency, count))]
                        wave_records = [f.result() for f in futures]
                        for record in wave_records:
                            raw.write(json.dumps(record, ensure_ascii=False) + "\n")
                            raw.flush()
                            records.append(record)
                            batch.append(record)
                        if any(r["status"] != "valid" for r in wave_records):
                            raise ValueError(next(r["error"] for r in wave_records if r["status"] != "valid"))
                result["batches"][name] = summarize(batch, time.perf_counter() - start)
                offset += count
                result["stage"] = "pd_evidence_" + name
                result["pd"] = evidence(offset, name)
        result["status"], result["stage"] = "valid", "complete"
    except Exception as exc:
        result["error"] = type(exc).__name__ + ": " + str(exc)
    result["completed"] = sum(r["status"] == "valid" for r in records)
    (output / "result.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    return 0 if result["status"] == "valid" else 1


if __name__ == "__main__":
    raise SystemExit(main())
