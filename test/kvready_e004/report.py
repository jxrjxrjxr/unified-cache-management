"""Strict, small E004 result aggregation. Raw records remain in requests.jsonl."""

from __future__ import annotations

import argparse
import json
import math
import statistics
from collections import Counter
from pathlib import Path

MODES = ("late", "eager", "progress", "joint")
FAMILIES = ("long_prefix", "long_suffix")


def finite_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def json_safe(value):
    """Keep nonfinite values visible without emitting non-standard JSON numbers."""
    if isinstance(value, float) and not math.isfinite(value):
        return {"invalid_number": str(value)}
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [json_safe(v) for v in value]
    return value


def append_record(path, record):
    with Path(path).open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(json_safe(record), ensure_ascii=False, allow_nan=False) + "\n")
        stream.flush()


def validate_request(record):
    errors = []
    if record.get("status") != "completed":
        errors.append(record.get("failure_stage", "request_failed"))
    names = ("planned_at", "sent_at", "first_at", "completed_at")
    times = [record.get(name) for name in names]
    if not all(finite_number(t) for t in times):
        errors.append("nonfinite_or_missing_timestamp")
    elif not (times[0] <= times[1] <= times[2] <= times[3]) or times[3] <= times[1]:
        errors.append("invalid_event_order")
    if record.get("phase") == "performance" and record.get("output_tokens") != 128:
        errors.append("output_tokens_not_128")
    if not isinstance(record.get("text"), str) or not record["text"].strip():
        errors.append("empty_output")
    if record.get("phase") in ("performance", "correctness") and record.get("kv_complete") is not True:
        errors.append("kv_completion_not_confirmed")
    return errors


def latency(record):
    """Milliseconds. TPOT applies only when at least two output tokens exist."""
    tokens = record.get("output_tokens", 0)
    return {
        "ttft_ms": (record["first_at"] - record["planned_at"]) * 1000,
        "service_ttft_ms": (record["first_at"] - record["sent_at"]) * 1000,
        "queue_ms": (record["sent_at"] - record["planned_at"]) * 1000,
        "e2e_ms": (record["completed_at"] - record["planned_at"]) * 1000,
        "tpot_ms": ((record["completed_at"] - record["first_at"]) * 1000 / (tokens - 1)
                    if isinstance(tokens, int) and tokens > 1 else "not_applicable"),
    }


def describe(values):
    if not values or not all(finite_number(v) for v in values):
        return {"status": "invalid", "count": len(values)}
    return {"status": "valid", "count": len(values),
            "mean": statistics.mean(values), "median": statistics.median(values)}


def summarize(records, run):
    formal = [r for r in records if r.get("phase") == "performance"]
    failures = [r for r in records if r.get("status") in ("invalid", "failed")]
    index = {}
    issues = []
    for r in formal:
        key = (r.get("batch"), r.get("mode"), r.get("case_id"))
        if key in index:
            issues.append("duplicate_request_record")
        index[key] = r
        issues.extend(validate_request(r))
    expected = {(batch, mode, f"{family}-{i:02d}")
                for batch in (0, 1) for mode in MODES
                for family in FAMILIES for i in range(20)}
    if set(index) != expected:
        issues.append("incomplete_or_unexpected_pair_set")
    qa = [r for r in records if r.get("phase") == "correctness"]
    qa_keys = {(r.get("mode"), r.get("case_id")) for r in qa}
    qa_expected = {(mode, f"qa-{i}") for mode in MODES for i in range(4)}
    qa_pass = (len(qa) == 16 and qa_keys == qa_expected and
               all(r.get("answer_pass") is True and not validate_request(r) for r in qa))
    if not qa_pass:
        issues.append("correctness_incomplete_or_failed")
    if run.get("status") != "completed":
        issues.append(run.get("failure_stage", "run_not_completed"))
    if failures:
        issues.append(failures[0].get("failure_stage", "earlier_phase_failed"))
    valid = not issues
    result = {
        "status": "valid" if valid else "invalid",
        "planned": 320, "records": len(formal),
        "completed": sum(not validate_request(r) for r in formal),
        "correctness": "PASS" if qa_pass else "invalid",
        "first_failure": (failures[0].get("failure_stage") if failures else
                          run.get("failure_stage") or (issues[0] if issues else "NONE")),
        "issues": list(dict.fromkeys(issues)), "comparisons": {},
        "producer_actions": dict(Counter(r.get("p_action", "unknown") for r in formal)),
        "producer_actions_by_mode": {mode: dict(Counter(r.get("p_action", "unknown")
                                                        for r in formal if r.get("mode") == mode))
                                     for mode in MODES},
    }
    if not valid:
        return result
    for before, after in (("late", "eager"), ("eager", "progress"), ("progress", "joint")):
        name = f"{after}_vs_{before}"
        result["comparisons"][name] = {}
        for batch in (0, 1):
            section = {}
            for family in ("all",) + FAMILIES:
                ids = sorted(k[2] for k in expected if k[:2] == (batch, before)
                             and (family == "all" or k[2].startswith(family)))
                left = [latency(index[(batch, before, i)]) for i in ids]
                right = [latency(index[(batch, after, i)]) for i in ids]
                base = statistics.mean(x["ttft_ms"] for x in left)
                saved = [a["ttft_ms"] - b["ttft_ms"] for a, b in zip(left, right)]
                section[family] = {
                    "paired_ttft_saved_ms": describe(saved),
                    "mean_ttft_reduction_pct": 100 * statistics.mean(saved) / base if base > 0 else "invalid",
                    "before": {metric: describe([x[metric] for x in left]) for metric in left[0]},
                    "after": {metric: describe([x[metric] for x in right]) for metric in right[0]},
                }
            result["comparisons"][name][str(batch)] = section
    return result


def summary_lines(result, run, path_result=None):
    def comparison(name):
        data = result["comparisons"].get(name)
        if not data:
            return "invalid"
        return "/".join(f"{data[str(b)]['all']['mean_ttft_reduction_pct']:+.2f}%" for b in (0, 1))

    def gap():
        data = result["comparisons"].get("progress_vs_eager")
        if not data:
            return "invalid"
        return "/".join(f"{data[str(b)]['all']['after']['tpot_ms']['mean']-data[str(b)]['all']['before']['tpot_ms']['mean']:+.3f}ms" for b in (0, 1))

    actions = result["producer_actions_by_mode"]["joint"]
    path = path_result or {"status": "not_run", "reason": "run_transfer_demo_separately"}
    return [
        f"RUN commit={run.get('commit', 'unknown')} model={run.get('model', 'unknown')} "
        f"completed={result['completed']}/320 correctness={result['correctness']} "
        f"status={result['status']} first_failure={result['first_failure']}",
        f"PREFETCH eager_vs_late_ttft={comparison('eager_vs_late')} "
        f"progress_vs_eager_ttft={comparison('progress_vs_eager')} decode_gap_change={gap()}",
        f"RESTORE joint_vs_progress_ttft={comparison('joint_vs_progress')} "
        f"joint_P_load={actions.get('LOAD', 0)} joint_P_recompute={actions.get('RECOMPUTE', 0)}",
        "PATH " + " ".join(f"{k}={v}" for k, v in path.items() if not isinstance(v, (dict, list))),
    ]


def write_report(directory):
    directory = Path(directory)
    run = json.loads((directory / "run.json").read_text(encoding="utf-8"))
    raw = directory / "requests.jsonl"
    records = [json.loads(line) for line in raw.read_text(encoding="utf-8").splitlines() if line] if raw.exists() else []
    result = summarize(records, run)
    path_file = directory / "transfer.json"
    path = json.loads(path_file.read_text(encoding="utf-8")) if path_file.exists() else None
    (directory / "comparison.json").write_text(json.dumps(json_safe(result), indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    lines = summary_lines(result, run, path)
    (directory / "summary.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    raise SystemExit(0 if write_report(parser.parse_args().directory)["status"] == "valid" else 1)
