"""Pure ordering and one-shot restore decisions for the E004 experiment."""

import math

MODES = frozenset(("late", "eager", "progress", "joint"))
KINDS = frozenset(("p_load", "p_save", "d_prefix", "d_new"))


def validate_calibration(values):
    """An empty calibration deliberately selects LOAD until measured."""
    if not values:
        return {}
    required = ("storage_bytes_per_ms", "prefill_tokens_per_ms", "margin_ms")
    missing = [key for key in required if key not in values]
    if missing:
        raise ValueError("missing calibration: " + ", ".join(missing))
    result = {key: float(values[key]) for key in required}
    for key, value in result.items():
        if not math.isfinite(value) or value < 0 or (key != "margin_ms" and value == 0):
            raise ValueError("invalid calibration: " + key)
    return result


def restore_plan(mode, calibration, *, load_bytes, recompute_tokens, new_tokens,
                 d_remaining_bytes=0, p_remaining_ms=0, d_remaining_ms=0, tail_ms=0,
                 new_transfer_ms=0):
    """Compare the P/D joint finish; shared read service is charged to LOAD."""
    if mode not in MODES:
        raise ValueError("unknown mode")
    if not calibration:
        return {"action": "LOAD", "reason": "missing_calibration"}
    bw = calibration["storage_bytes_per_ms"]
    rate = calibration["prefill_tokens_per_ms"]
    p_load = load_bytes / bw + new_tokens / rate + p_remaining_ms
    p_recompute = (new_tokens + recompute_tokens) / rate + p_remaining_ms
    d_read = max(d_remaining_bytes / bw, d_remaining_ms)
    # Both devices share the same measured storage service budget. Saved/new
    # ranges and the common D tail are held equal between the two decisions.
    load_finish = max(p_load + new_transfer_ms, d_read + load_bytes / bw) + tail_ms
    recompute_finish = max(p_recompute + new_transfer_ms, d_read) + tail_ms
    choose_recompute = (mode == "joint" and
                        recompute_finish + calibration["margin_ms"] < load_finish)
    action = "RECOMPUTE" if choose_recompute else "LOAD"
    return {
        "action": action,
        "reason": ("joint_finish_with_margin" if choose_recompute else
                   "fixed_load_baseline" if mode != "joint" else "load_within_margin"),
        "load_finish_ms": load_finish,
        "recompute_finish_ms": recompute_finish,
        "p_remaining_ms": p_recompute if choose_recompute else p_load,
        "d_remaining_ms": d_read,
        "new_transfer_ms": new_transfer_ms,
    }


def task_order(task, request, now_ms, calibration):
    """Only the D-prefix tie-breaker differs between eager and progress."""
    kind = task["kind"]
    priority = 0 if kind in ("p_save", "d_new") or (
        kind == "p_load" and task.get("urgent", True)) else 1 if kind == "p_load" else 2
    if kind == "d_prefix" and request["mode"] in ("progress", "joint"):
        bw = calibration.get("storage_bytes_per_ms")
        remaining_ms = request["prefix_remaining_bytes"] / bw if bw else 0.0
        deadline = request.get("producer_deadline_ms", request["created_ms"])
        slack = deadline - now_ms - remaining_ms - calibration.get("margin_ms", 0.0)
        return priority, slack, request["sequence"], task["sequence"]
    return priority, request["sequence"], task["layer"], task["sequence"]
