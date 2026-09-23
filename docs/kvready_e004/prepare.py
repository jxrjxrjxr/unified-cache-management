"""Create the new-host Qwen3-4B configuration from the recorded ASU recipe."""

from __future__ import annotations

import argparse
import json
import shlex
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "test/kvready_e004"))
from workload import model_layout, required_storage_bytes

GIB = 1024**3


def inmem_budget(proc=Path("/proc"), cgroup=Path("/sys/fs/cgroup")):
    """Admission estimate for a fresh inmem server, not a reserved storage quota."""
    fields = dict(line.split(":", 1) for line in (proc / "meminfo").read_text().splitlines())
    host_free = int(fields["MemAvailable"].split()[0]) * 1024
    candidates = [(host_free, "MemAvailable")]
    observed = False
    for entry in (proc / "self/cgroup").read_text().splitlines():
        _, controllers, relative = entry.split(":", 2)
        if controllers == "":
            root, limit_file, used_file = cgroup, "memory.max", "memory.current"
        elif "memory" in controllers.split(","):
            root, limit_file, used_file = cgroup / "memory", "memory.limit_in_bytes", "memory.usage_in_bytes"
        else:
            continue
        node = root / relative.lstrip("/")
        if not (node / limit_file).is_file():
            node = root  # Docker's private cgroup namespace exposes its own root.
        while node.is_relative_to(root):
            limit_path = node / limit_file
            if limit_path.is_file():
                value = limit_path.read_text().strip()
                observed = True
                if value != "max":
                    free = max(0, int(value) - int((node / used_file).read_text()))
                    candidates.append((free, str(limit_path)))
            if node == root:
                break
            node = node.parent
    if not observed:
        raise ValueError("Cannot read the container memory limit; preparation stopped before ASU startup")
    effective = min(value for value, _ in candidates)
    reserve = max(64 * GIB, effective // 4)
    budget = max(0, effective - reserve)
    evidence = {"basis": "fresh ASU --backing inmem; host/container free memory",
                "observations": [{"source": name, "free_bytes": value} for value, name in candidates],
                "reserve_bytes": reserve, "admission_bytes": budget,
                "reservation": "snapshot only; host RAM is not reserved"}
    return budget, evidence


def prepare(session, model=Path("/home/models/Qwen3-4B"), *, memory=None):
    import yaml

    session, model = session.resolve(), model.resolve()
    if session.exists():
        raise ValueError(f"Keep existing results; reuse {session}/env.sh, or choose --session with a new directory")
    model_config = json.loads((model / "config.json").read_text(encoding="utf-8"))
    if model_config.get("model_type") != "qwen3":
        raise ValueError("The new-host recipe selects the existing /home/models/Qwen3-4B")
    layout = model_layout(model, 128)
    requests = required_storage_bytes(layout)
    demo = 6144 * layout["kv_bytes_per_token"]
    budget, evidence = memory if memory is not None else inmem_budget()
    if budget < requests + demo:
        raise ValueError(f"Inmem budget {budget / GIB:.2f} GiB; fixed package needs "
                         f"{(requests + demo) / GIB:.2f} GiB. No requests or ASU service started")
    config = json.loads((REPO / "docs/kvready_e004/run-config.example.json").read_text(encoding="utf-8"))
    asu = yaml.safe_load((REPO / "examples/ucm_config_asu.yaml").read_text(encoding="utf-8"))
    for entry in asu["ucm_connectors"]:
        backend = entry["ucm_connector_config"]
        if backend.get("store_pipeline") != "ASU":
            raise ValueError("Expected the repository ASU configuration")
        backend.update(asu_ips=["127.0.0.1"], asu_ports=[19003])
    config.update(model_path=str(model), asu_config_path=str(session / "asu.yaml"),
                  runtime_dir=str(session / "runtime"), storage_available_bytes=budget,
                  storage_capacity_source=json.dumps(evidence, ensure_ascii=False),
                  storage_dedicated_to_test=True)
    session.mkdir(parents=True)
    (session / "asu.yaml").write_text(yaml.safe_dump(asu, sort_keys=False), encoding="utf-8")
    (session / "run-config.json").write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    (session / "memory-budget.json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    env = {"E004_REPO": str(REPO), "E004_SESSION": str(session), "E004_PY": sys.executable,
           "E004_CONFIG": str(session / "run-config.json"), "E004_RESULTS": str(session / "results"),
           "E004_ASU": str(session / "asu.yaml"), "ASCEND_RT_VISIBLE_DEVICES": "4,5,6,7",
           "NO_PROXY": "127.0.0.1,localhost", "no_proxy": "127.0.0.1,localhost",
           "PYTHONDONTWRITEBYTECODE": "1", "VLLM_SERVER_DEV_MODE": "1", "ENABLE_UCM_PATCH": "1",
           "VLLM_ENGINE_READY_TIMEOUT_S": "6000", "OMP_PROC_BIND": "false", "OMP_NUM_THREADS": "10",
           "PYTORCH_NPU_ALLOC_CONF": "expandable_segments:True", "HCCL_BUFFSIZE": "1024",
           "VLLM_ASCEND_ENABLE_FLASHCOMM1": "0", "TASK_QUEUE_ENABLE": "1", "UMC_ASU_OOB_MODE": "tcp"}
    script = "\n".join(f"export {key}={shlex.quote(value)}" for key, value in env.items())
    script += '\nexport PYTHONPATH="$E004_REPO${PYTHONPATH:+:$PYTHONPATH}"\ncd "$E004_REPO"\n'
    (session / "env.sh").write_text(script, encoding="utf-8")
    return requests, demo, budget


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session", type=Path, default=Path("/home/j00977581/kvready-e004-01"))
    args = parser.parse_args()
    try:
        requests, demo, budget = prepare(args.session)
    except (OSError, ValueError, KeyError) as error:
        print(f"RUN status=not_run first_failure=configuration detail={error}")
        return 1
    print(f"CONFIG_OK model=Qwen3-4B request_gib={requests / GIB:.3f} "
          f"demo_gib={demo / GIB:.3f} inmem_budget_gib={budget / GIB:.3f}")
    print(f"Environment: {args.session}/env.sh")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
