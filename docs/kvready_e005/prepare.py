"""Prepare an isolated E005 session using the original UCM PD example."""
import argparse
import json
import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

BASE = "3f748a5959614af998ec31c18e0e44926f5ae368"
ROOT = Path(__file__).resolve().parents[2]


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


def devices(value):
    parts = value.split(",")
    if len(parts) != 2 or any(not p.isdecimal() for p in parts):
        raise ValueError("Each role needs two physical NPU IDs, for example 3,5")
    ids = [int(p) for p in parts]
    if len(set(ids)) != 2:
        raise ValueError("Physical NPU IDs must differ")
    return ",".join(map(str, ids))


def validate(c):
    p, d = devices(c["p_devices"]), devices(c["d_devices"])
    if set(p.split(",")) & set(d.split(",")):
        raise ValueError("P and D physical devices must be disjoint on this single host")
    ports = [c[k] for k in ("p_port", "d_port", "proxy_port", "p_dist_port", "d_dist_port")]
    if any(type(p) is not int or not 1024 <= p <= 65000 for p in ports):
        raise ValueError("Ports must be integers in 1024..65000")
    # vLLM may allocate successive internal ports; reserve 100 per role.
    groups = [{p} for p in ports[:3]] + [
        set(range(c[k], c[k] + 100)) for k in ("p_dist_port", "d_dist_port")]
    if any(a & b for i, a in enumerate(groups) for b in groups[i + 1:]):
        raise ValueError("Service ports and internal port ranges must be disjoint")


def reuse_native(source, target=ROOT):
    """Link only built libraries after comparing native sources and build inputs."""
    source, target = Path(source).resolve(), Path(target).resolve()
    if source == target:
        raise ValueError("E004 and E005 directories must differ")
    tracked = git(source, "ls-files").splitlines()
    native = [p for p in tracked if
              Path(p).suffix.lower() in {".c", ".cc", ".cpp", ".h", ".hpp", ".cu", ".cuh", ".cmake"}
              or Path(p).name in {"CMakeLists.txt", "setup.py", "pyproject.toml"}]
    changed = git(source, "diff", BASE, "--", *native)
    if changed:
        raise ValueError("E004 native source/build inputs differ from E005 baseline")
    required = ["ucm/store/cache/libcachestore.so", "ucm/store/posix/libposixstore.so"]
    libs = sorted(p for p in (source / "ucm").rglob("*") if p.is_file() and ".so" in p.name)
    if not libs or any(not (source / p).is_file() for p in required):
        raise ValueError("Existing E004 Cache/Posix native libraries are required")
    links = []
    for lib in libs:
        rel = lib.relative_to(source)
        dst = target / rel
        if dst.exists() or dst.is_symlink():
            if dst.resolve() != lib.resolve():
                raise ValueError("Existing E005 library has a different target: " + str(dst))
        else:
            dst.parent.mkdir(parents=True, exist_ok=True)
            dst.symlink_to(lib)
        links.append(str(rel))
    return {"source": str(source), "source_commit": git(source, "rev-parse", "HEAD"),
            "libraries": links}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--e004", required=True, type=Path)
    ap.add_argument("--session", required=True, type=Path)
    ap.add_argument("--model", default="/home/models/Qwen3-4B")
    ap.add_argument("--p-devices", default="3,5")
    ap.add_argument("--d-devices", default="6,7")
    for key, default in [("p-port", 8100), ("d-port", 8200), ("proxy-port", 8300),
                         ("p-dist-port", 29500), ("d-dist-port", 29600)]:
        ap.add_argument("--" + key, type=int, default=default)
    for role in ("p", "d"):
        for var in ("hccl-if-base-port", "hccl-host-socket-port-range", "hccl-npu-socket-port-range"):
            ap.add_argument("--" + role + "-" + var, default="",
                            help="Empty inherits container default; explicit values pass through unchanged")
    a = ap.parse_args()
    c = vars(a).copy()
    c["e004"], c["session"] = str(a.e004.resolve()), str(a.session.resolve())
    c["model"] = str(Path(a.model).resolve())
    c["root"], c["python"] = str(ROOT), sys.executable
    validate(c)
    model = json.loads((Path(c["model"]) / "config.json").read_text())
    if model.get("model_type") != "qwen3":
        raise ValueError("The bounded E005 workload is prepared for the existing Qwen3-4B model")
    if git(ROOT, "diff", "--name-only", BASE, "--", "ucm", "kv_semantics", "setup.py", "CMakeLists.txt"):
        raise ValueError("Production code must match the E005 baseline")
    session = Path(c["session"])
    if session.exists():
        raise ValueError("Use a fresh session directory; existing measurements are preserved")
    # At most 18 * 1152 tokens, BF16 KV, plus both endpoints and 2x storage slack.
    token_bytes = (2 * 2 * model["num_hidden_layers"] * model["num_key_value_heads"]
                   * model.get("head_dim", model["hidden_size"] // model["num_attention_heads"]))
    disk_budget = max(8 * 1024**3, token_bytes * 18 * 1152 * 4)
    if shutil.disk_usage(session.parent).free < disk_budget:
        raise ValueError("Insufficient free disk space for the fixed workload")
    c["native_reuse"] = reuse_native(a.e004)
    c["commit"] = git(ROOT, "rev-parse", "HEAD")
    c["disk_budget_bytes"] = disk_budget
    session.mkdir()
    (session / "cache").mkdir()
    import yaml
    metrics_base = yaml.safe_load((ROOT / "examples/metrics/metrics_configs.yaml").read_text())
    for role in ("p", "d"):
        mp = session / ("metrics-" + role)
        mp.mkdir()
        metrics = dict(metrics_base, log_interval=1, multiproc_dir=str(mp))
        metric_file = session / ("metrics-" + role + ".yaml")
        metric_file.write_text(yaml.safe_dump(metrics), encoding="utf-8")
        config = {
            "ucm_connectors": [{"ucm_connector_name": "UcmPipelineStore",
                                "ucm_connector_config": {
                                    "store_pipeline": "Cache|Posix",
                                    "storage_backends": str(session / "cache"),
                                    "cache_buffer_capacity_gb": 1}}],
            "enable_event_sync": True, "use_layerwise": False,
            "persist_token_threshold": 0, "metrics_config_path": str(metric_file)}
        (session / ("ucm-" + role + ".yaml")).write_text(yaml.safe_dump(config), encoding="utf-8")
    (session / "config.json").write_text(json.dumps(c, indent=2), encoding="utf-8")
    (session / "env.sh").write_text(
        "export E005_CONFIG=" + shlex.quote(str(session / "config.json")) + "\n"
        "export E005_PY=" + shlex.quote(sys.executable) + "\n"
        "export E005_ROOT=" + shlex.quote(str(ROOT)) + "\n", encoding="utf-8")
    print("CONFIG_OK p=" + c["p_devices"] + " d=" + c["d_devices"] +
          " ports=" + ",".join(str(c[k]) for k in ("p_port", "d_port", "proxy_port")))
    print("CONFIG_FILE=" + str(session / "config.json"))


if __name__ == "__main__":
    main()
