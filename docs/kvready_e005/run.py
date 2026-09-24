"""Start the fixed E005 services, run once, and stop only these child groups."""
import argparse
import importlib.metadata
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
import urllib.request

from launch import build
from prepare import git, BASE, validate


def get(url):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(url, timeout=2) as r:
        return r.status, r.read()


def free_ports(c):
    for key in ("p_port", "d_port", "proxy_port"):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", c[key]))


def stop_children(children):
    for role, process in reversed(children):
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline and any(p.poll() is None for _, p in children):
        time.sleep(0.2)
    for role, process in reversed(children):
        # A vLLM parent can exit before its workers; terminate residual own group.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=10)


def summary(c, result, cleanup):
    lines = ["RUN status={} stage={} completed={}/18 cleanup={} commit={}".format(
        result["status"], result["stage"], result.get("completed", 0), cleanup, c["commit"][:12])]
    batches = result.get("batches", {})
    parts = []
    for name in ("c1", "c2"):
        if name in batches and result["status"] == "valid":
            b = batches[name]
            parts.append("{}_ttft_ms={:.2f} {}_tpot_ms={:.2f} {}_out_tok_s={:.2f}".format(
                name, b["ttft_ms"], name, b["tpot_ms"], name, b["output_tokens_per_s"]))
    lines.append("PERF " + (" ".join(parts) if parts else "status=not_reported"))
    pd = result.get("pd", {})
    lines.append("PD p_save_blocks={} d_load_blocks={} p={} d={}".format(
        sum(pd.get("p", {}).values()) if pd else "unknown",
        sum(pd.get("d", {}).values()) if pd else "unknown", c["p_devices"], c["d_devices"]))
    if result.get("error"):
        lines.append("ERROR " + result["error"].replace("\n", " ")[:220])
    return "\n".join(lines) + "\n"


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", required=True)
    a = ap.parse_args()
    c = json.loads(Path(a.config).read_text())
    session, root = Path(c["session"]), Path(c["root"])
    validate(c)
    if os.name != "posix":
        raise SystemExit("Run E005 inside the existing Linux container")
    # Exclusive marker: every new attempt gets a fresh cache and metrics directory.
    (session / "run.started").open("x").close()
    children, logs = [], []
    result = {"status": "invalid", "stage": "preflight", "completed": 0}
    cleanup = "complete"
    try:
        free_ports(c)
        if git(root, "rev-parse", "HEAD") != c["commit"]:
            raise ValueError("E005 commit changed after preparation")
        if git(root, "diff", BASE, "--", "ucm", "kv_semantics", "setup.py", "CMakeLists.txt"):
            raise ValueError("Production source differs from baseline")
        evidence = {"commit": c["commit"], "python": sys.executable,
                    "packages": {}, "commands": {}, "role_environment": {}, "pids": {}}
        for name in ("vllm", "vllm-ascend", "torch", "torch-npu", "transformers"):
            try:
                evidence["packages"][name] = importlib.metadata.version(name)
            except importlib.metadata.PackageNotFoundError:
                evidence["packages"][name] = "unavailable"
        probe_env = build(c, "p")[1]
        # Only import; no device initialization or model loading in this preflight.
        probe = ("import pathlib,ucm; "
                 "from ucm.store.pipeline import ucmpipelinestore; "
                 "from ucm.shared.metrics import ucmmetrics; "
                 "assert pathlib.Path(ucm.__file__).resolve().parent == pathlib.Path("
                 + repr(str(root / "ucm")) + "); print(ucm.__file__)")
        check = subprocess.run([c["python"], "-c", probe], cwd=root, env=probe_env,
                               capture_output=True, text=True, timeout=60)
        (session / "import.log").write_text(check.stdout + check.stderr, encoding="utf-8")
        if check.returncode:
            raise ValueError("E005 import preflight failed; see import.log")
        result["stage"] = "startup"
        for role in ("p", "d", "proxy"):
            command, env = build(c, role)
            evidence["commands"][role] = command
            evidence["role_environment"][role] = {
                k: v for k, v in env.items() if k in {
                    "ASCEND_RT_VISIBLE_DEVICES", "VLLM_PORT", "MASTER_PORT", "VLLM_HOST_IP",
                    "PYTHONPATH", "PROMETHEUS_MULTIPROC_DIR"} or k.startswith("HCCL_")}
            log = (session / (role + ".log")).open("x", encoding="utf-8")
            logs.append(log)
            process = subprocess.Popen(command, cwd=root, env=env, stdout=log,
                                       stderr=subprocess.STDOUT, start_new_session=True)
            children.append((role, process))
            evidence["pids"][role] = process.pid
            (session / "runtime.json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
        (session / "runtime.json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
        deadline = time.monotonic() + 600
        urls = ["http://127.0.0.1:" + str(c[r + "_port"]) + "/health" for r in ("p", "d")]
        urls.append("http://127.0.0.1:" + str(c["proxy_port"]) + "/healthcheck")
        while True:
            dead = [r for r, p in children if p.poll() is not None]
            if dead:
                raise ValueError("Service exited during startup: " + ",".join(dead))
            ready = True
            for url in urls:
                try:
                    ready &= get(url)[0] == 200
                except Exception:
                    ready = False
            if ready:
                break
            if time.monotonic() >= deadline:
                raise ValueError("Startup deadline exceeded (600 seconds)")
            time.sleep(2)
        print("SERVICES_READY p=" + c["p_devices"] + " d=" + c["d_devices"], flush=True)
        result["stage"] = "benchmark"
        log = (session / "benchmark.log").open("x", encoding="utf-8")
        logs.append(log)
        process = subprocess.Popen(
            [c["python"], str(root / "test/kvready_e005/benchmark.py"), "--config", a.config],
            cwd=root, env=probe_env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        children.append(("benchmark", process))
        evidence["pids"]["benchmark"] = process.pid
        (session / "runtime.json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
        deadline = time.monotonic() + 600
        while process.poll() is None:
            if any(p.poll() is not None for r, p in children if r != "benchmark"):
                raise ValueError("Service exited during benchmark")
            if time.monotonic() >= deadline:
                raise ValueError("Benchmark deadline exceeded (600 seconds)")
            time.sleep(0.5)
        result_file = session / "results/result.json"
        if result_file.exists():
            result = json.loads(result_file.read_text())
        else:
            raise ValueError("Benchmark did not produce result.json; see benchmark.log")
        if process.returncode and result["status"] == "valid":
            raise ValueError("Benchmark exit code disagrees with result")
    except (Exception, KeyboardInterrupt) as exc:
        result["status"] = "invalid"
        result["error"] = type(exc).__name__ + ": " + str(exc)
    finally:
        try:
            stop_children(children)
        except Exception as exc:
            cleanup = "incomplete"
            result["status"] = "invalid"
            result["cleanup_error"] = str(exc)
        for log in logs:
            log.close()
        # Surface the known HCCL code in the small feedback without rewriting logs.
        if result["status"] != "valid":
            for role in ("p", "d"):
                path = session / (role + ".log")
                if path.exists() and "EI0014" in path.read_text(errors="replace"):
                    result["error"] = result.get("error", "") + " " + role + ":EI0014"
        raw_path = session / "results/requests.jsonl"
        if raw_path.exists():
            completed = 0
            for line in raw_path.read_text(encoding="utf-8").splitlines():
                try:
                    completed += json.loads(line).get("status") == "valid"
                except json.JSONDecodeError:
                    pass  # A terminated write stays in the original evidence file.
            result["completed"] = completed
        result["cleanup"] = cleanup
        result["pids"] = {role: process.pid for role, process in children}
        (session / "outcome.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
        text = summary(c, result, cleanup)
        (session / "summary.txt").write_text(text, encoding="utf-8")
        print(text, end="", flush=True)
    return 0 if result["status"] == "valid" and cleanup == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
