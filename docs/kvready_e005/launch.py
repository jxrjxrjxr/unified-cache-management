"""Launch stock UCM services with explicit role-specific devices and ports."""
import argparse
import json
import os
import shlex
from pathlib import Path

from prepare import validate, devices


def build(c, role, inherited=None):
    validate(c)
    env = dict(os.environ if inherited is None else inherited)
    root, session = c["root"], c["session"]
    env["PYTHONPATH"] = root + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        env.pop(key, None)
    env["NO_PROXY"] = env["no_proxy"] = "127.0.0.1,localhost"
    env["PYTHONUNBUFFERED"] = "1"
    cmd = [c["python"], "-m"]
    if role == "proxy":
        return cmd + ["ucm.pd.toy_proxy_server", "--pd-disaggregation",
                      "--host", "127.0.0.1", "--port", str(c["proxy_port"]),
                      "--prefiller-host", "127.0.0.1", "--prefiller-port", str(c["p_port"]),
                      "--decoder-host", "127.0.0.1", "--decoder-port", str(c["d_port"])], env
    # Configuration is the single source of device selection, printed before exec.
    env["ASCEND_RT_VISIBLE_DEVICES"] = devices(c[role + "_devices"])
    env["VLLM_HOST_IP"] = "127.0.0.1"
    env["VLLM_PORT"] = str(c[role + "_dist_port"])
    env["MASTER_PORT"] = str(c[role + "_dist_port"])
    env["PROMETHEUS_MULTIPROC_DIR"] = str(Path(session) / ("metrics-" + role))
    for variable in ("HCCL_IF_BASE_PORT", "HCCL_HOST_SOCKET_PORT_RANGE", "HCCL_NPU_SOCKET_PORT_RANGE"):
        explicit = c.get(role + "_" + variable.lower(), "")
        if explicit:
            env[variable] = explicit
    transfer = {"kv_connector": "UCMConnector", "kv_role": "kv_both",
                "kv_connector_module_path": "ucm.integration.vllm.ucm_connector",
                "kv_connector_extra_config": {"UCM_CONFIG_FILE": str(Path(session) / ("ucm-" + role + ".yaml"))}}
    return cmd + ["vllm.entrypoints.cli.main", "serve", c["model"],
                  "--served-model-name", "e005", "--host", "127.0.0.1",
                  "--port", str(c[role + "_port"]), "--tensor-parallel-size", "2",
                  "--distributed-executor-backend", "mp", "--dtype", "bfloat16",
                  "--enforce-eager", "--no-enable-prefix-caching", "--block-size", "128",
                  "--max-model-len", "4096", "--max-num-batched-tokens", "4096",
                  "--max-num-seqs", "2", "--gpu-memory-utilization", "0.60",
                  "--kv-transfer-config", json.dumps(transfer)], env


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("role", choices=("p", "d", "proxy"))
    ap.add_argument("--config", required=True)
    ap.add_argument("--show", action="store_true")
    a = ap.parse_args()
    c = json.loads(Path(a.config).read_text())
    cmd, env = build(c, a.role)
    shown = {k: env[k] for k in ("ASCEND_RT_VISIBLE_DEVICES", "VLLM_PORT",
                                "MASTER_PORT", "HCCL_IF_BASE_PORT",
                                "HCCL_HOST_SOCKET_PORT_RANGE", "HCCL_NPU_SOCKET_PORT_RANGE") if k in env}
    print(json.dumps(shown) + "\n" + shlex.join(cmd), flush=True)
    if not a.show:
        os.chdir(c["root"])
        os.execvpe(cmd[0], cmd, env)


if __name__ == "__main__":
    main()
