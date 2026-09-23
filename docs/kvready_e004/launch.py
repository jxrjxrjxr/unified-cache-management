"""Build a role-specific copy of the existing ASU config and start one service."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import sys
from pathlib import Path
from urllib.parse import urlparse


def build_command(config, role):
    model = Path(config["model_path"])
    model_config = json.loads((model / "config.json").read_text(encoding="utf-8"))
    base = [sys.executable, "-m"]
    if role == "proxy":
        url = urlparse(config["proxy_url"])
        return base + ["ucm.pd.kvready_proxy", "--host", url.hostname,
                       "--port", str(url.port or 8300), "--prefill-url", config["prefill_url"],
                       "--decode-url", config["decode_url"], "--tp-size", "2",
                       "--num-layers", str(model_config["num_hidden_layers"])], None, None
    import yaml
    asu = yaml.safe_load(Path(config["asu_config_path"]).read_text(encoding="utf-8"))
    if not isinstance(asu, dict) or not asu.get("ucm_connectors"):
        raise ValueError("Use the existing, verified ASU UCM configuration")
    for connector in asu["ucm_connectors"]:
        backend = connector.get("ucm_connector_config", {})
        if "ASU" not in backend.get("store_pipeline", ""):
            raise ValueError("E004 formal comparisons require the declared real ASU path")
        if backend.get("asu_fake_backend_path") or backend.get("asu_fake_backend_complete_immediately"):
            raise ValueError("A fake ASU backend cannot serve a formal E004 run")
    asu["use_layerwise"] = True
    asu["enable_event_sync"] = True
    asu["persist_token_threshold"] = 0
    asu["kvready"] = {"role": role, "control_url": config["proxy_url"], "timeout": 120}
    runtime = Path(config.get("runtime_dir", "/tmp/kvready-e004"))
    output = runtime / f"ucm-{role}.yaml"
    transfer = {"kv_connector": "KVReadyConnector", "kv_role": "kv_both",
                "kv_connector_module_path": "ucm.integration.vllm.kvready_connector",
                "kv_connector_extra_config": {"UCM_CONFIG_FILE": str(output)}}
    url = urlparse(config["prefill_url"] if role == "p" else config["decode_url"])
    command = base + ["vllm.entrypoints.cli.main", "serve", str(model),
                      "--served-model-name", config["served_model_name"],
                      "--host", url.hostname, "--port", str(url.port),
                      "--tensor-parallel-size", "2", "--dtype", "bfloat16", "--enforce-eager",
                      "--enable-prefix-caching", "--block-size", str(config.get("block_size", 128)),
                      "--max-model-len", "9216", "--max-num-batched-tokens", "16384",
                      "--max-num-seqs", "4", "--gpu-memory-utilization", "0.75",
                      "--kv-transfer-config", json.dumps(transfer, separators=(",", ":"))]
    return command, output, yaml.safe_dump(asu, sort_keys=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("role", choices=("proxy", "p", "d"))
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--show", action="store_true", help="print the command without starting a service")
    args = parser.parse_args()
    config = json.loads(args.config.read_text(encoding="utf-8"))
    command, output, yaml_text = build_command(config, args.role)
    devices = {"p": "4,5", "d": "6,7"}.get(args.role)
    print((f"ASCEND_RT_VISIBLE_DEVICES={devices} " if devices else "") + shlex.join(command), flush=True)
    if args.show:
        return
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(yaml_text, encoding="utf-8")
    env = os.environ.copy()
    if devices:
        env["ASCEND_RT_VISIBLE_DEVICES"] = devices
    env["NO_PROXY"] = env["no_proxy"] = "127.0.0.1,localhost"
    os.execvpe(command[0], command, env)


if __name__ == "__main__":
    main()
