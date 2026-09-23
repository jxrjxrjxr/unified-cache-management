"""Deployment checks: real Qwen3 layout, memory admission, and reusable config."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "docs/kvready_e004"))
import prepare

GIB = 1024**3


class DeploymentContract(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.model = self.root / "Qwen3-4B"
        self.model.mkdir()
        self.model.joinpath("config.json").write_text(json.dumps({
            "model_type": "qwen3", "num_hidden_layers": 36, "num_key_value_heads": 8,
            "num_attention_heads": 32, "hidden_size": 2560, "head_dim": 128}))

    def memory_fixture(self, version):
        proc, group = self.root / "proc", self.root / "cgroup"
        (proc / "self").mkdir(parents=True)
        (proc / "meminfo").write_text(f"MemAvailable: {1000 * GIB // 1024} kB\n")
        if version == 2:
            (proc / "self/cgroup").write_text("0::/container\n")
            group.mkdir()
            (group / "memory.max").write_text("max")
            node, limit, used = group / "container", "memory.max", "memory.current"
        else:
            (proc / "self/cgroup").write_text("7:memory:/container\n")
            node = group / "memory/container"
            limit, used = "memory.limit_in_bytes", "memory.usage_in_bytes"
        node.mkdir(parents=True)
        (node / limit).write_text(str(800 * GIB))
        (node / used).write_text(str(100 * GIB))
        return proc, group

    def test_container_headroom_limits_budget_for_v1_and_v2(self):
        for version in (1, 2):
            with self.subTest(version=version), tempfile.TemporaryDirectory() as tmp:
                with patch.object(self, "root", Path(tmp)):
                    proc, group = self.memory_fixture(version)
                    budget, evidence = prepare.inmem_budget(proc, group)
                    self.assertEqual(budget, 525 * GIB)
                    self.assertEqual(evidence["reserve_bytes"], 175 * GIB)
                    self.assertEqual(evidence["reservation"], "snapshot only; host RAM is not reserved")

    def test_host_memory_also_limits_unbounded_container(self):
        proc, group = self.memory_fixture(2)
        (proc / "meminfo").write_text(f"MemAvailable: {200 * GIB // 1024} kB\n")
        (group / "container/memory.max").write_text("max")
        self.assertEqual(prepare.inmem_budget(proc, group)[0], 136 * GIB)

    def test_missing_memory_controller_stops_admission(self):
        proc, group = self.memory_fixture(2)
        (proc / "self/cgroup").write_text("8:cpu:/container\n")
        with self.assertRaisesRegex(ValueError, "memory limit"):
            prepare.inmem_budget(proc, group)

    def test_configuration_uses_qwen_head_dim_and_preserves_repository_yaml(self):
        original = (REPO / "examples/ucm_config_asu.yaml").read_bytes()
        session = self.root / "session"
        requests, demo, _ = prepare.prepare(session, self.model, memory=(600 * GIB, {"basis": "test"}))
        self.assertEqual(requests, 506.25 * GIB)
        self.assertEqual(demo, 0.84375 * GIB)
        config = json.loads((session / "run-config.json").read_text())
        self.assertEqual(config["model_path"], str(self.model.resolve()))
        self.assertTrue(config["storage_dedicated_to_test"])
        asu = yaml.safe_load((session / "asu.yaml").read_text())
        backend = asu["ucm_connectors"][0]["ucm_connector_config"]
        self.assertEqual((backend["asu_ips"], backend["asu_ports"]), (["127.0.0.1"], [19003]))
        self.assertEqual(backend["asu_trans_provider_backend"], "aiv")
        env = (session / "env.sh").read_text()
        self.assertIn("export VLLM_SERVER_DEV_MODE=1", env)
        self.assertIn("export ASCEND_RT_VISIBLE_DEVICES=4,5,6,7", env)
        self.assertEqual((REPO / "examples/ucm_config_asu.yaml").read_bytes(), original)
        with self.assertRaisesRegex(ValueError, "Keep existing results"):
            prepare.prepare(session, self.model, memory=(600 * GIB, {}))

    def test_insufficient_memory_leaves_no_partial_session(self):
        session = self.root / "session"
        with self.assertRaisesRegex(ValueError, "fixed package needs"):
            prepare.prepare(session, self.model, memory=(500 * GIB, {}))
        self.assertFalse(session.exists())


if __name__ == "__main__":
    unittest.main()
