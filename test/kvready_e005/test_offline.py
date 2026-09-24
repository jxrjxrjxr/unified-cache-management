"""Offline checks for deployment isolation, request validity, and evidence."""
import importlib.util
import json
import math
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "docs/kvready_e005"))
sys.path.insert(0, str(Path(__file__).parent))
import prepare
import launch
import run
import benchmark


def config():
    return dict(root=str(ROOT), session="/tmp/e005", model="/models/Qwen3-4B",
                python=sys.executable, commit="a" * 40, p_devices="3,5", d_devices="6,7",
                p_port=8100, d_port=8200, proxy_port=8300, p_dist_port=29500, d_dist_port=29600)


def response_lines(tokens=32, done=True, usage=True):
    rows = [{"choices": [{"text": "Clear ", "finish_reason": None}]},
            {"choices": [{"text": "answer.", "finish_reason": "length"}]}]
    if usage:
        rows.append({"choices": [], "usage": {"prompt_tokens": 1024, "completion_tokens": tokens}})
    lines = [("data: " + json.dumps(r) + "\n\n").encode() for r in rows]
    if done:
        lines.append(b"data: [DONE]\n\n")
    return lines


class Deployment(unittest.TestCase):
    def test_stock_entrypoints_and_bounded_runtime(self):
        for role in ("p", "d"):
            cmd, env = launch.build(config(), role, {})
            transfer = json.loads(cmd[cmd.index("--kv-transfer-config") + 1])
            self.assertEqual(transfer["kv_connector"], "UCMConnector")
            self.assertIn("--no-enable-prefix-caching", cmd)
            self.assertEqual(cmd[cmd.index("--tensor-parallel-size") + 1], "2")
            self.assertEqual(cmd[cmd.index("--max-num-seqs") + 1], "2")
        cmd, _ = launch.build(config(), "proxy", {})
        self.assertIn("ucm.pd.toy_proxy_server", cmd)
        self.assertIn("--pd-disaggregation", cmd)

    def test_user_config_is_actual_device_and_port(self):
        c = config()
        c.update(p_devices="0,2", p_port=9100, p_dist_port=31000)
        cmd, env = launch.build(c, "p", {"ASCEND_RT_VISIBLE_DEVICES": "4,5"})
        self.assertEqual(env["ASCEND_RT_VISIBLE_DEVICES"], "0,2")
        self.assertEqual(env["VLLM_PORT"], "31000")
        self.assertEqual(cmd[cmd.index("--port") + 1], "9100")

    def test_role_metrics_and_original_environment_preserved(self):
        inherited = {"PYTHONPATH": "/some/project", "HCCL_SOCKET_IFNAME": "eth0",
                     "http_proxy": "unused", "HCCL_IF_BASE_PORT": "61000"}
        c = config()
        c["d_hccl_if_base_port"] = "62000"
        _, p = launch.build(c, "p", inherited)
        _, d = launch.build(c, "d", inherited)
        self.assertEqual(p["HCCL_IF_BASE_PORT"], "61000")
        self.assertEqual(d["HCCL_IF_BASE_PORT"], "62000")
        self.assertNotEqual(p["PROMETHEUS_MULTIPROC_DIR"], d["PROMETHEUS_MULTIPROC_DIR"])
        self.assertNotIn("http_proxy", p)
        self.assertIn("http_proxy", inherited)
        self.assertTrue(p["PYTHONPATH"].startswith(str(ROOT)))
        self.assertEqual(p["HCCL_SOCKET_IFNAME"], "eth0")

    def test_invalid_devices_and_overlapping_ports_stop_before_launch(self):
        for changes in ({"p_devices": "4,4"}, {"p_devices": "4"}, {"p_devices": "6,5"},
                        {"proxy_port": 8100}, {"d_dist_port": 29550},
                        {"p_port": 29502}, {"p_port": 0}):
            with self.subTest(changes=changes):
                c = dict(config(), **changes)
                with self.assertRaises(ValueError):
                    launch.build(c, "p", {})

    def test_native_source_change_blocks_reuse(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(prepare, "git", side_effect=["ucm/store/a.cc", "diff"]):
                with self.assertRaisesRegex(ValueError, "native source"):
                    prepare.reuse_native(Path(tmp) / "e004", Path(tmp) / "e005")

    def test_missing_native_library_stops_without_install(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(prepare, "git", side_effect=["ucm/store/a.cc", ""]):
                with self.assertRaisesRegex(ValueError, "libraries"):
                    prepare.reuse_native(Path(tmp) / "e004", Path(tmp) / "e005")

    def test_cleanup_addresses_only_launched_process_groups(self):
        from unittest.mock import Mock
        children = [("p", Mock(pid=12345)), ("d", Mock(pid=23456))]
        for _, child in children:
            child.poll.return_value = 0
        with patch.object(run.os, "killpg", create=True) as kill, patch.object(run.signal, "SIGKILL", 9, create=True):
            run.stop_children(children)
        self.assertEqual({call.args[0] for call in kill.call_args_list}, {12345, 23456})


class Streaming(unittest.TestCase):
    def test_success_includes_one_record_and_finite_timing(self):
        result = benchmark.parse_stream(response_lines(), 1, iter([1.1, 1.4, 1.5]).__next__)
        self.assertEqual(result["usage"]["completion_tokens"], 32)
        self.assertAlmostEqual(result["ttft_ms"], 100)
        self.assertAlmostEqual(result["tpot_ms"], 300 / 31)
        self.assertEqual(result["text"], "Clear answer.")

    def test_incomplete_empty_and_wrong_counts_fail(self):
        samples = [response_lines(done=False), response_lines(tokens=1),
                   response_lines(usage=False), [b"data: [DONE]\n"],
                   [b'data: {"error":"decode failed"}\n']]
        for lines in samples:
            with self.subTest(lines=lines), self.assertRaises(ValueError):
                benchmark.parse_stream(lines, 0, lambda: 1)

    def test_nonfinite_timing_fails(self):
        with self.assertRaises(ValueError):
            benchmark.parse_stream(response_lines(), 0, lambda: math.inf)

    def test_real_http_request_and_failure_record(self):
        captured = []
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                captured.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
                if self.path == "/fail":
                    self.send_error(503)
                    return
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.end_headers()
                for chunk in response_lines():
                    self.wfile.write(chunk)
                    self.wfile.flush()
            def log_message(self, *a):
                pass
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            url = "http://127.0.0.1:" + str(server.server_port)
            good = benchmark.request(url + "/ok", [7] * 1024, "ok")
            bad = benchmark.request(url + "/fail", [8] * 1024, "fail")
            self.assertEqual(good["status"], "valid")
            self.assertEqual(bad["status"], "invalid")
            self.assertIn("503", bad["error"])
            self.assertEqual(len(captured), 2)
            self.assertEqual(captured[0]["max_tokens"], 32)
            self.assertTrue(captured[0]["ignore_eos"])
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


class Evidence(unittest.TestCase):
    def test_two_rank_completed_load_delta(self):
        raw = ('ucm:load_blocks_num_sum{model_name="e005",worker_id="id_0"} 16\n'
               'ucm:load_blocks_num_sum{model_name="e005",worker_id="id_1"} 16\n')
        workers = benchmark.metric_workers(raw, "load_blocks_num")
        self.assertEqual(benchmark.pd_evidence({}, workers, 16), (True, {"id_0": 16, "id_1": 16}))

    def test_one_rank_missing_insufficient_and_stale_fail(self):
        for before, after in [({}, {"rank_0": 16}),
                              ({}, {"rank_0": 16, "rank_1": 8}),
                              ({"rank_0": 16, "rank_1": 16}, {"rank_0": 16, "rank_1": 16})]:
            self.assertFalse(benchmark.pd_evidence(before, after, 16)[0])

    def test_failed_batch_not_silently_dropped(self):
        with self.assertRaises(ValueError):
            benchmark.summarize([{"status": "invalid"}], 1)

    def test_summary_does_not_publish_partial_performance(self):
        c = config()
        invalid = dict(status="invalid", stage="c2", batches={"c1": {"ttft_ms": 7}})
        text = run.summary(c, invalid, "complete")
        self.assertIn("PERF status=not_reported", text)
        self.assertNotIn("c1_ttft", text)

    def test_prompt_shape_with_template(self):
        class Tokenizer:
            def encode(self, value, add_special_tokens=False):
                return list(value.encode())
            def apply_chat_template(self, messages, **kwargs):
                assert kwargs["enable_thinking"] is False
                return "<user>" + messages[0]["content"] + "</user><assistant>"
        result = benchmark.prompts(Tokenizer())
        self.assertEqual(len(result), 18)
        self.assertTrue(all(len(p) == 1024 for p in result))
        self.assertEqual(len({tuple(p[:128]) for p in result}), 18)



class NativeProxyContract(unittest.TestCase):
    def test_original_proxy_prefills_once_then_streams_decode(self):
        """Exercise the unchanged upstream proxy over real local HTTP."""
        import logging
        import socket
        import subprocess
        import time
        calls = []
        class Backend(BaseHTTPRequestHandler):
            def do_POST(self):
                payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                calls.append(payload)
                self.send_response(200)
                if payload["stream"]:
                    self.send_header("Content-Type", "text/event-stream")
                    self.end_headers()
                    for chunk in response_lines():
                        self.wfile.write(chunk)
                else:
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(b'{"choices":[{"text":"prefill"}]}')
                self.wfile.flush()
            def log_message(self, *a):
                pass
        backend = ThreadingHTTPServer(("127.0.0.1", 0), Backend)
        thread = threading.Thread(target=backend.serve_forever, daemon=True)
        thread.start()
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            proxy_port = sock.getsockname()[1]
        bootstrap = (
            "import sys,types,logging,runpy; "
            "v=types.ModuleType('vllm'); l=types.ModuleType('vllm.logger'); "
            "l.init_logger=logging.getLogger; "
            "sys.modules['vllm']=v; sys.modules['vllm.logger']=l; "
            "runpy.run_path(" + repr(str(ROOT / "ucm/pd/toy_proxy_server.py")) + ",run_name='__main__')")
        args = [sys.executable, "-c", bootstrap, "--pd-disaggregation",
                "--host", "127.0.0.1", "--port", str(proxy_port),
                "--prefiller-host", "127.0.0.1", "--prefiller-port", str(backend.server_port),
                "--decoder-host", "127.0.0.1", "--decoder-port", str(backend.server_port)]
        with tempfile.TemporaryFile() as log:
            process = subprocess.Popen(args, stdout=log, stderr=log, env={
                **launch.build(config(), "proxy")[1], "PYTHONPATH": ""})
            try:
                base = "http://127.0.0.1:" + str(proxy_port)
                deadline = time.monotonic() + 15
                while True:
                    if process.poll() is not None:
                        log.seek(0)
                        self.fail(log.read().decode(errors="replace"))
                    try:
                        health = json.loads(benchmark.fetch(base + "/healthcheck"))
                        break
                    except Exception:
                        if time.monotonic() >= deadline:
                            self.fail("Offline proxy startup deadline")
                        time.sleep(0.1)
                self.assertEqual(health["mode"], "pd-disaggregation")
                result = benchmark.request(base + "/v1/completions", [100] * 1024, "proxy")
                self.assertEqual(result["status"], "valid", result)
                self.assertEqual(len(calls), 2)
                self.assertEqual(calls[0]["max_tokens"], 1)
                self.assertFalse(calls[0]["stream"])
                self.assertNotIn("stream_options", calls[0])
                self.assertEqual(calls[1]["max_tokens"], 32)
                self.assertTrue(calls[1]["stream"])
                self.assertEqual(calls[0]["prompt"], calls[1]["prompt"])
            finally:
                process.terminate()
                process.wait(timeout=10)
                backend.shutdown()
                backend.server_close()
                thread.join()

if __name__ == "__main__":
    unittest.main()
