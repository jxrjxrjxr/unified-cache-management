"""CPU-only checks for the fixed benchmark and result contracts (unittest)."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from report import MODES, FAMILIES, append_record, summarize, validate_request
from test_pd import completion_payload, consume_sse, state_complete
from workload import build_cases, correctness_cases, required_storage_bytes


def request_record(mode, batch, case_id, phase="performance"):
    return {"phase": phase, "mode": mode, "batch": batch, "case_id": case_id,
            "status": "completed", "planned_at": 1.0, "sent_at": 1.1,
            "first_at": 1.2, "completed_at": 1.6, "output_tokens": 128,
            "text": "林禾，12，周宁", "answer_pass": True,
            "kv_complete": True, "p_action": "LOAD"}


def complete_records():
    records = [request_record(mode, batch, f"{family}-{i:02d}")
               for batch in (0, 1) for mode in MODES for family in FAMILIES for i in range(20)]
    records.extend(request_record(mode, -1, f"qa-{i}", "correctness") for mode in MODES for i in range(4))
    return records


class ReportContract(unittest.TestCase):
    def test_full_paired_set_and_identical_input_yield_zero_difference(self):
        report = summarize(complete_records(), {"status": "completed"})
        self.assertEqual(report["status"], "valid")
        self.assertEqual(report["completed"], 320)
        self.assertEqual(report["comparisons"]["progress_vs_eager"]["0"]["all"]["mean_ttft_reduction_pct"], 0)

    def test_bad_timestamp_missing_pair_duplicate_and_short_output_are_invalid(self):
        for change in ("nan", "missing", "duplicate", "short"):
            with self.subTest(change=change):
                records = complete_records()
                if change == "nan":
                    records[0]["first_at"] = float("nan")
                elif change == "missing":
                    records.pop(0)
                elif change == "duplicate":
                    records.append(records[0].copy())
                else:
                    records[0]["output_tokens"] = 1
                result = summarize(records, {"status": "completed"})
                self.assertEqual(result["status"], "invalid")
                self.assertEqual(result["comparisons"], {})

    def test_text_variation_does_not_substitute_for_task_correctness(self):
        records = complete_records()
        records[0]["text"] = "Different valid summary wording."
        self.assertEqual(summarize(records, {"status": "completed"})["status"], "valid")
        records[-1]["answer_pass"] = False
        self.assertEqual(summarize(records, {"status": "completed"})["status"], "invalid")

    def test_raw_nonfinite_value_is_preserved_and_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "raw.jsonl"
            record = request_record("late", 0, "long_prefix-00")
            record["first_at"] = float("nan")
            append_record(path, record)
            restored = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(restored["first_at"], {"invalid_number": "nan"})
            self.assertIn("nonfinite_or_missing_timestamp", validate_request(restored))


class CharacterTokenizer:
    chat_template = "test"

    def apply_chat_template(self, messages, tokenize=False, add_generation_prompt=True):
        return "[user]" + messages[0]["content"] + "[/user][assistant]"

    def encode(self, text, add_special_tokens=False):
        return [ord(char) for char in text]


class WorkloadContract(unittest.TestCase):
    def test_shapes_prefix_identity_and_no_generator_state_dependency(self):
        cases = build_cases(CharacterTokenizer())
        self.assertEqual(len(cases), 40)
        self.assertEqual(cases, build_cases(CharacterTokenizer()))
        self.assertEqual([c["family"] for c in cases[:4]], ["long_prefix", "long_suffix"] * 2)
        self.assertTrue(all(len(c["prompt"]) == 8192 for c in cases))
        self.assertEqual(len({tuple(c["prompt"][:128]) for c in cases}), 40)
        self.assertTrue(all("".join(map(chr, c["prompt"])).startswith("[user]") for c in cases))
        self.assertTrue(all("".join(map(chr, c["prompt"])).endswith("[assistant]") for c in cases))
        self.assertTrue(all(c["prefix_tokens"] % 128 == 0 for c in cases))
        self.assertTrue(all(c["output_tokens"] == 128 for c in cases))

    def test_answers_depend_on_prefix_and_new_content(self):
        for case in correctness_cases(CharacterTokenizer(), 128):
            prefix = "".join(chr(t) for t in case["prompt"][:case["prefix_tokens"]])
            suffix = "".join(chr(t) for t in case["prompt"][case["prefix_tokens"]:])
            self.assertIn(case["expected"][0], prefix)
            self.assertNotIn(case["expected"][2], prefix)
            self.assertIn(case["expected"][2], suffix)
            self.assertIn(case["expected"][1] + "件", suffix)
            self.assertEqual(len(case["prompt"]), 8192)

    def test_whole_package_storage_budget_has_overhead(self):
        required = required_storage_bytes({"kv_bytes_per_token": 192 * 1024})
        self.assertEqual(required, 675 * 1024**3)


class HttpContract(unittest.TestCase):
    def test_payload_uses_exact_token_ids_and_fixed_length_only_for_performance(self):
        case = {"prompt": [10, 20, 30]}
        config = {"served_model_name": "local-model"}
        formal = completion_payload(config, case, "ns", "progress", "performance", "req")
        self.assertEqual(formal["prompt"], [10, 20, 30])
        self.assertEqual(formal["min_tokens"], 128)
        self.assertTrue(formal["ignore_eos"])
        qa = completion_payload(config, case, "ns", "late", "correctness", "req")
        self.assertEqual(qa["min_tokens"], 0)
        self.assertFalse(qa["ignore_eos"])

    def test_sse_usage_and_error_are_not_text_tokens(self):
        self.assertIsNone(consume_sse(": keepalive"))
        self.assertEqual(consume_sse("data: [DONE]"), {"done": True})
        self.assertEqual(consume_sse('data: {"usage":{"completion_tokens":128},"choices":[]}')["usage"]["completion_tokens"], 128)
        with self.assertRaises(RuntimeError):
            consume_sse('data: {"error":"backend failure"}')

    def test_http_success_alone_cannot_confirm_kv_completion(self):
        self.assertFalse(state_complete({"status": "completed"}))
        state = {"failed": False, "role": "pd", "producer_done": True,
                 "d_ready": True, "tasks": {"pending": 0, "inflight": 0, "complete": 2}}
        self.assertTrue(state_complete(state))
        state["tasks"]["inflight"] = 1
        self.assertFalse(state_complete(state))
        self.assertTrue(state_complete({"failed": False, "role": "prepare", "producer_done": True, "tasks": {"complete": 1}}))


if __name__ == "__main__":
    unittest.main()
