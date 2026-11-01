"""Offline regressions for the pinned upstream Q&A contract and its adapter."""
import argparse
import importlib.util
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location("tested_dbaiops_runner", Path(__file__).with_name("native_runner.py"))
native_runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(native_runner)


class UpstreamKnowledgeTest(unittest.TestCase):
    def retrieve(self, **kwargs):
        return native_runner.retrieve_knowledge(
            "检查锁等待", url="https://knowledge.invalid/api", api_key="dummy",
            dataset_id="2104", retries=2, similarity=0.7, topn=6,
            temperature=0.7, **kwargs,
        )

    def test_rule_comes_from_upstream(self):
        rule = native_runner.load_precise_rule()
        self.assertEqual((rule["id"], rule["similarity"], rule["topn"], rule["temperature"]),
                         ("default-fast-search", 0.7, 6, 0.7))
        self.assertIn("专业的数据库专家", rule["prompt"])

    def test_knowledge_request_matches_stream_chat(self):
        response = {"status": "200", "data": {"dataList": ["版本：PostgreSQL"], "imageList": []}}
        with patch.object(native_runner, "request_json", return_value=response) as request:
            items, responses = self.retrieve()
        self.assertEqual(items, ["版本：PostgreSQL"])
        self.assertEqual(responses, [response])
        self.assertEqual(request.call_args.args[1], {
            "question": "检查锁等待", "similarity": 0.7, "topn": 6,
            "dataset_name": "2104", "temperature": 0.7,
            "language": "中文", "isSupportImg": "true",
        })
        self.assertEqual(request.call_args.kwargs["attempts"], 2)

    def test_remote_failure_does_not_change_knowledge_source(self):
        with patch.object(native_runner, "request_json", side_effect=RuntimeError("HTTP 502")) as request:
            with self.assertRaisesRegex(RuntimeError, "HTTP 502"):
                self.retrieve()
        request.assert_called_once()
        self.assertFalse(hasattr(native_runner, "retrieve_local_knowledge"))

    def test_empty_rejected_and_wrong_schema_stop_without_retry(self):
        for response in [
            {"status": "200", "data": {"dataList": []}},
            {"status": "200", "data": ["legacy format"]},
            {"status": "403", "message": "denied"},
        ]:
            with self.subTest(response=response), patch.object(native_runner, "request_json", return_value=response) as request:
                with self.assertRaises(RuntimeError):
                    self.retrieve()
                request.assert_called_once()

    def test_invalid_retry_budget_never_makes_a_request(self):
        with patch.object(native_runner, "request_json") as request:
            with self.assertRaises(ValueError):
                native_runner.retrieve_knowledge("q", url="invalid", api_key="", dataset_id="2104",
                                                retries=0, similarity=0.7, topn=6, temperature=0.7)
            request.assert_not_called()

    def test_json_values_follow_upstream_string_conversion(self):
        values = [{"x": 1}, None, True, [1, None, False]]
        response = {"status": "200", "data": {"dataList": values}}
        with patch.object(native_runner, "request_json", return_value=response):
            items, _ = self.retrieve()
        self.assertEqual(items, ["[object Object]", "null", "true", "1,,false"])

    def test_marked_knowledge_uses_full_upstream_context(self):
        knowledge = "版本：PostgreSQL\n" + "x" * 2500
        messages = native_runner.build_native_messages("检查异常", [knowledge], "pg", native_runner.load_precise_rule())
        self.assertIn("知识库查询结果:\n" + knowledge, messages[1]["content"])
        self.assertIn("请结合PG兼容生态的专业知识", messages[0]["content"])
        self.assertIn(native_runner.DIAGNOSIS_REQUIREMENTS, messages[0]["content"])

    def test_unmarked_knowledge_uses_upstream_page_branch(self):
        messages = native_runner.build_native_messages("检查异常", ["x" * 2500], "mysql", native_runner.load_precise_rule())
        self.assertIn("页面URL: about:blank", messages[1]["content"])
        self.assertIn("x" * 2000 + "...", messages[1]["content"])
        self.assertNotIn("x" * 2001, messages[1]["content"])

    def test_non_chinese_question_keeps_language_instruction(self):
        messages = native_runner.build_native_messages("lock waits", ["版本：PG"], "pg", native_runner.load_precise_rule())
        self.assertIn("如果用户的问题不是中文", messages[0]["content"])


class AdapterExecutionTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.sample = self.root / "sample"
        self.sample.mkdir()
        for name, text in {
            "initialization_workload.sql": "SELECT 1;",
            "verified_workload.sql": "SELECT 2;",
            "user_phenomenon_description.txt": "检查锁等待",
            "anomaly_inject.sh": "PRIVATE_INJECTION_SENTINEL",
            "key_information.json": "PRIVATE_ANSWER_SENTINEL",
        }.items():
            (self.sample / name).write_text(text)
        self.args = argparse.Namespace(
            sample_dir=self.sample, output_dir=self.root / "output", snapshot=self.root / "snapshot.json",
            dbms="pg", context_chars=50000, knowledge_url="https://knowledge.invalid", knowledge_api_key="dummy-kb",
            dataset_id="2104", retries=2, model_url="https://model.invalid", model_api_key="dummy-model",
            model="test", api_version="", temperature=1.0, max_tokens=2048,
        )
        self.action = {"action": "finish", "diagnosis_report.md": "FROZEN\n",
                       "solution_script.sh": "# TIMEOUT THRESHOLD: 5\ncommand -v psql >/dev/null\nexit 1\n", "verified_workload.sql": ""}

    def tearDown(self):
        self.tmp.cleanup()

    def test_diagnosis_uses_rule_temperature_and_frozen_report(self):
        with patch.object(native_runner, "retrieve_knowledge", return_value=(["版本：PG"], [])), \
             patch.object(native_runner, "chat_completion", return_value=("FROZEN", {})) as chat, \
             patch.object(native_runner, "generate_artifact_action", return_value=(self.action, {})) as artifact:
            native_runner.run(self.args)
        self.assertEqual(chat.call_args.kwargs["temperature"], 0.7)
        self.assertEqual(artifact.call_args.args[0].temperature, 1.0)
        output = self.args.output_dir
        self.assertEqual((output / "native_diagnosis_report.md").read_text(), "FROZEN\n")
        self.assertEqual((output / "optimized_verified_workload.sql").read_text(), "SELECT 2;")
        trace = json.loads((output / "native_trace.json").read_text())
        self.assertEqual(trace["knowledge_source"], "remote_dbaiops")
        self.assertEqual(trace["model_temperature"], 0.7)
        self.assertEqual(trace["artifact_temperature"], 1.0)
        for secret in ["PRIVATE_INJECTION_SENTINEL", "PRIVATE_ANSWER_SENTINEL"]:
            self.assertNotIn(secret, json.dumps(trace))
            self.assertNotIn(secret, artifact.call_args.args[1])

    def test_native_report_survives_artifact_failure(self):
        with patch.object(native_runner, "retrieve_knowledge", return_value=(["版本：PG"], [])), \
             patch.object(native_runner, "chat_completion", return_value=("FROZEN", {})), \
             patch.object(native_runner, "generate_artifact_action", side_effect=ValueError("invalid artifact")):
            with self.assertRaisesRegex(ValueError, "invalid artifact"):
                native_runner.run(self.args)
        self.assertEqual((self.args.output_dir / "native_diagnosis_report.md").read_text(), "FROZEN\n")
        self.assertEqual(json.loads((self.args.output_dir / "native_trace.json").read_text())["artifact_status"], "failed")

    def test_empty_knowledge_prevents_model_and_artifact_calls(self):
        with patch.object(native_runner, "request_json", return_value={"status": "200", "data": {"dataList": []}}), \
             patch.object(native_runner, "chat_completion") as chat, \
             patch.object(native_runner, "generate_artifact_action") as artifact:
            with self.assertRaisesRegex(RuntimeError, "no matched documents"):
                native_runner.run(self.args)
            chat.assert_not_called()
            artifact.assert_not_called()

    def test_normalization_cannot_replace_the_native_diagnosis(self):
        changed = dict(self.action, **{"diagnosis_report.md": "REWRITTEN"})
        result = native_runner.normalize_artifact(changed, "FROZEN")
        self.assertEqual(result["diagnosis_report.md"], "FROZEN")

    def test_invalid_bash_is_rejected_without_execution(self):
        changed = dict(self.action, **{"solution_script.sh": "# TIMEOUT THRESHOLD: 5\nif\n"})
        with self.assertRaisesRegex(ValueError, "Invalid Bash"):
            native_runner.validate_action(changed)


if __name__ == "__main__":
    unittest.main()
