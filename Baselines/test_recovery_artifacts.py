"""Offline recovery-conversion regressions; never run scripts, DBs or model calls.

SQL below is syntax-only fixture material, not a deployable repair recipe.
"""
import argparse
import ast
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "Baselines/DB-GPT")]
from Baselines import recovery_artifacts as policy
from Baselines.ReAct import react
import llm_api


def load(name, relative):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


gpt = load("recovery_test_gpt", "Baselines/DB-GPT/artifact_adapter.py")
ops = load("recovery_test_ops", "Baselines/DBAIOps/native_runner.py")

# The validator must not ban target-object maintenance or database SQL writes.
PG_SCRIPT = '''# TIMEOUT THRESHOLD: 60
set -Eeuo pipefail
export PGCONNECT_TIMEOUT=5
psql -X -v ON_ERROR_STOP=1 -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" <<'SQL'
SET statement_timeout = '10s';
REINDEX TABLE public.fixture;
ANALYZE public.fixture;
SQL
'''
MYSQL_SCRIPT = '''# TIMEOUT THRESHOLD: 60
set -Eeuo pipefail
mysql --connect-timeout=5 -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" "$DB_NAME" <<'SQL'
KILL QUERY 17;
KILL CONNECTION 18;
ANALYZE TABLE fixture;
SQL
'''
FAIL_CLOSED = "# TIMEOUT THRESHOLD: 5\nset -Eeuo pipefail\ncommand -v psql >/dev/null\nexit 1\n"
REPORT = '  Native report: "inspection only", "need authorization", "do not repair".\r\n'
ORIGINAL = " \r\nSELECT 'original';\r\n"


def artifacts(script=PG_SCRIPT, workload=""):
    return {"solution_script.sh": script, "verified_workload.sql": workload}


class EvidencePolicyTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.sample = self.root / "sample"
        self.sample.mkdir()
        self.config = {"dbms": "PostgreSQL", "database_name": "target", "database_schema_name": "public",
                       "password": "SECRET_PASSWORD", "private_hint": "SECRET_HINT",
                       "superuser_config": {"DB_PASSWORD": "SECRET_ADMIN"}}
        (self.sample / "config.json").write_text(json.dumps(self.config))
        for name, text in {"user_phenomenon_description.txt": "Queries wait.",
                           "initialization_workload.sql": "SELECT 'INITIALIZATION_ONLY';",
                           "verified_workload.sql": ORIGINAL,
                           "anomaly_inject.sh": "SECRET_INJECTOR",
                           "key_information.json": "SECRET_ANSWER",
                           "anomaly_description.json": "SECRET_DEFINITION",
                           "solution_script.sh": "SECRET_OLD_SCRIPT"}.items():
            (self.sample / name).write_bytes(text.encode())
        self.snapshot = self.root / "snapshot.json"
        self.snapshot.write_text('"SECRET_SNAPSHOT"')

    def test_allowlisted_evidence_only(self):
        evidence = policy.collect_public_evidence(self.sample)
        self.assertEqual(set(evidence["public_inputs"]), set(policy.PUBLIC_FILES))
        self.assertEqual(set(evidence["target"]), set(policy.TARGET_FIELDS))
        prompt = gpt.build_prompt(REPORT, gpt.collect_evidence(self.sample, self.snapshot, 1))
        self.assertNotIn("SECRET_", prompt)
        self.assertIn("INITIALIZATION_ONLY", prompt)
        self.assertEqual(evidence["public_inputs"]["verified_workload.sql"], ORIGINAL)

    def test_complete_workload_is_not_truncated_by_legacy_limit(self):
        sql = "-- " + "x" * 80000 + "\nSELECT 'tail';\r\n"
        (self.sample / "verified_workload.sql").write_bytes(sql.encode())
        evidence = json.loads(gpt.collect_evidence(self.sample, self.snapshot, 1))
        self.assertEqual(evidence["public_inputs"]["verified_workload.sql"], sql)
        self.assertEqual(gpt.original_workload(self.sample), sql)

    def test_legacy_missing_config_uses_dbms_hint(self):
        (self.sample / "config.json").unlink()
        self.assertEqual(policy.collect_public_evidence(self.sample, "mysql")["target"]["dbms"], "mysql")

    def test_nested_target_or_nonobject_config_is_rejected(self):
        for config in [[], {"database_name": {"secret": "do-not-serialize"}}]:
            with self.subTest(config=config):
                (self.sample / "config.json").write_text(json.dumps(config))
                with self.assertRaises(ValueError):
                    policy.collect_public_evidence(self.sample)

    def test_missing_verification_does_not_substitute_initialization(self):
        (self.sample / "verified_workload.sql").unlink()
        with self.assertRaises(FileNotFoundError):
            policy.collect_public_evidence(self.sample)
        with self.assertRaises(FileNotFoundError):
            gpt.original_workload(self.sample)

    def test_authorization_live_checks_and_postconditions_are_explicit(self):
        prompt = gpt.build_prompt(REPORT, gpt.collect_evidence(self.sample, None, 1))
        for requirement in ["DBA-authorized", "not a frozen execution policy", "not binding",
                            "cancel/terminate CONFIRMED", "Rediscover targets", "bounded polling",
                            "No OS root", "at most 1800", "ORIGINAL verification SQL",
                            "exactly two string keys", policy.DIAGNOSIS_REQUIREMENTS]:
            self.assertIn(requirement, prompt)
        self.assertEqual(gpt.native_report_from_result({"report": REPORT}), REPORT)


class StaticValidationTest(unittest.TestCase):
    def test_postgres_maintenance_settings_and_cancellation_are_allowed(self):
        for sql in ["REINDEX TABLE fixture;", "ALTER DATABASE fixture SET work_mem = '16MB';",
                    "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE false;",
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE false;"]:
            with self.subTest(sql=sql):
                script = PG_SCRIPT.replace("REINDEX TABLE public.fixture;", sql)
                self.assertEqual(policy.validate_artifacts(artifacts(script)), artifacts(script))

    def test_mysql_kill_query_connection_and_maintenance_are_sql_not_os_kill(self):
        for script in [MYSQL_SCRIPT, MYSQL_SCRIPT.replace("KILL", "kill")]:
            with self.subTest(script=script):
                policy.validate_artifacts(artifacts(script))

    def test_unsafe_target_can_fail_closed_instead_of_inventing_a_repair(self):
        policy.validate_artifacts(artifacts(FAIL_CLOSED))

    def test_timeout_is_bounded(self):
        for header in ["", "# TIMEOUT THRESHOLD: 0", "# TIMEOUT THRESHOLD: -1",
                       "# TIMEOUT THRESHOLD: 1801", "# TIMEOUT THRESHOLD: 60.5"]:
            with self.subTest(header=header), self.assertRaisesRegex(ValueError, "timeout"):
                policy.validate_artifacts(artifacts(PG_SCRIPT.replace("# TIMEOUT THRESHOLD: 60", header)))
        policy.validate_artifacts(artifacts(PG_SCRIPT.replace(": 60", ": 1800")))

    def test_invalid_bash_and_missing_client_are_rejected(self):
        for script in [PG_SCRIPT + "\nif then\n", "# TIMEOUT THRESHOLD: 5\nexit 0\n"]:
            with self.subTest(script=script), self.assertRaises(ValueError):
                policy.validate_artifacts(artifacts(script))

    def test_private_sources_and_os_commands_are_rejected(self):
        for command in ["docker restart target", "echo x; sudo id", "systemctl restart postgresql",
                        "curl https://example.invalid", "kill -9 123", "pkill postgres",
                        "cat /proc/1/environ", "cat anomaly_inject.sh", "cat key_information.json",
                        'psql -c "SELECT pg_read_file(\'/tmp/x\')"',
                        'psql -c "COPY fixture TO PROGRAM \'id\'"',
                        'mysql -e "SELECT LOAD_FILE(\'/tmp/x\')"']:
            with self.subTest(command=command), self.assertRaisesRegex(ValueError, "Forbidden"):
                policy.validate_artifacts(artifacts(PG_SCRIPT + command + "\n"))

    def test_schema_is_exactly_two_strings(self):
        for value in [[], {}, artifacts(workload=None), artifacts(script=2),
                      dict(artifacts(), **{"diagnosis_report.md": "rewritten"})]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                policy.validate_artifacts(value)

    def test_validator_does_syntax_only_and_never_runs_generated_commands(self):
        with patch.object(policy.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, "", "")) as run:
            policy.validate_artifacts(artifacts())
        self.assertEqual(run.call_args.args[0], ["bash", "-n"])
        self.assertEqual(run.call_args.kwargs["input"], PG_SCRIPT)
        self.assertEqual(run.call_args.kwargs["timeout"], 15)


class OutputContractTest(unittest.TestCase):
    def test_optional_workload_fallback_and_rewrite_are_byte_preserved(self):
        for optional in ["", " \t\r\n", " \r\nSELECT 'rewrite';\r\n"]:
            with self.subTest(optional=optional), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                action = gpt.save_artifacts(root, REPORT, artifacts(workload=optional), ORIGINAL)
                self.assertEqual(set(action), {"action", "diagnosis_report.md", "solution_script.sh", "verified_workload.sql"})
                self.assertEqual(action["verified_workload.sql"], optional)
                self.assertEqual((root / "diagnosis_report.md").read_bytes(), REPORT.encode())
                self.assertEqual((root / "optimized_verified_workload.sql").read_bytes(), (optional if optional.strip() else ORIGINAL).encode())
                self.assertEqual((root / "solution_script.sh").stat().st_mode & 0o777, 0o700)

    def test_empty_effective_workload_cannot_emit_a_finish(self):
        with tempfile.TemporaryDirectory() as tmp, self.assertRaisesRegex(ValueError, "empty"):
            gpt.save_artifacts(Path(tmp), REPORT, artifacts(), " \n")
        with self.assertRaises(ValueError):
            policy.effective_workload(None, ORIGINAL)

    def test_legacy_and_two_field_ops_responses_keep_frozen_report(self):
        for value in [artifacts(), dict(artifacts(), action="finish", **{"diagnosis_report.md": "REWRITTEN"})]:
            result = ops.normalize_artifact(value, REPORT)
            self.assertEqual(result["diagnosis_report.md"], REPORT)
            self.assertEqual(result["action"], "finish")
            self.assertEqual(result["solution_script.sh"], PG_SCRIPT)

    def test_both_outputs_are_consumed_by_common_evaluator_save_finish(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            actions = [gpt.save_artifacts(root / "gpt-native", REPORT, artifacts(), ORIGINAL),
                       ops.normalize_artifact(artifacts(MYSQL_SCRIPT), REPORT)]
            for index, action in enumerate(actions):
                output = root / str(index)
                output.mkdir()
                react.save_finish(action, output, ORIGINAL)
                self.assertEqual((output / "diagnosis_report.md").read_bytes(), REPORT.encode())
                self.assertEqual((output / "optimized_verified_workload.sql").read_bytes(), ORIGINAL.encode())
                self.assertTrue((output / "solution_script.sh").read_text().strip())

    def test_metadata_tracks_source_version_fallback_and_unexecuted_artifacts(self):
        evidence = {"public_inputs": {"verified_workload.sql": ORIGINAL}}
        meta = policy.conversion_metadata("DB-GPT", REPORT, artifacts(), ORIGINAL, "prompt", evidence)
        self.assertEqual(meta["version"], policy.VERSION)
        self.assertEqual(meta["reference_version"], policy.REFERENCE_VERSION)
        self.assertEqual(meta["execution_status"], "not_executed")
        self.assertTrue(meta["original_workload_fallback"])
        self.assertEqual(meta["artifact_sha256"]["optimized_verified_workload.sql"], hashlib.sha256(ORIGINAL.encode()).hexdigest())
        changed = policy.conversion_metadata("DB-GPT", REPORT + "changed", artifacts(), ORIGINAL, "prompt", evidence)
        self.assertNotEqual(meta["source_identity"], changed["source_identity"])


class EvaluatorBoundaryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Load the real pure helpers without importing the evaluator's live
        # orchestration dependencies or executing any top-level CLI code.
        path = ROOT / "Evaluation/test_eval.py"
        names = {"build_recovery_db_config", "build_script_command", "read_agent_artifact"}
        nodes = [node for node in ast.parse(path.read_text()).body
                 if isinstance(node, ast.FunctionDef) and node.name in names]
        if len(nodes) != len(names):
            raise AssertionError("Evaluator public helper contract changed")
        cls.helpers = {"Path": Path, "normalize_dbms": react.normalize_dbms,
                       "DEFAULT_SUPERUSER_CONFIG": {}}
        exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec"), cls.helpers)

    def test_repair_uses_admin_without_changing_workload_account_or_routing(self):
        original = {"host": "target", "port": "5432", "user": "workload", "password": "ordinary",
                    "db_name": "fixture", "schema": "public"}
        admin = self.helpers["build_recovery_db_config"](
            original, {"DB_USER": "admin", "DB_PASSWORD": "admin-secret"})
        for key in ["host", "port", "db_name", "schema"]:
            self.assertEqual(admin[key], original[key])
        self.assertEqual(original["user"], "workload")
        self.assertEqual(original["password"], "ordinary")
        for dbms in ["pg", "mysql"]:
            command = self.helpers["build_script_command"]("solution_script.sh", admin, dbms)
            self.assertIn("DB_USER=admin", command)
            self.assertIn("DB_PASSWORD=admin-secret", command)
            self.assertIn("DB_NAME=fixture", command)
            self.assertEqual(command[-2:], ["bash", "solution_script.sh"])

    def test_no_rewrite_still_has_both_artifacts_required_for_recovery(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)
            gpt.save_artifacts(output, REPORT, artifacts(workload="  "), ORIGINAL)
            read = self.helpers["read_agent_artifact"]
            self.assertIsNotNone(read(output / "solution_script.sh"))
            self.assertIsNotNone(read(output / "optimized_verified_workload.sql"))


class ConversionTransportTest(unittest.TestCase):
    def test_gpt_schema_repair_accepts_actual_recovery(self):
        bad = artifacts(PG_SCRIPT.replace(": 60", ": 1801"))
        with patch.object(gpt, "request_completion", side_effect=[(json.dumps(bad), "bad"), (json.dumps(artifacts()), "valid")]) as request, \
             patch.object(gpt.time, "sleep"):
            generated, raw = gpt.generate_artifacts("frozen prompt", api_key="dummy", model="fixture", api_base="https://model.invalid", retry_times=2)
        self.assertEqual(generated, artifacts())
        self.assertEqual(raw, ["bad", "valid"])
        self.assertIn("bounded timeout", request.call_args.args[0])

    def test_gpt_transport_disables_context_truncation_and_rejects_partial_response(self):
        response = SimpleNamespace(model_dump_json=lambda: "{}", choices=[SimpleNamespace(finish_reason="stop", message=SimpleNamespace(content=json.dumps(artifacts())))])
        client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=Mock(return_value=response))))
        fake = SimpleNamespace(OpenAI=Mock(return_value=client))
        with patch.dict(sys.modules, {"openai": fake}), \
             patch.object(gpt, "prepare_request", side_effect=lambda request, options: (request, {})) as prepare:
            for reason in ["stop", "length", "content_filter"]:
                response.choices[0].finish_reason = reason
                if reason == "stop":
                    gpt.request_completion("prompt", api_key="dummy", model="fixture", api_base="https://model.invalid", api_version="", temperature=0, max_tokens=100, timeout=5)
                else:
                    with self.assertRaisesRegex(RuntimeError, "incomplete or filtered"):
                        gpt.request_completion("prompt", api_key="dummy", model="fixture", api_base="https://model.invalid", api_version="", temperature=0, max_tokens=100, timeout=5)
                self.assertIs(prepare.call_args.args[1]["allow_context_truncation"], False)

    def test_ops_schema_repair_preserves_report_and_never_truncates(self):
        args = SimpleNamespace(artifact_retries=1, model_url="https://model.invalid", model_api_key="dummy", model="fixture", api_version="", temperature=0, max_tokens=100)
        bad = artifacts(PG_SCRIPT.replace(": 60", ": 1801"))
        with tempfile.TemporaryDirectory() as tmp, \
             patch.object(ops, "chat_completion", side_effect=[(json.dumps(bad), {}), (json.dumps(artifacts(MYSQL_SCRIPT)), {})]) as chat:
            action, _ = ops.generate_artifact_action(args, "frozen prompt", REPORT, Path(tmp))
            attempts = json.loads((Path(tmp) / "artifact_attempts.json").read_text())
        self.assertEqual(action["diagnosis_report.md"], REPORT)
        self.assertEqual([item["status"] for item in attempts], ["invalid", "valid"])
        self.assertTrue(all(call.kwargs["allow_context_truncation"] is False for call in chat.call_args_list))
        self.assertIn("guarded target-database recovery", chat.call_args.args[0][0]["content"])

    def test_ops_transport_forwards_context_policy_without_changing_diagnosis_default(self):
        response = {"choices": [{"message": {"content": "fixture"}, "finish_reason": "stop"}]}
        def request(url, payload, key, **kwargs):
            kwargs["prepare_payload"](payload)
            return response
        with patch.object(ops, "request_json", side_effect=request), \
             patch.object(ops, "prepare_request", side_effect=lambda payload, options: (payload, {})) as prepare:
            for allowed in [False, True]:
                kwargs = {"allow_context_truncation": False} if not allowed else {}
                ops.chat_completion([{"role": "user", "content": "prompt"}], url="https://model.invalid", api_key="dummy", model="fixture", api_version="", temperature=0, max_tokens=100, **kwargs)
                self.assertIs(prepare.call_args.args[1]["allow_context_truncation"], allowed)

    def test_oversized_evidence_fails_instead_of_silently_losing_workload(self):
        request = {"model": "fixture", "messages": [{"role": "user", "content": "x" * 4096}], "max_tokens": 10}
        with patch.object(llm_api, "_encoding", return_value=None), self.assertRaises(llm_api.ContextBudgetError):
            llm_api.prepare_request(request, {"context_window_tokens": 1024, "context_margin_tokens": 0, "minimum_input_tokens": 10, "allow_context_truncation": False})
        self.assertEqual(len(request["messages"][0]["content"]), 4096)


class ConversionIntegrationTest(unittest.TestCase):
    setUp = EvidencePolicyTest.setUp

    def args(self):
        return argparse.Namespace(sample_dir=self.sample, output_dir=self.root / "output", snapshot=self.snapshot,
            dbms="pg", context_chars=1, knowledge_url="https://kb.invalid", knowledge_api_key="dummy-kb",
            dataset_id="2104", retries=1, model_url="https://model.invalid", model_api_key="dummy-model",
            model="fixture", api_version="", temperature=0, max_tokens=1024)

    def test_gpt_main_freezes_report_and_writes_auditable_effective_workload(self):
        result = self.root / "native_result.json"
        result.write_text(json.dumps({"report": REPORT}))
        output = self.root / "gpt-output"
        args = argparse.Namespace(diagnosis_result=result, sample_dir=self.sample, output_dir=output,
            snapshot=self.snapshot, context_chars=1, model="fixture", api_base="https://model.invalid",
            api_version="", temperature=0, max_tokens=1024, retry_times=1)
        with patch.object(gpt, "parse_args", return_value=args), patch.dict(os.environ, {"OPENAI_API_KEY": "dummy"}), \
             patch.object(gpt, "request_completion", return_value=(json.dumps(artifacts()), "{}")):
            self.assertEqual(gpt.main(), 0)
        self.assert_output(output, REPORT)

    def test_ops_run_accepts_two_field_recovery_and_freezes_native_report(self):
        args = self.args()
        with patch.object(ops, "retrieve_knowledge", return_value=(["版本：PG"], [])), \
             patch.object(ops, "chat_completion", side_effect=[(REPORT, {}), (json.dumps(artifacts()), {})]):
            ops.run(args)
        self.assert_output(args.output_dir, REPORT.rstrip() + "\n")
        trace = json.loads((args.output_dir / "native_trace.json").read_text())
        self.assertEqual(trace["artifact_status"], "completed")
        self.assertEqual(trace["recovery_conversion_version"], policy.VERSION)

    def test_ops_invalid_evidence_keeps_native_report_and_records_failed_conversion(self):
        (self.sample / "config.json").write_text('[]')
        args = self.args()
        with patch.object(ops, "retrieve_knowledge", return_value=(["版本：PG"], [])), \
             patch.object(ops, "chat_completion", return_value=(REPORT, {})), \
             patch.object(ops, "generate_artifact_action") as generate:
            with self.assertRaises(ValueError):
                ops.run(args)
            generate.assert_not_called()
        self.assertTrue((args.output_dir / "native_diagnosis_report.md").is_file())
        self.assertEqual(json.loads((args.output_dir / "native_trace.json").read_text())["artifact_status"], "failed")
        self.assertFalse((args.output_dir / "artifact_action.json").exists())

    def assert_output(self, output, report):
        self.assertEqual((output / "diagnosis_report.md").read_bytes(), report.encode())
        self.assertEqual((output / "optimized_verified_workload.sql").read_bytes(), ORIGINAL.encode())
        self.assertEqual((output / "solution_script.sh").read_bytes(), PG_SCRIPT.encode())
        prompt = (output / "conversion_prompt.txt").read_text()
        self.assertNotIn("SECRET_", prompt)
        meta = json.loads((output / "conversion_meta.json").read_text())
        self.assertEqual(meta["execution_status"], "not_executed")
        for filename, expected in meta["artifact_sha256"].items():
            self.assertEqual(hashlib.sha256((output / filename).read_bytes()).hexdigest(), expected)
        for filename in ["conversion_meta.json", "conversion_prompt.txt"]:
            self.assertEqual((output / filename).stat().st_mode & 0o777, 0o600)


if __name__ == "__main__":
    def deny_live_operations(event, args):
        if event in {"socket.connect", "socket.getaddrinfo"}:
            raise AssertionError("Offline tests must not contact a database/model/service")
        if event == "subprocess.Popen":
            command = args[1]
            if not isinstance(command, (tuple, list)) or list(command[:2]) != ["bash", "-n"]:
                raise AssertionError("Only Bash syntax checking is allowed: " + repr(command))
    sys.addaudithook(deny_live_operations)
    unittest.main()
