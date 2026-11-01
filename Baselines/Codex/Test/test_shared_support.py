"""Offline regressions for the shared preparation and container boundary.

Fixtures live in temporary directories. Docker, database and model operations
are never executed.
"""
import importlib
import importlib.util
import inspect
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from Baselines import container_boundary as boundary
from Baselines import preparation


ROOT = Path(__file__).resolve().parents[3]


class PreparationContracts(unittest.TestCase):
    def test_package_and_support_imports_do_not_load_diagnosis_drivers(self):
        code = (
            "import sys; import Baselines; "
            "from Baselines import preparation, container_boundary; "
            "assert '__getattr__' not in vars(Baselines); "
            "assert not any(name in vars(Baselines) "
            "for name in ('react', 'react_common', 'react_super')); "
            "assert 'docker_func' not in sys.modules; "
            "assert 'llm_api' not in sys.modules; "
            "assert 'Baselines.ReAct.react' not in sys.modules"
        )
        result = subprocess.run(
            [sys.executable, "-B", "-c", code], cwd=ROOT,
            capture_output=True, text=True, timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_all_drivers_share_the_migrated_helpers(self):
        for name in ("ReAct", "Codex", "Claude_Code"):
            with self.subTest(driver=name):
                driver = importlib.import_module(f"Baselines.{name}.react")
                self.assertIs(driver.diagnosis_llm_config, preparation.diagnosis_llm_config)
                self.assertIs(driver.direct_injection_command, preparation.direct_injection_command)
                self.assertIs(driver.staged_diagnosis, boundary.staged_diagnosis)
                self.assertIs(driver.sanitize_before_diagnosis, boundary.sanitize_before_diagnosis)
                # Claude's existing driver performs data preparation directly;
                # merging modules must not change that method's execution policy.
                if name in ("ReAct", "Codex"):
                    self.assertIs(driver.run_data_preparation, preparation.run_data_preparation)
                    self.assertIs(driver.PreparationError, preparation.PreparationError)
        from Evaluation import test_eval
        self.assertIs(test_eval.SampleStage, boundary.SampleStage)

    def test_native_adapters_import_the_existing_react_package(self):
        from Baselines.ReAct import react
        for relative in ("DB-GPT/db_gpt.py", "DBAIOps/dbaiops.py"):
            with self.subTest(adapter=relative):
                spec = importlib.util.spec_from_file_location(
                    "_offline_shared_adapter", ROOT / "Baselines" / relative,
                )
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                self.assertIs(module.react, react)

    def test_suite_help_works_as_script_and_module(self):
        with tempfile.TemporaryDirectory() as temporary:
            env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
            env.pop("PYTHONPATH", None)
            for args, cwd in (
                ([str(ROOT / "Baselines/preparation.py")], temporary),
                (["-m", "Baselines.preparation"], ROOT),
            ):
                with self.subTest(entry=args):
                    result = subprocess.run(
                        [sys.executable, "-B", *args, "--help"], cwd=cwd,
                        env=env, capture_output=True, text=True, timeout=30,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
                    for option in ("--framework", "--config-dir", "--output-dir"):
                        self.assertIn(option, result.stdout)
            self.assertEqual(list(Path(temporary).iterdir()), [])

    def test_llm_defaults_preserve_explicit_options_without_mutation(self):
        config = {"retry_times": 2, "timeout": 99, "sdk_max_retries": 1}
        result = preparation.diagnosis_llm_config(config)
        self.assertIsNot(result, config)
        self.assertEqual(config, {"retry_times": 2, "timeout": 99, "sdk_max_retries": 1})
        for key, value in config.items():
            self.assertEqual(result[key], value)
        self.assertEqual(result["request_retry_times"], 49)
        self.assertEqual(result["request_retry_delay_seconds"], 30)
        self.assertEqual(preparation.diagnosis_llm_config({})["retry_times"], 50)

    def test_injection_command_keeps_dbms_environment_and_single_execution(self):
        user = {"DB_HOST": "127.0.0.1", "DB_USER": "fixture", "DB_PASSWORD": "a b;$"}
        for dbms, port in (("PostgreSQL 16", "5432"), ("MySQL 8.0", "3306")):
            with self.subTest(dbms=dbms):
                command = preparation.direct_injection_command(
                    Path("/fixture/anomaly_inject.sh"), dbms, "fixture_db", "public", user, 60,
                )
                self.assertEqual(command[:4], ["timeout", "--signal=TERM", "--kill-after=30s", "60"])
                self.assertEqual(command[-2:], ["bash", "/fixture/anomaly_inject.sh"])
                self.assertEqual(command.count("bash"), 1)
                environment = dict(value.split("=", 1) for value in command[5:-2])
                self.assertEqual(environment["DB_PORT"], port)
                self.assertEqual(environment["DB_PASSWORD"], user["DB_PASSWORD"])
                self.assertEqual("PGOPTIONS" in environment, port == "5432")
        with self.assertRaises(ValueError):
            preparation.direct_injection_command("/fixture/inject", "pg", "db", "public", user, 0)

    def test_data_preparation_deadlines_and_executor_contract(self):
        for budget, requested, expected in ((1800, 600, 1800), (600, 2400, 2400)):
            with self.subTest(budget=budget), patch.dict(
                os.environ, {"DB_DIAG_DATA_PREPARATION_TIMEOUT_SECONDS": str(budget)},
            ):
                executor = Mock(return_value=(True, "done"))
                result = preparation.run_data_preparation(
                    executor, "fixture", ["sql"], "/tmp/offline.log", requested,
                    session_name="fixture_session",
                )
                self.assertEqual(result, (True, "done"))
                executor.assert_called_once_with(
                    "fixture", ["sql"], "/tmp/offline.log", expected,
                    session_name="fixture_session",
                )
        for budget in ("599", "7201", "invalid"):
            with self.subTest(budget=budget), patch.dict(
                os.environ, {"DB_DIAG_DATA_PREPARATION_TIMEOUT_SECONDS": budget},
            ):
                executor = Mock()
                with self.assertRaises(ValueError):
                    preparation.run_data_preparation(executor, "fixture", [], "/tmp/offline.log")
                executor.assert_not_called()

    def test_preparation_failure_classification_is_preserved(self):
        error = preparation.PreparationError("data_modification", "fixture failed")
        self.assertEqual(error.phase, "data_modification")
        self.assertEqual(str(error), "fixture failed")
        self.assertTrue(preparation.preparation_failure({"status": "preparation_failed"}))
        for prefix in ("Data modification failed:", "Anomaly injection failed:",
                       "Unable to initialize Docker container `", "Unable to initialize database `"):
            self.assertTrue(preparation.preparation_failure({"status": "failed", "error": prefix}))
        for status in (None, {}, {"status": "completed"}, {"status": "failed", "error": "agent failed"}):
            self.assertFalse(preparation.preparation_failure(status))

    def test_suite_without_preflight_evidence_never_starts_workers(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "output"
            argv = ["preparation", "--config-dir", temporary, "--output-dir", str(output)]
            with patch.object(sys, "argv", argv), \
                    patch.object(preparation, "OUTPUT", output), \
                    patch.object(preparation, "CONFIGS", Path(temporary)), \
                    patch.object(preparation.subprocess, "Popen") as popen, \
                    patch("builtins.print"):
                self.assertEqual(preparation.main(), 1)
                popen.assert_not_called()
            states = json.loads((output / "suite_status.json").read_text())
            self.assertEqual(set(states), set(preparation.FRAMEWORKS))
            self.assertTrue(all(s["status"] == "blocked_by_preflight" for s in states.values()))


class BoundaryContracts(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.project = self.root / "project"
        self.project.mkdir()
        (self.project / "Database_Instances").mkdir()
        self.source = self.root / "samples/7"
        self.source.mkdir(parents=True)
        self.contents = {name: "original " + name for name in boundary.PUBLIC_NAMES}
        self.contents.update({"anomaly_inject.sh": "private injector", "data_modification.sql": "private SQL"})
        for name, text in self.contents.items():
            (self.source / name).write_text(text)
        for name in ("PROJECT_ROOT", "ROOT"):
            patcher = patch.object(boundary, name, self.project)
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_staging_refresh_and_sanitization_preserve_the_source(self):
        with boundary.SampleStage(self.source, "7") as stage:
            self.assertEqual({p.name for p in stage.path.iterdir()}, set(self.contents))
            (stage.path / "verified_workload.sql").write_text("changed")
            (stage.path / "private_extra").mkdir()
            (stage.path / "private_extra/secret").write_text("secret")
            stage.sanitize()
            self.assertEqual({p.name for p in stage.path.iterdir()}, boundary.PUBLIC_NAMES)
            for name in boundary.PUBLIC_NAMES:
                self.assertEqual((stage.path / name).read_text(), self.contents[name])
            stage.refresh()
            self.assertEqual({p.name for p in stage.path.iterdir()}, set(self.contents))
        self.assertIsNone(stage.lock)
        self.assertEqual({p.name for p in stage.path.iterdir()}, boundary.PUBLIC_NAMES)
        self.assertEqual({p.name: p.read_text() for p in self.source.iterdir()}, self.contents)

    def test_staging_lock_prevents_concurrent_copy_and_is_released(self):
        with boundary.SampleStage(self.source, "7"):
            with self.assertRaises(BlockingIOError):
                with boundary.SampleStage(self.source, "7"):
                    self.fail("Second staging lock must not be acquired")
        with boundary.SampleStage(self.source, "7") as stage:
            self.assertTrue(stage.entered)

    def test_staging_rejects_unsafe_ids_in_project_sources_and_links(self):
        for sample_id in ("../7", "/tmp/7", ""):
            with self.subTest(sample_id=sample_id), self.assertRaises(ValueError):
                boundary.SampleStage(self.source, sample_id)
        with self.assertRaises(ValueError):
            boundary.SampleStage(self.project, "7")
        (self.source / "linked_private").symlink_to(self.source / "anomaly_inject.sh")
        with self.assertRaises(ValueError):
            with boundary.SampleStage(self.source, "7"):
                self.fail("Linked sample input must be rejected")

    def test_sanitization_failure_remains_a_boundary_error(self):
        stage = boundary.SampleStage(self.source, "7")
        with patch.object(stage, "_sanitize", side_effect=OSError("fixture error")):
            with self.assertRaises(boundary.BoundaryError):
                stage.sanitize()

    def test_container_visibility_uses_shared_read_only_probes(self):
        with boundary.SampleStage(self.source, "7") as stage:
            metadata = [{"Mounts": [{"Type": "bind", "Source": str(stage.path),
                                    "Destination": str(stage.path)}]}]
            names = "\n".join(sorted(boundary.PUBLIC_NAMES)) + "\n"
            view = "7\n\n--FILES--\n" + names + "\n--ALL--\n" + names
            with patch.object(boundary, "read_only_probe", side_effect=[json.dumps(metadata), view]) as probe:
                stage.verify_container("offline_container")
                self.assertEqual(probe.call_count, 2)
            with patch.object(boundary, "read_only_probe",
                              side_effect=[json.dumps(metadata), view + "private_file\n"]):
                with self.assertRaises(RuntimeError):
                    stage.verify_container("offline_container")

    def test_staged_diagnosis_preserves_signature_context_and_public_files(self):
        files = {
            "config": self.source / "config.json",
            "phenomenon": self.source / "user_phenomenon_description.txt",
            "initialization": self.source / "initialization_workload.sql",
            "verified": self.source / "verified_workload.sql",
            "inject": self.source / "anomaly_inject.sh",
            "modification": self.source / "data_modification.sql",
        }
        containers = []
        events = []

        def diagnose(case, files, container_config, logger, containers=None):
            stage = boundary.CURRENT_STAGE.get()
            self.assertIsNotNone(stage)
            self.assertEqual(files["config"].parent, stage.path)
            self.assertEqual(container_config["_sample_staging_dir"], str(stage.path))
            containers.append("offline_container")
            return boundary.sanitize_before_diagnosis(files, "offline_container")

        wrapped = boundary.staged_diagnosis(diagnose)
        self.assertEqual(inspect.signature(wrapped), inspect.signature(diagnose))
        with patch.object(boundary.SampleStage, "verify_container",
                          side_effect=lambda _: events.append("verify")), \
                patch.object(boundary, "seal", side_effect=lambda _: events.append("seal")):
            result = wrapped({"sample_id": "7"}, files, {"memory": "1g"}, None, containers)
        self.assertEqual(set(result), boundary.PUBLIC_KEYS)
        self.assertEqual(events, ["verify", "seal"])
        self.assertIsNone(boundary.CURRENT_STAGE.get())
        self.assertEqual(files["config"].parent, self.source)
        self.assertTrue(files["inject"].is_file())

    def test_staged_diagnosis_cleans_up_after_an_exception(self):
        def diagnose(case, files, container_config, logger):
            raise RuntimeError("offline failure")

        wrapped = boundary.staged_diagnosis(diagnose)
        with self.assertRaisesRegex(RuntimeError, "offline failure"):
            wrapped(case={"anomaly_id": 7}, files={"config": self.source / "config.json"},
                    container_config={}, logger=None)
        self.assertIsNone(boundary.CURRENT_STAGE.get())
        staged = self.project / "DiagDBench/7"
        self.assertEqual({p.name for p in staged.iterdir()}, boundary.PUBLIC_NAMES)
        with boundary.SampleStage(self.source, "7"):
            pass

    def test_diagnosis_requires_active_staging(self):
        with self.assertRaisesRegex(RuntimeError, "without isolated sample staging"):
            boundary.sanitize_before_diagnosis({}, "offline_container")

    def test_docker_command_delegation_and_resource_flags_are_preserved(self):
        command = ["docker", "run", "--memory=1g", "--cpus=2",
                   "-v", "/private:/private", "fixture-image"]
        self.assertIs(boundary.isolated_docker_command(command, {}), command)
        with boundary.SampleStage(self.source, "7") as stage:
            config = stage.container_config({})
            with patch.object(boundary, "scoped_run_command", return_value=["sentinel"]) as scoped:
                self.assertEqual(boundary.isolated_docker_command(command, config), ["sentinel"])
                scoped.assert_called_once_with(command, config)
            scoped = boundary.isolated_docker_command(command, config)
            self.assertIn("--memory=1g", scoped)
            self.assertIn("--cpus=2", scoped)
            self.assertNotIn("/private:/private", scoped)
            self.assertIn(f"type=bind,source={stage.path},target={stage.path}", scoped)
            self.assertEqual(command[-1], scoped[-1])

    def test_only_read_only_boundary_timeouts_are_retried(self):
        timeout = subprocess.TimeoutExpired(["probe"], 180)
        success = SimpleNamespace(returncode=0, stdout="ok", stderr="")
        with patch.object(boundary.subprocess, "run", side_effect=[timeout, success]) as run, \
                patch.object(boundary.time, "sleep"):
            self.assertEqual(boundary.read_only_probe(["probe"], operation="offline probe"), "ok")
            self.assertEqual(run.call_count, 2)
        with patch.object(boundary.subprocess, "run", side_effect=timeout) as run:
            with self.assertRaises(boundary.BoundaryError):
                boundary._run(["mutating"], operation="offline setup")
            run.assert_called_once()


if __name__ == "__main__":
    unittest.main()
