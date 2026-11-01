"""Offline contracts between Codex, shared callers, launchers, and evaluation."""
import asyncio
import contextlib
import importlib
import inspect
import io
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from Baselines.Codex import agent, codex, diagnosis_mcp, react, schemas
from Baselines.Claude_Code import diagnosis_mcp as claude_mcp
from Baselines import preparation
from Evaluation import test_baseline_eval as evaluation

ROOT = Path(__file__).resolve().parents[3]


class InterfaceContracts(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.sample = self.root / 'samples' / '7'
        self.sample.mkdir(parents=True)
        self.case = {'anomaly_id': 7, 'database_name': 'fixture', 'dbms': 'PostgreSQL 16.11'}
        for name in ('user_phenomenon_description.txt', 'initialization_workload.sql',
                     'verified_workload.sql', 'data_modification.sql', 'anomaly_inject.sh'):
            (self.sample / name).write_text('-- offline fixture\n')
        (self.sample / 'config.json').write_text(json.dumps(self.case))
        self.index = self.sample.parent / 'key_information.json'
        self.index.write_text(json.dumps([self.case]))
        self.config = json.loads((ROOT / 'Baselines/Codex/config.example.json').read_text())
        self.config.update(key_information_path=str(self.index), output_base_dir=str(self.root / 'output'))
        self.path = self.root / 'standalone.json'
        self.path.write_text(json.dumps(self.config))

    def test_entrypoint_preserves_driver_retry_defaults(self):
        self.config['llm_configuration'].pop('retry_times')
        self.path.write_text(json.dumps(self.config))
        output = io.StringIO()
        with contextlib.redirect_stdout(output), patch.object(codex, 'preflight') as preflight:
            codex.main(['--config', str(self.path), '--dry-run'])
        self.assertIn('Validated 1 samples', output.getvalue())
        preflight.assert_not_called()
        self.assertFalse((self.root / 'output').exists())

    def test_preflight_accepts_runtime_only_config(self):
        config = {'llm_configuration': self.config['llm_configuration']}
        self.path.write_text(json.dumps(config))
        with contextlib.redirect_stdout(io.StringIO()), \
                patch.object(codex, 'preflight', return_value={'ready': True}) as preflight, \
                patch.object(react, 'run') as run:
            codex.main(['--config', str(self.path), '--preflight'])
        preflight.assert_called_once_with(config)
        run.assert_not_called()

    def test_no_argument_main_remains_callable(self):
        with patch.object(sys, 'argv', ['caller', '--config', str(self.path), '--dry-run']), \
                contextlib.redirect_stdout(io.StringIO()), patch.object(codex, 'preflight') as preflight:
            codex.main()
        preflight.assert_not_called()

    def test_existing_positional_and_keyword_driver_calls(self):
        for mode in ('positional', 'keyword'):
            with self.subTest(mode=mode), patch.object(sys, 'argv', ['caller', '--dry-run']), \
                    contextlib.redirect_stdout(io.StringIO()):
                if mode == 'positional':
                    react.run(True, self.path, 'Codex', react.request_llm, 'codex', None, agent.run_agent)
                else:
                    react.run(privileged=True, default_config=self.path, description='Codex',
                              agent_runner=agent.run_agent, workflow_type='codex')
        self.assertFalse((self.root / 'output').exists())

    def test_public_agent_contracts_bind_existing_call_patterns(self):
        evaluation.check_signatures(agent)
        inspect.signature(agent.run_agent).bind('prompt', {}, 'container', self.root, {}, None)
        inspect.signature(agent.validate_config).bind({})
        inspect.signature(agent.preflight).bind({}, require_key=False)
        self.assertIs(evaluation.react, react)
        self.assertEqual(evaluation.METHOD_MODULES['codex'], 'Baselines.Codex.agent')
        self.assertEqual(preparation.ENTRYPOINTS['codex'].resolve(), Path(codex.__file__).resolve())

    def test_codex_modules_are_importable_by_other_packages(self):
        for name in ('agent', 'chat_transport', 'cli_runtime', 'codex', 'diagnosis_mcp',
                     'knowledge', 'native_agent', 'react', 'sandbox', 'schemas',
                     'subagents', 'tools', 'transport', 'workspace'):
            with self.subTest(module=name):
                self.assertIsNotNone(importlib.import_module('Baselines.Codex.' + name))

    def test_claude_mcp_reexports_same_server_and_schemas(self):
        self.assertIs(claude_mcp.main, diagnosis_mcp.main)
        self.assertIs(claude_mcp.make_server, diagnosis_mcp.make_server)
        self.assertIs(claude_mcp.ExecuteInput, schemas.ExecuteInput)
        self.assertIs(claude_mcp.FinishInput, schemas.FinishInput)
        (self.root / 'mcp').mkdir()
        server, tools = claude_mcp.make_server({
            'output_dir': str(self.root / 'mcp'), 'project_root': str(ROOT),
            'container_name': 'offline_fixture', 'owner_prefix': 'offline',
            'seal': {'container_id': 'offline'}, 'hidden_mounts': [],
            'enable_specialists': False, 'enable_web_docs': False,
        })
        self.addCleanup(tools.specialists.close)
        offered = asyncio.run(server.list_tools())
        self.assertEqual({tool.name for tool in offered}, tools.enabled)
        self.assertIn('db_execute', tools.enabled)
        self.assertNotIn('agent_spawn', tools.enabled)

    def evaluation_config(self, method='codex'):
        config = json.loads((ROOT / f'Configs/evaluation/{method}/example.json').read_text())
        config.update(key_information_path=str(self.index), output_root_dir=str(self.root / 'evaluation'))
        for section, key in (('baseline', 'llm_configuration'), ('eval', 'judge_llm_configuration')):
            config[section][key].update(api_key='offline', api_key_env='OFFLINE_INTERFACE_KEY',
                                        base_url='https://example.invalid/v1', model_name='offline-fixture')
        path = self.root / f'{method}_evaluation.json'
        path.write_text(json.dumps(config))
        return path

    def test_evaluation_loads_config_and_passes_agent_callback(self):
        path = self.evaluation_config()
        with patch.dict(os.environ, {'OFFLINE_INTERFACE_KEY': 'offline'}):
            config = evaluation.load_config(path)
        self.assertIs(config['method_module'], agent)
        pipeline = evaluation.Pipeline(config, logging.getLogger('offline-interfaces'))
        output = config['output_root'] / '7' / 'baseline'
        captured = {}
        signature = inspect.signature(react._run_case_body)

        def run_case(*args, **kwargs):
            captured.update(signature.bind(*args, **kwargs).arguments)
            captured['containers'].append('offline_worker')
            return 'offline_worker'

        with patch.object(react, '_run_case_body', side_effect=run_case), \
                patch.object(react, 'build_container_config', return_value={}), \
                patch.object(react, 'kill_tmux_session') as kill, \
                patch.object(react, 'force_remove_container') as remove:
            pipeline.run_diagnosis(0, self.case, output, pipeline.logger, 'offline', 35000)
        self.assertIs(captured['agent_runner'], agent.run_agent)
        self.assertEqual(captured['output_dir'], output)
        self.assertEqual(captured['case'], self.case)
        kill.assert_called_once_with('offline_worker')
        remove.assert_called_once_with('offline_worker')

    def test_finish_artifacts_match_evaluator_paths(self):
        output = self.root / '7' / 'baseline'
        output.mkdir(parents=True)
        original = 'SELECT 1;\n'
        react.save_finish({'action': 'finish', 'diagnosis_report.md': 'Offline evidence',
                           'solution_script.sh': '# TIMEOUT THRESHOLD: 10\ntrue\n',
                           'verified_workload.sql': ''}, output, original)
        paths = evaluation.baseline_artifact_paths(str(self.root), '7', 0)
        for key in ('diagnosis_report', 'solution_script', 'optimized_workload'):
            self.assertTrue(Path(paths[key]).is_file(), key)
        self.assertEqual(Path(paths['optimized_workload']).read_text(), original)

    def test_standalone_script_and_module_dry_run(self):
        env = dict(os.environ, PYTHONDONTWRITEBYTECODE='1')
        for target, cwd in (([str(Path(codex.__file__).resolve())], self.root),
                            (['-m', 'Baselines.Codex.codex'], ROOT)):
            with self.subTest(target=target):
                result = subprocess.run([sys.executable, '-B', *target, '--config', str(self.path), '--dry-run'],
                                        cwd=cwd, env=env, text=True, capture_output=True, timeout=30)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn('Validated 1 samples', result.stdout)
        self.assertFalse((self.root / 'output').exists())

    def test_codex_and_other_method_evaluation_dry_runs(self):
        env = dict(os.environ, PYTHONDONTWRITEBYTECODE='1', OFFLINE_INTERFACE_KEY='offline')
        for method, filename in (('codex', 'test_baseline_eval.py'), ('react', 'test_baseline_eval.py')):
            with self.subTest(method=method):
                path = self.evaluation_config(method)
                result = subprocess.run([sys.executable, '-B', str(ROOT / 'Evaluation' / filename),
                                         '--config', str(path), '--dry-run'],
                                        cwd=self.root, env=env, text=True, capture_output=True, timeout=30)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn('1 sample(s)', result.stdout)
        self.assertFalse((self.root / 'evaluation').exists())


if __name__ == '__main__':
    unittest.main()
