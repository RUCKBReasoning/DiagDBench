"""Offline regressions for the Codex entry point and shared runtime naming."""
import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from Baselines.Codex import agent, codex, react, sandbox
from Baselines.Codex.tools import Tools
from Baselines.Codex.workspace import CONTROL_DIRECTORY
from Baselines.Claude_Code import native_agent as claude_agent


class CodexEntrypoint(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = json.loads(Path(codex.__file__).with_name('config.example.json').read_text())
        self.config_path = self.root / 'config.json'
        self.config_path.write_text(json.dumps(self.config))

    def invoke(self, *args):
        output = io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
            codex.main(list(args))
        return output.getvalue()

    def test_single_codex_entrypoint(self):
        entries = {p.name for p in Path(codex.__file__).parent.glob('codex*.py')}
        self.assertEqual(entries, {'codex.py'})

    def test_help_needs_no_configuration_or_runtime(self):
        with patch.object(codex, 'preflight') as check, self.assertRaises(SystemExit) as caught:
            self.invoke('--help')
        self.assertEqual(caught.exception.code, 0)
        check.assert_not_called()

    def test_missing_or_invalid_config_is_an_argument_error(self):
        for content in (None, '{', '[]', '{}', 'null'):
            with self.subTest(content=content):
                if content is None:
                    self.config_path.unlink(missing_ok=True)
                else:
                    self.config_path.write_text(content)
                with self.assertRaises(SystemExit) as caught:
                    self.invoke('--config', str(self.config_path), '--dry-run')
                self.assertEqual(caught.exception.code, 2)

    def test_check_modes_are_mutually_exclusive(self):
        with self.assertRaises(SystemExit) as caught:
            self.invoke('--config', str(self.config_path), '--dry-run', '--preflight')
        self.assertEqual(caught.exception.code, 2)

    def test_dry_run_forwards_absolute_config_and_canonical_method(self):
        with contextlib.chdir(self.root), patch.object(codex, 'preflight') as check, \
                patch.object(react, 'run') as run:
            self.invoke('--config', 'config.json', '--dry-run')
        check.assert_not_called()
        kwargs = run.call_args.kwargs
        self.assertEqual(kwargs['workflow_type'], 'codex')
        self.assertEqual(kwargs['default_config'], self.config_path.resolve())
        self.assertEqual(kwargs['argv'], ['--config', str(self.config_path.resolve()), '--dry-run'])
        self.assertIs(kwargs['agent_runner'], agent.run_agent)

    def test_preflight_does_not_run_samples(self):
        with patch.object(codex, 'preflight', return_value={'ready': True}) as check, \
                patch.object(react, 'run') as run:
            output = self.invoke('--config', str(self.config_path), '--preflight')
        check.assert_called_once_with(self.config)
        run.assert_not_called()
        self.assertEqual(json.loads(output), {'ready': True})

    def test_normal_run_checks_runtime_before_dispatch(self):
        events = []
        with patch.object(codex, 'preflight', side_effect=lambda _: events.append('check')), \
                patch.object(react, 'run', side_effect=lambda **_: events.append('run')):
            self.invoke('--config', str(self.config_path))
        self.assertEqual(events, ['check', 'run'])

    def test_preflight_failure_prevents_dispatch(self):
        with patch.object(codex, 'preflight', side_effect=ValueError('runtime unavailable')), \
                patch.object(react, 'run') as run, self.assertRaisesRegex(ValueError, 'runtime unavailable'):
            self.invoke('--config', str(self.config_path))
        run.assert_not_called()

    def test_real_dry_run_reads_fixture_without_runtime_or_outputs(self):
        dataset = self.root / 'samples'
        sample = dataset / '7'
        sample.mkdir(parents=True)
        for name in ('config.json', 'user_phenomenon_description.txt', 'initialization_workload.sql',
                     'verified_workload.sql', 'data_modification.sql', 'anomaly_inject.sh'):
            (sample / name).write_text('{}' if name == 'config.json' else '-- offline fixture\n')
        index = dataset / 'key_information.json'
        index.write_text(json.dumps([{'sample_id': 7, 'database_name': 'fixture'}]))
        self.config.update(key_information_path='samples/key_information.json', output_base_dir='outputs')
        self.config_path.write_text(json.dumps(self.config))
        with patch.object(react, 'PROJECT_ROOT', self.root), patch.object(codex, 'preflight') as check, \
                patch.object(react, 'run_case') as run_case, patch.dict(os.environ, {}, clear=True):
            output = self.invoke('--config', str(self.config_path), '--dry-run')
        self.assertIn('[Codex] Validated 1 samples', output)
        check.assert_not_called()
        run_case.assert_not_called()
        self.assertFalse((self.root / 'outputs').exists())
        (sample / 'anomaly_inject.sh').unlink()
        with patch.object(react, 'PROJECT_ROOT', self.root), self.assertRaises(SystemExit) as caught:
            self.invoke('--config', str(self.config_path), '--dry-run')
        self.assertEqual(caught.exception.code, 2)


class CodexConfiguration(unittest.TestCase):
    def test_defaults_and_options_do_not_mutate_input(self):
        config = {'codex': {'enable_specialists': False}}
        options = agent.validate_config(config)
        self.assertEqual(options['max_aux_actions'], 100)
        self.assertEqual(options['max_model_calls'], 100)
        self.assertTrue(options['enable_web_docs'])
        self.assertFalse(options['enable_specialists'])
        self.assertEqual(config, {'codex': {'enable_specialists': False}})
        self.assertEqual(agent.validate_config({})['max_specialists'], 4)

    def test_option_block_must_be_an_object(self):
        for value in (None, [], 'invalid', True):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, 'JSON object'):
                agent.validate_config({'codex': value})

    def test_invalid_option_values_are_rejected(self):
        for key, value in [('max_aux_actions', True), ('max_model_calls', 201),
                           ('max_model_tokens', 999), ('max_specialists', 0),
                           ('specialist_turns', 11), ('enable_web_docs', 'yes'),
                           ('enable_specialists', 1)]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                agent.validate_config({'codex': {key: value}})

    def test_example_configuration_passes_both_validators(self):
        config = json.loads(Path(codex.__file__).with_name('config.example.json').read_text())
        agent.validate_config(config)
        react.validate_config(config, requires_superuser=True)

    def test_default_and_custom_credential_environment(self):
        for name in ('CODEX_MODEL_KEY', 'TEST_MODEL_KEY'):
            llm = {'provider': 'openai', 'api_key': ''}
            if name != 'CODEX_MODEL_KEY':
                llm['api_key_env'] = name
            with self.subTest(name=name), patch.dict(os.environ, {name: 'offline-key'}, clear=True):
                result = agent.model_config({'llm_configuration': llm})
            self.assertEqual(result['api_key'], 'offline-key')
            self.assertEqual(result['codex_transport'], 'chat_completions')
            self.assertEqual(llm['api_key'], '')

    def test_explicit_credentials_take_precedence(self):
        with patch.dict(os.environ, {'CODEX_MODEL_KEY': 'environment-key'}, clear=True):
            result = agent.model_config({'llm_configuration': {
                'provider': 'openai', 'api_key': 'direct-key', 'codex_api_key': 'specific-key',
            }})
        self.assertEqual(result['api_key'], 'specific-key')

    def test_missing_credential_is_rejected(self):
        with patch.dict(os.environ, {}, clear=True), self.assertRaisesRegex(ValueError, 'api_key_env'):
            agent.model_config({'llm_configuration': {'provider': 'openai', 'api_key': ''}})


class SharedRuntimeNaming(unittest.TestCase):
    def test_claude_prepare_and_tools_use_same_private_directory(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp)
            files = {}
            for name in ('initialization', 'verified'):
                files[name] = output / f'{name}.sql'
                files[name].write_text('SELECT 1;')
            seal = {'container_id': 'offline-fixture'}
            with patch.object(sandbox.DatabaseSandbox, 'seal', return_value=seal), \
                    patch.object(sandbox, 'command', return_value='[{"Id":"offline-image"}]'):
                context_path, _, _ = claude_agent.prepare('offline', {'docker_name': 'fixture'},
                                                        'fixture_db', output, files)
            tools = Tools(json.loads(context_path.read_text()))
            self.addCleanup(tools.specialists.pool.shutdown, wait=True)
            self.assertEqual(tools.private, output / CONTROL_DIRECTORY)
            self.assertEqual(context_path.parent, tools.private)
            self.assertEqual(CONTROL_DIRECTORY, 'agent_control')
            with self.assertRaisesRegex(ValueError, 'control state exists'):
                claude_agent.prepare('offline', {'docker_name': 'fixture'}, 'fixture_db', output, files)

    def test_execution_marker_matches_process_cleanup(self):
        database = sandbox.DatabaseSandbox('fixture_db', Path('/tmp'), 'fixture')
        with patch.object(sandbox, 'command', return_value='[]') as command, \
                patch('Baselines.container_boundary._namespace_prefix', return_value=['offline-exec']):
            database._execution_processes('fixture-token', terminate=False)
        cleanup_script = command.call_args.args[0]
        self.assertTrue(any('CODEX_EXECUTION_ID=' in str(value) for value in cleanup_script))
        with tempfile.TemporaryDirectory() as temp, \
                patch.object(database, 'verify'), \
                patch.object(database, '_process_snapshot', return_value={}), \
                patch.object(database, '_execution_processes', return_value=[]), \
                patch('Baselines.container_boundary.agent_prefix', return_value=['offline-exec']), \
                patch.object(sandbox, 'bounded_process', return_value={'returncode': 0, 'terminated': False}) as execute:
            database.execute('true', Path(temp) / 'log.txt', 1)
        args = execute.call_args.args[0]
        self.assertTrue(any(value.startswith('CODEX_EXECUTION_ID=') for value in args))


if __name__ == '__main__':
    unittest.main()
