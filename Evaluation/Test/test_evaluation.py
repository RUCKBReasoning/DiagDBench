"""Offline tests for the single reconstructed semantic-Judge evaluation path."""
import contextlib
import copy
import hashlib
import inspect
import io
import json
import logging
import os
import shutil
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from Evaluation import measurement, namespace, test_baseline_eval as pipeline, test_eval as scoring
from Baselines.Codex import react as codex_driver
from Baselines.ReAct import react as react_driver

ROOT = Path(__file__).resolve().parents[2]
LOGGER = logging.getLogger('offline-evaluation')


def workload(seconds=100, value=1):
    statement = {'sql': 'SELECT 1;', 'success': True, 'execution_time': seconds,
                 'result': {'result_type': 'rows', 'columns': ['v'], 'rows': [[value]], 'row_count': 1}}
    run = {'total_time': seconds, 'success': True, 'error_count': 0, 'results': [statement],
           'init_results': [], 'init_total_time': 1, 'init_success': True, 'init_error_count': 0,
           'timed_out': False}
    return dict(copy.deepcopy(run), measured_runs=[copy.deepcopy(run)], all_runs=[copy.deepcopy(run), copy.deepcopy(run)],
                median_runs=1, completed_measured_runs=1, measured_user_times=[seconds], warmup_user_time=seconds)


def model_config():
    return {'provider': 'openai', 'base_url': 'http://offline.invalid/v1', 'api_key': 'offline',
            'model_name': 'offline', 'temperature': 0, 'retry_times': 1}


class ScoreRules(unittest.TestCase):
    def score(self, recovered=None, verdict=1, solution_ok=False, optimized='SELECT 1;'):
        semantic = {'status': 'scored' if verdict is not None else 'unavailable',
                    'score': verdict, 'semantic_equivalent': None if verdict is None else bool(verdict)}
        with patch.object(scoring, 'judge_workload_semantics', return_value=semantic):
            return scoring.score_reinitialized_workload(
                workload(), recovered or workload(50), {'score': .6}, {'mode': 'both'}, LOGGER,
                'SELECT 1;', optimized, solution_ok=solution_ok)

    def test_failed_script_does_not_zero_recovery(self):
        result = self.score()
        self.assertEqual(result['score'], .81)
        self.assertEqual(result['diagnosis_score'], .6)
        self.assertEqual(result['recovery_score'], .9)
        self.assertFalse(result['solution_executed'])
        self.assertFalse(result['script_success_required'])
        self.assertEqual(result['recovery_detail']['zero_reasons'], [])

    def test_successful_and_failed_script_have_identical_score(self):
        a, b = self.score(solution_ok=False), self.score(solution_ok=True)
        for name in ('score', 'diagnosis_score', 'recovery_score', 'recovery_detail'):
            self.assertEqual(a[name], b[name])

    def test_fingerprint_difference_is_not_a_scoring_gate(self):
        self.assertEqual(self.score(workload(50, value=999))['score'], .81)

    def test_semantic_rejection_preserves_zero_gate(self):
        result = self.score(verdict=0)
        self.assertEqual(result['recovery_score'], 0)
        self.assertEqual(result['score'], .18)
        self.assertIn('semantic_mismatch', result['recovery_detail']['zero_reasons'])

    def test_judge_unavailable_is_retryable_not_a_fingerprint_fallback(self):
        result = self.score(verdict=None)
        self.assertEqual(result['status'], 'judge_unavailable')
        self.assertEqual(result['recovery_score'], 0)
        self.assertIn('semantic_judge_unavailable', result['recovery_detail']['zero_reasons'])

    def test_missing_rewrite_not_substituted_as_valid_semantics(self):
        result = self.score(optimized=None)
        self.assertEqual(result['recovery_score'], 0)
        self.assertIn('optimized_workload_unavailable', result['recovery_detail']['zero_reasons'])

    def test_user_errors_still_zero_recovery(self):
        recovered = workload(50)
        recovered.update(success=False, error_count=1)
        result = self.score(recovered)
        self.assertEqual(result['recovery_score'], 0)
        self.assertIn('recovered_user_workload_error', result['recovery_detail']['zero_reasons'])

    def test_warmup_failure_is_not_hidden_by_successful_measured_runs(self):
        recovered = workload(50)
        recovered['all_runs'][0].update(success=False, error_count=1)
        self.assertEqual(self.score(recovered)['recovery_score'], 0)

    def test_timeout_still_zero_recovery(self):
        recovered = workload(50)
        recovered.update(timed_out=True, timeout_phase='user')
        result = self.score(recovered)
        self.assertEqual(result['recovery_score'], 0)
        self.assertIn('user_workload_timeout', result['recovery_detail']['zero_reasons'])

    def test_initialization_errors_still_zero_recovery(self):
        recovered = workload(50)
        recovered['all_runs'][0]['init_results'] = [{'sql': 'invalid', 'success': False, 'error': 'bad SQL'}]
        self.assertEqual(self.score(recovered)['recovery_score'], 0)

    def test_missing_time_never_fabricates_a_measurement(self):
        recovered = workload(50)
        recovered['total_time'] = None
        result = self.score(recovered)
        self.assertEqual(result['recovery_score'], 0)
        self.assertIn('recovered_time_unavailable', result['recovery_detail']['zero_reasons'])

    def test_eight_times_boundary_and_original_piecewise_curve(self):
        for ratio, expected in ((.1, 1), (.2, .96), (.5, .9), (1, .8), (2, .7), (7, .2), (8, 0), (9, 0)):
            with self.subTest(ratio=ratio):
                self.assertEqual(self.score(workload(100 * ratio))['recovery_score'], expected)

    def test_disabled_judge_and_removed_modes_are_rejected(self):
        with self.assertRaises(ValueError):
            scoring.evaluation_options({'workload_judge_enabled': False})
        for mode in ('report', 'solution'):
            with self.assertRaises(ValueError):
                scoring.evaluation_mode({'mode': mode})

    def test_only_single_channel_in_aggregate(self):
        result = scoring.aggregate_scores([dict(self.score(), sample_id='1'),
                                           {'sample_id': '2', 'status': 'not_evaluated'},
                                           {'sample_id': '3', 'status': 'aborted', 'diagnosis_score': .6}])
        self.assertEqual(result['total_sample_num'], 2)
        self.assertEqual(result['overall_score'], .495)
        for name in result:
            self.assertNotIn('original_score', name)
            self.assertNotIn('post_remediation', name)
        self.assertEqual(result['score_channel'], scoring.SCORE_CHANNEL)


class SemanticJudgeVotes(unittest.TestCase):
    def judge(self, responses, original='SELECT 1;', optimized='SELECT 1;'):
        with patch('llm_api.get_llm_response', side_effect=responses) as request, \
                patch.object(scoring, 'prepare_judge_prompt', side_effect=lambda prompt, *_: (prompt, {'output_budget': 1024})):
            result = scoring.judge_workload_semantics(original, optimized, {}, model_config(), 3, LOGGER)
            return result, request

    def test_majority_uses_valid_binary_semantic_verdicts(self):
        yes = json.dumps({'semantic_equivalent': True, 'score': 1, 'reason': 'same program'})
        no = json.dumps({'semantic_equivalent': False, 'score': 0, 'reason': 'different program'})
        for responses, expected in (([yes, no, yes], 1), ([no, yes, no], 0)):
            with self.subTest(expected=expected):
                result, request = self.judge(responses)
                self.assertEqual(result['score'], expected)
                self.assertEqual(request.call_count, 3)
                prompt = request.call_args.args[0][0]
                self.assertIn('same position', prompt.lower())
                self.assertIn('reordering', prompt)
                self.assertIn('supporting evidence only', prompt)

    def test_changed_statement_count_needs_no_model_call(self):
        result, request = self.judge([], optimized='SELECT 1; SELECT 2;')
        self.assertEqual(result['score'], 0)
        request.assert_not_called()

    def test_invalid_responses_are_unavailable_not_a_result_fingerprint_fallback(self):
        invalid = json.dumps({'semantic_equivalent': True, 'score': 0, 'reason': 'inconsistent'})
        result, request = self.judge([invalid] * 6)
        self.assertIsNone(result['score'])
        self.assertEqual(result['status'], 'unavailable')
        self.assertEqual(request.call_count, 6)

    def test_fatal_quota_is_propagated(self):
        with self.assertRaises(scoring.FatalAPIQuotaError):
            self.judge(scoring.FatalAPIQuotaError('offline quota'))

    def test_judge_environment_key_does_not_mutate_configuration(self):
        raw = model_config()
        raw.update(api_key='', api_key_env='OFFLINE_JUDGE_KEY')
        with patch.dict(os.environ, {'OFFLINE_JUDGE_KEY': 'offline-secret'}):
            resolved = scoring.resolve_judge_llm_configuration({'judge_llm_configuration': raw})
        self.assertEqual(resolved['api_key'], 'offline-secret')
        self.assertEqual(raw['api_key'], '')

    def test_method_and_measurement_changes_invalidate_protocol(self):
        shared = scoring.build_evaluation_shared({'judge_llm_configuration': model_config()})
        one = scoring.protocol_digest(dict(shared, workflow_type='codex'), 3)
        two = scoring.protocol_digest(dict(shared, workflow_type='react'), 3)
        five = scoring.protocol_digest(dict(shared, workflow_type='codex'), 5)
        self.assertNotEqual(one, two)
        self.assertNotEqual(one, five)


class SampleLifecycle(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.sample = self.root / 'samples/7'
        self.sample.mkdir(parents=True)
        self.case = {'anomaly_id': 7, 'database_name': 'fixture', 'dbms': 'PostgreSQL 16.11'}
        config = dict(self.case, system_hardware_config={'CPU': 1, 'Memory': 2, 'Memory + Swap': 2, 'Storage': 8},
                      system_user_config={'DB_USER': 'u', 'DB_PASSWORD': 'p', 'DB_HOST': 'localhost', 'DB_PORT': '5432'})
        (self.sample / 'config.json').write_text(json.dumps(config))
        for name, text in {'user_phenomenon_description.txt': 'offline incident', 'initialization_workload.sql': 'SELECT 1;',
                           'verified_workload.sql': 'SELECT 1;', 'data_modification.sql': 'SELECT 1;',
                           'anomaly_inject.sh': '# TIMEOUT THRESHOLD: 10\ntrue\n'}.items():
            (self.sample / name).write_text(text)
        self.index = self.sample.parent / 'key_information.json'
        self.index.write_text(json.dumps([self.case]))
        self.artifacts = self.root / 'artifacts/7'
        self.artifacts.mkdir(parents=True)
        for name, text in {'solution_script.sh': '# TIMEOUT THRESHOLD: 10\nexit 1\n',
                           'optimized_verified_workload.sql': 'SELECT 1;', 'diagnosis_report.md': 'offline evidence'}.items():
            (self.artifacts / name).write_text(text)
        self.shared = dict(scoring.build_evaluation_shared({'judge_llm_configuration': model_config()}),
                           anomaly_directory=str(self.sample.parent), solution_script_directory=str(self.artifacts.parent),
                           output_base_dir=str(self.root / 'scores'), workflow_type='codex')

    def execute(self, injection_ok=True, script_ok=False, recovered=None, semantic=1):
        stage = MagicMock()
        stage.path = self.sample
        stage.container_config.side_effect = lambda config: config
        with contextlib.ExitStack() as stack:
            def mock(module, name, **kwargs):
                return stack.enter_context(patch.object(module, name, **kwargs))
            mock(scoring, 'SampleStage', return_value=stage)
            init = mock(scoring, 'initialize_sample_environment', side_effect=['fixture_clean', 'fixture_injected'])
            mock(scoring, 'build_container_config', return_value={})
            mock(scoring, 'container_connection_config', side_effect=lambda config, _: config)
            mock(scoring, 'compute_diagnosis_artifact_score', return_value={'score': .6})
            measure = mock(scoring, 'evaluate_verified_workload', side_effect=[workload(), recovered or workload(50)])
            def observed_script(container, command, log, *args, **kwargs):
                is_injection = kwargs.get('window_name') == 'injection'
                ok = injection_ok if is_injection else script_ok
                Path(log + scoring.EXIT_CODE_SUFFIX).write_text('0' if ok else '1')
                return ok, 'offline exit status'
            invoke = mock(scoring, 'run_docker_exec_observation', side_effect=observed_script)
            mock(scoring, 'reset_tmux_window')
            mock(scoring, 'wait_for_recovery')
            mock(scoring, 'judge_workload_semantics', return_value={'status': 'scored' if semantic is not None else 'unavailable',
                                                                   'score': semantic})
            cleanup = mock(scoring, 'cleanup_evaluation_container')
            stack.enter_context(patch('Baselines.container_boundary.seal'))
            record = scoring.evaluate_single_sample(0, self.case, self.shared, 'fixture', 35000, LOGGER, 8, median_runs=1)
            return record, init, measure, invoke, cleanup, stage

    def test_script_failure_still_measures_and_only_rebuilds_twice(self):
        record, init, measure, invoke, cleanup, stage = self.execute()
        self.assertFalse(record['is_aborted'], record['sample_detail'])
        self.assertEqual(record['score'], .81)
        self.assertTrue(record['solution_execution_error'])
        self.assertEqual(init.call_count, 2)
        self.assertEqual(measure.call_count, 2)
        self.assertEqual(invoke.call_count, 2)
        self.assertEqual(Path(measure.call_args.args[1]), self.artifacts / 'optimized_verified_workload.sql')
        cleanup.assert_called_once_with('fixture_injected')
        stage.__exit__.assert_called_once()
        detail = scoring.read_json(self.root / 'scores/7/score_detail.json')
        self.assertFalse(detail['solution_executed'])
        self.assertEqual(detail['solution_exit_code'], 1)
        self.assertNotIn('scenarios', detail)
        self.assertFalse((self.root / 'scores/7/post_remediation_capture.json').exists())

    def test_injection_failure_aborts_before_recovery_and_judge(self):
        record, _, measure, invoke, cleanup, _ = self.execute(injection_ok=False)
        self.assertTrue(record['is_aborted'])
        self.assertEqual(record['sample_detail']['recovery_score'], 0)
        self.assertEqual(measure.call_count, 1)
        self.assertEqual(invoke.call_count, 1)
        cleanup.assert_called_once_with('fixture_injected')

    def test_missing_script_is_recorded_but_not_an_automatic_zero(self):
        (self.artifacts / 'solution_script.sh').unlink()
        record, _, measure, invoke, _, _ = self.execute()
        self.assertTrue(record['solution_missing'])
        self.assertEqual(invoke.call_count, 1)
        self.assertEqual(measure.call_count, 2)
        self.assertEqual(record['score'], .81)

    def test_missing_rewrite_does_not_restore_original_scoring_channel(self):
        (self.artifacts / 'optimized_verified_workload.sql').unlink()
        record, _, _, _, _, _ = self.execute()
        self.assertEqual(record['sample_detail']['recovery_score'], 0)
        self.assertIn('optimized_workload_unavailable', record['sample_detail']['recovery_detail']['zero_reasons'])

    def test_unavailable_semantic_judge_retries(self):
        record, *_ = self.execute(semantic=None)
        self.assertTrue(record['is_aborted'])
        self.assertEqual(record['sample_detail']['status'], 'judge_unavailable')

    def test_old_version_cannot_resume(self):
        record, *_ = self.execute()
        root = self.root / 'scores'
        self.assertIsNotNone(scoring.load_terminal_evaluation_record(root, '7', 'both', 8))
        detail = record['sample_detail']
        detail['scoring_version'] = '2026-10-23-v5'
        scoring.write_json_atomic(root / '7/score_detail.json', detail)
        self.assertIsNone(scoring.load_terminal_evaluation_record(root, '7', 'both', 8))

    def test_result_artifact_change_invalidates_resume(self):
        self.execute()
        (self.artifacts / 'solution_script.sh').write_text('changed')
        self.assertIsNone(scoring.load_terminal_evaluation_record(self.root / 'scores', '7', 'both', 8))

    def test_dry_run_script_and_module_use_same_scoring_engine(self):
        config = {'key_information_path': str(self.index), 'solution_script_directory': str(self.artifacts.parent),
                  'output_base_dir': str(self.root / 'dry-output'), 'judge_llm_configuration': model_config()}
        path = self.root / 'scoring.json'
        path.write_text(json.dumps(config))
        for entry, cwd in (([str(ROOT / 'Evaluation/test_eval.py')], self.root),
                           (['-m', 'Evaluation.test_eval'], ROOT)):
            result = subprocess.run([sys.executable, '-B', *entry, '--config', str(path), '--dry-run'],
                                    cwd=cwd, text=True, capture_output=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn(scoring.SCORE_CHANNEL, result.stdout)
        self.assertFalse((self.root / 'dry-output').exists())

    def test_each_diagnosis_method_loads_without_mutating_module_globals(self):
        resolver = scoring.resolve_solution_artifact_paths
        for method in ('codex', 'react', 'db_gpt', 'dbaiops'):
            config = {'key_information_path': str(self.index), 'output_root_dir': str(self.root / method),
                      'baseline': {'workflow_type': method, 'docker_name': method, 'host_port': 35000,
                                   'superuser_config': {'DB_USER': 'root', 'DB_PASSWORD': 'root'},
                                   'llm_configuration': model_config(), 'conda_home': '/offline', 'conda_env': 'offline'},
                      'eval': {'docker_name': 'evaluation', 'host_port': 25000, 'judge_llm_configuration': model_config()}}
            path = self.root / 'pipeline.json'
            path.write_text(json.dumps(config))
            loaded = pipeline.load_config(path)
            obj = pipeline.Pipeline(loaded, LOGGER)
            self.assertIs(obj.driver, codex_driver if method == 'codex' else react_driver)
            self.assertIs(obj.agent_runner, None if method == 'react' else loaded['method_module'].run_agent)
            self.assertIs(pipeline.react, codex_driver)
            self.assertIs(scoring.resolve_solution_artifact_paths, resolver)
            pipeline.check_signatures(loaded['method_module'], obj.driver)

    def test_db_gpt_skips_unsupported_before_any_database(self):
        config = scoring.read_json(self.sample / 'config.json')
        config['dbms'] = 'MySQL 8.0'
        (self.sample / 'config.json').write_text(json.dumps(config))
        self.shared['workflow_type'] = 'db_gpt'
        record, init, measure, invoke, _, _ = self.execute()
        self.assertTrue(record['is_not_evaluated'])
        init.assert_not_called()
        measure.assert_not_called()
        invoke.assert_not_called()


    def standalone_config(self):
        return {'judge_llm_configuration': model_config(), 'docker_name': 'offline', 'host_port': 35000,
                'anomaly_directory': str(self.sample.parent), 'solution_script_directory': str(self.artifacts.parent),
                'output_base_dir': str(self.root / 'scores'), 'anomaly_pairs': [self.case], 'sample_retry_times': 0,
                'workflow_type': 'codex'}

    def test_standalone_resumes_only_matching_protocol_and_source(self):
        self.execute()
        config = self.standalone_config()
        with patch.object(scoring, 'evaluate_single_sample') as evaluate:
            result = scoring.original_workload_execution_performance_eval(config, LOGGER, median_runs=1)
        evaluate.assert_not_called()
        self.assertEqual(result['overall_score'], .81)
        (self.sample / 'verified_workload.sql').write_text('SELECT 2;')
        record = {'is_aborted': False, 'sample_detail': {'sample_id': '7', 'status': 'completed',
                  'score': .81, 'diagnosis_score': .6, 'recovery_score': .9}}
        with patch.object(scoring, 'evaluate_single_sample', return_value=record) as evaluate:
            scoring.original_workload_execution_performance_eval(config, LOGGER, median_runs=1)
        evaluate.assert_called_once()
        archived = list((self.root / 'scores/_attempts').glob('7_*/score_detail.json'))
        self.assertEqual(len(archived), 1)
        self.assertEqual(scoring.read_json(archived[0])['score'], .81)

    def test_changed_ground_truth_or_symptoms_changes_input_digest(self):
        before = scoring.input_digest(self.sample, self.artifacts)
        (self.sample / 'anomaly_description.json').write_text('{"cause":"updated"}')
        after = scoring.input_digest(self.sample, self.artifacts)
        self.assertNotEqual(before, after)
        (self.sample / 'user_phenomenon_description.txt').write_text('updated symptoms')
        self.assertNotEqual(after, scoring.input_digest(self.sample, self.artifacts))
        custom = scoring.input_digest(self.sample, self.artifacts, 'private_truth.json')
        (self.sample / 'private_truth.json').write_text('{"cause":"custom"}')
        self.assertNotEqual(custom, scoring.input_digest(self.sample, self.artifacts, 'private_truth.json'))

    def test_standalone_retries_aborted_measurement_not_completed_failure(self):
        config = dict(self.standalone_config(), sample_retry_times=1)
        records = iter([{'is_aborted': True, 'sample_detail': {'sample_id': '7', 'status': 'aborted'}},
                        {'is_aborted': False, 'sample_detail': {'sample_id': '7', 'status': 'completed',
                         'score': .18, 'diagnosis_score': .6, 'recovery_score': 0.0}}])
        def fake(index, pair, shared, *args, **kwargs):
            record = next(records)
            scoring.write_json_atomic(Path(shared['output_base_dir']) / '7/score_detail.json', record['sample_detail'])
            return record
        with patch.object(scoring, 'evaluate_single_sample', side_effect=fake) as evaluate:
            result = scoring.original_workload_execution_performance_eval(config, LOGGER, median_runs=1)
        self.assertEqual(evaluate.call_count, 2)
        self.assertEqual(result['overall_score'], .18)
        self.assertEqual(len(list((self.root / 'scores/_attempts').glob('7_*'))), 1)

    def test_integrated_stage_uses_local_resolver_and_archives_retry(self):
        obj = object.__new__(pipeline.Pipeline)
        obj.root = self.root / 'integrated'
        obj.logger = LOGGER
        obj.config = {'eval_shared': dict(self.shared, solution_script_directory=str(obj.root)),
                      'eval_retries': 1, 'timeout_times': 8, 'median_runs': 1}
        resolver = scoring.resolve_solution_artifact_paths
        attempts = []
        def fake(index, case, shared, *args, **kwargs):
            attempts.append(shared)
            aborted = len(attempts) == 1
            detail = {'sample_id': '7', 'status': 'aborted' if aborted else 'completed', 'score': .81}
            scoring.write_json_atomic(Path(shared['output_base_dir']) / '7/score_detail.json', detail)
            return {'is_aborted': aborted, 'sample_detail': detail}
        with patch.object(obj, 'terminal_eval_detail', return_value=None), \
                patch.object(scoring, 'cleanup_evaluation_prefix') as cleanup, \
                patch.object(scoring, 'evaluate_single_sample', side_effect=fake) as evaluate:
            detail = obj.stage_eval(0, self.case, '7', LOGGER, 'offline_diagnosis', 'offline_eval', 35000)
        self.assertEqual(evaluate.call_count, 2)
        self.assertEqual(detail['status'], 'completed')
        cleanup.assert_called_once_with('offline_diagnosis')
        self.assertIs(scoring.resolve_solution_artifact_paths, resolver)
        self.assertTrue(all(shared['artifact_resolver'] is pipeline.baseline_artifact_paths for shared in attempts))
        self.assertTrue((obj.root / '7/eval/score_detail.json').is_file())
        self.assertFalse((obj.root / '7/_eval_stage').exists())
        self.assertEqual(len(list((obj.root / '7/_attempts').glob('eval_aborted_*'))), 1)

    def test_all_methods_support_unified_cli_dry_run(self):
        for method in ('codex', 'react', 'db_gpt', 'dbaiops'):
            with self.subTest(method=method):
                config = {'key_information_path': str(self.index), 'output_root_dir': str(self.root / method),
                          'baseline': {'workflow_type': method, 'docker_name': method, 'host_port': 35000,
                                       'superuser_config': {'DB_USER': 'root', 'DB_PASSWORD': 'root'},
                                       'llm_configuration': model_config(), 'conda_home': '/offline', 'conda_env': 'offline'},
                          'eval': {'docker_name': 'evaluation', 'host_port': 25000, 'judge_llm_configuration': model_config()}}
                path = self.root / 'pipeline.json'
                path.write_text(json.dumps(config))
                result = subprocess.run([sys.executable, '-B', str(ROOT / 'Evaluation/test_baseline_eval.py'),
                                         '--method', method, '--config', str(path), '--dry-run'], cwd=self.root,
                                        text=True, capture_output=True, timeout=30)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(scoring.SCORE_CHANNEL, result.stdout)
                self.assertFalse((self.root / method).exists())
        with self.assertRaisesRegex(ValueError, 'does not match'):
            pipeline.main(['--method', 'codex', '--config', str(path), '--dry-run'])


    def test_integrated_resume_validates_semantics_protocol_and_source(self):
        record, *_ = self.execute()
        obj = object.__new__(pipeline.Pipeline)
        obj.root = self.root / 'integrated'
        obj.config = {'eval_shared': self.shared, 'median_runs': 1, 'diagnosis': {'workflow_type': 'codex'}}
        obj.driver, obj.cases, obj.anomaly_root = codex_driver, [self.case], self.sample.parent
        shutil.copytree(self.artifacts, obj.root / '7/baseline')
        output = obj.root / '7/eval'
        detail = record['sample_detail']
        scoring.write_json_atomic(output / 'score_detail.json', detail)
        self.assertIsNotNone(obj.terminal_eval_detail('7', output, LOGGER))
        for key, value in (('scoring_version', 'old'), ('mode', 'report'), ('measurement_median_runs', 5),
                           ('semantic_judge', {'score': None}), ('status', 'not_evaluated')):
            with self.subTest(field=key):
                scoring.write_json_atomic(output / 'score_detail.json', dict(detail, **{key: value}))
                self.assertIsNone(obj.terminal_eval_detail('7', output, LOGGER))
        scoring.write_json_atomic(output / 'score_detail.json', detail)
        (self.sample / 'anomaly_description.json').write_text('{"cause":"changed"}')
        self.assertIsNone(obj.terminal_eval_detail('7', output, LOGGER))


class NamespaceAndMeasurement(unittest.TestCase):
    def test_namespace_manager_points_to_merged_worker(self):
        with patch.object(namespace.os, 'geteuid', return_value=1000), \
                patch.object(namespace.os, 'getuid', return_value=1000), \
                patch.object(namespace.os, 'getgid', return_value=1000):
            command = namespace.worker_command(123)
        self.assertIn(str(ROOT / 'Evaluation/namespace.py'), command)
        self.assertIn('--worker', command)
        self.assertLess(command.index('/usr/bin/setpriv'), command.index(sys.executable))
        self.assertIn('--no-new-privs', command)
        self.assertIn('--bounding-set=-all', command)

    def test_worker_rejects_root_or_remaining_capabilities(self):
        for uid, cap, nnp in ((0, '0000000000000000', '1'), (1000, '0000000000000001', '1'), (1000, '0000000000000000', '0')):
            with patch.object(namespace.os, 'geteuid', return_value=uid), \
                    patch.object(Path, 'read_text', return_value=f'CapEff:\t{cap}\nNoNewPrivs:\t{nnp}\n'), \
                    self.assertRaisesRegex(RuntimeError, 'privilege drop'):
                namespace.worker_main()

    def test_worker_and_parent_share_canonical_evaluator(self):
        status = 'CapEff:\t0000000000000000\nNoNewPrivs:\t1\n'
        request = {'sql_path': '/offline.sql', 'config': {'_network_namespace_pid': 123}, 'timeout_seconds': 10}
        stdout = io.StringIO()
        with patch.object(namespace.os, 'geteuid', return_value=1000), \
                patch.object(Path, 'read_text', return_value=status), \
                patch.object(namespace.os, 'readlink', return_value='net:[offline]'), \
                patch.object(sys, 'stdin', io.StringIO(json.dumps(request) + '\n')), \
                contextlib.redirect_stdout(stdout), \
                patch.object(scoring, 'execute_measured_sql', return_value=[]) as run:
            namespace.worker_main()
        run.assert_called_once_with('/offline.sql', {}, 10)
        self.assertEqual([json.loads(line)['kind'] for line in stdout.getvalue().splitlines()], ['ready', 'result'])

    def test_nonroot_sql_routes_through_namespace_worker(self):
        config = {'_network_namespace_pid': 123}
        with patch.object(scoring.os, 'geteuid', return_value=1000), \
                patch.object(namespace, 'run_worker', return_value=['sentinel']) as worker:
            self.assertEqual(scoring.execute_sql_with_deadline('/offline.sql', config, 10), ['sentinel'])
        self.assertEqual(worker.call_args.args[0], 123)
        self.assertEqual(worker.call_args.args[1]['sql_path'], '/offline.sql')

    def test_exact_protocol_only_no_five_to_three_reuse(self):
        digest = lambda shared, runs: str(runs)
        self.assertFalse(measurement.protocol_matches({'measurement_median_runs': 5, 'protocol_digest': '5'}, {}, 3, digest))
        self.assertTrue(measurement.protocol_matches({'measurement_median_runs': 3, 'protocol_digest': '3'}, {}, 3, digest))
        for invalid in (True, 0, -1, 1.5):
            with self.assertRaises(ValueError):
                measurement.validate_measurement_runs(invalid)

    def test_capture_external_sort_matches_in_memory_and_retains_duplicates(self):
        class Cursor:
            description = [('v',)]
            statusmessage = 'SELECT 6'
            def __init__(self, values): self.rows = iter([[v] for v in values])
            def fetchmany(self, count):
                result = []
                for _ in range(count):
                    try: result.append(next(self.rows))
                    except StopIteration: break
                return result
        rows = [4, 3, 1, 1, 5, 2]
        values = dict(serialize=scoring.serialize_cell, normalize=scoring._normalize_result_value)
        a = measurement.capture_result(Cursor(rows), 'SELECT v FROM t', **values, chunk_rows=100)
        b = measurement.capture_result(Cursor(rows), 'SELECT v FROM t', **values, chunk_rows=1, merge_fan_in=2)
        self.assertEqual(a['_fingerprint'], b['_fingerprint'])
        self.assertEqual(a['row_count'], 6)
        c = measurement.capture_result(Cursor(list(reversed(rows))), 'SELECT v FROM t ORDER BY v', **values)
        d = measurement.capture_result(Cursor(rows), 'SELECT v FROM t ORDER BY v', **values)
        self.assertNotEqual(c['_fingerprint'], d['_fingerprint'])

    def test_old_modules_removed_and_public_signatures_retained(self):
        for name in ('namespace_runtime.py', 'namespace_sql_worker.py', 'result_capture.py', 'measurement_transition.py', 'test_method_eval.py'):
            self.assertFalse((ROOT / 'Evaluation' / name).exists())
        expected = ('i', 'anomaly_pair', 'shared', 'base_docker_name', 'worker_host_port', 'logger', 'timeout_times', 'median_runs')
        self.assertEqual(tuple(inspect.signature(scoring.evaluate_single_sample).parameters), expected)
        self.assertEqual(tuple(inspect.signature(scoring.resolve_solution_artifact_paths).parameters),
                         ('solution_script_directory', 'sample_id', 'case_index'))


if __name__ == '__main__':
    unittest.main()
