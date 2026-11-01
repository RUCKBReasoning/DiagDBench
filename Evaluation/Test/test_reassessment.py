"""Offline tests for frozen-report conversion and unchanged scoring boundaries."""
import copy
import json
import logging
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from Baselines.ReAct import react
from Baselines import recovery_artifacts
from Evaluation import recovery_reassessment as reassess
from Evaluation import test_baseline_eval as pipeline, test_eval as scoring
from llm_api import LLMResponseText

SCRIPT = '# TIMEOUT THRESHOLD: 60\nset -Eeuo pipefail\npsql -X -v ON_ERROR_STOP=1 -c "ANALYZE"\n'
REPORT = '  frozen report: inspection only; need authorization.\r\n'
WORKLOAD = ' \r\nSELECT 1;\r\n'


class Reassessment(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / 'native'
        self.original = self.source / '7/baseline'
        self.original.mkdir(parents=True)
        self.sample = self.root / 'samples/7'
        self.sample.mkdir(parents=True)
        for name, value in {'diagnosis_report.md': REPORT, 'solution_script.sh': SCRIPT,
                            'optimized_verified_workload.sql': WORKLOAD}.items():
            (self.original / name).write_bytes(value.encode())
        scoring.write_json_atomic(self.original / 'status.json', {'status': 'completed'})
        scoring.write_json_atomic(self.source / '7/eval/score_detail.json', {
            'sample_id': '7', 'diagnosis_score': .6, 'artifact_validation': scoring.agent_artifact_status(self.original)})
        scoring.write_json_atomic(self.source / '7/eval/diagnosis_judge_detail.json', {'score': .6, 'judgements': []})
        for name, value in {'config.json': '{"dbms":"PostgreSQL","database_name":"fixture","password":"PRIVATE_PASSWORD"}',
                            'user_phenomenon_description.txt': 'Queries wait.',
                            'initialization_workload.sql': 'SELECT 0;', 'verified_workload.sql': WORKLOAD,
                            'anomaly_inject.sh': 'PRIVATE_INJECTION', 'anomaly_description.json': 'PRIVATE_ANSWER'}.items():
            (self.sample / name).write_bytes(value.encode())
        self.obj = object.__new__(pipeline.Pipeline)
        self.obj.root = self.root / 'output'
        self.obj.driver = SimpleNamespace(find_sample_directory=lambda root, case: ('7', self.sample),
            load_json_file=scoring.read_json, normalize_dbms=react.normalize_dbms)
        self.obj.anomaly_root = self.sample.parent
        self.obj.config = {'native_output_root': self.source, 'method_name': 'DB-GPT',
            'diagnosis': {'workflow_type': 'db_gpt', 'llm_configuration': {'model_name': 'offline', 'api_key': 'PRIVATE_KEY'}}}
        self.obj.logger = logging.getLogger('offline-reassessment')
        self.obj.stop_event = threading.Event()
        self.obj.fatal_marker = self.obj.root / 'fatal_api_error.json'
        self.case = {'id': 7, 'dbms': 'PostgreSQL', 'database_name': 'fixture'}
        self.obj.cases = [self.case]
        self.response = json.dumps({'solution_script.sh': SCRIPT, 'verified_workload.sql': ''})

    def prepare(self):
        with patch('llm_api.get_llm_response', return_value=self.response) as request:
            status = self.obj.stage_baseline(0, self.case, '7', self.obj.logger, 'unused', 35000)
        return status, request

    def test_real_pipeline_branch_converts_without_running_native_diagnosis(self):
        with patch.object(self.obj, 'run_diagnosis') as native:
            status, request = self.prepare()
        native.assert_not_called()
        self.assertEqual(status['status'], 'completed')
        output = self.obj.root / '7/baseline'
        self.assertEqual((output / 'diagnosis_report.md').read_bytes(), REPORT.encode())
        self.assertEqual((output / 'optimized_verified_workload.sql').read_bytes(), WORKLOAD.encode())
        prompt = request.call_args.args[0][0]
        self.assertNotIn('PRIVATE_', prompt)
        self.assertNotIn('diagnosis_score', prompt)
        self.assertFalse(request.call_args.args[1]['allow_context_truncation'])
        self.assertEqual((self.original / 'diagnosis_report.md').read_bytes(), REPORT.encode())

    def test_valid_cache_does_not_call_model_again(self):
        self.prepare()
        _, request = self.prepare()
        request.assert_not_called()

    def test_changed_artifact_invalidates_cache_and_archives_previous_output(self):
        self.prepare()
        (self.obj.root / '7/baseline/solution_script.sh').write_text('tampered')
        _, request = self.prepare()
        request.assert_called_once()
        self.assertEqual(len(list((self.obj.root / '7/_attempts').glob('conversion_*'))), 1)

    def test_model_settings_change_invalidates_conversion_cache(self):
        self.prepare()
        self.obj.config['diagnosis']['llm_configuration']['model_name'] = 'offline-new'
        _, request = self.prepare()
        request.assert_called_once()

    def test_frozen_diagnosis_score_is_bound_to_original_artifacts(self):
        self.prepare()
        output = self.obj.root / '7/baseline'
        detail = reassess.preserved_diagnosis(self.source, '7', output, scoring.agent_artifact_status)
        self.assertEqual(detail['score'], .6)
        self.assertTrue(detail['recovery_reassessment_preserved'])
        (self.original / 'solution_script.sh').write_text('changed original submission')
        with self.assertRaisesRegex(ValueError, 'scored submission'):
            reassess.preserved_diagnosis(self.source, '7', output, scoring.agent_artifact_status)

    def test_changed_compiled_report_cannot_reuse_diagnosis_score(self):
        self.prepare()
        output = self.obj.root / '7/baseline'
        (output / 'diagnosis_report.md').write_text('new diagnosis')
        with self.assertRaisesRegex(ValueError, 'binding changed'):
            reassess.preserved_diagnosis(self.source, '7', output, scoring.agent_artifact_status)

    def test_missing_original_score_requests_normal_judge(self):
        (self.source / '7/eval/score_detail.json').unlink()
        self.prepare()
        self.assertIsNone(reassess.preserved_diagnosis(self.source, '7', self.obj.root / '7/baseline', scoring.agent_artifact_status))

    def test_invalid_diagnosis_values_fail_before_model_call(self):
        for value in [True, -1, 2, float('inf')]:
            path = self.source / '7/eval/score_detail.json'
            score = scoring.read_json(path)
            score['diagnosis_score'] = value
            path.write_text(json.dumps(score))
            with self.subTest(value=value), patch('llm_api.get_llm_response') as request:
                with self.assertRaises(ValueError):
                    reassess.prepare_sample(self.obj, self.case, '7', self.obj.logger)
                request.assert_not_called()

    def test_unfinalized_source_is_not_converted(self):
        scoring.write_json_atomic(self.original / 'status.json', {'status': 'running'})
        with patch('llm_api.get_llm_response') as request, self.assertRaisesRegex(ValueError, 'in-flight'):
            reassess.prepare_sample(self.obj, self.case, '7', self.obj.logger)
        request.assert_not_called()

    def test_overlapping_output_roots_are_rejected(self):
        for output in [self.source, self.source / 'derived', self.root]:
            with self.subTest(output=output), self.assertRaises(ValueError):
                reassess.validate_roots(self.source, output)
        self.assertEqual(reassess.validate_roots(self.source, self.obj.root), self.source)

    def test_private_score_sidecar_changes_resume_digest(self):
        self.prepare()
        output = self.obj.root / '7/baseline'
        before = scoring.input_digest(self.sample, output)
        policy = scoring.read_json(output / reassess.POLICY_FILE)
        policy['score'] = .1
        scoring.write_json_atomic(output / reassess.POLICY_FILE, policy)
        self.assertNotEqual(before, scoring.input_digest(self.sample, output))

    def test_schema_repair_rejects_truncated_response_and_preserves_raw_evidence(self):
        with patch('llm_api.get_llm_response', side_effect=[LLMResponseText(self.response, finish_reason='length'), self.response]) as request:
            reassess.prepare_sample(self.obj, self.case, '7', self.obj.logger)
        self.assertEqual(request.call_count, 2)
        self.assertIn('truncated', request.call_args.args[0][0])
        self.assertTrue((self.obj.root / '7/baseline/conversion_response_1.txt').exists())

    def test_failed_conversion_keeps_failure_status(self):
        with patch('llm_api.get_llm_response', return_value='{}'), self.assertRaises(ValueError):
            reassess.prepare_sample(self.obj, self.case, '7', self.obj.logger)
        self.assertEqual(scoring.read_json(self.obj.root / '7/baseline/status.json')['status'], 'conversion_failed')

    def test_completed_zero_is_not_retried(self):
        result = {'sample_summaries': [{'sample_id': '7', 'eval_status': 'completed', 'score': 0}]}
        self.assertEqual(reassess.unfinished(result, ['7']), [])
        result['sample_summaries'][0]['eval_status'] = 'aborted'
        self.assertEqual(reassess.unfinished(result, ['7']), ['7'])

    def test_incomplete_or_fatal_aggregate_is_not_continued(self):
        for rows in [[], [{'sample_id': '7'}, {'sample_id': '7'}], [{'sample_id': '7', 'error': 'fatal API failure'}]]:
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                reassess.unfinished({'sample_summaries': rows}, ['7'])

    def test_controller_filters_terminal_reports_and_retains_entire_cohort(self):
        self.obj.run = Mock(return_value={'overall_score': 0})
        with patch.object(scoring, 'anomaly_sample_id', return_value='7'):
            result = reassess.run_reassessment(self.obj)
        self.obj.run.assert_called_once_with({'7'})
        self.assertEqual(result['overall_score'], 0)
        manifest = scoring.read_json(self.obj.root / 'cohort_manifest.json')
        self.assertEqual(manifest['selected_sample_ids'], ['7'])

    def test_controller_does_not_restart_without_fresh_final_aggregate(self):
        self.obj.run = Mock(side_effect=SystemExit(1))
        self.obj.config['continuation_passes'] = 1
        with patch.object(scoring, 'anomaly_sample_id', return_value='7'), self.assertRaisesRegex(RuntimeError, 'freshly finalized'):
            reassess.run_reassessment(self.obj)
        self.obj.run.assert_called_once()


if __name__ == '__main__':
    unittest.main()
