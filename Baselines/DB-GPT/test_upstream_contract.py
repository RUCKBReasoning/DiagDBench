"""Offline tests: no database, model service, downloaded weights or injections."""
import ast
import asyncio
from contextlib import redirect_stdout
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import tempfile
import time
import sys
import types
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parent


def functions(relative, names, namespace=None):
    """Execute actual functions with inert dependencies, not the native CLI imports."""
    tree = ast.parse((ROOT / relative).read_text())
    nodes = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names]
    for node in nodes:
        node.returns = None
        for arg in node.args.args + node.args.kwonlyargs:
            arg.annotation = None
        # These tests explicitly supply the required search parameters.
        if node.name == 'search_docs':
            node.args.defaults = []
    namespace = dict(namespace or {})
    exec(compile(ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[])), str(ROOT / relative), 'exec'), namespace)
    return namespace


def module(relative):
    spec = importlib.util.spec_from_file_location('tested_' + Path(relative).stem, ROOT / relative)
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


class NativeEmbeddingTest(unittest.TestCase):
    def embedding(self, factory):
        return functions('multiagents/llms/sentence_embedding.py', ['sentence_embedding'],
                         {'SentenceTransformer': factory, 'os': os, '__file__': str(ROOT / 'multiagents/llms/sentence_embedding.py')})['sentence_embedding']

    def test_missing_dependency_fails_not_hashes(self):
        with self.assertRaisesRegex(RuntimeError, 'not an equivalent substitute'):
            self.embedding(None)('database lock')

    def test_native_semantic_encoder_is_used(self):
        encoded = Mock()
        encoded.tolist.return_value = [0.5, 0.25]
        encoder = Mock()
        encoder.encode.return_value = encoded
        factory = Mock(return_value=encoder)
        self.assertEqual(self.embedding(factory)('database lock'), [0.5, 0.25])
        encoder.encode.assert_called_once_with('database lock', convert_to_numpy=True)

    def test_native_remote_model_fallback_is_same_semantic_model(self):
        encoder = Mock()
        encoder.encode.return_value.tolist.return_value = [1.0]
        factory = Mock(side_effect=[FileNotFoundError(), encoder])
        self.assertEqual(self.embedding(factory)('lock'), [1.0])
        self.assertEqual(factory.call_args.args[0], 'sentence-transformers/all-mpnet-base-v2')

    def test_unknown_model_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'Unsupported embedding model'):
            self.embedding(None)('q', 'hash-model')


class NativeRetrievalTest(unittest.TestCase):
    def test_vector_factory_threshold_and_cause_deduplication(self):
        class Doc:
            def __init__(self, cause):
                self.page_content = cause
                self.metadata = {'cause_name': cause, 'desc': cause}
            def dict(self): return {'metadata': self.metadata}
        kb = Mock()
        kb.search_docs.return_value = [(Doc('A'), .1), (Doc('A'), .2), (Doc('B'), .3), (Doc('C'), .4)]
        factory = Mock()
        factory.get_service.return_value = kb
        ns = functions('server/knowledge_base/kb_doc_api.py', ['search_docs'], {
            'KBServiceFactory': factory, 'DEFAULT_VS_TYPES': ['chromadb'], 'DocumentWithScore': SimpleNamespace,
        })
        with redirect_stdout(io.StringIO()):
            results = ns['search_docs']('lock', 'CpuExpert', 2, .4)
        factory.get_service.assert_called_once_with('CpuExpert', 'chromadb')
        kb.search_docs.assert_called_once_with('lock', 4, .4)
        self.assertEqual([r.metadata['cause_name'] for r in results], ['A', 'B'])

    def test_registry_is_not_replaced_with_experiment_roster(self):
        ns = functions('server/knowledge_base/kb_doc_api.py', ['fetch_expert_kb_names'], {
            'get_kb_details': lambda: [{'kb_name': 'CpuExpert'}, {'kb_name': 'CustomExpert'}, {'kb_name': 'samples'}],
        })
        self.assertEqual(ns['fetch_expert_kb_names'](), ['CpuExpert', 'CustomExpert'])

    def test_unavailable_vector_service_is_not_replaced(self):
        factory = Mock()
        factory.get_service.side_effect = RuntimeError('missing index')
        ns = functions('server/knowledge_base/kb_doc_api.py', ['search_docs'], {
            'KBServiceFactory': factory, 'DEFAULT_VS_TYPES': ['chromadb'], 'DocumentWithScore': SimpleNamespace,
        })
        with self.assertRaisesRegex(RuntimeError, 'Knowledge base lookup failed'):
            ns['search_docs']('lock', 'CpuExpert', 2, .4)


class MetricContractTest(unittest.TestCase):
    def test_explicit_catalog_default_and_container_adapter(self):
        catalog = module('benchmark_metrics.py')
        native = catalog.load_metric_catalog('prometheus')
        self.assertTrue(any('node_load1' in metric for metric in native['cpu_metrics']))
        adapted = catalog.load_metric_catalog('container')
        self.assertIn('cpu_usage', adapted['cpu_metrics'])
        self.assertIn('pg_stat_activity_count', adapted['cpu_metrics'])
        with self.assertRaises(ValueError): catalog.load_metric_catalog('invalid')

    def test_expert_routing_matches_upstream(self):
        tree = ast.parse((ROOT / 'multiagents/tools/metric_monitor/anomaly_analysis.py').read_text())
        function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'metric_analysis_results')
        for name, expected in [('cpu', 'cpu'), ('workload', 'cpu'), ('index', 'cpu'), ('query', 'cpu'),
                               ('write', 'io'), ('io', 'io'), ('memory', 'memory'),
                               ('configuration', 'io'), ('network', 'cpu'), ('other', 'cpu')]:
            namespace = {'agent_name': name}
            exec(compile(ast.Module(body=[function.body[0]], type_ignores=[]), '<routing>', 'exec'), namespace)
            self.assertEqual(namespace['metric_prefix'], expected, name)

    def test_top_five_scores_stay_in_sync_after_replacements(self):
        import numpy as np
        with tempfile.TemporaryDirectory() as temporary:
            Path(temporary, 'run').mkdir()
            values = {'metric'+str(i): [float(i)] for i in range(1, 13)}
            ns = functions('multiagents/tools/metric_monitor/anomaly_analysis.py', ['metric_analysis_results'], {
                'prometheus_metrics': {'cpu_metrics': list(values)}, 'np': np,
                'obtain_values_of_metrics': lambda _, metrics, *args: {key: values[key] for key in metrics if key in values},
                'detect_anomalies': lambda data: (float(data[0]), True),
                'processed_values': lambda data, **kw: str(data), 'LANGUAGE': 'en',
                'generate_prometheus_chart_content': lambda *args, **kw: '',
                'DIAGNOSTIC_RESULTS_PATH': temporary, 'current_diag_time': 'run',
                'search_docs': lambda *args, **kw: [],
            })
            _, charts, _ = ns['metric_analysis_results']('cpu', 'CpuExpert', '', '0', False, 0, 1)
            self.assertEqual({chart['values'][0] for chart in charts}, {8., 9., 10., 11., 12.})

    def test_short_series_does_not_produce_zero_step(self):
        import numpy as np
        ns = functions('multiagents/tools/metrics.py', ['processed_values'], {'np': np})
        self.assertIn('mean value is 1.0', ns['processed_values']([1.]))


class RuntimeIsolationTest(unittest.TestCase):
    def test_native_state_paths_are_per_invocation(self):
        with tempfile.TemporaryDirectory() as temporary:
            config_path = Path(temporary, 'configs/diagnose_config.py')
            config_path.parent.mkdir()
            namespaces = []
            for label in ['a', 'b']:
                ns = {'__file__': str(config_path)}
                with patch.dict(os.environ, {'DB_GPT_RUN_DIR': str(Path(temporary, label))}):
                    exec(compile((ROOT / 'configs/diagnose_config.py').read_text(), str(config_path), 'exec'), ns)
                namespaces.append(ns)
            for key in ['DIAGNOSTIC_RESULTS_PATH', 'DIAGNOSE_RUN_DATA_PATH', 'DIAGNOSE_RUN_LOG_PATH',
                        'DIAGNOSE_RUN_PID_PATH', 'DIAGNOSE_USER_FEEDBACK_PATH']:
                self.assertNotEqual(namespaces[0][key], namespaces[1][key])
                self.assertTrue(Path(namespaces[0][key]).is_relative_to(Path(temporary, 'a')))

    def test_report_is_written_to_exact_invocation_path(self):
        class Agents:
            async def run(self, args): return args.sample, {'report': args.sample}
        factory = SimpleNamespace(from_task=lambda *args: (Agents(), 'test-model'))
        with tempfile.TemporaryDirectory() as temporary:
            for label in ['sample-a', 'sample-b']:
                run = Path(temporary, label)
                result = run / 'native_result.json'
                ns = functions('run_diagnose.py', ['main', 'create_dir_if_not_exists'], {
                    'Path': Path, 'os': os, 'shutil': shutil, 'json': json, 'time': time,
                    'DIAGNOSTIC_RESULTS_PATH': str(run / 'alert_results'), 'MultiAgents': factory,
                    'current_diag_time': 'same-second',
                })
                with patch.dict(os.environ, {'DB_GPT_RESULT_PATH': str(result)}), redirect_stdout(io.StringIO()):
                    asyncio.run(ns['main'](SimpleNamespace(agent_conf_name='test', sample=label)))
                self.assertEqual(json.loads(result.read_text())['report'], label)
            self.assertEqual(json.loads(Path(temporary, 'sample-a/native_result.json').read_text())['report'], 'sample-a')

    def test_launcher_does_not_scan_shared_reports(self):
        script = (ROOT / 'script.sh').read_text()
        self.assertIn('DB_GPT_RESULT_PATH', script)
        self.assertNotIn('rglob("*.jsonl")', script)
        self.assertIn('mktemp -d', script)
        self.assertNotIn(':-bundled', script)

    def test_report_prompt_is_not_artifact_compiler_prompt(self):
        source = (ROOT / 'multiagents/agents/reporter.py').read_text()
        self.assertNotIn('concrete, verifiable, and reversible SQL', source)
        self.assertIn('with_diagnosis_requirements', (ROOT / 'multiagents/agents/base.py').read_text())

    def test_preflight_checks_resources_without_querying_embeddings(self):
        preflight = module('native_preflight.py')
        with tempfile.TemporaryDirectory() as temporary:
            config = types.ModuleType('configs')
            config.DEFAULT_VS_TYPES = ['chromadb']
            config.kbs_config = {'chromadb': {'persist_directory': temporary}}
            service = types.ModuleType('server.knowledge_base.kb_service.base')
            service.get_kb_details = Mock(return_value=[{'kb_name': 'CustomExpert', 'in_db': True, 'file_count': 1}])
            stubs = {'configs': config, 'server.knowledge_base.kb_service.base': service}
            with patch.dict(sys.modules, stubs), patch.dict(os.environ, {'DB_GPT_KB_MODE': 'native'}), \
                 patch.object(preflight.importlib.util, 'find_spec', return_value=object()):
                with self.assertRaisesRegex(RuntimeError, 'vector index is missing'):
                    preflight.check_knowledge()
                self.assertEqual(list(Path(temporary).iterdir()), [])
                Path(temporary, 'chroma.sqlite3').write_text('fixture')
                with redirect_stdout(io.StringIO()):
                    self.assertEqual(preflight.check_knowledge(), ['CustomExpert'])
                service.get_kb_details.return_value[0]['in_db'] = False
                with self.assertRaisesRegex(RuntimeError, 'not populated'):
                    preflight.check_knowledge()

    def test_preflight_rejects_obsolete_lexical_mode_before_importing_backend(self):
        preflight = module('native_preflight.py')
        with patch.dict(os.environ, {'DB_GPT_KB_MODE': 'bundled'}):
            with self.assertRaisesRegex(RuntimeError, 'no longer supported'):
                preflight.check_knowledge()


if __name__ == '__main__':
    unittest.main()
