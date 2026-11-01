"""Cross-file native-method regressions. All DB/model boundaries are simulated."""
import importlib.util
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from Baselines import native_runtime
from Baselines.ReAct import react
from Evaluation import method_adapters


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class NativeEntryCompatibility(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.sample = self.root / 'sample'
        self.sample.mkdir()
        (self.sample / 'config.json').write_text(json.dumps({'dbms': 'PostgreSQL', 'database_name': 'test'}))
        (self.sample / 'verified_workload.sql').write_text('SELECT 1;\n')
        self.files = {'config': self.sample / 'config.json'}
        self.config = {
            'workflow_type': 'dbaiops', 'key_information_path': 'fixture', 'docker_name': 'fixture',
            'parallelism_num': 1, 'conda_home': 'relative-env', 'conda_env': 'test-env',
            'superuser_config': {'DB_USER': 'fixture_admin', 'DB_PASSWORD': 'fixture_password'},
            'llm_configuration': {'api_key': 'dummy-llm', 'base_url': 'https://model.invalid',
                                  'model_name': 'fixture', 'temperature': 1, 'retry_times': 2},
            'knowledge_service': {'api_key_env': 'DIAGDB_TEST_KNOWLEDGE_TOKEN'},
        }
        self.action = {'action': 'finish', 'diagnosis_report.md': 'FROZEN\n',
                       'solution_script.sh': '# TIMEOUT THRESHOLD: 5\ncommand -v psql >/dev/null\nexit 1\n', 'verified_workload.sql': ''}
        self.logger = Mock()
        self.calls = []
        self.real_run = subprocess.run
        self.gpt = load('compat_dbgpt_entry', ROOT / 'Baselines/DB-GPT/db_gpt.py')
        self.ops = load('compat_dbaiops_entry', ROOT / 'Baselines/DBAIOps/dbaiops.py')

    def tearDown(self):
        self.temp.cleanup()

    def transport(self, command, **kwargs):
        self.calls.append((command, kwargs))
        if command[:2] == ['docker', 'port']:
            return subprocess.CompletedProcess(command, 0, '127.0.0.1:25432\n', '')
        if command[:2] == ['bash', '-n']:
            return self.real_run(command, **kwargs)
        if command[0] == 'bash' and Path(command[1]).name == 'script.sh':
            env = kwargs['env']
            run = Path(env['DIAGDBENCH_NATIVE_RUN_DIR'])
            (run / 'artifact_action.json').write_text(json.dumps(self.action))
            # A concurrent/future-dated report must never be chosen by this caller.
            poison = run.parent / 'foreign-run'
            poison.mkdir(exist_ok=True)
            marker = poison / 'artifact_action.json'
            marker.write_text(json.dumps(dict(self.action, **{'diagnosis_report.md': 'WRONG WORKER'})))
            os.utime(marker, (4102444800, 4102444800))
            return subprocess.CompletedProcess(command, 0, 'fixture stdout', '')
        raise AssertionError('Unexpected subprocess: ' + repr(command))

    def test_real_common_dispatch_loads_both_entries_and_saves_common_finish(self):
        with patch.dict(os.environ, {'DIAGDB_TEST_KNOWLEDGE_TOKEN': 'dummy-kb'}), \
             patch.object(subprocess, 'run', side_effect=self.transport), \
             patch.dict(method_adapters._modules, {}, clear=True):
            for workflow in ('db_gpt', 'dbaiops'):
                with self.subTest(workflow=workflow):
                    config = dict(self.config, workflow_type=workflow)
                    output = self.root / workflow
                    action = method_adapters.run_agent('prompt', config, 'fixture', output, self.files, self.logger)
                    react.save_finish(action, output, 'SELECT 1;\n')
                    self.assertEqual(action, self.action)
                    self.assertEqual((output / 'diagnosis_report.md').read_text(), 'FROZEN\n')
                    self.assertEqual((output / 'optimized_verified_workload.sql').read_text(), 'SELECT 1;\n')
                    self.assertTrue(callable(getattr(method_adapters._modules[workflow],
                                                     'run_db_gpt_agent' if workflow == 'db_gpt' else 'run_dbaiops_agent')))

    def test_standalone_dbaiops_uses_credentials_file_and_sample_admin_override(self):
        credentials = self.root / 'credentials.json'
        credentials.write_text(json.dumps({'api_key': 'file-kb-key', 'url': 'https://knowledge.invalid'}))
        config = dict(self.config, knowledge_service={'credentials_file': str(credentials)})
        (self.sample / 'config.json').write_text(json.dumps({'dbms': 'PostgreSQL', 'superuser_config':
            {'DB_USER': 'sample-admin', 'DB_PASSWORD': 'sample-password'}}))
        with patch.object(subprocess, 'run', side_effect=self.transport):
            action = self.ops.run_dbaiops_agent('p', config, 'fixture', self.root / 'direct', self.files, self.logger)
        env = next(kw['env'] for command, kw in self.calls if command[0] == 'bash')
        self.assertEqual(env['DBAIOPS_KNOWLEDGE_API_KEY'], 'file-kb-key')
        self.assertEqual(env['DBAIOPS_KNOWLEDGE_URL'], 'https://knowledge.invalid')
        self.assertEqual(env['DBAIOPS_SNAPSHOT_ADMIN_USER'], 'sample-admin')
        self.assertEqual(env['DBAIOPS_SNAPSHOT_ADMIN_PASSWORD'], 'sample-password')
        self.assertEqual(action, self.action)

    def test_relative_conda_path_is_project_relative_before_changing_cwd(self):
        with patch.object(subprocess, 'run', side_effect=self.transport):
            self.gpt.run_db_gpt_agent('p', self.config, 'fixture', self.root / 'relative', self.files, self.logger)
        env = next(kw['env'] for command, kw in self.calls if command[0] == 'bash')
        self.assertEqual(Path(env['CONDA_HOME']), (ROOT / 'relative-env').resolve())
        command, _ = next((command,kw) for command,kw in self.calls if command[0] == 'bash')
        self.assertTrue(Path(command[command.index('--sample-dir') + 1]).is_absolute())
        self.assertTrue(Path(command[command.index('--output-root') + 1]).is_absolute())

    def test_both_direct_entries_preserve_failure_instead_of_using_stale_results(self):
        for entry in (self.gpt.run_db_gpt_agent, self.ops.run_dbaiops_agent):
            with self.subTest(entry=entry.__name__):
                output = self.root / entry.__name__
                def empty_transport(command, **kwargs):
                    if command[0] == 'docker': return subprocess.CompletedProcess(command,0,'127.0.0.1:25432\n','')
                    run = Path(kwargs['env']['DIAGDBENCH_NATIVE_RUN_DIR'])
                    stale = run.parent / 'older-run'
                    stale.mkdir(exist_ok=True)
                    (stale/'artifact_action.json').write_text(json.dumps(self.action))
                    return subprocess.CompletedProcess(command,0,'','')
                with patch.object(subprocess,'run',side_effect=empty_transport), \
                     patch.dict(os.environ,{'DIAGDB_TEST_KNOWLEDGE_TOKEN':'dummy-kb'}):
                    with self.assertRaisesRegex(RuntimeError,"this invocation"):
                        entry('p',self.config,'fixture',output,self.files,self.logger)

    def test_direct_dbaiops_does_not_substitute_llm_credentials(self):
        with patch.dict(os.environ, {'DIAGDB_TEST_KNOWLEDGE_TOKEN': '', 'DBAIOPS_KNOWLEDGE_API_KEY': ''}), \
             patch.object(subprocess, 'run', side_effect=self.transport):
            with self.assertRaisesRegex(RuntimeError, 'knowledge-service credential'):
                self.ops.run_dbaiops_agent('p', self.config, 'fixture', self.root/'no-key', self.files, self.logger)
        self.assertFalse(any(command[0]=='bash' for command, _ in self.calls))

    def test_both_entries_reject_missing_sample_config_before_launch(self):
        for entry in (self.gpt.run_db_gpt_agent, self.ops.run_dbaiops_agent):
            with patch.object(subprocess, 'run') as run:
                with self.assertRaises(FileNotFoundError):
                    entry('p',self.config,'fixture',self.root/'missing',{'config':self.root/'missing.json'},self.logger)
                run.assert_not_called()

    def test_preflight_and_direct_execution_resolve_same_conda_home(self):
        home = self.root/'conda';(home/'etc/profile.d').mkdir(parents=True)
        (home/'etc/profile.d/conda.sh').write_text('# fixture')
        python=home/'envs/test-env/bin/python';python.parent.mkdir(parents=True);python.write_text('# fixture')
        config=dict(self.config,conda_home=str(home),knowledge_service={'api_key':'dummy-kb'})
        from Evaluation import namespace
        with patch.object(namespace,'preflight',return_value={'ok':True}), \
             patch.object(subprocess,'run',return_value=subprocess.CompletedProcess([],0,'ok','')) as run:
            result=method_adapters.preflight(config)
        env=run.call_args.kwargs['env']
        self.assertEqual(env['CONDA_HOME'],str(native_runtime.resolve_conda_home(config)))
        self.assertEqual(result['native_python'],str(python))


class LazyExportCompatibility(unittest.TestCase):
    def test_unknown_attributes_do_not_initialize_model_clients(self):
        module=load('compat_multiagents',ROOT/'Baselines/DB-GPT/multiagents/__init__.py')
        with patch.object(module,'_load_exports') as initialize:
            self.assertFalse(hasattr(module,'load_tests'))
            self.assertFalse(hasattr(module,'unknown_attribute'))
            initialize.assert_not_called()

    def test_declared_public_exports_still_initialize_on_demand(self):
        module=load('compat_multiagents',ROOT/'Baselines/DB-GPT/multiagents/__init__.py')
        sentinel=object()
        def initialize(): module.MultiAgents=sentinel
        with patch.object(module,'_load_exports',side_effect=initialize) as called:
            self.assertIs(module.MultiAgents,sentinel)
            called.assert_called_once()


class LegacyArgumentCompatibility(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.runner=load('compat_dbaiops_runner',ROOT/'Baselines/DBAIOps/native_runner.py')

    def test_previous_keyword_signature_is_accepted_without_local_retrieval(self):
        response={'status':'200','data':{'dataList':['版本：PG']}}
        with patch.object(self.runner,'request_json',return_value=response):
            items,_=self.runner.retrieve_knowledge('q',url='https://knowledge.invalid',api_key='dummy',
                dataset_id='2104',retries=1,similarity=.7,topn=6,temperature=.7,
                fallback_dir=Path('/must-not-read'),dbms='pg')
        self.assertEqual(items,['版本：PG'])

    def test_legacy_argument_never_enables_silent_algorithm_substitution(self):
        with patch.object(self.runner,'request_json',side_effect=RuntimeError('remote failure')):
            with self.assertRaisesRegex(RuntimeError,'remote failure'):
                self.runner.retrieve_knowledge('q',url='https://knowledge.invalid',api_key='dummy',
                    dataset_id='2104',retries=1,similarity=.7,topn=6,temperature=.7,
                    fallback_dir=Path('/must-not-read'),dbms='pg')

    def test_previous_cli_option_parses_without_running_the_method(self):
        args=self.runner.parse_args(['--sample-dir','sample','--output-dir','out','--snapshot','snapshot',
            '--dbms','pg','--knowledge-url','https://kb.invalid','--knowledge-api-key','dummy',
            '--model-url','https://llm.invalid','--model-api-key','dummy','--model','fixture',
            '--local-knowledge-dir','legacy-docs'])
        self.assertEqual(args.local_knowledge_dir,Path('legacy-docs'))
        self.assertEqual(args.dataset_id,'2104')


# Executable shim for copied (byte-identical) launchers. Only external services
# are replaced; real CLI parsers, report serialization and artifact writers run.
PYTHON_SHIM = r'''import argparse, importlib.util, json, os, runpy, sys, types
from pathlib import Path
ROOT=Path(os.environ['DIAGDB_TEST_ROOT'])
sys.path[:0]=[str(ROOT),str(ROOT/'Baselines/DB-GPT')]
sys.dont_write_bytecode=True
ALLOWED=Path(os.environ['DIAGDB_TEST_TMP']).resolve()
def guard(event,args):
    if event in ('socket.connect','socket.getaddrinfo'):
        raise AssertionError('network forbidden')
    if event=='open':
        path,mode,flags=args
        write=(isinstance(flags,int) and flags & (os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC)) or (isinstance(mode,str) and any(c in mode for c in 'wax+'))
        if write and isinstance(path,(str,bytes,os.PathLike)) and not Path(os.fsdecode(path)).resolve().is_relative_to(ALLOWED):
            raise AssertionError('fixture cannot write outside its temporary directory')
    if event=='subprocess.Popen':
        command=args[1]
        if not (isinstance(command,(list,tuple)) and command[:2]==['bash','-n']):
            raise AssertionError('unexpected native subprocess '+repr(command))
sys.addaudithook(guard)
args=sys.argv[1:]
while args and args[0]=='-B':args.pop(0)
if args[0]=='-c':
    code=args[1]
    if code=='import openai, psycopg2':raise SystemExit(0)
    raise AssertionError('unexpected -c '+code)
if args[0]=='-':
    exec(compile(sys.stdin.read(),'<launcher inline>','exec'))
    raise SystemExit(0)
name=Path(args[0]).name
actual=ROOT/'Baselines'/('DBAIOps' if name in ('native_runner.py','collect_db_snapshot.py') else 'DB-GPT')/name
sys.argv=[str(actual)]+args[1:]
def load():
    spec=importlib.util.spec_from_file_location('fixture_native',actual)
    m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
if name=='native_preflight.py':
    print('fixture: external KB prerequisites supplied by test');raise SystemExit(0)
if name=='collect_runtime_metrics.py':
    m=load()
    m.collect=lambda args:dict(diagnosis_start_epoch=1,snapshot_end_epoch=2,
        diagnosis_start_time='2026-01-01T00:00:00Z',snapshot_end_time='2026-01-01T00:00:01Z',
        samples=[],time_series={'cpu_usage':[1.0]*12})
    raise SystemExit(m.main())
if name=='collect_db_snapshot.py':
    # No DB connection; this test focuses on the launcher/consumer JSON boundary.
    out=Path(sys.argv[sys.argv.index('--output')+1]);out.write_text('{}');raise SystemExit(0)
if name=='run_diagnose.py':
    parser=argparse.ArgumentParser();parser.add_argument('--anomaly_file');parsed=parser.parse_args()
    run=Path(os.environ['DB_GPT_RUN_DIR'])
    config=types.ModuleType('configs');config.DIAGNOSTIC_RESULTS_PATH=str(run/'alert_results');sys.modules['configs']=config
    cli=types.ModuleType('multiagents.our_argparse');parsed.agent_conf_name='fixture';cli.args=parsed;sys.modules[cli.__name__]=cli
    metrics=types.ModuleType('multiagents.tools.metrics');metrics.current_diag_time='fixture';metrics.WORKLOAD_FILE_NAME=str(run/'workload_info.json');sys.modules[metrics.__name__]=metrics
    class Agents:
        @classmethod
        def from_task(cls,*args):return cls(),'fixture-model'
        async def run(self,args):return 'FROZEN\n',{'report':'FROZEN\n'}
    module=types.ModuleType('multiagents.multiagents');module.MultiAgents=Agents;sys.modules[module.__name__]=module
    runpy.run_path(str(actual),run_name='__main__');raise SystemExit(0)
if name=='artifact_adapter.py':
    m=load();m.request_completion=lambda *a,**kw:(json.dumps({'solution_script.sh':'# TIMEOUT THRESHOLD: 5\ncommand -v psql >/dev/null\nexit 1\n','verified_workload.sql':''}),'{}')
    raise SystemExit(m.main())
if name=='native_runner.py':
    m=load();m.request_json=lambda *a,**kw:{'status':'200','data':{'dataList':['版本：PG']}}
    def chat(messages,**kwargs):
        if messages[0]['role']=='system':return 'FROZEN',{}
        return json.dumps({'action':'finish','diagnosis_report.md':'IGNORED','solution_script.sh':'# TIMEOUT THRESHOLD: 5\ncommand -v psql >/dev/null\nexit 1\n','verified_workload.sql':''}),{}
    m.chat_completion=chat
    raise SystemExit(m.main())
raise AssertionError('unknown native command '+name)
'''


class LauncherChainCompatibility(unittest.TestCase):
    def test_real_launchers_accept_relative_paths_and_produce_consumable_artifacts(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);sample=root/'sample';sample.mkdir()
            for name,text in {'user_phenomenon_description.txt':'检查锁等待','initialization_workload.sql':'SELECT 1;',
                              'verified_workload.sql':'SELECT 2;','config.json':'{"dbms":"PostgreSQL"}'}.items():
                (sample/name).write_text(text)
            conda=root/'conda';profile=conda/'etc/profile.d/conda.sh';profile.parent.mkdir(parents=True)
            profile.write_text('conda() { export CONDA_PREFIX="$CONDA_HOME/envs/$2"; export CONDA_DEFAULT_ENV="$2"; }\n')
            python=conda/'envs/fixture/bin/python';python.parent.mkdir(parents=True)
            python.write_text('#!'+sys.executable+'\n'+PYTHON_SHIM);python.chmod(0o700)
            # Collector imports psycopg2 in DB-GPT; use the existing runtime's
            # package only for importing its real parser, never for connecting.
            for method in ['DB-GPT','DBAIOps']:
                with self.subTest(method=method):
                    script=root/method/'script.sh';script.parent.mkdir();script.write_bytes((ROOT/'Baselines'/method/'script.sh').read_bytes())
                    bound=root/'bound'/method;bound.mkdir(parents=True)
                    env=dict(os.environ,CONDA_HOME='conda',CONDA_ENV='fixture',OPENAI_API_KEY='dummy-llm',
                             OPENAI_MODEL='fixture',DBAIOPS_KNOWLEDGE_API_KEY='dummy-kb',
                             DIAGDB_TEST_ROOT=str(ROOT),DIAGDB_TEST_TMP=str(root),
                             DIAGDBENCH_NATIVE_RUN_DIR=str(bound),PYTHONDONTWRITEBYTECODE='1')
                    command=['bash',str(script),'--sample-dir','sample','--output-root','relative-output',
                             '--host','127.0.0.1','--port','25432','--user','fixture','--password','fixture','--db','fixture']
                    command+=['--container','fixture'] if method=='DB-GPT' else ['--dbms','pg','--collect-db-snapshot']
                    completed=subprocess.run(command,cwd=root,env=env,text=True,capture_output=True,timeout=30)
                    self.assertEqual(completed.returncode,0,completed.stdout+'\n'+completed.stderr)
                    action=json.loads((bound/'artifact_action.json').read_text())
                    self.assertEqual(action['action'],'finish')
                    self.assertEqual(action['diagnosis_report.md'],'FROZEN\n')
                    self.assertEqual((bound/'optimized_verified_workload.sql').read_text().strip(),'SELECT 2;')
                    self.assertFalse((script.parent/'relative-output').exists())
                    if method=='DB-GPT':
                        self.assertTrue((bound/'native_result.json').is_file())
                        self.assertTrue((bound/'workload_info.json').is_file())


if __name__ == '__main__':
    unittest.main()
