#!/usr/bin/env bash
set -Eeuo pipefail

CALLER_DIR="$PWD"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

# Defaults may be edited here or overridden with environment variables/CLI flags.
OPENAI_API_KEY="${OPENAI_API_KEY:-}"
OPENAI_MODEL="${OPENAI_MODEL:-gpt-4-0613}"
OPENAI_API_BASE="${OPENAI_API_BASE:-https://api.openai.com/v1}"
OPENAI_API_VERSION="${OPENAI_API_VERSION:-}"
OPENAI_TEMPERATURE="${OPENAI_TEMPERATURE:-1}"
OPENAI_RETRY_TIMES="${OPENAI_RETRY_TIMES:-24}"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-5432}"
USER="${DB_USER:-postgres}"
PASSWORD="${PASSWORD:-}"
METRIC_SUPERUSER="${METRIC_SUPERUSER:-}"
METRIC_SUPERPASSWORD="${METRIC_SUPERPASSWORD:-}"
DB="${DB:-postgres}"
CONTAINER_NAME="${CONTAINER_NAME:-}"
SAMPLE_DIR="${SAMPLE_DIR:-}"
OUTPUT_ROOT="${OUTPUT_ROOT:-${SCRIPT_DIR}/diagnostic_runs}"
CONDA_ENV="${CONDA_ENV:-db_gpt_baseline}"
CONDA_HOME="${CONDA_HOME:-$(conda info --base 2>/dev/null || printf '%s' "$HOME/miniforge3")}"
case "$CONDA_HOME" in
    "~/"*) CONDA_HOME="$HOME/${CONDA_HOME:2}" ;;
    /*) ;;
    *) CONDA_HOME="$CALLER_DIR/$CONDA_HOME" ;;
esac
MAX_CONTEXT_CHARS="${MAX_CONTEXT_CHARS:-50000}"
METRIC_SAMPLE_COUNT="${METRIC_SAMPLE_COUNT:-12}"
METRIC_SAMPLE_INTERVAL="${METRIC_SAMPLE_INTERVAL:-1}"

usage() {
    cat <<'EOF'
Usage: ./script.sh [options]

Run DB-GPT's native multi-agent PostgreSQL diagnosis.

Options:
  --api-key KEY          OpenAI-compatible API key
  --model MODEL          Model name
  --api-base URL         API base URL, normally ending in /v1
  --api-version VERSION  Azure-compatible API version
  --temperature VALUE    Model temperature
  --retry-times COUNT    Maximum model request retries
  --host HOST            PostgreSQL host
  --port PORT            PostgreSQL port
  --user USER            PostgreSQL user
  --password PASSWORD    PostgreSQL password
  --metric-superuser USER
                         Read-only metric fallback user for exhausted connections
  --metric-superpassword PASSWORD
                         Password for the metric fallback user
  --db DATABASE          PostgreSQL database
  --container NAME       Target Docker container for system metrics
  --sample-dir PATH      Anomaly sample directory
  --output-root PATH     Parent directory for timestamped runs
  --conda-env NAME       Conda environment name
  -h, --help             Show this help

The same values can be supplied through OPENAI_API_KEY, OPENAI_MODEL,
OPENAI_API_BASE, OPENAI_API_VERSION, HOST, PORT, DB_USER, PASSWORD, DB,
SAMPLE_DIR, and OUTPUT_ROOT.
DB_USER is used because USER is normally reserved by the shell.
The workload SQL is read as evidence only and is never executed.
EOF
}

die() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}


if [[ "${1:-}" == "--preflight" ]]; then
    [[ $# -eq 1 && -f "$CONDA_HOME/etc/profile.d/conda.sh" ]] || die "Invalid preflight environment"
# shellcheck disable=SC1091
source "$CONDA_HOME/etc/profile.d/conda.sh"
conda activate "$CONDA_ENV"
# CONDA_PREFIX may already be active while a parent prepended another env.
# Never trust a bare python lookup after conda reactivation.
PYTHON="$CONDA_PREFIX/bin/python"
export PATH="$CONDA_PREFIX/bin:$PATH"
hash -r
cd "$SCRIPT_DIR"
    "$PYTHON" -B -c 'import os,sys; assert os.path.realpath(sys.prefix)==os.path.realpath(os.environ["CONDA_PREFIX"]); print("[NATIVE RUNTIME] env="+os.environ["CONDA_DEFAULT_ENV"]+" python="+sys.executable)'
    export DB_GPT_KB_MODE="${DB_GPT_KB_MODE:-native}"
    export DB_GPT_METRICS_SOURCE=container
    "$PYTHON" -B "$SCRIPT_DIR/native_preflight.py"
    "$PYTHON" -B - <<'IMPORT_PREFLIGHT'
import json, os, pathlib, runpy, sys, tempfile
# Construct clients at import without making requests or needing live secrets.
os.environ.setdefault("OPENAI_API_KEY", "offline-import-preflight-only")
with tempfile.TemporaryDirectory(prefix="db_gpt_import_preflight_") as temporary:
    fixture=pathlib.Path(temporary)/"anomaly.json"
    fixture.write_text(json.dumps({"exceptions":{"runtime_observations":{}},"alerts":[],"workload":"SELECT 1;","slow_queries":[],"workload_statistics":[],"start_time":0,"end_time":1}))
    sys.argv=["run_diagnose.py","--anomaly_file",str(fixture)]
    runpy.run_path("run_diagnose.py",run_name="_dbdiag_import_preflight")
print("Full native diagnosis import preflight passed (not argparse --help).")
IMPORT_PREFLIGHT
    exit 0
fi

while (($#)); do
    case "$1" in
        --api-key) OPENAI_API_KEY="${2:?missing value}"; shift 2 ;;
        --model) OPENAI_MODEL="${2:?missing value}"; shift 2 ;;
        --api-base) OPENAI_API_BASE="${2:?missing value}"; shift 2 ;;
        --api-version) OPENAI_API_VERSION="${2:?missing value}"; shift 2 ;;
        --temperature) OPENAI_TEMPERATURE="${2:?missing value}"; shift 2 ;;
        --retry-times) OPENAI_RETRY_TIMES="${2:?missing value}"; shift 2 ;;
        --host) HOST="${2:?missing value}"; shift 2 ;;
        --port) PORT="${2:?missing value}"; shift 2 ;;
        --user) USER="${2:?missing value}"; shift 2 ;;
        --password) PASSWORD="${2-}"; shift 2 ;;
        --metric-superuser) METRIC_SUPERUSER="${2-}"; shift 2 ;;
        --metric-superpassword) METRIC_SUPERPASSWORD="${2-}"; shift 2 ;;
        --db) DB="${2:?missing value}"; shift 2 ;;
        --container) CONTAINER_NAME="${2:?missing value}"; shift 2 ;;
        --sample-dir) SAMPLE_DIR="${2:?missing value}"; shift 2 ;;
        --output-root) OUTPUT_ROOT="${2:?missing value}"; shift 2 ;;
        --conda-env) CONDA_ENV="${2:?missing value}"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) die "unknown option: $1" ;;
    esac
done

[[ "$SAMPLE_DIR" = /* || -z "$SAMPLE_DIR" ]] || SAMPLE_DIR="$CALLER_DIR/$SAMPLE_DIR"
[[ "$OUTPUT_ROOT" = /* ]] || OUTPUT_ROOT="$CALLER_DIR/$OUTPUT_ROOT"

[[ -n "$OPENAI_API_KEY" ]] || die "OPENAI_API_KEY is required"
[[ -n "$CONTAINER_NAME" ]] || die "--container is required"
[[ -d "$SAMPLE_DIR" ]] || die "sample directory does not exist: $SAMPLE_DIR"
[[ -s "$SAMPLE_DIR/user_phenomenon_description.txt" ]] ||
    die "missing user_phenomenon_description.txt in $SAMPLE_DIR"
[[ "$PORT" =~ ^[0-9]+$ ]] || die "PORT must be numeric"
[[ -f "$CONDA_HOME/etc/profile.d/conda.sh" ]] ||
    die "Conda initialization script not found under $CONDA_HOME"

# shellcheck disable=SC1091
source "$CONDA_HOME/etc/profile.d/conda.sh"
conda activate "$CONDA_ENV"
# CONDA_PREFIX may already be active while a parent prepended another env.
# Never trust a bare python lookup after conda reactivation.
PYTHON="$CONDA_PREFIX/bin/python"
export PATH="$CONDA_PREFIX/bin:$PATH"
hash -r
cd "$SCRIPT_DIR"
"$PYTHON" -c 'import openai, psycopg2' >/dev/null ||
    die "environment $CONDA_ENV lacks openai or psycopg2"

mkdir -p "$OUTPUT_ROOT"
if [[ -n "${DIAGDBENCH_NATIVE_RUN_DIR:-}" ]]; then
    RUN_DIR="$DIAGDBENCH_NATIVE_RUN_DIR"
    [[ "$RUN_DIR" = /* ]] || RUN_DIR="$CALLER_DIR/$RUN_DIR"
    mkdir -p -- "$RUN_DIR"
else
    RUN_DIR="$(mktemp -d "$OUTPUT_ROOT/$(date -u +%Y%m%dT%H%M%SZ)_$(basename "$SAMPLE_DIR").XXXXXX")"
fi
RUN_DIR="$(cd "$RUN_DIR" && pwd)"
export DB_GPT_RUN_DIR="$RUN_DIR"
export DB_GPT_RESULT_PATH="$RUN_DIR/native_result.json"

export OPENAI_API_KEY
export DB_GPT_OPENAI_MODEL="$OPENAI_MODEL"
export DB_GPT_OPENAI_API_BASE="${OPENAI_API_BASE%/}"
export DB_GPT_OPENAI_API_VERSION="$OPENAI_API_VERSION"
export DB_GPT_OPENAI_TEMPERATURE="$OPENAI_TEMPERATURE"
export DB_GPT_OPENAI_RETRY_TIMES="$OPENAI_RETRY_TIMES"
export DB_GPT_POSTGRES_HOST="$HOST"
export DB_GPT_POSTGRES_PORT="$PORT"
export DB_GPT_POSTGRES_USER="$USER"
export DB_GPT_POSTGRES_PASSWORD="$PASSWORD"
export DB_GPT_POSTGRES_DB="$DB"
export DB_GPT_KB_MODE="${DB_GPT_KB_MODE:-native}"
export DB_GPT_METRICS_SOURCE=container
export HOST PORT USER PASSWORD DB SAMPLE_DIR MAX_CONTEXT_CHARS RUN_DIR

"$PYTHON" -B "$SCRIPT_DIR/native_preflight.py"

printf 'Collecting real PostgreSQL and container metrics from %s (%s samples)...\n' \
    "$CONTAINER_NAME" "$METRIC_SAMPLE_COUNT"
"$PYTHON" "$SCRIPT_DIR/collect_runtime_metrics.py" \
    --container "$CONTAINER_NAME" \
    --host "$HOST" \
    --port "$PORT" \
    --user "$USER" \
    --password "$PASSWORD" \
    --superuser "$METRIC_SUPERUSER" \
    --superpassword "$METRIC_SUPERPASSWORD" \
    --database "$DB" \
    --samples "$METRIC_SAMPLE_COUNT" \
    --interval "$METRIC_SAMPLE_INTERVAL" \
    --output "$RUN_DIR/database_snapshot.json"

"$PYTHON" - <<'PY'
import json
import os
from pathlib import Path

run_dir = Path(os.environ["RUN_DIR"])
sample_dir = Path(os.environ["SAMPLE_DIR"])
limit = int(os.environ["MAX_CONTEXT_CHARS"])
snapshot = json.loads((run_dir / "database_snapshot.json").read_text(encoding="utf-8"))

def read(name, required=False):
    path = sample_dir / name
    if not path.exists():
        if required:
            raise FileNotFoundError(path)
        return ""
    text = path.read_text(encoding="utf-8", errors="replace")
    return text[:limit] + ("\n...[truncated]" if len(text) > limit else "")

phenomenon = read("user_phenomenon_description.txt", required=True)
initialization = read("initialization_workload.sql")
verified = read("verified_workload.sql")
workload = (
    "-- Preparation phase: execute first\n"
    + initialization.strip()
    + "\n\n--- anomaly-verified workload phase boundary ---\n\n"
    + "-- Anomaly verification phase: execute second\n"
    + verified.strip()
).strip()
snapshot_summary = dict(snapshot)
samples = snapshot_summary.pop("samples", [])
snapshot_summary["first_sample"] = samples[0] if samples else {}
snapshot_summary["last_sample"] = samples[-1] if samples else {}
supplement = (
    "\n\nReal read-only database and container time-series observations:\n"
    + json.dumps(snapshot_summary, ensure_ascii=False)
)

anomaly = {
    "start_time": snapshot["diagnosis_start_epoch"],
    "end_time": snapshot["snapshot_end_epoch"],
    "alerts": [{
        "status": "firing",
        "alerts": [{
            "annotations": {"description": phenomenon + supplement},
            "labels": {
                "instance": f'{os.environ["HOST"]}:{os.environ["PORT"]}',
                "severity": "warning",
                "alertname": "UserObservedDatabaseAnomaly",
            },
            "startsAt": snapshot["diagnosis_start_time"],
            "endsAt": snapshot["snapshot_end_time"],
        }],
    }],
    "labels": ["user_observed_anomaly"],
    "exceptions": {"runtime_observations": snapshot["time_series"]},
    "workload": workload,
    "slow_queries": [],
    "workload_statistics": [],
}
tmp = run_dir / "db_gpt_anomaly.json.tmp"
tmp.write_text(json.dumps(anomaly, ensure_ascii=False, indent=2), encoding="utf-8")
os.replace(tmp, run_dir / "db_gpt_anomaly.json")
PY

cat >"$RUN_DIR/run_metadata.txt.tmp" <<EOF
method=DB-GPT
upstream_commit=73cb3297fae0823c4bf8c3451513f7dbd3961f4b
knowledge_retrieval=native
metric_source=container_snapshot_adapter
artifact_generation=additional_llm_call
model=$OPENAI_MODEL
api_base=${OPENAI_API_BASE%/}
database=$HOST:$PORT/$DB
sample_dir=$SAMPLE_DIR
started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
EOF
mv "$RUN_DIR/run_metadata.txt.tmp" "$RUN_DIR/run_metadata.txt"

cd "$SCRIPT_DIR"
printf 'Starting DB-GPT multi-agent diagnosis...\n'
set +e
"$PYTHON" run_diagnose.py --anomaly_file "$RUN_DIR/db_gpt_anomaly.json" \
    2>&1 | tee "$RUN_DIR/diagnosis.log"
status=${PIPESTATUS[0]}
set -e
((status == 0)) || die "DB-GPT exited with status $status; see $RUN_DIR/diagnosis.log"

"$PYTHON" - <<'PY'
import json
import os
from pathlib import Path

run_dir = Path(os.environ["RUN_DIR"])
source = Path(os.environ["DB_GPT_RESULT_PATH"])
if not source.is_file():
    raise SystemExit("DB-GPT completed but this invocation produced no native_result.json")
payload = json.loads(source.read_text(encoding="utf-8"))

tmp_json = run_dir / "diagnosis_result.json.tmp"
tmp_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
os.replace(tmp_json, run_dir / "diagnosis_result.json")

report = payload.get("report")
if not isinstance(report, str) or not report.strip():
    raise SystemExit("DB-GPT result does not contain a non-empty Markdown report")
tmp_md = run_dir / "native_diagnosis_report.md.tmp"
tmp_md.write_text(report, encoding="utf-8")
os.replace(tmp_md, run_dir / "native_diagnosis_report.md")
PY

printf 'Generating ReAct-compatible recovery artifacts from the native DB-GPT report...\n'
"$PYTHON" artifact_adapter.py \
    --diagnosis-result "$RUN_DIR/diagnosis_result.json" \
    --sample-dir "$SAMPLE_DIR" \
    --snapshot "$RUN_DIR/database_snapshot.json" \
    --output-dir "$RUN_DIR" \
    --model "$OPENAI_MODEL" \
    --api-base "${OPENAI_API_BASE%/}" \
    --api-version "$OPENAI_API_VERSION"

printf 'Diagnosis completed.\nReport: %s\nRecovery script: %s\nRaw result: %s\n' \
    "$RUN_DIR/diagnosis_report.md" "$RUN_DIR/solution_script.sh" \
    "$RUN_DIR/diagnosis_result.json"
