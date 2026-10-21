#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
OPENAI_API_KEY="${OPENAI_API_KEY:-}"
OPENAI_MODEL="${OPENAI_MODEL:-gpt-4o-mini}"
OPENAI_API_BASE="${OPENAI_API_BASE:-https://api.openai.com/v1}"
OPENAI_PROVIDER="${OPENAI_PROVIDER:-}"
OPENAI_API_VERSION="${OPENAI_API_VERSION:-}"
DBAIOPS_KNOWLEDGE_URL="${DBAIOPS_KNOWLEDGE_URL:-https://api.bic-qa.com/api/chat/message}"
DBAIOPS_KNOWLEDGE_API_KEY="${DBAIOPS_KNOWLEDGE_API_KEY:-}"
DBAIOPS_DATASET_ID="${DBAIOPS_DATASET_ID:-}"
DBAIOPS_RETRY_TIMES="${DBAIOPS_RETRY_TIMES:-3}"
TEMPERATURE="${TEMPERATURE:-0.7}"
MAX_TOKENS="${MAX_TOKENS:-8192}"
MAX_CONTEXT_CHARS="${MAX_CONTEXT_CHARS:-50000}"
SAMPLE_DIR="${SAMPLE_DIR:-}"
OUTPUT_ROOT="${OUTPUT_ROOT:-${SCRIPT_DIR}/diagnostic_runs}"
CONDA_ENV="${CONDA_ENV:-dbaiops_baseline}"
CONDA_HOME="${CONDA_HOME:-$HOME/miniforge3}"
DBMS_KIND="${DBMS_KIND:-pg}"
COLLECT_DB_SNAPSHOT="${COLLECT_DB_SNAPSHOT:-false}"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-5432}"
USER="${DB_USER:-diagnosis}"
PASSWORD="${PASSWORD:-}"
DB="${DB:-postgres}"

die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

while (($#)); do
    case "$1" in
        --host) HOST="${2:?missing value}"; shift 2 ;;
        --port) PORT="${2:?missing value}"; shift 2 ;;
        --user) USER="${2:?missing value}"; shift 2 ;;
        --password) PASSWORD="${2-}"; shift 2 ;;
        --db) DB="${2:?missing value}"; shift 2 ;;
        --dbms) DBMS_KIND="${2:?missing value}"; shift 2 ;;
        --sample-dir) SAMPLE_DIR="${2:?missing value}"; shift 2 ;;
        --output-root) OUTPUT_ROOT="${2:?missing value}"; shift 2 ;;
        --conda-env) CONDA_ENV="${2:?missing value}"; shift 2 ;;
        --collect-db-snapshot) COLLECT_DB_SNAPSHOT=true; shift ;;
        -h|--help)
            echo "Usage: $0 --sample-dir PATH --output-root PATH --dbms pg|mysql [connection options]"
            exit 0
            ;;
        *) die "unknown option: $1" ;;
    esac
done

[[ -n "$OPENAI_API_KEY" ]] || die "OPENAI_API_KEY is required"
[[ -n "$DBAIOPS_KNOWLEDGE_API_KEY" ]] ||
    die "DBAIOPS_KNOWLEDGE_API_KEY is required for the native DBAIOps knowledge service"
[[ -n "$SAMPLE_DIR" && -d "$SAMPLE_DIR" ]] || die "valid --sample-dir is required"
[[ "$DBMS_KIND" == "pg" || "$DBMS_KIND" == "mysql" ]] || die "--dbms must be pg or mysql"
[[ -f "$CONDA_HOME/etc/profile.d/conda.sh" ]] || die "Conda initialization script not found"

# shellcheck disable=SC1091
source "$CONDA_HOME/etc/profile.d/conda.sh"
conda activate "$CONDA_ENV"

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)_$(basename "$SAMPLE_DIR")"
RUN_DIR="$OUTPUT_ROOT/$RUN_ID"
mkdir -p "$RUN_DIR"
SNAPSHOT="$RUN_DIR/database_snapshot.json"

if [[ "$COLLECT_DB_SNAPSHOT" == "true" ]]; then
    python "$SCRIPT_DIR/collect_db_snapshot.py" \
        --dbms "$DBMS_KIND" \
        --host "$HOST" \
        --port "$PORT" \
        --user "$USER" \
        --password "$PASSWORD" \
        --database "$DB" \
        --output "$SNAPSHOT"
else
    printf '{"mode":"native_knowledge_qa_without_live_snapshot"}\n' >"$SNAPSHOT"
fi

python "$SCRIPT_DIR/native_runner.py" \
    --sample-dir "$SAMPLE_DIR" \
    --output-dir "$RUN_DIR" \
    --snapshot "$SNAPSHOT" \
    --dbms "$DBMS_KIND" \
    --dataset-id "$DBAIOPS_DATASET_ID" \
    --knowledge-url "$DBAIOPS_KNOWLEDGE_URL" \
    --knowledge-api-key "$DBAIOPS_KNOWLEDGE_API_KEY" \
    --model-url "$OPENAI_API_BASE" \
    --model-api-key "$OPENAI_API_KEY" \
    --model "$OPENAI_MODEL" \
    --provider "$OPENAI_PROVIDER" \
    --api-version "$OPENAI_API_VERSION" \
    --temperature "$TEMPERATURE" \
    --max-tokens "$MAX_TOKENS" \
    --retries "$DBAIOPS_RETRY_TIMES" \
    --context-chars "$MAX_CONTEXT_CHARS"

printf 'DBAIOps knowledge-Q&A diagnosis completed: %s\n' "$RUN_DIR"
