#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
    printf 'Usage: %s CONFIG_JSON\n' "$0" >&2
    exit 2
}

[[ $# -eq 1 ]] || usage

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"
CONFIG_PATH="$1"
[[ "$CONFIG_PATH" = /* ]] || CONFIG_PATH="$PROJECT_ROOT/$CONFIG_PATH"
[[ -f "$CONFIG_PATH" ]] || {
    printf 'ERROR: configuration does not exist: %s\n' "$CONFIG_PATH" >&2
    exit 1
}

PYTHON_BIN="${DB_DIAG_PYTHON:-$(command -v python3 || true)}"
[[ -n "$PYTHON_BIN" ]] && command -v "$PYTHON_BIN" >/dev/null || {
    printf 'ERROR: activate the runtime environment or set DB_DIAG_PYTHON.\n' >&2
    exit 1
}

eval "$("$PYTHON_BIN" - "$CONFIG_PATH" <<'PY'
import json
import re
import shlex
import sys
from pathlib import Path

path = Path(sys.argv[1]).resolve()
config = json.loads(path.read_text(encoding="utf-8"))
required = (
    "workflow_type", "docker_name", "host_port",
    "parallelism_num", "output_base_dir",
)
missing = [key for key in required if key not in config]
if missing:
    raise SystemExit("Missing configuration fields: " + ", ".join(missing))

if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", str(config["docker_name"])):
    raise SystemExit("Invalid Docker container prefix")
parallelism = int(config["parallelism_num"])
if parallelism < 1:
    raise SystemExit("parallelism_num must be positive")

values = {
    "WORKFLOW_TYPE": str(config["workflow_type"]).lower(),
    "DOCKER_PREFIX": str(config["docker_name"]),
    "HOST_PORT": str(int(config["host_port"])),
    "PARALLELISM": str(parallelism),
    "OUTPUT_DIR": str(Path(config["output_base_dir"]).resolve()),
    "MODEL_NAME": str(config.get("llm_configuration", {}).get("model_name", "")),
}
for key, value in values.items():
    print(f"{key}={shlex.quote(value)}")
PY
)"

case "$WORKFLOW_TYPE" in
    react|react_super|react_common)
        ENTRYPOINT="$PROJECT_ROOT/Baselines/ReAct/react.py"
        ;;
    codex)
        ENTRYPOINT="$PROJECT_ROOT/Baselines/Codex/codex.py"
        ;;
    claude_code)
        ENTRYPOINT="$PROJECT_ROOT/Baselines/Claude_Code/claude_code.py"
        ;;
    db_gpt|dbgpt)
        ENTRYPOINT="$PROJECT_ROOT/Baselines/DB-GPT/db_gpt.py"
        ;;
    dbaiops)
        ENTRYPOINT="$PROJECT_ROOT/Baselines/DBAIOps/dbaiops.py"
        ;;
    *)
        printf 'ERROR: unsupported workflow: %s\n' "$WORKFLOW_TYPE" >&2
        exit 1
        ;;
esac

if docker ps -a --format '{{.Names}}' | grep -Eq "^${DOCKER_PREFIX}_w[0-9]+([_-]|$)"; then
    printf 'ERROR: existing Docker containers use prefix %s_w*: \n' "$DOCKER_PREFIX" >&2
    docker ps -a --format '{{.Names}}' | grep -E "^${DOCKER_PREFIX}_w[0-9]+([_-]|$)" >&2
    exit 1
fi

"$PYTHON_BIN" - "$HOST_PORT" "$PARALLELISM" <<'PY'
import socket
import sys

start = int(sys.argv[1])
count = int(sys.argv[2])
sockets = []
try:
    for port in range(start, start + count):
        sock = socket.socket()
        sock.bind(("0.0.0.0", port))
        sockets.append(sock)
except OSError as exc:
    raise SystemExit(f"Port {port} is unavailable: {exc}")
finally:
    for sock in sockets:
        sock.close()
PY

"$PYTHON_BIN" "$ENTRYPOINT" --config "$CONFIG_PATH" --dry-run

mkdir -p "$OUTPUT_DIR"
exec 9>"$OUTPUT_DIR/.baseline_launch.lock"
flock -n 9 || {
    printf 'ERROR: this output already has an active launcher\n' >&2
    exit 1
}
SNAPSHOT="$OUTPUT_DIR/$(basename "$CONFIG_PATH")"
TEMP_SNAPSHOT="$SNAPSHOT.tmp.$$"
cp -- "$CONFIG_PATH" "$TEMP_SNAPSHOT"
chmod 600 "$TEMP_SNAPSHOT"
mv -- "$TEMP_SNAPSHOT" "$SNAPSHOT"

printf 'Starting baseline: workflow=%s model=%s parallelism=%s docker_prefix=%s ports=%s-%s\n' \
    "$WORKFLOW_TYPE" "$MODEL_NAME" "$PARALLELISM" "$DOCKER_PREFIX" "$HOST_PORT" \
    "$((HOST_PORT + PARALLELISM - 1))"
printf 'Configuration snapshot: %s\n' "$SNAPSHOT"

exec "$PYTHON_BIN" -u "$ENTRYPOINT" --config "$CONFIG_PATH"
