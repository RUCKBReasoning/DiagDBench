import json
from pathlib import Path
from Baselines.input_contract import anomaly_verified_workload

def build_prompt(max_turns, hardware, user_config, superuser_config, files):
    """Request observable DBA evidence and native tools, without reasoning transcripts."""
    sections = [
        "# Database diagnosis task",
        "Diagnose and recover the supplied incident in its isolated evaluation database. "
        "Inspect the database using db_execute, perform targeted database recovery, and "
        "validate both phases of the supplied anomaly-verified workload before finishing. "
        "Use the configured normal account for workload validation and the supplied "
        "administrator account only where needed. Preserve committed data and workload semantics. "
        "Do not restart the OS, destroy containers, or kill unrelated processes. "
        "Keep observations bounded with selective queries, LIMIT, or aggregation. "
        "Use non-interactive Bash with pagers disabled; each script must begin with "
        "'# TIMEOUT THRESHOLD: <seconds>'.",
        "Submit db_finish_diagnosis after inspection and validation. Provide a Markdown "
        "report of observed evidence, root cause, actions taken, validation results and "
        "unresolved issues; provide a reusable Bash recovery script. Treat the original "
        "verified workload as an ordered sequence. Supply non-empty optimized SQL only "
        "when it has a concrete potential performance benefit, retains exactly the same "
        "number of statements in exactly the same order, and preserves each statement's "
        "semantics and order-sensitive logic. Starting from the same initialized database "
        "state, sequential execution must produce exactly the same observable result for "
        "every corresponding statement, including returned rows and values, duplicate "
        "counts, affected-row counts, and persistent database state. Do not add, delete, "
        "merge, split, replace, or reorder statements. Any execution-result mismatch is "
        "a diagnosis failure. If exact equivalence cannot be guaranteed, leave "
        "verified_workload empty so the original workload is retained. "
        "The report should contain verifiable findings and concise explanations.",
        f"Maximum tool actions including finish: {max_turns}.",
        "## Hardware\n" + json.dumps(hardware, ensure_ascii=False),
        "## Database account\n" + json.dumps(user_config, ensure_ascii=False),
        "## Administrator account\n" + json.dumps(superuser_config, ensure_ascii=False),
    ]
    sections.append("## Anomaly-verified workload\n" + anomaly_verified_workload(files))
    sections.append(
        "## Observed symptoms\n" + files["phenomenon"].read_text(encoding="utf-8")
    )
    return "\n\n".join(sections)


def finish_result(output):
    path = output / "native_finish.json"
    if not path.is_file():
        raise RuntimeError("Native agent ended without db_finish_diagnosis and required artifacts.")
    return json.loads(path.read_text(encoding="utf-8"))


def fatal_api_error(output, error):
    # Suite supervisor detects this immediately, stops both workers and removes
    # only containers belonging to this framework's configured prefix.
    target = Path(output).parent / "fatal_api_error.json"
    temp = target.with_name(f".fatal_{Path(output).name}.json")
    temp.write_text(json.dumps({"sample_id": Path(output).name, "error": str(error)}, ensure_ascii=False), encoding="utf-8")
    temp.replace(target)
