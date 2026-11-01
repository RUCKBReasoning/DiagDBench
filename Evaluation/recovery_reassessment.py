"""Explicit report-only conversion and hash-bound diagnosis-score preservation.

Called by the normal Pipeline; never patches globals, runs native diagnosis,
executes repair scripts, or supplies private score evidence to the converter.
"""
from __future__ import annotations

import copy
import hashlib
import json
import math
from pathlib import Path
import re
import subprocess

from Baselines import recovery_artifacts as recovery

VERSION = "2026-11-02-native-report-reassessment-v1"
POLICY_FILE = "diagnosis_score_policy.json"


def digest(data):
    return hashlib.sha256(data).hexdigest()


def validate_roots(source, output):
    source, output = Path(source).resolve(), Path(output).resolve()
    if not source.is_dir():
        raise ValueError("native_output_root must be an existing diagnosis output directory")
    if source == output or source in output.parents or output in source.parents:
        raise ValueError("Original and reassessment output directories must be disjoint")
    return source


def source_report(source, sample_id):
    sample_id = str(sample_id)
    if Path(sample_id).name != sample_id or sample_id in {".", ".."}:
        raise ValueError("Noncanonical sample ID")
    directory = Path(source) / sample_id / "baseline"
    status = json.loads((directory / "status.json").read_text())
    if status.get("status") not in {"completed", "failed"}:
        raise ValueError("Do not convert an in-flight or unfinalized native diagnosis")
    report = recovery.read_utf8(directory / "diagnosis_report.md")
    if not report.strip():
        raise ValueError("Native diagnosis report is empty")
    return directory, report


def source_score_policy(source, sample_id, report, artifact_status):
    path = Path(source) / str(sample_id) / "eval/score_detail.json"
    policy = {"version": VERSION, "policy": "judge_native_report_once",
              "report_sha256": digest(report.encode()), "sample_id": str(sample_id)}
    if not path.is_file():
        return policy
    raw = path.read_bytes()
    score = json.loads(raw)
    value = score.get("diagnosis_score")
    if value is None:
        return policy
    if (type(value) not in (int, float) or not math.isfinite(value) or not 0 <= value <= 1
            or str(score.get("sample_id", sample_id)) != str(sample_id)):
        raise ValueError("Source diagnosis score/sample ID is invalid")
    original = Path(source) / str(sample_id) / "baseline"
    expected = (score.get("artifact_validation") or {}).get("digest")
    if not expected or artifact_status(original)["digest"] != expected:
        raise ValueError("Source artifacts no longer match their scored submission")
    detail_path = path.parent / "diagnosis_judge_detail.json"
    detail = json.loads(detail_path.read_text()) if detail_path.is_file() else {}
    if detail.get("score", value) != value:
        raise ValueError("Source diagnosis detail and score disagree")
    policy.update(policy="preserve_original_diagnosis_score", score=value, detail=detail,
                  source_score_sha256=digest(raw), source_artifact_digest=expected,
                  source_detail_sha256=digest(detail_path.read_bytes()) if detail_path.is_file() else None)
    return policy


def convert_report(report, evidence, output, method, llm_config, request, write_json):
    """The same public evidence, authorization and artifact contract as native adapters."""
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    prompt = recovery.build_recovery_prompt({"native_diagnosis_report": report, "public_evidence": evidence})
    (output / "conversion_prompt.txt").write_text(prompt)
    (output / "conversion_prompt.txt").chmod(0o600)
    config = dict(llm_config, allow_context_truncation=False)
    config.setdefault("max_output_tokens", 16384)
    config.setdefault("timeout", 300)
    errors = []
    for attempt in range(3):
        # Transport/quota errors propagate, rather than being retried as schema errors.
        text = request([prompt + ("\nCorrect this validation error and return complete JSON: " + errors[-1] if errors else "")], config)
        raw_path = output / f"conversion_response_{attempt + 1}.txt"
        raw_path.write_text(str(text)); raw_path.chmod(0o600)
        try:
            if getattr(text, "truncated", False):
                raise ValueError("Model response was truncated; regenerate complete artifacts")
            raw = str(text).strip()
            if raw.startswith("```"):
                raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.I)
            generated = recovery.validate_artifacts(json.loads(raw))
            break
        except (ValueError, subprocess.SubprocessError) as exc:
            errors.append(str(exc))
    else:
        raise ValueError("Recovery conversion exhausted schema repair: " + "; ".join(errors))
    original = evidence["public_inputs"]["verified_workload.sql"]
    values = {"diagnosis_report.md": report, "solution_script.sh": generated["solution_script.sh"],
              "optimized_verified_workload.sql": recovery.effective_workload(generated["verified_workload.sql"], original)}
    for name, text in values.items():
        (output / name).write_bytes(text.encode())
    (output / "solution_script.sh").chmod(0o700)
    write_json(output / "artifact_action.json", dict(generated, action="finish", **{"diagnosis_report.md": report}))
    return recovery.conversion_metadata(method, report, generated, original, prompt, evidence)


def prepare_sample(pipeline, case, sample_id, logger):
    """Pipeline.stage_baseline branch for explicitly configured report reassessment."""
    from Evaluation import test_eval as scoring
    from Evaluation.test_baseline_eval import archive, read_json_if_file
    from llm_api import get_llm_response

    source = pipeline.config["native_output_root"]
    _, report = source_report(source, sample_id)
    _, sample = pipeline.driver.find_sample_directory(pipeline.anomaly_root, case)
    evidence = recovery.collect_public_evidence(sample)
    policy = source_score_policy(source, sample_id, report, scoring.agent_artifact_status)
    config = pipeline.config["diagnosis"]["llm_configuration"]
    # Hash generation settings but never persist credentials or private score input
    # in the model prompt. The latter is used only by the trusted scorer below.
    settings = {k: v for k, v in config.items() if k not in {"api_key", "api_key_env"}}
    identity = digest(json.dumps({"version": VERSION, "converter": recovery.VERSION, "report": report,
        "evidence": evidence, "settings": settings, "policy": policy}, ensure_ascii=False, sort_keys=True).encode())
    sample_root = pipeline.root / str(sample_id)
    output = sample_root / "baseline"
    meta = read_json_if_file(output / "conversion_meta.json")
    valid = (isinstance(meta, dict) and meta.get("reassessment_identity") == identity
             and meta.get("version") == recovery.VERSION
             and set(meta.get("artifact_sha256", {})) == set(scoring.REQUIRED_AGENT_ARTIFACTS))
    if valid:
        valid = all((output / name).is_file() and digest((output / name).read_bytes()) == expected
                    for name, expected in meta["artifact_sha256"].items())
        valid = valid and read_json_if_file(output / POLICY_FILE) == policy
    if not valid:
        archive(output, sample_root, "conversion")
        archive(sample_root / "eval", sample_root, "reassessment")
        try:
            meta = convert_report(report, evidence, output, pipeline.config["method_name"], config,
                                  get_llm_response, scoring.write_json_atomic)
            meta.update(reassessment_identity=identity, reassessment_version=VERSION)
            scoring.write_json_atomic(output / POLICY_FILE, policy)
            scoring.write_json_atomic(output / "conversion_meta.json", meta)
            (output / "conversion_meta.json").chmod(0o600)
        except Exception:
            output.mkdir(parents=True, exist_ok=True)
            scoring.write_json_atomic(output / "status.json", {"status": "conversion_failed", "stage": "report_reassessment"})
            raise
        scoring.write_json_atomic(output / "status.json", {"status": "completed", "stage": "report_reassessment",
            "converter_version": recovery.VERSION, "reassessment_identity": identity})
    logger.info("[REASSESSMENT] %s native report; diagnosis score policy=%s",
                "Reused hash-checked" if valid else "Converted", policy["policy"])
    return {"status": "completed", "stage": "report_reassessment"}


def preserved_diagnosis(source, sample_id, artifact_directory, artifact_status):
    """Revalidate source and compiled report per sample; no thread-local/global patch."""
    output = Path(artifact_directory)
    policy = json.loads((output / POLICY_FILE).read_text())
    _, report = source_report(source, sample_id)
    expected = source_score_policy(source, sample_id, report, artifact_status)
    if policy != expected or digest((output / "diagnosis_report.md").read_bytes()) != expected["report_sha256"]:
        raise ValueError("Native diagnosis/report score binding changed after conversion")
    if policy["policy"] == "judge_native_report_once":
        return None
    detail = copy.deepcopy(policy["detail"])
    detail.update(score=policy["score"], recovery_reassessment_preserved=True,
                  source_score_sha256=policy["source_score_sha256"], report_sha256=policy["report_sha256"])
    return detail


def unfinished(result, selected_ids):
    """A completed zero recovery score is not a retry candidate."""
    rows = result.get("sample_summaries", [])
    selected = {str(value) for value in selected_ids}
    if len(rows) != len(selected) or {str(row.get("sample_id")) for row in rows} != selected:
        raise ValueError("Final aggregate does not cover the selected cohort exactly once")
    if any("fatal" in str(row.get("error", "")).lower() for row in rows):
        raise ValueError("A fatal stop must not be continued automatically")
    return [str(row["sample_id"]) for row in rows if row.get("eval_status") != "completed"]


def run_reassessment(pipeline, selected_ids=None):
    """Locked, bounded continuation within the normal controller, not PID polling."""
    import fcntl
    import signal
    import time
    from Evaluation import test_eval as scoring

    eligible = []
    for case in pipeline.cases:
        sid = scoring.anomaly_sample_id(case)
        _, sample = pipeline.driver.find_sample_directory(pipeline.anomaly_root, case)
        dbms = pipeline.driver.load_json_file(sample / "config.json").get("dbms", case.get("dbms"))
        if pipeline.config["diagnosis"]["workflow_type"] == "db_gpt" and pipeline.driver.normalize_dbms(dbms) != "pg":
            continue
        try:
            source_report(pipeline.config["native_output_root"], sid)
        except (OSError, ValueError):
            continue
        eligible.append(sid)
    selected = {str(value) for value in (selected_ids or eligible)}
    if not selected or not selected <= set(eligible):
        raise ValueError("Select terminal nonempty native reports supported by this method")
    pipeline.root.mkdir(parents=True, exist_ok=True)
    with (pipeline.root / "reassessment.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        scoring.write_json_atomic(pipeline.root / "cohort_manifest.json", {
            "version": VERSION, "converter_version": recovery.VERSION,
            "selected_sample_ids": sorted(selected), "eligible_sample_ids": eligible,
            "score_channel": scoring.SCORE_CHANNEL, "native_reports_not_regenerated": True,
        })
        def stop(signum, frame):
            pipeline.logger.warning("Stop requested; draining active workers")
            pipeline.stop_event.set()
        previous = {sig: signal.signal(sig, stop) for sig in (signal.SIGTERM, signal.SIGINT)}
        try:
            for attempt in range(pipeline.config.get("continuation_passes", 0) + 1):
                path = pipeline.root / "evaluation_result.json"
                before = path.stat().st_mtime_ns if path.exists() else None
                try:
                    return pipeline.run(selected)
                except SystemExit as exc:
                    if exc.code != 1 or pipeline.stop_event.is_set() or pipeline.fatal_marker.exists():
                        raise
                if not path.is_file() or path.stat().st_mtime_ns == before:
                    raise RuntimeError("No freshly finalized aggregate; continuation refused")
                result = json.loads(path.read_text())
                pending = unfinished(result, selected)
                if not pending:
                    return result
                if attempt == pipeline.config.get("continuation_passes", 0):
                    raise SystemExit(1)
                scoring.write_json_atomic(pipeline.root / "_continuation" / f"pass_{attempt + 1}.json", result)
                for _ in range(120):
                    if pipeline.stop_event.is_set():
                        raise SystemExit(42)
                    time.sleep(1)
                # Include the whole cohort in each final aggregate. Completed
                # samples are reused by the existing hash/protocol resume gates.
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)
