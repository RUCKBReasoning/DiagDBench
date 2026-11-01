#!/usr/bin/env python3
"""Offline historical-snapshot migration; never a fresh production score.

Adapted from the original Test/re_eval. Preserve measured evidence, native
scores and unrelated legacy channels; remove only the remediation-exit gate.
Database/model modules are never imported. The current production policy lives
in test_eval.py; this explicit archive utility does not add competing channels.
"""
from __future__ import annotations

import argparse
import ast
import copy
import datetime as dt
import hashlib
import io
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import tarfile
import uuid

POLICY = "2026-11-02-offline-script-gate-migration-v2"
TOKEN = "solution_not_executed_successfully"
CHANNEL = "scenarios.Reinitialized.optimized_llm_judge"
SCORER_AST_SHA256 = "bb9a87344ea29b3927375267f44da3ee82237cf8f97e45a7e8c2d313c1059bc7"
SCENE = "Reinitialized"


def sha(data):
    return hashlib.sha256(data).hexdigest()


def encoded(value):
    return (json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode()


def load(path):
    return json.loads(path.read_bytes())


def write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as f:
        f.write(data)
    path.chmod(0o600)


def safe(name):
    p = PurePosixPath(name)
    if p.is_absolute() or ".." in p.parts or not p.parts or str(p) != name:
        raise ValueError(f"Unsafe/noncanonical archive path: {name}")
    return Path(*p.parts)


def scorer(source):
    """Compile only reviewed pure scoring functions; ignore all evaluator imports."""
    functions = {"round_score", "compute_recovery_effect_score", "compute_sample_score", "evaluation_mode"}
    constants = {"SCORE_PRECISION", "DIAGNOSIS_SCORE_WEIGHT", "RECOVERY_SCORE_WEIGHT",
                 "RECOVERY_ZERO_SCORE_RATIO", "RECOVERY_FULL_SCORE_RATIO"}
    nodes = []
    for node in ast.parse(Path(source).read_bytes()).body:
        if isinstance(node, ast.FunctionDef) and node.name in functions:
            nodes.append(node)
        elif isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id in constants for t in node.targets):
            nodes.append(node)
    module = ast.Module(body=nodes, type_ignores=[])
    def canonical(node):
        # Python adds optional AST fields between releases (e.g. type_params).
        # Ignore empty fields, but retain every populated semantic field.
        if isinstance(node, ast.AST):
            return {"type": type(node).__name__, **{
                key: canonical(value) for key, value in ast.iter_fields(node)
                if value is not None and value != []}}
        if isinstance(node, list):
            return [canonical(value) for value in node]
        return node
    identity = sha(json.dumps(canonical(module), sort_keys=True, separators=(",", ":")).encode())
    if identity != SCORER_AST_SHA256:
        raise ValueError("Unsupported scoring implementation; review before historical migration")
    namespace = {"math": math}
    exec(compile(module, str(source), "exec"), namespace)
    return namespace["compute_sample_score"]


def operation_gate(outcome, semantic=None):
    measurement = outcome.get("measurement") or {}
    complete = (
        measurement.get("success") is True
        and not measurement.get("timed_out")
        and not measurement.get("errors")
        and measurement.get("error_count", 0) == 0
        and outcome.get("recovered_error_free") is True
        and outcome.get("initialization_errors_compatible") is True
    )
    # Use the SAME functional check as the original score channel.
    return bool(complete and (
        outcome.get("results_equivalent") is True
        if semantic is None else semantic.get("score") == 1
    ))


def replace_score(old, *, baseline, recovered, gate, compute, mode):
    """Leave every unaffected score byte-equivalent; remove one exact reason."""
    reasons = old.get("recovery_detail", {}).get("zero_reasons", [])
    if TOKEN not in reasons:
        return copy.deepcopy(old), False
    diagnosis = {"score": old["diagnosis_score"]}
    # Confirm original stored arithmetic before changing any input to the scorer.
    original = compute(diagnosis, old["recovery_detail"]["functional_gate"],
                       baseline, recovered, None, 8.0, mode=mode,
                       failure_reasons=reasons)
    for key in ("score", "diagnosis_score", "recovery_score", "performance_score"):
        if original[key] != old[key]:
            raise ValueError(f"Stored scorer mismatch in {key}")
    remaining = [x for x in reasons if x != TOKEN]
    new = compute(diagnosis, gate, baseline, recovered, None, 8.0,
                  mode=mode, failure_reasons=remaining)
    # Unknown diagnostic fields must not be silently removed.
    result = copy.deepcopy(old)
    result.update(new)
    result["recovery_detail"] = {**old["recovery_detail"], **new["recovery_detail"]}
    if result["diagnosis_score"] != old["diagnosis_score"] or result["score"] < old["score"]:
        raise ValueError("Removing the script gate changed diagnosis or lowered score")
    if any(x not in result["recovery_detail"]["zero_reasons"] for x in remaining):
        raise ValueError("An unrelated zero reason was removed")
    return result, True


def rescore(old, compute, source_hash):
    if old.get("offline_re_evaluation"):
        raise ValueError("Refuse double rescoring of an already-derived record")
    new = copy.deepcopy(old)
    scene = new["scenarios"][SCENE]
    baseline = scene["baseline_total_time"]
    optimized = scene["optimized"]
    recovered = optimized["recovered_total_time"]
    eligible = old.get("submission_valid") is True
    changes = []
    old_judge = copy.deepcopy(scene["optimized_llm_judge"])
    targets = [
        ("optimized", optimized["score_detail"],
         operation_gate(optimized)),
        ("optimized_llm_judge", scene["optimized_llm_judge"],
         operation_gate(optimized, None if scene["semantic_judge"].get("status") == "disabled"
                        else scene["semantic_judge"])),
    ]
    for name, detail, gate in targets:
        if not eligible:
            continue
        replacement, touched = replace_score(
            detail, baseline=baseline, recovered=recovered,
            gate=gate, compute=compute, mode=old["mode"])
        if touched:
            changes.append(name)
            if name == "optimized":
                optimized["score_detail"] = replacement
                optimized["functional_gate"] = gate
                optimized["failure_reasons"] = [
                    x for x in optimized["failure_reasons"] if x != TOKEN]
            else:
                scene[name] = replacement
    # Legacy top-level execution/fingerprint-score aliases are a distinct
    # channel. Recompute using their own original reasons, NOT the Judge score.
    root_detail = {
        k: old[k] for k in ("score", "diagnosis_score", "recovery_score", "performance_score")
    }
    root_detail["recovery_detail"] = old["recovery_score_detail"]
    if eligible:
        root_new, root_changed = replace_score(
            root_detail, baseline=old["baseline_total_time"],
            recovered=old["recovered_total_time"], gate=operation_gate(optimized),
            compute=compute, mode=old["mode"])
        if root_changed:
            changes.append("top_level_execution_aliases")
            for key in ("score", "recovery_score", "performance_score"):
                new[key] = root_new[key]
            new["recovery_score_detail"] = root_new["recovery_detail"]
            new["optimized_score"] = root_new["score"]
            new["optimized_performance_score"] = root_new["performance_score"]
            new["optimized_recovery_score_detail"] = copy.deepcopy(root_new["recovery_detail"])
            new["functional_gate"] = root_new["recovery_detail"]["functional_gate"]
            new["optimized_functional_gate"] = new["functional_gate"]
            new["recovery_successful"] = new["functional_gate"]
            for key in ("recovery_failure_reasons", "optimized_recovery_failure_reasons"):
                new[key] = [x for x in old[key] if x != TOKEN]
    if "optimized_llm_judge" in changes:
        new["optimized_llm_judge_score"] = scene["optimized_llm_judge"]["score"]
        new["optimized_llm_judge_performance_score"] = scene["optimized_llm_judge"]["performance_score"]
    # Raw evidence is immutable even when its scoring interpretation changes.
    if not (new["solution_executed"] == old["solution_executed"]):
        raise ValueError("Historical raw evidence changed during rescoring")
    if not (new["diagnosis_score"] == old["diagnosis_score"]):
        raise ValueError("Historical raw evidence changed during rescoring")
    if not (new["scenarios"].get("Post_Remediation") == old["scenarios"].get("Post_Remediation")):
        raise ValueError("Historical raw evidence changed during rescoring")
    if not (scene["original"] == old["scenarios"][SCENE]["original"]):
        raise ValueError("Historical raw evidence changed during rescoring")
    if not (scene["semantic_judge"] == old["scenarios"][SCENE]["semantic_judge"]):
        raise ValueError("Historical raw evidence changed during rescoring")
    if not (scene["optimized"]["measurement"] == old["scenarios"][SCENE]["optimized"]["measurement"]):
        raise ValueError("Historical raw evidence changed during rescoring")
    for key in old:
        if key.startswith("original_"):
            if not (new[key] == old[key]):
                raise ValueError("Historical raw evidence changed during rescoring")
    audit = {
        "policy": POLICY, "source_score_sha256": source_hash,
        "score_channel": CHANNEL, "removed_zero_reason": TOKEN,
        "scope": "Reinitialized optimized and optimized_llm_judge; matching top-level aliases",
        "applied_channels": changes,
        "script_success_required": False,
        "observation_solution_executed": old["solution_executed"],
        "measurement_unchanged": True, "diagnosis_unchanged": True,
        "original_workload_scores_unchanged": True, "post_remediation_unchanged": True,
        "old_primary_score": old_judge["score"],
        "new_primary_score": scene["optimized_llm_judge"]["score"],
        "old_primary_recovery_score": old_judge["performance_score"],
        "new_primary_recovery_score": scene["optimized_llm_judge"]["performance_score"],
        "old_primary_zero_reasons": old_judge["recovery_detail"]["zero_reasons"],
        "new_primary_zero_reasons": scene["optimized_llm_judge"]["recovery_detail"]["zero_reasons"],
    }
    new["offline_re_evaluation"] = audit
    return new, audit


def latest(day):
    dirs = sorted(p for p in day.iterdir() if p.is_dir()
                  and re.fullmatch(r"\d{4}-\d{2}-\d{2}_\d{6}", p.name))
    if not dirs:
        raise ValueError("No timestamped snapshots")
    if not (dirs[-1] / "SNAPSHOT_RECEIPT.json").is_file():
        raise ValueError("Latest timestamp is not fully published; do not silently use older data")
    return dirs[-1]


def verify_receipts(source):
    entries = {}
    receipts = {}
    for name in ("SNAPSHOT_RECEIPT.json", "TABLES_RECEIPT.json"):
        raw = (source / name).read_bytes()
        receipts[name] = sha(raw)
        for relative, digest in json.loads(raw)["files"].items():
            if relative in entries and entries[relative] != digest:
                raise ValueError("Conflicting source receipt")
            if sha((source / safe(relative)).read_bytes()) != digest:
                raise ValueError(f"Source receipt mismatch: {relative}")
            entries[relative] = digest
    return entries, receipts


def run(source, output_root, evaluator):
    source = source.resolve()
    output_root = output_root.resolve()
    if any(path.is_symlink() for path in source.rglob("*")):
        raise ValueError("Snapshot symlinks are not accepted")
    if (source / "RE_EVAL_MANIFEST.json").exists():
        raise ValueError("Select an original, not re-evaluated snapshot")
    destination = output_root / source.name
    if destination.exists():
        raise FileExistsError(f"Never overwrite existing output: {destination}")
    if source == destination or source in destination.parents:
        raise ValueError("Output cannot be inside source snapshot")
    verified, receipts = verify_receipts(source)
    compute = scorer(evaluator)
    registry = load(source / "code/servers.json")
    ids = [str(value) for value in registry["table_order"]]
    by_id = {str(r["server"]): r for r in registry["runs"]}
    if (not ids or len(set(ids)) != len(ids) or not set(ids) <= by_id.keys()
            or any(not re.fullmatch(r"[A-Za-z0-9_-]+", value) for value in ids)):
        raise ValueError("Invalid selected-run registry")
    output_root.mkdir(parents=True, exist_ok=True)
    temp = output_root / f".{source.name}.{uuid.uuid4().hex}.partial"
    temp.mkdir()
    # Preserve original receipts and metadata separately; do not fabricate new
    # remote collection checks or reinterpret observation times as rescoring.
    for name in receipts:
        write(temp / "provenance" / f"source_{name}", (source / name).read_bytes())
    for folder in ("code", "inspection", "inspection_after"):
        if (source / folder).is_dir():
            shutil.copytree(source / folder, temp / folder)
    for name in ("offline_rescore.py",):
        write(temp / "re_eval_code" / name, Path(__file__).read_bytes())
    write(temp / "re_eval_code/scoring_reference.py", evaluator.read_bytes())
    audits, runs_report = [], {}
    for run_id in ids:
        metadata_path = source / f"snapshots/server{run_id}.json"
        metadata = load(metadata_path)
        original_meta = copy.deepcopy(metadata)
        write(temp / "provenance" / metadata_path.name, metadata_path.read_bytes())
        check = source / f"snapshots/server{run_id}_recheck.json"
        write(temp / "snapshots" / check.name, check.read_bytes())
        valid = {str(r["sample_id"]): r for r in metadata["samples"] if r["valid"]}
        if any(not re.fullmatch(r"[A-Za-z0-9_-]+", sid) for sid in valid):
            raise ValueError("Unsafe sample identifier")
        file_index = {r["archive_path"]: r for r in metadata["files"]}
        if len(file_index) != len(metadata["files"]):
            raise ValueError("Duplicate archive manifest entry")
        filename = safe(metadata["archive_filename"])
        if len(filename.parts) != 1:
            raise ValueError("Archive filename must be a basename")
        archive = source / "archives" / filename
        if sha(archive.read_bytes()) != metadata["archive_sha256"]:
            raise ValueError("Source archive mismatch")
        target_archive = temp / "archives" / metadata["archive_filename"]
        target_archive.parent.mkdir(exist_ok=True)
        seen, sample_seen, per_run = set(), set(), []
        with tarfile.open(archive, "r|*") as inp, \
                tarfile.open(target_archive, "w:xz", preset=1) as target:
            for member in inp:
                safe(member.name)
                if not member.isfile() or member.name in seen or member.name not in file_index:
                    raise ValueError("Unsafe/duplicate/unindexed archive member")
                seen.add(member.name)
                raw = inp.extractfile(member).read()
                entry = file_index[member.name]
                if sha(raw) != entry["sha256"] or len(raw) != entry["bytes"]:
                    raise ValueError("Archived file differs from manifest")
                if member.name.endswith("/eval/score_detail.json"):
                    sid = PurePosixPath(member.name).parts[-3]
                    if sid in valid:
                        if sid in sample_seen:
                            raise ValueError("Duplicate sample score")
                        sample_seen.add(sid)
                        row = valid[sid]
                        if sha(raw) != row["score_sha256"]:
                            raise ValueError("Sample score hash mismatch")
                        original_hash = sha(raw)
                        old = json.loads(raw)
                        new, audit = rescore(old, compute, original_hash)
                        write(temp / "original_scores" / run_id / sid / "score_detail.json", raw)
                        audit.update(run_id=run_id, sample_id=sid, dbms=row["dbms"],
                                     archive_path=member.name)
                        audits.append(audit)
                        per_run.append(audit)
                        raw = encoded(new)
                        row["original_score_sha256"] = original_hash
                        row["score_sha256"] = sha(raw)
                        row["offline_re_evaluation"] = {
                            "policy": POLICY, "source_score_sha256": original_hash,
                            "measurement_unchanged": True,
                            "script_success_required": False,
                        }
                        for scene_name, projection in row["scores"].items():
                            sc = new["scenarios"][scene_name]
                            projection["original"] = sc["original"]["score_detail"]["score"]
                            projection["optimized"] = sc["optimized_llm_judge"]["score"]
                        entry["original_sha256"] = entry["sha256"]
                        entry["sha256"] = sha(raw)
                        entry["bytes"] = len(raw)
                info = copy.copy(member)
                info.size = len(raw)
                target.addfile(info, io.BytesIO(raw))
                # Full per-sample files at the same methods/<...>/<ID> paths.
                write(temp / safe(member.name), raw)
        if seen != set(file_index) or sample_seen != set(valid):
            raise ValueError("Archive/sample coverage changed")
        metadata["archive_sha256"] = sha(target_archive.read_bytes())
        metadata["archive_bytes"] = target_archive.stat().st_size
        metadata["offline_re_evaluation"] = {
            "policy": POLICY, "source_snapshot": str(source),
            "original_archive_sha256": original_meta["archive_sha256"],
            "source_metadata_sha256": sha(metadata_path.read_bytes()),
            "original_collection_checks_retained": True,
            "note": "evidence_hashes/source_path/mtime/production_resume_valid describe original capture",
        }
        write(temp / "snapshots" / metadata_path.name, encoded(metadata))
        runs_report[run_id] = {
            "valid": len(per_run),
            "script_gate_removed_records": sum(bool(x["applied_channels"]) for x in per_run),
            "primary_score_increased": sum(x["new_primary_score"] > x["old_primary_score"] for x in per_run),
            "still_zero_recovery": sum(x["new_primary_recovery_score"] == 0 for x in per_run),
        }
        print(json.dumps({"run": run_id, **runs_report[run_id]}), flush=True)
    manifest = {
        "policy": POLICY, "created_at": dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).isoformat(),
        "source_snapshot": str(source), "destination": str(destination),
        "source_receipts": receipts, "source_verified_files": verified,
        "evaluator_sha256": sha(evaluator.read_bytes()), "script_sha256": sha(Path(__file__).read_bytes()),
        "record_count": len(audits), "runs": runs_report,
        "primary_score_increased": sum(a["new_primary_score"] > a["old_primary_score"] for a in audits),
        "database_or_llm_execution": False, "source_unchanged": True,
        "score_formula_unchanged": True, "measurements_unchanged": True,
        "removed_zero_reason": TOKEN, "retained_diagnosis_weight": 0.3,
        "retained_recovery_weight": 0.7, "retained_latency_zero_ratio": 8.0,
        "record_audit_sha256": sha(encoded(audits)),
    }
    write(temp / "RE_EVAL_MANIFEST.json", encoded(manifest))
    write(temp / "re_evaluation_audit.json", encoded(audits))
    write(temp / "README.md", (
        "# Offline rescored snapshot\n\n"
        f"Policy: `{POLICY}`. Source: `{source}`.\n\n"
        "Only remediation-script-success gating is removed from Reinitialized optimized scores. "
        "Anomaly injection validity, measurements, original-workload scores, diagnosis and "
        "Post_Remediation remain unchanged. No Docker/SQL/LLM execution.\n\n"
        "Original scores are in original_scores/. Original receipt and metadata are in provenance/. "
        "Preserved observation hashes/timestamps refer to the source capture, not new live checks. "
        "The modified score hash is score_sha256; original_score_sha256 preserves lineage.\n"
    ).encode())
    # Recheck protected source bytes after the complete transformation.
    for relative, digest in verified.items():
        if sha((source / relative).read_bytes()) != digest:
            raise ValueError("Source changed while rescoring")
    paths = [p for p in temp.rglob("*") if p.is_file() and "methods" not in p.relative_to(temp).parts]
    receipt = {
        "phase": "offline_rescoring", "destination": str(destination),
        "published_at": manifest["created_at"], "policy": POLICY,
        "source_snapshot": str(source), "source_receipts": receipts,
        "files": {str(p.relative_to(temp)): sha(p.read_bytes()) for p in sorted(paths)},
        "all_hashes_verified": True,
    }
    write(temp / "SNAPSHOT_RECEIPT.json", encoded(receipt))
    if destination.exists():
        raise FileExistsError("Output appeared concurrently; partial directory retained")
    temp.rename(destination)
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    return destination


def verify(destination):
    """Verify all derived hashes and reproduce every changed score independently."""
    root = Path(destination).resolve()
    manifest = load(root / "RE_EVAL_MANIFEST.json")
    if manifest.get("policy") != POLICY:
        raise ValueError("Unexpected offline migration policy")
    receipt = load(root / "SNAPSHOT_RECEIPT.json")
    for name, expected in receipt["files"].items():
        if sha((root / safe(name)).read_bytes()) != expected:
            raise ValueError("Derived receipt mismatch: " + name)
    compute = scorer(root / "re_eval_code/scoring_reference.py")
    checked = 0
    for meta_path in (root / "snapshots").glob("server*.json"):
        if "_recheck" in meta_path.name:
            continue
        meta = load(meta_path)
        run_id = str(meta["server"])
        if not re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
            raise ValueError("Unsafe run identifier")
        valid = {str(row["sample_id"]): row for row in meta["samples"] if row["valid"]}
        archive = root / "archives" / safe(meta["archive_filename"])
        if sha(archive.read_bytes()) != meta["archive_sha256"]:
            raise ValueError("Derived archive mismatch")
        seen = set()
        with tarfile.open(archive, "r|*") as stream:
            for member in stream:
                safe(member.name)
                if not member.isfile():
                    raise ValueError("Non-file archive member")
                raw = stream.extractfile(member).read()
                if sha((root / safe(member.name)).read_bytes()) != sha(raw):
                    raise ValueError("Expanded artifact and archive disagree")
                if not member.name.endswith("/eval/score_detail.json"):
                    continue
                sid = PurePosixPath(member.name).parts[-3]
                if sid not in valid:
                    continue
                if sid in seen:
                    raise ValueError("Duplicate derived sample score")
                seen.add(sid)
                old_raw = (root / "original_scores" / run_id / safe(sid) / "score_detail.json").read_bytes()
                if sha(raw) != valid[sid]["score_sha256"] or sha(old_raw) != valid[sid]["original_score_sha256"]:
                    raise ValueError("Score lineage hash mismatch")
                reproduced, _ = rescore(json.loads(old_raw), compute, sha(old_raw))
                if reproduced != json.loads(raw):
                    raise ValueError("Derived score does not reproduce from frozen original")
                checked += 1
        if seen != set(valid):
            raise ValueError("Derived sample coverage changed")
    if checked != manifest["record_count"]:
        raise ValueError("Derived record count mismatch")
    return {"status": "passed", "policy": POLICY, "records_verified": checked,
            "database_or_llm_execution": False}


def main(argv=None):
    project = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, help="Original published historical snapshot")
    parser.add_argument("--source-day", type=Path, help="Explicit day directory; select its latest published snapshot")
    parser.add_argument("--output-root", type=Path, default=project / "Output/offline_re_evaluation")
    parser.add_argument("--evaluator", type=Path, default=project / "Evaluation/test_eval.py")
    parser.add_argument("--verify", type=Path, help="Verify an already derived historical snapshot only")
    args = parser.parse_args(argv)
    if args.verify:
        print(json.dumps(verify(args.verify), indent=2))
    else:
        if not args.source and not args.source_day:
            parser.error("Provide --source or --source-day; no dated production default is inferred")
        run(args.source or latest(args.source_day), args.output_root, args.evaluator)


if __name__ == "__main__":
    main()
