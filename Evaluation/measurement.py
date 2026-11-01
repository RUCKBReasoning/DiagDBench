"""Bounded-memory, lossless fingerprints compatible with the existing evaluator.

Rows are never truncated. Unordered results use the same Python tuple ordering and
JSON representation as the old in-memory implementation, with an external merge
sort when necessary. Only compact fingerprints cross the worker IPC boundary.
"""
import hashlib
import heapq
import json
from pathlib import Path
import tempfile
import time


def capture_result(cursor, sql, serialize, normalize, *, batch_size=256,
                   chunk_bytes=8 * 1024 * 1024, chunk_rows=8192, merge_fan_in=32):
    if cursor.description is None:
        return {"result_type": "command",
                "status_message": getattr(cursor, "statusmessage", ""),
                "row_count": cursor.rowcount}
    import sqlparse
    started = time.perf_counter()
    retrieval_seconds = 0.0
    columns = [column[0] for column in cursor.description]
    parsed = sqlparse.parse(str(sql))
    ordered = bool(parsed and any(t.normalized == "ORDER BY" for t in parsed[0].tokens))
    digest = hashlib.sha256()
    digest.update(("[" + json.dumps(columns, ensure_ascii=False) + ", [").encode("utf-8"))
    count = 0
    emitted = 0

    def update(row):
        nonlocal emitted
        if emitted:
            digest.update(b", ")
        digest.update(json.dumps(row, ensure_ascii=False).encode("utf-8"))
        emitted += 1

    def rows_from(path):
        with path.open(encoding="utf-8") as stream:
            for line in stream:
                yield tuple(json.loads(line))

    with tempfile.TemporaryDirectory(prefix="dbdiag_result_capture_") as directory:
        directory = Path(directory)
        paths, buffered = [], []
        buffered_bytes = 0
        serial = 0

        def spill():
            nonlocal buffered, buffered_bytes, serial
            buffered.sort()
            path = directory / str(serial)
            serial += 1
            with path.open("w", encoding="utf-8") as stream:
                for row in buffered:
                    stream.write(json.dumps(row, ensure_ascii=False) + "\n")
            paths.append(path)
            buffered, buffered_bytes = [], 0

        while True:
            before = time.perf_counter()
            batch = cursor.fetchmany(batch_size)
            serialized = [[serialize(value) for value in row] for row in batch]
            retrieval_seconds += time.perf_counter() - before
            if not batch:
                break
            for row in serialized:
                normalized = tuple(normalize(value) for value in row)
                count += 1
                if ordered:
                    update(normalized)
                else:
                    buffered.append(normalized)
                    buffered_bytes += sum(len(value.encode("utf-8")) + 64 for value in normalized)
                    if len(buffered) >= chunk_rows or buffered_bytes >= chunk_bytes:
                        spill()
            del serialized, batch

        if not ordered:
            if paths:
                if buffered:
                    spill()
                # Bound the number of simultaneously open merge streams.
                while len(paths) > merge_fan_in:
                    merged = []
                    for start in range(0, len(paths), merge_fan_in):
                        group = paths[start:start + merge_fan_in]
                        path = directory / str(serial)
                        serial += 1
                        with path.open("w", encoding="utf-8") as stream:
                            for row in heapq.merge(*(rows_from(p) for p in group)):
                                stream.write(json.dumps(row, ensure_ascii=False) + "\n")
                        for old in group:
                            old.unlink()
                        merged.append(path)
                    paths = merged
                for row in heapq.merge(*(rows_from(p) for p in paths)):
                    update(row)
            else:
                for row in sorted(buffered):
                    update(row)
        digest.update(b"]]")
    return {
        "result_type": "rows", "status_message": getattr(cursor, "statusmessage", ""),
        "row_count": count, "columns": columns,
        "_fingerprint": ("rows", count, digest.hexdigest()),
        # Hashing/sorting was outside SQL timing previously; keep it excluded.
        "_capture_overhead_seconds": max(0.0, time.perf_counter() - started - retrieval_seconds),
    }


def validate_measurement_runs(value):
    if type(value) is not int or value <= 0:
        raise ValueError("median_runs must be a positive integer.")
    return value


def protocol_matches(detail, shared, median_runs, digest):
    """Only exactly matching current measurement protocols can resume."""
    validate_measurement_runs(median_runs)
    return (detail.get("measurement_median_runs") == median_runs
            and detail.get("protocol_digest") == digest(shared, median_runs))
