#!/usr/bin/env python3
"""Verify LFS parts and atomically restore the PostgreSQL IMDB seed dump."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile


DIRECTORY = Path(__file__).resolve().parent / "postgresql_instances"
MANIFEST = "imdb.dump.parts.json"
BLOCK_BYTES = 8 * 1024 * 1024


def digest(path):
    sha = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(BLOCK_BYTES), b""):
            sha.update(block)
    return sha.hexdigest()


def load_manifest(directory):
    manifest = json.loads((directory / MANIFEST).read_text(encoding="utf-8"))
    if manifest.get("version") != 1 or manifest.get("filename") != "imdb.dump":
        raise ValueError("Unsupported dump manifest.")
    if not isinstance(manifest.get("size"), int) or manifest["size"] <= 0:
        raise ValueError("Invalid original dump size.")
    if not re.fullmatch(r"[0-9a-f]{64}", manifest.get("sha256", "")):
        raise ValueError("Invalid original dump SHA-256.")
    parts = manifest.get("parts")
    if not isinstance(parts, list) or not parts:
        raise ValueError("The manifest contains no parts.")
    for number, part in enumerate(parts):
        if part.get("filename") != f"imdb.dump.part{number:03d}":
            raise ValueError("Part names must be consecutive and ordered from part000.")
        if not isinstance(part.get("size"), int) or not 0 < part["size"] <= 1024**3:
            raise ValueError("Each part must be no larger than 1 GiB.")
        if not re.fullmatch(r"[0-9a-f]{64}", part.get("sha256", "")):
            raise ValueError("Invalid part SHA-256.")
    if sum(part["size"] for part in parts) != manifest["size"]:
        raise ValueError("Part sizes do not sum to the original dump size.")
    expected = {part["filename"] for part in parts}
    actual = {p.name for p in directory.glob("imdb.dump.part[0-9][0-9][0-9]")}
    if actual != expected:
        raise ValueError(f"Part set mismatch: missing={expected-actual}, extra={actual-expected}")
    return manifest


def verify_parts(directory, manifest, output=None):
    combined = hashlib.sha256()
    for part in manifest["parts"]:
        path = directory / part["filename"]
        if path.is_symlink() or not path.is_file() or path.stat().st_size != part["size"]:
            raise ValueError(f"Missing or wrong-size part: {path.name}. Run git lfs pull.")
        sha = hashlib.sha256()
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(BLOCK_BYTES), b""):
                sha.update(block)
                combined.update(block)
                if output is not None:
                    output.write(block)
        if sha.hexdigest() != part["sha256"]:
            raise ValueError(f"SHA-256 mismatch: {path.name}")
    if combined.hexdigest() != manifest["sha256"]:
        raise ValueError("Combined parts do not match the original dump SHA-256.")


def restore(directory=DIRECTORY, check_only=False):
    directory = Path(directory)
    manifest = load_manifest(directory)
    target = directory / manifest["filename"]
    if target.is_symlink():
        raise ValueError("Refusing to use a symlink as the restored dump.")
    if check_only or target.exists():
        verify_parts(directory, manifest)
        if target.exists() and (
            target.stat().st_size != manifest["size"]
            or digest(target) != manifest["sha256"]
        ):
            raise ValueError("Existing imdb.dump differs from the manifest; it was not overwritten.")
        print("IMDB parts verified; original SHA-256: " + manifest["sha256"])
        if check_only:
            return
        print("Existing imdb.dump already matches; no restoration needed.")
        return
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=directory, prefix="imdb.dump.restore-", delete=False
        ) as output:
            temporary = Path(output.name)
            verify_parts(directory, manifest, output)
            output.flush()
            os.fsync(output.fileno())
        # Publish only a fully verified file; do not replace a concurrently
        # created dump. The temporary file is on the same filesystem.
        os.link(temporary, target)
        print(f"Restored {target} ({manifest['size']} bytes).")
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Verify parts and any existing dump without writing files.")
    args = parser.parse_args()
    try:
        restore(check_only=args.check)
    except (OSError, ValueError, TypeError, KeyError) as exc:
        parser.exit(1, f"ERROR: {exc}\n")


if __name__ == "__main__":
    main()
