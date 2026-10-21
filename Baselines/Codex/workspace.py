"""Evidence workspace with no-follow file access and explicit writable paths."""
from __future__ import annotations

import hashlib
import os
from pathlib import Path, PurePosixPath
import stat
import uuid

MAX_FILE_BYTES = 2 * 1024 * 1024
MAX_WORKSPACE_BYTES = 32 * 1024 * 1024
MAX_FILES = 256


def clean_parts(path: str) -> tuple[str, ...]:
    if not path or "\x00" in path or "\\" in path:
        raise ValueError("Use a relative POSIX file path.")
    parts = PurePosixPath(path).parts
    if PurePosixPath(path).is_absolute() or any(x in {".", ".."} or x.startswith(".") for x in parts):
        raise ValueError("Absolute, parent and hidden paths are not allowed.")
    if not parts or parts[0] not in {"inputs", "work"}:
        raise ValueError("Only inputs/ and work/ are visible.")
    return parts


def page_file(path: Path, offset: int = 0, limit: int = 16000) -> dict:
    """UTF-8 byte cursor. Decode only complete codepoints at page boundaries."""
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ValueError("Only regular, non-linked files can be read.")
        stream.seek(offset)
        raw = stream.read(limit)
        # A continuation byte at arbitrary user offsets is replaced; normal
        # next_offset cursors never split a UTF-8 character.
        if offset + len(raw) < info.st_size:
            for trim in range(4):
                try:
                    text = raw[:len(raw)-trim if trim else None].decode("utf-8")
                    if trim:
                        raw = raw[:-trim]
                    break
                except UnicodeDecodeError as exc:
                    if exc.end != len(raw) - trim or trim == 3:
                        text = raw.decode("utf-8", errors="replace")
                        break
        else:
            text = raw.decode("utf-8", errors="replace")
    next_offset = offset + len(raw)
    return {"text": text, "offset": offset, "bytes": len(raw), "total_bytes": info.st_size,
            "has_more": next_offset < info.st_size,
            "next_offset": next_offset if next_offset < info.st_size else None}


class Workspace:
    def __init__(self, root: Path):
        self.root = root
        if root.is_symlink():
            raise ValueError("Workspace root must not be a symlink.")
        root.mkdir(mode=0o755, parents=True, exist_ok=True)
        for name in ("inputs", "work"):
            target = root / name
            if target.is_symlink():
                raise ValueError("Workspace directory must not be a symlink.")
            target.mkdir(mode=0o755, exist_ok=True)

    def path(self, relative: str, *, write: bool = False, create_parents: bool = False) -> Path:
        parts = clean_parts(relative)
        if write and parts[0] != "work":
            raise ValueError("Only work/ is writable.")
        target = self.root
        for part in parts[:-1]:
            target /= part
            if create_parents and not target.exists():
                target.mkdir(mode=0o755)
            mode = target.lstat().st_mode
            if not stat.S_ISDIR(mode) or stat.S_ISLNK(mode):
                raise ValueError("Symlink or non-directory component rejected.")
        target /= parts[-1]
        if target.is_symlink():
            raise ValueError("Symlinks are not allowed.")
        if target.exists():
            st = target.lstat()
            if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1:
                raise ValueError("Only regular, non-linked files are allowed.")
        return target

    def inventory(self) -> list[dict]:
        items = []
        total = 0
        for directory, dirs, files in os.walk(self.root, followlinks=False):
            for name in dirs:
                if (Path(directory) / name).is_symlink():
                    raise ValueError("Workspace contains a symlink.")
            for name in files:
                relative = str((Path(directory) / name).relative_to(self.root))
                path = self.path(relative)
                size = path.stat().st_size
                total += size
                items.append({"path": relative, "bytes": size})
                if len(items) > MAX_FILES or total > MAX_WORKSPACE_BYTES:
                    raise ValueError("Workspace quota exceeded.")
        return sorted(items, key=lambda x: x["path"])

    def read(self, path: str, offset: int = 0, limit: int = 16000) -> dict:
        return {"path": path, **page_file(self.path(path), offset, limit)}

    def write(self, path: str, content: str, expected_sha256: str | None = None) -> dict:
        raw = content.encode("utf-8")
        if len(raw) > MAX_FILE_BYTES:
            raise ValueError("File exceeds the 2 MiB limit.")
        target = self.path(path, write=True, create_parents=True)
        old_size = target.stat().st_size if target.exists() else 0
        if expected_sha256 is not None:
            old = target.read_bytes() if target.exists() else b""
            if hashlib.sha256(old).hexdigest() != expected_sha256:
                raise ValueError("File changed; read it again before editing.")
        current = self.inventory()
        if sum(x["bytes"] for x in current) - old_size + len(raw) > MAX_WORKSPACE_BYTES:
            raise ValueError("Workspace exceeds the 32 MiB quota.")
        if not target.exists() and len(current) >= MAX_FILES:
            raise ValueError("Workspace file limit reached.")
        temp = target.with_name(".write_" + uuid.uuid4().hex)
        try:
            fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644)
            with os.fdopen(fd, "wb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp, target)
        finally:
            temp.unlink(missing_ok=True)
        return {"path": path, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}

    def patch(self, path: str, old_text: str, new_text: str, expected_sha256: str) -> dict:
        target = self.path(path, write=True)
        raw = target.read_bytes()
        if len(raw) > MAX_FILE_BYTES or hashlib.sha256(raw).hexdigest() != expected_sha256:
            raise ValueError("File size or SHA-256 precondition failed.")
        text = raw.decode("utf-8")
        if not old_text or text.count(old_text) != 1:
            raise ValueError("Patch old_text must match exactly once.")
        return self.write(path, text.replace(old_text, new_text, 1), expected_sha256)

    def search(self, query: str, offset: int, limit: int) -> dict:
        matches = []
        total = 0
        for item in self.inventory():
            if item["bytes"] > MAX_FILE_BYTES:
                continue
            path = self.path(item["path"])
            with path.open(encoding="utf-8", errors="replace") as stream:
                for number, line in enumerate(stream, 1):
                    if query.casefold() in line.casefold():
                        if offset <= total < offset + limit:
                            matches.append({"path": item["path"], "line": number, "text": line[:1000].rstrip()})
                        total += 1
        return {"matches": matches, "total": total, "has_more": total > offset + limit,
                "next_offset": offset + limit if total > offset + limit else None}
