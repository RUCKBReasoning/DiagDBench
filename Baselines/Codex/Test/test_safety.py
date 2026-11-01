"""Local policy regressions; run using the project's db_diag Python."""
import hashlib
import json
from pathlib import Path
import socket
import tempfile
import threading
import unittest
from unittest.mock import patch

from Baselines.Codex.knowledge import Knowledge, PublicHTTPS, official_url
from Baselines.Codex.tools import Tools
from Baselines.Codex.workspace import Workspace, page_file


class WorkspaceSafety(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.workspace = Workspace(self.root / "workspace")
        self.secret = self.root / "original_sample.txt"
        self.secret.write_text("SOURCE_CANARY")

    def tearDown(self):
        self.temp.cleanup()

    def test_parent_absolute_and_control_paths_are_blocked(self):
        for name in ("../original_sample.txt", str(self.secret), "work/../../original_sample.txt",
                     "agent_control/context.json", "inputs/../work/x", "work/.hidden", "/proc/self/environ"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.workspace.read(name)

    def test_symlink_and_directory_symlink_are_blocked(self):
        (self.workspace.root / "work/link").symlink_to(self.secret)
        (self.workspace.root / "work/dir").symlink_to(self.root, target_is_directory=True)
        for name in ("work/link", "work/dir/original_sample.txt"):
            with self.assertRaises(ValueError):
                self.workspace.read(name)
            with self.assertRaises(ValueError):
                self.workspace.write(name, "tamper")
        self.assertEqual(self.secret.read_text(), "SOURCE_CANARY")

    def test_hardlink_is_blocked(self):
        (self.workspace.root / "work/link").hardlink_to(self.secret)
        with self.assertRaises(ValueError):
            self.workspace.read("work/link")
        with self.assertRaises(ValueError):
            self.workspace.write("work/link", "tamper")

    def test_inputs_are_immutable(self):
        with self.assertRaises(ValueError):
            self.workspace.write("inputs/source.txt", "tamper")

    def test_patch_requires_current_hash_and_unique_match(self):
        first = self.workspace.write("work/query.sql", "select 1;\n")
        with self.assertRaises(ValueError):
            self.workspace.patch("work/query.sql", "select", "SELECT", "0" * 64)
        self.workspace.patch("work/query.sql", "select", "SELECT", first["sha256"])
        self.assertEqual(self.workspace.read("work/query.sql")["text"], "SELECT 1;\n")

    def test_full_log_pagination_preserves_utf8(self):
        original = ("中文证据 abc\n" * 8000).encode()
        log = self.root / "observation.log"
        log.write_bytes(original)
        offset = 0
        pages = []
        while True:
            page = page_file(log, offset, 4097)
            pages.append(page["text"])
            if not page["has_more"]:
                break
            offset = page["next_offset"]
        self.assertEqual("".join(pages).encode(), original)

    def test_search_paginates_without_reading_other_directories(self):
        for i in range(3):
            self.workspace.write(f"work/{i}.txt", "evidence\n")
        result = self.workspace.search("evidence", 1, 1)
        self.assertEqual(result["total"], 3)
        self.assertEqual(len(result["matches"]), 1)
        self.assertTrue(result["has_more"])
        self.assertEqual(self.workspace.search("SOURCE_CANARY", 0, 20)["total"], 0)


class KnowledgeSafety(unittest.TestCase):
    def test_only_exact_versioned_public_docs(self):
        good = "https://www.postgresql.org/docs/16/using-explain.html"
        self.assertTrue(official_url(good))
        for url in ("file:///etc/passwd", "http://127.0.0.1/a", "https://www.postgresql.org.evil.example/docs/16/a",
                    "https://www.postgresql.org/docs/16/../../a", good + "?url=http://127.0.0.1",
                    "https://user@www.postgresql.org/docs/16/a", "https://www.postgresql.org/docs/17/a",
                    "https://www.postgresql.org/docs/16/%2e%2e/a"):
            self.assertFalse(official_url(url), url)

    def test_dns_rebinding_to_private_ip_fails_before_connect(self):
        for addr in ("127.0.0.1", "10.0.0.1", "169.254.169.254", "192.168.1.1"):
            with patch("socket.getaddrinfo", return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", (addr, 443))]):
                with self.assertRaises(ValueError):
                    PublicHTTPS("www.postgresql.org", timeout=1).connect()

    def test_pinned_document_hash_tampering_is_detected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "doc.txt").write_text("changed")
            (root / "manifest.json").write_text(json.dumps({"documents": [{
                "file": "doc.txt", "id": "pg", "url": "https://www.postgresql.org/docs/16/a",
                "sha256": hashlib.sha256(b"original").hexdigest(),
            }]}))
            with self.assertRaises(ValueError):
                Knowledge(root)


class AgentPolicy(unittest.TestCase):
    def test_aux_budget_is_shared_with_specialists(self):
        tools = object.__new__(Tools)
        tools.enabled = {"plan_read"}
        tools.lock = threading.RLock()
        tools.finished = tools.broken = False
        tools.context = {"max_aux_actions": 1}
        tools.aux_actions = 0
        tools.plan = []
        tools.audit = lambda event: None
        self.assertEqual(tools.invoke("plan_read", {}, actor="specialist_1"), {"steps": []})
        with self.assertRaisesRegex(ValueError, "budget exhausted"):
            tools.invoke("plan_read", {})

    def test_specialist_cannot_mutate_execute_or_delegate(self):
        tools = object.__new__(Tools)
        tools.enabled = {"db_execute", "analysis_execute", "workspace_write", "agent_spawn", "db_finish_diagnosis", "web_fetch"}
        for name in tools.enabled:
            with self.assertRaises(ValueError):
                tools.invoke(name, {}, actor="specialist_1")

    def test_unknown_tools_are_rejected_before_execution(self):
        tools = object.__new__(Tools)
        tools.enabled = set()
        with self.assertRaises(ValueError):
            tools.invoke("exec_command", {"cmd": "cat /raw/sample"})


if __name__ == "__main__":
    unittest.main()
