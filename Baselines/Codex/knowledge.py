"""Pinned documentation retrieval and optional public documentation web search.

Unrestricted web browsing is deliberately excluded: it can retrieve benchmark
answers. Network calls are controller-owned HTTPS GETs, never agent-selected
hosts, headers, ports, proxies or credentials. DNS is pinned to public addresses.
"""
from __future__ import annotations

import hashlib
from html.parser import HTMLParser
import http.client
import ipaddress
import json
from pathlib import Path
import re
import socket
import ssl
import urllib.parse

MAX_WEB_BYTES = 1024 * 1024
OFFICIAL = {
    "www.postgresql.org": ("/docs/16/",),
    "dev.mysql.com": ("/doc/refman/8.0/en/",),
    "docs.oracle.com": ("/cd/E17952_01/mysql-8.0-en/",),
}


def official_url(url: str) -> bool:
    p = urllib.parse.urlsplit(url)
    return (p.scheme == "https" and p.hostname in OFFICIAL and not p.username
            and not p.password and p.port in (None, 443)
            and any(p.path.startswith(prefix) for prefix in OFFICIAL[p.hostname])
            and not p.query and not p.fragment
            and ".." not in urllib.parse.unquote(p.path) and "%" not in p.path)


class PublicHTTPS(http.client.HTTPSConnection):
    def connect(self):
        addresses = socket.getaddrinfo(self.host, 443, type=socket.SOCK_STREAM)
        public = []
        for family, kind, proto, _, sockaddr in addresses:
            if not ipaddress.ip_address(sockaddr[0]).is_global:
                raise ValueError("Private, local and reserved network destinations are blocked.")
            public.append((family, kind, proto, sockaddr))
        if not public:
            raise ValueError("No public destination found.")
        last = None
        for family, kind, proto, sockaddr in public:
            raw = socket.socket(family, kind, proto)
            try:
                raw.settimeout(self.timeout)
                raw.connect(sockaddr)
                self.sock = self._context.wrap_socket(raw, server_hostname=self.host)
                return
            except OSError as exc:
                raw.close()
                last = exc
        raise ValueError("Public HTTPS connection failed.") from last


def https_get(url: str, *, search: bool = False) -> tuple[str, str]:
    p = urllib.parse.urlsplit(url)
    search_ok = search and p.scheme == "https" and p.netloc == "www.postgresql.org" and p.path == "/search/"
    if not (search_ok or official_url(url)):
        raise ValueError("Only PostgreSQL 16 / MySQL 8.0 public documentation URLs are allowed.")
    connection = PublicHTTPS(p.hostname, timeout=15, context=ssl.create_default_context())
    try:
        connection.request("GET", p.path + (("?" + p.query) if p.query else ""),
                           headers={"User-Agent": "Codex-Pro-Docs/1.0", "Accept": "text/html,text/plain",
                                    "Accept-Encoding": "identity"})
        response = connection.getresponse()
        if response.status != 200:
            # No automatic redirects (including redirects to metadata endpoints).
            raise ValueError(f"Documentation server returned HTTP {response.status}; redirect following is disabled.")
        if not response.getheader("Content-Type", "").lower().startswith(("text/html", "text/plain")):
            raise ValueError("Only text documentation is accepted.")
        data = response.read(MAX_WEB_BYTES + 1)
        if len(data) > MAX_WEB_BYTES:
            raise ValueError("Documentation response exceeds the 1 MiB limit.")
        return data.decode("utf-8", errors="replace"), hashlib.sha256(data).hexdigest()
    finally:
        connection.close()


class TextParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.text = []
        self.links = []
        self.skip = 0

    def handle_starttag(self, tag, attrs):
        if tag in {"script", "style"}:
            self.skip += 1
        if tag == "a":
            href = dict(attrs).get("href")
            if href:
                self.links.append(href)
        if tag in {"p", "div", "li", "h1", "h2", "h3", "pre", "tr"}:
            self.text.append("\n")

    def handle_endtag(self, tag):
        if tag in {"script", "style"}:
            self.skip = max(0, self.skip - 1)

    def handle_data(self, data):
        if not self.skip:
            self.text.append(data)

    def plain(self):
        return "\n".join(" ".join(line.split()) for line in "".join(self.text).splitlines() if line.strip())


class Knowledge:
    def __init__(self, root: Path):
        self.root = root
        self.manifest = json.loads((root / "manifest.json").read_text())
        self.documents = {}
        for entry in self.manifest["documents"]:
            name = entry["file"]
            if Path(name).name != name or not official_url(entry["url"]):
                raise ValueError("Invalid pinned documentation manifest.")
            path = root / name
            if path.is_symlink():
                raise ValueError("Pinned documentation must not be a symlink.")
            raw = path.read_bytes()
            if hashlib.sha256(raw).hexdigest() != entry["sha256"]:
                raise ValueError("Pinned documentation SHA-256 mismatch.")
            self.documents[entry["id"]] = (entry, raw.decode("utf-8"))

    def search(self, query: str, limit: int = 5, dbms: str = "all") -> dict:
        terms = set(re.findall(r"\w+", query.casefold()))
        results = []
        for key, (meta, text) in self.documents.items():
            if dbms != "all" and meta["dbms"] != dbms:
                continue
            paragraphs = text.splitlines()
            scored = [(sum(term in paragraph.casefold() for term in terms), paragraph) for paragraph in paragraphs]
            scored.sort(key=lambda x: x[0], reverse=True)
            hits = [paragraph[:1200] for score, paragraph in scored[:3] if score]
            score = sum(s for s, _ in scored[:3])
            if score:
                results.append({"id": key, "title": meta["title"], "version": meta["version"],
                                "url": meta["url"], "sha256": meta["sha256"],
                                "snapshot": self.manifest["snapshot"], "score": score, "excerpts": hits})
        results.sort(key=lambda x: (-x["score"], x["id"]))
        return {"results": results[:limit], "source": "pinned_docs", "untrusted_reference_content": True}

    def read(self, doc_id: str, offset: int, limit: int) -> dict:
        if doc_id not in self.documents:
            raise ValueError("Unknown documentation ID; use docs_search first.")
        meta, text = self.documents[doc_id]
        end = min(len(text), offset + limit)
        return {"id": doc_id, "text": text[offset:end], "offset": offset, "total_chars": len(text),
                "next_offset": end if end < len(text) else None, "has_more": end < len(text),
                "url": meta["url"], "sha256": meta["sha256"], "version": meta["version"]}

    def web_search(self, query: str, limit: int = 5) -> dict:
        # Search only the public PostgreSQL manual index. MySQL is available
        # through pinned docs and web_fetch; no benchmark/general search engine.
        url = "https://www.postgresql.org/search/?" + urllib.parse.urlencode({"q": query, "a": "1"})
        html, digest = https_get(url, search=True)
        parsed = TextParser()
        parsed.feed(html)
        urls = []
        for link in parsed.links:
            full = urllib.parse.urljoin(url, link).split("#")[0]
            if official_url(full) and full not in urls:
                urls.append(full)
        return {"results": [{"url": u} for u in urls[:limit]], "scope": "official_versioned_docs_only",
                "response_sha256": digest, "untrusted_reference_content": True,
                "note": "Unrestricted web/benchmark search is disabled to preserve evaluation integrity."}

    def web_fetch(self, url: str, offset: int, limit: int) -> dict:
        html, digest = https_get(url)
        parsed = TextParser()
        parsed.feed(html)
        text = parsed.plain()
        end = min(len(text), offset + limit)
        return {"url": url, "text": text[offset:end], "offset": offset, "total_chars": len(text),
                "next_offset": end if end < len(text) else None, "has_more": end < len(text),
                "response_sha256": digest, "untrusted_reference_content": True}
