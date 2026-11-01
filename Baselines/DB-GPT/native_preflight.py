"""Offline prerequisite check; never embed text, query a model or create an index."""
import importlib.util
import os
from pathlib import Path


def check_knowledge():
    if os.getenv("DB_GPT_KB_MODE", "native") != "native":
        raise RuntimeError(
            "DB_GPT_KB_MODE=bundled is no longer supported: initialize the upstream "
            "knowledge/vector stores and use native retrieval (see UPSTREAM.md)."
        )
    from configs import DEFAULT_VS_TYPES, kbs_config
    from server.knowledge_base.kb_service.base import get_kb_details
    try:
        details = [item for item in get_kb_details() if "expert" in item["kb_name"].lower()]
    except Exception as exc:
        raise RuntimeError(
            "Native KB metadata is unavailable. Initialize it using init_database.py "
            "and populate the vector store; see UPSTREAM.md."
        ) from exc
    if not details or any(not item.get("in_db") or not item.get("file_count") for item in details):
        raise RuntimeError(
            "Native expert knowledge bases are not populated. Initialize the metadata "
            "and vector store from knowledge_base/*/content; see UPSTREAM.md."
        )
    if "chromadb" in DEFAULT_VS_TYPES:
        directory = Path(kbs_config["chromadb"]["persist_directory"])
        if not directory.is_dir() or not any(p.is_file() for p in directory.rglob("*")):
            raise RuntimeError(f"Native Chroma vector index is missing: {directory}; see UPSTREAM.md")
    if importlib.util.find_spec("sentence_transformers") is None:
        raise RuntimeError("Native DB-GPT requires sentence-transformers; hash embeddings are not supported")
    names = [item["kb_name"] for item in details]
    print(f"Local knowledge prerequisites passed; experts={len(names)}. "
          "Embedding availability and remote services have NOT been probed.")
    return names


if __name__ == "__main__":
    check_knowledge()
