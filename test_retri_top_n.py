import json
import os
import argparse
import logging

# python test_retri_top_n.py --n 2 \
#     --anomaly_dir ./Anomaly_Def/anomaly_def_formatted \
#     --retrieve_dir ./User_Spe_Phe/PG_Retrieval \
#     --output_dir ./Anomaly_Def/anomaly_def_rag


def _resolve_path(path, base_dir):
    """Resolve a possibly-relative path against the script's base directory."""
    if os.path.isabs(path):
        return path
    return os.path.normpath(os.path.join(base_dir, path))


def _numeric_stem_key(file_name):
    """Sort key: integer parsed from the filename stem (e.g. '12.json' -> 12)."""
    stem = os.path.splitext(file_name)[0]
    return (int(stem) if stem.lstrip("-").isdigit() else -1, file_name)


if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))

    parser = argparse.ArgumentParser(description="Expand anomaly definitions with top-n retrieved RAG content")
    parser.add_argument("--n", type=int, default=4,
                        help="Number of top retrieved items (error codes + timeout reasons) to keep per anomaly.")
    parser.add_argument("--anomaly_dir", type=str, default="./Anomaly_Def/anomaly_def_formatted",
                        help="Directory of formatted anomaly definition files (one [{...}] per file).")
    parser.add_argument("--retrieve_dir", type=str, default="./User_Spe_Phe/MySQL_Retrieval",
                        help="Directory of retrieval result files produced by test_retri.py.") # MySQL, PG
    parser.add_argument("--output_dir", type=str, default="./Anomaly_Def/anomaly_def_mysql_rag", # mysql, pg
                        help="Output directory for the RAG-expanded anomaly files.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger = logging.getLogger("DB_DIAG.retri_top_n")

    n = args.n
    anomaly_dir = _resolve_path(args.anomaly_dir, base_dir)
    retrieve_dir = _resolve_path(args.retrieve_dir, base_dir)
    output_dir = _resolve_path(args.output_dir, base_dir)

    if not os.path.isdir(anomaly_dir):
        logger.error(f"[MAIN][ERROR] Anomaly directory does not exist: {anomaly_dir}")
        exit()
    if not os.path.isdir(retrieve_dir):
        logger.error(f"[MAIN][ERROR] Retrieve directory does not exist: {retrieve_dir}")
        exit()
    os.makedirs(output_dir, exist_ok=True)

    logger.info(f"[MAIN] n             = {n}")
    logger.info(f"[MAIN] Anomaly dir   = {anomaly_dir}")
    logger.info(f"[MAIN] Retrieve dir  = {retrieve_dir}")
    logger.info(f"[MAIN] Output dir    = {output_dir}")

    # Traverse anomaly files sorted ascending by the numeric value in the filename.
    anomaly_files = sorted(
        [name for name in os.listdir(anomaly_dir) if name.endswith(".json")],
        key=_numeric_stem_key,
    )

    id = 0  # Global, continuously incrementing output file index.
    for file_name in anomaly_files:
        anomaly_path = os.path.join(anomaly_dir, file_name)
        # Join retrieve_dir with the same filename to locate the retrieval result.
        retrieval_path = os.path.join(retrieve_dir, file_name)

        if not os.path.exists(retrieval_path):
            logger.warning(f"[MAIN][WARN] Retrieval file not found, skipping: {retrieval_path}")
            continue

        try:
            with open(anomaly_path, "r", encoding="utf-8") as f:
                anomaly_content = json.load(f)
            with open(retrieval_path, "r", encoding="utf-8") as f:
                retrieval_results = json.load(f)
        except (ValueError, json.JSONDecodeError) as e:
            logger.error(f"[MAIN][ERROR] Failed to parse JSON for `{file_name}`: {e} --> skipping.")
            continue

        # Each formatted file wraps a single anomaly definition as [{...}];
        # unwrap it so 'anomaly_def' holds the definition dict directly.
        anomaly_def = anomaly_content[0] if isinstance(anomaly_content, list) else anomaly_content

        # Concatenate matched error codes and matched timeout reasons into one list,
        # then keep only the top-n items.
        matched_error_codes = retrieval_results.get("matched_error_codes", [])
        matched_timeout_reasons = retrieval_results.get("matched_timeout_reasons", [])
        rag_content_list = (matched_error_codes + matched_timeout_reasons)[:n]

        # For each retrieved item, build a RAG anomaly record and write it out.
        for rag_content in rag_content_list:
            rag_anomaly = {
                "anomaly_def": anomaly_def,
                "rag_content": rag_content,
            }
            with open(os.path.join(output_dir, f"{id}.json"), "w", encoding="utf-8") as f:
                json.dump([rag_anomaly], f, ensure_ascii=False, indent=4)
            id += 1

    logger.info(f"[MAIN][RETRIEVAL TOP-N] == COMPLETED == Total RAG files generated: {id}")
