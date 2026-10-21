import json
import os
import re
import math
import argparse
import logging

try:
    from tqdm import tqdm
except Exception:  # tqdm is optional; fall back to a no-op iterator wrapper.
    def tqdm(iterable, **kwargs):
        return iterable

# python test_retri.py --dbms_name "PostgreSQL 16.11" \
#     --anomaly_dir ./Anomaly_Def/anomaly_def_formatted \
#     --output_dir ./User_Spe_Phe/PG_Retrieval

# ============================================================
# Retrieval hyper-parameters and count-list state
# (kept identical to test_syn_int.py)
# ============================================================
ERROR_RETRIEVAL_COUNT_LISTS = {}
TIMEOUT_RETRIEVAL_COUNT_LISTS = {}
ERROR_TOP_PERCENTAGE = 0.25
TIMEOUT_TOP_PERCENTAGE = 0.40

ERROR_FIELD_WEIGHTS = {
    "error_name": 3.0,
    "official_description": 3.0,
    "observable_symptoms": 2.5,
    "category": 2.0,
    "system_domain": 2.0,
    "cause_analysis": 1.5,
    "error_class": 1.0,
    "error_code": 1.0,
    "solution_guidance": 1.0,
}
TIMEOUT_FIELD_WEIGHTS = {
    "timeout_category": 3.0,
    "official_definition": 3.0,
    "parameter_name": 2.5,
    "trigger_condition": 2.5,
    "observable_symptoms": 2.5,
    "system_domain": 2.0,
    "workload_impact": 1.5,
    "cause_analysis": 1.5,
    "scope": 1.0,
    "solution_guidance": 1.0,
}

# Hybrid score = HYBRID_BM25_WEIGHT * normalized_BM25 + HYBRID_LEXICAL_WEIGHT * lexical_overlap
HYBRID_BM25_WEIGHT = 0.7
HYBRID_LEXICAL_WEIGHT = 0.3
BM25_K1 = 1.5
BM25_B = 0.75


def _canonicalize_dbms_name(dbms_name):
    dbms_text = str(dbms_name or "").strip().lower()
    if 'postgresql' in dbms_text: return "PostgreSQL 16.11"
    else : return "MySQL 8.4"


def _flatten_retrieval_text(value):
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, dict):
        return " ".join(_flatten_retrieval_text(v) for v in value.values())
    if isinstance(value, (list, tuple, set)):
        return " ".join(_flatten_retrieval_text(v) for v in value)
    return str(value)


def _tokenize_retrieval_text(text):
    tokens = re.findall(r"[a-z0-9_]+", str(text or "").lower())
    return [token for token in tokens if len(token) > 1]


def _build_anomaly_query_text(anomaly_definition):
    anomaly_text = _flatten_retrieval_text(anomaly_definition)
    return anomaly_text.strip()


def _build_candidate_text(entry, fields):
    return " ".join(_flatten_retrieval_text(entry.get(field, "")) for field in fields)


def _build_weighted_term_freq(entry, field_weights):
    """Build a field-weighted term-frequency map for one candidate entry.

    Tokens coming from high-signal fields (e.g. error_name, timeout_category)
    are counted with a larger weight, so BM25 favours matches on the fields
    that best describe the anomaly rather than on long auxiliary text.
    """
    term_freq = {}
    for field, weight in field_weights.items():
        field_tokens = _tokenize_retrieval_text(_flatten_retrieval_text(entry.get(field, "")))
        for token in field_tokens:
            term_freq[token] = term_freq.get(token, 0.0) + weight
    return term_freq


def _build_bm25_corpus(candidates, field_weights):
    doc_term_freqs = []
    doc_lengths = []
    doc_freq = {}
    for entry in candidates:
        term_freq = _build_weighted_term_freq(entry, field_weights)
        doc_term_freqs.append(term_freq)
        doc_lengths.append(sum(term_freq.values()))
        for token in term_freq:
            doc_freq[token] = doc_freq.get(token, 0) + 1

    doc_count = len(candidates)
    avg_doc_length = (sum(doc_lengths) / doc_count) if doc_count else 0.0

    idf = {}
    for token, freq in doc_freq.items():
        # BM25 idf with the standard +1 smoothing to keep values non-negative.
        idf[token] = math.log(1.0 + (doc_count - freq + 0.5) / (freq + 0.5))

    return {
        "doc_term_freqs": doc_term_freqs,
        "doc_lengths": doc_lengths,
        "avg_doc_length": avg_doc_length,
        "idf": idf,
    }


def _compute_bm25_score(query_tokens, doc_index, corpus):
    """Compute the BM25 score of one document for the given query tokens."""
    term_freq = corpus["doc_term_freqs"][doc_index]
    doc_length = corpus["doc_lengths"][doc_index]
    avg_doc_length = corpus["avg_doc_length"] or 1.0
    idf = corpus["idf"]

    score = 0.0
    matched_terms = []
    for token in set(query_tokens):
        freq = term_freq.get(token, 0.0)
        if freq <= 0.0:
            continue
        matched_terms.append(token)
        numerator = freq * (BM25_K1 + 1.0)
        denominator = freq + BM25_K1 * (1.0 - BM25_B + BM25_B * (doc_length / avg_doc_length))
        score += idf.get(token, 0.0) * (numerator / denominator)
    return score, sorted(matched_terms)


def _compute_overlap_score(query_tokens, candidate_tokens):
    query_set = set(query_tokens)
    candidate_set = set(candidate_tokens)
    if not query_set or not candidate_set:
        return 0.0, []

    matched_terms = sorted(query_set & candidate_set)
    if not matched_terms:
        return 0.0, []

    recall = len(matched_terms) / len(query_set)
    precision = len(matched_terms) / len(candidate_set)
    score = (0.7 * recall) + (0.3 * precision)
    return score, matched_terms


def _rank_candidates_hybrid(query_tokens, candidates, field_weights, count_list, identity_field):
    if not candidates:
        return []

    corpus = _build_bm25_corpus(candidates, field_weights)
    candidate_fields = list(field_weights.keys())

    raw_results = []
    max_bm25 = 0.0
    for candidate_index, entry in enumerate(candidates):
        bm25_score, bm25_terms = _compute_bm25_score(query_tokens, candidate_index, corpus)
        candidate_tokens = _tokenize_retrieval_text(_build_candidate_text(entry, candidate_fields))
        lexical_score, lexical_terms = _compute_overlap_score(query_tokens, candidate_tokens)

        if bm25_score <= 0.0 and lexical_score <= 0.0:
            continue

        max_bm25 = max(max_bm25, bm25_score)
        matched_terms = sorted(set(bm25_terms) | set(lexical_terms))
        raw_results.append(
            {
                "candidate_index": candidate_index,
                "bm25_score": bm25_score,
                "lexical_score": lexical_score,
                "matched_terms": matched_terms,
                "match_count": count_list[candidate_index],
                "entry": entry,
            }
        )

    ranked = []
    for item in raw_results:
        normalized_bm25 = (item["bm25_score"] / max_bm25) if max_bm25 > 0.0 else 0.0
        hybrid_score = (
            HYBRID_BM25_WEIGHT * normalized_bm25
            + HYBRID_LEXICAL_WEIGHT * item["lexical_score"]
        )
        ranked.append(
            {
                "candidate_index": item["candidate_index"],
                "score": hybrid_score,
                "bm25_score": item["bm25_score"],
                "lexical_score": item["lexical_score"],
                "matched_terms": item["matched_terms"],
                "match_count": item["match_count"],
                "entry": item["entry"],
            }
        )

    ranked.sort(
        key=lambda item: (
            -item["score"],
            item["entry"].get(identity_field, ""),
        )
    )
    return ranked


def _build_counter_cache_key(dbms_name, json_path):
    return f"{dbms_name}::{os.path.abspath(json_path)}"


def _get_entry_identity(entry, identity_field):
    identity_value = entry.get(identity_field)
    if identity_value is not None and str(identity_value).strip() != "":
        return str(identity_value)
    return json.dumps(entry, sort_keys=True, ensure_ascii=False)


def _ensure_count_state(counter_store, cache_key, candidates, identity_field):
    # identity_list [the unique identifier for each candidate entry, used to detect changes in the candidate corpus]
    # count_list [the mutable list of counts corresponding to each candidate entry, updated across iterations]
    identity_list = [_get_entry_identity(entry, identity_field) for entry in candidates]
    state = counter_store.get(cache_key)
    if state is None or state.get("identity_list") != identity_list:
        state = {
            "identity_list": identity_list,
            "count_list": [0] * len(candidates),
        }
        counter_store[cache_key] = state
    return state


def _select_retained_matches(ranked_items, count_list, top_percentage, identity_field, top_k=2):
    if not ranked_items:
        return []

    retained_candidate_num = int(len(ranked_items) * top_percentage)
    if len(ranked_items) * top_percentage > retained_candidate_num:
        retained_candidate_num += 1
    retained_candidate_num = max(1, retained_candidate_num)
    retained_candidate_num = min(len(ranked_items), retained_candidate_num)

    filtered_candidates = ranked_items[:retained_candidate_num]

    filtered_candidates.sort(
        key=lambda item: (
            item["match_count"],
            -item["score"],
            item["entry"].get(identity_field, ""),
        )
    )

    selected_items = filtered_candidates[:top_k]
    for item in selected_items:
        count_list[item["candidate_index"]] += 1
        item["match_count"] = count_list[item["candidate_index"]]
    return selected_items


def match_user_description_with_anomaly_definition(
    dbms_name,
    anomaly_definition,
    logger,
    error_code_json_path=None,
    timeout_json_path=None,
):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    canonical_dbms_name = _canonicalize_dbms_name(dbms_name)
    error_code_json_path = error_code_json_path or os.path.join(base_dir, "User_Spe_Phe", "error_code_list.json")
    timeout_json_path = timeout_json_path or os.path.join(base_dir, "User_Spe_Phe", "timeout_list.json")

    if not isinstance(anomaly_definition, dict):
        logger.error("[USER DESCRIPTION RETRIEVAL][ERROR] Invalid anomaly definition: `anomaly_definition` must be a dictionary.")
        exit()

    # error_code_doc, timeout_doc
    try:
        with open(error_code_json_path, "r") as f:
            error_code_doc = json.load(f)
    except Exception as e:
        logger.error(f"[USER DESCRIPTION RETRIEVAL][ERROR] Failed to load error-code JSON file `{error_code_json_path}`: {e}")
        exit()

    try:
        with open(timeout_json_path, "r") as f:
            timeout_doc = json.load(f)
    except Exception as e:
        logger.error(f"[USER DESCRIPTION RETRIEVAL][ERROR] Failed to load timeout JSON file `{timeout_json_path}`: {e}")
        exit()

    # anomaly definition for retrieval -> token list
    query_text = _build_anomaly_query_text(anomaly_definition)
    query_tokens = _tokenize_retrieval_text(query_text) #

    error_candidates = error_code_doc.get(canonical_dbms_name, [])
    timeout_candidates = timeout_doc.get(canonical_dbms_name, [])
    if error_candidates == [] or timeout_candidates == [] :
        logger.error(f"[USER DESCRIPTION RETRIEVAL][ERROR] No candidates found for DBMS `{canonical_dbms_name}` in error-code or timeout corpus.")
        exit()

    # Maintain count state for candidates to enable dynamic adjustment of ranking based on previous matches.
    error_count_state = _ensure_count_state(
        ERROR_RETRIEVAL_COUNT_LISTS,
        _build_counter_cache_key(canonical_dbms_name, error_code_json_path),
        error_candidates,
        "error_code",
    )
    timeout_count_state = _ensure_count_state(
        TIMEOUT_RETRIEVAL_COUNT_LISTS,
        _build_counter_cache_key(canonical_dbms_name, timeout_json_path),
        timeout_candidates,
        "timeout_id",
    )
    error_count_list = error_count_state["count_list"]
    timeout_count_list = timeout_count_state["count_list"]

    # retrieve ranked candidates with hybrid BM25 + lexical matching
    ranked_errors = _rank_candidates_hybrid(
        query_tokens,
        error_candidates,
        ERROR_FIELD_WEIGHTS,
        error_count_list,
        "error_code",
    )
    ranked_timeouts = _rank_candidates_hybrid(
        query_tokens,
        timeout_candidates,
        TIMEOUT_FIELD_WEIGHTS,
        timeout_count_list,
        "timeout_id",
    )

    # Select retained matches based on top percentages [match counts with retrieval scores]
    retained_error_codes = _select_retained_matches(
        ranked_errors,
        error_count_list,
        ERROR_TOP_PERCENTAGE,
        "error_code",
        top_k=2,
    )
    retained_timeout_reasons = _select_retained_matches(
        ranked_timeouts,
        timeout_count_list,
        TIMEOUT_TOP_PERCENTAGE,
        "timeout_id",
        top_k=2,
    )

    return {
        "dbms": canonical_dbms_name,
        "anomaly_name": anomaly_definition.get("Name", ""),
        "query_text": query_text,
        "query_terms": sorted(set(query_tokens)),
        "error_score_threshold": None,
        "timeout_score_threshold": None,
        "error_top_percentage": ERROR_TOP_PERCENTAGE,
        "timeout_top_percentage": TIMEOUT_TOP_PERCENTAGE,
        "ranked_errors": ranked_errors,
        "ranked_timeouts": ranked_timeouts,
        "matched_error_codes": retained_error_codes,
        "matched_timeout_reasons": retained_timeout_reasons,
    }


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
     
    # MySQL 8.4, PostgreSQL 16.11
    parser = argparse.ArgumentParser(description="Key-information retrieval over anomaly definitions")
    parser.add_argument("--dbms_name", type=str, default="MySQL 8.4",
                        help="DBMS name; determines the candidate corpus and output subdirectory.")
    parser.add_argument("--anomaly_dir", type=str, default="./Anomaly_Def/anomaly_def_formatted",
                        help="Directory of formatted anomaly definition files (one [{...}] per file).")
    parser.add_argument("--output_dir", type=str, default=None,
                        help="Output directory. Defaults to ./User_Spe_Phe/{PG_Retrieval|MySQL_Retrieval} based on dbms_name.")
    args = parser.parse_args()

    # ----- logger -----
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger = logging.getLogger("DB_DIAG.retrieval")

    # ----- resolve I/O directories -----
    dbms_name = args.dbms_name
    canonical_dbms_name = _canonicalize_dbms_name(dbms_name)
    # Store results under PG_Retrieval or MySQL_Retrieval depending on the DBMS.
    retrieval_subdir = "PG_Retrieval" if "postgresql" in canonical_dbms_name.lower() else "MySQL_Retrieval"

    anomaly_dir = _resolve_path(args.anomaly_dir, base_dir)
    if args.output_dir is None:
        output_dir = os.path.join(base_dir, "User_Spe_Phe", retrieval_subdir)
    else:
        output_dir = _resolve_path(args.output_dir, base_dir)

    if not os.path.isdir(anomaly_dir):
        logger.error(f"[MAIN][ERROR] Anomaly directory does not exist: {anomaly_dir}")
        exit()
    os.makedirs(output_dir, exist_ok=True)

    # Count-list files persisted in the output directory (maintained across runs).
    error_count_path = os.path.join(output_dir, "error_code_count_list.json")
    timeout_count_path = os.path.join(output_dir, "timeout_count_list.json")

    # Restore previously accumulated count-list state if present.
    if os.path.exists(error_count_path):
        with open(error_count_path, "r") as f:
            ERROR_RETRIEVAL_COUNT_LISTS = json.load(f)
    if os.path.exists(timeout_count_path):
        with open(timeout_count_path, "r") as f:
            TIMEOUT_RETRIEVAL_COUNT_LISTS = json.load(f)

    logger.info(f"[MAIN] DBMS = {canonical_dbms_name}")
    logger.info(f"[MAIN] Anomaly directory = {anomaly_dir}")
    logger.info(f"[MAIN] Output directory  = {output_dir}")

    # Traverse anomaly files sorted ascending by the numeric value in the filename.
    anomaly_files = sorted(
        [name for name in os.listdir(anomaly_dir) if name.endswith(".json")],
        key=_numeric_stem_key,
    )
    logger.info(f"[MAIN] Total anomaly files to retrieve: {len(anomaly_files)}")

    for file_name in tqdm(anomaly_files, total=len(anomaly_files), desc="KEY INFORMATION RETRIEVAL for Anomalies"):
        anomaly_path = os.path.join(anomaly_dir, file_name)
        try:
            with open(anomaly_path, "r", encoding="utf-8") as f:
                anomaly_content = json.load(f)
        except (ValueError, json.JSONDecodeError) as e:
            logger.error(f"[MAIN][ERROR] Failed to parse anomaly file `{anomaly_path}`: {e} --> skipping.")
            continue

        # Each formatted file wraps a single anomaly definition as [{...}].
        anomaly_definition = anomaly_content[0] if isinstance(anomaly_content, list) else anomaly_content

        # Retrieval of user-phenomenon suggestions based on the anomaly definition.
        retrieval_results = match_user_description_with_anomaly_definition(
            dbms_name,
            anomaly_definition,
            logger,
        )

        # Output filename is the anomaly's numeric label (same stem as the input).
        stem = os.path.splitext(file_name)[0]
        with open(os.path.join(output_dir, f"{stem}.json"), "w", encoding="utf-8") as f:
            json.dump(retrieval_results, f, ensure_ascii=False, indent=4)

        # Persist the maintained count lists in the same output directory.
        with open(error_count_path, "w", encoding="utf-8") as f:
            json.dump(ERROR_RETRIEVAL_COUNT_LISTS, f, ensure_ascii=False, indent=4)
        with open(timeout_count_path, "w", encoding="utf-8") as f:
            json.dump(TIMEOUT_RETRIEVAL_COUNT_LISTS, f, ensure_ascii=False, indent=4)

    logger.info("[MAIN][KEY INFORMATION RETRIEVAL] == COMPLETED ==")
