import argparse
from collections import Counter
from datetime import datetime
import json
import os
import shutil
from typing import Any, Dict, List


DEFAULT_INPUT_DIR = "Output"
DEFAULT_OUTPUT_DIR = "Output/_agg_directory"
DEFAULT_PATTERN = "pg_ano"


def load_json_file(file_path: str) -> Any:
    """Load a JSON file with UTF-8 encoding."""
    with open(file_path, "r", encoding="utf-8") as file:
        return json.load(file)


def dump_json_file(file_path: str, data: Any) -> None:
    """Write a JSON file with UTF-8 encoding and stable formatting."""
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)
        file.write("\n")


def find_pattern_directories(input_dir: str, pattern: str) -> List[str]:
    """Find directories whose names match the {time}_{pattern} format."""
    if not os.path.isdir(input_dir):
        raise NotADirectoryError(
            f"Input directory does not exist or is not a directory: {input_dir}"
        )

    suffix = f"_{pattern}"
    matched_dirs: List[str] = []
    for name in os.listdir(input_dir):
        full_path = os.path.join(input_dir, name)
        if not os.path.isdir(full_path):
            continue
        if not name.endswith(suffix):
            continue

        time_part = name[: -len(suffix)]
        if time_part.isdigit():
            matched_dirs.append(full_path)

    matched_dirs.sort(key=lambda path: os.path.basename(path))
    return matched_dirs


def list_numeric_child_names(parent_dir: str) -> List[str]:
    """Return numeric child directory names sorted by integer value."""
    numeric_names = []
    for name in os.listdir(parent_dir):
        child_path = os.path.join(parent_dir, name)
        if os.path.isdir(child_path) and name.isdigit():
            numeric_names.append(name)
    numeric_names.sort(key=int)
    return numeric_names


def build_ano_dir_list(pattern_dirs: List[str]) -> List[List[str]]:
    """Collect numeric anomaly directory names for each matched input directory."""
    ano_dir_list: List[List[str]] = []
    for pattern_dir in pattern_dirs:
        ano_dir_list.append(list_numeric_child_names(pattern_dir))
    return ano_dir_list


def flatten_ano_dir_list(ano_dir_list: List[List[str]]) -> List[str]:
    """Merge all sublists while preserving duplicate anomaly ids."""
    cur_ano_list: List[str] = []
    for ano_sublist in ano_dir_list:
        cur_ano_list.extend(ano_sublist)
    return cur_ano_list


def find_duplicate_anomaly_ids(cur_ano_list: List[str]) -> List[str]:
    """Return anomaly ids that occur more than once."""
    counter = Counter(cur_ano_list)
    duplicates = [ano_id for ano_id, count in counter.items() if count > 1]
    duplicates.sort(key=int)
    return duplicates


def build_output_directory(output_dir: str, pattern: str) -> str:
    """Build output_dir/{cur_time}_{pattern}_agg using a YYMMDDHHMMSS timestamp."""
    cur_time = datetime.now().strftime("%y%m%d%H%M%S")
    return os.path.join(output_dir, f"{cur_time}_{pattern}_agg")


def load_key_information_list(pattern_dir: str) -> List[Dict[str, Any]]:
    """Load key_information.json and normalize it as a list of dictionaries."""
    key_information_path = os.path.join(pattern_dir, "key_information.json")
    if not os.path.isfile(key_information_path):
        raise FileNotFoundError(
            f"Missing key_information.json in matched directory: {pattern_dir}"
        )

    key_information_data = load_json_file(key_information_path)
    if not isinstance(key_information_data, list):
        raise TypeError(
            f"Expected a list in `{key_information_path}`, "
            f"got {type(key_information_data).__name__}."
        )

    for item in key_information_data:
        if not isinstance(item, dict):
            raise TypeError(
                f"Expected every item in `{key_information_path}` to be a dict."
            )
    return key_information_data


def sort_key_information(
    key_information_list: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Sort merged key information by numeric anomaly_id."""
    try:
        return sorted(key_information_list, key=lambda item: int(item["anomaly_id"]))
    except KeyError as error:
        raise KeyError("Every key information item must contain `anomaly_id`.") from error
    except (TypeError, ValueError) as error:
        raise ValueError("Every `anomaly_id` must be convertible to int.") from error


def aggregate_anomaly_directories(
    input_dir: str,
    output_dir: str,
    pattern: str,
) -> str:
    """Aggregate non-overlapping anomaly directories and key information files."""
    pattern_dirs = find_pattern_directories(input_dir, pattern)
    if not pattern_dirs:
        raise RuntimeError(
            f"No matched directories found under `{input_dir}` for pattern `{pattern}`."
        )

    ano_dir_list = build_ano_dir_list(pattern_dirs)
    cur_ano_list = flatten_ano_dir_list(ano_dir_list)
    duplicates = find_duplicate_anomaly_ids(cur_ano_list)
    if duplicates:
        print(
            "Duplicate numeric anomaly directories detected; aggregation stopped. "
            f"Duplicate ids: {', '.join(duplicates)}"
        )
        raise SystemExit(1)

    os.makedirs(output_dir, exist_ok=True)
    final_output_dir = build_output_directory(output_dir, pattern)
    os.makedirs(final_output_dir)

    merged_key_information: List[Dict[str, Any]] = []
    for pattern_dir in pattern_dirs:
        for ano_dir_name in list_numeric_child_names(pattern_dir):
            src_dir = os.path.join(pattern_dir, ano_dir_name)
            dst_dir = os.path.join(final_output_dir, ano_dir_name)
            shutil.copytree(src_dir, dst_dir)

        merged_key_information.extend(load_key_information_list(pattern_dir))

    dump_json_file(
        os.path.join(final_output_dir, "key_information.json"),
        sort_key_information(merged_key_information),
    )
    return final_output_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate database anomaly output directories."
    )
    parser.add_argument(
        "--input_dir",
        type=str,
        default=DEFAULT_INPUT_DIR,
        help="Directory containing {time}_{pattern} anomaly output directories.",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default=DEFAULT_OUTPUT_DIR,
        help="Root directory used to store the aggregated anomaly output.",
    )
    parser.add_argument(
        "--pattern",
        type=str,
        default=DEFAULT_PATTERN,
        help="Directory pattern suffix, for example pg_ano or mysql_ano.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        result_directory = aggregate_anomaly_directories(
            input_dir=args.input_dir,
            output_dir=args.output_dir,
            pattern=args.pattern,
        )
    except (OSError, RuntimeError, TypeError, ValueError, KeyError) as error:
        raise SystemExit(f"[ERROR] {error}") from error

    print(f"Aggregation completed: {result_directory}")
