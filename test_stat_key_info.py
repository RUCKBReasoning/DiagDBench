from pathlib import Path
import argparse
from datetime import datetime, timedelta, timezone
import json
import os
import shutil
from typing import Any, Dict, List, Optional, Tuple

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, **_kwargs):
        """Fallback iterator when tqdm is unavailable in the active Python env."""
        return iterable

PROGRESS_MIN_ITEMS = 50


def progress_iter(iterable, desc: str, unit: str, min_items: int = PROGRESS_MIN_ITEMS):
    """Show tqdm only for large data-sample loops.

    Small loops and step-level control flow stay quiet to keep terminal output
    readable. tqdm itself provides current/total counts, percentage, rate, and
    ETA when enabled.
    """
    total = len(iterable)
    if total < min_items:
        return iterable
    return tqdm(
        iterable,
        desc=desc,
        total=total,
        unit=unit,
        dynamic_ncols=True,
    )



def extract_prefix_id(dir_name: str) -> Optional[int]:
    """Extract the leading numeric prefix id from a directory name.

    The prefix id is the substring before the first underscore. Returns the
    integer value when it is purely numeric, otherwise returns None.
    """
    prefix = dir_name.split("_", 1)[0]
    return int(prefix) if prefix.isdigit() else None


def extract_suffix_time(dir_name: str) -> int:
    """Extract the numeric timestamp suffix after the final underscore."""
    suffix = dir_name.rsplit("_", 1)[-1]
    return int(suffix) if suffix.isdigit() else -1


def build_dynamic_output_directory(
    output_root_directory: str,
    suffix: str,
    matched_dirs: Optional[List[str]] = None,
) -> str:
    """Build the final output directory using current time and DB type."""
    output_time = datetime.now().strftime("%y%m%d%H%M%S")
    suffix_path_text = " ".join([suffix] + (matched_dirs or []))
    db_type = "mysql" if "mysql" in suffix_path_text.lower() else "pg"
    return os.path.join(output_root_directory, f"{output_time}_{db_type}_ano")


def str_to_bool(value: str) -> bool:
    """Parse CLI boolean values without using interactive input."""
    if isinstance(value, bool):
        return value
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y"}:
        return True
    if normalized in {"0", "false", "no", "n"}:
        return False
    raise argparse.ArgumentTypeError(f"Invalid boolean value: {value}")


def parse_mode(value: str) -> str:
    """Parse the execution mode.

    Supported values:
    - stat: only count test_key_information directories in matched synthesis runs.
    - output: count directories and run the full anomaly sample aggregation flow.
    """
    normalized = str(value).strip().lower()
    if normalized not in {"stat", "output"}:
        raise argparse.ArgumentTypeError(
            f"Invalid mode `{value}`. Supported values are: stat, output."
        )
    return normalized


def ensure_empty_directory(directory: str, output_empty: bool) -> None:
    """Create the output directory and optionally clear all existing children."""
    abs_directory = os.path.abspath(directory)
    if abs_directory in {"/", os.path.expanduser("~")}:
        raise ValueError(f"Refusing to clear unsafe output directory: {abs_directory}")

    os.makedirs(abs_directory, exist_ok=True)
    if not output_empty:
        return

    output_entries = os.listdir(abs_directory)
    for name in output_entries:
        path = os.path.join(abs_directory, name)
        if os.path.isdir(path) and not os.path.islink(path):
            shutil.rmtree(path)
        else:
            os.remove(path)


def load_json_file(file_path: str) -> Any:
    with open(file_path, "r", encoding="utf-8") as file:
        return json.load(file)


def dump_json_file(file_path: str, data: Any) -> None:
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)
        file.write("\n")


def normalize_anomaly_description(data: Any, source_path: str) -> Dict[str, Any]:
    """Return one anomaly-description dict from either dict or single-item list input."""
    if isinstance(data, dict):
        return data
    if isinstance(data, list) and len(data) == 1 and isinstance(data[0], dict):
        return data[0]
    raise TypeError(
        f"Unsupported anomaly description format in `{source_path}`. "
        "Expected a dict or a single-item list containing a dict."
    )


def collect_anomaly_definitions(anomaly_def_directory: str, output_directory: str) -> None:
    """Collect anomaly definition JSON files in numeric filename order."""
    if not os.path.isdir(anomaly_def_directory):
        raise NotADirectoryError(
            f"Anomaly definition directory does not exist: {anomaly_def_directory}"
        )

    anomaly_files = []
    anomaly_entries = os.listdir(anomaly_def_directory)
    for name in anomaly_entries:
        stem, ext = os.path.splitext(name)
        if ext.lower() != ".json" or not stem.isdigit():
            continue
        anomaly_files.append((int(stem), os.path.join(anomaly_def_directory, name)))
    anomaly_files.sort(key=lambda item: item[0])

    anomaly_descriptions = []
    for _, path in progress_iter(
        anomaly_files,
        desc="Loading anomaly definitions",
        unit="file",
    ):
        anomaly_descriptions.append(
            normalize_anomaly_description(load_json_file(path), path)
        )
    dump_json_file(
        os.path.join(output_directory, "anomaly_description_list.json"),
        anomaly_descriptions,
    )


def find_run_directories(
    input_base_directory: str,
    suffix: str,
    start_id: int,
    end_id: int,
) -> List[str]:
    """Find per-run output directories by numeric prefix and suffix."""
    matched_dirs: List[str] = []
    input_entries = os.listdir(input_base_directory)
    for name in input_entries:
        full_path = os.path.join(input_base_directory, name)
        if not os.path.isdir(full_path):
            continue
        if not name.endswith(suffix):
            continue
        prefix_id = extract_prefix_id(name)
        if prefix_id is None:
            continue
        if start_id <= prefix_id <= end_id:
            matched_dirs.append(full_path)

    matched_dirs.sort(key=lambda path: extract_prefix_id(os.path.basename(path)))
    return matched_dirs


def find_step_directories(run_dir: str, prefix: str, sub_suffix: int) -> List[str]:
    """Collect step directories whose timestamp suffix is not older than sub_suffix."""
    step_dirs: List[str] = []
    try:
        sub_entries = os.listdir(run_dir)
    except OSError as error:
        print(f"[WARN] Failed to read directory `{run_dir}`, skipping: {error}")
        return step_dirs

    for sub_name in sub_entries:
        full_path = os.path.join(run_dir, sub_name)
        suffix_time = extract_suffix_time(sub_name)
        if (
            os.path.isdir(full_path)
            and sub_name.startswith(prefix)
            and suffix_time >= sub_suffix
        ):
            step_dirs.append(full_path)

    step_dirs.sort(key=lambda path: extract_suffix_time(os.path.basename(path)))
    return step_dirs


def align_run_step_directories(run_dir: str, sub_suffix: int) -> List[Tuple[str, str]]:
    """Validate and pair key-information dirs with syn_int dirs inside one run.

    A run can have one extra trailing test_syn_int directory when the pipeline
    stopped before producing the corresponding key-information result. In that
    case, drop only that trailing syn_int directory for the current run.
    """
    key_information_dirs = find_step_directories(
        run_dir,
        "test_key_information",
        sub_suffix,
    )
    syn_int_dirs = find_step_directories(run_dir, "test_syn_int", sub_suffix)

    if len(key_information_dirs) == len(syn_int_dirs) - 1:
        syn_int_dirs = syn_int_dirs[:-1]
    elif len(key_information_dirs) != len(syn_int_dirs):
        raise RuntimeError(
            f"Mismatched step directory counts under `{run_dir}`: "
            f"test_key_information={len(key_information_dirs)}, "
            f"test_syn_int={len(syn_int_dirs)}"
        )

    return list(zip(key_information_dirs, syn_int_dirs))


def count_key_information_directories(
    matched_dirs: List[str],
    sub_suffix: int,
) -> Tuple[int, List[Dict[str, Any]]]:
    """Count test_key_information directories under every matched run directory."""
    total_count = 0
    run_statistics: List[Dict[str, Any]] = []
    for run_dir in matched_dirs:
        key_information_dirs = find_step_directories(
            run_dir,
            "test_key_information",
            sub_suffix,
        )
        key_information_count = len(key_information_dirs)
        total_count += key_information_count
        run_statistics.append(
            {
                "run_directory": run_dir,
                "key_information_count": key_information_count,
            }
        )
    return total_count, run_statistics


def print_key_information_statistics(total_count: int, run_statistics: List[Dict[str, Any]]) -> None:
    """Print the unified key-information directory count and per-run details."""
    # Print the current Beijing time (UTC+8) before the statistics. A fixed
    # UTC+8 offset is used so the result is independent of the server timezone.
    beijing_time = datetime.now(timezone(timedelta(hours=8)))
    print(f"Beijing time (UTC+8): {beijing_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Matched run directories: {len(run_statistics)}")
    print(f"Total key_information directories: {total_count}")
    for item in run_statistics:
        print(f"{item['run_directory']}: {item['key_information_count']}")


def find_single_child_directory(parent_dir: str) -> str:
    """Return the single synthesized anomaly child directory in a key-info dir."""
    child_dirs = []
    parent_entries = os.listdir(parent_dir)
    for name in parent_entries:
        child_path = os.path.join(parent_dir, name)
        if os.path.isdir(child_path):
            child_dirs.append(child_path)
    if len(child_dirs) != 1:
        raise RuntimeError(
            f"Expected exactly one child directory under `{parent_dir}`, got {len(child_dirs)}."
        )
    return child_dirs[0]


def remove_result_fields_from_workload_performance(root_dir: str) -> Tuple[int, int, int]:
    """Remove result fields from every merged_workload_performance.json under root_dir."""
    processed_files = 0
    processed_entries = 0
    removed_fields = 0

    for current_dir, _, file_names in os.walk(root_dir):
        if "merged_workload_performance.json" not in file_names:
            continue

        performance_path = os.path.join(
            current_dir,
            "merged_workload_performance.json",
        )
        try:
            performance_data = load_json_file(performance_path)
        except (OSError, json.JSONDecodeError) as error:
            raise RuntimeError(
                f"Failed to read workload performance JSON `{performance_path}`: {error}"
            ) from error

        if not isinstance(performance_data, list):
            raise TypeError(
                f"Expected a list in workload performance JSON: {performance_path}"
            )

        for index, item in enumerate(performance_data):
            if not isinstance(item, dict):
                raise TypeError(
                    f"Expected dict item at index {index} in `{performance_path}`."
                )
            processed_entries += 1
            if "result" in item:
                del item["result"]
                removed_fields += 1

        dump_json_file(performance_path, performance_data)
        processed_files += 1

    if processed_files:
        print(
            "Removed result fields from workload performance files: "
            f"files={processed_files}, entries={processed_entries}, "
            f"removed_fields={removed_fields}, root={root_dir}"
        )
    else:
        print(f"[WARN] No merged_workload_performance.json found under `{root_dir}`.")

    return processed_files, processed_entries, removed_fields


def copy_and_rename_key_information_dirs(
    paired_step_dirs: List[Tuple[str, str]],
    output_directory: str,
) -> None:
    """Copy key-information dirs, refresh anomaly_description.json, and rename by anomaly id."""
    for key_info_dir, syn_int_dir in progress_iter(
        paired_step_dirs,
        desc="Copying key-information dirs",
        unit="dir",
    ):
        syn_int_config_path = os.path.join(syn_int_dir, "config.json")
        if not os.path.isfile(syn_int_config_path):
            raise FileNotFoundError(f"Missing syn_int config: {syn_int_config_path}")

        syn_int_config = load_json_file(syn_int_config_path)
        anomaly_description_path = syn_int_config.get("anomaly_description_path")
        if not anomaly_description_path or not os.path.isfile(anomaly_description_path):
            raise FileNotFoundError(
                f"Invalid anomaly_description_path in `{syn_int_config_path}`: "
                f"{anomaly_description_path}"
            )

        copied_dir = os.path.join(output_directory, os.path.basename(key_info_dir))
        if os.path.exists(copied_dir):
            raise FileExistsError(f"Output directory already exists: {copied_dir}")
        shutil.copytree(key_info_dir, copied_dir)
        remove_result_fields_from_workload_performance(copied_dir)

        child_dir = find_single_child_directory(copied_dir)
        dump_json_file(
            os.path.join(child_dir, "anomaly_description.json"),
            normalize_anomaly_description(
                load_json_file(anomaly_description_path),
                anomaly_description_path,
            ),
        )

        anomaly_id = os.path.splitext(os.path.basename(anomaly_description_path))[0]
        if not anomaly_id.isdigit():
            raise ValueError(
                f"Anomaly description filename must be numeric: {anomaly_description_path}"
            )
        renamed_dir = os.path.join(output_directory, anomaly_id)
        if os.path.exists(renamed_dir):
            raise FileExistsError(f"Renamed output directory already exists: {renamed_dir}")
        os.rename(copied_dir, renamed_dir)


def sorted_numeric_child_directories(output_directory: str) -> List[str]:
    child_dirs = []
    output_entries = os.listdir(output_directory)
    for name in output_entries:
        child_path = os.path.join(output_directory, name)
        if os.path.isdir(child_path) and name.isdigit():
            child_dirs.append(child_path)
    child_dirs.sort(key=lambda path: int(os.path.basename(path)))
    return child_dirs


def flatten_key_information_directory(parent_dir: str) -> Dict[str, Any]:
    """Normalize one copied key-info directory and return its key-information dict."""
    root_config = os.path.join(parent_dir, "config.json")
    if os.path.exists(root_config):
        os.remove(root_config)

    key_information_path = os.path.join(parent_dir, "key_information.json")
    key_information_data = load_json_file(key_information_path)
    if isinstance(key_information_data, list):
        if len(key_information_data) != 1:
            raise RuntimeError(
                f"Expected one key information item in `{key_information_path}`, "
                f"got {len(key_information_data)}."
            )
        key_information = key_information_data[0]
    elif isinstance(key_information_data, dict):
        key_information = key_information_data
    else:
        raise TypeError(f"Unsupported key_information.json format: {key_information_path}")

    anomaly_id = os.path.basename(parent_dir)
    key_information["anomaly_id"] = anomaly_id

    child_dir = find_single_child_directory(parent_dir)
    anomaly_description_path = os.path.join(child_dir, "anomaly_description.json")
    anomaly_description = load_json_file(anomaly_description_path)
    entry = anomaly_description["rag_content"]["entry"]

    if "error_code" in entry:
        key_information["phe_desp"] = entry["official_description"]
        key_information["cause_analysis"] = entry["cause_analysis"]
    elif "timeout_id" in entry:
        key_information["phe_desp"] = entry["official_definition"]
        key_information["cause_analysis"] = entry["cause_analysis"]
    else:
        raise KeyError(
            f"Neither error_code nor timeout_id found in anomaly entry: "
            f"{anomaly_description_path}"
        )

    os.remove(key_information_path)

    child_entries = os.listdir(child_dir)
    for name in child_entries:
        src = os.path.join(child_dir, name)
        dst = os.path.join(parent_dir, name)
        if os.path.exists(dst):
            raise FileExistsError(f"Cannot flatten `{src}` because `{dst}` already exists.")
        shutil.move(src, dst)
    os.rmdir(child_dir)

    return key_information

# python test_stat_key_info.py ---mode output
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Statistics for synthesized anomaly key information directories."
    )
    parser.add_argument(
        "--mode",
        type=parse_mode,
        default="stat",
        help=(
            "Execution mode. `stat` only counts test_key_information directories; "
            "`output` also aggregates anomaly sample data into output_directory."
        ),
    )
    parser.add_argument("--start_id", type=int, default=0, help="Start prefix id.")
    parser.add_argument("--end_id", type=int, default=100, help="End prefix id.")
    parser.add_argument(
        "--input_base_directory",
        type=str,
        default=str(Path(__file__).resolve().parent / "Output"),
        help="Base directory containing the per-run synthesis output directories.",
    )
    parser.add_argument(
        "--suffix",
        type=str,
        default="_ano_syn",
        help="Suffix used to filter the per-run synthesis output directories.",
    )
    parser.add_argument(
        "--sub_suffix",
        type=int,
        default=260501000000,
        help=(
            "Minimum timestamp suffix for child synthesis directories. "
            "Only subdirectories with a YYMMDDHHMMSS suffix greater than or "
            "equal to this value are processed."
        ),
    )
    parser.add_argument(
        "--anomaly_def_directory",
        type=str,
        default=str(Path(__file__).resolve().parent / "Anomaly_Def/anomaly_def_pg_rag"),
        help="Directory containing numeric anomaly definition JSON files.",
    )
    parser.add_argument(
        "--output_directory",
        type=str,
        default=str(Path(__file__).resolve().parent / "Output"),
        help="Directory used to store the merged database anomaly output.",
    )
    parser.add_argument(
        "--output_empty",
        type=str_to_bool,
        default=True,
        help="Whether to clear output_directory before processing.",
    )
    args = parser.parse_args()

    mode = args.mode
    start_id = args.start_id
    end_id = args.end_id
    input_base_directory = args.input_base_directory
    suffix = args.suffix
    sub_suffix = args.sub_suffix
    anomaly_def_directory = args.anomaly_def_directory
    output_root_directory = args.output_directory
    output_empty = args.output_empty

    # Validate the id range so an inverted range fails fast rather than yielding 0.
    if start_id > end_id:
        raise ValueError(f"start_id ({start_id}) must not be greater than end_id ({end_id}).")

    if sub_suffix < 0:
        raise ValueError(f"sub_suffix ({sub_suffix}) must be non-negative.")

    # Validate that the base directory exists and is readable.
    if not os.path.isdir(input_base_directory):
        raise NotADirectoryError(f"Input base directory does not exist or is not a directory: {input_base_directory}")
    try:
        base_entries = os.listdir(input_base_directory)
    except OSError as error:
        raise OSError(f"Failed to read input base directory `{input_base_directory}`: {error}")

    # Keep this read to preserve explicit validation and a clear failure message.
    del base_entries

    matched_dirs = find_run_directories(input_base_directory, suffix, start_id, end_id)
    total_key_information_count, run_statistics = count_key_information_directories(
        matched_dirs,
        sub_suffix,
    )

    if mode == "stat":
        print_key_information_statistics(total_key_information_count, run_statistics)
        raise SystemExit(0)

    print_key_information_statistics(total_key_information_count, run_statistics)

    output_directory = build_dynamic_output_directory(
        output_root_directory,
        suffix,
        matched_dirs,
    )
    print(f"Final output directory: {output_directory}")

    ensure_empty_directory(output_directory, output_empty)
    collect_anomaly_definitions(anomaly_def_directory, output_directory)

    for run_dir in matched_dirs:
        paired_step_dirs = align_run_step_directories(run_dir, sub_suffix)
        copy_and_rename_key_information_dirs(
            paired_step_dirs,
            output_directory,
        )

    # After all id_suffix run directories have been copied into output_directory,
    # normalize every anomaly-id directory: remove temporary files, flatten the
    # inner synthesized case directory, and update key_information fields.
    output_subdirectories = sorted_numeric_child_directories(output_directory)
    key_information_list: List[Dict[str, Any]] = []
    for subdirectory in progress_iter(
        output_subdirectories,
        desc="Normalizing output anomaly dirs",
        unit="dir",
    ):
        key_information_list.append(flatten_key_information_directory(subdirectory))

    dump_json_file(
        os.path.join(output_directory, "key_information.json"),
        key_information_list,
    )
    print(f"Completed processing {len(key_information_list)} anomaly directories into `{output_directory}`.")
