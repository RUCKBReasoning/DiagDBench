import argparse
import logging
import shutil
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from utils import set_up_logger  # noqa: E402

ARTIFACT_NAMES = (
    "solution_script.sh",
    "diagnosis_report.md",
    "optimized_verified_workload.sql",
    "verified_workload.sql",
)
TOP_LEVEL_METADATA = ("key_information.json", "log.txt")
# Dedicated log file for this extraction step. Kept distinct from the copied
# `log.txt` metadata so the two never overwrite each other.
LOG_FILE_NAME = "extract_solution_log.txt"

logger = logging.getLogger(f"DB_DIAG.{__name__}")


def resolve_path(path_value: str | Path | None) -> Path | None:
    if path_value is None:
        return None
    path = Path(path_value).expanduser()
    return path if path.is_absolute() else BASE_DIR / path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract solution artifacts from LLM Baselines output directories."
    )
    parser.add_argument(
        "--solution-directory",
        type=str,
        required=True,
        help="Baselines output directory containing per-sample solution artifact folders.",
    )
    parser.add_argument(
        "--output-directory",
        type=str,
        required=True,
        help="Directory where extracted artifacts will be copied.",
    )
    parser.add_argument(
        "--truncate-num",
        type=int,
        default=None,
        help="Maximum number of sample directories to extract. Negative means all.",
    )
    parser.add_argument(
        "--clean-output",
        action="store_true",
        help="Remove output directory before extraction.",
    )
    return parser.parse_args()


def sample_sort_key(path: Path) -> tuple[int, int | str]:
    try:
        return (0, int(path.name))
    except ValueError:
        return (1, path.name)


def is_solution_sample_directory(path: Path) -> bool:
    if not path.is_dir():
        return False
    return any((path / artifact_name).is_file() for artifact_name in ARTIFACT_NAMES)


def discover_solution_directories(solution_directory: Path) -> list[Path]:
    if not solution_directory.is_dir():
        raise NotADirectoryError(f"solution_directory is not a directory: {solution_directory}")
    return sorted(
        (path for path in solution_directory.iterdir() if is_solution_sample_directory(path)),
        key=sample_sort_key,
    )


def prepare_output_directory(output_directory: Path, clean_output: bool) -> None:
    if clean_output and output_directory.exists():
        logger.info(f"[EXTRACT] Removing existing output directory: `{output_directory}`.")
        shutil.rmtree(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)


def copy_optional_file(source_path: Path, target_path: Path) -> bool:
    if not source_path.is_file():
        return False
    target_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, target_path)
    return True


def extract_solution_artifacts(
    solution_directory: Path,
    output_directory: Path,
    truncate_num: int,
) -> tuple[int, int]:
    sample_directories = discover_solution_directories(solution_directory)
    logger.info(f"[EXTRACT] Discovered {len(sample_directories)} solution sample directory(ies) under `{solution_directory}`.")
    if truncate_num >= 0:
        sample_directories = sample_directories[:truncate_num]
        logger.info(f"[EXTRACT] Truncated to the first {len(sample_directories)} sample directory(ies) (truncate_num=`{truncate_num}`).")

    extracted_count = 0
    missing_count = 0
    total = len(sample_directories)
    for output_index, sample_directory in enumerate(sample_directories):
        target_directory = output_directory / str(output_index)
        target_directory.mkdir(parents=True, exist_ok=True)

        copied_any = False
        for artifact_name in ARTIFACT_NAMES:
            source_artifact = sample_directory / artifact_name
            target_name = (
                "optimized_verified_workload.sql"
                if artifact_name == "verified_workload.sql"
                else artifact_name
            )
            copied_any = copy_optional_file(source_artifact, target_directory / target_name) or copied_any

        if copied_any:
            extracted_count += 1
            logger.info(f"[EXTRACT][{output_index + 1}/{total}] Extracted sample directory `{sample_directory}` -> `{target_directory}`.")
        else:
            missing_count += 1
            logger.warning(f"[EXTRACT][{output_index + 1}/{total}][WARNING] No solution artifacts found in `{sample_directory}`.")

    logger.info("[EXTRACT] Copying top-level metadata (key_information.json, log.txt, config.json) ...")
    for metadata_name in TOP_LEVEL_METADATA:
        if copy_optional_file(solution_directory / metadata_name, output_directory / metadata_name):
            logger.info(f"[EXTRACT] Copied metadata `{metadata_name}`.")
    if copy_optional_file(solution_directory / "config.json", output_directory / "source_config.json"):
        logger.info("[EXTRACT] Copied `config.json` -> `source_config.json`.")

    return extracted_count, missing_count


def main() -> int:
    args = parse_args()

    output_directory = resolve_path(args.output_directory)
    # The output directory may be wiped by --clean-output, so create it (and the
    # logger writing into it) only after preparation. This mirrors the project
    # logger convention (`utils.set_up_logger`) used by test_eval.py / test_syn_ano.py.
    prepare_output_directory(output_directory, args.clean_output)
    set_up_logger(str(output_directory / LOG_FILE_NAME))

    logger.info("[MAIN][Extract Solution Artifacts] == START ==")
    try:
        solution_directory = resolve_path(args.solution_directory)
        truncate_num = args.truncate_num if args.truncate_num is not None else -1
        logger.info(f"[MAIN] Solution Directory: `{solution_directory}`.")
        logger.info(f"[MAIN] Output Directory: `{output_directory}`.")
        logger.info(f"[MAIN] Truncate Num: `{truncate_num}` (negative means all).")

        extracted_count, missing_count = extract_solution_artifacts(
            solution_directory,
            output_directory,
            truncate_num,
        )
    except (OSError, ValueError) as exc:
        logger.error(f"[MAIN][ERROR] {exc}")
        return 1

    logger.info(
        f"[MAIN][Extract Solution Artifacts] == DONE == extracted=`{extracted_count}`, "
        f"missing=`{missing_count}`, output_directory=`{output_directory}`."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
