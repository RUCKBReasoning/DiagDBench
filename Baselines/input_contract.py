"""Shared, ground-truth-free input formatting for baseline prompts."""

from __future__ import annotations

from pathlib import Path
from typing import Mapping


WORKLOAD_SEPARATOR = "\n\n--- anomaly-verified workload phase boundary ---\n\n"
_WORKLOAD_TEMPLATE_BLOCK = """## INITIALIZATION WORKLOAD:
{INITIALIZATION_WORKLOAD}

## ANOMALY TRIGGERED (VERIFIED) WORKLOAD:
{VERIFIED_WORKLOAD}"""
_MERGED_WORKLOAD_TEMPLATE_BLOCK = """## ANOMALY-VERIFIED WORKLOAD:
{ANOMALY_VERIFIED_WORKLOAD}"""


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def anomaly_verified_workload(files: Mapping[str, Path]) -> str:
    """Return one ordered workload input while retaining the phase boundary."""
    initialization = read_text(files["initialization"]).strip()
    verified = read_text(files["verified"]).strip()
    return (
        "-- Preparation phase: execute first\n"
        + initialization
        + WORKLOAD_SEPARATOR
        + "-- Anomaly verification phase: execute second\n"
        + verified
    ).strip()


def render_diagnosis_prompt(template: str, values: Mapping[str, object]) -> str:
    """Render legacy templates with one workload section in the final prompt."""
    if _WORKLOAD_TEMPLATE_BLOCK not in template:
        raise ValueError("Diagnosis prompt template has an unexpected workload layout.")
    merged_template = template.replace(
        _WORKLOAD_TEMPLATE_BLOCK,
        _MERGED_WORKLOAD_TEMPLATE_BLOCK,
        1,
    )
    return merged_template.format(**values)
