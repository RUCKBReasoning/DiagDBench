"""Artifact schemas shared by the two native agent integrations."""
from pydantic import BaseModel, ConfigDict, Field


class ExecuteInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    shell: str = Field(
        min_length=1, max_length=100000,
        description="Complete Bash script; first line: # TIMEOUT THRESHOLD: <seconds>.",
    )


class FinishInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    diagnosis_report: str = Field(
        min_length=1, description="Evidence, root cause, actions, recovery postchecks, validation and unresolved issues.",
    )
    solution_script: str = Field(
        min_length=1,
        description=(
            "Self-contained Bash recovery from the anomalous state. First line: "
            "# TIMEOUT THRESHOLD: <seconds>. Include bounded polling/postchecks "
            "until relevant rollback, locks, I/O, maintenance and residual recovery "
            "effects have cleared. Exit nonzero if readiness cannot be verified; "
            "successful exit means immediate workload validation is safe."
        ),
    )
    verified_workload: str = Field(
        default="",
        description=(
            "Optimize only verified_workload.sql; leave initialization unchanged. "
            "Keep exactly the original statement count/order and each statement's "
            "observable semantics, including rows, duplicates, NULLs, affected counts, "
            "transaction boundaries, side effects and final state. Never add/delete/"
            "merge/split/reorder statements or reduce work. Return empty if exact "
            "equivalence cannot be justified or no concrete performance benefit exists."
        ),
    )
