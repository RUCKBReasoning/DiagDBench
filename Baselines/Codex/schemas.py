from pydantic import BaseModel, ConfigDict, Field

class ExecuteInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    shell: str = Field(min_length=1, max_length=100000, description="Complete Bash script; first line: # TIMEOUT THRESHOLD: <seconds>.")


class FinishInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    diagnosis_report: str = Field(min_length=1, description="Markdown report: evidence, root cause, applied recovery and validation results.")
    solution_script: str = Field(min_length=1, description="Reusable Bash recovery script, first line # TIMEOUT THRESHOLD: <seconds>.")
    verified_workload: str = Field(
        default="",
        description=(
            "Optimized SQL only when it has a potential performance benefit and "
            "retains every original statement in exactly the same order, without "
            "adding, deleting, merging, splitting, replacing, or reordering statements. "
            "Sequential execution from the same initialized state must produce exactly "
            "the same observable result for every corresponding statement, including "
            "returned rows and values, duplicate counts, affected-row counts, and "
            "persistent database state. Any result mismatch is a diagnosis failure. "
            "Return empty to retain the original workload when exact equivalence cannot "
            "be guaranteed."
        ),
    )
