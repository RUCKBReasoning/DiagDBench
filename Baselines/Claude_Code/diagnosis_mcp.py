"""Claude's MCP entry point uses the same sealed, sample-scoped tools as Codex."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from Baselines.Codex.diagnosis_mcp import main, make_server
from Baselines.Codex.schemas import ExecuteInput, FinishInput

if __name__ == "__main__":
    main()
