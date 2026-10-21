"""Database diagnosis baseline implementations."""

import sys

from . import react

react_common = react
react_super = react
sys.modules[f"{__name__}.react_common"] = react
sys.modules[f"{__name__}.react_super"] = react

__all__ = ["react", "react_common", "react_super"]
