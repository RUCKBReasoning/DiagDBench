"""Load diagnostic registrations when the agent framework is used.

Importing standalone data, SQL, or prompt utilities must not parse the
diagnostic command line or require an anomaly file.
"""
from importlib import import_module

__all__ = [
    "DBDiag", "QwenDBDiag", "env_registry", "Rule", "order_registry",
    "describer_registry", "selector_registry", "updater_registry",
    "visibility_registry", "MultiAgents", "prepare_task_config",
    "load_agent", "load_environment", "load_tools", "load_llm", "load_memory",
]
_exports_loaded = False


def _load_exports():
    global _exports_loaded
    if _exports_loaded:
        return
    exports = {}
    agent_conf = import_module(".agent_conf", __name__)
    exports.update({name: value for name, value in vars(agent_conf).items()
                    if not name.startswith("_")})
    for module, names in (
        (".agent_conf.output_parser", ("DBDiag",)),
        (".environments", ("env_registry",)),
        (".environments.rules", ("Rule",)),
        (".environments.rules.order", ("order_registry",)),
        (".environments.rules.describer", ("describer_registry",)),
        (".environments.rules.selector", ("selector_registry",)),
        (".environments.rules.updater", ("updater_registry",)),
        (".environments.rules.visibility", ("visibility_registry",)),
        (".multiagents", ("MultiAgents",)),
        (".initialization", (
            "prepare_task_config", "load_agent", "load_environment",
            "load_tools", "load_llm", "load_memory",
        )),
    ):
        loaded = import_module(module, __name__)
        exports.update({name: getattr(loaded, name) for name in names})
    globals().update(exports)
    _exports_loaded = True


def __getattr__(name):
    # Tooling may probe load_tests or other unrelated attributes. Only declared
    # public exports should initialize registrations and optional model clients.
    if name not in __all__:
        raise AttributeError(name)
    _load_exports()
    if name in globals():
        return globals()[name]
    raise AttributeError(name)
