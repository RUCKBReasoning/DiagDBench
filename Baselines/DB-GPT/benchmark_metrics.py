"""Explicit metric-source adapter; never switch catalogs on ImportError.

The CLI normally reads upstream Prometheus-named evidence. DiagDBench's launcher
explicitly opts into its smaller container/SQL snapshot catalog instead.
"""
import os
from pathlib import Path


def load_metric_catalog(source=None):
    source = source or os.getenv("DB_GPT_METRICS_SOURCE", "prometheus")
    if source == "container":
        return {
            "cpu_usage": "cpu_usage",
            "memory_usage": "memory_usage",
            # Upstream routes workload/index/query/default experts to CPU metrics.
            "cpu_metrics": ["cpu_usage", "pg_stat_activity_count",
                            "pg_stat_activity_active_count", "pg_locks_count",
                            "pg_stat_database_tup_returned_per_second"],
            "memory_metrics": ["memory_usage"],
            "io_metrics": ["container_block_read_bytes", "container_block_write_bytes",
                           "pg_stat_database_temp_bytes_per_second",
                           "pg_stat_database_xact_rollback_per_second",
                           "pg_stat_database_blks_hit_ratio"],
            "network_metrics": ["container_network_receive_bytes",
                                "container_network_transmit_bytes"],
        }
    if source != "prometheus":
        raise ValueError("DB_GPT_METRICS_SOURCE must be prometheus or container")
    from prometheus_service_docker.yaml_utils import read_prometheus_metrics_yaml
    return read_prometheus_metrics_yaml(
        str(Path(__file__).resolve().parent / "prometheus_service_docker" / "prometheus_metrics.yaml"),
        node_exporter_instance=os.getenv("DB_GPT_NODE_EXPORTER_INSTANCE", "localhost:9100"),
        postgresql_exporter_instance=os.getenv("DB_GPT_POSTGRES_EXPORTER_INSTANCE", "localhost:9187"),
    )
