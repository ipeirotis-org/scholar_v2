"""Shared BigQuery helpers used across components.

Provides standard functions for building fully-qualified BigQuery
table/view references. Each component's Config class delegates to these.
"""


def bq_view(project_id, stats_dataset, view_name):
    """Build a fully-qualified BigQuery statistics view/table reference."""
    return f"`{project_id}.{stats_dataset}.{view_name}`"


def bq_raw(project_id, raw_dataset, table_name):
    """Build a fully-qualified BigQuery raw data table reference."""
    return f"`{project_id}.{raw_dataset}.{table_name}`"
