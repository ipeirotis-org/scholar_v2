"""Tests for materialize.py against the real gs_statistics SQL directory."""

import os
import re

from gs_crawler import materialize


class _Client:
    def __init__(self):
        self.sql = []

    def query(self, sql):
        self.sql.append(sql)

        class _Job:
            def result(self):
                return None
        return _Job()


def test_order_covers_every_sql_file():
    names = {n for n, _ in materialize.load_order()}
    on_disk = {f for f in os.listdir(materialize.SQL_DIR) if f.endswith(".sql")}
    assert names == on_disk


def test_dependencies_come_first():
    order = [n for n, _ in materialize.load_order()]
    for i, name in enumerate(order):
        sql = open(os.path.join(materialize.SQL_DIR, name)).read()
        own = name[:-4]
        refs = set(re.findall(r"gs_statistics\.(\w+)`", sql)) - {own}
        for ref in refs:
            assert f"{ref}.sql" in order[:i], f"{name} uses {ref} before it is defined"


def test_no_s2_sources():
    for name, _ in materialize.load_order():
        sql = open(os.path.join(materialize.SQL_DIR, name)).read()
        assert "s2_data" not in sql and "scholar-version2.statistics." not in sql, name


def test_run_tables_only():
    client = _Client()
    done = materialize.run(kinds=("table",), client=client)
    assert done == ["author_coauthor_fields.sql", "dist_publication_citations.sql",
                    "dist_author_metrics.sql", "dist_pip_thresholds.sql",
                    "dist_pip_scores.sql", "dist_pip_auc_scores.sql",
                    "dist_pip_auc_scores_field.sql"]
    assert all("CREATE OR REPLACE TABLE" in s for s in client.sql)
