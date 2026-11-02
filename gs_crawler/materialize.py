"""Deploy gs_statistics views and rebuild its distribution tables.

Order comes from bigquery/gs_statistics/deploy_order.txt (shared with
.github/workflows/bigquery-views.yml). Distribution tables (dist_*) hold the
percentile breakpoints, so rebuild them after crawls change the population.
"""

import logging
import os

from google.cloud import bigquery

from gs_crawler.config import Config

logger = logging.getLogger(__name__)

SQL_DIR = os.environ.get(
    "GS_STATISTICS_SQL_DIR",
    os.path.join(os.path.dirname(__file__), "..", "bigquery", "gs_statistics"),
)


def load_order(sql_dir=SQL_DIR):
    """Return [(filename, kind)] in deploy order; kind is 'view' or 'table'."""
    order = []
    with open(os.path.join(sql_dir, "deploy_order.txt")) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            name, kind = line.split()
            if kind not in ("view", "table"):
                raise ValueError(f"bad kind {kind!r} for {name}")
            order.append((name, kind))
    return order


def run(kinds=("table",), sql_dir=SQL_DIR, client=None):
    """Execute the SQL files of the given kinds, in dependency order."""
    client = client or bigquery.Client(project=Config.PROJECT_ID)
    done = []
    for name, kind in load_order(sql_dir):
        if kind not in kinds:
            continue
        with open(os.path.join(sql_dir, name)) as f:
            sql = f.read()
        logger.info("Running %s (%s)", name, kind)
        client.query(sql).result()
        done.append(name)
    return done
