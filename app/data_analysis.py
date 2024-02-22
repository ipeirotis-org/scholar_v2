import pandas as pd
import numpy as np
import logging
import datetime
from data_access import get_firestore_cache, set_firestore_cache
from scholar import get_author, get_publication, get_author_publications, get_author_last_modification
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)


def get_author_pub_stats_bg(author_id):
    SQL = f"""
        WITH
          pub_details AS ( (
            SELECT
              JSON_EXTRACT_SCALAR(DATA, '$.data.author_pub_id') AS author_pub_id,
              JSON_EXTRACT_SCALAR(DATA, '$.data.bib.title') AS title,
              JSON_EXTRACT_SCALAR(DATA, '$.data.bib.citation') AS citation,
              CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.bib.pub_year') AS INT64) AS pub_year,
              CAST(JSON_EXTRACT_SCALAR(DATA, '$.data.num_citations') AS INT64) AS num_citations
            FROM
              `scholar-version2.firestore_export.scholar_raw_pub_raw_latest` ) )
        SELECT
          P.*,
          S.num_citations_percentile,
          S.publication_rank,
          S.num_papers_percentile
        FROM
          `scholar-version2.statistics.author_pub_stats` S
        JOIN
          pub_details P
        ON
          P.author_pub_id = S.author_pub_id
        WHERE
          S.scholar_id = '{author_id}'
        ORDER BY
          S.publication_rank
    """

    bq = bigquery.Client()
    query_job = bq.query(SQL)
    results = query_job.result()  # Waits for the query to finish
    df = results.to_dataframe()
    list_of_dicts = df.to_dict("records")
    return list_of_dicts


def get_author_stats_bg(author_id):
    SQL = f"""
        SELECT
          S.*, P.pip_auc_score, P.pip_auc_score_percentile
        FROM
          `scholar-version2.statistics.author_stats` S
        LEFT JOIN
          `scholar-version2.statistics.author_pip_scores` P
        ON
          P.scholar_id = S.scholar_id
        WHERE
          S.scholar_id = '{author_id}'
    """

    bq = bigquery.Client()
    query_job = bq.query(SQL)
    results = query_job.result()  # Waits for the query to finish
    df = results.to_dataframe()
    list_of_dicts = df.to_dict("records")
    if len(list_of_dicts) == 1:
        return list_of_dicts[0]
    else:
        return None


def get_author_stats(author_id):
    author = get_author(author_id)
    if not author:
        return None

    author_last_modified = get_author_last_modification(author_id)
    
    author_pub_stats, pub_stats_timestamp = get_firestore_cache("author_pub_stats",author_id)
    if not author_pub_stats or author_last_modified > pub_stats_timestamp:
        author_pub_stats = get_author_pub_stats_bg(author_id)
    if len(author_pub_stats) == 0:
        return None
    set_firestore_cache("author_pub_stats", author_id, author_pub_stats)
    author["publications"] = author_pub_stats

    author_stats, stats_timestamp = get_firestore_cache("author_stats",author_id)
    if not author_stats or author_last_modified > stats_timestamp:
        author_stats = get_author_stats_bg(author_id)
    if not author_stats:
        return None
    set_firestore_cache("author_stats", author_id, author_stats)
    author["stats"] = author_stats

    return author
