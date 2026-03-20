"""Tests for cache_layer BigQuery client."""

from datetime import datetime, timezone
from unittest import mock

import pytest

from cache_layer.bigquery_client import BigQueryClient


def _make_client(query_result=None):
    """Create a BigQueryClient with a mocked BQ client."""
    mock_bq = mock.MagicMock()
    if query_result is not None:
        mock_bq.query.return_value.result.return_value.to_dataframe.return_value = query_result
    return BigQueryClient(client=mock_bq), mock_bq


class TestGetAuthorPubStats:
    def test_returns_records(self):
        import pandas as pd
        df = pd.DataFrame([
            {"author_pub_id": "abc:1", "title": "Paper 1", "num_citations_percentile": 0.8},
        ])
        client, _ = _make_client(df)
        result = client.get_author_pub_stats("abc123")
        assert len(result) == 1
        assert result[0]["title"] == "Paper 1"

    def test_returns_empty_on_failure(self):
        mock_bq = mock.MagicMock()
        mock_bq.query.side_effect = Exception("BQ error")
        client = BigQueryClient(client=mock_bq)
        result = client.get_author_pub_stats("abc123")
        assert result == []


class TestGetAuthorStats:
    def test_returns_single_dict(self):
        import pandas as pd
        df = pd.DataFrame([
            {"scholar_id": "abc123", "name": "Test", "hindex": 10, "pip_auc_score": 0.7},
        ])
        client, _ = _make_client(df)
        result = client.get_author_stats("abc123")
        assert result["name"] == "Test"
        assert result["pip_auc_score"] == 0.7

    def test_returns_none_if_empty(self):
        import pandas as pd
        client, _ = _make_client(pd.DataFrame())
        result = client.get_author_stats("abc123")
        assert result is None


class TestGetPublicationStats:
    def test_returns_records(self):
        import pandas as pd
        df = pd.DataFrame([
            {"citation_year": 2020, "yearly_citations": 5},
            {"citation_year": 2021, "yearly_citations": 8},
        ])
        client, _ = _make_client(df)
        result = client.get_publication_stats("abc:123")
        assert len(result) == 2

    def test_returns_empty_on_failure(self):
        mock_bq = mock.MagicMock()
        mock_bq.query.side_effect = Exception("BQ error")
        client = BigQueryClient(client=mock_bq)
        result = client.get_publication_stats("abc:123")
        assert result == []


class TestGetAuthorTemporalStats:
    def test_returns_records(self):
        import pandas as pd
        df = pd.DataFrame([
            {"state_year": 2020, "hindex": 5},
            {"state_year": 2021, "hindex": 7},
        ])
        client, _ = _make_client(df)
        result = client.get_author_temporal_stats("abc123")
        assert len(result) == 2


class TestGetAuthorFreshness:
    def test_author_exists(self):
        import pandas as pd
        ts = datetime(2026, 1, 15, tzinfo=timezone.utc)
        df = pd.DataFrame([{"last_updated": ts}])
        client, _ = _make_client(df)
        exists, last_updated = client.get_author_freshness("abc123")
        assert exists is True
        assert last_updated == ts

    def test_author_not_found(self):
        import pandas as pd
        df = pd.DataFrame([{"last_updated": None}])
        client, _ = _make_client(df)
        exists, last_updated = client.get_author_freshness("unknown")
        assert exists is False
        assert last_updated is None

    def test_query_failure(self):
        mock_bq = mock.MagicMock()
        mock_bq.query.side_effect = Exception("BQ error")
        client = BigQueryClient(client=mock_bq)
        exists, last_updated = client.get_author_freshness("abc123")
        assert exists is False
        assert last_updated is None


class TestGetRecentlyAnalyzedAuthors:
    def test_returns_records(self):
        import pandas as pd
        df = pd.DataFrame([
            {"scholar_id": "a1", "name": "Author 1"},
            {"scholar_id": "a2", "name": "Author 2"},
        ])
        client, _ = _make_client(df)
        result = client.get_recently_analyzed_authors(limit=10)
        assert len(result) == 2

    def test_returns_empty_on_failure(self):
        mock_bq = mock.MagicMock()
        mock_bq.query.side_effect = Exception("BQ error")
        client = BigQueryClient(client=mock_bq)
        result = client.get_recently_analyzed_authors()
        assert result == []


class TestGetAllAuthorIds:
    def test_returns_ids(self):
        import pandas as pd
        df = pd.DataFrame({"scholar_id": ["a1", "a2", "a3"]})
        client, _ = _make_client(df)
        result = client.get_all_author_ids()
        assert result == ["a1", "a2", "a3"]

    def test_returns_empty_on_failure(self):
        mock_bq = mock.MagicMock()
        mock_bq.query.side_effect = Exception("BQ error")
        client = BigQueryClient(client=mock_bq)
        result = client.get_all_author_ids()
        assert result == []
