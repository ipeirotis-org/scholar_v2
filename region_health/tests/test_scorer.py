"""Tests for region_health.scorer."""

from unittest import mock

from region_health.config import DEFAULT_WEIGHT, MIN_WEIGHT
from region_health.scorer import compute_weight, update_scores


class TestComputeWeight:
    def test_zero_errors(self):
        assert compute_weight(100, 0, 0) == 1.0

    def test_ten_percent_errors(self):
        w = compute_weight(90, 10, 0)
        assert abs(w - 0.70) < 0.01

    def test_ten_percent_timeouts(self):
        w = compute_weight(90, 0, 10)
        assert abs(w - 0.70) < 0.01

    def test_mixed_errors_and_timeouts(self):
        w = compute_weight(80, 10, 10)
        assert abs(w - 0.40) < 0.01

    def test_high_error_rate_clamped_to_min(self):
        w = compute_weight(50, 50, 0)
        assert w == MIN_WEIGHT

    def test_all_errors(self):
        w = compute_weight(0, 100, 0)
        assert w == MIN_WEIGHT

    def test_no_data_returns_default(self):
        w = compute_weight(0, 0, 0)
        assert w == DEFAULT_WEIGHT

    def test_one_third_error_rate(self):
        w = compute_weight(67, 33, 0)
        # error_rate ~0.33, weight ~0.01 -> clamped to MIN_WEIGHT
        assert w == MIN_WEIGHT


class TestUpdateScores:
    @mock.patch("region_health.scorer._query_region_stats", return_value={})
    def test_writes_all_regions(self, mock_query):
        mock_monitoring = mock.MagicMock()

        mock_batch = mock.MagicMock()
        mock_firestore = mock.MagicMock()
        mock_firestore.batch.return_value = mock_batch

        update_scores(
            monitoring_client=mock_monitoring,
            firestore_client=mock_firestore,
            project_id="test-project",
        )

        # Should call batch.set for each region
        from region_health.config import AVAILABLE_FUNCTION_REGIONS
        assert mock_batch.set.call_count == len(AVAILABLE_FUNCTION_REGIONS)
        mock_batch.commit.assert_called_once()

    def test_handles_monitoring_failure(self):
        mock_monitoring = mock.MagicMock()
        mock_monitoring.list_time_series.side_effect = Exception("API error")

        mock_firestore = mock.MagicMock()

        # Should not raise
        update_scores(
            monitoring_client=mock_monitoring,
            firestore_client=mock_firestore,
        )
        # Firestore batch should not be committed
        mock_firestore.batch.return_value.commit.assert_not_called()

    @mock.patch("region_health.scorer._query_region_stats", return_value={})
    def test_handles_firestore_write_failure(self, mock_query):
        mock_monitoring = mock.MagicMock()

        mock_batch = mock.MagicMock()
        mock_batch.commit.side_effect = Exception("Firestore error")
        mock_firestore = mock.MagicMock()
        mock_firestore.batch.return_value = mock_batch

        # Should not raise
        update_scores(
            monitoring_client=mock_monitoring,
            firestore_client=mock_firestore,
        )

    @mock.patch("region_health.scorer._query_region_stats")
    def test_weight_values_in_documents(self, mock_query):
        """Verify the weight written to Firestore matches compute_weight logic."""
        mock_query.return_value = {
            "us-central1": {"ok": 90, "error": 10, "timeout": 0},
        }

        mock_monitoring = mock.MagicMock()
        mock_batch = mock.MagicMock()
        mock_firestore = mock.MagicMock()
        mock_firestore.batch.return_value = mock_batch

        update_scores(
            monitoring_client=mock_monitoring,
            firestore_client=mock_firestore,
        )

        # Find the set call for us-central1
        found = False
        for call in mock_batch.set.call_args_list:
            doc_data = call[0][1]
            if doc_data["region"] == "us-central1":
                assert doc_data["ok"] == 90
                assert doc_data["error"] == 10
                assert doc_data["weight"] == round(compute_weight(90, 10, 0), 4)
                found = True
                break
        assert found, "us-central1 document not found in batch writes"
