"""
test_tlc_ingest_operator.py

Unit tests for TLCIngestOperator.
Mocks boto3 and requests to test retry logic and idempotency
without any real I/O.
"""

from unittest.mock import MagicMock, patch

import pytest

from dags.operators.tlc_ingest_operator import TLCIngestOperator


@pytest.mark.unit
class TestTLCIngestOperatorIdempotency:
    def _make_operator(self):
        return TLCIngestOperator(task_id="test_ingest", year=2024, month=1)

    def test_skips_download_if_file_exists(self):
        op = self._make_operator()

        with (
            patch.object(op, "_get_s3_client") as mock_client_fn,
            patch.object(op, "_object_exists", return_value=True),
            patch("dags.operators.tlc_ingest_operator._load_config") as mock_cfg,
        ):
            mock_cfg.return_value = {
                "storage": {
                    "endpoint": "http://localhost:9000",
                    "buckets": {"bronze": "bronze"},
                    "paths": {"bronze_yellow_taxi": "raw/yellow_taxi"},
                },
                "source": {"tlc_base_url": "https://example.com"},
            }
            mock_client = MagicMock()
            mock_client_fn.return_value = mock_client
            mock_context = {"run_id": "test_run_123"}

            result = op.execute(mock_context)

        assert "s3://bronze/" in result
        mock_client.create_multipart_upload.assert_not_called()

    def test_uploads_when_file_does_not_exist(self):
        op = self._make_operator()

        with (
            patch.object(op, "_get_s3_client") as mock_client_fn,
            patch.object(op, "_object_exists", return_value=False),
            patch.object(op, "_stream_to_minio", return_value=50_000_000) as mock_stream,
            patch("dags.operators.tlc_ingest_operator._load_config") as mock_cfg,
        ):
            mock_cfg.return_value = {
                "storage": {
                    "endpoint": "http://localhost:9000",
                    "buckets": {"bronze": "bronze"},
                    "paths": {"bronze_yellow_taxi": "raw/yellow_taxi"},
                },
                "source": {"tlc_base_url": "https://example.com"},
            }
            mock_client_fn.return_value = MagicMock()
            mock_context = {"run_id": "test_run_456"}

            result = op.execute(mock_context)

        assert mock_stream.called
        assert "s3://bronze/" in result


@pytest.mark.unit
class TestObjectExists:
    def test_returns_true_when_head_succeeds(self):
        mock_client = MagicMock()
        mock_client.head_object.return_value = {"ContentLength": 100}
        result = TLCIngestOperator._object_exists(mock_client, "bronze", "some/key.parquet")
        assert result is True

    def test_returns_false_when_head_raises(self):
        mock_client = MagicMock()
        mock_client.head_object.side_effect = Exception("Not Found")
        mock_client.exceptions.ClientError = Exception
        result = TLCIngestOperator._object_exists(mock_client, "bronze", "some/key.parquet")
        assert result is False
