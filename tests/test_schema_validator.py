"""Unit tests for dags/utils/schema_validator.py."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# _read_parquet_columns
# ---------------------------------------------------------------------------

class TestReadParquetColumns:
    def test_returns_none_on_s3_error(self):
        from utils.schema_validator import _read_parquet_columns

        with patch("utils.schema_validator.boto3.client") as mock_boto:
            mock_boto.return_value.get_object.side_effect = Exception("NoSuchKey")
            result = _read_parquet_columns("my-bucket", "raw/cat/ds/data.parquet")

        assert result is None

    def test_returns_column_names_on_success(self):
        from utils.schema_validator import _read_parquet_columns

        mock_schema = MagicMock()
        mock_schema.names = ["id", "name", "value"]

        with patch("utils.schema_validator.boto3.client") as mock_boto, \
             patch("utils.schema_validator.pq.read_schema", return_value=mock_schema):
            mock_boto.return_value.get_object.return_value = {
                "Body": MagicMock(read=lambda: b"PAR1fakedata")
            }
            result = _read_parquet_columns("my-bucket", "raw/cat/ds/data.parquet")

        assert result == ["id", "name", "value"]

    def test_returns_none_when_parquet_unreadable(self):
        from utils.schema_validator import _read_parquet_columns

        with patch("utils.schema_validator.boto3.client") as mock_boto, \
             patch("utils.schema_validator.pq.read_schema", side_effect=Exception("corrupt")):
            mock_boto.return_value.get_object.return_value = {
                "Body": MagicMock(read=lambda: b"notparquet")
            }
            result = _read_parquet_columns("my-bucket", "raw/cat/ds/data.parquet")

        assert result is None


# ---------------------------------------------------------------------------
# validate_schema
# ---------------------------------------------------------------------------

class TestValidateSchema:
    def test_returns_true_when_no_existing_parquet(self):
        from utils.schema_validator import validate_schema

        with patch("utils.schema_validator._read_parquet_columns", return_value=None):
            ok, reason = validate_schema(["a", "b"], "bucket", "key")

        assert ok is True
        assert reason == ""

    def test_returns_true_when_columns_identical(self):
        from utils.schema_validator import validate_schema

        with patch("utils.schema_validator._read_parquet_columns", return_value=["a", "b"]):
            ok, reason = validate_schema(["a", "b"], "bucket", "key")

        assert ok is True
        assert reason == ""

    def test_returns_false_when_column_added(self):
        from utils.schema_validator import validate_schema

        with patch("utils.schema_validator._read_parquet_columns", return_value=["a"]):
            ok, reason = validate_schema(["a", "b_new"], "bucket", "key")

        assert ok is False
        assert "added" in reason
        assert "b_new" in reason

    def test_returns_false_when_column_removed(self):
        from utils.schema_validator import validate_schema

        with patch("utils.schema_validator._read_parquet_columns", return_value=["a", "b"]):
            ok, reason = validate_schema(["a"], "bucket", "key")

        assert ok is False
        assert "removed" in reason
        assert "b" in reason

    def test_reason_mentions_schema_drift(self):
        from utils.schema_validator import validate_schema

        with patch("utils.schema_validator._read_parquet_columns", return_value=["old_col"]):
            _, reason = validate_schema(["new_col"], "bucket", "key")

        assert "schema drift" in reason

    def test_drift_report_includes_both_added_and_removed(self):
        from utils.schema_validator import validate_schema

        existing = ["common", "will_be_removed"]
        incoming = ["common", "newly_added"]

        with patch("utils.schema_validator._read_parquet_columns", return_value=existing):
            ok, reason = validate_schema(incoming, "bucket", "key")

        assert ok is False
        assert "newly_added" in reason
        assert "will_be_removed" in reason

    def test_calls_read_parquet_columns_with_correct_args(self):
        from utils.schema_validator import validate_schema

        with patch("utils.schema_validator._read_parquet_columns", return_value=None) as mock_read:
            validate_schema(["col"], "my-bucket", "curated/cat/ds/data.parquet")

        mock_read.assert_called_once_with("my-bucket", "curated/cat/ds/data.parquet")

    def test_logs_warning_on_drift(self):
        from utils.schema_validator import validate_schema

        with patch("utils.schema_validator._read_parquet_columns", return_value=["old"]), \
             patch("utils.schema_validator.log") as mock_log:
            validate_schema(["new"], "bucket", "key")

        mock_log.warning.assert_called_once()
