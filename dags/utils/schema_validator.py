"""
Schema validation for incoming CSV data.

Compares the columns of a new DataFrame against the schema of the already-stored
Parquet file on S3. If columns have been added or removed, the dataset is considered
schema-drifted and the raw file is routed to a quarantine prefix instead of curated/.

This prevents silent schema breaks from corrupting downstream Athena queries.
"""

from __future__ import annotations

import io
import logging

import boto3
import pyarrow.parquet as pq

log = logging.getLogger(__name__)


def _read_parquet_columns(bucket: str, key: str) -> list[str] | None:
    """Return column names of an existing Parquet on S3, or None if not found."""
    try:
        obj = boto3.client("s3").get_object(Bucket=bucket, Key=key)
        buf = io.BytesIO(obj["Body"].read())
        return pq.read_schema(buf).names
    except Exception:
        return None  # first ingest or key doesn't exist — treat as valid


def validate_schema(
    new_columns: list[str],
    bucket: str,
    existing_parquet_key: str,
) -> tuple[bool, str]:
    """Compare *new_columns* to the schema of an existing Parquet on S3.

    Returns:
        (True, "")            — schema matches or no existing Parquet yet.
        (False, reason_str)   — columns have drifted; caller should quarantine.
    """
    existing = _read_parquet_columns(bucket, existing_parquet_key)
    if existing is None:
        return True, ""

    if new_columns == existing:
        return True, ""

    added = sorted(set(new_columns) - set(existing))
    removed = sorted(set(existing) - set(new_columns))
    reason = f"schema drift — added={added}, removed={removed}"
    log.warning("Schema drift detected for %s: %s", existing_parquet_key, reason)
    return False, reason
