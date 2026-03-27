"""S3 helper utilities for the mex-open-data pipeline."""

from __future__ import annotations

import json

import boto3


def upload(bucket: str, key: str, body: bytes | str, content_type: str = "application/octet-stream") -> None:
    """Upload bytes or a UTF-8 string to S3."""
    if isinstance(body, str):
        body = body.encode("utf-8")
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)


def download_json(bucket: str, key: str) -> dict:
    """Download and parse a JSON object from S3."""
    obj = boto3.client("s3").get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read())


def list_folder_prefixes(bucket: str, prefix: str, delimiter: str = "/") -> list[str]:
    """Return all immediate sub-prefixes under *prefix* (like ls on a folder).

    Example: list_folder_prefixes(bucket, "raw/") returns
    ["raw/agricultura/", "raw/economia/", ...]
    """
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    prefixes: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter=delimiter):
        for entry in page.get("CommonPrefixes", []):
            prefixes.append(entry["Prefix"])
    return prefixes
