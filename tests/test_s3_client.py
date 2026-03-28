"""Tests for dags/utils/s3_client.py — all AWS calls are mocked."""
import json
from unittest.mock import MagicMock, call, patch

import pytest

from utils.s3_client import download_json, list_folder_prefixes, upload


# ---------------------------------------------------------------------------
# upload
# ---------------------------------------------------------------------------

@patch("utils.s3_client.boto3.client")
def test_upload_bytes(mock_client):
    s3 = MagicMock()
    mock_client.return_value = s3

    upload("my-bucket", "raw/file.bin", b"\x00\x01", "application/octet-stream")

    s3.put_object.assert_called_once_with(
        Bucket="my-bucket",
        Key="raw/file.bin",
        Body=b"\x00\x01",
        ContentType="application/octet-stream",
    )


@patch("utils.s3_client.boto3.client")
def test_upload_str_encoded_to_utf8(mock_client):
    s3 = MagicMock()
    mock_client.return_value = s3

    upload("bucket", "key.json", '{"a":1}', "application/json")

    args = s3.put_object.call_args.kwargs
    assert args["Body"] == b'{"a":1}'


@patch("utils.s3_client.boto3.client")
def test_upload_default_content_type(mock_client):
    s3 = MagicMock()
    mock_client.return_value = s3

    upload("b", "k", b"data")

    assert s3.put_object.call_args.kwargs["ContentType"] == "application/octet-stream"


# ---------------------------------------------------------------------------
# download_json
# ---------------------------------------------------------------------------

@patch("utils.s3_client.boto3.client")
def test_download_json_parses_response(mock_client):
    s3 = MagicMock()
    mock_client.return_value = s3
    payload = {"datasets": [{"slug": "test"}]}
    s3.get_object.return_value = {"Body": MagicMock(read=lambda: json.dumps(payload).encode())}

    result = download_json("bucket", "raw/cat/_catalog.json")

    assert result == payload
    s3.get_object.assert_called_once_with(Bucket="bucket", Key="raw/cat/_catalog.json")


@patch("utils.s3_client.boto3.client")
def test_download_json_propagates_client_error(mock_client):
    from botocore.exceptions import ClientError

    s3 = MagicMock()
    mock_client.return_value = s3
    s3.get_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "Not found"}}, "GetObject"
    )

    with pytest.raises(ClientError):
        download_json("bucket", "nonexistent.json")


# ---------------------------------------------------------------------------
# list_folder_prefixes
# ---------------------------------------------------------------------------

@patch("utils.s3_client.boto3.client")
def test_list_folder_prefixes_single_page(mock_client):
    s3 = MagicMock()
    mock_client.return_value = s3
    paginator = MagicMock()
    s3.get_paginator.return_value = paginator
    paginator.paginate.return_value = [
        {"CommonPrefixes": [{"Prefix": "raw/agricultura/"}, {"Prefix": "raw/economia/"}]}
    ]

    result = list_folder_prefixes("bucket", "raw/")

    assert result == ["raw/agricultura/", "raw/economia/"]
    paginator.paginate.assert_called_once_with(Bucket="bucket", Prefix="raw/", Delimiter="/")


@patch("utils.s3_client.boto3.client")
def test_list_folder_prefixes_multi_page(mock_client):
    s3 = MagicMock()
    mock_client.return_value = s3
    paginator = MagicMock()
    s3.get_paginator.return_value = paginator
    paginator.paginate.return_value = [
        {"CommonPrefixes": [{"Prefix": "raw/a/"}]},
        {"CommonPrefixes": [{"Prefix": "raw/b/"}]},
    ]

    result = list_folder_prefixes("bucket", "raw/")

    assert result == ["raw/a/", "raw/b/"]


@patch("utils.s3_client.boto3.client")
def test_list_folder_prefixes_empty(mock_client):
    s3 = MagicMock()
    mock_client.return_value = s3
    paginator = MagicMock()
    s3.get_paginator.return_value = paginator
    paginator.paginate.return_value = [{}]  # no CommonPrefixes key

    result = list_folder_prefixes("bucket", "raw/")

    assert result == []
