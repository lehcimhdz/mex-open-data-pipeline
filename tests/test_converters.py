"""Tests for dags/utils/converters.py."""
import io
import json

import pandas as pd
import pytest

from utils.converters import csv_to_parquet, excel_to_json


# ---------------------------------------------------------------------------
# csv_to_parquet
# ---------------------------------------------------------------------------

def test_csv_to_parquet_returns_parquet_magic_bytes():
    csv = "id,name\n1,alpha\n2,beta\n"
    result = csv_to_parquet(csv)
    assert result[:4] == b"PAR1", "Output should start with Parquet magic bytes"


def test_csv_to_parquet_preserves_row_count():
    csv = "id,val\n1,a\n2,b\n3,c\n"
    result = csv_to_parquet(csv)
    df = pd.read_parquet(io.BytesIO(result))
    assert len(df) == 3


def test_csv_to_parquet_preserves_column_names():
    csv = "ciudad,poblacion\nCDMX,9000000\n"
    result = csv_to_parquet(csv)
    df = pd.read_parquet(io.BytesIO(result))
    assert list(df.columns) == ["ciudad", "poblacion"]


def test_csv_to_parquet_unicode():
    csv = "nombre,estado\nNiño,México\nJosé,Jalisco\n"
    result = csv_to_parquet(csv)
    df = pd.read_parquet(io.BytesIO(result))
    assert df["nombre"].tolist() == ["Niño", "José"]


def test_csv_to_parquet_header_only():
    """A CSV with only headers and no data rows should produce a valid empty Parquet."""
    csv = "id,name\n"
    result = csv_to_parquet(csv)
    df = pd.read_parquet(io.BytesIO(result))
    assert len(df) == 0
    assert list(df.columns) == ["id", "name"]


def test_csv_to_parquet_no_index():
    """Parquet output must not include the pandas RangeIndex as a column."""
    csv = "x,y\n1,2\n"
    result = csv_to_parquet(csv)
    df = pd.read_parquet(io.BytesIO(result))
    assert "index" not in df.columns
    assert "__index_level_0__" not in df.columns


# ---------------------------------------------------------------------------
# excel_to_json
# ---------------------------------------------------------------------------

def _make_excel(data: dict) -> bytes:
    """Helper — build an in-memory Excel file from a dict of columns."""
    buf = io.BytesIO()
    pd.DataFrame(data).to_excel(buf, index=False, engine="openpyxl")
    return buf.getvalue()


def test_excel_to_json_basic():
    raw = _make_excel({"id": [1, 2], "city": ["CDMX", "GDL"]})
    result = excel_to_json(raw)
    records = json.loads(result)
    assert len(records) == 2
    assert records[0]["city"] == "CDMX"


def test_excel_to_json_returns_records_orientation():
    raw = _make_excel({"a": [10], "b": [20]})
    result = excel_to_json(raw)
    records = json.loads(result)
    assert isinstance(records, list)
    assert "a" in records[0]


def test_excel_to_json_unicode():
    raw = _make_excel({"nombre": ["México", "Jalisco"]})
    result = excel_to_json(raw)
    records = json.loads(result)
    assert records[0]["nombre"] == "México"


def test_excel_to_json_invalid_bytes_raises():
    with pytest.raises(Exception):
        excel_to_json(b"this is not an excel file")
