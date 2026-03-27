"""Format conversion utilities for the mex-open-data pipeline."""

from __future__ import annotations

import io

import pandas as pd


def csv_to_parquet(csv_content: str) -> bytes:
    """Convert a CSV string to Parquet bytes (pyarrow engine, no index)."""
    df = pd.read_csv(io.StringIO(csv_content), low_memory=False)
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    return buf.getvalue()


def excel_to_json(raw_bytes: bytes) -> str:
    """Convert an XLS/XLSX file (as bytes) to a JSON records string.

    Requires openpyxl: pip install openpyxl
    """
    df = pd.read_excel(io.BytesIO(raw_bytes), engine="openpyxl")
    return df.to_json(orient="records", force_ascii=False, date_format="iso")
