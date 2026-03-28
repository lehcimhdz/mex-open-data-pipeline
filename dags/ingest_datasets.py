"""
DAG: ingest_datasets
====================
Schedule : Mon-Fri at 07:00 UTC (one hour after sync_catalog)
Purpose  : For each category, read the catalog from S3, fetch full DatasetDetail
           for every dataset, download all resource files, and store them in the
           data lake:

           raw/{cat}/{dataset}/_metadata.json   ← DatasetDetail (all fields)
           raw/{cat}/{dataset}/resources/{id}.csv          ← original CSV
           raw/{cat}/{dataset}/resources/{id}.xlsx         ← original Excel
           raw/{cat}/{dataset}/resources/{id}.json         ← Excel→JSON or non-CSV metadata
           raw/{cat}/{dataset}/resources/{id}.{ext}        ← other binary files

           curated/{cat}/{dataset}/{id}/data.parquet       ← CSV converted to Parquet

Smart skip: before downloading a dataset, the catalog's last_updated is compared
with the value stored in the existing _metadata.json on S3.  If they match the
dataset is skipped entirely — no HTTP requests, no S3 writes.  On a typical day
only 5-15 % of datasets change, so the pipeline runs in ~20-30 min instead of
4+ hours.

The Glue crawler is only triggered when at least one dataset was actually updated.
After all 28 categories are ingested, the Glue Catalog reflects the latest files.
"""

from __future__ import annotations

import asyncio
import io
import logging
from datetime import datetime, timedelta

import boto3
import httpx
import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule

from utils.callbacks import on_dag_failure, on_sla_miss
from utils.converters import csv_to_parquet, excel_to_json
from utils.s3_client import download_json, list_folder_prefixes, upload
from utils.schema_validator import validate_schema

log = logging.getLogger(__name__)

CSV_FORMATS = {"csv"}
EXCEL_FORMATS = {"xls", "xlsx"}
_RETRYABLE_STATUS = {429, 500, 502, 503, 504}
_S3_MULTIPART_THRESHOLD = 8 * 1024 * 1024  # 8 MB — use multipart above this size


def _bucket() -> str:
    return Variable.get("DATA_LAKE_BUCKET")


def _glue_crawler() -> str:
    return Variable.get("GLUE_CRAWLER_NAME")


# ---------------------------------------------------------------------------
# Resource-level processing (called inside async context)
# ---------------------------------------------------------------------------


async def _download_bytes(url: str, timeout: float = 120.0, max_retries: int = 3) -> bytes:
    """Download a URL as bytes with exponential backoff on transient errors."""
    last_exc: Exception | None = None
    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as http:
                resp = await http.get(url)
                if resp.status_code in _RETRYABLE_STATUS:
                    raise httpx.HTTPStatusError(
                        f"HTTP {resp.status_code}", request=resp.request, response=resp
                    )
                resp.raise_for_status()
                return resp.content
        except (httpx.TransportError, httpx.HTTPStatusError) as exc:
            last_exc = exc
            if attempt < max_retries - 1:
                wait = 2**attempt  # 1 s, 2 s, 4 s
                log.warning("Download attempt %d/%d failed (%s) — retrying in %ds", attempt + 1, max_retries, exc, wait)
                await asyncio.sleep(wait)
    raise last_exc  # type: ignore[misc]


async def _stream_to_s3(url: str, bucket: str, key: str, content_type: str = "application/octet-stream") -> int:
    """Stream a URL directly to S3 via multipart upload — never buffers the full file in RAM.

    Returns the total number of bytes uploaded.
    Aborts the multipart upload automatically on any error.
    """
    PART_SIZE = _S3_MULTIPART_THRESHOLD
    s3 = boto3.client("s3")
    mpu = s3.create_multipart_upload(Bucket=bucket, Key=key, ContentType=content_type)
    upload_id: str = mpu["UploadId"]
    parts: list[dict] = []

    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(300.0, connect=10.0), follow_redirects=True
        ) as http:
            async with http.stream("GET", url) as resp:
                resp.raise_for_status()
                buffer = bytearray()
                total_bytes = 0
                part_number = 1

                async for chunk in resp.aiter_bytes(chunk_size=1024 * 1024):
                    buffer.extend(chunk)
                    total_bytes += len(chunk)
                    if len(buffer) >= PART_SIZE:
                        part = s3.upload_part(
                            Bucket=bucket, Key=key,
                            UploadId=upload_id, PartNumber=part_number,
                            Body=bytes(buffer),
                        )
                        parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
                        part_number += 1
                        buffer = bytearray()

                # Last (possibly smaller) part — S3 allows any size for the final part
                if buffer:
                    part = s3.upload_part(
                        Bucket=bucket, Key=key,
                        UploadId=upload_id, PartNumber=part_number,
                        Body=bytes(buffer),
                    )
                    parts.append({"PartNumber": part_number, "ETag": part["ETag"]})

        if not parts:
            # Empty file — abort multipart and upload as empty object
            s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
            s3.put_object(Bucket=bucket, Key=key, Body=b"", ContentType=content_type)
            return 0

        s3.complete_multipart_upload(
            Bucket=bucket, Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
        return total_bytes

    except Exception:
        s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        raise


async def _process_resource(client, resource, category_slug: str, dataset_slug: str, bucket: str) -> None:
    """Download one resource and write raw + curated files to S3."""
    if not resource.download_url:
        return

    fmt = (resource.format or "").lower().strip(".")
    raw_base = f"raw/{category_slug}/{dataset_slug}/resources/{resource.resource_id}"
    curated_prefix = f"curated/{category_slug}/{dataset_slug}/{resource.resource_id}"

    try:
        if fmt in CSV_FORMATS:
            csv_str = await client.get_resource_data(resource)
            upload(bucket, f"{raw_base}.csv", csv_str, "text/csv")

            # Schema validation — compare columns to the stored Parquet before overwriting
            df = pd.read_csv(io.StringIO(csv_str), low_memory=False)
            parquet_key = f"{curated_prefix}/data.parquet"
            valid, reason = validate_schema(list(df.columns), bucket, parquet_key)
            if not valid:
                quarantine_key = f"raw/{category_slug}/{dataset_slug}/quarantine/{resource.resource_id}.csv"
                upload(bucket, quarantine_key, csv_str, "text/csv")
                log.warning("Quarantined %s — %s", resource.resource_id, reason)
            else:
                buf = io.BytesIO()
                df.to_parquet(buf, engine="pyarrow", index=False)
                upload(bucket, parquet_key, buf.getvalue())

        elif fmt in EXCEL_FORMATS:
            # Stream directly to S3 — Excel files can be large
            await _stream_to_s3(resource.download_url, bucket, f"{raw_base}.{fmt}")
            try:
                raw_bytes = await _download_bytes(resource.download_url)
                json_str = excel_to_json(raw_bytes)
                upload(bucket, f"{raw_base}.json", json_str, "application/json")
            except Exception as exc:
                log.warning("Excel→JSON failed for %s: %s", resource.resource_id, exc)

        else:
            # Unknown format — stream to S3 (best effort) + metadata JSON
            try:
                ext = fmt if fmt else "bin"
                await _stream_to_s3(resource.download_url, bucket, f"{raw_base}.{ext}")
            except Exception as exc:
                log.warning("Binary stream failed for %s: %s", resource.resource_id, exc)
            upload(bucket, f"{raw_base}_meta.json", resource.model_dump_json(), "application/json")

    except Exception as exc:
        log.error("resource %s (%s): %s", resource.resource_id, fmt, exc)


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------


@dag(
    dag_id="ingest_datasets",
    schedule="0 7 * * 1-5",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "mex-open-data",
        "retries": 1,
        "retry_delay": timedelta(minutes=15),
        "on_failure_callback": on_dag_failure,
    },
    tags=["ingest", "datos-gob-mx"],
    doc_md=__doc__,
    sla_miss_callback=on_sla_miss,
)
def ingest_datasets():

    @task
    def get_category_slugs() -> list[str]:
        """Read category slugs from the raw/ prefixes written by sync_catalog."""
        prefixes = list_folder_prefixes(_bucket(), "raw/")
        # Each prefix looks like "raw/seguridad/" — extract the slug
        return [p.split("/")[1] for p in prefixes if p.split("/")[1]]

    @task(execution_timeout=timedelta(hours=3), sla=timedelta(hours=2))
    def ingest_category(category_slug: str) -> dict:
        """Download every dataset in *category_slug* and upload to raw + curated.

        Uses a single DatosGobMX client for the whole category so the httpx
        connection pool is reused across all datasets.
        """
        from open_data_mexico import DatosGobMX

        bucket = _bucket()

        async def _run() -> dict:
            catalog = download_json(bucket, f"raw/{category_slug}/_catalog.json")
            dataset_items = catalog["datasets"]
            log.info("[%s] %d datasets in catalog", category_slug, len(dataset_items))

            ok = 0
            skipped = 0
            failed = 0

            async with DatosGobMX(request_delay=0.5, max_retries=3) as client:
                for ds in dataset_items:
                    slug = ds["slug"]
                    catalog_updated = ds.get("last_updated")  # ISO string from catalog
                    meta_key = f"raw/{category_slug}/{slug}/_metadata.json"

                    try:
                        # Smart skip: compare last_updated before making any HTTP request
                        try:
                            stored = download_json(bucket, meta_key)
                            if catalog_updated and stored.get("last_updated") == catalog_updated:
                                skipped += 1
                                continue  # nothing changed — no download, no S3 write
                        except Exception:
                            pass  # _metadata.json doesn't exist yet → process normally

                        detail = await client.get_dataset(slug)
                        if detail is None:
                            log.warning("[%s] %s — not found", category_slug, slug)
                            continue

                        # Store updated metadata
                        upload(bucket, meta_key, detail.model_dump_json(), "application/json")

                        # Process each resource file
                        for resource in detail.resources:
                            await _process_resource(client, resource, category_slug, slug, bucket)

                        ok += 1

                    except Exception as exc:
                        log.error("[%s] %s: %s", category_slug, slug, exc)
                        failed += 1

            log.info("[%s] done — updated=%d, skipped=%d, failed=%d", category_slug, ok, skipped, failed)
            return {"category": category_slug, "ok": ok, "skipped": skipped, "failed": failed}

        return asyncio.run(_run())

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def trigger_glue_crawler(results: list[dict]) -> str:
        """Start the Glue crawler only if at least one dataset was updated today."""
        total_ok = sum(r.get("ok", 0) for r in results)
        total_skipped = sum(r.get("skipped", 0) for r in results)
        total_failed = sum(r.get("failed", 0) for r in results)
        log.info("Ingest complete — updated=%d, skipped=%d, failed=%d", total_ok, total_skipped, total_failed)

        if total_ok == 0:
            log.info("Nothing changed today — Glue crawler skipped")
            return "crawler_skipped:no_changes"

        crawler_name = _glue_crawler()
        boto3.client("glue").start_crawler(Name=crawler_name)
        log.info("Glue crawler '%s' started (%d datasets updated)", crawler_name, total_ok)
        return f"crawler_started:{crawler_name}"

    # Wait for sync_catalog to finish before reading catalogs from S3.
    # mode="reschedule" frees the worker slot while the sensor is poking.
    wait_for_catalog = ExternalTaskSensor(
        task_id="wait_for_catalog",
        external_dag_id="sync_catalog",
        external_task_id=None,  # None = wait for the whole DAG to succeed
        mode="reschedule",
        timeout=3600,           # give up after 1 h if sync_catalog hasn't finished
        poke_interval=60,
    )

    slugs = get_category_slugs()
    results = ingest_category.expand(category_slug=slugs)
    trigger_glue_crawler(results)

    wait_for_catalog >> slugs


ingest_datasets()
