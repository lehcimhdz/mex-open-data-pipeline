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
from datetime import datetime, timedelta

import boto3
import httpx
from airflow.decorators import dag, task
from airflow.models import Variable

from utils.converters import csv_to_parquet, excel_to_json
from utils.s3_client import download_json, list_folder_prefixes, upload

CSV_FORMATS = {"csv"}
EXCEL_FORMATS = {"xls", "xlsx"}


def _bucket() -> str:
    return Variable.get("DATA_LAKE_BUCKET")


def _glue_crawler() -> str:
    return Variable.get("GLUE_CRAWLER_NAME")


# ---------------------------------------------------------------------------
# Resource-level processing (called inside async context)
# ---------------------------------------------------------------------------


async def _download_bytes(url: str, timeout: float = 120.0) -> bytes:
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as http:
        resp = await http.get(url)
        resp.raise_for_status()
        return resp.content


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

            parquet_bytes = csv_to_parquet(csv_str)
            upload(bucket, f"{curated_prefix}/data.parquet", parquet_bytes)

        elif fmt in EXCEL_FORMATS:
            raw_bytes = await _download_bytes(resource.download_url)
            upload(bucket, f"{raw_base}.{fmt}", raw_bytes)

            try:
                json_str = excel_to_json(raw_bytes)
                upload(bucket, f"{raw_base}.json", json_str, "application/json")
            except Exception as exc:
                print(f"    [WARN] Excel→JSON failed for {resource.resource_id}: {exc}")

        else:
            # Unknown format — store raw bytes (best effort) + metadata JSON
            try:
                raw_bytes = await _download_bytes(resource.download_url)
                ext = fmt if fmt else "bin"
                upload(bucket, f"{raw_base}.{ext}", raw_bytes)
            except Exception as exc:
                print(f"    [WARN] Binary download failed for {resource.resource_id}: {exc}")

            meta_json = resource.model_dump_json()
            upload(bucket, f"{raw_base}_meta.json", meta_json, "application/json")

    except Exception as exc:
        print(f"    [ERROR] resource {resource.resource_id} ({fmt}): {exc}")


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------


@dag(
    dag_id="ingest_datasets",
    schedule="0 7 * * 1-5",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=15),
    },
    tags=["ingest", "datos-gob-mx"],
    doc_md=__doc__,
)
def ingest_datasets():

    @task
    def get_category_slugs() -> list[str]:
        """Read category slugs from the raw/ prefixes written by sync_catalog."""
        prefixes = list_folder_prefixes(_bucket(), "raw/")
        # Each prefix looks like "raw/seguridad/" — extract the slug
        return [p.split("/")[1] for p in prefixes if p.split("/")[1]]

    @task(execution_timeout=timedelta(hours=3))
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
            print(f"[{category_slug}] {len(dataset_items)} datasets in catalog")

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
                            print(f"  [SKIP] {slug} — not found")
                            continue

                        # Store updated metadata
                        upload(bucket, meta_key, detail.model_dump_json(), "application/json")

                        # Process each resource file
                        for resource in detail.resources:
                            await _process_resource(client, resource, category_slug, slug, bucket)

                        ok += 1

                    except Exception as exc:
                        print(f"  [ERROR] {category_slug}/{slug}: {exc}")
                        failed += 1

            print(f"[{category_slug}] done — updated={ok}, skipped={skipped}, failed={failed}")
            return {"category": category_slug, "ok": ok, "skipped": skipped, "failed": failed}

        return asyncio.run(_run())

    @task
    def trigger_glue_crawler(results: list[dict]) -> str:
        """Start the Glue crawler only if at least one dataset was updated today."""
        total_ok = sum(r.get("ok", 0) for r in results)
        total_skipped = sum(r.get("skipped", 0) for r in results)
        total_failed = sum(r.get("failed", 0) for r in results)
        print(f"Ingest complete — updated={total_ok}, skipped={total_skipped}, failed={total_failed}")

        if total_ok == 0:
            print("Nothing changed today — Glue crawler skipped")
            return "crawler_skipped:no_changes"

        crawler_name = _glue_crawler()
        boto3.client("glue").start_crawler(Name=crawler_name)
        print(f"Glue crawler '{crawler_name}' started ({total_ok} datasets updated)")
        return f"crawler_started:{crawler_name}"

    slugs = get_category_slugs()
    results = ingest_category.expand(category_slug=slugs)
    trigger_glue_crawler(results)


ingest_datasets()
