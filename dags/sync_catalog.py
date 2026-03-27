"""
DAG: sync_catalog
=================
Schedule : Mon-Fri at 06:00 UTC
Purpose  : Fetch all 28 categories and their dataset listings from datos.gob.mx
           and write them to S3 as JSON.  This lightweight DAG produces the
           manifest that ingest_datasets.py consumes one hour later.

S3 output per category:
  raw/{category_slug}/_catalog.json
  {
    "category": { ...Category fields... },
    "datasets": [ {...Dataset fields...}, ... ]
  }
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable

from utils.s3_client import upload  # noqa: E402  (Airflow adds dags/ to sys.path)


def _bucket() -> str:
    return Variable.get("DATA_LAKE_BUCKET")


@dag(
    dag_id="sync_catalog",
    schedule="0 6 * * 1-5",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["catalog", "datos-gob-mx"],
    doc_md=__doc__,
)
def sync_catalog():

    @task
    def fetch_category_slugs() -> list[str]:
        """Return the slug of every category on datos.gob.mx."""
        from open_data_mexico import DatosGobMX  # imported here to keep DAG parsing fast

        async def _run() -> list[str]:
            async with DatosGobMX(request_delay=0.5, max_retries=3) as client:
                cats = await client.get_categories()
            return [c.slug for c in cats]

        return asyncio.run(_run())

    @task
    def fetch_and_store_category(category_slug: str) -> dict:
        """Fetch category metadata + full dataset listing and write to S3."""
        from open_data_mexico import DatosGobMX

        async def _run() -> dict:
            async with DatosGobMX(request_delay=0.5, max_retries=3) as client:
                cat = await client.get_category(category_slug)
                datasets = await client.get_category_datasets(category_slug)
            return {
                "category": cat.model_dump(mode="json"),
                "datasets": [ds.model_dump(mode="json") for ds in datasets],
            }

        payload = asyncio.run(_run())

        upload(
            bucket=_bucket(),
            key=f"raw/{category_slug}/_catalog.json",
            body=json.dumps(payload, ensure_ascii=False, default=str),
            content_type="application/json",
        )

        return {"category_slug": category_slug, "dataset_count": len(payload["datasets"])}

    slugs = fetch_category_slugs()
    fetch_and_store_category.expand(category_slug=slugs)


sync_catalog()
