"""
DAG: load_to_db
===============
Schedule : Mon-Fri at 08:30 UTC (after ingest_datasets finishes)
Purpose  : Read catalog and metadata JSON files from S3 and upsert them
           into the PostgreSQL database used by the FastAPI backend.

           Only rows whose last_updated value has changed are updated —
           unchanged datasets are skipped to keep the run fast.

Upsert strategy:
  categories  — ON CONFLICT (slug) DO UPDATE always (catalog changes are rare)
  datasets    — ON CONFLICT (slug) DO UPDATE only when last_updated differs
  resources   — ON CONFLICT (resource_id) DO UPDATE always (small table)

Airflow Variable required:
  DATABASE_URL — e.g. postgresql://user:pass@host:5432/mex_open_data
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

import boto3
import psycopg2
import psycopg2.extras
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor

from utils.callbacks import on_dag_failure
from utils.s3_client import download_json, list_folder_prefixes

log = logging.getLogger(__name__)


def _bucket() -> str:
    return Variable.get("DATA_LAKE_BUCKET")


def _db_url() -> str:
    return Variable.get("DATABASE_URL")


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _connect():
    return psycopg2.connect(_db_url())


def _upsert_category(cur, slug: str, name: str, description: str | None, updated_at: str | None) -> None:
    cur.execute(
        """
        INSERT INTO categories (slug, name, description, updated_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (slug) DO UPDATE SET
            name        = EXCLUDED.name,
            description = EXCLUDED.description,
            updated_at  = EXCLUDED.updated_at
        """,
        (slug, name, description, updated_at),
    )


def _upsert_dataset(cur, row: dict) -> bool:
    """Upsert dataset. Returns True if the row was inserted/updated."""
    cur.execute(
        """
        INSERT INTO datasets (slug, category_slug, title, description, organization,
                              last_updated, resource_count, ingested_at)
        VALUES (%(slug)s, %(category_slug)s, %(title)s, %(description)s,
                %(organization)s, %(last_updated)s, %(resource_count)s, %(ingested_at)s)
        ON CONFLICT (slug) DO UPDATE SET
            title          = EXCLUDED.title,
            description    = EXCLUDED.description,
            organization   = EXCLUDED.organization,
            last_updated   = EXCLUDED.last_updated,
            resource_count = EXCLUDED.resource_count,
            ingested_at    = EXCLUDED.ingested_at
        WHERE datasets.last_updated IS DISTINCT FROM EXCLUDED.last_updated
        """,
        row,
    )
    return cur.rowcount > 0


def _upsert_resource(cur, row: dict) -> None:
    cur.execute(
        """
        INSERT INTO resources (resource_id, dataset_slug, name, format, download_url)
        VALUES (%(resource_id)s, %(dataset_slug)s, %(name)s, %(format)s, %(download_url)s)
        ON CONFLICT (resource_id) DO UPDATE SET
            name         = EXCLUDED.name,
            format       = EXCLUDED.format,
            download_url = EXCLUDED.download_url
        """,
        row,
    )


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    dag_id="load_to_db",
    schedule="30 8 * * 1-5",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "mex-open-data",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "on_failure_callback": on_dag_failure,
    },
    tags=["load", "postgres", "datos-gob-mx"],
    doc_md=__doc__,
)
def load_to_db():

    wait_for_ingest = ExternalTaskSensor(
        task_id="wait_for_ingest",
        external_dag_id="ingest_datasets",
        external_task_id=None,
        mode="reschedule",
        timeout=3600,
        poke_interval=60,
    )

    @task(execution_timeout=timedelta(hours=2))
    def load_category(category_slug: str) -> dict:
        bucket = _bucket()
        ingested_at = datetime.now(timezone.utc).isoformat()

        catalog = download_json(bucket, f"raw/{category_slug}/_catalog.json")
        cat = catalog["category"]
        dataset_items = catalog["datasets"]

        conn = _connect()
        ok = skipped = failed = 0

        try:
            with conn:
                with conn.cursor() as cur:
                    # Upsert category
                    _upsert_category(
                        cur,
                        slug=category_slug,
                        name=cat.get("name", category_slug),
                        description=cat.get("description"),
                        updated_at=cat.get("updated_at"),
                    )

                    # Update dataset_count on category
                    cur.execute(
                        "UPDATE categories SET dataset_count = %s WHERE slug = %s",
                        (len(dataset_items), category_slug),
                    )

                    for ds in dataset_items:
                        slug = ds["slug"]
                        meta_key = f"raw/{category_slug}/{slug}/_metadata.json"

                        try:
                            meta = download_json(bucket, meta_key)
                        except Exception:
                            skipped += 1
                            continue  # metadata not yet ingested

                        org = meta.get("organization") or {}
                        org_name = org.get("title") or org.get("name") if isinstance(org, dict) else str(org)
                        resources = meta.get("resources", [])

                        updated = _upsert_dataset(cur, {
                            "slug": slug,
                            "category_slug": category_slug,
                            "title": meta.get("title", slug),
                            "description": meta.get("description"),
                            "organization": org_name,
                            "last_updated": meta.get("last_updated"),
                            "resource_count": len(resources),
                            "ingested_at": ingested_at,
                        })

                        if updated:
                            for res in resources:
                                _upsert_resource(cur, {
                                    "resource_id": res.get("resource_id", ""),
                                    "dataset_slug": slug,
                                    "name": res.get("name"),
                                    "format": (res.get("format") or "").lower().strip(".") or None,
                                    "download_url": res.get("download_url"),
                                })
                            ok += 1
                        else:
                            skipped += 1

        finally:
            conn.close()

        log.info("[%s] done — updated=%d, skipped=%d, failed=%d", category_slug, ok, skipped, failed)
        return {"category": category_slug, "ok": ok, "skipped": skipped, "failed": failed}

    @task
    def get_category_slugs() -> list[str]:
        prefixes = list_folder_prefixes(_bucket(), "raw/")
        return [p.split("/")[1] for p in prefixes if p.split("/")[1]]

    @task
    def log_summary(results: list[dict]) -> None:
        total_ok = sum(r.get("ok", 0) for r in results)
        total_skipped = sum(r.get("skipped", 0) for r in results)
        log.info("load_to_db complete — updated=%d, skipped=%d", total_ok, total_skipped)

    slugs = get_category_slugs()
    results = load_category.expand(category_slug=slugs)
    log_summary(results)

    wait_for_ingest >> slugs


load_to_db()
