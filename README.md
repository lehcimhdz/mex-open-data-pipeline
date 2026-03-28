# mex-open-data-pipeline

Airflow 2.9 pipeline that ingests all datasets from [datos.gob.mx](https://datos.gob.mx) into an AWS S3 data lake daily. Uses the [open-data-mexico](https://pypi.org/project/open-data-mexico/) library as the API client.

Infrastructure (S3, IAM, Glue, Athena) is provisioned separately in [mex-open-data-aws-s3](https://github.com/lehcimhdz/mex-open-data-aws-s3).

---

## Architecture

```
datos.gob.mx API
      │
      ▼
┌─────────────────────────────────────────────────────────────┐
│  DAG 1: sync_catalog  (Mon-Fri 06:00 UTC)                   │
│                                                             │
│  get_categories() ──► [per category, parallel]              │
│    get_category() + get_category_datasets()                 │
│    └─► S3: raw/{cat}/_catalog.json                          │
└─────────────────────────────────────────────────────────────┘
      │
      │  ExternalTaskSensor (waits for sync_catalog)
      ▼
┌─────────────────────────────────────────────────────────────┐
│  DAG 2: ingest_datasets  (Mon-Fri 07:00 UTC)                │
│                                                             │
│  per category (28 parallel tasks):                          │
│    read S3 catalog ──► for each dataset:                    │
│      smart skip: compare last_updated vs stored metadata    │
│      if changed:                                            │
│        get_dataset() ──► S3: raw/{cat}/{ds}/_metadata.json  │
│        per resource:                                        │
│          CSV  ──► raw/.../resources/{id}.csv                │
│                ──► curated/{cat}/{ds}/{id}/data.parquet     │
│          Excel ──► raw/.../resources/{id}.xlsx (stream)     │
│                ──► raw/.../resources/{id}.json              │
│          other ──► raw/.../resources/{id}.{ext} (stream)    │
│                                                             │
│  trigger_glue_crawler (only if any dataset changed)         │
└─────────────────────────────────────────────────────────────┘
      │
      ▼
   Glue Catalog ──► Athena queries
```

**Smart skip:** `ingest_datasets` compares each dataset's `last_updated` field against the value stored in `_metadata.json` on S3. On a typical day only 5–15% of datasets change, reducing runtime from ~4 hours to ~25 minutes.

---

## DAGs

| DAG | Schedule | Purpose | SLA |
|-----|----------|---------|-----|
| `sync_catalog` | Mon-Fri 06:00 UTC | Fetch category + dataset listings → S3 | 30 min |
| `ingest_datasets` | Mon-Fri 07:00 UTC | Download resources, convert to Parquet | 2 h |

---

## S3 layout

```
raw/
  {category}/
    _catalog.json                        ← dataset listing (from sync_catalog)
    {dataset}/
      _metadata.json                     ← DatasetDetail JSON
      resources/
        {resource_id}.csv
        {resource_id}.xlsx
        {resource_id}.json               ← Excel→JSON or metadata for other formats
        {resource_id}.{ext}              ← other binary files

curated/
  {category}/
    {dataset}/
      {resource_id}/
        data.parquet                     ← CSV converted to Parquet (queryable via Athena)
```

---

## Prerequisites

- Docker and Docker Compose v2
- AWS account with the pipeline IAM role (from `mex-open-data-aws-s3`) — or static credentials with S3 + Glue access
- The S3 bucket and Glue crawler names from `terraform output` in `mex-open-data-aws-s3`

---

## Local setup

```bash
# 1. Clone
git clone https://github.com/lehcimhdz/mex-open-data-pipeline.git
cd mex-open-data-pipeline

# 2. Configure environment
cp .env.example .env
# Edit .env — fill in AIRFLOW_SECRET_KEY and AIRFLOW_ADMIN_PASSWORD at minimum
# Generate a secret key:
python3 -c "import secrets; print(secrets.token_hex(32))"

# 3. Initialise Airflow (runs db migrate + creates admin user)
docker compose up airflow-init

# 4. Start scheduler and webserver
docker compose up -d airflow-scheduler airflow-webserver

# 5. Set Airflow Variables (bucket name and crawler name from terraform output)
docker compose exec airflow-scheduler \
  airflow variables set DATA_LAKE_BUCKET mex-open-data-lake-123456789012

docker compose exec airflow-scheduler \
  airflow variables set GLUE_CRAWLER_NAME mex-open-data-curated-crawler

# Optional — enable email alerts (requires SMTP config in .env)
docker compose exec airflow-scheduler \
  airflow variables set ALERT_EMAIL you@example.com
```

Open the Airflow UI at **http://localhost:8080**, then unpause both DAGs.

---

## Airflow Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DATA_LAKE_BUCKET` | Yes | S3 bucket name — from `terraform output bucket_name` |
| `GLUE_CRAWLER_NAME` | Yes | Glue crawler name — from `terraform output glue_crawler_name` |
| `ALERT_EMAIL` | No | Email for failure alerts (requires SMTP configured) |

---

## Environment variables (.env)

| Variable | Description |
|----------|-------------|
| `AIRFLOW_UID` | UID for the Airflow process (default `50000`) |
| `AIRFLOW_SECRET_KEY` | Webserver secret key — generate with `secrets.token_hex(32)` |
| `AIRFLOW_ADMIN_USER` | Admin username (default `admin`) |
| `AIRFLOW_ADMIN_PASSWORD` | Admin password — **required**, no default |
| `AWS_DEFAULT_REGION` | AWS region (default `mx-central-1`) |
| `AWS_ACCESS_KEY_ID` | Only needed for local runs without an EC2 IAM role |
| `AWS_SECRET_ACCESS_KEY` | Only needed for local runs without an EC2 IAM role |

---

## EC2 production setup

The `ec2/setup.sh` script bootstraps a fresh Ubuntu 22.04 instance with Docker and starts Airflow automatically.

**Prerequisites:** EC2 instance with the `mex-open-data-pipeline-profile` IAM instance profile attached and port 8080 open in the security group.

```bash
# On the EC2 instance
chmod +x ec2/setup.sh
# Edit BUCKET_NAME at the top of setup.sh first
./ec2/setup.sh
```

The script installs Docker, clones this repo, runs `airflow-init`, and starts the scheduler and webserver. It prints the public IP at the end.

---

## Failure alerts

`dags/utils/callbacks.py` provides `on_dag_failure`, registered on both DAGs via `default_args`. On failure it:

1. Logs a structured error with DAG, task, run ID, and exception
2. Sends an email to the `ALERT_EMAIL` Airflow Variable if set (requires SMTP in `.env`)

To add **Slack** alerts, install `apache-airflow-providers-slack`, create a `slack_default` connection, and uncomment the `SlackWebhookHook` block in `callbacks.py`.

---

## Dependencies

Pinned in `requirements.txt`. Update via:

```bash
pip install pip-tools
pip-compile requirements.in --output-file requirements.txt
```

| Package | Purpose |
|---------|---------|
| `open-data-mexico` | datos.gob.mx async API client |
| `boto3` | S3 uploads and Glue trigger |
| `pandas` + `pyarrow` | CSV → Parquet conversion |
| `openpyxl` | Excel parsing |

---

## Troubleshooting

**`ingest_datasets` starts before `sync_catalog` finishes**
The `wait_for_catalog` ExternalTaskSensor polls every 60 s and times out after 1 h. If `sync_catalog` is not finishing, check for failures in that DAG first.

**Task fails with `NoCredentialsError`**
On EC2: verify the IAM instance profile is attached to the instance.
Local: set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in `.env`.

**`DATA_LAKE_BUCKET` or `GLUE_CRAWLER_NAME` variable not found**
Set them via `docker compose exec airflow-scheduler airflow variables set ...` as shown in the setup steps.

**Glue crawler not triggered despite datasets being updated**
Check the `trigger_glue_crawler` task log. If `total_ok == 0`, all datasets were skipped (nothing changed). If there's a Glue API error, verify the IAM role has `glue:StartCrawler` on `arn:aws:glue:*:*:crawler/mex-open-data-*`.

**Excel → JSON conversion fails for some resources**
This is non-fatal. The raw `.xlsx` is always uploaded first; the JSON conversion failure is logged as a warning and the pipeline continues.

**OOM on large binary files**
Large files (>8 MB) are streamed via multipart upload and never fully buffered in RAM. If OOM still occurs, reduce the `_S3_MULTIPART_THRESHOLD` constant in `ingest_datasets.py`.
