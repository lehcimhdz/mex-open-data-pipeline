#!/usr/bin/env bash
# setup.sh — Bootstrap Airflow on a fresh Ubuntu 22.04 EC2 instance
#
# Prerequisites:
#   - EC2 instance with the mex-open-data-pipeline-profile IAM instance profile attached
#   - Ubuntu 22.04 LTS
#   - Inbound port 8080 open in the security group (for the Airflow UI)
#
# Usage:
#   chmod +x setup.sh
#   ./setup.sh <bucket_name> [crawler_name]
#
# Arguments:
#   bucket_name   — value of `terraform output bucket_name` (required)
#   crawler_name  — value of `terraform output glue_crawler_name`
#                   (default: mex-open-data-curated-crawler)
#
# Example:
#   ./setup.sh mex-open-data-lake-123456789012
# ---------------------------------------------------------------------------
set -euo pipefail

# ---------------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------------
if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <bucket_name> [crawler_name]" >&2
  exit 1
fi

BUCKET_NAME="$1"
CRAWLER_NAME="${2:-mex-open-data-curated-crawler}"
REPO_URL="https://github.com/lehcimhdz/mex-open-data-pipeline.git"
APP_DIR="/opt/mex-open-data-pipeline"

echo "==> Updating system packages"
sudo apt-get update -y
sudo apt-get install -y docker.io docker-compose-plugin git curl python3-pip

echo "==> Installing cryptography (needed to generate Fernet key)"
pip3 install --quiet cryptography

echo "==> Adding ubuntu user to docker group"
sudo usermod -aG docker ubuntu
newgrp docker

echo "==> Cloning pipeline repository"
sudo git clone "$REPO_URL" "$APP_DIR" || (cd "$APP_DIR" && sudo git pull)
sudo chown -R ubuntu:ubuntu "$APP_DIR"
cd "$APP_DIR"

echo "==> Setting up environment file"
cp .env.example .env
# EC2 uses the attached IAM role — no static credentials needed
sed -i '/^# AWS_ACCESS_KEY_ID=/d; /^# AWS_SECRET_ACCESS_KEY=/d' .env

# ── Auto-generate secrets ──────────────────────────────────────────────────
echo "==> Generating AIRFLOW_SECRET_KEY"
SECRET_KEY=$(python3 -c "import secrets; print(secrets.token_hex(32))")
sed -i "s|AIRFLOW_SECRET_KEY=change-me-to-a-random-32-char-hex-string|AIRFLOW_SECRET_KEY=${SECRET_KEY}|" .env

echo "==> Generating AIRFLOW__CORE__FERNET_KEY (encrypts Connections and Variables)"
FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
sed -i "s|AIRFLOW__CORE__FERNET_KEY=change-me-generate-with-fernet|AIRFLOW__CORE__FERNET_KEY=${FERNET_KEY}|" .env

echo ""
echo "  ┌──────────────────────────────────────────────────────────────────┐"
echo "  │  Set AIRFLOW_ADMIN_PASSWORD in $APP_DIR/.env               │"
echo "  │  then press Enter to continue.                                   │"
echo "  └──────────────────────────────────────────────────────────────────┘"
read -r

echo "==> Creating runtime directories"
mkdir -p logs plugins

echo "==> Initialising Airflow (db migrate + admin user)"
docker compose up airflow-init --exit-code-from airflow-init

echo "==> Starting Airflow (scheduler + worker + webserver)"
docker compose up -d airflow-scheduler airflow-worker airflow-webserver

echo "==> Waiting for Airflow webserver to become healthy..."
TIMEOUT=120
ELAPSED=0
INTERVAL=5
until curl -sf http://localhost:8080/health | grep -q '"status": "healthy"'; do
  if [[ $ELAPSED -ge $TIMEOUT ]]; then
    echo "ERROR: Airflow did not become healthy within ${TIMEOUT}s" >&2
    docker compose logs airflow-webserver | tail -30 >&2
    exit 1
  fi
  sleep $INTERVAL
  ELAPSED=$((ELAPSED + INTERVAL))
  echo "  ... waiting (${ELAPSED}s / ${TIMEOUT}s)"
done
echo "  Airflow is healthy."

echo "==> Setting Airflow Variables"
docker compose exec airflow-scheduler \
  airflow variables set DATA_LAKE_BUCKET "$BUCKET_NAME"

docker compose exec airflow-scheduler \
  airflow variables set GLUE_CRAWLER_NAME "$CRAWLER_NAME"

# ── Optional variables ─────────────────────────────────────────────────────
echo ""
read -rp "DATABASE_URL for FastAPI backend (leave blank to skip): " db_url
if [[ -n "$db_url" ]]; then
  docker compose exec airflow-scheduler \
    airflow variables set DATABASE_URL "$db_url"
fi

read -rp "SLACK_WEBHOOK_URL for failure/SLA alerts (leave blank to skip): " slack_url
if [[ -n "$slack_url" ]]; then
  docker compose exec airflow-scheduler \
    airflow variables set SLACK_WEBHOOK_URL "$slack_url"
fi

read -rp "ALERT_EMAIL for email alerts (leave blank to skip): " alert_email
if [[ -n "$alert_email" ]]; then
  docker compose exec airflow-scheduler \
    airflow variables set ALERT_EMAIL "$alert_email"
fi

PUBLIC_IP=$(curl -sf http://169.254.169.254/latest/meta-data/public-ipv4 || echo "unknown")

echo ""
echo "======================================================"
echo "  Airflow is running!"
echo "  UI      : http://${PUBLIC_IP}:8080"
echo "  Bucket  : ${BUCKET_NAME}"
echo "  Crawler : ${CRAWLER_NAME}"
echo "  Worker  : 1 Celery worker (concurrency=4)"
echo ""
echo "  Next steps:"
echo "  1. Unpause all three DAGs in the Airflow UI:"
echo "       sync_catalog  →  ingest_datasets  →  load_to_db"
echo "  2. Trigger sync_catalog manually for the first run."
echo "  3. Scale workers if needed:"
echo "     docker compose up -d --scale airflow-worker=2"
echo "======================================================"
