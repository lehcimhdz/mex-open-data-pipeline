#!/usr/bin/env bash
# setup.sh — Bootstrap Airflow on a fresh Ubuntu EC2 instance
#
# Prerequisites:
#   - EC2 instance with the mex-open-data-pipeline-profile IAM instance profile attached
#   - Ubuntu 22.04 LTS
#   - Inbound port 8080 open in the security group (for the Airflow UI)
#
# Usage:
#   chmod +x setup.sh && ./setup.sh
# ---------------------------------------------------------------------------
set -euo pipefail

REPO_URL="https://github.com/lehcimhdz/mex-open-data-pipeline.git"
APP_DIR="/opt/mex-open-data-pipeline"
BUCKET_NAME=""          # Set this — output of: terraform output bucket_name
CRAWLER_NAME="mex-open-data-curated-crawler"

echo "==> Updating system packages"
sudo apt-get update -y
sudo apt-get install -y docker.io docker-compose-plugin git curl

echo "==> Adding ubuntu user to docker group"
sudo usermod -aG docker ubuntu
newgrp docker

echo "==> Cloning pipeline repository"
sudo git clone "$REPO_URL" "$APP_DIR" || (cd "$APP_DIR" && sudo git pull)
sudo chown -R ubuntu:ubuntu "$APP_DIR"
cd "$APP_DIR"

echo "==> Setting up environment file"
cp .env.example .env
sed -i "s/^# AWS_ACCESS_KEY_ID=.*//" .env   # EC2 uses IAM role — no static credentials needed

echo ""
echo "  ┌─────────────────────────────────────────────────────────┐"
echo "  │  Edit $APP_DIR/.env if needed, then press Enter.        │"
echo "  └─────────────────────────────────────────────────────────┘"
read -r

echo "==> Creating runtime directories"
mkdir -p logs plugins

echo "==> Initialising Airflow (db migrate + admin user)"
docker compose up airflow-init

echo "==> Starting Airflow (scheduler + webserver)"
docker compose up -d airflow-scheduler airflow-webserver

echo "==> Waiting for Airflow to become healthy (60 s)"
sleep 60

echo "==> Setting Airflow Variables"
if [[ -z "$BUCKET_NAME" ]]; then
  echo "  [WARN] BUCKET_NAME is empty — set it manually:"
  echo "  docker compose exec airflow-scheduler airflow variables set DATA_LAKE_BUCKET <bucket>"
else
  docker compose exec airflow-scheduler \
    airflow variables set DATA_LAKE_BUCKET "$BUCKET_NAME"
fi

docker compose exec airflow-scheduler \
  airflow variables set GLUE_CRAWLER_NAME "$CRAWLER_NAME"

echo ""
echo "======================================================"
echo "  Airflow is running!"
echo "  UI  : http://$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4):8080"
echo "  User: admin / Password: admin"
echo ""
echo "  Next steps:"
echo "  1. Change the admin password in the Airflow UI."
echo "  2. Unpause the sync_catalog and ingest_datasets DAGs."
echo "  3. Trigger sync_catalog manually for the first run."
echo "======================================================"
