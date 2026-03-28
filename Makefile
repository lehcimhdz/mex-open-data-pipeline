.PHONY: help lint test docker-up docker-down logs shell set-vars

help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@echo "  lint        Run ruff (check + format) and mypy"
	@echo "  test        Run pytest with coverage (≥70%)"
	@echo "  docker-up   Start Airflow scheduler + webserver (detached)"
	@echo "  docker-down Stop and remove containers"
	@echo "  logs        Tail logs for scheduler and webserver"
	@echo "  shell       Open a bash shell in the running scheduler"
	@echo "  set-vars    Set required Airflow Variables (prompts for values)"

lint:
	ruff check dags/
	ruff format --check dags/
	mypy dags/ --ignore-missing-imports

test:
	pytest tests/ --cov=dags/utils --cov-report=term-missing --cov-fail-under=70

docker-up:
	docker compose up -d airflow-scheduler airflow-webserver

docker-down:
	docker compose down

logs:
	docker compose logs -f airflow-scheduler airflow-webserver

shell:
	docker compose exec airflow-scheduler bash

set-vars:
	@read -p "DATA_LAKE_BUCKET: " bucket; \
	docker compose exec airflow-scheduler airflow variables set DATA_LAKE_BUCKET "$$bucket"
	@read -p "GLUE_CRAWLER_NAME [mex-open-data-curated-crawler]: " crawler; \
	crawler=$${crawler:-mex-open-data-curated-crawler}; \
	docker compose exec airflow-scheduler airflow variables set GLUE_CRAWLER_NAME "$$crawler"
	@read -p "ALERT_EMAIL (leave blank to skip): " email; \
	if [ -n "$$email" ]; then \
	  docker compose exec airflow-scheduler airflow variables set ALERT_EMAIL "$$email"; \
	fi
