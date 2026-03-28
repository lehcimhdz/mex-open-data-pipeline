# Plan de proyecto — mex-open-data-pipeline

Hoja de ruta para llevar el pipeline de Airflow a un estado profesional, seguro y escalable.

---

## Estado actual

- 2 DAGs funcionales (`sync_catalog`, `ingest_datasets`) con skip inteligente por `last_updated`
- Docker Compose para desarrollo local y script de bootstrap para EC2
- Sin tests, sin CI/CD, sin observabilidad, sin `.gitignore`

---

## 🔴 1 — Seguridad y estabilidad

Sin estos ítems el proyecto no debe ir a producción.

- [x] Crear `.gitignore` — excluir `.env`, `logs/`, `__pycache__/`, `*.pyc`, `.pytest_cache/`, `.venv/`
- [x] Mover password de Airflow a variable de entorno en `.env` (eliminar `admin` hardcodeado en `docker-compose.yml`)
- [x] Añadir retry con exponential backoff en `_download_bytes()` — actualmente un timeout mata toda la tarea
- [x] Reemplazar carga en memoria de archivos grandes por streaming a S3 (multipart upload) para evitar OOM en recursos > 500 MB
- [x] Pin exacto de dependencias en `requirements.txt` (`==` en lugar de `>=`) para builds reproducibles

---

## 🟠 2 — Operabilidad

Para que el pipeline pueda monitorearse y recuperarse sin intervención manual.

- [x] Reemplazar todos los `print()` por `logging.getLogger(__name__)` con niveles INFO/WARNING/ERROR
- [x] Añadir `ExternalTaskSensor` al inicio de `ingest_datasets` para esperar que `sync_catalog` termine correctamente antes de leer el catálogo
- [x] Añadir `max_active_runs=1` en ambos DAGs para evitar solapamiento de ejecuciones
- [x] Configurar SLAs por tarea (`sla=timedelta(minutes=30)` en `sync_catalog`, `sla=timedelta(hours=2)` en `ingest_datasets`)
- [x] Configurar alerta de fallo por email o Slack (`on_failure_callback`) en ambos DAGs (`dags/utils/callbacks.py`)
- [x] Cambiar `trigger_rule` de `trigger_glue_crawler` a `none_failed_min_one_success` para que no falle si alguna categoría individual falla

---

## 🟡 3 — Calidad de código y CI/CD

- [x] Crear `.github/workflows/ci.yml`:
  - `ruff check` y `ruff format --check` en cada push/PR
  - `mypy dags/` para type checking
  - `pytest tests/ --cov` con cobertura mínima del 70 %
  - Validación de sintaxis de DAGs (`python -c "import dags.sync_catalog"`)
- [x] Crear `.pre-commit-config.yaml` con hooks: ruff, mypy, detect-secrets
- [x] Añadir type hints completos en `dags/utils/s3_client.py` y `dags/utils/converters.py`
- [x] Crear `tests/` con pruebas unitarias:
  - `test_converters.py` — CSV vacío, unicode, columnas faltantes, Excel corrupto
  - `test_s3_client.py` — con mocks de boto3
  - `test_dags.py` — estructura del DAG, dependencias entre tareas, ausencia de ciclos
- [x] Crear `pyproject.toml` con configuración de ruff y mypy

---

## 🟢 4 — Madurez y documentación

- [ ] Escribir README completo: arquitectura, prerrequisitos, setup local, variables de Airflow, troubleshooting
- [ ] Crear `Makefile` con: `make lint`, `make test`, `make docker-up`, `make docker-down`, `make logs`
- [ ] Añadir validación de esquema Parquet: comparar columnas del CSV entrante contra esquema esperado; mover archivos inválidos a `raw/{cat}/{slug}/quarantine/`
- [ ] Arreglar `ec2/setup.sh`: reemplazar `sleep 60` por health check real sobre `/health`, añadir `set -euo pipefail`, parametrizar `BUCKET_NAME` como argumento CLI
- [ ] Añadir campo `owner` en los decoradores `@dag()` y docstrings detallados por tarea

---

## Orden de implementación

```
1. Seguridad (1)     → .gitignore, password, retry, streaming, pin deps
2. Operabilidad (2)  → logging, sensor, max_active_runs, SLAs, alertas
3. CI/CD y tests (3) → workflows, pre-commit, tests unitarios, pyproject.toml
4. Madurez (4)       → README, Makefile, schema validation, ec2/setup.sh robusto
```
