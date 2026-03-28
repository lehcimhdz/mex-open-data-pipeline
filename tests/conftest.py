"""Pytest configuration — adds dags/ to sys.path so utils can be imported."""
import sys
from pathlib import Path

# Allow `from utils.xxx import yyy` in tests (mirrors the PYTHONPATH in docker-compose)
sys.path.insert(0, str(Path(__file__).parent.parent / "dags"))
