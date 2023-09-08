"""Основные настройки проекта ETL."""

import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from frozendict import frozendict

load_dotenv()

CHUNK_SIZE = 100
INITIAL_TIME = datetime(2000, 1, 1, 0, 0, 0)  # noqa: WPS432

BASE_DIR = Path(__file__).resolve().parent.parent
STORAGE_DIR = BASE_DIR / 'storage/'
PG_DSL = frozendict({
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT'),
})
