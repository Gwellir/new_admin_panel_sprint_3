"""Основные настройки проекта ETL."""

from pathlib import Path

from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class PostgresSettings(BaseModel):
    """Описывает настройки подключения к БД Postgres."""

    dbname: str
    user: str
    password: str
    host: str
    port: int


class Settings(BaseSettings):
    """Описывает основные настройки приложения."""

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        env_nested_delimiter='__',
    )

    initial_timestamp: float
    storage_subdir: str
    chunk_size: int = 100

    elastic_url: str
    elastic_index: str

    log_file: str

    pg_dsn: PostgresSettings

    base_dir: Path = Path(__file__).resolve().parent

    @property
    def storage_dir(self) -> Path:
        """Возвращает директорию для хранения файлов состояния.

        Returns:
            сформированную директорию для файлов состояния
        """
        return self.base_dir / self.storage_subdir


settings = Settings()
