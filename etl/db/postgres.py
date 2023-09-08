"""Модуль, работающий с БД Postgres."""
from collections import namedtuple
from datetime import datetime
from typing import Any

from psycopg2 import connect, sql
from psycopg2.extras import NamedTupleCursor

from etl.common.deco import backoff
from etl.config.settings import PG_DSL
from etl.db import queries


class PostgresAdapter:
    """Выполняет запросы к БД Postgres и возвращает данные."""

    def __init__(self, chunk_size: int):
        """Инициализирует подключение к БД и размер блока данных.

        Args:
            chunk_size: размер блока данных.
        """
        self._connection = connect(
            **PG_DSL,
            cursor_factory=NamedTupleCursor,
        )
        self._chunk_size = chunk_size

    def get_ids_after_time(self, table: str, last_modified: datetime):
        """Загружает id для обновленных записей в таблице.

        Args:
            table: название таблицы
            last_modified: время последнего обновления данных.

        Returns:
            Ряды table.id, table.updated_at в виде списка именованных кортежей.
        """
        query = self._prepare_query(
            queries.UPDATED_IDS_QUERY,
            table=sql.Identifier(table),
            modified=sql.Literal(last_modified),
            chunk_size=sql.Literal(self._chunk_size),
        )

        return self._get_query_rows(query)

    def get_related_film_work_ids(self, table, ids: list[int]):
        """Загружает связанные id film_work для обновленных записей.

        Args:
            table: название вторичной таблицы
            ids: набор id вторичной таблицы для загрузки данных.

        Returns:
            Ряды fw.id, fw.updated_at в виде списка именованных кортежей.
        """
        query = self._prepare_query(
            queries.RELATED_FILM_WORK_QUERY,
            cross_table=sql.Identifier('{0}_film_work'.format(table)),
            cross_id=sql.Identifier('{0}_id'.format(table)),
            ids=sql.SQL(', ').join(sql.Literal(id_) for id_ in ids),
            chunk_size=sql.Literal(self._chunk_size),
        )

        return self._get_query_rows(query)

    def get_enriched_data(self, fw_ids: list[int]):
        """Загружает расширенный набор данных для обновленных записей.

        Args:
            fw_ids: набор id film_work для загрузки данных.

        Returns:
            Ряды данных БД в виде списка именованных кортежей.
        """
        query = self._prepare_query(
            queries.ENRICHED_DATA_QUERY,
            ids=sql.SQL(', ').join(sql.Literal(f_id) for f_id in fw_ids),
        )

        return self._get_query_rows(query)

    @backoff()
    def _get_query_rows(self, query) -> list[namedtuple]:
        with self._connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    def _prepare_query(self, pattern: str, **query_params: Any):
        return sql.SQL(pattern).format(
            **query_params,
        )
