"""Модуль, работающий с БД Postgres."""
import logging
from collections import namedtuple
from datetime import datetime
from typing import Any

from common.deco import backoff
from config import settings
from db import queries
from psycopg2 import InterfaceError, OperationalError, connect, sql
from psycopg2.extras import NamedTupleCursor

logger = logging.getLogger(__name__)


class PostgresClient:
    """Выполняет запросы к БД Postgres и возвращает данные."""

    def __init__(self):
        """Инициализирует подключение к БД и размер блока данных."""
        self._connection = None

    @property
    def connection(self):
        """Возвращает активное подключение к БД Postgres.

        Также восстанавливает его при разрывах.

        Returns:
            Подключение к БД Postgres
        """
        if not self._connection or self._connection.closed:
            self._connection = connect(
                **settings.pg_dsn.dict(),
                cursor_factory=NamedTupleCursor,
            )
        return self._connection

    def close(self):
        """Закрывает подключение к Postgres."""
        self._connection.close()

    @backoff(
        exceptions=(OperationalError, InterfaceError),
        logger_func=logger.warning,
    )
    def get_query_rows(self, query) -> list[namedtuple]:
        """Обращается к БД со сформированным запросом.

        Повторяет обращение, если возникают проблемы с соединением.

        Args:
            query: готовый sql-запрос.

        Returns:
            набор рядов данных, соответствующих ответу сервера БД.
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

        return rows

    def prepare_query(self, pattern: str, **query_params: Any):
        """Подготавливает sql-запрос через метод sql.SQL psycopg2.

        Args:
            pattern: шаблон выражения
            query_params: дополнительные параметры для запроса

        Returns:
            отформатированный SQL-запрос с подставленными параметрами
        """
        return sql.SQL(pattern).format(
            **query_params,
        )


class PostgresQueryWrapper:
    """Передаёт предоформленные запросы к БД Postgres."""

    def __init__(self, chunk_size: int):
        """Инициализирует подключение к клиенту Postgres.

        Args:
            chunk_size: размер блока данных для получения из БД
        """
        self.client = PostgresClient()
        self._chunk_size = chunk_size

    def get_last_modified_time(self, table: str, cross=False) -> datetime:
        """Получает время последней модификации данных в таблице.

        Args:
            table: название таблицы
            cross: является ли таблица кросс-таблицей

        Returns:
            время последнего обновления данных.
        """
        if cross:
            time_field = 'created_at'
        else:
            time_field = 'updated_at'
        query = self.client.prepare_query(
            queries.LAST_MODIFIED_QUERY,
            table=sql.Identifier(table),
            time_field=sql.Identifier(time_field),
        )

        return self.client.get_query_rows(query)[0].updated_at

    def get_ids_after_time(
        self,
        table: str,
        last_modified: datetime,
        cross=False,
    ):
        """Загружает id для обновленных записей в таблице.

        Args:
            table: название таблицы,
            last_modified: время последнего обновления данных,
            cross: таблица является кросс-таблицей

        Returns:
            Ряды table.id, table.updated_at в виде списка именованных кортежей.
        """
        if cross:
            updated_ids_query = queries.UPDATED_CROSS_IDS_QUERY
        else:
            updated_ids_query = queries.UPDATED_IDS_QUERY
        query = self.client.prepare_query(
            updated_ids_query,
            table=sql.Identifier(table),
            modified=sql.Literal(last_modified),
            chunk_size=sql.Literal(self._chunk_size),
        )

        return self.client.get_query_rows(query)

    def get_related_film_work_ids(self, table, ids: list[int]):
        """Загружает связанные id film_work для обновленных записей.

        Args:
            table: название вторичной таблицы
            ids: набор id вторичной таблицы для загрузки данных.

        Returns:
            Ряды fw.id, fw.updated_at в виде списка именованных кортежей.
        """
        query = self.client.prepare_query(
            queries.RELATED_FILM_WORK_QUERY,
            cross_table=sql.Identifier('{0}_film_work'.format(table)),
            cross_id=sql.Identifier('{0}_id'.format(table)),
            ids=sql.SQL(', ').join(sql.Literal(id_) for id_ in ids),
            chunk_size=sql.Literal(self._chunk_size),
        )

        return self.client.get_query_rows(query)

    def get_enriched_rows(self, fw_ids: list[int]):
        """Загружает расширенный набор данных для обновленных записей.

        Args:
            fw_ids: набор id film_work для загрузки данных.

        Returns:
            Ряды данных БД в виде списка именованных кортежей.
        """
        query = self.client.prepare_query(
            queries.ENRICHED_DATA_QUERY,
            ids=sql.SQL(', ').join(sql.Literal(f_id) for f_id in fw_ids),
        )

        return self.client.get_query_rows(query)
