"""Модуль, отвечающий за последовательную выгрузку данных из БД Postgres."""

from datetime import datetime
from typing import Generator, Optional

from etl.common.state_processor import State
from etl.config import settings
from etl.db.postgres import PostgresAdapter


class PostgresExtractor:
    """Извлекает свежие данные из БД Postgres."""

    table_list: list[str] = [
        'person',
        'genre',
        'film_work',
    ]

    def __init__(self, chunk_size: Optional[int] = None):
        """Инициализирует текущее состояние и подключает адаптер БД.

        Args:
            chunk_size: размер блока данных.
        """
        self._last_modified: datetime = settings.INITIAL_TIME
        self._current_table: Optional[str] = self.table_list[0]
        self._state = State('pg_extractor')
        self._init_state()
        self._db = PostgresAdapter(chunk_size)

    def extract(self) -> Generator[list[dict]]:
        """Метод запроса данных из БД.

        Yields:
            Набор данных, соответствующий всем выбранным изменениям.
        """
        next_table = {
            self.table_list[i]: self.table_list[i + 1]
            for i in range(len(self.table_list) - 1)
        }

        while self._current_table:
            updated_fw = set()
            updated_fw.update(self._get_table_updates(self._current_table))
            if not updated_fw:
                self._current_table = next_table.get(self._current_table)
                continue
            updated_fw_list = list(updated_fw)

            enriched_data = [
                record._asdict()  # noqa: WPS437
                for record in updated_fw_list
            ]

            self._state.set_state(
                'data',
                enriched_data,
            )
            self._state.set_state('modified', self._last_modified.timestamp())

            yield enriched_data

    def _init_state(self):
        if self._state.get_state('last_modified'):
            self._last_modified = datetime.fromtimestamp(
                self._state.get_state('last_modified'),
            )
        else:
            self._state.set_state(
                'last_modified',
                self._last_modified.timestamp(),
            )

        if self._state.get_state('current_table'):
            self._current_table = self._state.get_state('current_table')
        else:
            self._state.set_state(
                'current_table',
                self._current_table,
            )

    def _get_table_updates(self, table):

        table_rows = self._db.get_ids_after_time(table, self._last_modified)
        if not table_rows:
            return []
        self._last_modified = table_rows[-1].updated_at
        table_ids = [entry.id for entry in table_rows]

        if table == 'film_work':
            film_work_ids = table_ids
        else:
            related_fw_rows = self._db.get_related_film_work_ids(
                table,
                table_ids,
            )
            film_work_ids = [film_work.id for film_work in related_fw_rows]

        return self._db.get_enriched_data(film_work_ids)
