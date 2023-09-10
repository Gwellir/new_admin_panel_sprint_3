"""Модуль, отвечающий за последовательную выгрузку данных из БД Postgres."""
import logging
from collections import OrderedDict
from datetime import datetime
from typing import Any, Iterator, Optional

from common.state_processor import State
from config import settings
from db.postgres import PostgresQueryWrapper

logger = logging.getLogger(__name__)


class PostgresExtractor:  # noqa: WPS214
    """Извлекает свежие данные из БД Postgres."""

    watched_tables: OrderedDict[str, str] = {
        'film_work': 'primary',
        'person': 'related',
        'genre': 'related',
        'person_film_work': 'cross',
        'genre_film_work': 'cross',
    }

    def __init__(self, chunk_size: Optional[int] = None):
        """Инициализирует текущее состояние и подключает адаптер БД.

        Args:
            chunk_size: размер блока данных.
        """
        self._current_modified: Optional[datetime] = None
        self._primary_table = self._get_primary_table()
        self._enriched_data: dict[str, Any] = {}
        self._state = State('pg_extractor')
        self._db = PostgresQueryWrapper(chunk_size)

        if self._state.data:
            self.table_names = list(self.watched_tables.keys())
        else:
            # если мы проводим синхронизацию в первый раз, мы можем обойтись
            # одной основной таблицей и просто записать время последних
            # обновлений для остальных
            self.table_names = [self._primary_table]
            self._update_last_modified_for_skipped()

        self._next_table = {
            self.table_names[i]: self.table_names[i + 1]
            for i in range(len(self.table_names) - 1)
        }
        self._current_table: Optional[str] = self.table_names[0]
        self._init_state()

    def _get_primary_table(self):
        """Возвращает название основной таблицы.

        Raises:
            ValueError: в случае неправильно настройки списка таблиц.

        Returns:
            имя таблицы с маркером 'primary'.
        """
        primary_tables = [
            key
            for key in self.watched_tables.keys()
            if self.watched_tables[key] == 'primary'
        ]
        if len(primary_tables) != 1:
            raise ValueError(
                'watched_tables должны содержать одну primary таблицу!',
            )

        return primary_tables[0]

    @property
    def _last_modified(self):
        """Возвращает last_modified из состояния или иницdиализирует его.

        Returns:
            текущее значение last_modified для таблицы _current_table
        """
        modified_key = 'last_modified_{0}'.format(self._current_table)
        last_timestamp = self._state.get(
            modified_key,
            settings.initial_timestamp,
        )
        return datetime.utcfromtimestamp(last_timestamp)

    @_last_modified.setter
    def _last_modified(self, modified_time):
        modified_key = 'last_modified_{0}'.format(self._current_table)
        self._state[modified_key] = datetime.timestamp(modified_time)

    def extract(self) -> Iterator[list[dict]]:
        """Метод запроса данных из БД.

        Yields:
            Набор данных, соответствующий всем выбранным изменениям.
        """
        while self._current_table:
            # если мы получили enriched data из состояния - сразу
            # пытаемся отдать
            if self._enriched_data:
                yield self._enriched_data
                self._enriched_data = None
                self._state['data'] = None

            updated_fw = set()
            logger.info(
                'Проверяем свежие записи в таблице {0}'.format(
                    self._current_table,
                ),
            )
            updated_fw.update(self._get_table_updates(self._current_table))
            # если в таблице больше нет свежих данных, мы переходим к следующей
            # или None, если таблиц больше нет (это завершает работу extract)
            if not updated_fw:
                logger.debug(
                    'Новых данных нет, переходим к следующей таблице...',
                )
                self._current_table = self._next_table.get(self._current_table)
                continue

            self._enriched_data = [
                record._asdict()  # noqa: WPS437
                for record in updated_fw
            ]

            # после обработки мы сохраняем данные в хранилище и ожидаем
            # корректной отдачи
            self._state['data'] = self._enriched_data
            self._last_modified = self._current_modified

            logger.debug(
                'Отправка новых данных от таблицы {0}, записей: {1}.'.format(
                    self._current_table,
                    len(self._enriched_data),
                ),
            )
            yield self._enriched_data

            self._enriched_data = None
            self._state['data'] = None

        self._reset_state()

    def _reset_state(self):
        """Сбрасывает состояние для последующего повторного использования.

        Сбрасывает ключ current_table, закрывает подключение к postgres
        """
        self._state['current_table'] = None
        # чтобы не переоткрывать подключение каждый чанк данных
        self._db.client.close()

    def _update_last_modified_for_skipped(self):
        """Обновляет параметр last_modified для всех таблиц, кроме основной."""
        table_list = [
            name
            for name in self.watched_tables.keys()
            if name != self._primary_table
        ]
        for table in table_list:
            state_key = 'last_modified_{0}'.format(table)
            modified_time = self._db.get_last_modified_time(
                table,
                cross=self._is_cross_table(table),
            )
            self._state[state_key] = modified_time.timestamp()
        logger.debug(
            'Обновлены данные последних модификаций для таблиц {0}'.format(
                table_list,
            ),
        )

    def _is_cross_table(self, table) -> bool:
        """Возвращает флаг, сообщающий, является ли таблица кросс-таблицей.

        Args:
            table: название таблицы

        Returns:
            Флаг, является ли таблица кросс-таблицей
        """
        return self.watched_tables[table] == 'cross'

    def _init_state(self):
        """Инициализирует нужные данные из состояния."""
        current_table = self._state.get('current_table')
        if current_table:
            self._current_table = current_table
        else:
            self._state['current_table'] = self._current_table

        current_data = self._state.get('data')
        if current_data:
            self._enriched_data = current_data

        logger.debug(
            'Инициализирован pg_extractor: таблица: {0}, данные: {1}'.format(
                current_table,
                current_data is not None,
            ),
        )

    def _get_table_updates(self, table):
        """Функция пытается получить чанк данных из очередной таблицы.

        Получает актуальные записи, привязывает их к записям film_work
        и в конце формирует набор строк для формирования полной информации
        для elastic.

        Args:
            table: название таблицы БД

        Returns:
            полный набор данных для формирования enriched data
        """
        table_rows = self._db.get_ids_after_time(
            table,
            self._last_modified,
            cross=self._is_cross_table(table),
        )
        # если на этом шаге мы не получили id, то можем выходить
        if not table_rows:
            return []
        self._current_modified = table_rows[-1].updated_at
        table_ids = [entry.id for entry in table_rows]

        # в случае related таблицы подтягиваем id film_work через M2M
        # в иных случаях мы уже имеем эти id
        if self.watched_tables[table] == 'related':
            related_fw_rows = self._db.get_related_film_work_ids(
                table,
                table_ids,
            )
            film_work_ids = [film_work.id for film_work in related_fw_rows]
        else:
            film_work_ids = table_ids

        # возвращаем полные записи, соответствующие всей нужной информации
        return self._db.get_enriched_rows(film_work_ids)
