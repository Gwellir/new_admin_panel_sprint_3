"""Модуль, отвечающий за конвертацию из формата Postgres в elastic."""

from collections import defaultdict
from typing import Any, Iterator

from frozendict import frozendict

from etl.common.state_processor import State


class PostgresElasticTransformer:
    """Конвертирует данные, полученные от Postgres, в формат Elastic search."""

    def __init__(self):
        """Инициализирует словарь для данных преобразования."""
        self.film_work_data = {}
        self._state = State('pg_to_elastic')

    def transform(
        self,
        bd_data: list[dict[str, Any]],
    ) -> Iterator[list[dict]]:
        """Метод преобразования к формату elastic search.

        Args:
            bd_data: Данные, полученные от БД Postgres.

        Yields:
            Наборы словарей с отформатированными данными для elastic.
        """
        cached_data = self._state.get('data')
        if cached_data:
            yield cached_data

        state_data = self._process_bd_data(bd_data)
        self._state['data'] = state_data

        if state_data:
            yield state_data
        self._state['data'] = None
        self.film_work_data = {}

    def _process_bd_data(
        self,
        bd_data: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Метод строит готовый объект, соответствующий структуре индекса.

        Получается словарь по ключу id записей film_work, содержащий структуры
            для передачи в elastic search.

        Args:
            bd_data: набор рядов данных из pg_extractor.

        Returns:
            Список словарей с данными film_work.
        """
        for record in bd_data:
            film_work_id = record['fw_id']
            if film_work_id not in self.film_work_data:
                self.film_work_data[film_work_id] = defaultdict(set)
                self.film_work_data[film_work_id].update(
                    {
                        'id': record['fw_id'],
                        'title': record['fw_title'],
                        'description': record['fw_description'],
                        'imdb_rating': record['fw_rating'],
                    },
                )

            film_work_entry = self.film_work_data[film_work_id]
            film_work_entry['genre'].add(record['g_genre'])

            person = frozendict(
                id=record['p_id'],
                name=record['p_full_name'],
            )
            if record['p_role'] == 'actor':
                film_work_entry['actors'].add(person)
            elif record['p_role'] == 'writer':
                film_work_entry['writers'].add(person)
            elif record['p_role'] == 'director':
                film_work_entry['director'].add(record['p_full_name'])

        self._prepare()

        new_data = list(self.film_work_data.values())
        self._state['data'] = new_data

        return new_data

    def _prepare(self):
        """Готовит расширенный объект к передаче в elastic search.

        Преобразует и формирует нужные списки к полностью собранным объектам.
        """
        for film_id in self.film_work_data.keys():
            entry = self.film_work_data[film_id]
            entry['genre'] = list(entry['genre'])
            entry['actors'] = list(entry['actors'])
            entry['actors_names'] = [
                actor['name']
                for actor in entry['actors']
            ]
            entry['writers'] = list(entry['writers'])
            entry['writers_names'] = [
                writer['name']
                for writer in entry['writers']
            ]
            entry['director'] = list(entry['director'])
