"""Модуль, отвечающий за конвертацию из формата Postgres в elastic."""

from collections import defaultdict
from typing import Any

from frozendict import frozendict


class PostgresElasticTransformer:
    """Конвертирует данные, полученные от Postgres, в формат Elastic search."""

    def __init__(self):
        """Инициализирует словарь для данных преобразования."""
        self.film_work_data = {}

    def transform(self, bd_data) -> list[dict[str, Any]]:
        """Метод преобразования к формату elastic search.

        Args:
            bd_data: Данные, полученные от БД Postgres.
        """
        for record in bd_data:
            film_work_id = record.fw_id
            if film_work_id not in self.film_work_data:
                self.film_work_data[film_work_id] = defaultdict(set)
                self.film_work_data[film_work_id]['film_work'] = (
                    record.fw_id,
                    record.fw_title,
                    record.fw_description,
                    record.fw_rating,
                    record.fw_type,
                )
            film_work_entry = self.film_work_data[film_work_id]
            film_work_entry['genres'].add(record.g_genre)
            person = frozendict(
                id=record.p_id,
                name=record.p_full_name,
            )
            if record.p_role == 'actor':
                film_work_entry['actors'].add(person)
            elif record.p_role == 'writer':
                film_work_entry['writers'].add(person)
            elif record.p_role == 'director':
                film_work_entry['director'] = record.p_full_name

        self.prepare()

    def prepare(self):
        """Готовит расширенный объект к передаче в elastic search."""
        for film_id in self.film_work_data.items():
            entry = self.film_work_data[film_id]
            actors = entry['actors']
            actors = list(actors)
            entry['actors_names'] = ', '.join(
                actor['name']
                for actor in actors
            )
            writers = entry['writers']
            writers = list(writers)
            entry['writers_names'] = ', '.join(
                writer['name']
                for writer in writers
            )
