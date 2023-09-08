"""Модуль, отвечающий за загрузку данных в elastic search."""

from typing import Any


class ElasticLoader:
    """Загружает чанк данных в Elastic Search."""

    def load(self, elastic_data: list[dict[str, Any]]) -> str:
        """Метод пакетной загрузки в индекс elastic search.

        Args:
            elastic_data: данные, подготовленные к загрузке в elastic search.
        """
