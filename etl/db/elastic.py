"""Модуль, отвечающий за общение с API elastic search."""

import requests

from etl.common.deco import backoff


class ElasticClient:
    """Выполняет запросы к API Elastic Search."""

    def __init__(self, url: str, index_name: str):
        """Задаёт базовые параметры подключения к API.

        Args:
            url: базовый url elastic search API
            index_name: наименование индекса
        """
        self._url = url
        self._index_name = index_name
        self._headers = {'Content-Type': 'application/json'}

    @backoff()
    def post_bulk(self, data_string: str):
        """Отправляет набор данных в индекс elastic search.

        Args:
            data_string: строка с данными в специальном формате

        Returns:
            Результат обработки запроса (HTTP Response)
        """
        bulk_url = '{0}/_bulk/'.format(self._url)
        return requests.post(
            bulk_url,
            headers=self._headers,
            data=data_string,
            timeout=10,
        )
