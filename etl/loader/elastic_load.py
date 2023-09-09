"""Модуль, отвечающий за загрузку данных в elastic search."""
import json
import logging
from typing import Any

from common.state_processor import State
from db.elastic import ElasticClient

logger = logging.getLogger(__name__)


class ElasticLoader:
    """Загружает чанк данных в Elastic Search."""

    def __init__(self, url: str, index_name: str):
        """Настраивает параметры работы с elastic.

        Args:
            url: базовый адрес доступа к elastic search API
            index_name: наименование индекса для сохранения данных
        """
        self.elastic = ElasticClient(url, index_name)
        self._index_name = index_name
        self._state = State('elastic_load')

    def load(self, elastic_data: list[dict[str, Any]]) -> list[str]:
        """Метод пакетной загрузки в индекс elastic search.

        Args:
            elastic_data: данные, подготовленные к загрузке в elastic search.

        Returns:
            Ответы системы elastic search на размещение данных в индексе.
        """
        answers = []

        cached_data = self._state.get('data')
        if cached_data:
            answers.append(self._send_data(cached_data))

        self._state['data'] = elastic_data
        answers.append(self._send_data(elastic_data))
        self._state['data'] = None

        return answers

    def _send_data(self, elastic_data):
        bulk_string = self._get_bulk_body(elastic_data)
        answer = self.elastic.post_bulk(bulk_string)
        logger.info(
            'Отправлено в elastic: код {0}, размер {1}, ошибки {2}'.format(
                answer, len(elastic_data), answer.json().get('errors'),
            ),
        )
        return answer

    def _get_bulk_body(self, data_object: list[dict]) -> str:
        return '\n'.join(
            [
                self._wrap_bulk_entry(entry)
                for entry in data_object
            ],
        ) + '\n'

    def _wrap_bulk_entry(self, entry):
        return (
            '{{"index": {{"_index": "{index}", "_id": "{entry_id}"}}}}\n'
            + '{entry_json}'
        ).format(
            index=self._index_name,
            entry_id=entry.get('id'),
            entry_json=json.dumps(entry),
        )
