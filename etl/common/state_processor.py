"""Модуль, отвечающий за работу с состоянием и его хранилищами."""

import abc
import collections
import json
import logging
from typing import Any, Dict, Optional

from config import settings

logger = logging.getLogger(__name__)


class BaseStorage(abc.ABC):
    """Абстрактное хранилище состояния.

    Позволяет сохранять и получать состояние.
    Способ хранения состояния может варьироваться в зависимости
    от итоговой реализации. Например, можно хранить информацию
    в базе данных или в распределённом файловом хранилище.
    """

    @abc.abstractmethod
    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище.

        Args:
            state: текущий словарь состояния
        """

    @abc.abstractmethod
    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""


class JsonFileStorage(BaseStorage):
    """Реализация хранилища, использующего локальный файл.

    Формат хранения: JSON
    """

    def __init__(self, file_path: str) -> None:
        """Инициализирует хранилище путём к json-файлу.

        Args:
            file_path: путь к json-файлу.
        """
        self.file_path = file_path

    def __repr__(self):  # noqa: CCE001
        return '{0}: <{1}>'.format(self.__class__.__name__, self.file_path)

    __str__ = __repr__

    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище.

        Args:
            state: текущий словарь состояния.
        """
        with open(self.file_path, 'w') as json_file:
            json.dump(state, json_file)

    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища.

        Returns:
            текущее сохранённое состояние (пустой словарь при отсутствии файла)
        """
        try:
            with open(self.file_path, 'r') as json_file:
                return json.load(json_file)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}


class State(collections.UserDict):
    """Класс для работы с состояниями.

    Так как функционал практически совпадает со словарём, класс
    максимально приближен к словарю через наследование.
    """

    def __init__(
        self,
        name: str,
        storage: Optional[BaseStorage] = None,
    ) -> None:
        """Задаёт параметры для json storage и инициализирует данные из файла.

        Args:
            name: имя хранилища
            storage: хранилище данных типа Storage.
        """
        super().__init__()
        self.name = name
        if not storage:
            storage = JsonFileStorage(
                settings.storage_dir / '{0}.json'.format(name),
            )
        self.storage = storage
        self.data: dict = self.storage.retrieve_state()  # noqa: WPS110
        logger.debug(
            'Инициализирован класс состояний: {0}, ключей: {1}'.format(
                self,
                len(self.data.keys()),
            ),
        )

    def __repr__(self):
        return '{0}("{1}"): {2}'.format(
            self.__class__.__name__,
            self.name,
            self.storage,
        )

    __str__ = __repr__

    def __setitem__(self, key, value):
        self.set_state(key, value)

    def set_state(self, key: str, value: Any):  # noqa: WPS110
        """Установить состояние для определённого ключа.

        Args:
            key: имя ключа
            value: значение для ключа.
        """
        self.data[key] = value
        self.storage.save_state(self.data)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу.

        Args:
            key: имя ключа.

        Returns:
            значение, отвечающее ключу.
        """
        return self.data.get(key, None)
