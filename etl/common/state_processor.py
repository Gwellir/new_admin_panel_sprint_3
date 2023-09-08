"""Модуль, отвечающий за работу с состоянием и его хранилищами."""

import abc
import json
from typing import Any, Dict, Optional

from etl.config.settings import STORAGE_DIR


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


class State:
    """Класс для работы с состояниями."""

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
        self.name = name
        if not storage:
            storage = JsonFileStorage(STORAGE_DIR / '{0}.json'.format(name))
        self.storage = storage
        self.current_state = self.storage.retrieve_state()

    def set_state(self, key: str, value: Any) -> Any:  # noqa: WPS110
        """Установить состояние для определённого ключа.

        Args:
            key: имя ключа
            value: значение для ключа.

        Returns:
            устанавливаемое значение.
        """
        self.current_state[key] = value
        self.storage.save_state(self.current_state)
        return value

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу.

        Args:
            key: имя ключа.

        Returns:
            значение, отвечающее ключу.
        """
        return self.current_state.get(key, None)
