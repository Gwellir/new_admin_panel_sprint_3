import logging
from functools import wraps
from time import sleep
from typing import Callable

from psycopg2 import InterfaceError, OperationalError
from requests import exceptions as exc

logger = logging.getLogger(__name__)


def backoff(
    start_sleep_time: float = 0.1,
    factor: int = 2,
    max_sleep_time: float = 10,
):
    """
    Декоратор для повторного запуска функции при ошибках.

    Использует наивный экспоненциальный рост времени повтора (factor)
    до граничного времени ожидания (max_sleep_time)

    Формула:
        t = start_sleep_time * (factor ^ n), если t < max_sleep_time
        t = max_sleep_time, иначе

    Args:
        start_sleep_time: начальное время ожидания
        factor: во сколько раз нужно увеличивать время ожидания на каждой итерации
        max_sleep_time: максимальное время ожидания
    Returns:
        результат выполнения функции
    """
    def wrapper(func: Callable):
        @wraps(func)
        def inner(*args, **kwargs):
            sleep_time = start_sleep_time
            is_done = False
            while not is_done:
                try:
                    func_result = func(*args, **kwargs)
                    is_done = True
                except (
                    OperationalError,
                    InterfaceError,
                    exc.HTTPError,
                    exc.Timeout,
                    exc.ConnectionError,
                ) as e:
                    logger.warning('Произошла ошибка в функции {0}: {1}'.format(func.__name__, e.__doc__))
                    logger.debug('Сообщение об ошибке: {0}'.format(e.args[0]))
                    logger.warning('Ожидаем {0} секунд'.format(sleep_time))
                    sleep(sleep_time)
                    if sleep_time < max_sleep_time:
                        sleep_time = min(sleep_time * factor, max_sleep_time)

            return func_result

        return inner

    return wrapper
