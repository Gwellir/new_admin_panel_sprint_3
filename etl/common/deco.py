import logging
from functools import wraps
from time import sleep
from typing import Callable

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
            func_result = None
            while not func_result:
                try:
                    func_result = func(*args, **kwargs)
                except Exception as e:
                    logger.warning('Exc in {}: {}'.format(func.__name__, e.args))
                    sleep(sleep_time)
                    if sleep_time < max_sleep_time:
                        sleep_time = max(sleep_time * factor, max_sleep_time)

            return func_result

        return inner

    return wrapper
