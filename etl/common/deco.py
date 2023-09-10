import logging
from functools import wraps
from time import sleep
from typing import Callable

default_logger = logging.getLogger(__name__)


def backoff(
    exceptions: [Exception | tuple[Exception]] = Exception,
    start_sleep_time: float = 0.1,
    factor: int = 2,
    max_sleep_time: float = 10,
    logger_func: Callable = default_logger.warning,
):
    """
    Декоратор для повторного запуска функции при ошибках.

    Использует наивный экспоненциальный рост времени повтора (factor)
    до граничного времени ожидания (max_sleep_time)

    Формула:
        t = start_sleep_time * (factor ^ n), если t < max_sleep_time
        t = max_sleep_time, иначе

    Args:
        exceptions: набор исключений для отслеживания, по умолчанию - все
        start_sleep_time: начальное время ожидания
        factor: во сколько раз нужно увеличивать время ожидания на каждой
            итерации
        max_sleep_time: максимальное время ожидания
        logger_func: функция логирования с уровнем, по умолчанию -
            logger.warning этого модуля
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
                except exceptions as e:
                    logger_func(
                        'Произошла ошибка в функции {0}: {1}'.format(
                            func.__name__, e.args[0]
                        )
                    )
                    logger_func('Ожидаем {0} секунд...'.format(sleep_time))
                    sleep(sleep_time)
                    if sleep_time < max_sleep_time:
                        sleep_time = min(sleep_time * factor, max_sleep_time)

            return func_result

        return inner

    return wrapper
