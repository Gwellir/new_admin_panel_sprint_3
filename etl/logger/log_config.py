"""Модуль с настройками логгера."""

import logging

from config import settings


def setup_logging():
    """Осуществляет настройку системы логирования.

    Устанавливает консольный логгер меньшей подробности и дебажный
    файловый логгер.
    """
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
        datefmt='%m-%d %H:%M:%S',  # noqa: WPS323
        filename=settings.log_file,
        filemode='a',
    )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter(settings.log_format)
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)
