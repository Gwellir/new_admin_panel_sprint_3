"""Модуль с настройками логгера."""

import logging
import sys

from config import settings

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    datefmt='%m-%d %H:%M:%S',  # noqa: WPS323
    filename=settings.log_file,
    filemode='a',
)
console = logging.StreamHandler(stream=sys.stderr)
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)
