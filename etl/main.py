"""Основной луп скрипта ETL."""
import logging
from time import sleep

from config import settings
from extractor.pg_extract import PostgresExtractor
from loader.elastic_load import ElasticLoader
from logger.log_config import setup_logging
from transformer.pg_to_elastic import PostgresElasticTransformer

setup_logging()

logger = logging.getLogger(__name__)


if __name__ == '__main__':
    logger.info('Скрипт запущен')
    while True:  # noqa: WPS457
        logger.info('Процесс обновления запущен...')
        pg_extractor = PostgresExtractor(chunk_size=settings.chunk_size)
        pg_elastic_transformer = PostgresElasticTransformer()
        elastic_loader = ElasticLoader(
            settings.elastic_url,
            settings.elastic_index,
        )
        elastic_answers = []
        for pg_data in pg_extractor.extract():
            for elastic_data in pg_elastic_transformer.transform(pg_data):
                elastic_answers.extend(elastic_loader.load(elastic_data))
        logger.info(
            'Обновление завершено, ожидаем {0} секунд...'.format(
                settings.request_interval,
            ),
        )
        sleep(settings.request_interval)
