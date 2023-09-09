"""Основной луп скрипта ETL."""
import logging
from time import sleep

from config import settings
from extractor.pg_extract import PostgresExtractor
from loader.elastic_load import ElasticLoader
from transformer.pg_to_elastic import PostgresElasticTransformer

logger = logging.getLogger(__name__)


if __name__ == '__main__':
    logger.info('info started!')
    logger.warning('warning started!')
    logger.error('error started!')
    logger.critical('critical started!')
    while True:  # noqa: WPS457
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
        sleep(10)
