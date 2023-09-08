"""Основной луп скрипта ETL."""

from config import settings
from extractor.pg_extract import PostgresExtractor
from loader.elastic_load import ElasticLoader
from transformer.pg_to_elastic import PostgresElasticTransformer

if __name__ == '__main__':
    pg_extractor = PostgresExtractor(chunk_size=settings.CHUNK_SIZE)
    pg_elastic_transformer = PostgresElasticTransformer()
    elastic_loader = ElasticLoader()
    for pg_data in pg_extractor.extract():
        elastic_data = pg_elastic_transformer.transform(pg_data)
        elastic_loader.load(elastic_data)
