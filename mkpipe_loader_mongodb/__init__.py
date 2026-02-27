import gc
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from mkpipe.spark.base import BaseLoader
from mkpipe.models import ConnectionConfig, ExtractResult, TableConfig
from mkpipe.utils import get_logger

logger = get_logger(__name__)


class MongoDBLoader(BaseLoader, variant='mongodb'):
    def __init__(self, connection: ConnectionConfig):
        self.connection = connection
        self.mongo_uri = connection.mongo_uri or (
            f'mongodb://{connection.user}:{connection.password}'
            f'@{connection.host}:{connection.port or 27017}/{connection.database}'
        )
        self.database = connection.database

    def load(self, table: TableConfig, data: ExtractResult, spark) -> None:
        target_name = table.target_name
        df = data.df

        if df is None:
            logger.info({'table': target_name, 'status': 'skipped', 'reason': 'no data'})
            return

        etl_time = datetime.now()
        if 'etl_time' in df.columns:
            df = df.drop('etl_time')
        df = df.withColumn('etl_time', F.lit(etl_time).cast(TimestampType()))

        logger.info({'table': target_name, 'status': 'loading'})

        (
            df.write.format('mongodb')
            .option('connection.uri', self.mongo_uri)
            .option('database', self.database)
            .option('collection', target_name)
            .mode(data.write_mode)
            .save()
        )

        df.unpersist()
        gc.collect()
        logger.info({'table': target_name, 'status': 'loaded'})
