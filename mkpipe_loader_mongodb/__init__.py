import gc
from datetime import datetime
from typing import List
from urllib.parse import parse_qs, urlparse

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import TimestampType

from mkpipe.exceptions import ConfigError, LoadError
from mkpipe.models import ConnectionConfig, ExtractResult, TableConfig, WriteStrategy
from mkpipe.spark.base import BaseLoader
from mkpipe.spark.columns import add_etl_columns, normalize_column_names
from mkpipe.strategy import resolve_write_strategy
from mkpipe.utils import get_logger

JAR_PACKAGES = ['org.mongodb.spark:mongo-spark-connector_2.13:10.5.0']

logger = get_logger(__name__)

_JVM_TLS_CONFIGURED = False


def _is_tls_insecure(uri: str) -> bool:
    """Check if the MongoDB URI requests insecure TLS."""
    try:
        parsed = urlparse(uri)
        params = parse_qs(parsed.query)
        for key in ('tlsInsecure', 'tlsAllowInvalidCertificates'):
            values = params.get(key, [])
            if any(v.lower() in ('true', '1') for v in values):
                return True
    except Exception:
        logger.debug('Failed to parse MongoDB URI for TLS flags, falling back to substring match')
        uri_lower = uri.lower()
        return 'tlsinsecure=true' in uri_lower or 'tlsallowinvalidcertificates=true' in uri_lower

    return False


def _configure_jvm_tls_insecure(spark) -> None:
    """Install a trust-all SSLContext as the JVM default.

    The MongoDB Spark Connector delegates TLS to the JVM default
    ``SSLContext``.  Even when ``tlsInsecure=true`` is present in the
    connection URI, the connector still uses the JVM default trust
    manager which triggers PKIX errors for self-signed or untrusted
    certificates.

    This calls ``com.mkpipe.ssl.TrustAllManager.install()`` — a small
    helper class bundled in ``mkpipe-tls-helper.jar`` — which replaces
    the JVM-wide default ``SSLContext`` with one that accepts all
    certificates.

    .. warning::
        This affects **all** TLS connections in the JVM, not just MongoDB.
    """
    global _JVM_TLS_CONFIGURED  # noqa: PLW0603
    if _JVM_TLS_CONFIGURED:
        return

    try:
        jvm = spark.sparkContext._jvm
        jvm.com.mkpipe.ssl.TrustAllManager.install()
    except Exception as exc:
        raise RuntimeError(
            'Failed to install trust-all SSLContext. '
            'Ensure mkpipe-tls-helper.jar is on the Spark classpath.'
        ) from exc

    _JVM_TLS_CONFIGURED = True
    logger.warning(
        'Installed trust-all SSLContext — all JVM TLS connections '
        'will skip certificate validation'
    )


class MongoDBLoader(BaseLoader, variant='mongodb'):
    def __init__(self, connection: ConnectionConfig):
        self.connection = connection
        self.mongo_uri = connection.mongo_uri or (
            f'mongodb://{connection.user}:{connection.password}'
            f'@{connection.host}:{connection.port or 27017}/{connection.database}'
        )
        self.database = connection.database

    def _base_writer(self, df: DataFrame, target_name: str):
        writer = (
            df.write.format('mongodb')
            .option('connection.uri', self.mongo_uri)
            .option('database', self.database)
            .option('collection', target_name)
        )
        for key, value in self.connection.extra.items():
            writer = writer.option(key, str(value))
        return writer

    def _append(self, df: DataFrame, target_name: str) -> None:
        self._base_writer(df, target_name).mode('append').save()

    def _replace(self, df: DataFrame, target_name: str) -> None:
        self._base_writer(df, target_name).mode('overwrite').save()

    def _upsert(self, df: DataFrame, target_name: str, write_key: List[str]) -> None:
        # Deduplicate by write_key to prevent E11000 duplicate key errors.
        # dropDuplicates is cheaper than Window+row_number (uses hash-based dedup).
        df = df.dropDuplicates(write_key)

        (
            self._base_writer(df, target_name)
            .option('operationType', 'replace')
            .option('idFieldList', ','.join(write_key))
            .option('ordered', 'false')
            .mode('append')
            .save()
        )

    def _delete_all(self, collection_name: str) -> None:
        from pymongo import MongoClient

        client = MongoClient(self.mongo_uri)
        try:
            db = client[self.database]
            result = db[collection_name].delete_many({})
            logger.info({
                'collection': collection_name,
                'status': 'truncated',
                'deleted_count': result.deleted_count,
            })
        finally:
            client.close()

    def _ensure_index(self, collection_name: str, write_key: List[str]) -> None:
        from pymongo import MongoClient

        client = MongoClient(self.mongo_uri)
        try:
            db = client[self.database]
            coll = db[collection_name]
            index_fields = [(k, 1) for k in write_key]
            coll.create_index(index_fields, unique=True, background=True)
            logger.info({
                'collection': collection_name,
                'status': 'index_ensured',
                'fields': write_key,
            })
        finally:
            client.close()

    def load(self, table: TableConfig, data: ExtractResult, spark) -> None:
        target_name = table.target_name
        df = data.df

        if df is None:
            logger.info({'table': target_name, 'status': 'skipped', 'reason': 'no data'})
            return

        if _is_tls_insecure(self.mongo_uri):
            _configure_jvm_tls_insecure(spark)

        col_name = self.ingested_at_column
        etl_time = datetime.now()
        if table.dedup_columns:
            df = add_etl_columns(
                df, etl_time,
                dedup_columns=table.dedup_columns,
                ingested_at_column=col_name,
                ingestion_id_column=self.ingestion_id_column,
            )
        else:
            if col_name in df.columns:
                df = df.drop(col_name)
            df = df.withColumn(col_name, F.lit(etl_time).cast(TimestampType()))
        df = normalize_column_names(df, self.column_name_case)

        if table.write_partitions:
            df = df.coalesce(table.write_partitions)

        strategy = resolve_write_strategy(table, data)

        # Deprecation warning: dedup_columns without explicit write_strategy
        if table.dedup_columns and table.write_strategy is None:
            logger.warning(
                "Table '%s': dedup_columns is set but write_strategy is not. "
                "Implicit upsert via dedup_columns is deprecated. "
                "Use write_strategy='upsert' with write_key explicitly.",
                target_name,
            )
            # Backward compat: fall back to upsert with ingestion_id_column
            strategy = WriteStrategy.UPSERT
            write_key = [self.ingestion_id_column]
        else:
            write_key = table.write_key

        logger.info({
            'table': target_name,
            'status': 'loading',
            'write_strategy': strategy.value,
        })

        try:
            match strategy:
                case WriteStrategy.APPEND:
                    self._append(df, target_name)
                case WriteStrategy.REPLACE:
                    if self.if_exists == 'append':
                        self._delete_all(target_name)
                        self._append(df, target_name)
                    else:
                        self._replace(df, target_name)
                case WriteStrategy.UPSERT:
                    if not write_key:
                        raise ConfigError(
                            f"write_strategy 'upsert' requires write_key for table '{target_name}'"
                        )
                    self._ensure_index(target_name, write_key)
                    self._upsert(df, target_name, write_key)
                case _:
                    raise ConfigError(
                        f"MongoDB loader does not support write_strategy: {strategy.value}"
                    )
        except (ConfigError, LoadError):
            raise
        except Exception as e:
            raise LoadError(f"Failed to write '{target_name}': {e}") from e

        df.unpersist()
        gc.collect()
        logger.info({'table': target_name, 'status': 'loaded'})
