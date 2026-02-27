import gc
from datetime import datetime
from urllib.parse import parse_qs, urlparse

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from mkpipe.spark.base import BaseLoader
from mkpipe.models import ConnectionConfig, ExtractResult, TableConfig
from mkpipe.utils import get_logger

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

    def load(self, table: TableConfig, data: ExtractResult, spark) -> None:
        target_name = table.target_name
        df = data.df

        if df is None:
            logger.info({'table': target_name, 'status': 'skipped', 'reason': 'no data'})
            return

        if _is_tls_insecure(self.mongo_uri):
            _configure_jvm_tls_insecure(spark)

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
