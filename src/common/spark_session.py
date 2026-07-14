"""Factory de SparkSession com configurações padrão do projeto."""

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "ifood-case-nyc-taxi") -> SparkSession:
    """Retorna (ou reaproveita) uma SparkSession configurada para Delta Lake.

    Em Databricks (Community Edition ou não), `spark` já existe no contexto
    do notebook/job, então `getOrCreate()` simplesmente reaproveita a sessão
    ativa. As configs extras só são aplicadas quando a sessão é criada do zero
    (ex.: execução local/testes).
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    return builder.getOrCreate()
