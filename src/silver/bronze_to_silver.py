"""Bronze -> Silver.

Silver é a primeira camada "confiável": schema tipado e validado, colunas
renomeadas para snake_case (padrão de consumo), deduplicação e remoção de
registros claramente inválidos. Mantém apenas as colunas exigidas pelo case
(VendorID, passenger_count, total_amount, tpep_pickup_datetime,
tpep_dropoff_datetime) + colunas derivadas de particionamento.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config import LAKE, REQUIRED_COLUMNS
from src.common.spark_session import get_spark_session
from src.common.utils import ensure_schema, get_logger

logger = get_logger(__name__)

_RENAME_MAP = {
    "VendorID": "vendor_id",
    "passenger_count": "passenger_count",
    "total_amount": "total_amount",
    "tpep_pickup_datetime": "tpep_pickup_datetime",
    "tpep_dropoff_datetime": "tpep_dropoff_datetime",
}


def select_required_columns(df: DataFrame) -> DataFrame:
    """Restringe o dataframe às colunas exigidas pela camada de consumo."""
    return df.select(*REQUIRED_COLUMNS)


def rename_to_snake_case(df: DataFrame) -> DataFrame:
    """Padroniza nomenclatura de colunas para snake_case (convenção de
    consumo), evitando misturar convenções (VendorID vs vendor_id) entre
    camadas."""
    for original, renamed in _RENAME_MAP.items():
        df = df.withColumnRenamed(original, renamed)
    return df


def cast_types(df: DataFrame) -> DataFrame:
    """Aplica a tipagem definitiva de cada coluna."""
    return (
        df.withColumn("VendorID", F.col("VendorID").cast("int"))
        .withColumn("passenger_count", F.col("passenger_count").cast("int"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
        .withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast("timestamp"))
        .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast("timestamp"))
    )


def add_partition_columns(df: DataFrame) -> DataFrame:
    """Deriva ano/mês de referência a partir do pickup datetime — é essa
    coluna de negócio (não o nome do arquivo) que deve reger o
    particionamento a partir da Silver em diante."""
    return df.withColumn("trip_year", F.year("tpep_pickup_datetime")).withColumn(
        "trip_month", F.month("tpep_pickup_datetime")
    )


def filter_invalid_records(df: DataFrame) -> DataFrame:
    """Remove registros claramente inconsistentes.

    Regras de qualidade aplicadas (documentadas para auditoria):
    - datas de pickup/dropoff não nulas e dropoff >= pickup;
    - total_amount não nulo e não negativo (retornos/estornos são raros e
      fora do escopo das perguntas de negócio do case);
    - passenger_count não nulo e > 0 (0 passageiros é erro de sensor comum
      neste dataset).
    - registros restritos ao range Jan-Mai/2023, pois arquivos mensais da
      NYC TLC ocasionalmente trazem corridas de meses vizinhos por erro de
      relógio do taxímetro.
    """
    return df.filter(
        F.col("tpep_pickup_datetime").isNotNull()
        & F.col("tpep_dropoff_datetime").isNotNull()
        & (F.col("tpep_dropoff_datetime") >= F.col("tpep_pickup_datetime"))
        & F.col("total_amount").isNotNull()
        & (F.col("total_amount") >= 0)
        & F.col("passenger_count").isNotNull()
        & (F.col("passenger_count") > 0)
        & (F.col("trip_year") == 2023)
        & F.col("trip_month").between(1, 5)
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """Remove duplicatas exatas (mesma corrida ingerida mais de uma vez,
    ex.: reprocessamento acidental da Bronze)."""
    return df.dropDuplicates(
        ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "total_amount", "passenger_count"]
    )


def transform(bronze_df: DataFrame) -> DataFrame:
    """Pipeline de transformação Bronze -> Silver, composto por funções
    puras e testáveis individualmente (facilita unit tests sem subir
    cluster)."""
    return (
        bronze_df.transform(select_required_columns)
        .transform(cast_types)
        .transform(add_partition_columns)
        .transform(filter_invalid_records)
        .transform(deduplicate)
    )


def write_silver(df: DataFrame, table_name: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("trip_year", "trip_month")
        .saveAsTable(table_name)
    )


def run() -> None:
    spark: SparkSession = get_spark_session()
    ensure_schema(spark, LAKE.catalog, LAKE.silver_db)

    bronze_table = LAKE.bronze_table_fqn
    logger.info("Lendo Bronze: %s", bronze_table)
    bronze_df = spark.table(bronze_table)

    silver_df = transform(bronze_df)

    table_name = LAKE.silver_table_fqn
    logger.info("Escrevendo Silver: %s", table_name)
    write_silver(silver_df, table_name)
    logger.info("Silver concluída: %d registros.", silver_df.count())


if __name__ == "__main__":
    run()
