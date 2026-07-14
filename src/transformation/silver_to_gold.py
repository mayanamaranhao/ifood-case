"""Silver -> Gold.

Gold contém tabelas de agregados de negócio, já modeladas para consumo
direto (BI, SQL ad-hoc, dashboards) — sem necessidade de o usuário final
escrever joins ou lógica de limpeza. Cada tabela Gold aqui responde
diretamente a uma das perguntas do case.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config import LAKE
from src.common.spark_session import get_spark_session
from src.common.utils import ensure_schema, get_logger

logger = get_logger(__name__)


def avg_total_amount_by_month(silver_df: DataFrame) -> DataFrame:
    """Pergunta 1: média de total_amount por mês, considerando toda a
    frota de yellow táxis."""
    return (
        silver_df.groupBy("trip_year", "trip_month")
        .agg(F.round(F.avg("total_amount"), 2).alias("avg_total_amount"))
        .orderBy("trip_year", "trip_month")
    )


def avg_passenger_count_by_hour_may(silver_df: DataFrame) -> DataFrame:
    """Pergunta 2: média de passenger_count por hora do dia, apenas para
    corridas de maio/2023, considerando toda a frota."""
    return (
        silver_df.filter((F.col("trip_year") == 2023) & (F.col("trip_month") == 5))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .groupBy("pickup_hour")
        .agg(F.round(F.avg("passenger_count"), 2).alias("avg_passenger_count"))
        .orderBy("pickup_hour")
    )


def write_gold(df: DataFrame, table_name: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )


def run() -> None:
    spark: SparkSession = get_spark_session()
    ensure_schema(spark, LAKE.catalog, LAKE.gold_db)

    silver_table = LAKE.silver_table_fqn
    logger.info("Lendo Silver: %s", silver_table)
    silver_df = spark.table(silver_table).cache()

    gold_tables = {
        "avg_total_amount_by_month": avg_total_amount_by_month(silver_df),
        "avg_passenger_count_by_hour_may": avg_passenger_count_by_hour_may(silver_df),
    }

    for table_short_name, gold_df in gold_tables.items():
        table_name = LAKE.gold_table_fqn(table_short_name)
        logger.info("Escrevendo Gold: %s", table_name)
        write_gold(gold_df, table_name)

    silver_df.unpersist()
    logger.info("Gold concluída: %d tabela(s).", len(gold_tables))


if __name__ == "__main__":
    run()
