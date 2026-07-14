"""Landing -> Bronze.

Bronze é a cópia fiel do dado de origem, apenas convertido para um formato
transacional (Delta) e com metadados técnicos de rastreabilidade
(arquivo de origem, timestamp de ingestão). Nenhuma regra de negócio ou
limpeza é aplicada aqui — isso é o que garante que, se uma regra de
limpeza da Silver estiver errada, o dado original ainda está intacto
para reprocessamento.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config import LAKE
from src.common.spark_session import get_spark_session
from src.common.utils import ensure_schema, get_logger

logger = get_logger(__name__)


def read_landing(spark: SparkSession, landing_path: str) -> DataFrame:
    """Lê os arquivos parquet da landing zone com schema inferido.

    Schema inferido é aceitável aqui porque a Bronze é propositalmente
    "schema-on-read" e tolerante a variações — a tipagem rígida entra
    apenas na Silver.
    """
    return spark.read.parquet(landing_path)


def add_technical_metadata(df: DataFrame) -> DataFrame:
    """Enriquece o dataframe com colunas técnicas de auditoria/linhagem.

    `_ref_year`/`_ref_month` são extraídos do nome do arquivo de origem
    (ex.: yellow_tripdata_2023-05.parquet) e usados apenas para
    particionamento físico da Bronze — não são regra de negócio, apenas
    otimização de leitura/reprocessamento incremental por mês.
    """
    source_file = F.input_file_name()
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", source_file)
        .withColumn("_ref_year", F.regexp_extract(source_file, r"(\d{4})-(\d{2})", 1).cast("int"))
        .withColumn("_ref_month", F.regexp_extract(source_file, r"(\d{4})-(\d{2})", 2).cast("int"))
    )


def write_bronze(df: DataFrame, table_name: str) -> None:
    """Persiste a Bronze como tabela Delta **gerenciada** pelo Unity
    Catalog (sem `path` explícito — a Free Edition não permite apontar
    para locations externas arbitrárias sem um External Location
    configurado, então deixamos o UC gerenciar o storage).
    Particionada por ano/mês de referência do arquivo de origem."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("_ref_year", "_ref_month")
        .saveAsTable(table_name)
    )


def run() -> None:
    spark = get_spark_session()
    ensure_schema(spark, LAKE.catalog, LAKE.bronze_db)

    logger.info("Lendo landing zone: %s", LAKE.landing_path)
    raw_df = read_landing(spark, LAKE.landing_path)
    enriched_df = add_technical_metadata(raw_df)

    table_name = LAKE.bronze_table_fqn
    logger.info("Escrevendo Bronze: %s", table_name)
    write_bronze(enriched_df, table_name)
    logger.info("Bronze concluída: %d registros.", enriched_df.count())


if __name__ == "__main__":
    run()
