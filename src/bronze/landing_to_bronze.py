"""Landing -> Bronze.

Bronze é a cópia fiel do dado de origem, apenas convertido para um formato
transacional (Delta) e com metadados técnicos de rastreabilidade
(arquivo de origem, timestamp de ingestão). Nenhuma regra de negócio ou
limpeza é aplicada aqui — isso é o que garante que, se uma regra de
limpeza da Silver estiver errada, o dado original ainda está intacto
para reprocessamento.
"""

from __future__ import annotations

from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DoubleType, IntegerType, LongType, ShortType

from src.config import LAKE
from src.common.spark_session import get_spark_session
from src.common.utils import ensure_schema, get_logger

logger = get_logger(__name__)

# Tipos numéricos "estreitos" que a NYC TLC ocasionalmente alterna entre
# meses para a mesma coluna (ex.: airport_fee ora INT64, ora DOUBLE).
_NARROW_NUMERIC_TYPES = (IntegerType, LongType, ShortType, DecimalType)


def _normalize_numeric_types(df: DataFrame) -> DataFrame:
    """Promove colunas numéricas "estreitas" para DOUBLE.

    Arquivos diferentes podem trazer a mesma coluna com tipos numéricos
    distintos entre si (ex.: INT64 em um mês, DOUBLE em outro — já
    aconteceu de fato com `airport_fee` neste dataset). Normalizamos tudo
    para DOUBLE (upcast seguro, sem perda de dado) antes do `unionByName`,
    para o schema final ficar consistente entre meses. Colunas não
    numéricas (string, timestamp, boolean) não são afetadas.
    """
    for field in df.schema.fields:
        if isinstance(field.dataType, _NARROW_NUMERIC_TYPES):
            df = df.withColumn(field.name, F.col(field.name).cast(DoubleType()))
    return df


def _tag_source_file(df: DataFrame, file_path: Path) -> DataFrame:
    """Anexa o path do arquivo de origem e o ano/mês de referência como
    colunas literais, extraídos do próprio nome do arquivo.

    Feito aqui (no loop por arquivo) em vez de usar a coluna oculta
    `_metadata.file_path` depois do `unionByName`: essa coluna especial é
    resolvida pelo mecanismo de scan do Spark, e seu comportamento após
    unir DataFrames de leituras distintas não é garantido. Como já
    sabemos o path de cada arquivo no momento da leitura, é mais simples
    e confiável gravá-lo como literal desde já.
    """
    file_name = file_path.name  # ex.: yellow_tripdata_2023-05.parquet
    ref_year, ref_month = file_name.split("_")[-1].replace(".parquet", "").split("-")
    return (
        df.withColumn("_source_file", F.lit(str(file_path)))
        .withColumn("_ref_year", F.lit(int(ref_year)))
        .withColumn("_ref_month", F.lit(int(ref_month)))
    )


def read_landing(spark: SparkSession, landing_path: str) -> DataFrame:
    """Lê os arquivos parquet da landing zone, um de cada vez, já com
    metadados técnicos de origem anexados.

    Ler arquivo a arquivo (em vez de `spark.read.parquet(pasta_inteira)`)
    evita que o Spark precise reconciliar automaticamente schemas
    conflitantes entre meses (`PARQUET_COLUMN_DATA_TYPE_MISMATCH`) — algo
    que acontece de fato neste dataset, pois a NYC TLC já alternou o tipo
    de colunas como `airport_fee` entre releases mensais. Cada arquivo é
    normalizado individualmente e depois unidos via
    `unionByName(allowMissingColumns=True)`, que também tolera colunas
    presentes em uns meses e ausentes em outros.
    """
    files = sorted(Path(landing_path).glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"Nenhum arquivo parquet encontrado em {landing_path}")

    logger.info("Lendo %d arquivo(s) individualmente da landing zone.", len(files))
    per_file_dfs = [
        _tag_source_file(_normalize_numeric_types(spark.read.parquet(str(f))), f)
        for f in files
    ]
    return reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), per_file_dfs)


def add_ingestion_timestamp(df: DataFrame) -> DataFrame:
    """Marca o momento em que o dado entrou na Bronze (auditoria/linhagem)."""
    return df.withColumn("_ingested_at", F.current_timestamp())


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
    enriched_df = add_ingestion_timestamp(raw_df)

    table_name = LAKE.bronze_table_fqn
    logger.info("Escrevendo Bronze: %s", table_name)
    write_bronze(enriched_df, table_name)
    logger.info("Bronze concluída: %d registros.", enriched_df.count())


if __name__ == "__main__":
    run()