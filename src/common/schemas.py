"""Schemas explícitos usados na camada Silver.

Definir o schema explicitamente (em vez de inferir) evita quebras silenciosas
quando o provedor da fonte (NYC TLC) muda tipos de coluna entre meses —
algo que já aconteceu historicamente nesse dataset (ex.: VendorID como
int vs long em diferentes releases).
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType,
    DoubleType,
    TimestampType,
)

# Schema mínimo exigido pelo case + colunas de suporte à qualidade/análise.
# Mantemos apenas o que é necessário: reduz custo de storage e I/O nas
# camadas Silver/Gold (princípio de "narrow tables" para consumo).
SILVER_SCHEMA = StructType(
    [
        StructField("vendor_id", IntegerType(), nullable=True),
        StructField("passenger_count", IntegerType(), nullable=True),
        StructField("total_amount", DoubleType(), nullable=True),
        StructField("tpep_pickup_datetime", TimestampType(), nullable=False),
        StructField("tpep_dropoff_datetime", TimestampType(), nullable=False),
        StructField("trip_year", IntegerType(), nullable=False),
        StructField("trip_month", IntegerType(), nullable=False),
    ]
)
