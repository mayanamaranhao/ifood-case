"""Análise exploratória e resposta das perguntas de negócio do case.

Pensado para rodar como notebook Databricks (célula a célula) ou como
script standalone. Reaproveita as funções de agregação de
src/transformation/silver_to_gold.py para não duplicar lógica de negócio
entre pipeline e análise.
"""

from src.common.spark_session import get_spark_session
from src.config import LAKE
from src.transformation.silver_to_gold import (
    avg_total_amount_by_month,
    avg_passenger_count_by_hour_may,
)

spark = get_spark_session()
silver_df = spark.table(f"{LAKE.silver_db}.{LAKE.silver_table}")

# --- Pergunta 1 -------------------------------------------------------
print("Média de total_amount por mês (toda a frota):")
avg_total_amount_by_month(silver_df).show(truncate=False)

# --- Pergunta 2 -------------------------------------------------------
print("Média de passenger_count por hora do dia (maio/2023):")
avg_passenger_count_by_hour_may(silver_df).show(24, truncate=False)

# --- Análises exploratórias complementares (contexto extra) -----------

# Volume de corridas por mês, para checar consistência do range Jan-Mai
print("Volume de corridas por mês:")
silver_df.groupBy("trip_year", "trip_month").count().orderBy(
    "trip_year", "trip_month"
).show()

# Distribuição de passenger_count, para validar o filtro de qualidade
# aplicado na Silver (passenger_count > 0)
print("Distribuição de passenger_count:")
silver_df.groupBy("passenger_count").count().orderBy("passenger_count").show()
