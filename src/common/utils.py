"""Utilitários genéricos compartilhados entre as etapas do pipeline."""

import logging
from pyspark.sql import SparkSession


def get_logger(name: str) -> logging.Logger:
    """Logger padronizado (nível INFO, formato consistente entre módulos)."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


def ensure_schema(spark: SparkSession, catalog: str, schema: str) -> None:
    """Garante que o schema do Unity Catalog exista antes do write.

    Substitui o antigo `CREATE DATABASE` do Hive Metastore: na Free
    Edition (Unity Catalog obrigatório), schemas são sempre qualificados
    por catalog (`catalog.schema`).
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
