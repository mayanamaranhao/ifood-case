"""Testes unitários das funções de transformação.

Usam uma SparkSession local (sem cluster/Databricks), o que permite rodar
`pytest` em CI de forma rápida e isolada. Cada teste cobre uma regra de
negócio/qualidade específica, documentada nas docstrings dos módulos
originais.
"""

from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from src.transformation.bronze_to_silver import transform as bronze_to_silver_transform
from src.transformation.silver_to_gold import (
    avg_total_amount_by_month,
    avg_passenger_count_by_hour_may,
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[2]")
        .appName("ifood-case-tests")
        .getOrCreate()
    )


def _bronze_row(**overrides):
    base = dict(
        VendorID=1,
        passenger_count=2,
        total_amount=15.5,
        tpep_pickup_datetime=datetime(2023, 5, 10, 8, 0, 0),
        tpep_dropoff_datetime=datetime(2023, 5, 10, 8, 20, 0),
    )
    base.update(overrides)
    return base


def test_bronze_to_silver_filters_invalid_passenger_count(spark):
    df = spark.createDataFrame(
        [
            _bronze_row(passenger_count=2),
            _bronze_row(passenger_count=0),  # deve ser descartado
        ]
    )
    result = bronze_to_silver_transform(df)
    assert result.count() == 1
    assert result.first()["passenger_count"] == 2


def test_bronze_to_silver_filters_negative_total_amount(spark):
    df = spark.createDataFrame(
        [
            _bronze_row(total_amount=20.0),
            _bronze_row(total_amount=-5.0),  # deve ser descartado
        ]
    )
    result = bronze_to_silver_transform(df)
    assert result.count() == 1
    assert result.first()["total_amount"] == 20.0


def test_bronze_to_silver_filters_dropoff_before_pickup(spark):
    df = spark.createDataFrame(
        [
            _bronze_row(
                tpep_pickup_datetime=datetime(2023, 5, 10, 9, 0, 0),
                tpep_dropoff_datetime=datetime(2023, 5, 10, 8, 0, 0),  # inválido
            )
        ]
    )
    result = bronze_to_silver_transform(df)
    assert result.count() == 0


def test_bronze_to_silver_deduplicates(spark):
    row = _bronze_row()
    df = spark.createDataFrame([row, row])  # duplicata exata
    result = bronze_to_silver_transform(df)
    assert result.count() == 1


def test_avg_total_amount_by_month(spark):
    silver_df = spark.createDataFrame(
        [
            dict(trip_year=2023, trip_month=5, total_amount=10.0,
                 vendor_id=1, passenger_count=1,
                 tpep_pickup_datetime=datetime(2023, 5, 1, 10)),
            dict(trip_year=2023, trip_month=5, total_amount=20.0,
                 vendor_id=1, passenger_count=1,
                 tpep_pickup_datetime=datetime(2023, 5, 1, 11)),
        ]
    )
    result = avg_total_amount_by_month(silver_df).collect()
    assert len(result) == 1
    assert result[0]["avg_total_amount"] == 15.0


def test_avg_passenger_count_by_hour_may(spark):
    silver_df = spark.createDataFrame(
        [
            dict(trip_year=2023, trip_month=5, total_amount=10.0,
                 vendor_id=1, passenger_count=2,
                 tpep_pickup_datetime=datetime(2023, 5, 1, 10, 15)),
            dict(trip_year=2023, trip_month=5, total_amount=10.0,
                 vendor_id=1, passenger_count=4,
                 tpep_pickup_datetime=datetime(2023, 5, 1, 10, 45)),
        ]
    )
    result = avg_passenger_count_by_hour_may(silver_df).collect()
    assert len(result) == 1
    assert result[0]["pickup_hour"] == 10
    assert result[0]["avg_passenger_count"] == 3.0
