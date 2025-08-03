"""Testes para o módulo de ingestão."""

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from src.ingestion.main import save_to_parquet


@pytest.fixture(scope="module")
def spark_session():
    spark = SparkSession.builder.master("local[*]").appName("tests").getOrCreate()
    yield spark
    spark.stop()


def test_save_products_to_parquet(tmp_path, spark_session):
    data = [
        {
            "id": 1,
            "title": "Produto A",
            "price": 10,
            "description": "desc",
            "category": "cat",
            "image": "img",
            "rating": {"rate": 4.5, "count": 2},
        }
    ]

    output_path = tmp_path / "products.parquet"
    save_to_parquet("products", data, str(output_path), spark_session)

    assert output_path.exists()


def test_save_users_to_parquet(tmp_path, spark_session):
    data = [{"id": 1, "name": "John", "email": "john@example.com"}]

    output_path = tmp_path / "users.parquet"
    save_to_parquet("users", data, str(output_path), spark_session)

    assert output_path.exists()

