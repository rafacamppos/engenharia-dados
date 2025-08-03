"""Módulo responsável pela ingestão de dados brutos da FakeStore API."""

import os
from typing import List, Dict, Any

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    IntegerType,
    StringType,
)

API_BASE_URL = os.environ.get("API_BASE_URL", "https://fakestoreapi.com")
PRODUCTS_ENDPOINT = os.environ.get("PRODUCTS_ENDPOINT", "/products")
USERS_ENDPOINT = os.environ.get("USERS_ENDPOINT", "/users")


def fetch_api_data(endpoint: str) -> List[Dict[str, Any]]:
    """Busca dados JSON no endpoint informado."""
    response = requests.get(f"{API_BASE_URL}{endpoint}")
    response.raise_for_status()
    return response.json()


def save_to_parquet(api: str, data: List[Dict[str, Any]], output_path: str, spark: SparkSession) -> None:
    """Salva lista de dicionários em um arquivo Parquet usando Spark."""
    if not data:
        return

    if api == "products":
        for item in data:
            item["price"] = float(item["price"])
            if isinstance(item.get("rating"), dict):
                item["rating"]["rate"] = float(item["rating"]["rate"])

        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("title", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("description", StringType(), True),
            StructField("category", StringType(), True),
            StructField("image", StringType(), True),
            StructField("rating", StructType([
                StructField("rate", DoubleType(), True),
                StructField("count", IntegerType(), True),
            ]), True),
        ])
        df_final = spark.createDataFrame(data, schema=schema)
    elif api == "users":
        df_final = spark.createDataFrame(
            [
                {"id": item["id"], "name": item["name"], "email": item["email"]}
                for item in data
            ]
        )
    else:
        raise ValueError(f"API desconhecida: {api}")

    df_final.write.mode("overwrite").parquet(output_path)




def main() -> None:
    spark = SparkSession.builder.config("spark.driver.memory", "4g").appName("FakeStoreIngestion").getOrCreate()

    products_data = fetch_api_data(PRODUCTS_ENDPOINT)
    users_data = fetch_api_data(USERS_ENDPOINT)

    os.makedirs("data/bronze/products", exist_ok=True)
    os.makedirs("data/bronze/users", exist_ok=True)

    save_to_parquet("products", products_data, "data/bronze/products/products.parquet", spark)
    save_to_parquet("users", users_data, "data/bronze/users/users.parquet", spark)

    spark.stop()


if __name__ == "__main__":
    main()
