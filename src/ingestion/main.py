import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType
)

API_BASE_URL = os.environ.get("API_BASE_URL", "https://fakestoreapi.com")
PRODUCTS_ENDPOINT = os.environ.get("PRODUCTS_ENDPOINT", "/products")
USERS_ENDPOINT = os.environ.get("USERS_ENDPOINT", "/users")


def fetch_api_data(endpoint: str) -> list:
    """Fetch JSON data from the given API endpoint."""
    response = requests.get(f"{API_BASE_URL}{endpoint}")
    response.raise_for_status()
    return response.json()


def save_to_parquet(api: str, data: list, output_path: str, spark: SparkSession) -> None:
    """Save list of dicts to a parquet file using Spark."""
    if not data:
        return

    if api == "products":
        # Exemplo de pré-conversão
        for item in data:
            # garante que price e rating.rate são float
            item["price"] = float(item["price"])
            # se rating estiver presente e for dict:
            if isinstance(item.get("rating"), dict):
                item["rating"]["rate"] = float(item["rating"]["rate"])
                # rating["count"] já é int, ok para IntegerType
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("title", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("description", StringType(), True),
            StructField("category", StringType(), True),
            StructField("image", StringType(), True),
            StructField("rating", StructType([
                StructField("rate", DoubleType(), True),
                StructField("count", IntegerType(), True)
            ]), True)
        ])
        # 1) Cria DF a partir da lista Python
        df_final = spark.createDataFrame(data, schema=schema)

    elif api == "users":
        # Para usuários, filtra só os campos desejados
        df_final = spark.createDataFrame(
            [{"id": item["id"], "name": item["name"], "email": item["email"]} 
             for item in data]
        )

        # 5) Escreve o DataFrame correto
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
