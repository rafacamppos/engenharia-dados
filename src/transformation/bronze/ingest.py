import json
import os
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, IntegerType, StringType,
                               DoubleType, StructType as ST, StructField as SF, IntegerType as IT)
from pyspark.sql.functions import input_file_name, current_timestamp, col

PATH_RAW_FILE = "data/raw/fakestore/products/2025-08-16/12086be9f04649509033feae8af0699a/part-ba441b58257a4a829c39d06be94ff4af-00001.jsonl.gz"

BRONZE_PATH = "data/bronze/fakestore/products"


builder = (
    SparkSession.builder
    .appName("DeltaEnabled")
    # [DELTA_CONFIGURE_SPARK_SESSION_WITH_EXTENSION_AND_CATALOG]
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()


# ---- schema products ----
products_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("description", StringType(), True),
    StructField("category", StringType(), True),
    StructField("image", StringType(), True),
    StructField("rating", ST([
        SF("rate",  DoubleType(), True),
        SF("count", IT(), True),
    ]), True),
])

# ---- read RAW → cast & meta ----
df_raw = spark.read.schema(products_schema).json(PATH_RAW_FILE)

df_bronze = (
    df_raw
    .withColumn("file_origin", input_file_name())
    .withColumn("ingestion_ts", current_timestamp())
)

# (opcional) dedup por id
df_bronze = df_bronze.dropDuplicates(["id"])

# ---- write Delta (append) ----
(df_bronze
 .write
 .format("delta")
 .mode("append")
 .partitionBy("category")  # opcional
 .save(BRONZE_PATH))

# (opcional) registrar tabela por LOCATION
spark.sql("DROP TABLE IF EXISTS default.bronze_products")
spark.sql(f"""
  CREATE TABLE default.bronze_products
  USING DELTA
  LOCATION '{BRONZE_PATH}'
""")
spark.sql("SHOW TABLES").show()
print("Existe _delta_log?:", os.path.exists(os.path.join(BRONZE_PATH, "_delta_log")))
df = spark.read.format("delta").load(BRONZE_PATH).show()
df_bronze.createOrReplaceTempView("bronze_products")
spark.sql("""
    SELECT * FROM bronze_products """).show(truncate=False)
#spark.sql(f""" SELECT * FROM default.bronze_products USING DELTA LOCATION '{BRONZE_PATH}'""").show()


spark.stop()