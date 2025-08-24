import json
import os
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, IntegerType, StringType,
                               DoubleType, StructType as ST, StructField as SF, IntegerType as IT)
from pyspark.sql.functions import input_file_name, current_timestamp, col

PATH_PRODUCTS_RAW_FILE = "data/raw/fakestore/products/part-a993305996e04052a5791fe3a3711a8c-00001.jsonl.gz"

PATH_USERS_RAW_FILE = "data/raw/fakestore/users/part-928fb9b1183943ae988a112411745c38-00001.jsonl.gz"
BRONZE_PRODUCTS_PATH = "data/bronze/fakestore/products"
BRONZE_USERS_PATH = "data/bronze/fakestore/users"


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

users_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("username", StringType(), True),
    StructField("password", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("__v", IntegerType(), True),

    StructField("name", StructType([
        StructField("firstname", StringType(), True),
        StructField("lastname", StringType(), True),
    ]), True),

    StructField("address", StructType([
        StructField("city", StringType(), True),
        StructField("street", StringType(), True),
        StructField("number", IntegerType(), True),
        StructField("zipcode", StringType(), True),
        StructField("geolocation", StructType([
            StructField("lat", StringType(), True),
            StructField("long", StringType(), True),
        ]), True),
    ]), True),
])

# ---- read RAW → cast & meta ----
df_products_raw = spark.read.schema(products_schema).json(PATH_PRODUCTS_RAW_FILE)
df_users_raw = spark.read.schema(users_schema).json(PATH_USERS_RAW_FILE)

df_products_bronze = (
    df_products_raw
    .withColumn("file_origin", input_file_name())
    .withColumn("ingestion_ts", current_timestamp())
)

df_users_bronze = (
    df_users_raw
    .withColumn("file_origin", input_file_name())
    .withColumn("ingestion_ts", current_timestamp())
)

# (opcional) dedup por id
df_products_bronze = df_products_bronze.dropDuplicates(["id"])
df_users_bronze = df_users_bronze.dropDuplicates(["id"])

# ---- write Delta (append) ----
(df_products_bronze
 .write
 .format("delta")
 .option("mergeSchema", "true")
 .mode("append")
 .partitionBy("category")  # opcional
 .save(BRONZE_PRODUCTS_PATH))

(df_users_bronze
 .write
 .format("delta")
 .option("mergeSchema", "true")
 .mode("append")
 .save(BRONZE_USERS_PATH))

spark.sql("CREATE DATABASE IF NOT EXISTS default")

# (opcional) registrar tabela por LOCATION
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS default.bronze_products
  USING DELTA
  LOCATION '{BRONZE_PRODUCTS_PATH}'
""")

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS default.bronze_users
  USING DELTA
  LOCATION '{BRONZE_USERS_PATH}'
""")

#spark.sql("SHOW TABLES").show()
#print("Existe _delta_log?:", os.path.exists(os.path.join(BRONZE_PRODUCTS_PATH, "_delta_log")))

#spark.read.format("delta").load(BRONZE_PRODUCTS_PATH).show()
#spark.read.format("delta").load(BRONZE_USERS_PATH).show()

df_products_bronze.createOrReplaceTempView("bronze_products")
df_users_bronze.createOrReplaceTempView("bronze_users")
spark.sql(""" SELECT * FROM bronze_products """).show(truncate=False)
spark.sql(""" SELECT * FROM bronze_users """).show(truncate=False)
spark.stop()