import json
import os
import glob
from delta import configure_spark_with_delta_pip
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, IntegerType, StringType,
                               DoubleType, StructType as ST, StructField as SF, IntegerType as IT)
from pyspark.sql.functions import input_file_name, current_timestamp, col
from pyspark.sql.types import TimestampType

BASE = os.getcwd()
PATH_RAW_FILE = "data/raw/contabilidade/*/*/*.jsonl.gz"
PATH_USERS_RAW_FILE = "data/raw/contabilidade/*/*/*.jsonl.gz"
BRONZE_PATH = f"file://{BASE}/data/bronze/contabilidade"
print(BRONZE_PATH)






builder = (
    SparkSession.builder
    .appName("BronzeContabilidadeIngest")
    # [DELTA_CONFIGURE_SPARK_SESSION_WITH_EXTENSION_AND_CATALOG]
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

bronze_schema = StructType([
    StructField("cnpj_base", StringType(), True),
    StructField("cnpj_ordem", StringType(), True),
    StructField("cnpj_dv", StringType(), True),
    StructField("matriz_filial", StringType(), True),
    StructField("nome_fantasia", StringType(), True),

    StructField("situacao_cadastral", StringType(), True),
    StructField("data_situacao_cadastral", StringType(), True),
    StructField("motivo_situacao_cadastral", StringType(), True),

    StructField("nome_cidade_exterior", StringType(), True),
    StructField("pais", StringType(), True),
    StructField("data_inicio_atividade", StringType(), True),

    StructField("cnae_principal", StringType(), True),
    StructField("cnaes_secundarios", StringType(), True),

    StructField("tipo_logradouro", StringType(), True),
    StructField("logradouro", StringType(), True),
    StructField("numero", StringType(), True),
    StructField("complemento", StringType(), True),
    StructField("bairro", StringType(), True),
    StructField("cep", StringType(), True),
    StructField("uf", StringType(), True),
    StructField("codigo_municipio", StringType(), True),

    StructField("ddd1", StringType(), True),
    StructField("telefone1", StringType(), True),
    StructField("ddd2", StringType(), True),
    StructField("telefone2", StringType(), True),
    StructField("ddd_fax", StringType(), True),
    StructField("fax", StringType(), True),

    StructField("email1", StringType(), True),
    StructField("email2", StringType(), True),
    StructField("email3", StringType(), True),

    # colunas técnicas
    StructField("_source_system", StringType(), True),
    StructField("_source_file", StringType(), True),
    StructField("_ingestion_ts", TimestampType(), True),
    StructField("_batch_id", StringType(), True),
    StructField("y", StringType(), True),
    StructField("m", StringType(), True),
    StructField("d", StringType(), True),
])

files = glob.glob(PATH_RAW_FILE)
print("Arquivos encontrados:", files[:5])

spark = configure_spark_with_delta_pip(builder).getOrCreate()
df_contabilidade_bronze = spark.read.option("multiline", "false").schema(bronze_schema).json(PATH_RAW_FILE)

# df_raw: DataFrame carregado do CSV/JSON, coerente com bronze_schema (strings)
df_bronze = (
    df_contabilidade_bronze
    .withColumn("_source_system", F.lit("rfb_estabelecimentos"))
    .withColumn("_source_file", F.input_file_name())
    .withColumn("_ingestion_ts", F.current_timestamp())
    .withColumn("_batch_id", F.expr("uuid()"))
    .withColumn("y", F.date_format(F.current_timestamp(), "yyyy"))
    .withColumn("m", F.date_format(F.current_timestamp(), "MM"))
    .withColumn("d", F.date_format(F.current_timestamp(), "dd"))
)
df_bronze.show()
print(f"Tamanho do bronze: {df_bronze.count()} linhas")

(df_bronze
 .write
 .format("delta")
 .mode("append")
 .partitionBy("y","m","d")
 .save(BRONZE_PATH))


spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
spark.sql("USE bronze")

# (opcional) registrar tabela por LOCATION
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS bronze.rfb_estabelecimentos
  USING DELTA
  LOCATION '{BRONZE_PATH}'
""")

spark.sql("SHOW TABLES").show()
spark.sql("DESCRIBE EXTENDED bronze.rfb_estabelecimentos").show(truncate=False)
spark.sql(""" SELECT * FROM bronze.rfb_estabelecimentos """).show(truncate=False)
spark.stop()