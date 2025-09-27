"""Ingestao Bronze para financas.

Le os arquivos da camada RAW (jsonl.gz) gerados por
`src/ingestion/raw/contabilidade/financas.py` e materializa duas tabelas Delta:
- bronze.financas_renda
- bronze.financas_gastos

Cada registro recebe colunas tecnicas padronizadas (_source_system, _source_file,
_ingestion_ts, _batch_id, y, m, d) e classificacao `tipo_movimento`.
"""

from __future__ import annotations

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType


BASE_DIR = os.getcwd()
RAW_PATH_PATTERN = os.path.join(
    BASE_DIR,
    "data",
    "raw",
    "financas",
    "*",
    "*",
    "*.jsonl.gz",
)

BRONZE_BASE_PATH = f"file://{BASE_DIR}/data/bronze/financas"
BRONZE_RENDA_PATH = f"{BRONZE_BASE_PATH}/renda"
BRONZE_GASTOS_PATH = f"{BRONZE_BASE_PATH}/gastos"

TABLE_RENDA = "bronze.financas_renda"
TABLE_GASTOS = "bronze.financas_gastos"

ENTRADA_CATS = ["salario", "bonus", "pix recebido"]

schema = StructType([
    StructField("DATA", StringType(), True),
    StructField("M\u00caS", StringType(), True),
    StructField("CATEGORIA", StringType(), True),
    StructField("DESCRICAO", StringType(), True),
    StructField("VALOR", StringType(), True),
])


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("BronzeFinancasIngest")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def load_raw_financas(spark: SparkSession):
    return (
        spark.read
        .schema(schema)
        .json(RAW_PATH_PATTERN)
    )


def enrich_with_metadata(df):
    file_col = F.input_file_name()

    df_norm = (
        df.withColumnRenamed("DATA", "data")
        .withColumnRenamed("M\u00caS", "mes")
        .withColumnRenamed("CATEGORIA", "categoria")
        .withColumnRenamed("DESCRICAO", "descricao")
        .withColumnRenamed("VALOR", "valor")
        .withColumn("_source_system", F.lit("financas"))
        .withColumn("_source_file", file_col)
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_batch_id", F.regexp_extract(file_col, r"financas/[^/]+/([^/]+)/", 1))
        .withColumn("_raw_date", F.regexp_extract(file_col, r"financas/([^/]+)/[^/]+/", 1))
    )

    df_norm = df_norm.withColumn(
        "_batch_id",
        F.when(F.col("_batch_id") == "", F.expr("uuid()"))
        .otherwise(F.col("_batch_id"))
    )

    df_norm = df_norm.withColumn(
        "_raw_date",
        F.when(F.col("_raw_date") == "", F.date_format(F.col("_ingestion_ts"), "yyyy-MM-dd"))
        .otherwise(F.col("_raw_date"))
    )

    df_norm = df_norm.withColumn("dt_raw", F.to_date(F.col("_raw_date")))
    df_norm = df_norm.withColumn("y", F.date_format(F.col("dt_raw"), "yyyy"))
    df_norm = df_norm.withColumn("m", F.date_format(F.col("dt_raw"), "MM"))
    df_norm = df_norm.withColumn("d", F.date_format(F.col("dt_raw"), "dd"))

    df_norm = df_norm.withColumn(
        "tipo_movimento",
        F.when(F.lower(F.col("categoria")).isin(*ENTRADA_CATS), F.lit("entrada"))
        .otherwise(F.lit("saida"))
    )

    return df_norm


def write_delta(df, path: str, table_name: str, spark: SparkSession) -> None:
    (
        df.drop("dt_raw")
        .write
        .format("delta")
        .mode("append")
        .partitionBy("y", "m", "d")
        .save(path)
    )

    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    spark.sql("USE bronze")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        USING DELTA
        LOCATION '{path}'
        """
    )


def main() -> None:
    spark = build_spark()

    try:
        raw_df = load_raw_financas(spark)

        if raw_df.rdd.isEmpty():
            print("Nenhum arquivo encontrado para ingestao Bronze de financas.")
            return

        bronze_df = enrich_with_metadata(raw_df)

        renda_df = bronze_df.filter(F.col("tipo_movimento") == "entrada")
        gastos_df = bronze_df.filter(F.col("tipo_movimento") == "saida")

        if not renda_df.rdd.isEmpty():
            write_delta(renda_df, BRONZE_RENDA_PATH, TABLE_RENDA, spark)
            print(f"Tabela Bronze renda escrita em {BRONZE_RENDA_PATH}")
        else:
            print("Aviso: nenhuma linha classificada como renda.")

        if not gastos_df.rdd.isEmpty():
            write_delta(gastos_df, BRONZE_GASTOS_PATH, TABLE_GASTOS, spark)
            print(f"Tabela Bronze gastos escrita em {BRONZE_GASTOS_PATH}")
        else:
            print("Aviso: nenhuma linha classificada como gasto.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
