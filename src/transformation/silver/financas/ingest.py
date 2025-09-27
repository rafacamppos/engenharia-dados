"""Ingestao Silver para financas.

Le tabelas Bronze (`bronze.financas_renda` e `bronze.financas_gastos`) e aplica
tratamentos de qualidade: normalizacao de tipos, deduplicacao e preservacao de
colunas tecnicas. O resultado e persistido em `data/silver/financas`.
"""

from __future__ import annotations

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


BASE_DIR = os.getcwd()

BRONZE_RENDA_PATH = f"file://{BASE_DIR}/data/bronze/financas/renda"
BRONZE_GASTOS_PATH = f"file://{BASE_DIR}/data/bronze/financas/gastos"

SILVER_RENDA_PATH = f"file://{BASE_DIR}/data/silver/financas/renda"
SILVER_GASTOS_PATH = f"file://{BASE_DIR}/data/silver/financas/gastos"

TABLE_SILVER_RENDA = "silver.financas_renda"
TABLE_SILVER_GASTOS = "silver.financas_gastos"


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("SilverFinancasIngest")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    spark.sql("USE silver")
    return spark


def normalize(df):
    return (
        df.withColumn("data", F.to_date("data", "dd/MM/yyyy"))
          .withColumn("mes", F.upper(F.trim(F.col("mes"))))
          .withColumn("categoria", F.initcap(F.trim(F.col("categoria"))))
          .withColumn("descricao", F.trim(F.col("descricao")))
          # 1) remove tudo exceto dígitos, vírgula e sinal (SEM ponto)
          .withColumn("valor", F.regexp_replace("valor", r"[^0-9,\-]", ""))
          # 2) vírgula -> ponto
          .withColumn("valor", F.regexp_replace("valor", ",", "."))
          # 3) cast seguro
          .withColumn("valor", F.col("valor").cast("decimal(18,2)"))
          .withColumn("processed_at", F.current_timestamp())
    )


def dedup(df, partition_cols):
    w = Window.partitionBy(*partition_cols).orderBy(F.col("_ingestion_ts").desc(), F.col("processed_at").desc())
    return (
        df.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )


def write_delta(df, path: str, table_name: str, spark: SparkSession) -> None:
    (
        df.write
        .format("delta")
        .mode("append")
        .save(path)
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        USING DELTA
        LOCATION '{path}'
        """
    )


def process(bronze_path: str, target_path: str, table_name: str, spark: SparkSession) -> None:
    bronze_df = spark.read.format("delta").load(bronze_path)

    if bronze_df.rdd.isEmpty():
        print(f"Nenhuma linha encontrada em {bronze_path}, nada a fazer.")
        return

    df = normalize(bronze_df)
    df = dedup(df, ["data", "categoria", "descricao", "valor", "tipo_movimento"])

    write_delta(df, target_path, table_name, spark)
    print(f"Tabela Silver escrita em {target_path}")


def main() -> None:
    spark = build_spark()

    try:
        process(BRONZE_RENDA_PATH, SILVER_RENDA_PATH, TABLE_SILVER_RENDA, spark)
        process(BRONZE_GASTOS_PATH, SILVER_GASTOS_PATH, TABLE_SILVER_GASTOS, spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
