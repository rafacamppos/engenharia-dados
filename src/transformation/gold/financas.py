from __future__ import annotations

import os

# Configura Spark + Delta e funcoes utilitarias usadas no processamento
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Caminhos de entrada/saida resolvidos a partir do diretorio do projeto
BASE_DIR = os.getcwd()
INPUT_JSON = os.path.join(BASE_DIR, "resources", "financas.json")

GOLD_BASE_PATH = f"file://{BASE_DIR}/data/gold/financas"
P_VW_RENDA_MENSAL = f"{GOLD_BASE_PATH}/vw_renda_mensal"
P_VW_GASTOS_MENSAL = f"{GOLD_BASE_PATH}/vw_gastos_mensal"

T_VW_RENDA_MENSAL = "gold.vw_financas_renda_mensal"
T_VW_GASTOS_MENSAL = "gold.vw_financas_gastos_mensal"

# Sessao Spark configurada com extensoes Delta para permitir leitura/escrita em Delta Lake
spark = configure_spark_with_delta_pip(
    SparkSession.builder
    .appName("BuildGold_Financas")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
).getOrCreate()

# Ordem cronologica fixa para garantir consistencia nos pivots mensais
MESES_ORDENADOS = [
    "JANEIRO",
    "FEVEREIRO",
    "MARCO",
    "ABRIL",
    "MAIO",
    "JUNHO",
    "JULHO",
    "AGOSTO",
    "SETEMBRO",
    "OUTUBRO",
    "NOVEMBRO",
    "DEZEMBRO",
]

# Mapa id -> nome do mes (sem acentuacao) usado para normalizar os dados de entrada
MESES_MAP = F.create_map(
    F.lit(1), F.lit("JANEIRO"),
    F.lit(2), F.lit("FEVEREIRO"),
    F.lit(3), F.lit("MARCO"),
    F.lit(4), F.lit("ABRIL"),
    F.lit(5), F.lit("MAIO"),
    F.lit(6), F.lit("JUNHO"),
    F.lit(7), F.lit("JULHO"),
    F.lit(8), F.lit("AGOSTO"),
    F.lit(9), F.lit("SETEMBRO"),
    F.lit(10), F.lit("OUTUBRO"),
    F.lit(11), F.lit("NOVEMBRO"),
    F.lit(12), F.lit("DEZEMBRO")
)

# Categorias consideradas receitas; todo o restante sera tratado como despesa
ENTRADAS_CATEGORIAS = ["salario", "bonus", "pix recebido"]


def build_pivot_table(df: DataFrame, *, months: list[str], total_label: str = "TOTAL") -> DataFrame:
    """Transforma um dataframe transacional em uma tabela pivotada por mes.

    - Mantem o grao (ano, categoria)
    - Garante existencia das colunas de mes
    - Calcula o total anual e adiciona a linha TOTAL
    """
    pivot = (
        df.groupBy("ano", "categoria")
        .pivot("mes_nome", months)
        .agg(F.sum("valor"))
    )

    for mes in months:
        if mes not in pivot.columns:
            pivot = pivot.withColumn(mes, F.lit(0.0))
        pivot = pivot.withColumn(mes, F.round(F.coalesce(F.col(mes), F.lit(0.0)), 2))

    soma_meses = None
    for mes in months:
        soma_meses = (
            F.coalesce(F.col(mes), F.lit(0.0))
            if soma_meses is None
            else soma_meses + F.coalesce(F.col(mes), F.lit(0.0))
        )

    pivot = pivot.withColumn("TOTAL", F.round(soma_meses, 2))
    pivot = pivot.withColumn("updated_at", F.current_timestamp())

    totais = pivot.groupBy("ano").agg(
        *[F.round(F.sum(F.col(mes)), 2).alias(mes) for mes in months],
        F.round(F.sum(F.col("TOTAL")), 2).alias("TOTAL"),
        F.max("updated_at").alias("updated_at"),
    ).withColumn("categoria", F.lit(total_label))

    colunas = ["ano", "categoria", *months, "TOTAL", "updated_at"]

    resultado = pivot.select(*colunas).unionByName(
        totais.select(*colunas), allowMissingColumns=False
    )

    return resultado.orderBy(
        F.col("ano"),
        F.when(F.col("categoria") == total_label, F.lit(1)).otherwise(F.lit(0)),
        F.col("categoria"),
    )



# Garante que o catalogo Delta esteja pronto para receber as tabelas ouro
spark.sql("CREATE DATABASE IF NOT EXISTS gold")
spark.sql("USE gold")

# Leitura da planilha JSON (estrutura multi-line) com os lancamentos financeiros
raw_financas = (
    spark.read
    .option("multiLine", True)
    .json(INPUT_JSON)
)

# Normalizacao das colunas: datas, valores monetarios e classificacao entrada/saida
financas = (
    raw_financas
    .withColumnRenamed("DATA", "data_texto")
    .withColumnRenamed("MÊS", "mes_nome_original")
    .withColumnRenamed("CATEGORIA", "categoria")
    .withColumnRenamed("DESCRICAO", "descricao")
    .withColumnRenamed("VALOR", "valor_texto")
    .withColumn("data", F.to_date("data_texto", "dd/MM/yyyy"))
    .withColumn("descricao", F.trim(F.col("descricao")))
    .withColumn("mes", F.month("data"))
    .withColumn("mes_nome", F.element_at(MESES_MAP, F.col("mes")))
    .withColumn("valor_limpo", F.regexp_replace(F.col("valor_texto"), r"[^0-9,-]", ""))
    .withColumn("valor_limpo", F.regexp_replace(F.col("valor_limpo"), ",", "."))
    .withColumn("valor", F.col("valor_limpo").cast("double"))
    .withColumn("ano", F.year("data"))
    .withColumn(
        "tipo_movimento",
        F.when(F.lower(F.col("categoria")).isin(ENTRADAS_CATEGORIAS), F.lit("entrada"))
        .otherwise(F.lit("saida"))
    )
    .filter(F.col("data").isNotNull() & F.col("valor").isNotNull())
    .dropDuplicates(["data", "categoria", "descricao", "valor"])
    .select("ano", "mes", "mes_nome", "categoria", "valor", "tipo_movimento")
)

# Gera tabela agregada de receitas e salva em Delta/Metastore
renda_entradas = financas.where(F.col("tipo_movimento") == "entrada")
renda_mensal = build_pivot_table(renda_entradas, months=MESES_ORDENADOS, total_label="TOTAL")
renda_mensal.write.format("delta").mode("overwrite").partitionBy("ano").save(P_VW_RENDA_MENSAL)
spark.sql(f"CREATE TABLE IF NOT EXISTS {T_VW_RENDA_MENSAL} USING DELTA LOCATION '{P_VW_RENDA_MENSAL}'")

# Gera a tabela agregada de despesas com a mesma estrutura
gastos = financas.where(F.col("tipo_movimento") == "saida")
gastos_mensal = build_pivot_table(gastos, months=MESES_ORDENADOS, total_label="TOTAL")
gastos_mensal.write.format("delta").mode("overwrite").partitionBy("ano").save(P_VW_GASTOS_MENSAL)
spark.sql(f"CREATE TABLE IF NOT EXISTS {T_VW_GASTOS_MENSAL} USING DELTA LOCATION '{P_VW_GASTOS_MENSAL}'")

# Libera os recursos da sessao Spark
spark.stop()
