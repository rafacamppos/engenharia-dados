from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
import os
from delta import configure_spark_with_delta_pip

# --------- paths/tabelas ---------
BASE = os.getcwd()
SILVER_PATH = f"file://{BASE}/data/silver/contabilidade"
GOLD_PATH = f"file://{BASE}/data/gold/contabilidade"
P_DIM_LOC  = f"{GOLD_PATH}/dim_localidade"
P_DIM_CNAE = f"{GOLD_PATH}/dim_cnae"
P_DIM_EST  = f"{GOLD_PATH}/dim_estabelecimento"
P_FACT_SNP = f"{GOLD_PATH}/fact_estab_snapshot_daily"
P_FACT_LOC = f"{GOLD_PATH}/fact_estab_metrics_by_localidade"
P_VW_CAT   = f"{GOLD_PATH}/vw_estab_catalog"

T_DIM_LOC  = "gold.dim_localidade"
T_DIM_CNAE = "gold.dim_cnae"
T_DIM_EST  = "gold.dim_estabelecimento"
T_FACT_SNP = "gold.fact_estab_snapshot_daily"
T_FACT_LOC = "gold.fact_estab_metrics_by_localidade"
T_VW_CAT   = "gold.vw_estab_catalog"

# --------- spark + delta ---------
builder = (SparkSession.builder
    .appName("BuildGold_RFB")
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog"))
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.getConf().get("spark.jars.packages")
spark.sql("SHOW DATABASES").show(truncate=False)
spark.sql("CREATE DATABASE IF NOT EXISTS gold")
spark.sql("USE gold")

silver = spark.read.format("delta").load(SILVER_PATH)


# --------- dim_localidade ---------
dim_localidade = (
    silver
    .select("uf","codigo_municipio")
    .where(F.col("uf").isNotNull() & F.col("codigo_municipio").isNotNull())
    .dropDuplicates(["uf","codigo_municipio"])
    .withColumn("localidade_key",
        F.abs(F.hash(F.col("uf"), F.col("codigo_municipio"))).cast("bigint"))
    .withColumn("is_active", F.lit(True))
    .withColumn("created_at", F.current_timestamp())
    .withColumn("updated_at", F.current_timestamp())
    .select("localidade_key","uf","codigo_municipio",
            "is_active","created_at","updated_at")
)
dim_localidade.write.format("delta").mode("overwrite").save(P_DIM_LOC)
spark.sql(f"CREATE TABLE IF NOT EXISTS {T_DIM_LOC} USING DELTA LOCATION '{P_DIM_LOC}'")
spark.sql(f"DESCRIBE TABLE {T_DIM_LOC}").show()

# --------- dim_cnae ---------
dim_cnae = (
    silver
    .select("cnae_principal")
    .where(F.col("cnae_principal").isNotNull() & (F.length("cnae_principal")==7))
    .dropDuplicates()
    .withColumn("cnae_key",
        F.abs(F.hash(F.col("cnae_principal"))).cast("bigint"))
    .withColumn("is_active", F.lit(True))
    .withColumn("created_at", F.current_timestamp())
    .withColumn("updated_at", F.current_timestamp())
    .select("cnae_key","cnae_principal","is_active","created_at","updated_at")
)
dim_cnae.write.format("delta").mode("overwrite").save(P_DIM_CNAE)
spark.sql(f"CREATE TABLE IF NOT EXISTS {T_DIM_CNAE} USING DELTA LOCATION '{P_DIM_CNAE}'")
spark.sql(f"DESCRIBE TABLE {T_DIM_CNAE}").show()

# --------- dim_estabelecimento (SCD1 simples) ---------
dim_estab = (
    silver.alias("s")
    .join(dim_localidade.alias("l"),
          (F.col("s.uf")==F.col("l.uf")) & (F.col("s.codigo_municipio")==F.col("l.codigo_municipio")),
          "left")
    .join(dim_cnae.alias("c"), F.col("s.cnae_principal")==F.col("c.cnae_principal"), "left")
    .select(
        F.abs(F.hash(F.col("s.cnpj"))).cast("bigint").alias("estab_key"),
        F.col("s.cnpj"),
        F.col("s.nome_fantasia"),
        F.col("s.matriz_filial"),
        F.col("l.localidade_key"),
        F.col("c.cnae_key"),
        F.col("s.cep"), F.col("s.cep_formatado"),
        F.col("s.tipo_logradouro"), F.col("s.logradouro"),
        F.col("s.numero"), F.col("s.complemento"), F.col("s.bairro"),
        F.col("s.email1"), F.col("s.telefone1")
    )
    .dropDuplicates(["cnpj"])
    .withColumn("is_active", F.lit(True))
    .withColumn("created_at", F.current_timestamp())
    .withColumn("updated_at", F.current_timestamp())
)
dim_estab.write.format("delta").mode("overwrite").save(P_DIM_EST)
spark.sql(f"CREATE TABLE IF NOT EXISTS {T_DIM_EST} USING DELTA LOCATION '{P_DIM_EST}'")
spark.sql(f"DESCRIBE TABLE {T_DIM_EST}").show()

# --------- fato: snapshot diário ---------
df_snap = (
    silver
    .withColumn("estab_key", F.abs(F.hash(F.col("cnpj"))).cast("bigint"))
    .withColumn("cnae_key",  F.abs(F.hash(F.col("cnae_principal"))).cast("bigint"))
    .withColumn("localidade_key",
        F.abs(F.hash(F.col("uf"), F.col("codigo_municipio"))).cast("bigint"))
    .withColumn("dt", F.to_date(
        F.coalesce("data_situacao_cadastral","data_inicio_atividade", F.current_date().cast("date"))
    ))
    .withColumn("is_ativo",  F.when(F.col("situacao_cadastral")==2, F.lit(1)).otherwise(F.lit(0)))  # ajuste o código que representa "ATIVA"
    .withColumn("is_matriz", F.when(F.col("matriz_filial")==1, 1).otherwise(0))
    .select("dt","estab_key","cnae_key","localidade_key","situacao_cadastral","is_ativo","is_matriz")
    .where(F.col("dt").isNotNull())
)
(df_snap.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("dt")
    .save(P_FACT_SNP))
spark.sql(f"CREATE TABLE IF NOT EXISTS {T_FACT_SNP} USING DELTA LOCATION '{P_FACT_SNP}'")
spark.sql(f"DESCRIBE TABLE {T_FACT_SNP}").show()

# --------- fato agregado por localidade ---------
fact_loc = (
    df_snap.alias("f")
    .join(dim_localidade.alias("l"), "localidade_key")
    .groupBy("f.dt","l.uf","l.codigo_municipio")
    .agg(
        F.countDistinct("estab_key").alias("qtd_estab_total"),
        F.sum("is_ativo").alias("qtd_estab_ativos"),
        F.sum("is_matriz").alias("qtd_matrizes"),
    )
)
(fact_loc.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("dt")
    .save(P_FACT_LOC))
spark.sql(f"CREATE TABLE IF NOT EXISTS {T_FACT_LOC} USING DELTA LOCATION '{P_FACT_LOC}'")
spark.sql(f"DESCRIBE TABLE {T_FACT_LOC}").show()

# --------- view wide (catálogo) materializada em Delta ---------
vw_catalog = (
    dim_estab.alias("e")
    .join(dim_localidade.alias("l"), "localidade_key", "left")
    .join(dim_cnae.alias("c"), "cnae_key", "left")
    .select(
        "e.estab_key","e.cnpj","e.nome_fantasia","e.matriz_filial",
        "l.uf","l.codigo_municipio",
        "c.cnae_principal",
        "e.cep_formatado","e.tipo_logradouro","e.logradouro","e.numero","e.complemento","e.bairro",
        "e.email1","e.telefone1",
        "e.updated_at"
    )
)
vw_catalog.write.format("delta").mode("overwrite").save(P_VW_CAT)
spark.sql(f"CREATE TABLE IF NOT EXISTS {T_VW_CAT} USING DELTA LOCATION '{P_VW_CAT}'")
spark.sql(f"SELECT cnpj, nome_fantasia, bairro  FROM gold.vw_estab_catalog  WHERE uf IN ('BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE')  LIMIT 5").show()

# --------- validações rápidas ---------
#print("Tabelas em gold:")
#spark.sql("SHOW TABLES IN gold").show(truncate=False)
#spark.table(T_VW_CAT).show(5, truncate=False)
#spark.table(T_FACT_LOC).orderBy(F.desc("dt")).show(5, truncate=False)

spark.stop()