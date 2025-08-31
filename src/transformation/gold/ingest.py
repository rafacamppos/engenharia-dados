# build_gold_from_silver.py
# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession, Window
from delta import configure_spark_with_delta_pip
from pyspark.sql import functions as F
import os
from delta.tables import DeltaTable

# ==========================
# PATHS (Silver → Gold)
# ==========================
SILVER_PRODUCTS_PATH = "data/silver/fakestore/products"
SILVER_USERS_PATH    = "data/silver/fakestore/users"


GOLD_DIM_CATEGORY_PATH   = os.path.abspath("data/gold/fakestore/dim_category")
GOLD_DIM_PRODUCT_PATH    = os.path.abspath("data/gold/fakestore/dim_product")
GOLD_FACT_PROD_DAILY     = os.path.abspath("data/gold/fakestore/fact_product_metrics_daily")
GOLD_VW_PROD_CATALOG     = os.path.abspath("data/gold/fakestore/vw_product_catalog")

GOLD_DIM_CITY_PATH       = os.path.abspath("data/gold/fakestore/dim_city")
GOLD_DIM_USER_PATH       = os.path.abspath("data/gold/fakestore/dim_user")
GOLD_VW_USER_DIRECTORY   = os.path.abspath("data/gold/fakestore/vw_user_directory")

# Tabelas
DIM_CATEGORY_TBL = "gold.dim_category"
DIM_PRODUCT_TBL  = "gold.dim_product"
FACT_PROD_DAILY  = "spark_catalog.gold.fact_product_metrics_daily"
VW_PROD_CATALOG  = "gold.vw_product_catalog"

DIM_CITY_TBL     = "gold.dim_city"
DIM_USER_TBL     = "gold.dim_user"
VW_USER_DIR_TBL  = "gold.vw_user_directory"



# ==========================
# Spark + Delta
# ==========================
spark = configure_spark_with_delta_pip(
    SparkSession.builder
      .appName("Delta")
      .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
).getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS gold")
spark.sql("USE gold")

# ==========================
# Ler Silver
# ==========================
silver_products = spark.read.format("delta").load(SILVER_PRODUCTS_PATH)
silver_users    = spark.read.format("delta").load(SILVER_USERS_PATH)
silver_products.show(5, truncate=False)
silver_users.show(5, truncate=False)
# ============================================================
# GOLD – PRODUCTS
# ============================================================

# 1) Dimensão de Categoria
dim_category = (
    silver_products
    .select(F.lower(F.col("category")).alias("category"))
    .where(F.col("category").isNotNull())
    .distinct()
    .withColumn("category_key", F.abs(F.hash("category")).cast("bigint"))
    .withColumn("is_active", F.lit(True))
    .withColumn("created_at", F.current_timestamp())
    .withColumn("updated_at", F.current_timestamp())
    .select("category_key","category","is_active","created_at","updated_at")
)
(dim_category.write.format("delta").mode("overwrite").save(GOLD_DIM_CATEGORY_PATH))
spark.sql(f"CREATE TABLE IF NOT EXISTS {DIM_CATEGORY_TBL} USING DELTA LOCATION '{GOLD_DIM_CATEGORY_PATH}'")

# 2) Dimensão de Produto (SCD1 simples)
# Mapeia categoria → category_key
prod_enriched = (
    silver_products.alias("s")
    .join(
        dim_category.select("category_key","category").alias("c"),
        on=F.lower(F.col("s.category")) == F.col("c.category"),
        how="left"
    )
)

dim_product = (
    prod_enriched
    .select(
        F.abs(F.hash(F.col("s.id"))).cast("bigint").alias("product_key"),
        F.col("s.id").alias("product_id"),
        F.col("s.title").alias("title"),
        F.col("s.description").alias("description"),
        F.col("s.image").alias("image"),
        F.col("c.category_key").alias("category_key"),
        F.col("s.price").alias("current_price"),
        F.col("s.rating_rate").alias("current_rating_rate"),
        F.col("s.rating_count").alias("current_rating_count"),
    )
    .dropDuplicates(["product_id"])  # SCD1 simples (último vence)
    .withColumn("is_active", F.lit(True))
    .withColumn("created_at", F.current_timestamp())
    .withColumn("updated_at", F.current_timestamp())
)
(dim_product.write.format("delta").mode("overwrite").save(GOLD_DIM_PRODUCT_PATH))
spark.sql(f"CREATE TABLE IF NOT EXISTS {DIM_PRODUCT_TBL} USING DELTA LOCATION '{GOLD_DIM_PRODUCT_PATH}'")

# 3) Fato diária de métricas de produto
#   - grão: product_key, dt
silver_prod_daily = (
    silver_products
    .withColumn("product_key", F.abs(F.hash(F.col("id"))).cast("bigint"))
    .withColumn("dt", F.to_date(F.coalesce("processed_at","ingestion_ts")))
    .select("dt","product_key","price","rating_rate","rating_count")
    .where(F.col("dt").isNotNull())
)

# Estatísticas globais para score bayesiano
stats = silver_prod_daily.select(
    F.avg("rating_rate").alias("m_global"),
    F.lit(20).alias("C")  # prior mínimo de votos (ajuste ao seu contexto)
).first()

if stats is not None:
    m_global = float(stats["m_global"] or 0.0)
    C = int(stats["C"])
else:
    m_global = 0.0
    C = 20

w_cum = Window.partitionBy("product_key").orderBy("dt").rowsBetween(Window.unboundedPreceding, Window.currentRow)

fact_product_metrics_daily = (
    silver_prod_daily
    .withColumn("rating_count_cum", F.sum(F.coalesce("rating_count", F.lit(0))).over(w_cum))
    .withColumn("rating_count_prev", F.lag("rating_count_cum").over(Window.partitionBy("product_key").orderBy("dt")))
    .withColumn("rating_count_incremental", F.coalesce(F.col("rating_count_cum") - F.col("rating_count_prev"), F.col("rating_count_cum")))
    .withColumn(
        "rating_bayes_score",
        ((F.lit(C) * F.lit(m_global)) + (F.col("rating_rate") * F.col("rating_count_cum"))) /
        (F.lit(C) + F.col("rating_count_cum"))
    )
    .select("dt","product_key","price","rating_rate","rating_count_incremental","rating_count_cum","rating_bayes_score")
)
fact_product_metrics_daily.show(20, truncate=False)
(fact_product_metrics_daily.write
 .format("delta")
 .mode("overwrite")
 .partitionBy("dt")
 .save(GOLD_FACT_PROD_DAILY))

df_fact = spark.read.format("delta").load(GOLD_FACT_PROD_DAILY)
df_fact.show(20, truncate=False)


# 1) ver namespace atual
spark.sql("SHOW CURRENT NAMESPACE").show(truncate=False)

spark.sql(f"""
  CREATE TABLE gold.fact_product_metrics_daily
  USING DELTA
  LOCATION '{GOLD_FACT_PROD_DAILY}'
""")

# 2) listar tabelas no gold
spark.sql("SHOW TABLES IN gold").show(truncate=False)

# 3) checar se existe no catálogo
print("tableExists(catalogado)?", spark.catalog.tableExists("gold.fact_product_metrics_daily"))

# 4) descrever e descobrir o path físico
describe = spark.sql("DESCRIBE EXTENDED gold.fact_product_metrics_daily").show(truncate=False)

# 5) garantir metadados atualizados
spark.sql("REFRESH TABLE gold.fact_product_metrics_daily")

# 6) CONSULTAS CORRETAS

# 6a) usando a variável Python (se você tem FACT_PROD_DAILY = 'gold.fact_product_metrics_daily')
df = spark.sql(f"SELECT * FROM {FACT_PROD_DAILY} LIMIT 20")
df.show(truncate=False)

# 6b) usando nome totalmente qualificado (evita problemas de 'USE <schema>')
spark.sql("SELECT * FROM spark_catalog.gold.fact_product_metrics_daily LIMIT 20").show(truncate=False)

# 6c) lendo direto pelo path registrado (pega o 'Location' do DESCRIBE EXTENDED)
gold_fact_path = "file:/Users/rafaelcampos/git/engenharia-dados/data/gold/fakestore/fact_product_metrics_daily"
spark.read.format("delta").load(gold_fact_path).show(20, truncate=False)

# ============================================================
# View “wide” – Catálogo de Produtos
# ============================================================
spark.sql(f"CREATE TABLE IF NOT EXISTS {FACT_PROD_DAILY} USING DELTA LOCATION '{GOLD_FACT_PROD_DAILY}'")
spark.read.format("delta").load(GOLD_FACT_PROD_DAILY).show(10)

spark.sql(f"SELECT * FROM {FACT_PROD_DAILY}")


last_dt = spark.sql(f"""
SELECT product_key, MAX(dt) AS dt
FROM gold.fact_product_metrics_daily
GROUP BY product_key
""")




# ------------ Sessão Spark com Delta (Spark 4.0.0 + Delta 4.0.0) ------------
builder = (
    SparkSession.builder
      .appName("MaterializeProductCatalog")
      .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# ------------ Tabelas/paths ------------
DIM_PRODUCT_TBL = "spark_catalog.gold.dim_product"
DIM_CATEGORY_TBL = "spark_catalog.gold.dim_category"
FACT_PROD_DAILY  = "spark_catalog.gold.fact_product_metrics_daily"

VW_PROD_CATALOG_TBL  = "spark_catalog.gold.vw_product_catalog"
VW_PROD_CATALOG_PATH = os.path.abspath("data/gold/fakestore/vw_product_catalog")

# ------------ Garantir database gold ------------
spark.sql("CREATE DATABASE IF NOT EXISTS spark_catalog.gold")

# ------------ Montar o 'latest por produto' via Window (sem last_dt auxiliar) ------------
p = spark.table(DIM_PRODUCT_TBL).alias("p")
c = spark.table(DIM_CATEGORY_TBL).alias("c")
f = spark.table(FACT_PROD_DAILY).alias("f")

w = Window.partitionBy("f.product_key").orderBy(F.col("f.dt").desc())

latest_df = (
    p.join(c, "category_key")
     .join(f, "product_key")
     .withColumn("rn", F.row_number().over(w))
     .filter("rn = 1")
     .select(
         F.col("p.product_id").alias("id"),
         F.col("p.title").alias("title"),
         F.col("c.category").alias("category"),
         F.col("p.current_price").alias("current_price"),
         F.col("p.current_rating_rate").alias("current_rating_rate"),
         F.col("p.current_rating_count").alias("current_rating_count"),
         F.col("f.rating_bayes_score").alias("rating_bayes_score"),
         F.col("p.image").alias("image"),
         F.col("f.dt").alias("last_dt"),
     )
)

# ------------ Materialização incremental (CREATE + MERGE SCD1) ------------
# 1ª carga: cria o Delta e registra no catálogo
(latest_df.write
    .format("delta")
    .mode("append")
    .save(VW_PROD_CATALOG_PATH))

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {VW_PROD_CATALOG_TBL}
    USING DELTA
    LOCATION '{VW_PROD_CATALOG_PATH}'
""")


# ------------ Validação rápida ------------
spark.sql(f"REFRESH TABLE {VW_PROD_CATALOG_TBL}")
spark.sql(f"SELECT * FROM {VW_PROD_CATALOG_TBL} ORDER BY last_dt DESC LIMIT 20").show(truncate=False)

# ============================================================
# GOLD – USERS
# ============================================================

# 1) Dimensão de Cidade (normalização simples)
dim_city = (
    silver_users
    .select(
        F.initcap(F.trim(F.col("city"))).alias("city"),
        F.upper(F.trim(F.col("zipcode"))).alias("zipcode")
    )
    .where(F.col("city").isNotNull() | F.col("zipcode").isNotNull())
    .dropDuplicates()
    .withColumn("city_key", F.abs(F.hash(F.coalesce("city", F.lit("")), F.coalesce("zipcode", F.lit("")))).cast("bigint"))
    .withColumn("is_active", F.lit(True))
    .withColumn("created_at", F.current_timestamp())
    .withColumn("updated_at", F.current_timestamp())
    .select("city_key","city","zipcode","is_active","created_at","updated_at")
)
(dim_city.write.format("delta").mode("overwrite").save(GOLD_DIM_CITY_PATH))
spark.sql(f"CREATE TABLE IF NOT EXISTS {DIM_CITY_TBL} USING DELTA LOCATION '{GOLD_DIM_CITY_PATH}'")

# 2) Dimensão de Usuário (SCD1 simples)
dim_user = (
    silver_users.alias("u")
    .join(dim_city.alias("c"),
          (F.initcap(F.trim(F.col("u.city"))) == F.col("c.city")) &
          (F.upper(F.trim(F.col("u.zipcode"))) == F.col("c.zipcode")),
          how="left")
    .select(
        F.abs(F.hash(F.col("u.id"))).cast("bigint").alias("user_key"),
        F.col("u.id").alias("user_id"),
        F.col("u.full_name").alias("full_name"),
        F.col("u.firstname").alias("first_name"),
        F.col("u.lastname").alias("last_name"),
        F.col("u.email").alias("email"),
        F.col("u.username").alias("username"),
        F.col("u.phone").alias("phone"),
        F.col("c.city_key").alias("city_key"),
        F.col("u.lat").alias("latitude"),
        F.col("u.long").alias("longitude"),
    )
    .dropDuplicates(["user_id"])
    .withColumn("is_active", F.lit(True))
    .withColumn("created_at", F.current_timestamp())
    .withColumn("updated_at", F.current_timestamp())
)
(dim_user.write.format("delta").mode("overwrite").save(GOLD_DIM_USER_PATH))
spark.sql(f"CREATE TABLE IF NOT EXISTS {DIM_USER_TBL} USING DELTA LOCATION '{GOLD_DIM_USER_PATH}'")

# 3) View “wide” – Diretório de usuários
vw_user_directory = (
    spark.table(DIM_USER_TBL).alias("u")
    .join(spark.table(DIM_CITY_TBL).alias("c"), "city_key", "left")
    .select(
        F.col("u.user_id").alias("id"),
        "u.full_name","u.first_name","u.last_name",
        "u.email","u.username","u.phone",
        "c.city","c.zipcode",
        "u.latitude","u.longitude",
        "u.is_active","u.updated_at"
    )
)
(vw_user_directory.write.format("delta").mode("overwrite").save(GOLD_VW_USER_DIRECTORY))
spark.sql(f"CREATE TABLE IF NOT EXISTS {VW_USER_DIR_TBL} USING DELTA LOCATION '{GOLD_VW_USER_DIRECTORY}'")

# ============================================================
# CONTRATOS (constraints) – Exemplo
# ============================================================
# Products
spark.sql(f"ALTER TABLE {DIM_PRODUCT_TBL} ALTER COLUMN product_id SET NOT NULL")
spark.sql(f"ALTER TABLE {DIM_CATEGORY_TBL} ALTER COLUMN category_key SET NOT NULL")
spark.sql(f"ALTER TABLE {FACT_PROD_DAILY}  ADD CONSTRAINT chk_rate_range CHECK (rating_rate IS NULL OR (rating_rate >= 0 AND rating_rate <= 5))")

# Users
spark.sql(f"ALTER TABLE {DIM_USER_TBL} ALTER COLUMN user_id SET NOT NULL")

# ============================================================
# VALIDAÇÕES RÁPIDAS
# ============================================================
print("Gold tables:")
spark.sql("SHOW TABLES IN gold").show(truncate=False)

spark.table(VW_PROD_CATALOG).show(5, truncate=False)
spark.table(VW_USER_DIR_TBL).show(5, truncate=False)

spark.stop()