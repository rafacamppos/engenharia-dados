from pyspark.sql import functions as F
from pyspark.sql import SparkSession  
from delta import configure_spark_with_delta_pip  
from pyspark.sql import Window
from pyspark.sql.types import DoubleType, StringType
import os

BRONZE_PRODUCTS_PATH = "data/bronze/fakestore/products"
BRONZE_USERS_PATH = "data/bronze/fakestore/users"
SILVER_PRODUCTS_PATH = "data/silver/fakestore/products"
SILVER_USERS_PATH = "data/silver/fakestore/users"
SILVER_PRODUCTS_QUAR_PATH = "data/silver/fakestore/products_quarantine"
SILVER_USERS_QUAR_PATH = "data/silver/fakestore/users_quarantine"
SILVER_USERS_TABLE  = "silver.fakestore_users"
SILVER_PRODUCTS_TABLE  = "silver.fakestore_products"

# Cria/obtém a SparkSession com Delta configurado
builder = (
    SparkSession.builder
    .appName("DeltaEnabled")
    # [DELTA_CONFIGURE_SPARK_SESSION_WITH_EXTENSION_AND_CATALOG]
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

# Cria a SparkSession com Delta configurado
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# garante o schema/database
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("USE silver")

bronze_products = spark.read.format("delta").load(BRONZE_PRODUCTS_PATH)
bronze_users = spark.read.format("delta").load(BRONZE_USERS_PATH)
bronze_products.show(5, truncate=False)
bronze_users.show(5, truncate=False)

def to_double_nullable(col):
    # tenta converter strings como " -37.31 " em double; se falhar, vira null
    return F.when(F.col(col).rlike(r"^\s*-?\d+(\.\d+)?\s*$"), F.col(col).cast(DoubleType())).otherwise(F.lit(None).cast(DoubleType()))

# -------------------------------------------------------------------
# 1) BASELINE
#    - Flatten + normalização básica
# ------------------------------------------------------------------- 
silver_users_base = (
    bronze_users
    # Flatten
    .withColumn("firstname", F.col("name.firstname"))
    .withColumn("lastname",  F.col("name.lastname"))
    .withColumn("city",      F.col("address.city"))
    .withColumn("street",    F.col("address.street"))
    .withColumn("number",    F.col("address.number"))
    .withColumn("zipcode",   F.col("address.zipcode"))
    .withColumn("lat_raw",   F.col("address.geolocation.lat"))
    .withColumn("long_raw",  F.col("address.geolocation.long"))

    # Normalizações de texto
    .withColumn("email",    F.lower(F.trim(F.col("email"))))
    .withColumn("username", F.lower(F.trim(F.col("username"))))
    .withColumn("firstname",F.initcap(F.trim(F.col("firstname"))))
    .withColumn("lastname", F.initcap(F.trim(F.col("lastname"))))
    .withColumn("city",     F.initcap(F.trim(F.col("city"))))
    .withColumn("street",   F.initcap(F.regexp_replace(F.trim(F.col("street")), r"\s+", " ")))
    .withColumn("zipcode",  F.upper(F.trim(F.col("zipcode"))))

    # Telefone: remove espaços; mantém dígitos e separadores básicos
    .withColumn("phone", F.regexp_replace(F.trim(F.col("phone")), r"[^\d\-\+\(\)\s]", ""))

    # Cast de lat/long (strings -> double, se válido)
    .withColumn("lat",  to_double_nullable("lat_raw"))
    .withColumn("long", to_double_nullable("long_raw"))

    # Derivados
    .withColumn("full_name", F.concat_ws(" ", F.col("firstname"), F.col("lastname")))
    .withColumn("address_line", F.concat_ws(", ", F.col("street"), F.col("number").cast(StringType())))
    .withColumn("processed_at", F.current_timestamp())

    # Seleção final ordenada
    .select(
        "id", "email", "username", "password", "phone",
        "firstname", "lastname", "full_name",
        "city", "street", "number", "zipcode",
        "lat", "long",
        F.col("__v").alias("_v"),
        # se sua Bronze tiver colunas técnicas como file_origin/ingestion_ts, mantenha:
        F.col("file_origin").alias("file_origin"),
        F.col("ingestion_ts").alias("ingestion_ts"),
        "processed_at"
    )
)


# -------------------------------------------------------------------
# 3) REGRAS DE QUALIDADE (DQ) LEVES
#    - Checagens simples: email válido, zipcode, latitude/longitude no range
#    - Flags e campo de motivos para troubleshooting
# -------------------------------------------------------------------
email_regex  = r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$"
zip_regex    = r"^\d{5}(-\d{4})?$"        # 12345 ou 12345-6789 (ajuste ao seu padrão)
lat_ok       = (F.col("lat").isNull()) | ((F.col("lat") >= -90) & (F.col("lat") <= 90))
long_ok      = (F.col("long").isNull())| ((F.col("long")>= -180) & (F.col("long")<=180))



silver_users_qc = (
    silver_users_base
    .withColumn("dq_email_invalid",  ~F.col("email").rlike(email_regex))
    .withColumn("dq_zip_invalid",    ~F.col("zipcode").rlike(zip_regex))
    .withColumn("dq_geo_invalid",    ~(lat_ok & long_ok))
    .withColumn(
        "dq_issues",
        F.array_remove(F.array(
            F.when(F.col("id").isNull(),              F.lit("id_null")),
            F.when(F.col("email").isNull() | F.col("dq_email_invalid"), F.lit("email_invalid")),
            F.when(F.col("username").isNull() | (F.length("username")==0), F.lit("username_empty")),
            F.when(F.col("dq_zip_invalid"),           F.lit("zipcode_invalid")),
            F.when(F.col("dq_geo_invalid"),           F.lit("geolocation_invalid")),
        ), None)
    )
    .withColumn("dq_invalid_flag", F.size(F.col("dq_issues")) > 0)
)


# -------------------------------------------------------------------
# 4) DEDUPLICAÇÃO
#    - Mantém o registro mais recente por id (usa ingestion_ts e processed_at)
# -------------------------------------------------------------------
w = Window.partitionBy("id").orderBy(F.col("ingestion_ts").desc_nulls_last(), F.col("processed_at").desc())
silver_users_dedup = (
    silver_users_qc
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)
silver_users_dedup.show(5, truncate=False)



# -------------------------------------------------------------------
# 5) ESCRITA EM DELTA (SILVER)
#    - 1ª execução: "overwrite"; depois, prefira MERGE (SCD1) por id
# -------------------------------------------------------------------
(silver_users_dedup
 .write
 .format("delta")
 .mode("append")          # 1ª vez; depois avalie MERGE/append
 .save(SILVER_USERS_PATH))


# Cria database e registra tabela corretamente
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_USERS_TABLE}
USING DELTA
LOCATION '{SILVER_USERS_PATH}'
""")


# -------------------------------------------------------------------
# 6) REGISTRO DE VIEWS/TEMP TABLES (opcional)
# -------------------------------------------------------------------

bronze_products.createOrReplaceTempView("fakestore_products")
bronze_users.createOrReplaceTempView("fakestore_users")

# -------------------------------------------------------------------
# 7) VALIDAÇÃO RÁPIDA
# -------------------------------------------------------------------
spark.table("fakestore_users").show()

# -------------------------------------------------------------------
# 8) PROCESSAMENTO SIMILAR PARA PRODUCTS
# -------------------------------------------------------------------   
silver_products_base = (
    bronze_products
    .select(
        F.col("id").cast("int").alias("id"),
        F.trim(F.col("title")).alias("title"),
        F.round(F.col("price").cast("double"), 2).alias("price"),
        F.trim(F.col("description")).alias("description"),
        F.lower(F.trim(F.col("category"))).alias("category_raw"),
        F.col("image").alias("image"),
        F.col("rating.rate").cast("double").alias("rating_rate"),
        F.col("rating.count").cast("int").alias("rating_count"),
        F.col("ingestion_ts"),
    )
    .withColumn("processed_at", F.current_timestamp())
)

# Taxonomia de categorias (exemplo simples)
mapping = F.create_map(
    F.lit("men's clothing"), F.lit("mens_clothing"),
    F.lit("women's clothing"), F.lit("womens_clothing"),
    F.lit("jewelery"), F.lit("jewelry"),
    F.lit("electronics"), F.lit("electronics")
)
# Normalização de categorias
# (pode ser substituído por uma tabela de mapeamento externa)
silver_products_normalizado = (
    silver_products_base
    .withColumn("category_mapped", mapping.getItem(F.col("category_raw")))
    .withColumn("category_unmapped", F.col("category_mapped").isNull())
    .withColumn("category", F.coalesce(F.col("category_mapped"), F.lit("unknown")))
    .drop("category_mapped", "category_raw")
)
silver_products_normalizado.show(5, truncate=False)
# Qualidade de Dados (DQ) e Transformações
silver_products_qc = (
    silver_products_normalizado
    .withColumn("price", F.when(F.col("price") < 0, None).otherwise(F.col("price")))
    .withColumn("rating_rate", F.when(~(F.col("rating_rate").between(0,5)), None).otherwise(F.col("rating_rate")))
    .withColumn("rating_count", F.when(F.col("rating_count") < 0, None).otherwise(F.col("rating_count")))
    .withColumn("image", F.when(~F.col("image").rlike(r"^https?://"), None).otherwise(F.col("image")))
    .withColumn(
        "dq_issues",
        F.array_remove(F.array(
            F.when(F.col("id").isNull(), F.lit("id_null")),
            F.when(F.col("title").isNull() | (F.length("title") == 0), F.lit("title_empty")),
            F.when(F.col("price").isNull(), F.lit("price_invalid")),
            F.when(F.col("category") == "unknown", F.lit("category_unmapped")),
        ), None)
    )
    .withColumn("dq_invalid_flag", F.size(F.col("dq_issues")) > 0)
)

# Deduplicação por id
w = Window.partitionBy("id").orderBy(F.col("ingestion_ts").desc_nulls_last(), F.col("processed_at").desc())
silver_products_dedup = (
    silver_products_qc
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")
    .drop("rn")
)
silver_products_dedup.show(5, truncate=False)
  

# Escreve Silver (Delta)
(silver_products_dedup.write
   .format("delta")
   .mode("append")  # 1ª vez; depois use "merge" (SCD1) ou "append" conforme estratégia
   .save(SILVER_PRODUCTS_PATH))



spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_PRODUCTS_TABLE}
USING DELTA
LOCATION '{SILVER_PRODUCTS_PATH}'
""")


silver_products = spark.read.format("delta").load(SILVER_PRODUCTS_PATH)
silver_users    = spark.read.format("delta").load(SILVER_USERS_PATH)
silver_products.show(5, truncate=False)
silver_users.show(5, truncate=False)
spark.table("fakestore_products").show()