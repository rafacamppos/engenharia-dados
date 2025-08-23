from pyspark.sql import functions as F
from pyspark.sql import SparkSession  
from delta import configure_spark_with_delta_pip  
from pyspark.sql import Window

BRONZE_PATH = "data/bronze/fakestore/products"
SILVER_PATH = "data/silver/fakestore/products"
SILVER_QUAR_PATH = "data/silver/fakestore/products_quarantine"

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

bronze = spark.read.format("delta").load(BRONZE_PATH)

# Flatten + normalização
silver_base = (
    bronze
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
silver_norm = (
    silver_base
    .withColumn("category_mapped", mapping.getItem(F.col("category_raw")))
    .withColumn("category_unmapped", F.col("category_mapped").isNull())
    .withColumn("category", F.coalesce(F.col("category_mapped"), F.lit("unknown")))
    .drop("category_mapped", "category_raw")
)

# Qualidade de Dados (DQ) e Transformações
silver_qc = (
    silver_norm
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

# Deduplicação (mais recente por id)
w = Window.partitionBy("id").orderBy(F.col("ingestion_ts").desc_nulls_last(), F.col("processed_at").desc())
silver_dedup = (
    silver_qc
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")
    .drop("rn")
)

# Split válidos x inválidos (opcional)
valid = silver_dedup.filter(~F.col("dq_invalid_flag"))
invalid = silver_dedup.filter(F.col("dq_invalid_flag"))

# Escreve Silver (Delta)
(valid.write
   .format("delta")
   .mode("overwrite")  # 1ª vez; depois use "merge" (SCD1) ou "append" conforme estratégia
   .save(SILVER_PATH))

# Quarentena Silver
(invalid.write
   .format("delta")
   .mode("overwrite")
   .save(SILVER_QUAR_PATH))