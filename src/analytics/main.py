import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip
from openAI import main

texto = "Quero estabelecimento das cidades do sul e nordeste com cnpj, " \
"nome_fantasia, bairro, email e data da ultima atualizacao limite 2"
query = main(texto).replace("\n", " ")
BASE = os.getcwd()
GOLD_PATH = f"file://{BASE}/data/gold/contabilidade"
P_VW_CAT   = f"{GOLD_PATH}/vw_estab_catalog"


builder = (SparkSession.builder
    .appName("CheckGold_RFB")
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog"))
spark = configure_spark_with_delta_pip(builder).getOrCreate()

gold = spark.read.format("delta").load(P_VW_CAT)
gold.createOrReplaceTempView("vw_estab_catalog")
gold.show(5, truncate=False)
spark.sql(f'describe table vw_estab_catalog').show(truncate=False)
spark.sql(query).show(truncate=False)