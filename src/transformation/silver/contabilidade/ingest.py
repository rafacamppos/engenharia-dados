from pyspark.sql import functions as F
from pyspark.sql import SparkSession  
from delta import configure_spark_with_delta_pip  
from pyspark.sql import Window
from pyspark.sql.types import DoubleType, StringType
import os
# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType
from delta import configure_spark_with_delta_pip

BASE = os.getcwd()
BRONZE_PATH = "data/bronze/contabilidade"
SILVER_TABLE  = "silver.rfb_estabelecimentos"
SILVER_PATH = f"file://{BASE}/data/silver/contabilidade"



# Cria/obtém a SparkSession com Delta configurado
builder = (
    SparkSession.builder
    .appName("DeltaContabilidadeIngest")
    # [DELTA_CONFIGURE_SPARK_SESSION_WITH_EXTENSION_AND_CATALOG]
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

# Cria a SparkSession com Delta configurado
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# garante o schema/database
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("USE silver")

bronze = spark.read.format("delta").load(BRONZE_PATH)
bronze.show(5, truncate=False)

# ==========================
# FUNÇÕES DE NORMALIZAÇÃO
# ==========================
def only_digits(col):
    return F.regexp_replace(F.col(col), r"[^0-9]", "")

def clean_str(col):
    # trim, remove múltiplos espaços internos
    return F.regexp_replace(F.trim(F.col(col)), r"\s+", " ")

def to_initcap(col):
    return F.initcap(clean_str(col))

def to_upper(col):
    return F.upper(F.trim(F.col(col)))

def to_date_yyyymmdd(col):
    return F.to_date(F.col(col), "yyyyMMdd")

def cnpj_concat(cnpj_base, cnpj_ordem, cnpj_dv):
    return F.concat_ws("", F.col(cnpj_base), F.col(cnpj_ordem), F.col(cnpj_dv))

def cep_format(col_digits8):
    # Se tem 8 dígitos, vira NNNNN-NNN; senão, retorna o que veio
    return F.when(F.length(F.col(col_digits8)) == 8,
                  F.concat(F.substring(F.col(col_digits8),1,5), F.lit("-"), F.substring(F.col(col_digits8),6,3))
          ).otherwise(F.col(col_digits8))

def phone_digits(col):
    # somente números (sem validar formato completo)
    return F.regexp_replace(F.col(col), r"[^0-9]", "")

def email_clean(col):
    return F.lower(F.trim(F.col(col)))

def split_cnaes(col):
    # Divide a coluna de CNAEs secundários por vírgula, remove espaços e mantém apenas dígitos
    return F.split(F.regexp_replace(F.col(col), r"\s+", ""), ",")


# ==========================
# NORMALIZAÇÃO BASE
# ==========================
df_norm = (
    bronze
    # Identificação
    .withColumn("cnpj_base",   only_digits("cnpj_base"))
    .withColumn("cnpj_ordem",  only_digits("cnpj_ordem"))
    .withColumn("cnpj_dv",     only_digits("cnpj_dv"))
    .withColumn("cnpj",        cnpj_concat("cnpj_base","cnpj_ordem","cnpj_dv"))
    .withColumn("matriz_filial", F.col("matriz_filial").cast("int"))

    # Cadastro/situação
    .withColumn("nome_fantasia", clean_str("nome_fantasia"))
    .withColumn("situacao_cadastral", F.col("situacao_cadastral").cast("int"))
    .withColumn("data_situacao_cadastral", to_date_yyyymmdd("data_situacao_cadastral"))
    .withColumn("motivo_situacao_cadastral", clean_str("motivo_situacao_cadastral"))

    # Localização
    .withColumn("tipo_logradouro", to_upper("tipo_logradouro"))
    .withColumn("logradouro",      to_initcap("logradouro"))
    .withColumn("numero",          clean_str("numero"))
    .withColumn("complemento",     clean_str("complemento"))
    .withColumn("bairro",          to_initcap("bairro"))
    .withColumn("cep",             only_digits("cep"))
    .withColumn("cep_formatado",   cep_format("cep"))
    .withColumn("uf",              to_upper("uf"))
    .withColumn("codigo_municipio", F.col("codigo_municipio").cast("int"))

    # Contatos
    .withColumn("ddd1",       only_digits("ddd1"))
    .withColumn("telefone1",  phone_digits("telefone1"))
    .withColumn("ddd2",       only_digits("ddd2"))
    .withColumn("telefone2",  phone_digits("telefone2"))
    .withColumn("ddd_fax",    only_digits("ddd_fax"))
    .withColumn("fax",        phone_digits("fax"))
    .withColumn("email1",     email_clean("email1"))
    .withColumn("email2",     email_clean("email2"))
    .withColumn("email3",     email_clean("email3"))

    # Atividade econômica
    .withColumn("data_inicio_atividade", to_date_yyyymmdd("data_inicio_atividade"))
    .withColumn("cnae_principal", F.lpad(only_digits("cnae_principal"), 7, "0"))
    .withColumn("cnaes_secundarios", split_cnaes("cnaes_secundarios"))

    # Técnicos (mantém da Bronze; adiciona processed_at e row_hash)
    .withColumn("_processed_at", F.current_timestamp())
    .withColumn("_row_hash",
        F.sha2(
            F.concat_ws("§",
                F.coalesce(F.col("cnpj"), F.lit("")),
                F.coalesce(F.col("nome_fantasia"), F.lit("")),
                F.coalesce(F.col("uf"), F.lit("")),
                F.coalesce(F.col("codigo_municipio").cast("string"), F.lit("")),
                F.coalesce(F.col("logradouro"), F.lit("")),
                F.coalesce(F.col("numero"), F.lit(""))
            ), 256
        )
    )
)
print(f"Total normalizados: {df_norm.count()} linhas")
# ==========================
# REGRAS DE QUALIDADE (DQ)
# ==========================
email_regex = r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$"
uf_list = ["AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA",
           "PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"]

dq_df = (
    df_norm
    # CNPJ: 14 dígitos
    .withColumn("dq_cnpj_len_invalid", F.length("cnpj") != 14)
    # CEP: 8 dígitos (aceita vazio)
    .withColumn("dq_cep_invalid", (F.col("cep").isNotNull()) & (F.length("cep") > 0) & (F.length("cep") != 8))
    # UF: precisa ser um dos códigos válidos
    .withColumn("dq_uf_invalid", ~F.col("uf").isin(uf_list))
    # E-mails (se não vazios, validar padrão)
    .withColumn("dq_email1_invalid", (F.col("email1") != "") & (~F.col("email1").rlike(email_regex)))
    .withColumn("dq_email2_invalid", (F.col("email2") != "") & (~F.col("email2").rlike(email_regex)))
    .withColumn("dq_email3_invalid", (F.col("email3") != "") & (~F.col("email3").rlike(email_regex)))
    # CNAE principal: 7 dígitos (aceita vazio, mas marca inválido se diferente de 7 quando informado)
    .withColumn("dq_cnae_principal_invalid",
                (F.col("cnae_principal").isNotNull()) & (F.length("cnae_principal") > 0) & (F.length("cnae_principal") != 7))
    # Telefones: se informado, somente dígitos (já normalizado); checar tamanho mínimo (ex.: >=8)
    .withColumn("dq_tel1_invalid", (F.length("telefone1") > 0) & (F.length("telefone1") < 8))
    .withColumn("dq_tel2_invalid", (F.length("telefone2") > 0) & (F.length("telefone2") < 8))
    # Campos essenciais: CNPJ e UF
    .withColumn("dq_cnpj_null", F.col("cnpj").isNull() | (F.length("cnpj")==0))
    .withColumn("dq_uf_null",   F.col("uf").isNull()   | (F.length("uf")==0))
    # Agrega issues
    .withColumn("dq_issues",
        F.array_remove(F.array(
            F.when(F.col("dq_cnpj_null") | F.col("dq_cnpj_len_invalid"), F.lit("cnpj_invalid")),
            F.when(F.col("dq_uf_null")   | F.col("dq_uf_invalid"),       F.lit("uf_invalid")),
            F.when(F.col("dq_cep_invalid"),           F.lit("cep_invalid")),
            F.when(F.col("dq_email1_invalid"),        F.lit("email1_invalid")),
            F.when(F.col("dq_email2_invalid"),        F.lit("email2_invalid")),
            F.when(F.col("dq_email3_invalid"),        F.lit("email3_invalid")),
            F.when(F.col("dq_cnae_principal_invalid"),F.lit("cnae_principal_invalid")),
            F.when(F.col("dq_tel1_invalid"),          F.lit("telefone1_invalid")),
            F.when(F.col("dq_tel2_invalid"),          F.lit("telefone2_invalid")),
        ), None)
    )
    .withColumn("dq_invalid_flag", F.size(F.col("dq_issues")) > 0)
)
print(f"Total com DQ aplicado: {dq_df.count()} linhas")

# ==========================
# DEDUPLICAÇÃO POR CNPJ
# ==========================
w = Window.partitionBy("cnpj").orderBy(F.col("_ingestion_ts").desc_nulls_last(), F.col("_processed_at").desc())
df_dedup = (
    dq_df
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")
    .drop("rn")
)
print(f"Total deduplicados: {df_dedup.count()} linhas")
# ==========================
# ESCRITA DELTA (PARTICIONADA POR UF)
# ==========================
(df_dedup
 .write
 .format("delta")
 .mode("overwrite")  # 1ª execução; depois, prefira MERGE/append
 .partitionBy("uf")
 .save(SILVER_PATH))

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE}
USING DELTA
LOCATION '{SILVER_PATH}'
""")

spark.sql("SHOW TABLES").show()
spark.sql(""" SELECT * FROM silver.rfb_estabelecimentos """).show(truncate=False)
spark.sql(f"SELECT COUNT(*) AS total_validos FROM {SILVER_TABLE}").show()
