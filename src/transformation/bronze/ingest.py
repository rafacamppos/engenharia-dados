from pyspark.sql import SparkSession
import json

# 1. Cria/obtém a SparkSession
spark = SparkSession.builder \
    .appName("LerParquet") \
    .getOrCreate()

# 2. Lê um diretório ou arquivo Parquet
#    Você pode passar um caminho único ou uma lista de caminhos/globs:
df = spark.read.parquet("data/bronze/products/products_20250809/")  
# ou, por exemplo:
# df = spark.read.parquet("s3a://bucket/path/*.parquet")

# 3. Exibe as primeiras linhas no console
df.show(truncate=False)

# 4. (Opcional) Imprime o esquema para entender os tipos de colunas
df.printSchema()

# 5. Se quiser coletar em Python e imprimir como lista de dicts:
data = df.collect()
for row in data:
    #print(row.asDict())
    print(json.dumps(row.asDict(), indent=2, ensure_ascii=False))

# 6. Encerra a sessão
spark.stop()