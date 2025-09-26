#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Spark Thrift Server + Delta Lake 4.0.0 (Spark 4.x)
# ============================================================

# 1) Paths do seu ambiente (ajuste se necessário)
export SPARK_HOME="${SPARK_HOME:-/opt/spark}"                # caminho do Spark 4.x
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk}" # Java 11 ou 17 recomendado

# 2) Network/warehouse
PORT="${PORT:-10000}"
HOST="${HOST:-0.0.0.0}"
WAREHOUSE_DIR="${WAREHOUSE_DIR:-file:///$PWD/spark-warehouse}"  # use file:/// ABSOLUTO

# 3) Pacotes (Delta 4.0.0 para Scala 2.12, Spark 4.x)
DELTA_COORD="io.delta:delta-spark_2.13:4.0.0"

# 4) S3/MinIO (opcional) — defina USE_S3=true para habilitar
USE_S3="${USE_S3:-false}"  # true|false
S3_PACKAGES="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
S3_ENDPOINT="${S3_ENDPOINT:-http://minio:9000}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-admin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-admin12345}"
S3_PATH_STYLE="${S3_PATH_STYLE:-true}"
S3_SSL_ENABLED="${S3_SSL_ENABLED:-false}"

# 5) Recursos (ajuste conforme sua máquina)
DRIVER_MEM="${DRIVER_MEM:-4g}"
SHUFFLE_PARTITIONS="${SHUFFLE_PARTITIONS:-200}"

# ------------------------------------------------------------
# Montagem de pacotes/jars
PKGS="$DELTA_COORD"
if [[ "$USE_S3" == "true" ]]; then
  PKGS="$PKGS,$S3_PACKAGES"
fi

# Opções comuns
COMMON_OPTS=(
  --master "'local[*]'"  
  --hiveconf "hive.server2.thrift.port=$PORT"
  --hiveconf "hive.server2.thrift.bind.host=$HOST"
  --conf "spark.driver.memory=$DRIVER_MEM"
  --conf "spark.sql.shuffle.partitions=$SHUFFLE_PARTITIONS"
  --conf "spark.sql.warehouse.dir=$WAREHOUSE_DIR"

 
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
)

# Config S3/MinIO (se habilitado)
if [[ "$USE_S3" == "true" ]]; then
  COMMON_OPTS+=(
    --conf "spark.hadoop.fs.s3a.endpoint=$S3_ENDPOINT"
    --conf "spark.hadoop.fs.s3a.access.key=$S3_ACCESS_KEY"
    --conf "spark.hadoop.fs.s3a.secret.key=$S3_SECRET_KEY"
    --conf "spark.hadoop.fs.s3a.path.style.access=$S3_PATH_STYLE"
    --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=$S3_SSL_ENABLED"
  )
fi

echo ">> Iniciando Spark Thrift Server com Delta 4.0.0 na porta $PORT"
echo ">> SPARK_HOME: $SPARK_HOME"
echo ">> Pacotes: $PKGS"

# Execução
"$SPARK_HOME/sbin/start-thriftserver.sh" \
  --packages "$PKGS" \
  "${COMMON_OPTS[@]}"

echo ">> Pronto! Conecte no DBeaver (Apache Spark / HiveServer2):"
echo "   Host: $HOST | Porta: $PORT | Database: default"