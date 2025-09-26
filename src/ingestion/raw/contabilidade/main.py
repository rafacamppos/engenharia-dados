"""Módulo de ingestão e utilidades para persistência.

Boas práticas aplicadas:
- Tipagem explícita e docstrings consistentes.
- Separação de responsabilidades em funções pequenas e testáveis.
- Escrita segura em caminho local e função utilitária para Parquet.
"""

from __future__ import annotations

import io
import gzip
import json
import os
import uuid
from datetime import date
from typing import Any, Dict, Iterable, List, Tuple


from pyspark.sql import DataFrame, SparkSession
import pandas as pd



# Ex.: "data/raw" ou "s3a://raw"
BASE_RAW_PATH = os.getenv("BASE_RAW_PATH", "data/raw")
SOURCE_NAME = os.getenv("SOURCE_NAME", "contabilidade")
MAX_LINES_PER_FILE = int(os.getenv("MAX_LINES_PER_FILE", "20000"))

# Config de acesso S3/MinIO (não utilizado no momento para escrita, mantido para futura integração)
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "")
AWS_ACCESS = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "")

# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================


def is_s3a(path: str) -> bool:
    """
    Verifica se o caminho informado está no formato s3a://
    para decidir se a escrita/leitura será via Hadoop FS.
    """
    return path.startswith("s3a://")


def ensure_local_dir(path: str) -> None:
    """
    Garante que o diretório local exista antes de salvar arquivos.
    Ignora caminhos s3a:// (MinIO/S3), pois neles o FS é virtual.
    """
    if not is_s3a(path):
        os.makedirs(path, exist_ok=True)


def join_path(*parts: str) -> str:
    """
    Junta partes de um caminho, respeitando o esquema s3a:// se presente.
    Remove barras duplicadas e normaliza o separador.
    """
    clean = [p.strip("/").replace("\\", "/") for p in parts if p]
    if not clean:
        return ""
    if clean[0].startswith("s3a://"):  # preserva o esquema s3a
        return "/".join(clean)
    return "/".join(clean)


def jsonl_gz_bytes(rows: Iterable[Dict[str, Any]]) -> Tuple[bytes, int]:
    """
    Converte uma lista de dicionários Python para bytes no formato NDJSON comprimido (gzip).
    Cada objeto JSON ocupa uma linha.
    Retorna (bytes, total_de_linhas).
    """
    buf = io.BytesIO()
    count = 0
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        for obj in rows:
            gz.write((json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8"))
            count += 1
    return buf.getvalue(), count


def chunk_by_lines(data: Sequence[Dict[Any, Any]], max_lines: int) -> Iterable[Sequence[Dict[Any, Any]]]:
    """
    Divide a lista de registros em chunks menores com até `max_lines` linhas cada.
    Usado para evitar arquivos muito grandes na RAW.
    """
    for i in range(0, len(data), max_lines):
        yield data[i:i + max_lines]


def write_bytes(dest_path: str, blob: bytes, spark: SparkSession) -> None:
    """
    Escreve um arquivo de bytes:
    - Se for local, salva dentro da pasta 'data' do projeto.
    - Se for s3a://, envia via Hadoop FS, usando 'data/tmp' como pasta temporária local.
    """
    # Garante caminho base do projeto
    base_data_path = os.getcwd()
    ensure_local_dir(base_data_path)

    if not is_s3a(dest_path):
        # Se for caminho local, salva dentro da pasta data/
        full_dest_path = os.path.join(base_data_path, dest_path.lstrip("/"))
        ensure_local_dir(os.path.dirname(full_dest_path))
        with open(full_dest_path, "wb") as f:
            f.write(blob)
        return

    # Pasta temporária local para escrita de arquivos antes do upload S3
    tmp_dir = os.path.join(base_data_path, "tmp")
    ensure_local_dir(tmp_dir)
    tmp_file = os.path.join(tmp_dir, f"{uuid.uuid4().hex}.bin")

    with open(tmp_file, "wb") as f:
        f.write(blob)

    # Integração S3 via Hadoop FS pode ser reativada quando necessário.
    # Mantido propositalmente desativado para reduzir acoplamento e facilitar testes locais.





def _exists_and_nonempty_local(path: str) -> bool:
    """Retorna True se o arquivo local existir e tiver tamanho > 0."""
    return os.path.isfile(path) and os.path.getsize(path) > 0



def _preview_json_valid_local(path: str, max_lines: int = 3) -> bool:
    """Abre .jsonl.gz localmente e valida as primeiras linhas como JSON."""
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            for i, line in enumerate(f):
                if i >= max_lines:
                    break
                json.loads(line)  # levanta se inválido
        return True
    except Exception:
        return False

def _preview_json_valid_s3(spark: SparkSession, path: str, max_lines: int = 3) -> bool:
    """
    Usa Spark para ler texto de um .jsonl.gz em s3a:// e validar algumas linhas.
    Spark lida com gzip automaticamente.
    """
    try:
        df = spark.read.text(path)  # lê como texto (cada linha do jsonl)
        rows = [r.value for r in df.limit(max_lines).collect()]
        for line in rows:
            json.loads(line)  # levanta se inválido
        return True
    except ( Exception):
        return False

def _validate_saved_file(spark: SparkSession, path: str, max_preview: int = 3) -> None:
    """
    Valida a existência, tamanho e conteúdo JSON do arquivo salvo.
    Lança RuntimeError se algo estiver errado.
    """
    
    exists = _exists_and_nonempty_local(path)
    content_ok = _preview_json_valid_local(path, max_preview)

    if not exists:
        raise RuntimeError(f"Arquivo não encontrado ou vazio: {path}")
    if not content_ok:
        raise RuntimeError(f"Conteúdo inválido (JSON) no arquivo: {path}")

from typing import Sequence

def save_entity_to_raw(
    items: Sequence[Dict[Any, Any]],
    spark: SparkSession,
    data_atual: str,
    batch_id: str,
) -> None:
    """
    Salva um conjunto de registros de uma entidade (products, users, etc.) na RAW:
    - Define o caminho base com particionamento temporal e batch_id.
    - Divide em chunks e gera arquivos NDJSON comprimidos (.jsonl.gz).
    - Salva cada arquivo localmente ou no bucket S3/MinIO.
    - Após salvar, valida existência, tamanho e algumas linhas JSON.
    """
    if not items:
        return

    base_entity = join_path(BASE_RAW_PATH, SOURCE_NAME, data_atual, batch_id)
    if not is_s3a(base_entity):
        ensure_local_dir(base_entity)

    for seq, chunk in enumerate(chunk_by_lines(items, MAX_LINES_PER_FILE), start=1):
        blob, _ = jsonl_gz_bytes(chunk)
        fname = f"part-{uuid.uuid4().hex}-{seq:05d}.jsonl.gz"
        dest  = join_path(base_entity, fname)

        # grava
        write_bytes(dest, blob, spark)

        # valida pós-escrita (existência, tamanho, e conteúdo JSON)
        _validate_saved_file(spark, dest, max_preview=3)

        # opcional: log
        print(f"✔ salvo e validado: {dest}")


def save_to_parquet(entity: str, items: List[Dict[str, Any]], output_path: str, spark: SparkSession) -> str:
    """Salva uma lista de dicionários como Parquet usando Spark.

    - Cria um DataFrame a partir de `items` (inferencia de schema).
    - Escreve em `output_path` no modo overwrite.
    - Retorna o caminho de saída para conveniência/chaining.

    Observações:
    - O caminho Parquet é um diretório; o sufixo ``.parquet`` é opcional.
    - Suporta campos aninhados (ex.: ``rating`` em products).
    """
    if not items:
        # Cria um DF vazio com uma coluna dummy para materializar diretório vazio de forma consistente
        # Porém, preferimos levantar erro para evitar outputs vazios silenciosos.
        raise ValueError("'items' não pode ser vazio para escrita Parquet")

    df: DataFrame = spark.createDataFrame(items)
    # Boas práticas: padronizar colunas e evitar nomes com espaços, mas manteremos o input fiel aqui.
    # Escrita idempotente no contexto de testes.
    df.write.mode("overwrite").parquet(output_path)
    return output_path

# ============================================================
# MAIN
# ============================================================

def main() -> None:
    """
    Orquestra a ingestão RAW:
    - Configura Spark (com suporte a S3/MinIO se necessário).
    - Busca dados da API para 'products' e 'users'.
    - Gera partições por ano/mês/dia/hora + batch_id único.
    - Salva na camada RAW no formato NDJSON .jsonl.gz.
    """
    # Spark Session (necessária para escrita em s3a://)
    builder = (
        SparkSession.builder
        .appName("ContabilidadeToRaw")
        .config("spark.driver.memory", "2g")
    )
    
    spark = builder.getOrCreate()

    # Busca dados da API
    estabecimentos = pd.read_csv("resources/estabelecimentos_50k.csv").to_dict(orient="records")

    
    batch_id = uuid.uuid4().hex
    data_atual = date.today().isoformat()

    # Salva entidades
    save_entity_to_raw(estabecimentos, spark, data_atual, batch_id)
    

    print("✅ RAW gravada em:", join_path(BASE_RAW_PATH, SOURCE_NAME, "contabilidade", data_atual, batch_id))
    spark.stop()


if __name__ == "__main__":
    main()

