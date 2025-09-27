"""Módulo de ingestão e utilidades para persistência.

Boas práticas aplicadas:
- Tipagem explícita e docstrings consistentes.
- Separação de responsabilidades em funções pequenas e testáveis.
- Escrita segura em caminho local e função utilitária para Parquet.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import date
from typing import Any, Sequence

from pyspark.sql import SparkSession

from ingestion_raw import join_path, save_entity_to_raw


RESOURCES_PATH = os.path.join(os.getcwd(), "resources", "financas.json")
SOURCE_NAME = os.getenv("SOURCE_NAME", "financas")
BASE_RAW_PATH = os.getenv("BASE_RAW_PATH", "data/raw")


def load_financas() -> Sequence[dict[str, Any]]:
    """Carrega o arquivo JSON de financas em memoria."""
    if not os.path.exists(RESOURCES_PATH):
        raise FileNotFoundError(f"Arquivo nao encontrado: {RESOURCES_PATH}")

    with open(RESOURCES_PATH, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    if not isinstance(data, list):
        raise ValueError("O arquivo financas.json deve conter uma lista de registros.")

    return data


def build_spark() -> SparkSession:
    """Sessao minima apenas para compatibilidade com utilitarios de escrita."""
    return SparkSession.builder.appName("FinancasToRaw").getOrCreate()


def main() -> None:
    spark = build_spark()

    try:
        financas_data = load_financas()

        if not financas_data:
            raise ValueError("Nenhum registro encontrado em financas.json.")

        # Ajusta o SOURCE_NAME para salvar sob contabilidade/financas
        source_name = join_path(SOURCE_NAME, "financas")

        batch_id = uuid.uuid4().hex
        data_atual = date.today().isoformat()

        save_entity_to_raw(financas_data, spark, data_atual, batch_id, SOURCE_NAME)

        print(
            "RAW finanças salva em:",
            join_path(
                BASE_RAW_PATH,
                source_name,
                data_atual,
                batch_id,
            ),
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
