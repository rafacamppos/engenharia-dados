# nl2sql_app.py
# pip install langchain-openai python-dotenv

from __future__ import annotations
import os
import re
from dataclasses import dataclass
from typing import Optional, Union

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from pydantic import SecretStr


# =========================
# 1) Configuração do app
# =========================
@dataclass(frozen=True)
class AppConfig:
    """
    Responsável por carregar configurações do ambiente.
    - Lê .env (se existir)
    - Expõe a OPENAI_API_KEY e parâmetros de modelo/guardrails.
    """
    openai_api_key: str
    model: str = "gpt-4o-mini"
    temperature: float = 0.0
    default_limit: int = 50              # LIMIT padrão caso o LLM não inclua
    enforce_limit: bool = True           # força adicionar LIMIT se faltar
    allowed_schemas: Optional[list[str]] = None  # ex.: ["gold"]

    @staticmethod
    def load() -> "AppConfig":
        load_dotenv()  # carrega .env se existir
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("Defina OPENAI_API_KEY no ambiente (.env ou variável de ambiente).")

        # Permite sobrepor via variáveis de ambiente
        model = os.getenv("LLM_MODEL", "gpt-4o-mini")
        temperature = float(os.getenv("LLM_TEMPERATURE", "0.0"))
        default_limit = int(os.getenv("DEFAULT_LIMIT", "50"))
        enforce_limit_str = os.getenv("ENFORCE_LIMIT", "true").strip().lower()
        enforce_limit = enforce_limit_str in {"1", "true", "yes", "on"}

        allowed_raw = os.getenv("ALLOWED_SCHEMAS")
        allowed_schemas = (
            [s.strip() for s in allowed_raw.split(",") if s.strip()] if allowed_raw else None
        )

        return AppConfig(
            openai_api_key=api_key,
            model=model,
            temperature=temperature,
            default_limit=default_limit,
            enforce_limit=enforce_limit,
            allowed_schemas=allowed_schemas,
        )


# =========================
# 2) Cliente do LLM
# =========================
class LLMClient:
    """
    Encapsula a criação do cliente ChatOpenAI para manter responsabilidades isoladas.
    """
    def __init__(self, cfg: AppConfig) -> None:
        self._cfg = cfg
        self._llm = ChatOpenAI(
            model=cfg.model,
            temperature=cfg.temperature,
            api_key=SecretStr(cfg.openai_api_key)  
        )

    @property
    def llm(self) -> ChatOpenAI:
        return self._llm


# =========================
# 3) Gerador NL -> SQL
# =========================
SchemaInput = Union[str, dict[str, str], list[str], tuple[str, ...]]


class NL2SQLGenerator:
    """
    Responsável por:
    - Montar o prompt (PromptTemplate)
    - Encadear LLM + parser (LCEL)
    - Gerar SQL a partir de (schema, question)
    - Aplicar guardrails (ex.: LIMIT)
    """
    _BASE_TEMPLATE = (
        "Gere SQL compatível com Spark SQL. Regras:\n"
        "- Qualifique com schema quando necessário (ex.: vw_estab_catalog).\n"
        "- SEMPRE inclua LIMIT (padrão {default_limit}) se o usuário não pedir diferente.\n"
        "- Use apenas colunas do schema abaixo.\n"
        "{allowed}\n\n"
        "Schema:\n{schema}\n\n"
        "Pergunta:\n{question}\n\n"
        "Responda SOMENTE com o SQL, sem explicações.\n"
        "Se vier cercas de código (``` ou ```sql), remova-as."
    )

    def __init__(self, llm_client: LLMClient, cfg: AppConfig) -> None:
        self._cfg = cfg
        self._llm = llm_client.llm
        self._parser = StrOutputParser()
        self._prompt = PromptTemplate(
            input_variables=["schema", "question", "default_limit", "allowed"],
            template=self._BASE_TEMPLATE,
        )

        # Monta o "allowed" textual (guardrail suave)
        if cfg.allowed_schemas:
            allowed_msg = f"- Restrinja-se aos schemas: {', '.join(cfg.allowed_schemas)}."
        else:
            allowed_msg = "- Evite criar tabelas/colunas inexistentes e não use DDL/DML perigosos."
        self._allowed_msg = allowed_msg

        # Encadeia via LCEL (prompt | llm | parser)
        self._chain = self._prompt | self._llm | self._parser

    @staticmethod
    def _strip_code_fences(text: str) -> str:
        """Remove cercas de código markdown (``` ou ```sql ... ```), e trim."""
        # Remove blocos ```...```
        cleaned = re.sub(r"```(?:sql)?\s*([\s\S]*?)\s*```", r"\1", text, flags=re.IGNORECASE)
        return cleaned.strip()

    def _ensure_limit(self, sql: str) -> str:
        """
        Aplica LIMIT padrão se:
        - enforce_limit=True
        - não houver LIMIT presente no SQL
        """
        if not self._cfg.enforce_limit:
            return sql

        # Procura um LIMIT N em qualquer lugar (case-insensitive)
        if re.search(r"\blimit\s+\d+\b", sql, flags=re.IGNORECASE):
            return sql

        # injeta um LIMIT no final com o valor default
        sql = sql.rstrip().rstrip(";")
        return f"{sql}\nLIMIT {self._cfg.default_limit}"

    def _basic_safety(self, sql: str) -> str:
        """
        Regras simples de segurança para evitar comandos perigosos.
        (Você pode expandir com regex, parser SQL, etc.)
        """
        # Remove comentários antes de checar
        no_block_comments = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
        no_line_comments = re.sub(r"--.*?$", " ", no_block_comments, flags=re.MULTILINE)

        # Checagem por palavras perigosas com limites de palavra
        dangerous_keywords = [
            r"\bdrop\b",
            r"\bdelete\b",
            r"\bupdate\b",
            r"\binsert\b",
            r"\bcreate\b",
            r"\balter\b",
            r"\btruncate\b",
        ]
        pattern = re.compile("|".join(dangerous_keywords), flags=re.IGNORECASE)
        if pattern.search(no_line_comments):
            raise ValueError(
                "SQL potencialmente destrutivo detectado (DDL/DML). Revise a pergunta ou afine o prompt."
            )
        return sql

    def _enforce_allowed_schemas(self, sql: str) -> None:
        """
        Se allowed_schemas estiver configurado, verifica usos de schema qualificado (schema.tabela)
        e garante que pertençam à lista permitida. Implementação heurística (não um parser SQL).
        """
        if not self._cfg.allowed_schemas:
            return

        allowed = {s.lower() for s in self._cfg.allowed_schemas}
        # Heurística: pega tokens do tipo schema.ident
        for match in re.finditer(r"\b([a-zA-Z_][\w]*)\.", sql):
            schema = match.group(1).lower()
            if schema not in allowed:
                raise ValueError(
                    f"Schema não permitido detectado: '{schema}'. Permitidos: {', '.join(sorted(allowed))}."
                )

    @staticmethod
    def _format_schema(schema: SchemaInput) -> str:
        """Normaliza o schema para string única que será enviada ao prompt."""
        if isinstance(schema, str):
            normalized = schema.strip()
            if not normalized:
                raise ValueError("Schema não pode ser vazio.")
            return normalized

        if isinstance(schema, dict):
            blocks: list[str] = []
            for table_name, definition in schema.items():
                table = table_name.strip()
                if not table:
                    raise ValueError("Nome de tabela inválido detectado no schema.")

                definition_text = definition.strip()
                if not definition_text:
                    raise ValueError(f"Definição vazia para a tabela '{table}'.")

                first_line = definition_text.splitlines()[0].strip().lower()
                if table.lower() in first_line:
                    blocks.append(definition_text)
                else:
                    column_lines = [line.strip() for line in definition_text.splitlines() if line.strip()]
                    columns_block = ",\n  ".join(column_lines)
                    blocks.append(f"{table} (\n  {columns_block}\n)")

            if not blocks:
                raise ValueError("Nenhuma tabela válida encontrada no schema.")
            return "\n\n".join(blocks)

        if isinstance(schema, (list, tuple)):
            blocks = [str(item).strip() for item in schema if str(item).strip()]
            if not blocks:
                raise ValueError("Lista de schemas vazia.")
            return "\n\n".join(blocks)

        raise TypeError(
            "Schema deve ser string, dict[str, str] ou lista/tupla de strings com as definições das tabelas."
        )

    def generate_sql(self, schema: SchemaInput, question: str) -> str:
        """
        Gera SQL (string) a partir de um schema e uma pergunta em linguagem natural.
        - Usa o chain LCEL (prompt | llm | parser)
        - Aplica guardrails: LIMIT e checagem básica
        """
        schema_block = self._format_schema(schema)
        raw_sql = self._chain.invoke({
            "schema": schema_block,
            "question": question,
            "default_limit": self._cfg.default_limit,
            "allowed": self._allowed_msg,
        }).strip()

        cleaned_sql = self._strip_code_fences(raw_sql)
        safe_sql = self._basic_safety(cleaned_sql)
        self._enforce_allowed_schemas(safe_sql)
        final_sql = self._ensure_limit(safe_sql)
        return final_sql


# =========================
# 4) Ponto de entrada
# =========================
def main(texto: str, schemas: Optional[SchemaInput] = None) -> str:
    """
    Exemplo de uso:
    - Carrega config
    - Cria LLMClient
    - Gera SQL a partir de schema + pergunta
    """
    cfg = AppConfig.load()
    client = LLMClient(cfg)
    generator = NL2SQLGenerator(client, cfg)

    default_schema: SchemaInput = [
        """
        vw_estab_catalog(
          estab_key bigint, cnpj string, nome_fantasia string, matriz_filial int,
          uf string, codigo_municipio int, cnae_principal string,
          cep_formatado string, tipo_logradouro string, logradouro string, numero string,
          complemento string, bairro string, email1 string, telefone1 string, updated_at timestamp
        )
        """
    ]

    schema_definition: SchemaInput = schemas if schemas is not None else default_schema

    question = texto

    sql_query = generator.generate_sql(schema_definition, question)
    print("\n=== SQL GERADO ===\n")
    print(sql_query)
    return sql_query


if __name__ == "__main__":
    # Exemplo simples de execução local
    main(
        "Quero estabelecimento sao paulo com cnpj, nome_fantasia e bairro limite 1"
    )
