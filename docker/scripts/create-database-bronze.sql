-- Este script será executado automaticamente na inicialização do container

-- 1) Cria schema e tabela
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.products (
  id             INTEGER,
  title          TEXT,
  price          DOUBLE PRECISION,
  description    TEXT,
  category       TEXT,
  image          TEXT,
  rating_rate    DOUBLE PRECISION,
  rating_count   INTEGER,
  date_ingest    TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 2) Índices
CREATE INDEX IF NOT EXISTS idx_bronze_products_category
  ON bronze.products(category);

CREATE INDEX IF NOT EXISTS idx_bronze_products_date_ingest
  ON bronze.products(date_ingest);