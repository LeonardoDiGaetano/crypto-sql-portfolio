-- =====================================================================
-- 01_schema.sql
-- Schema for crypto tokens and OHLC time-series data in PostgreSQL
-- =====================================================================

-- ---------------------------------------------------------------------
-- Table: tokens
-- Metadata for each token (one row per address)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tokens (
    address   TEXT PRIMARY KEY,
    decimals  INTEGER,
    name      TEXT,
    symbol    TEXT
);

CREATE INDEX IF NOT EXISTS idx_tokens_symbol
    ON tokens(symbol);


-- ---------------------------------------------------------------------
-- Table: ohlcv
-- OHLC time-series data for each token
--
-- One CSV file per token will be loaded into this table.
-- The token address will be stored in asset_address, using a DEFAULT
-- set before each \copy command.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ohlcv (
    id             BIGSERIAL PRIMARY KEY,
    asset_address  TEXT NOT NULL REFERENCES tokens(address),
    price_open     NUMERIC(30, 12) NOT NULL,
    price_high     NUMERIC(30, 12) NOT NULL,
    price_low      NUMERIC(30, 12) NOT NULL,
    price_close    NUMERIC(30, 12) NOT NULL,
    ts_unix        BIGINT,          -- raw UNIX timestamp from CSV (column "timestamp")
    ts             TIMESTAMPTZ NOT NULL  -- parsed from CSV column "date"
);

-- Index optimized for time-series queries per asset
CREATE INDEX IF NOT EXISTS idx_ohlcv_asset_ts
    ON ohlcv(asset_address, ts);
