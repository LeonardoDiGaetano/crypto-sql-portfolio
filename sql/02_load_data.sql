\set ON_ERROR_STOP off

\echo ---
\echo filepath is :filepath
\echo asset_address is :asset_address
\echo ---

\echo Loading OHLC data for token :asset_address
\echo From CSV file :filepath

-- Tabella finale (creata solo la prima volta)
CREATE TABLE IF NOT EXISTS ohlc_raw (
    asset_address  text      NOT NULL,
    ts             timestamp NOT NULL,  -- da colonna "date"
    ts_unix        bigint    NOT NULL,  -- da colonna "timestamp"
    price_open     numeric,
    price_high     numeric,
    price_low      numeric,
    price_close    numeric
);

-- Tabella temporanea per il singolo file
DROP TABLE IF EXISTS ohlc_raw_tmp;

CREATE TEMP TABLE ohlc_raw_tmp (
    price_open     numeric,
    price_high     numeric,
    price_low      numeric,
    price_close    numeric,
    "timestamp"    bigint,
    "date"         timestamp
);

-- IMPORT CSV -> tabella temporanea
-- ATTENZIONE: riga singola, inizia con \copy
-- IMPORT CSV -> tabella temporanea
\copy ohlc_raw_tmp (price_open, price_high, price_low, price_close, "timestamp", "date") FROM :filepath WITH (FORMAT csv, HEADER true);

-- INSERT nella tabella finale
INSERT INTO ohlc_raw (asset_address, ts, ts_unix, price_open, price_high, price_low, price_close)
SELECT
    :'asset_address'::text AS asset_address,
    "date"                 AS ts,
    "timestamp"            AS ts_unix,
    price_open,
    price_high,
    price_low,
    price_close
FROM ohlc_raw_tmp;

\echo Done loading OHLC data for token :asset_address
