\set ON_ERROR_STOP on

\echo Creating and loading table tokens...

CREATE TABLE IF NOT EXISTS tokens (
    address  text PRIMARY KEY,
    decimals integer,
    name     text,
    symbol   text
);

-- Optionally clear existing data to reload clean
TRUNCATE TABLE tokens;

-- Adjust the path if your CSV is in a different location
\copy tokens (address, decimals, name, symbol) FROM 'data/token_metadata.csv' WITH (FORMAT csv, HEADER true);

\echo Done loading tokens.
