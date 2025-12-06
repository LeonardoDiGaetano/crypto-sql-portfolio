\set ON_ERROR_STOP on

\echo Building daily close and log-returns...

-- 1) Drop and recreate the daily table
DROP TABLE IF EXISTS ohlc_daily;

-- 2) For each asset and day, take the *last* close in that day,
--    then compute log-returns over days.

CREATE TABLE ohlc_daily AS
WITH per_tick AS (
    SELECT
        asset_address,
        date_trunc('day', ts)::date AS day,
        ts,
        price_close,
        ROW_NUMBER() OVER (
            PARTITION BY asset_address, date_trunc('day', ts)::date
            ORDER BY ts DESC
        ) AS rn
    FROM ohlc_raw
),
daily_close AS (
    -- Keep only the last observation per (asset, day)
    SELECT
        asset_address,
        day,
        price_close AS close
    FROM per_tick
    WHERE rn = 1
),
with_lag AS (
    SELECT
        asset_address,
        day,
        close,
        LAG(close) OVER (
            PARTITION BY asset_address
            ORDER BY day
        ) AS prev_close
    FROM daily_close
)
SELECT
    asset_address,
    day,
    close,
    CASE
        WHEN prev_close IS NULL OR prev_close <= 0 THEN NULL
        ELSE LN(close / prev_close)
    END AS log_return
FROM with_lag;

-- 3) Optional: add indexes for faster queries
CREATE INDEX idx_ohlc_daily_asset_day
ON ohlc_daily (asset_address, day);

\echo Done building ohlc_daily.
