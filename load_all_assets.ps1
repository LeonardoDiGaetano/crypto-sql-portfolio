# ---------------------------------------
# PostgreSQL connection parameters
# ---------------------------------------
# These variables configure how psql connects to your database.
# The password is taken from the environment variable PGPASSWORD,
# which you set before running this script.
$PGUSER = "postgres"
$PGDATABASE = "crypto_sql"
$PGHOST = "localhost"
$PGPORT = "5432"

# ---------------------------------------
# Folder containing the OHLC CSV files
# ---------------------------------------
# This path is *relative* to the project root.
# Using relative paths avoids issues with absolute paths changing
# depending on the user or machine.
$CsvFolder = "data/ohlc_subset"

# Resolve the relative path into a *full absolute path*.
# This ensures that even if the current working directory changes,
# the CSV path is always valid and psql will be able to read it.
$CsvFolderAbsolute = (Resolve-Path $CsvFolder).Path

Write-Host "Current directory: $(Get-Location)"
Write-Host "CSV folder       : $CsvFolder"
Write-Host "Absolute path    : $CsvFolderAbsolute"


# ---------------------------------------
# Loop through every CSV file in the folder
# ---------------------------------------
Get-ChildItem -Path $CsvFolder -Filter *.csv | ForEach-Object {

    # Construct the ABSOLUTE path to each CSV file.
    # This avoids all quoting/escaping problems when constructing SQL.
    $absolutePath = Join-Path $CsvFolderAbsolute $_.Name

    # Extract the asset address from the file name (without .csv extension)
    $asset_address = $_.BaseName

    Write-Host "---------------------------------------------"
    Write-Host "Asset address: $asset_address"
    Write-Host "CSV file     : $absolutePath"


    # ---------------------------------------
    # Create a temporary SQL file
    # ---------------------------------------
    # Instead of relying on psql variable substitution (:filepath),
    # which caused quoting/escaping issues, we generate a complete SQL
    # script dynamically.
    #
    # This SQL script already contains the correct absolute path
    # and asset address, so psql doesn't need to substitute anything.
    # This guarantees reliability and eliminates the previous errors.
    $tempSqlFile = [System.IO.Path]::GetTempFileName() -replace '\.tmp$', '.sql'

    # Create a UTF-8 encoder *without BOM*.
    # If a file has a UTF-8 BOM, psql may interpret the first bytes as
    # invalid content, breaking commands such as \copy.
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)


    # ---------------------------------------
    # Build the SQL script that will be executed by psql
    # ---------------------------------------
    # This script:
    # - creates the target table if needed,
    # - creates a temporary table for each file,
    # - loads the CSV into that table using \copy,
    # - inserts the rows into ohlc_raw.
    #
    # NOTE: \copy runs *on the client side* (psql), so the CSV file must be
    # accessible from the machine running PowerShell. Using absolute paths
    # ensures this.
    $sqlScript = @"
\set ON_ERROR_STOP off

\echo Loading OHLC data for token $asset_address

-- Create final table only if it does not exist.
CREATE TABLE IF NOT EXISTS ohlc_raw (
    asset_address  text      NOT NULL,
    ts             timestamp NOT NULL,
    ts_unix        bigint    NOT NULL,
    price_open     numeric,
    price_high     numeric,
    price_low      numeric,
    price_close    numeric
);

-- Temporary table used for each individual CSV file.
DROP TABLE IF EXISTS ohlc_raw_tmp;

CREATE TEMP TABLE ohlc_raw_tmp (
    price_open     numeric,
    price_high     numeric,
    price_low      numeric,
    price_close    numeric,
    "timestamp"    bigint,
    "date"         timestamp
);

-- Load CSV → temporary table.
-- IMPORTANT: \copy expects a file path that exists on the client machine.
-- By embedding the absolute path, we avoid all quoting/escaping issues.
\copy ohlc_raw_tmp (price_open, price_high, price_low, price_close, "timestamp", "date") FROM '$absolutePath' WITH (FORMAT csv, HEADER true);

-- Insert into the main table, tagging each row with its asset address.
INSERT INTO ohlc_raw (asset_address, ts, ts_unix, price_open, price_high, price_low, price_close)
SELECT
    '$asset_address'::text AS asset_address,
    "date"                 AS ts,
    "timestamp"            AS ts_unix,
    price_open,
    price_high,
    price_low,
    price_close
FROM ohlc_raw_tmp;
"@

    # Write the SQL script to disk (UTF-8 without BOM)
    [System.IO.File]::WriteAllText($tempSqlFile, $sqlScript, $utf8NoBom)


    # ---------------------------------------
    # Execute SQL script using psql
    # ---------------------------------------
    # psql simply reads the SQL file and executes it.
    # We no longer use "-v filepath=...", so we avoid all problems where
    # PowerShell and psql disagree on quoting.
    psql `
      -h $PGHOST `
      -p $PGPORT `
      -U $PGUSER `
      -d $PGDATABASE `
      -f $tempSqlFile


    # ---------------------------------------
    # Cleanup temporary file
    # ---------------------------------------
    Remove-Item $tempSqlFile -Force
}

Write-Host "---------------------------------------------"
Write-Host "Done loading all assets."
