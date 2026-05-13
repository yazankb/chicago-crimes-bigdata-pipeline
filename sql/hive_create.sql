-- ============================================================
-- hive_create.sql — Hive External Table for Chicago Crimes
-- Run via beeline (hive on hadoop-03:10001)
-- ============================================================

USE team2_projectdb;

-- Drop if exists for idempotency
DROP TABLE IF EXISTS crimes;
DROP TABLE IF EXISTS crimes_sample;
DROP VIEW IF EXISTS crimes_features;
DROP VIEW IF EXISTS arrest_balance;

-- ============================================================
-- External table pointing to Sqoop Parquet+Snappy output
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS crimes (
    id                  BIGINT,
    case_number         STRING,
    `date`              BIGINT,
    block               STRING,
    iucr                STRING,
    primary_type        STRING,
    description         STRING,
    location_description STRING,
    arrest              BOOLEAN,
    domestic            BOOLEAN,
    beat                STRING,
    district            INT,
    ward                INT,
    community_area      INT,
    fbi_code            STRING,
    x_coordinate        INT,
    y_coordinate        INT,
    year                INT,
    updated_on          BIGINT,
    latitude            DOUBLE,
    longitude           DOUBLE,
    location            STRING
)
STORED AS PARQUET
LOCATION '/user/team2/project/warehouse/crimes'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

-- ============================================================
-- Analysis view with derived datetime features
-- ============================================================
CREATE VIEW IF NOT EXISTS crimes_features AS
SELECT
    id, case_number, from_unixtime(`date`) AS crime_date,
    block, iucr, primary_type, description, location_description,
    arrest, domestic, beat, district, ward, community_area,
    fbi_code, x_coordinate, y_coordinate, year,
    latitude, longitude, location,
    HOUR(from_unixtime(`date`)) AS hour_of_day,
    DAYOFWEEK(from_unixtime(`date`)) AS day_of_week,
    MONTH(from_unixtime(`date`)) AS month_of_year,
    QUARTER(from_unixtime(`date`)) AS quarter_of_year,
    SIN(HOUR(from_unixtime(`date`)) * 2 * 3.14159 / 24) AS hour_sin,
    COS(HOUR(from_unixtime(`date`)) * 2 * 3.14159 / 24) AS hour_cos,
    SIN(DAYOFWEEK(from_unixtime(`date`)) * 2 * 3.14159 / 7) AS day_sin,
    COS(DAYOFWEEK(from_unixtime(`date`)) * 2 * 3.14159 / 7) AS day_cos,
    IF(arrest = TRUE, 1, 0) AS arrest_flag
FROM crimes
WHERE from_unixtime(`date`) IS NOT NULL
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL;

-- ============================================================
-- Stratified sample table for ML prototyping
-- ============================================================
CREATE TABLE IF NOT EXISTS crimes_sample
STORED AS PARQUET
LOCATION '/user/team2/project/warehouse/crimes_sample'
AS
SELECT *
FROM crimes_features
LIMIT 850000;

-- ============================================================
-- Class balance summary view
-- ============================================================
CREATE VIEW IF NOT EXISTS arrest_balance AS
SELECT
    arrest,
    COUNT(*) AS record_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS pct_of_total
FROM crimes
GROUP BY arrest;

SELECT 'Hive tables and views created successfully' AS status;
