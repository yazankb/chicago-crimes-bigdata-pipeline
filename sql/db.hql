-- ============================================================
-- db.hql — Stage II: Hive Database + Partitioned + Bucketed Tables
-- ============================================================

-- Execution engine
SET hive.execution.engine=tez;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- Drop database for idempotency
DROP DATABASE IF EXISTS team2_projectdb CASCADE;
CREATE DATABASE team2_projectdb LOCATION 'project/hive/warehouse';
USE team2_projectdb;

-- ============================================================
-- External table on Sqoop Parquet output (Sqoop warehouse)
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
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- ============================================================
-- Partitioned + Bucketed table for optimized EDA
-- Partitioned by year (prunes scans), bucketed by district (22 buckets)
-- ============================================================
CREATE EXTERNAL TABLE crimes_optimized (
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
    ward                INT,
    community_area      INT,
    fbi_code            STRING,
    x_coordinate        INT,
    y_coordinate        INT,
    updated_on          BIGINT,
    latitude            DOUBLE,
    longitude           DOUBLE,
    location            STRING
)
PARTITIONED BY (year INT)
CLUSTERED BY (district) INTO 22 BUCKETS
STORED AS PARQUET
LOCATION 'project/hive/warehouse/crimes_optimized'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Migrate data from unpartitioned to partitioned+bucketed
INSERT OVERWRITE TABLE crimes_optimized PARTITION (year)
SELECT id, case_number, `date`, block, iucr, primary_type, description,
       location_description, arrest, domestic, beat, ward, community_area,
       fbi_code, x_coordinate, y_coordinate, updated_on,
       latitude, longitude, location, year
FROM crimes;

-- Drop unpartitioned table (EDA will use optimized table only)
DROP TABLE IF EXISTS crimes;

-- ============================================================
-- Features view (cyclical datetime encoding)
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
FROM crimes_optimized
WHERE from_unixtime(`date`) IS NOT NULL
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL;

-- Sample table for ML prototyping
CREATE TABLE IF NOT EXISTS crimes_sample
STORED AS PARQUET
LOCATION 'project/hive/warehouse/crimes_sample'
AS
SELECT * FROM crimes_features
LIMIT 850000;

-- Arrest balance view
CREATE VIEW IF NOT EXISTS arrest_balance AS
SELECT arrest, COUNT(*) AS record_count,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS pct_of_total
FROM crimes_optimized
GROUP BY arrest;

-- Verify
SELECT 'DB setup complete' AS status;
SELECT COUNT(*) AS row_count FROM crimes_optimized;
SELECT * FROM arrest_balance;
