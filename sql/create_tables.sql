-- ============================================================
-- create_tables.sql — PostgreSQL DDL for Chicago Crimes Dataset
-- ============================================================

-- Drop existing objects for idempotency
DROP TABLE IF EXISTS crimes CASCADE;

-- Main crimes table
CREATE TABLE IF NOT EXISTS crimes (
    id                  BIGINT PRIMARY KEY,
    case_number         VARCHAR(20),
    date                TIMESTAMP,
    block               VARCHAR(200),
    iucr                VARCHAR(10),
    primary_type        VARCHAR(50) NOT NULL,
    description         VARCHAR(300),
    location_description VARCHAR(100),
    arrest              BOOLEAN NOT NULL DEFAULT FALSE,
    domestic            BOOLEAN NOT NULL DEFAULT FALSE,
    beat                VARCHAR(10),
    district            SMALLINT,
    ward                SMALLINT,
    community_area      SMALLINT,
    fbi_code            VARCHAR(10),
    x_coordinate        INTEGER,
    y_coordinate        INTEGER,
    year                SMALLINT,
    updated_on          TIMESTAMP,
    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    location            VARCHAR(300)
);

-- Index on frequently queried columns
CREATE INDEX IF NOT EXISTS idx_crimes_arrest ON crimes(arrest);
CREATE INDEX IF NOT EXISTS idx_crimes_year ON crimes(year);
CREATE INDEX IF NOT EXISTS idx_crimes_primary_type ON crimes(primary_type);
CREATE INDEX IF NOT EXISTS idx_crimes_date ON crimes(date);
CREATE INDEX IF NOT EXISTS idx_crimes_district ON crimes(district);
CREATE INDEX IF NOT EXISTS idx_crimes_community_area ON crimes(community_area);

-- Statistics table for data quality tracking
CREATE TABLE IF NOT EXISTS data_quality_stats (
    check_date          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_rows          BIGINT,
    null_date_count     BIGINT,
    null_lat_count      BIGINT,
    null_lon_count      BIGINT,
    null_arrest_count   BIGINT,
    arrest_true_count   BIGINT,
    arrest_false_count  BIGINT,
    year_min            SMALLINT,
    year_max            SMALLINT
);

-- Function to update quality stats
CREATE OR REPLACE FUNCTION update_data_quality_stats()
RETURNS VOID AS $$
BEGIN
    DELETE FROM data_quality_stats;
    INSERT INTO data_quality_stats (
        total_rows, null_date_count, null_lat_count, null_lon_count,
        null_arrest_count, arrest_true_count, arrest_false_count, year_min, year_max
    )
    SELECT
        COUNT(*),
        SUM(CASE WHEN date IS NULL THEN 1 ELSE 0 END),
        SUM(CASE WHEN latitude IS NULL THEN 1 ELSE 0 END),
        SUM(CASE WHEN longitude IS NULL THEN 1 ELSE 0 END),
        SUM(CASE WHEN arrest IS NULL THEN 1 ELSE 0 END),
        SUM(CASE WHEN arrest = TRUE THEN 1 ELSE 0 END),
        SUM(CASE WHEN arrest = FALSE THEN 1 ELSE 0 END),
        MIN(year),
        MAX(year)
    FROM crimes;
END;
$$ LANGUAGE plpgsql;

SELECT 'DDL completed successfully' AS status;