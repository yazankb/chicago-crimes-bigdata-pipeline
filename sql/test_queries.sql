-- ============================================================
-- test_queries.sql — Validation queries after data import
-- ============================================================

-- 1. Row count check
SELECT 'Row count' AS metric, COUNT(*)::TEXT AS value FROM crimes
UNION ALL
SELECT 'Unique IDs', COUNT(DISTINCT id)::TEXT FROM crimes
UNION ALL
SELECT 'Max year', MAX(year)::TEXT FROM crimes
UNION ALL
SELECT 'Min year', MIN(year)::TEXT FROM crimes;

-- 2. Arrest distribution
SELECT arrest, COUNT(*) AS count,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS pct
FROM crimes
GROUP BY arrest
ORDER BY arrest;

-- 3. Null rates for key columns
SELECT
    ROUND(100.0 * SUM(CASE WHEN date IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS date_null_pct,
    ROUND(100.0 * SUM(CASE WHEN latitude IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS lat_null_pct,
    ROUND(100.0 * SUM(CASE WHEN longitude IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS lon_null_pct,
    ROUND(100.0 * SUM(CASE WHEN primary_type IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS type_null_pct
FROM crimes;

-- 4. Year distribution
SELECT year, COUNT(*) AS records
FROM crimes
GROUP BY year
ORDER BY year DESC
LIMIT 10;

-- 5. Top crime types
SELECT primary_type, COUNT(*) AS count
FROM crimes
GROUP BY primary_type
ORDER BY count DESC
LIMIT 15;

-- 6. Geospatial bounds
SELECT MIN(latitude) AS min_lat, MAX(latitude) AS max_lat,
       MIN(longitude) AS min_lon, MAX(longitude) AS max_lon
FROM crimes
WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

-- 7. Sample records
SELECT * FROM crimes ORDER BY RANDOM() LIMIT 5;