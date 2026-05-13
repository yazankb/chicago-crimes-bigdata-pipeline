-- ============================================================
-- import_data.sql — COPY commands for bulk loading Chicago Crimes
-- Usage: Called from Python (build_projectdb.py) via copy_expert
-- ============================================================

SET datestyle = 'ISO, MDY';

COPY crimes(id, case_number, date, block, iucr, primary_type, description,
            location_description, arrest, domestic, beat, district, ward,
            community_area, fbi_code, x_coordinate, y_coordinate, year,
            updated_on, latitude, longitude, location)
FROM STDIN
WITH (
    FORMAT csv,
    HEADER TRUE,
    DELIMITER ',',
    NULL '',
    ENCODING 'UTF8'
);