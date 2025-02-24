CREATE TABLE tayloro.colorado_crimes_2016_2020_with_cities AS
WITH unique_city_mapping AS (
    SELECT DISTINCT city
    FROM tayloro.colorado_city_county_zip
)
SELECT
    ccr.*,
    ucm.city AS city
FROM tayloro.colorado_crimes_2016_2020_raw ccr
INNER JOIN unique_city_mapping ucm
    ON LOWER(TRIM(ccr.pub_agency_name)) = LOWER(TRIM(ucm.city));