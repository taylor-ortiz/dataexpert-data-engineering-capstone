CREATE TABLE tayloro.colorado_county_agency_crime_counts AS
SELECT
    county_name,
    agency_name,
    COUNT(*) AS total_crimes
FROM tayloro.colorado_crimes
GROUP BY county_name, agency_name
ORDER BY total_crimes DESC;