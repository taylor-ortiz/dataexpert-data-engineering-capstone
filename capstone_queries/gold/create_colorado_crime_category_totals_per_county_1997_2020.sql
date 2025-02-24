CREATE TABLE tayloro.colorado_crime_category_totals_per_county_1997_2020 AS
SELECT
    offense_category_name,  -- Crime type
    COUNT(*) AS crime_count, -- Count of crimes per category
    county_name
FROM
    academy.tayloro.colorado_crimes
WHERE
    EXTRACT(YEAR FROM incident_date) BETWEEN 1997 AND 2020
GROUP BY
    offense_category_name, county_name
ORDER BY
    crime_count DESC;