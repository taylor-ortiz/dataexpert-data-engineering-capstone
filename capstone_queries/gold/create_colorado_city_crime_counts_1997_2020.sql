CREATE TABLE tayloro.colorado_city_crime_counts_1997_2020 AS
SELECT
    city,
    COUNT(*) AS crime_count
FROM
    academy.tayloro.colorado_crimes_with_cities
WHERE
    EXTRACT(YEAR FROM incident_date) BETWEEN 1997 AND 2020
    and city is not null
GROUP BY
    city
ORDER BY
    crime_count DESC;