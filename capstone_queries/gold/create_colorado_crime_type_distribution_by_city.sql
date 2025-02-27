CREATE TABLE tayloro.colorado_crime_type_distribution_by_city AS
SELECT
    city,
    crime_against,  -- e.g., Person, Property, Society
    COUNT(*) AS total_crimes
FROM tayloro.colorado_crimes_with_cities
WHERE city IS NOT NULL
and crime_against is not null
and crime_against <> 'Not a Crime'
GROUP BY city, crime_against
ORDER BY city, total_crimes DESC;