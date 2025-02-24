SELECT
    offense_category_name,  -- Crime type
    COUNT(*) AS crime_count  -- Count of crimes per category
FROM
    academy.tayloro.colorado_crimes
WHERE
    EXTRACT(YEAR FROM incident_date) BETWEEN 1997 AND 2020
    AND county_name = '$county'  -- Grafana variable for county selection
GROUP BY
    offense_category_name
ORDER BY
    crime_count DESC;