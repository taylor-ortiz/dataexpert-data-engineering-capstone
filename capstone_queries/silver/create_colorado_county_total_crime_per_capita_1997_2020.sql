CREATE TABLE tayloro.colorado_county_crime_per_capita_1997_2020 AS
WITH crimes_total AS (
    SELECT
        county_name,
        COUNT(*) AS total_crimes
    FROM tayloro.colorado_crimes
    WHERE incident_date IS NOT NULL
        AND YEAR(incident_date) BETWEEN 1997 AND 2020
    GROUP BY county_name
),
population_total AS (
    SELECT
        county AS county_name,
        SUM(TOTALPOPULATION) AS total_population
    FROM tayloro.colorado_population_raw
    WHERE DATATYPE = 'Estimate'
        AND YEAR BETWEEN 1997 AND 2020
    GROUP BY county
),
crime_rate_per_capita AS (
    SELECT
        c.county_name,
        c.total_crimes,
        p.total_population,
        (c.total_crimes * 100000.0 / p.total_population) AS crime_per_100k
    FROM crimes_total c
    JOIN population_total p
        ON LOWER(TRIM(c.county_name)) = LOWER(TRIM(p.county_name))
)
SELECT * FROM crime_rate_per_capita;