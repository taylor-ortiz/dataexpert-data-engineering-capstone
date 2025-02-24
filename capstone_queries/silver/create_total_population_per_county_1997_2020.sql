CREATE TABLE academy.tayloro.colorado_total_population_per_county_1997_2020 AS
WITH filtered_population AS (
    SELECT
        county,
        year,
        SUM(total_population) AS total_population  -- Aggregating population in case of duplicates
    FROM tayloro.colorado_population_raw
    WHERE year BETWEEN 1997 AND 2020
        AND data_type = 'Estimate'  -- Ensuring only estimated values
    GROUP BY county, year
)
SELECT * FROM filtered_population
ORDER BY county, year;