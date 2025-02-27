CREATE TABLE tayloro.colorado_final_county_tier_rank AS
WITH combined_ranks AS (
    SELECT
        COALESCE(c.county_name, i.county, p.county) AS county,
        COALESCE(c.overall_crime_tier, 0) AS crime_rank,
        COALESCE(i.income_tier, 0) AS income_rank,
        COALESCE(p.crime_per_capita_tier, 0) AS population_rank
    FROM academy.tayloro.colorado_crime_tier_county_rank c
    FULL OUTER JOIN academy.tayloro.colorado_income_household_tier_rank i
        ON LOWER(c.county_name) = LOWER(i.county)
    FULL OUTER JOIN academy.tayloro.colorado_population_crime_per_capita_rank p
        ON LOWER(c.county_name) = LOWER(p.county)
),
average_rank AS (
    SELECT
        county,
        crime_rank,
        income_rank,
        population_rank,
        -- Apply weights: crime is weighted 2x, income and population 1x each.
        ROUND((2 * crime_rank + income_rank + population_rank) / 4.0) AS final_rank
    FROM combined_ranks
    WHERE crime_rank > 0 AND income_rank > 0 AND population_rank > 0
)
SELECT *
FROM average_rank
ORDER BY final_rank ASC;