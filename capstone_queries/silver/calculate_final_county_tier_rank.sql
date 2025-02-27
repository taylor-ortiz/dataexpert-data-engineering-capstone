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
        ROUND(
            (crime_rank + income_rank + population_rank) /
            NULLIF((CASE WHEN crime_rank > 0 THEN 1 ELSE 0 END
                  + CASE WHEN income_rank > 0 THEN 1 ELSE 0 END
                  + CASE WHEN population_rank > 0 THEN 1 ELSE 0 END), 0)
        ) AS final_rank -- Calculate the average rank and round to nearest whole number
    FROM combined_ranks
    WHERE crime_rank > 0 AND income_rank > 0 AND population_rank > 0 -- Ensure all ranks are not null
)
SELECT * FROM average_rank
ORDER BY avg_rank ASC; -- Order by the final average rank
