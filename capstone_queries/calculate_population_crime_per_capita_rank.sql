CREATE TABLE tayloro.colorado_population_crime_per_capita_rank AS
WITH crime_per_capita_per_year AS (
    -- Step 1: Compute Crime Per Capita for Each Year (1997-2000)
    SELECT
        LOWER(c.county_name) AS county, -- Standardizing county name for consistent join
        EXTRACT(YEAR FROM c.incident_date) AS year,
        COUNT(*) * 1000.0 / p.total_population AS crime_per_1000
    FROM academy.tayloro.colorado_crimes c
    JOIN academy.tayloro.colorado_total_population_per_county_1997_2020 p
        ON LOWER(c.county_name) = LOWER(p.county) -- Ensure lowercase match
        AND EXTRACT(YEAR FROM c.incident_date) = p.year
    WHERE p.total_population > 0 -- Avoid division by zero
    GROUP BY LOWER(c.county_name), EXTRACT(YEAR FROM c.incident_date), p.total_population
),
average_crime_per_capita AS (
    -- Step 2: Compute Average Crime Per Capita for 1997-2000
    SELECT
        county,
        AVG(crime_per_1000) AS avg_crime_per_1000
    FROM crime_per_capita_per_year
    GROUP BY county
),
crime_distribution AS (
    -- Step 3: Compute Percentile Rank Based on Average Crime Per Capita
    SELECT
        county,
        avg_crime_per_1000,
        PERCENT_RANK() OVER (ORDER BY avg_crime_per_1000) AS crime_percentile
    FROM average_crime_per_capita
),
crime_tiers AS (
    -- Step 4: Assign Crime Per Capita Tiers and Capitalize First Letter
    SELECT
        CONCAT(UPPER(SUBSTRING(county, 1, 1)), LOWER(SUBSTRING(county, 2))) AS county, -- Capitalize first letter
        avg_crime_per_1000,
        crime_percentile,
        CASE
            WHEN crime_percentile >= 0.75 THEN 4 -- Highest crime per capita (Top 25%)
            WHEN crime_percentile >= 0.50 THEN 3 -- Medium-high (50%-75%)
            WHEN crime_percentile >= 0.25 THEN 2 -- Medium-low (25%-50%)
            ELSE 1 -- Lowest crime per capita (Bottom 25%)
        END AS crime_per_capita_tier
    FROM crime_distribution
)
SELECT * FROM crime_tiers;
