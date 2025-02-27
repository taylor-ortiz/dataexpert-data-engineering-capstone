CREATE TABLE IF NOT EXISTS tayloro.colorado_crime_tier_stolen_property AS
WITH crime_data AS (
    -- Compute total crime count per county from 1997-2000
    SELECT
        county_name,
        COUNT(*) AS total_crimes
    FROM academy.tayloro.colorado_crimes
    WHERE offense_category_name = 'Stolen Property Offenses'
      AND EXTRACT(YEAR FROM incident_date) BETWEEN 1997 AND 2020
    GROUP BY county_name
),
crime_percentiles AS (
    -- Assign percentile ranks based on crime count
    SELECT
        county_name,
        total_crimes,
        PERCENT_RANK() OVER (ORDER BY total_crimes) AS crime_percentile
    FROM crime_data
),
crime_tiers AS (
    -- Categorize counties into Tiers
    SELECT
        county_name,
        total_crimes,
        crime_percentile,
        CASE
            WHEN crime_percentile >= 0.75 THEN 4  -- Very High Crime
            WHEN crime_percentile >= 0.50 THEN 3  -- High Crime
            WHEN crime_percentile >= 0.25 THEN 2  -- Moderate Crime
            ELSE 1  -- Low Crime
        END AS crime_tier
    FROM crime_percentiles
)
SELECT * FROM crime_tiers;