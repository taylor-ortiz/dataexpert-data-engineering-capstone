CREATE TABLE tayloro.colorado_household_income_tier_rank AS
WITH income_aggregated AS (
    -- Step 1: Aggregate income per county across all years and compute average
    SELECT
        county,
        AVG(income) AS avg_median_household_income
    FROM academy.tayloro.colorado_income_1997_2020
    WHERE inctype = 3 -- 3 represents "Median Household Income"
    GROUP BY county
),
income_percentiles AS (
    -- Step 2: Compute percentile rank for average income per county
    SELECT
        county,
        avg_median_household_income,
        PERCENT_RANK() OVER (ORDER BY avg_median_household_income) AS income_percentile
    FROM income_aggregated
),
income_tiers AS (
    -- Step 3: Assign tiers based on percentile rankings
    SELECT
        county,
        avg_median_household_income,
        income_percentile,
        CASE
            WHEN income_percentile >= 0.75 THEN 1 -- Highest income counties (Top 25%)
            WHEN income_percentile >= 0.50 THEN 2 -- Middle-High (50%-75%)
            WHEN income_percentile >= 0.25 THEN 3 -- Middle-Low (25%-50%)
            ELSE 4 -- Lowest income counties (Bottom 25%)
        END AS income_tier
    FROM income_percentiles
)
SELECT * FROM income_tiers;
