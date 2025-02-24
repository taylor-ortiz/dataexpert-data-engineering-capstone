SELECT
    DATE_PARSE(CAST(year AS VARCHAR) || '-01-01', '%Y-%m-%d') AS time, -- Ensure date formatting for Grafana
    CAST(MAX(crime_per_100k) AS DOUBLE) AS crime_per_100k, -- Max ensures single value per year
    CAST(MAX(median_household_income) AS DOUBLE) AS median_household_income -- Max ensures single value per year
FROM academy.tayloro.colorado_crime_vs_median_household_income_1997_2020
WHERE county_name = '$county'
GROUP BY year
ORDER BY year