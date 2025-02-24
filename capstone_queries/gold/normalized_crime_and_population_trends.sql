SELECT
    DATE_PARSE(CAST(year AS VARCHAR) || '-01-01', '%Y-%m-%d') AS time, -- Convert year to date for Grafana
    total_crimes,  -- Raw total crimes per year
    total_population  -- Raw total population per year
FROM academy.tayloro.colorado_crime_population_trends
WHERE county_name = '$county'
ORDER BY year





