SELECT
    DATE_PARSE(CAST(i.year AS VARCHAR) || '-01-01', '%Y-%m-%d') AS time, -- Convert year to date for Grafana
    i.county,
    i.household_income_per_capita,
    c.total_crimes
FROM (
    -- Median Household Income Query
    SELECT
        TRIM(REPLACE(areaname, 'County', '')) AS county,
        periodyear AS year,
        income AS household_income_per_capita
    FROM academy.tayloro.colorado_income_raw
    WHERE areatyname = 'County'
      AND periodyear BETWEEN 1997 AND 2020
      AND inctype = 2
) i
LEFT JOIN (
    -- Total Crimes Query
    SELECT
        EXTRACT(YEAR FROM incident_date) AS year,
        county_name AS county,
        COUNT(*) AS total_crimes
    FROM academy.tayloro.colorado_crimes
    WHERE incident_date IS NOT NULL
      AND county_name IS NOT NULL
    GROUP BY EXTRACT(YEAR FROM incident_date), county_name
) c
ON i.year = c.year
AND i.county = c.county
WHERE i.county = 'Boulder' -- Add a dynamic county filter
ORDER BY i.year