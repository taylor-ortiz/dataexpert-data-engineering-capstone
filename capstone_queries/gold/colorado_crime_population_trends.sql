SELECT
    DATE_PARSE(CAST(EXTRACT(YEAR FROM incident_date) AS VARCHAR) || '-01-01', '%Y-%m-%d') AS time, -- Convert year to date for Grafana
    county_name,
    COUNT(*) AS total_crimes
FROM academy.tayloro.colorado_crimes
WHERE county_name = '$county'
  AND incident_date IS NOT NULL -- Exclude records with null incident_date
  AND county_name IS NOT NULL
GROUP BY EXTRACT(YEAR FROM incident_date), county_name -- Group by the extracted year and county
ORDER BY EXTRACT(YEAR FROM incident_date), county_name -- Order by the extracted year and county
