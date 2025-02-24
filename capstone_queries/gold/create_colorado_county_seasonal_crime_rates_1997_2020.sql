CREATE TABLE tayloro.colorado_county_seasonal_crime_rates_1997_2020 AS
WITH monthly_crimes AS (
    SELECT
        county_name,
        MONTH(incident_date) AS crime_month,  -- Numeric month (1-12)
        CASE
            WHEN MONTH(incident_date) = 1 THEN 'January'
            WHEN MONTH(incident_date) = 2 THEN 'February'
            WHEN MONTH(incident_date) = 3 THEN 'March'
            WHEN MONTH(incident_date) = 4 THEN 'April'
            WHEN MONTH(incident_date) = 5 THEN 'May'
            WHEN MONTH(incident_date) = 6 THEN 'June'
            WHEN MONTH(incident_date) = 7 THEN 'July'
            WHEN MONTH(incident_date) = 8 THEN 'August'
            WHEN MONTH(incident_date) = 9 THEN 'September'
            WHEN MONTH(incident_date) = 10 THEN 'October'
            WHEN MONTH(incident_date) = 11 THEN 'November'
            WHEN MONTH(incident_date) = 12 THEN 'December'
        END AS month_name,
        COUNT(*) AS total_crimes
    FROM tayloro.colorado_crimes
    WHERE incident_date IS NOT NULL
    GROUP BY county_name, MONTH(incident_date)  -- Ensure grouping includes month
),
normalized_crime AS (
    SELECT
        mc.county_name,
        mc.crime_month,
        mc.month_name,
        AVG(mc.total_crimes) AS avg_monthly_crimes
    FROM monthly_crimes mc
    GROUP BY mc.county_name, mc.crime_month, mc.month_name  -- Ensure grouping includes crime_month
)
SELECT
    county_name,
    crime_month,
    month_name,
    avg_monthly_crimes
FROM normalized_crime
ORDER BY county_name, crime_month;
