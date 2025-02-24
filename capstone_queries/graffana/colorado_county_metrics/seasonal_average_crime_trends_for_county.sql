SELECT
    county_name,
    month_name,
    avg_monthly_crimes
FROM academy.tayloro.colorado_county_seasonal_crime_rates_1997_2020
WHERE county_name = LOWER('$county')
ORDER BY crime_month ASC
