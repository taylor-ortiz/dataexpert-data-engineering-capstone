SELECT
    offense_category_name,  -- Crime type
    crime_count  -- Count of crimes per category
FROM
    academy.tayloro.colorado_crime_category_totals_per_county_1997_2020
WHERE county_name = LOWER('$county')  -- Grafana variable for county selection