CREATE TABLE tayloro.colorado_crime_vs_median_household_income_1997_2020 AS
SELECT
    c.county_name,
    c.year,  -- Ensure the year column is included
    c.total_crimes,
    c.total_population,
    (c.total_crimes * 100000.0 / NULLIF(c.total_population, 0)) AS crime_per_100k, -- Crime rate per 100k residents
    i.income AS median_household_income  -- Median household income per year
FROM academy.tayloro.colorado_crime_population_trends c
LEFT JOIN academy.tayloro.colorado_income_1997_2020 i
    ON LOWER(c.county_name) = LOWER(i.county)
    AND c.year = i.periodyear
    AND i.inctype = 3  -- 3 corresponds to "Median Household Income"
ORDER BY c.year;