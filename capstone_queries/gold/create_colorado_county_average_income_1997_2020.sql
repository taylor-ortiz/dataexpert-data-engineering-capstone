SELECT
    county,
    ROUND(AVG(CASE WHEN inctype = 1 THEN income END), 2) AS avg_total_personal_income,  -- Total Personal Income
    ROUND(AVG(CASE WHEN inctype = 2 THEN income END), 2) AS avg_per_capita_income,  -- Per Capita Personal Income
    ROUND(AVG(CASE WHEN inctype = 3 THEN income END), 2) AS avg_median_household_income  -- Median Household Income
FROM academy.tayloro.colorado_income_1997_2020
WHERE periodyear BETWEEN 1997 AND 2020
GROUP BY county
ORDER BY county;