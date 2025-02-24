CREATE TABLE tayloro.colorado_avg_age_per_crime_category_1997_2020 AS
SELECT
    city,
    offense_category_name,
    ROUND(AVG(age_num), 2) AS avg_age
FROM tayloro.colorado_crimes_with_cities
WHERE age_num IS NOT NULL
and city is not null
GROUP BY city, offense_category_name
ORDER BY city, offense_category_name;