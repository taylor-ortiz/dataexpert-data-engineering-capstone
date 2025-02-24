CREATE TABLE tayloro.colorado_city_crime_time_likelihood AS
WITH crime_counts AS (
    SELECT
        city,
        offense_category_name,
        CASE
            WHEN incident_hour BETWEEN 6 AND 18 THEN 'Day'
            ELSE 'Night'
        END AS time_of_day,
        COUNT(*) AS total_crimes
    FROM tayloro.colorado_crimes_with_cities
    WHERE incident_hour IS NOT NULL
        AND offense_category_name IS NOT NULL
        AND city IS NOT NULL
    GROUP BY
        city,
        offense_category_name,
        CASE
            WHEN incident_hour BETWEEN 6 AND 18 THEN 'Day'
            ELSE 'Night'
        END
),
crime_pivot AS (
    SELECT
        city,
        offense_category_name,
        AVG(CASE WHEN time_of_day = 'Day' THEN total_crimes ELSE NULL END) AS avg_day_crimes,
        AVG(CASE WHEN time_of_day = 'Night' THEN total_crimes ELSE NULL END) AS avg_night_crimes
    FROM crime_counts
    GROUP BY city, offense_category_name
)
SELECT
    city,
    offense_category_name,
    avg_day_crimes,
    avg_night_crimes,
    CASE
        WHEN avg_night_crimes > avg_day_crimes THEN TRUE
        ELSE FALSE
    END AS is_night_likely,
    CASE
        WHEN avg_day_crimes > avg_night_crimes THEN TRUE
        ELSE FALSE
    END AS is_day_likely
FROM crime_pivot
ORDER BY city, offense_category_name;
