INSERT INTO tayloro.coloraodo_county_crime_rate_per_capita
WITH crimes_per_year AS (
    SELECT
        county_name,
        YEAR(incident_date) AS periodyear,  -- Extract year from incident_date
        COUNT(*) AS total_crimes
    FROM tayloro.colorado_crimes
    WHERE
        incident_date IS NOT NULL
        AND YEAR(incident_date) BETWEEN 1997 AND 2020  -- Filter within date range
    GROUP BY county_name, YEAR(incident_date)  -- Group by county and year
),
population_per_year AS (
    SELECT
        county AS county_name,
        YEAR AS periodyear,
        SUM(TOTALPOPULATION) AS total_population  -- Aggregate population by year and county
    FROM tayloro.colorado_population_raw
    WHERE DATATYPE = 'Estimate'  -- Use only estimated population data
    GROUP BY county, YEAR  -- Group by county and year
),
crime_rate_per_capita AS (
    SELECT
        c.county_name,
        c.periodyear,
        c.total_crimes,
        p.total_population,
        (c.total_crimes * 100000.0 / p.total_population) AS crime_per_100k  -- Crime per 100,000 people
    FROM crimes_per_year c
    JOIN population_per_year p
        ON LOWER(TRIM(c.county_name)) = LOWER(TRIM(p.county_name))  -- Ensure proper join
        AND c.periodyear = p.periodyear  -- Match years
)
SELECT * FROM crime_rate_per_capita;