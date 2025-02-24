SELECT
    county,
    cent_lat AS lat,
    cent_long AS lon,
    crime_per_100k,
    LN(1 + crime_per_100k) AS crime_log_scale  -- Use LN() for natural log (PostgreSQL, DuckDB, etc.)
FROM academy.tayloro.colorado_county_crime_per_capita_with_coordinates