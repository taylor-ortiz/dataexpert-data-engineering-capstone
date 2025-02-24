CREATE TABLE tayloro.colorado_crime_tier_county_rank AS
WITH county_scores AS (
    SELECT
        county_name,
        -- Select the crime tiers for each category
        MAX(CASE WHEN table_name = 'colorado_crime_tier_property_destruction' THEN crime_tier ELSE NULL END) AS property_destruction_tier,
        MAX(CASE WHEN table_name = 'colorado_crime_tier_burglary' THEN crime_tier ELSE NULL END) AS burglary_tier,
        MAX(CASE WHEN table_name = 'colorado_crime_tier_larceny_theft' THEN crime_tier ELSE NULL END) AS larceny_theft_tier,
        MAX(CASE WHEN table_name = 'colorado_crime_tier_vehicle_theft' THEN crime_tier ELSE NULL END) AS vehicle_theft_tier,
        MAX(CASE WHEN table_name = 'colorado_crime_tier_robbery' THEN crime_tier ELSE NULL END) AS robbery_tier,
        MAX(CASE WHEN table_name = 'colorado_crime_tier_arson' THEN crime_tier ELSE NULL END) AS arson_tier,
        MAX(CASE WHEN table_name = 'colorado_crime_tier_stolen_property' THEN crime_tier ELSE NULL END) AS stolen_property_tier,
        -- Calculate the average of all crime tiers
        ROUND(
            AVG(
                CASE
                    WHEN table_name IN (
                        'colorado_crime_tier_property_destruction',
                        'colorado_crime_tier_burglary',
                        'colorado_crime_tier_larceny_theft',
                        'colorado_crime_tier_vehicle_theft',
                        'colorado_crime_tier_robbery',
                        'colorado_crime_tier_arson',
                        'colorado_crime_tier_stolen_property'
                    ) THEN crime_tier
                    ELSE NULL
                END
            )
        ) AS overall_crime_tier -- Round to the nearest whole number
    FROM (
        -- Union all 7 crime tier tables
        SELECT county_name, 'colorado_crime_tier_property_destruction' AS table_name, crime_tier FROM academy.tayloro.colorado_crime_tier_property_destruction
        UNION ALL
        SELECT county_name, 'colorado_crime_tier_burglary' AS table_name, crime_tier FROM academy.tayloro.colorado_crime_tier_burglary
        UNION ALL
        SELECT county_name, 'colorado_crime_tier_larceny_theft' AS table_name, crime_tier FROM academy.tayloro.colorado_crime_tier_larceny_theft
        UNION ALL
        SELECT county_name, 'colorado_crime_tier_vehicle_theft' AS table_name, crime_tier FROM academy.tayloro.colorado_crime_tier_vehicle_theft
        UNION ALL
        SELECT county_name, 'colorado_crime_tier_robbery' AS table_name, crime_tier FROM academy.tayloro.colorado_crime_tier_robbery
        UNION ALL
        SELECT county_name, 'colorado_crime_tier_arson' AS table_name, crime_tier FROM academy.tayloro.colorado_crime_tier_arson
        UNION ALL
        SELECT county_name, 'colorado_crime_tier_stolen_property' AS table_name, crime_tier FROM academy.tayloro.colorado_crime_tier_stolen_property
    ) combined
    GROUP BY county_name
)
SELECT * FROM county_scores
ORDER BY overall_crime_tier DESC;
