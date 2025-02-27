CREATE TABLE tayloro.colorado_crime_weighted_scores AS
WITH crime_weighted AS (
    SELECT
        TRIM(SPLIT_PART(county_name, ';', 1)) AS county_name, -- Extract first county if multiple are listed
        EXTRACT(YEAR FROM incident_date) AS year,  -- Correctly extract year for grouping
        SUM(CASE WHEN offense_category_name = 'Destruction/Damage/Vandalism of Property' THEN 10 ELSE 0 END) AS destruction_vandalism,
        SUM(CASE WHEN offense_category_name = 'Burglary/Breaking & Entering' THEN 9 ELSE 0 END) AS burglary,
        SUM(CASE WHEN offense_category_name = 'Larceny/Theft Offenses' THEN 8 ELSE 0 END) AS larceny_theft,
        SUM(CASE WHEN offense_category_name = 'Motor Vehicle Theft' THEN 7 ELSE 0 END) AS motor_vehicle_theft,
        SUM(CASE WHEN offense_category_name = 'Robbery' THEN 7 ELSE 0 END) AS robbery,
        SUM(CASE WHEN offense_category_name = 'Arson' THEN 7 ELSE 0 END) AS arson,
        SUM(CASE WHEN offense_category_name = 'Stolen Property Offenses' THEN 6 ELSE 0 END) AS stolen_property,
        SUM(CASE WHEN offense_category_name = 'Counterfeiting/Forgery' THEN 5 ELSE 0 END) AS counterfeiting_forgery,
        SUM(CASE WHEN offense_category_name = 'Fraud Offenses' THEN 5 ELSE 0 END) AS fraud,
        SUM(CASE WHEN offense_category_name = 'Bribery' THEN 4 ELSE 0 END) AS bribery,
        SUM(CASE WHEN offense_category_name = 'Extortion/Blackmail' THEN 4 ELSE 0 END) AS extortion_blackmail,
        SUM(CASE WHEN offense_category_name = 'Embezzlement' THEN 3 ELSE 0 END) AS embezzlement,
        SUM(CASE WHEN offense_category_name = 'Weapon Law Violations' THEN 3 ELSE 0 END) AS weapon_law_violations,
        SUM(CASE WHEN offense_category_name = 'Drug/Narcotic Offenses' THEN 3 ELSE 0 END) AS drug_offenses,
        SUM(CASE WHEN offense_category_name = 'Prostitution Offenses' THEN 2 ELSE 0 END) AS prostitution_offenses,
        SUM(CASE WHEN offense_category_name = 'Kidnapping/Abduction' THEN 2 ELSE 0 END) AS kidnapping_abduction,
        SUM(CASE WHEN offense_category_name = 'Sex Offenses' THEN 2 ELSE 0 END) AS sex_offenses,
        SUM(CASE WHEN offense_category_name = 'Human Trafficking' THEN 1 ELSE 0 END) AS human_trafficking,
        SUM(CASE WHEN offense_category_name = 'Gambling Offenses' THEN 1 ELSE 0 END) AS gambling_offenses,
        SUM(CASE WHEN offense_category_name = 'Animal Cruelty' THEN 1 ELSE 0 END) AS animal_cruelty,
        SUM(
            CASE
                WHEN offense_category_name = 'Destruction/Damage/Vandalism of Property' THEN 10
                WHEN offense_category_name = 'Burglary/Breaking & Entering' THEN 9
                WHEN offense_category_name = 'Larceny/Theft Offenses' THEN 8
                WHEN offense_category_name = 'Motor Vehicle Theft' THEN 7
                WHEN offense_category_name = 'Robbery' THEN 7
                WHEN offense_category_name = 'Arson' THEN 7
                WHEN offense_category_name = 'Stolen Property Offenses' THEN 6
                WHEN offense_category_name = 'Counterfeiting/Forgery' THEN 5
                WHEN offense_category_name = 'Fraud Offenses' THEN 5
                WHEN offense_category_name = 'Bribery' THEN 4
                WHEN offense_category_name = 'Extortion/Blackmail' THEN 4
                WHEN offense_category_name = 'Embezzlement' THEN 3
                WHEN offense_category_name = 'Weapon Law Violations' THEN 3
                WHEN offense_category_name = 'Drug/Narcotic Offenses' THEN 3
                WHEN offense_category_name = 'Prostitution Offenses' THEN 2
                WHEN offense_category_name = 'Kidnapping/Abduction' THEN 2
                WHEN offense_category_name = 'Sex Offenses' THEN 2
                WHEN offense_category_name = 'Human Trafficking' THEN 1
                WHEN offense_category_name = 'Gambling Offenses' THEN 1
                WHEN offense_category_name = 'Animal Cruelty' THEN 1
                ELSE 0
            END
        ) AS total_crime_score
    FROM tayloro.colorado_crimes
    WHERE TRIM(county_name) IS NOT NULL
      AND TRIM(county_name) <> ''
      AND incident_date IS NOT NULL
    GROUP BY county_name, EXTRACT(YEAR FROM incident_date)  -- Fix: Use correct grouping
)
SELECT * FROM crime_weighted;