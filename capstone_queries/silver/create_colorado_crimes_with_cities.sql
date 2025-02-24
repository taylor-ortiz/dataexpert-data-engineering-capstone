CREATE TABLE tayloro.colorado_crimes_with_cities AS
SELECT
    -- Clean and coalesce agency names from both datasets
    COALESCE(
      trim(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              regexp_replace(a.agency_name, '(?i)\\bDrug Enforcement Team\\b', 'Drug Task Force'),
              '(?i)\\s+Police Department\\b', ''
            ),
            '(?i)\\s+County Sheriff(?:''s Office)?\\b', ''
          ),
          '(?i)\\s+Sheriff(?:''s Office)?\\b', ''
        )
      ),
      trim(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              regexp_replace(b.pub_agency_name, '(?i)\\bDrug Enforcement Team\\b', 'Drug Task Force'),
              '(?i)\\s+Police Department\\b', ''
            ),
            '(?i)\\s+County Sheriff(?:''s Office)?\\b', ''
          ),
          '(?i)\\s+Sheriff(?:''s Office)?\\b', ''
        )
      )
    ) AS agency_name,

    -- Standardize and clean county names with first letter capitalized
    COALESCE(
        UPPER(SUBSTRING(LOWER(TRIM(a.primary_county)), 1, 1)) || LOWER(SUBSTRING(LOWER(TRIM(a.primary_county)), 2)),
        UPPER(SUBSTRING(LOWER(TRIM(b.county_name)), 1, 1)) || LOWER(SUBSTRING(LOWER(TRIM(b.county_name)), 2))
    ) AS county_name,

    -- Combine and clean incident dates, converting them to DATE
    COALESCE(
        CAST(date_parse(TRIM(a.incident_date), '%m/%d/%Y') AS DATE),
        CAST(date_parse(TRIM(b.incident_date), '%m/%d/%Y') AS DATE)
    ) AS incident_date,

    -- Standardize incident hour
    COALESCE(a.incident_hour, b.incident_hour) AS incident_hour,

    -- Capitalize offense names (first letter capitalized, rest lowercase)
    COALESCE(
        UPPER(SUBSTRING(LOWER(TRIM(a.offense_name)), 1, 1)) || LOWER(SUBSTRING(LOWER(TRIM(a.offense_name)), 2)),
        UPPER(SUBSTRING(LOWER(TRIM(b.offense_name)), 1, 1)) || LOWER(SUBSTRING(LOWER(TRIM(b.offense_name)), 2))
    ) AS offense_name,

    -- Clean crime_against and offense_category_name
    COALESCE(a.crime_against, b.crime_against) AS crime_against,
    COALESCE(a.offense_category_name, b.offense_category_name) AS offense_category_name,

    -- Clean age_num
    COALESCE(a.age_num, b.age_num) AS age_num,

    -- Coalesce the city values: use city_name from the 1997–2015 data when available; otherwise, use city from the 2016–2020 data
    COALESCE(a.city_name, b.city) AS city
FROM
    tayloro.colorado_crimes_1997_2015_raw a
FULL OUTER JOIN
    tayloro.colorado_crimes_2016_2020_with_cities b
ON
    -- Clean agency names in the join condition so they match properly
    LOWER(
      trim(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              regexp_replace(a.agency_name, '(?i)\\bDrug Enforcement Team\\b', 'Drug Task Force'),
              '(?i)\\s+Police Department\\b', ''
            ),
            '(?i)\\s+County Sheriff(?:''s Office)?\\b', ''
          ),
          '(?i)\\s+Sheriff(?:''s Office)?\\b', ''
        )
      )
    ) =
    LOWER(
      trim(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              regexp_replace(b.pub_agency_name, '(?i)\\bDrug Enforcement Team\\b', 'Drug Task Force'),
              '(?i)\\s+Police Department\\b', ''
            ),
            '(?i)\\s+County Sheriff(?:''s Office)?\\b', ''
          ),
          '(?i)\\s+Sheriff(?:''s Office)?\\b', ''
        )
      )
    )
    AND LOWER(TRIM(a.primary_county)) = LOWER(TRIM(b.county_name))
    AND CAST(date_parse(TRIM(a.incident_date), '%m/%d/%Y') AS DATE) = CAST(date_parse(TRIM(b.incident_date), '%m/%d/%Y') AS DATE)
    AND a.incident_hour = b.incident_hour
    AND a.offense_name = b.offense_name;