CREATE TABLE tayloro.colorado_crimes AS
SELECT
    -- Clean and coalesce agency names using regex replacements:
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

    -- Normalize county names: convert to lowercase, replace semicolons with commas,
    -- remove quotes, split on commas, trim each element, sort, and rejoin.
    COALESCE(
      array_join(
        array_sort(
          transform(
            split(
              regexp_replace(regexp_replace(lower(trim(a.primary_county)), ';', ','), '"', '')
              , ','
            ),
            x -> trim(x)
          )
        ),
        ', '
      ),
      array_join(
        array_sort(
          transform(
            split(
              regexp_replace(regexp_replace(lower(trim(b.county_name)), ';', ','), '"', '')
              , ','
            ),
            x -> trim(x)
          )
        ),
        ', '
      )
    ) AS county_name,

    -- Parse and coalesce incident dates
    COALESCE(
        CAST(date_parse(trim(a.incident_date), '%m/%d/%Y') AS DATE),
        CAST(date_parse(trim(b.incident_date), '%m/%d/%Y') AS DATE)
    ) AS incident_date,

    -- Standardize incident hour
    COALESCE(a.incident_hour, b.incident_hour) AS incident_hour,

    -- Capitalize offense names (first letter uppercase, rest lowercase)
    COALESCE(
        UPPER(SUBSTRING(LOWER(trim(a.offense_name)), 1, 1)) || LOWER(SUBSTRING(LOWER(trim(a.offense_name)), 2)),
        UPPER(SUBSTRING(LOWER(trim(b.offense_name)), 1, 1)) || LOWER(SUBSTRING(LOWER(trim(b.offense_name)), 2))
    ) AS offense_name,

    -- Coalesce crime_against and offense_category_name
    COALESCE(a.crime_against, b.crime_against) AS crime_against,
    COALESCE(a.offense_category_name, b.offense_category_name) AS offense_category_name,

    -- Coalesce age_num
    COALESCE(a.age_num, b.age_num) AS age_num
FROM
    tayloro.colorado_crimes_1997_2015_raw a
FULL OUTER JOIN
    tayloro.colorado_crimes_2016_2020_raw b
ON
    -- Join on the cleaned agency names:
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
    AND
    -- Join on normalized county names:
    LOWER(
      array_join(
        array_sort(
          transform(
            split(
              regexp_replace(regexp_replace(lower(trim(a.primary_county)), ';', ','), '"', '')
            , ','
            ),
            x -> trim(x)
          )
        ),
        ', '
      )
    ) = LOWER(
      array_join(
        array_sort(
          transform(
            split(
              regexp_replace(regexp_replace(lower(trim(b.county_name)), ';', ','), '"', '')
            , ','
            ),
            x -> trim(x)
          )
        ),
        ', '
      )
    )
    AND CAST(date_parse(trim(a.incident_date), '%m/%d/%Y') AS DATE) = CAST(date_parse(trim(b.incident_date), '%m/%d/%Y') AS DATE)
    AND a.incident_hour = b.incident_hour
    AND a.offense_name = b.offense_name;
