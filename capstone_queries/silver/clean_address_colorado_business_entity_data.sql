SELECT
    cbe.principalcity,
    COUNT(*) AS cnt
FROM tayloro.colorado_business_entities cbe
WHERE cbe.principalstate = 'CO'
  AND NOT EXISTS (
    SELECT 1
    FROM tayloro.colorado_city_county_zip ccz
    WHERE LOWER(ccz.city) = LOWER(cbe.principalcity)
  )
GROUP BY cbe.principalcity
ORDER BY cbe.principalcity;

SELECT
    cbe.principalzipcode,
    cbe.principalcity,
    COUNT(*) AS record_count
FROM tayloro.colorado_business_entities cbe
LEFT JOIN tayloro.colorado_city_county_zip ccz
  ON TRIM(LOWER(cbe.principalcity)) = TRIM(LOWER(ccz.city))
  AND TRIM(CAST(cbe.principalzipcode AS VARCHAR)) = TRIM(CAST(ccz.zip_code AS VARCHAR))
WHERE cbe.principalstate = 'CO'
  AND ccz.city IS NULL
and cbe.principalzipcode is not null
and cbe.principalcity is not null
GROUP BY
    cbe.principalzipcode,
    cbe.principalcity
ORDER BY
    record_count DESC;




select * from tayloro.colorado_business_entities where principalzipcode = '80401'

select * from tayloro.colorado_city_county_zip where lower(city) LIKE '%timn%' order by zip_code
select * from tayloro.colorado_city_county_zip where zip_code = 80303

INSERT INTO tayloro.colorado_city_county_zip (city, zip_code, county)
VALUES ('Evans', 80634, 'Weld'),
    ('Evans', 80645, 'Weld')

SELECT DISTINCT principalzipcode, principalcounty
FROM tayloro.colorado_business_entities
WHERE LOWER(principalcounty) LIKE '%douglas%';

SELECT COUNT(*) AS unmatched_count
FROM tayloro.colorado_business_entities cbe
LEFT JOIN tayloro.colorado_city_county_zip ccz
  ON TRIM(LOWER(cbe.principalcity)) = TRIM(LOWER(ccz.city))
  AND TRIM(CAST(cbe.principalzipcode AS VARCHAR)) = TRIM(CAST(ccz.zip_code AS VARCHAR))
WHERE cbe.principalstate = 'CO'
  AND ccz.city IS NULL;



UPDATE tayloro.colorado_business_entities
SET principalcity = 'Peyton'
WHERE principalstate = 'CO'
  AND principalcity = 'peyton co';


UPDATE tayloro.colorado_business_entities
SET principalcity = 'Fort Collins'
WHERE principalstate = 'CO'
  AND principalzipcode = '80524';



SELECT *
FROM tayloro.colorado_business_entities cbe
LEFT JOIN tayloro.colorado_city_county_zip ccz
  ON LOWER(cbe.principalcity) = LOWER(ccz.city)
  AND cbe.principalzipcode = CAST(ccz.zip_code AS VARCHAR(5))
WHERE cbe.principalstate = 'CO'
  AND ccz.city IS NULL;


SELECT *
FROM tayloro.colorado_business_entities
WHERE principalzipcode = '80422'
and lower(principalcity) <> 'black hawk'

SELECT
    CASE
        WHEN ccz.city IS NULL THEN 'No Match'
        ELSE 'Match'
    END AS match_status,
    COUNT(*) AS count
FROM tayloro.colorado_business_entities cbe
LEFT JOIN tayloro.colorado_city_county_zip ccz
    ON LOWER(cbe.principalcity) = LOWER(ccz.city)
    AND cbe.principalzipcode = CAST(ccz.zip_code AS VARCHAR(5))
WHERE cbe.principalstate = 'CO'
GROUP BY
    CASE
        WHEN ccz.city IS NULL THEN 'No Match'
        ELSE 'Match'
    END;


SELECT *
FROM tayloro.colorado_business_entities cbe
LEFT JOIN tayloro.colorado_city_county_zip ccz
  ON TRIM(LOWER(cbe.principalcity)) = TRIM(LOWER(ccz.city))
  AND TRIM(CAST(cbe.principalzipcode AS VARCHAR)) = TRIM(CAST(ccz.zip_code AS VARCHAR))
WHERE cbe.principalstate = 'CO'
  AND ccz.city IS NULL;

select * from tayloro.colorado_business_entities where principalcounty LIKE '%County%'




UPDATE tayloro.colorado_business_entities
SET
  principalcity = (
    SELECT geo_city
    FROM tayloro.colorado_temp_geocoded_entities
    WHERE entityid = tayloro.colorado_business_entities.entityid
  ),
  principalzipcode = (
    SELECT geo_postcode
    FROM tayloro.colorado_temp_geocoded_entities
    WHERE entityid = tayloro.colorado_business_entities.entityid
  ),
  principalcounty = (
    SELECT replace(geo_county, ' County', '')
    FROM tayloro.colorado_temp_geocoded_entities
    WHERE entityid = tayloro.colorado_business_entities.entityid
  )
WHERE entityid IN (
  SELECT entityid
  FROM tayloro.colorado_temp_geocoded_entities
);


select cbe.principalcity, ctge.geo_city, cbe.principalcounty, ctge.geo_county, cbe.principalzipcode, ctge.geo_postcode from tayloro.colorado_temp_geocoded_entities ctge
join tayloro.colorado_business_entities cbe
on ctge.entityid = cbe.entityid

UPDATE tayloro.colorado_business_entities
SET
  principalcity = (
    SELECT geo_city
    FROM taylorSELECT
  CASE
    WHEN ccz.city IS NOT NULL THEN 'Match'
    ELSE 'No Match'
  END AS match_status,
  COUNT(*) AS count
FROM tayloro.colorado_business_entities cbe
LEFT JOIN tayloro.colorado_city_county_zip ccz
  ON TRIM(LOWER(cbe.principalcity)) = TRIM(LOWER(ccz.city))
  AND TRIM(CAST(cbe.principalzipcode AS VARCHAR)) = TRIM(CAST(ccz.zip_code AS VARCHAR))
WHERE cbe.principalstate = 'CO'
GROUP BY
  CASE
    WHEN ccz.city IS NOT NULL THEN 'Match'
    ELSE 'No Match'
  END;o.colorado_temp_geocoded_entities
    WHERE entityid = tayloro.colorado_business_entities.entityid
  ),
  principalzipcode = (
    SELECT geo_postcode
    FROM tayloro.colorado_temp_geocoded_entities
    WHERE entityid = tayloro.colorado_business_entities.entityid
  ),
  principalcounty = (
    SELECT replace(geo_county, ' County', '')
    FROM tayloro.colorado_temp_geocoded_entities
    WHERE entityid = tayloro.colorado_business_entities.entityid
  )
WHERE entityid IN (
  SELECT entityid
  FROM tayloro.colorado_temp_geocoded_entities
);



SELECT DISTINCT
    cbe.principalzipcode,
    cbe.principalcity
FROM tayloro.colorado_business_entities cbe
LEFT JOIN tayloro.colorado_city_county_zip ccz
  ON TRIM(LOWER(cbe.principalcity)) = TRIM(LOWER(ccz.city))
  AND TRIM(CAST(cbe.principalzipcode AS VARCHAR)) = TRIM(CAST(ccz.zip_code AS VARCHAR))
WHERE cbe.principalstate = 'CO'
  AND ccz.city IS NULL
ORDER BY cbe.principalcity, cbe.principalzipcode;


DELETE FROM tayloro.colorado_business_entities
WHERE principalstate = 'CO'
  AND entityid IN (
    SELECT cbe.entityid
    FROM tayloro.colorado_business_entities cbe
    LEFT JOIN tayloro.colorado_city_county_zip ccz
      ON TRIM(LOWER(cbe.principalcity)) = TRIM(LOWER(ccz.city))
      AND TRIM(CAST(cbe.principalzipcode AS VARCHAR)) = TRIM(CAST(ccz.zip_code AS VARCHAR))
    WHERE cbe.principalstate = 'CO'
      AND ccz.city IS NULL
  );

SELECT county, COUNT(*) AS count
FROM tayloro.colorado_city_county_zip
WHERE LOWER(county) LIKE '%county%'
GROUP BY county
ORDER BY count DESC;

SELECT
    cbe.entityid,
    cbe.principalcity,
    cbe.principalzipcode,
    ccz.city AS ref_city,
    ccz.zip_code AS ref_zip,
    ccz.county AS ref_county
FROM tayloro.colorado_business_entities cbe
JOIN tayloro.colorado_city_county_zip ccz
  ON TRIM(LOWER(cbe.principalcity)) = TRIM(LOWER(ccz.city))
  AND TRIM(CAST(cbe.principalzipcode AS VARCHAR)) = TRIM(CAST(ccz.zip_code AS VARCHAR))
WHERE cbe.principalstate = 'CO'
LIMIT 100;


UPDATE tayloro.colorado_business_entities
SET principalcounty = (
    SELECT ccz.county
    FROM tayloro.colorado_city_county_zip ccz
    WHERE TRIM(LOWER(principalcity)) = TRIM(LOWER(ccz.city))
      AND TRIM(CAST(principalzipcode AS VARCHAR)) = TRIM(CAST(ccz.zip_code AS VARCHAR))
)
WHERE principalstate = 'CO';

CREATE TABLE tayloro.colorado_city_county_zip_new AS
SELECT DISTINCT zip_code, city, county
FROM tayloro.colorado_city_county_zip;

SELECT COUNT(*) AS original_count FROM tayloro.colorado_city_county_zip;
SELECT COUNT(*) AS distinct_count FROM tayloro.colorado_city_county_zip_new;

DROP TABLE tayloro.colorado_city_county_zip;
ALTER TABLE tayloro.colorado_city_county_zip_new RENAME TO tayloro.colorado_city_county_zip;


SELECT principalcity, principalzipcode, principalcounty
FROM tayloro.colorado_business_entities
WHERE principalstate = 'CO'
LIMIT 20;
