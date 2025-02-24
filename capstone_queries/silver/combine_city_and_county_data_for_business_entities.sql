INSERT INTO tayloro.colorado_business_entities
SELECT b.entityid,
       b.entityname,
       b.principaladdress1,
       b.principaladdress2,
       b.principalcity,
       c.county AS principalcounty, -- Mapping county from cities dataset
       b.principalstate,
       b.principalzipcode,
       b.principalcountry,
       b.entitystatus,
       b.jurisdictonofformation,
       b.entitytype,
       CASE
           WHEN regexp_like(TRIM(b.entityformdate), '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$')
               THEN CAST(date_parse(TRIM(b.entityformdate), '%m/%d/%Y') AS DATE)
           ELSE NULL
           END  AS entityformdate   -- Convert only valid dates
FROM tayloro.colorado_business_entities_raw b
         INNER JOIN tayloro.colorado_cities_and_counties c
                   ON LOWER(TRIM(b.principalcity)) = LOWER(TRIM(c.city))