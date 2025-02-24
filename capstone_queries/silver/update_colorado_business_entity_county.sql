UPDATE tayloro.colorado_business_entities
SET principalcounty = (
    SELECT ccz.county
    FROM tayloro.colorado_city_county_zip ccz
    WHERE TRIM(LOWER(principalcity)) = TRIM(LOWER(ccz.city))
      AND TRIM(CAST(principalzipcode AS VARCHAR)) = TRIM(CAST(ccz.zip_code AS VARCHAR))
)
WHERE principalstate = 'CO';