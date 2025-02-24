UPDATE tayloro.colorado_business_entities
SET principalzipcode = CASE
    WHEN principalzipcode LIKE '%-%' THEN SUBSTRING(principalzipcode, 1, 5)
    ELSE principalzipcode
END
WHERE principalstate = 'CO'
  AND NOT regexp_like(principalzipcode, '^\d{5}$');