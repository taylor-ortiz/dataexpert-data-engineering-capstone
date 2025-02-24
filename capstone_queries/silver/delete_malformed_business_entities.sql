DELETE FROM tayloro.colorado_business_entities
WHERE principalstate = 'CO'
  AND (
    CASE
      WHEN principalzipcode LIKE '%-%' THEN SUBSTRING(principalzipcode, 1, 5)
      ELSE principalzipcode
    END
  ) NOT LIKE '8%';