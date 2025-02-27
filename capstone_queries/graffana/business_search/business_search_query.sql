SELECT
    entityname AS Company_Name,
    principaladdress1 AS Address,
    principalcity AS City,
    principalstate AS State,
    principalcounty AS County
FROM
    academy.tayloro.colorado_business_entities_with_ranking
WHERE
    LOWER(entityname) LIKE LOWER('%$entityname%') AND principalstate = 'CO' -- Case-insensitive search
ORDER BY
    entityname