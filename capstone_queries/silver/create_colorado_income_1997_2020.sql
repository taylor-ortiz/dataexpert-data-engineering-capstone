CREATE TABLE tayloro.colorado_income_1997_2020 AS
WITH colorado_income_1997_2020 AS (
    SELECT
        TRIM(REPLACE(areaname, 'County', '')) AS county,
        incdesc,
        inctype,
        incsource,
        income,
        incrank,
        periodyear
    FROM tayloro.colorado_income_raw
    WHERE stateabbrv = 'CO'
        AND areatyname = 'County'
        AND periodyear BETWEEN 1997 AND 2020 -- Filter for years 1997-2020
)
SELECT * FROM colorado_income_1997_2020;