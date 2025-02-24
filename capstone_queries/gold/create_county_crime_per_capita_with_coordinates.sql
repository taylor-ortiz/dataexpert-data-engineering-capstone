CREATE TABLE tayloro.colorado_county_crime_per_capita_with_coordinates AS
select ccc.label AS county,
       crime_per_100k,
       ccc.cent_lat,
       ccc.cent_long
from tayloro.colorado_county_crime_per_capita_1997_2020 crpc
         inner join tayloro.colorado_county_coordinates_raw ccc
                    on LOWER(crpc.county_name) = LOWER(ccc.label)