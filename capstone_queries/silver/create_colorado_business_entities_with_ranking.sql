CREATE TABLE tayloro.colorado_business_entities_with_ranking AS
SELECT
    be.*,
    fr.crime_rank,
    fr.income_rank,
    fr.population_rank,
    fr.final_rank
FROM tayloro.colorado_business_entities be
LEFT JOIN tayloro.colorado_final_county_tier_rank fr
    ON LOWER(be.principalcounty) = LOWER(fr.county);