SELECT agency_name, total_crimes FROM academy.tayloro.colorado_county_agency_crime_counts where county_name = LOWER('$county') order by total_crimes desc
