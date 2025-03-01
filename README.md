<p align="center">
<img width="547" alt="Screenshot 2025-02-27 at 6 01 10 PM" src="https://github.com/user-attachments/assets/3eedd775-764b-429b-af8e-3c2fe6b59f75" />
</p>

#

<p align="center">
  <strong>A data engineering capstone project that builds a program for Colorado's OEDIT to allocate security system subsidies to businesses using historical crime, population, and income data trends from 1997-2020.</strong>
</p>

### About

My name is Taylor Ortiz and I enrolled in Zach Wilson's Dataexpert.io Data Engineering bootcamp as part of the January 2025 bootcamp. Part of the requirements for achieving the highest certification in the bootcamp is to complete a capstone project that showcases the skills attained during the bootcamp and the general skills that I possess as a data engineer and data architect. The following capstone business case, KPIs, and use cases are fictional and made up by me for the sake of the exercise (although I think it has merit for an actual State of Colorado OEDIT program one day). The technology stack used was built entirely from scratch using publicly accessible data from [data.colorado.gov](https://data.colorado.gov) and [CDPHE Open Data](https://data-cdphe.opendata.arcgis.com/). Enjoy!

### Features

* 9,048,771 rows of source data extracted
* Grafana dashboards displaying 16 required KPIs
* 28 task DAG using Airflow data pipeline orchestration running in Astronomer production
* Medallion architecture data design patterns
* Comprehensive architecture diagram
* Comprehensive data dictionary displaying all data sources and manipulations used
* Geoapify Geocoding API
* Spark jobs with AWS

## Table of Contents
1. [Capstone Requirements](#capstone-requirements)
2. [Project Overview](#project-overview)
   1. [Problem Statement](#problem-statement)
   2. [KPIs and Use Cases](#kpis-and-use-cases)
      1. [County Level](#county-level)
      2. [City Level](#city-level)
   3. [Data Sources](#data-sources)
   4. [Data Tables](#data-tables)
   5. [Technologies Used](#technologies-used)
   6. [Architecture](#architecture)
3. [Business Entity Tier Ranking](#business-entity-tier-ranking)
   1. [Subsidy Tiers](#subsidy-tiers)
   2. [How Tiers Are Calculated and Assigned](#how-tiers-are-calculated-and-assigned)
        1. [Step 1: Identify Criteria](#step-1-identify-criteria)
        2. [Step 2: Establish Individual Rankings](#step-2-establish-individual-rankings)
        3. [Step 3: Merge Individual Crime Rankings to Form a Unified Crime Tier County Rank](#merge-individual-crime-rankings)
        4. [Step 4: Merge Rankings for Final County Tier Rank](#final-county-tier)
        5. [Step 5: Generate Business Entities Table with Final Ranking](#backfill-business-entities)
4. [KPI and Use Case Visualizations](#kpi-and-use-case-visualizations)
    1. [Colorado County Dashboard](#colorado-county-dashboard)
    2. [Colorado City Dashboard](#colorado-city-dashboard)
    3. [Colorado Crime Density Dashboard](#colorado-crime-density-dashboard)
5. [Business Entity Data Pipeline](#business-entity-data-pipeline)
    1. [Business Entity Daily DAG](#business-entity-daily-dag)
    2. [Task Descriptions](#task-descriptions)
    3. [DAG Flow Order](#dag-flow-order)
    4. [Astronomer Cloud Deployment](#astronomer-cloud-deployment)
6. [Putting It All Together](#putting-it-all-together)
    1. [Business Entity Search Dashboard](#business-entity-search-dashboard)
7. [Challenges and Findings](#challenges-and-findings)
    1. [In-depth Summary of Business Entities Data Cleaning and Update Process](#business-entity-data-cleaning)
    2. [Out-of-Memory (OOM) Exceptions and Spark Configuration Adjustments](#out-of-memory-exceptions)
8. [Closing Thoughts and Next Steps](#closing-thoughts-and-next-steps)


## Capstone Requirements

* Identify a problem you'd like to solve.
* Scope the project and choose datasets.
* Use at least 2 different data sources and formats (e.g., CSV, JSON, APIs), with at least 1 million rows.
* Define your use case: analytics table, relational database, dashboard, etc.
* Choose your tech stack and justify your choices.
* Explore and assess the data.
* Identify and resolve data quality issues.
* Document your data cleaning steps.
* Create a data model diagram and dictionary.
* Build ETL pipelines to transform and load the data.
* Run pipelines and perform data quality checks.
* Visualize or present the data through dashboards, applications, or reports.

## Project Overview

### Problem Statement

* The State of Colorado has hired your data firm to develop an internal back-office tool to support a critical initiative for the Office of Economic Development and International Trade (OEDIT). The program, B.A.S.E. (Business Assistance for Security Enhancements), is designed to evaluate and qualify businesses across the state for business security system subsidies to protect their business based on historical crime trends, income, and population data by city and county. Historical data from 1997 to 2020, as well as real time data, will be analyzed to determine which security tiers businesses qualify for.
* Additionally, the State of Colorado has sixteen KPIs and use cases they would like to see vizualized out of the extracted, transformed and aggregated data to assist them in planning, funding and outreach initiatives. 
* This tool will allow OEDIT to automatically notify active businesses in good standing about the subsidies they qualify for, streamlining the application process for business owners seeking to participate. Lastly, the State requires the creation of an intuitive user experience that enables businesses to search for their assigned tier, providing them with easy access to their eligibility information.

### KPIs and Use Cases

<details>
<summary id="county-level">County Level KPIs and Use Cases</summary>

1. **KPI:** Set a goal for each police agency within a given county to reduce crime by 5% within the next fiscal year, based on a baseline crime count from historical data.  
   - **Use Case:** Calculate crime count for police agencies in a given county to get a baseline number.

2. **KPI:** Bring 100% free self defense and resident safety programs to lower income counties.  
   - **Use Case:** Calculate the average median household income per county.

3. **KPI:** Evaluate additional police agency support required in counties that show a correlation between rising population and rising crime.  
   - **Use Case:** Show population trends for each county alongside corresponding crime trends by year.

4. **KPI:** Identify counties with an average annual population growth rate exceeding 10% from 1997–2020, and adjust state support allocations to improve funding alignment for resident programs.  
   - **Use Case:** Show population trends for each county from 1997–2020.

5. **KPI:** Inform police agencies of which crime categories are most prevalent in their respective county.  
   - **Use Case:** Calculate the totals for each crime category per county from 1997 to 2020 to gauge frequency.

6. **KPI:** Enable police agencies to be proactive by identifying which months of the year have a higher volume of crime.  
   - **Use Case:** Show average seasonal crime trends for each month by county.

7. **KPI:** Provide an interactive visual representation of crime density across counties for the entire state.  
   - **Use Case:** Create a geo-map of crime density for counties from 1997–2020.

8. **KPI:** Drive further marketing outreach for supportive safety programs in lower income counties.  
   - **Use Case:** Show average income per capita for counties.

9. **KPI:** Display crime rates compared to median household income to pinpoint high-risk areas.  
   - **Use Case:** Compare crime data with median household income for each county.

10. **KPI:** Illustrate crime type distribution by county by Property, Person and Society to better understand where to best allocate safety resources 
    - **Use Case:** Analyze the distribution of different crime types across each county from 1997–2020 for Property, Person and Society crimes.

</details>

<details>
<summary id="city-level">City Level KPIs and Use Cases</summary>

1. **KPI:** Deploy an interactive dashboard displaying seasonal crime trends for each city to detect seasonal peaks to guide targeted patrol planning.  
   - **Use Case:** Show seasonal crime trends for the year in each city.

2. **KPI:** Identify and report the top three crime categories most prevalent during daytime (6 AM–6 PM) in each city to optimize resource allocation during peak hours.  
   - **Use Case:** Determine which crimes are more likely to happen during the day for a city.

3. **KPI:** Calculate the average age of individuals involved in each crime category across cities to support the development of tailored intervention programs.  
   - **Use Case:** Compute the average age for crime categories across cities.

4. **KPI:** Identify and report the top three crime categories most common at night (6 PM–6 AM) in each city to inform optimized night patrol scheduling.  
   - **Use Case:** Determine which crimes are more likely to happen at night for a city.

5. **KPI:** Develop a dynamic visualization that shows the percentage distribution of crime types by city by Property, Person and Society to support targeted law enforcement initiatives.  
   - **Use Case:** Display crime type distribution by city.

6. **KPI:** Provide an analysis dashboard showing average crime trends by day of the week for each city, highlighting peak crime days to drive strategic patrol scheduling.  
   - **Use Case:** Show crime trends on average by day of the week to determine when to patrol more.

</details>

### Data Sources

- [Crimes in Colorado (2016-2020)](https://data.colorado.gov/Public-Safety/Crimes-in-Colorado/j6g4-gayk/about_data): Offenses in Colorado for 2016 through 2020 by Agency from the FBI's Crime Data Explorer.
    - Number of rows: 3.1M
- [Crimes in Colorado (1997-2015)](https://data.colorado.gov/Public-Safety/Crimes-in-Colorado-1997-to-2015/6vnq-az4b/about_data): Crime stats for the State of Colorado from 1997 to 2015. Data provided by the CDPS and the FBI's Crime Data Explorer (CDE).
    - Number of rows: 4.95M
- [Personal Income in Colorado](https://data.colorado.gov/Labor-and-Employment/Personal-Income-in-Colorado/2cpa-vbur/about_data): Income (per capita or total) for each county by year with rank and population. From Colorado Department of Labor and Employment (CDLE), since 1969.
    - Number of rows: 10k
- [Population Projections in Colorado](https://data.colorado.gov/Demographics/Population-Projections-in-Colorado/q5vp-adf3/about_data): Actual and predicted population data by gender and age from the Department of Local Affairs (DOLA), from 1990 to 2040.
    - Number of rows: 382k
- [Business Entities in Colorado](https://data.colorado.gov/Business/Business-Entities-in-Colorado/4ykn-tg5h/about_data): Colorado Business Entities (corporations, LLCs, etc.) registered with the Colorado Department of State (CDOS) since 1864.
    - Number of rows: 2.81M
- [Colorado County Boundaries](https://data-cdphe.opendata.arcgis.com/datasets/CDPHE::colorado-county-boundaries/about): This feature class contains county boundaries for all 64 Colorado counties and 2010 US Census attributes data describing the population within each county.
    - Number of rows: 64


### Data Tables

<details>
  <summary id="bronze-layer">Bronze Layer (raw) data</summary>

1. **colorado_business_entities_raw**  
   - **Number of rows:** 984,368
2. **colorado_city_county_zip_raw**  
   - **Number of rows:** 760
3. **colorado_county_coordinates_raw**  
   - **Number of rows:** 64
4. **colorado_crimes_1997_2015_raw**  
   - **Number of rows:** 4,952,282
5. **colorado_crimes_2016_2020_raw**  
   - **Number of rows:** 3,101,365
6. **colorado_income_raw**  
   - **Number of rows:** 9,932
  

</details>

<details>
  <summary id="silver-layer">Silver Layer (transform) data</summary>

1. **colorado_business_entities**  
   - **Number of rows:** 817,324 (grows by Airflow pipeline)
2. **colorado_business_entities_duplicates**  
   - **Number of rows:** 721 (grows by Airflow pipeline)
3. **colorado_business_entities_misfit_entities**  
   - **Number of rows:** 71 (grows by Airflow pipeline)
4. **colorado_business_entities_with_ranking**  
   - **Number of rows:** 817,324 (grows by Airflow pipeline)
5. **colorado_crime_population_trends**  
   - **Number of rows:** 1,536
6. **colorado_crime_tier_arson**  
   - **Number of rows:** 72
7. **colorado_crime_tier_burglary**  
   - **Number of rows:** 79
8. **colorado_crime_tier_county_rank**  
   - **Number of rows:** 79
9. **colorado_crime_tier_larceny_theft**  
   - **Number of rows:** 79
10. **colorado_crime_tier_property_destruction**  
    - **Number of rows:** 79
11. **colorado_crime_tier_property_destruction**  
    - **Number of rows:** 70
12. **colorado_crime_tier_stolen_property**  
    - **Number of rows:** 75
13. **colorado_crime_tier_vehicle_theft**  
    - **Number of rows:** 79
14. **colorado_crimes**  
    - **Number of rows:** 8,053,647
15. **colorado_crimes_2016_2020_with_cities**  
    - **Number of rows:** 2,771,984
16. **colorado_crimes_with_cities**  
    - **Number of rows:** 7,724,266
17. **colorado_final_county_tier_rank**  
    - **Number of rows:** 64
18. **colorado_income_1997_2020**  
    - **Number of rows:** 4604
19. **colorado_income_household_tier_rank**  
    - **Number of rows:** 64
20. **colorado_population_raw**  
    - **Number of rows:** 381,504
21. **colorado_temp_geocoded_entities**  
    - **Number of rows:** 2,033
</details>

<details>
  <summary id="gold-layer">Gold Layer (aggregate) data</summary>

1. **colorado_avg_age_per_crime_category_1997_2020**  
   - **Number of rows:** 3,154
2. **colorado_city_crime_counts_1997_2020**  
   - **Number of rows:** 182
3. **colorado_city_crime_time_likelihood**  
   - **Number of rows:** 3,206
4. **colorado_city_seasonal_crime_rates_1997_2020**  
   - **Number of rows:** 2,196
5. **colorado_county_agency_crime_counts**  
   - **Number of rows:** 526
6. **colorado_county_average_income_1997_2020**  
   - **Number of rows:** 64
7. **colorado_county_crime_per_capita_1997_2020**  
   - **Number of rows:** 64
8. **colorado_county_crime_per_capita_with_coordinates**  
   - **Number of rows:** 64
9. **colorado_county_crime_vs_population_1997_2020**  
   - **Number of rows:** 1,665
10. **colorado_county_seasonal_crime_rates_1997_2020**  
    - **Number of rows:** 911
11. **colorado_crime_category_totals_per_county_1997_2020**  
    - **Number of rows:** 1,520
12. **colorado_crime_type_distribution_by_city**  
    - **Number of rows:** 549
13. **colorado_crime_vs_median_household_income_1997_2020**  
    - **Number of rows:** 1,536
14. **colorado_population_crime_per_capita_rank**  
    - **Number of rows:** 64
15. **colorado_total_population_per_county_1997_2020**  
    - **Number of rows:** 1,536
16. **coloraodo_county_crime_rate_per_capita**  
    - **Number of rows:** 1,458
</details>

Access the entire [capstone data dictionary](https://github.com/taylor-ortiz/dataexpert-data-engineering-capstone/blob/main/Capstone-data-dictionary.csv) for more detailed info on these datasets used.

### Technologies Used
- **Python:** Programming language used for Spark jobs and Airflow orchestration
- **Airflow:** orchestration tool that manages, schedules and automates workflows using Apache Airflow
- **AWS:** S3 storage for CSV extracts
- **Tabular:** data lake operations
- **Trino:** distributed SQL query engine
- **Geoapify:** geocoding api for address validation of business entities
- **Grafana:** dashboards for data visualizations
- **Astronomer:** data orchestraton platform that provides a managed service for Apache Airflow
- **Apache Spark:** open source distributed computing system for processing source extracts from AWS

### Architecture
![B A S E  Future State Diagram (4)](https://github.com/user-attachments/assets/78f8aaf2-af71-44d2-a08c-52d9b0d766da)

## Business Entity Tier Ranking

### Subsidy Tiers

Tiers are represented as a range of 1 through 4 in the B.A.S.E. program. 1 indicates the lowest level need and 4 indicates the highest level need. Each subsidy tier below offers a gradual increase of security system services based on the tier that your business qualifies for. The idea is that tiers will be backfilled and assigned on the source business entities dataset and as new business entities are added daily to the Colorado Information Marketplace, those records will also be assigned a tier through the Airflow orchestration that we will cover below. However, how are tiers actually calculated and assigned? Read on!

<img width="1406" alt="Screenshot 2025-02-28 at 4 44 20 PM" src="https://github.com/user-attachments/assets/46d1a897-69bd-4d81-a2b7-61c1001996ef" />

### How Tiers Are Calculated and Assigned

<details>
<summary id="step-1-identify-criteria"><strong>Step 1: Identify Criteria</strong></summary>

- **Crimes:**  
  Out of all the crime categories that exist, I chose 7 that closely resembled property-related or adjacent crimes that would factor into the ranking:
  - Destruction/Damage/Vandalism of Property
  - Burglary/Breaking & Entering
  - Larceny/Theft Offenses
  - Motor Vehicle Theft
  - Robbery
  - Arson
  - Stolen Property Offenses

- **Population:**  
  Identify crime per capita (per 1,000 residents) for all counties between the years of 1997 and 2020.

- **Income:**  
  Identify Median Household Income for all counties between the years of 1997 and 2020.

</details>

<details>
<summary id="step-2-establish-individual-rankings"><strong>Step 2: Establish Individual Rankings</strong></summary>
<br/>
For each metric, we transform raw data into a standardized ranking by following a similar process:

##### Crime Categories
For each of the 7 selected crime categories:
- **Aggregate Data:**  
  Count the number of incidents per county.
- **Compute Percentile Ranks:**  
  Use a percentile function (e.g., `PERCENT_RANK()`) to determine each county’s standing relative to others.
- **Assign Tiers:**  
  Counties in the highest percentiles (top 25%) receive an assignment of 4. Counties >= 50% and <= 75% receive an assignment of 3. Counties >= 25% and <= 50% receive an assignment of 2. Counties that are <= 25% receive an assignment of 1. 

##### Population-Adjusted Crime (Crime Per Capita)
We adjust raw crime counts by county population to calculate the crime rate per 1,000 residents. This involves:
- **Calculating Yearly Crime Rates:**  
  Join crime data with population figures.
- **Averaging:**  
  Compute an average crime rate per county over the selected years.
- **Ranking:**  
  Counties in the highest percentiles (top 25%) receive an assignment of 4. Counties >= 50% and <= 75% receive an assignment of 3. Counties >= 25% and <= 50% receive an assignment of 2. Counties that are <= 25% receive an assignment of 1. 

##### Income
For median household income, we:
- **Aggregate Income Data:**  
  Average the income per county across the years.
- **Compute Percentile Ranks:**  
  Establish how each county compares to others.
- **Assign Tiers:**  
  Income is the inverse of the previous two. We actually want to rank the best Income counties the lowest because they would require less subsidy assistance than lower income counties. Therefore, Counties in the highest percentiles (top 25%) receive an assignment of 1. Counties >= 50% and <= 75% receive an assignment of 2. Counties >= 25% and <= 50% receive an assignment of 3. Counties that are <= 25% receive an assignment of 4. 

  Below is an example distribution. This is not perfect and I would certainly look to have this be a little more even but the result is good for a public dataset.

  <img width="1123" alt="Screenshot 2025-02-28 at 5 24 50 PM" src="https://github.com/user-attachments/assets/b81e41d2-5fc1-4cf4-8055-36df7663a537" />

  <br/>
Below is an example query for evaluating the Destruction/Damage/Vandalism of Property crime category and assigning a tier to counties:

```sql
CREATE TABLE tayloro.colorado_crime_tier_destruction_property AS
WITH crime_aggregated AS (
    -- Step 1: Aggregate property destruction crime counts per county
    SELECT
        county,
        COUNT(*) AS total_property_destruction_crimes
    FROM academy.tayloro.colorado_crimes
    WHERE offense_category_name = 'Destruction/Damage/Vandalism of Property'
    GROUP BY county
),
crime_percentiles AS (
    -- Step 2: Compute percentile rank for property destruction crime counts per county
    SELECT
        county,
        total_property_destruction_crimes,
        PERCENT_RANK() OVER (ORDER BY total_arson_crimes) AS crime_percentile
    FROM crime_aggregated
),
crime_tiers AS (
    -- Step 3: Assign tiers based on percentile rankings
    SELECT
        county,
        total_property_destruction_crimes,
        crime_percentile,
        CASE
            WHEN crime_percentile >= 0.75 THEN 1 -- Counties with the highest counts (Top 25%)
            WHEN crime_percentile >= 0.50 THEN 2 -- Next 25% (50%-75%)
            WHEN crime_percentile >= 0.25 THEN 3 -- Next 25% (25%-50%)
            ELSE 4                             -- Counties with the lowest counts (Bottom 25%)
        END AS crime_tier
    FROM crime_percentiles
)
SELECT * FROM crime_tiers;
```

  <br/>
<img width="969" alt="Screenshot 2025-02-28 at 9 07 04 PM" src="https://github.com/user-attachments/assets/56a36431-784e-4ca9-a5c3-a9c155987be1" />

</details>

<details>
<summary id="merge-individual-crime-rankings"><strong>Step 3: Merge Individual Crime Rankings to Form a Unified Crime Tier County Rank</strong></summary>

After calculating individual crime tiers for each of the 7 selected crime categories, the next step is to merge these rankings into a single unified score per county. This unified score—referred to as the **overall crime tier**—is computed by averaging the individual tiers for each county, then rounding the result to the nearest whole number. The process is accomplished by:

- **Unioning the Individual Tables:**  
  All individual crime tier tables (e.g., for property destruction, burglary, larceny/theft, etc.) are combined using a `UNION ALL`. Each record is tagged with its corresponding table name for identification.

- **Pivoting and Aggregating Data:**  
  The combined dataset is grouped by county. For each crime category, the query selects the crime tier, and then computes an average of these tiers across all categories.

- **Final Ordering:**  
  The unified table is then sorted by the overall crime tier in descending order, highlighting counties with the highest aggregated crime tiers.

Below is the SQL query that performs these operations:

```sql
CREATE TABLE tayloro.colorado_crime_tier_county_rank AS
WITH county_scores AS (
    SELECT
        county_name,
        -- Select the crime tiers for each category
        MAX(CASE WHEN table_name = 'colorado_crime_tier_property_destruction' THEN crime_tier ELSE NULL END) AS property_destruction_tier,
        MAX(CASE WHEN table_name = 'colorado_crime_tier_burglary' THEN crime_tier ELSE NULL END) AS burglary_tier,
        MAX(CASE WHEN table_name = 'colorado_crime_tier_larceny_theft' THEN crime_tier ELSE NULL END) AS larceny_theft_tier,
        MAX(CASE WHEN table_name = 'colorado_crime_tier_vehicle_theft' THEN crime_tier ELSE NULL END) AS vehicle_theft_tier,
        MAX(CASE WHEN table_name = 'colorado_crime_tier_robbery' THEN crime_tier ELSE NULL END) AS robbery_tier,
        MAX(CASE WHEN table_name = 'colorado_crime_tier_arson' THEN crime_tier ELSE NULL END) AS arson_tier,
        MAX(CASE WHEN table_name = 'colorado_crime_tier_stolen_property' THEN crime_tier ELSE NULL END) AS stolen_property_tier,
        -- Calculate the average of all crime tiers
        ROUND(
            AVG(
                CASE
                    WHEN table_name IN (
                        'colorado_crime_tier_property_destruction',
                        'colorado_crime_tier_burglary',
                        'colorado_crime_tier_larceny_theft',
                        'colorado_crime_tier_vehicle_theft',
                        'colorado_crime_tier_robbery',
                        'colorado_crime_tier_arson',
                        'colorado_crime_tier_stolen_property'
                    ) THEN crime_tier
                    ELSE NULL
                END
            )
        ) AS overall_crime_tier -- Round to the nearest whole number
    FROM (
        -- Union all 7 crime tier tables
        SELECT county_name, 'colorado_crime_tier_property_destruction' AS table_name, crime_tier FROM academy.tayloro.colorado_crime_tier_property_destruction
        UNION ALL
        SELECT county_name, 'colorado_crime_tier_burglary' AS table_name, crime_tier FROM academy.tayloro.colorado_crime_tier_burglary
        UNION ALL
        SELECT county_name, 'colorado_crime_tier_larceny_theft' AS table_name, crime_tier FROM academy.tayloro.colorado_crime_tier_larceny_theft
        UNION ALL
        SELECT county_name, 'colorado_crime_tier_vehicle_theft' AS table_name, crime_tier FROM academy.tayloro.colorado_crime_tier_vehicle_theft
        UNION ALL
        SELECT county_name, 'colorado_crime_tier_robbery' AS table_name, crime_tier FROM academy.tayloro.colorado_crime_tier_robbery
        UNION ALL
        SELECT county_name, 'colorado_crime_tier_arson' AS table_name, crime_tier FROM academy.tayloro.colorado_crime_tier_arson
        UNION ALL
        SELECT county_name, 'colorado_crime_tier_stolen_property' AS table_name, crime_tier FROM academy.tayloro.colorado_crime_tier_stolen_property
    ) combined
    GROUP BY county_name
)
SELECT * FROM county_scores
ORDER BY overall_crime_tier DESC;
```

<img width="1605" alt="Screenshot 2025-02-28 at 9 20 22 PM" src="https://github.com/user-attachments/assets/349ce15c-1724-453a-960e-45e0506d089e" />

</details>

<details>
<summary id="final-county-tier"><strong>Step 4: Merge Rankings for Final County Tier Rank</strong></summary>

In this step, we combine the overall crime tier county rank, the income tier rank, and the population (crime per capita) rank to generate a final composite ranking for each county. Since our primary focus is on crime, we weigh the crime rank a little heavier in our final calculation.

The merging process involves:
- **Combining Ranks:**  
  We perform a full outer join on the three individual ranking tables to ensure every county is represented.
- **Handling Missing Values:**  
  `COALESCE` is used to manage missing values by defaulting to 0 when a rank is not available.
- **Calculating the Final Rank:**  
  The final rank is computed as the average of the three rankings. With crime being our primary focus, its rank is given slightly more influence in this average.
- **Final Ordering:**  
  Counties are ordered by the final average rank (in ascending order) to highlight those with the highest overall tier.

Below is the SQL query that accomplishes these steps:

```sql
CREATE TABLE tayloro.colorado_final_county_tier_rank AS
WITH combined_ranks AS (
    SELECT
        COALESCE(c.county_name, i.county, p.county) AS county,
        COALESCE(c.overall_crime_tier, 0) AS crime_rank,
        COALESCE(i.income_tier, 0) AS income_rank,
        COALESCE(p.crime_per_capita_tier, 0) AS population_rank
    FROM academy.tayloro.colorado_crime_tier_county_rank c
    FULL OUTER JOIN academy.tayloro.colorado_income_household_tier_rank i
        ON LOWER(c.county_name) = LOWER(i.county)
    FULL OUTER JOIN academy.tayloro.colorado_population_crime_per_capita_rank p
        ON LOWER(c.county_name) = LOWER(p.county)
),
average_rank AS (
    SELECT
        county,
        crime_rank,
        income_rank,
        population_rank,
        ROUND(
            (crime_rank + income_rank + population_rank) /
            NULLIF((CASE WHEN crime_rank > 0 THEN 1 ELSE 0 END
                  + CASE WHEN income_rank > 0 THEN 1 ELSE 0 END
                  + CASE WHEN population_rank > 0 THEN 1 ELSE 0 END), 0)
        ) AS final_rank -- Calculate the average rank and round to the nearest whole number
    FROM combined_ranks
    WHERE crime_rank > 0 AND income_rank > 0 AND population_rank > 0 -- Ensure all ranks are present
)
SELECT * FROM average_rank
ORDER BY final_rank ASC; -- Order by the final average rank
```

<img width="1032" alt="Screenshot 2025-02-28 at 9 21 32 PM" src="https://github.com/user-attachments/assets/3b42d140-7104-4218-9634-de3663caea83" />
</details>

<details>
<summary id="backfill-business-entities"><strong>Step 5: Generate Business Entities Table with Final Ranking</strong></summary>
<br/>
In this final step, we join the final county tier rank table with the business entities table. This allows us to backfill all business entities with their corresponding ranking information based on county. The query uses a `LEFT JOIN` to ensure that every business entity is retained, matching on a case-insensitive comparison of county names.

```sql
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
```

<img width="1122" alt="Screenshot 2025-02-28 at 9 39 08 PM" src="https://github.com/user-attachments/assets/75eb5f91-baa6-4e74-adc3-7a64d3c16595" />
</details>

![B A S E  Future State Diagram (5)](https://github.com/user-attachments/assets/d665e2b9-ae7f-4bd7-85f0-6c5f5fedd3b6)

## KPI and Use Case Visualizations

### Colorado County Dashboard
<img width="1575" alt="Screenshot 2025-02-28 at 5 07 06 PM" src="https://github.com/user-attachments/assets/1b246758-a1ab-4e9b-a1f5-824fa8b3e81c" />

### Colorado City Dashboard
<img width="1516" alt="Screenshot 2025-02-28 at 5 05 24 PM" src="https://github.com/user-attachments/assets/fd79e5a5-904a-4bcb-9d06-9c76d78383e4" />

### Colorado Crime Density Dashboard

<img width="1412" alt="Screenshot 2025-02-23 at 9 33 19 PM" src="https://github.com/user-attachments/assets/504f4e15-24c7-4ddd-bdb4-fddab9cbb16b" />


### Business Entity Data Pipeline

#### Business Entity Daily DAG

This DAG calls out to the public Colorado Information Marketplace API every day at 6 AM MT and fetches yesterday's Colorado Business Entity data for processing. It then cleans the data, joins matching County data based on Zip and City and then assigns an appropriate subsidy tier. This DAG involves several tasks to ingest, clean, validate, enrich, and load data from the data.colorado.gov API into production tables. Below is a breakdown of each task and its purpose.

---

#### Task Descriptions

1. **create_business_entities_yesterday_stage**  
   *Description:* Creates a staging table for business entities for yesterday’s data. This table is partitioned by `principalcounty` and sets up the structure to temporarily hold the data for further processing.

2. **create_misfit_business_entities_table**  
   *Description:* Creates a table for misfit business entities that do not meet the expected data patterns. This helps segregate records that require special attention or further review.

3. **create_dupes_staging_table**  
   *Description:* Creates a staging table for capturing potential duplicate records from the business entities data. It mirrors the structure of the main table and is partitioned by `principalcounty`.

4. **create_duplicates_table**  
   *Description:* Creates a permanent table to store confirmed duplicate records from the business entities dataset.

5. **create_geo_validation_staging_table**  
   *Description:* Creates a temporary table to hold the results of geo-validated address data. This table is partitioned by `geo_county` and is used to store responses from the Geoapify API.

6. **create_addded_city_county_zip_staging_table**  
   *Description:* Creates a staging table to store newly discovered city/county/zip combinations from geo-validation. This table is later used to update the reference data for address mapping.

7. **fetch_and_insert_business_entities_yesterday**  
   *Description:* Fetches business entity data for yesterday from the data.colorado.gov API and inserts the records into the staging table. This task calls the API, transforms the response into records, and executes an INSERT query.

8. **filter_invalid_addresses**  
   *Description:* Deletes records from the staging table that have NULL values in critical address fields (`principalcity` or `principalzipcode`), ensuring only valid addresses proceed to the next steps.

9. **reformat_zip_code**  
   *Description:* Reformats the `principalzipcode` field in the staging table to a standard 5-digit format, ensuring consistency for downstream processing.

10. **update_county_on_staging_table**  
    *Description:* Updates the `principalcounty` field in the staging table by joining with the cleaned reference table (`colorado_city_county_zip`). The matching is done using normalized (trimmed and lowercased) city names and ZIP codes.

11. **process_unmatched_entities_task**  
    *Description:* Processes records from the staging table where `principalcounty` is still NULL. It calls the Geoapify API to fetch validated address data for these unmatched entities, transforms the API responses, and inserts the results into the geo validation staging table.

12. **check_validated_address_combos**  
    *Description:* Checks validated address combinations by joining the geo validation staging table with the staging table. This task identifies new city/county/zip combinations not present in the reference table.

13. **insert_validated_address_combos**  
    *Description:* Inserts the new, validated city/county/zip combinations into the reference table (`colorado_city_county_zip`). This enhances the mapping accuracy for future data processing.

14. **override_from_validated_addresses**  
    *Description:* Updates the main business entities table by overriding existing address fields with geo-validated data. This ensures that the most accurate and standardized addresses are stored.

15. **retry_update_county_on_staging_table**  
    *Description:* Reattempts the update of the `principalcounty` field on the staging table for any records that remain unmatched after initial processing.

16. **identify_duplicates_task**  
    *Description:* Identifies duplicate records within the staging table by comparing key address and entity fields. The duplicates are then inserted into the duplicates staging table.

17. **run_dq_check_null_values**  
    *Description:* Runs a data quality check that counts records with NULL values in critical fields (`entityname`, `principalcity`, `entitytype`, `entityformdate`) to ensure data completeness.

18. **run_dq_check_principalstate**  
    *Description:* Executes a data quality check to verify that all records in the staging table have `principalstate` equal to 'CO'.

19. **run_dq_check_entityformdate**  
    *Description:* Performs a data quality check to ensure that the `entityformdate` in the staging table matches the expected date (yesterday’s date).

20. **run_dq_check_no_unexpected_duplicates**  
    *Description:* Checks for any unexpected duplicates in the staging table by grouping on `entityid` and counting occurrences greater than one.

21. **insert_new_records_task**  
    *Description:* Inserts new business entity records from the staging table into the production table, excluding any records that have been flagged as duplicates and ensuring that `principalcounty` is populated.

22. **insert_business_entities_with_ranking**  
    *Description:* Inserts business entity records along with their associated ranking information by joining the staging table with the final county tier rank table.

23. **insert_duplicates**  
    *Description:* Inserts duplicate records from the duplicates staging table into the permanent duplicates table for further review or archival.

24. **insert_misfits**  
    *Description:* Inserts misfit records (those without a valid `principalcounty`) from the staging table into the misfits table, isolating records that could not be processed normally.

25. **drop_staging_table_task**  
    *Description:* Drops the staging table to clean up temporary data once processing is complete.

26. **drop_dupes_staging_table_task**  
    *Description:* Drops the duplicates staging table to remove temporary duplicate data after it has been archived.

27. **drop_geo_validation_staging_table**  
    *Description:* Drops the geo validation staging table that was used for holding geo API responses once the data has been processed and merged.

28. **drop_added_city_county_zip_staging_table**  
    *Description:* Drops the staging table for newly added city/county/zip combinations, finalizing the cleanup of temporary tables.

---

#### DAG Flow Order

The tasks order looks like this:

<img width="543" alt="Screenshot 2025-02-28 at 10 29 57 PM" src="https://github.com/user-attachments/assets/fd136dc9-b948-41db-ba49-b3230e2c338e" />

#### Astronomer Cloud Deployment

One of the core requirements from Zach for the capstone project is that it must be running in a production cloud environment to count. Below are snapshots of my DAG in the Astronomer production environment running daily.

<img width="870" alt="Screenshot 2025-02-28 at 6 23 12 AM" src="https://github.com/user-attachments/assets/73aeb31d-b6fc-416d-80ef-6f64d3fbb91b" />


<img width="1011" alt="Screenshot 2025-02-28 at 6 22 22 AM" src="https://github.com/user-attachments/assets/b2785011-3f07-43fd-8588-a07875926614" />

### Putting It All Together

#### Business Entity Search Dashboard
<br/>
The dashboard below enables a business owner to navigate to this dashboard, search for their business entity and see information about their business, information on the available security system subsidy tiers and exactly which tier their business qualifies for. 
<br/>
<img width="1412" alt="Screenshot 2025-02-28 at 5 08 32 PM" src="https://github.com/user-attachments/assets/3da8052f-2a73-4a00-aaa3-1314f4ea01b1" />

### Challenges and Findings

<details>
<summary id="business-entity-data-cleaning"><strong>In-depth Summary of Business Entities Data Cleaning and Update Process</strong></summary>

**Challenges Faced:**  
- **Mismatches in Data:** A significant number of records in the `colorado_business_entities` table did not have corresponding county information when joined with the reference table (`colorado_city_county_zip`).  
- **Inconsistent Data Formatting:** Issues such as typos, case discrepancies, and extra spaces in city names and ZIP codes led to failed matches.  
- **Incomplete Reference Data:** Missing city/ZIP combinations and duplicate entries in the reference table required both augmentation and deduplication to ensure accurate mapping.

**Process Overview:**

1. **Initial Analysis and Identification of Issues**  
   - Ran queries to identify records in `colorado_business_entities` with no matching county in the reference table.  
   - Examined unmatched city/ZIP pairs to determine the most frequent discrepancies.

2. **Data Correction in the Business Entities Table**  
   - Performed manual corrections for known typos and inconsistencies (e.g., updating “peyton co” to “Peyton”, correcting ZIP 80524 entries to “Fort Collins”).  
   - Verified corrections by running SELECT queries against both the business entities and reference datasets.

3. **Assessing Match Quality**  
   - Executed queries to compare the count of matched versus unmatched records, revealing a high number of mismatches that required further attention.

4. **Inserting Missing Reference Data**  
   - Augmented the reference table by inserting missing city/ZIP combinations (e.g., added entries for “Evans” in Weld County).  
   - Inspected the reference table to ensure that the inserted data met quality standards.

5. **Deduplicating the Reference Table**  
   - Identified duplicate rows for certain city/ZIP pairs (such as “Burlington, 80807” and “New Raymer, 80742”).  
   - Created a new deduplicated version of the reference table and replaced the original with the cleaned version.

6. **Updating the Business Entities with County Data**  
   - Employed a final update query (using a correlated subquery) to backfill the `principalcounty` field in `colorado_business_entities` by matching normalized city and ZIP code data from the cleaned reference table.  
   - Verified the update by confirming that previously unmatched records were now successfully mapped.

7. **Final Outcome**  
   - **Normalized Addresses:** Data cleaning (trimming, lowercasing, casting ZIP codes) ensured consistent comparisons.  
   - **Accurate Mapping:** Manual corrections and enhanced reference data led to a complete and unique set of city/ZIP combinations.  
   - **Successful Update:** The final update query backfilled the `principalcounty` field for all records, reducing unmatched counts to zero.

</details>

<details>
<summary id="out-of-memory-spark"><strong>Out-of-Memory (OOM) Exceptions and Spark Configuration Adjustments</strong></summary>

When processing large datasets (over a million rows) using Spark, I frequently encountered out-of-memory exceptions—especially when fetching large CSV files from S3 and writing to Iceberg. The default Spark settings were insufficient for these workloads.

To address this challenge, I reviewed the Spark documentation and applied recommendations from the bootcamp. I adjusted the Spark configuration to allocate more memory to both the executors and the driver, and to optimize shuffle operations. The following settings helped mitigate the memory issues:

```python
conf.set('spark.executor.memory', '16g')         # Increase executor memory based on your system's capacity
conf.set('spark.sql.shuffle.partitions', '200')    # Optimize the number of shuffle partitions
conf.set('spark.driver.memory', '16g')             # Increase driver memory to handle larger workloads
```
By increasing the memory allocation and tuning the shuffle partitions, I was able to process the large CSV files efficiently and write to Iceberg without running into out-of-memory errors.
</details>

## Closing Thoughts and Next Steps
