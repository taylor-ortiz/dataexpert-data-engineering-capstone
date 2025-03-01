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
4. [Business Entity Search Dashboard](#business-entity-search-dashboard)
5. [Colorado County Dashboard](#colorado-county-dashboard)
6. [Colorado City Dashboard](#colorado-city-dashboard)
7. [Colorado Crime Density Dashboard](#colorado-crime-density-dashboard)
8. [Business Entity Data Pipeline](#business-entity-data-pipeline)
9. [Challenges and Findings](#challenges-and-findings)
10. [Closing Thoughts and Next Steps](#closing-thoughts-and-next-steps)


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

Tiers are represented as a range of 1 through 4 in the B.A.S.E. program. Each subsidy tier below offers a gradual increase of security system services based on the tier that your business qualifies for. The idea is that tiers will be backfilled and assigned on the source business entities dataset and as new business entities are added daily to the Colorado Information Marketplace, those records will also be assigned a tier through the Airflow orchestration that we will cover below. However, how are tiers actually calculated and assigned? Read on!

<img width="1406" alt="Screenshot 2025-02-28 at 4 44 20 PM" src="https://github.com/user-attachments/assets/46d1a897-69bd-4d81-a2b7-61c1001996ef" />

### How Tiers Are Calculated and Assigned

#### Step 1: Identify Criteria

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

#### Step 2: Establish Individual Rankings

For each metric, we transform raw data into a standardized ranking by following a similar process:

##### Crime Categories
For each of the 7 selected crime categories:
- **Aggregate Data:**  
  Count the number of incidents per county.
- **Compute Percentile Ranks:**  
  Use a percentile function (e.g., `PERCENT_RANK()`) to determine each county’s standing relative to others.
- **Assign Tiers:**  
  Based on the percentile (for example, dividing into quartiles), assign a tier (usually 1–4) for that specific crime.

##### Population-Adjusted Crime (Crime Per Capita)
We adjust raw crime counts by county population to calculate the crime rate per 1,000 residents. This involves:
- **Calculating Yearly Crime Rates:**  
  Join crime data with population figures.
- **Averaging:**  
  Compute an average crime rate per county over the selected years.
- **Ranking:**  
  Determine percentile ranks and assign tiers similarly to the individual crime categories.

##### Income
For median household income, we:
- **Aggregate Income Data:**  
  Average the income per county across the years.
- **Compute Percentile Ranks:**  
  Establish how each county compares to others.
- **Assign Tiers:**  
  Counties in the highest percentiles (top 25%) receive one tier, and so on for the others.




![B A S E  Future State Diagram (5)](https://github.com/user-attachments/assets/d665e2b9-ae7f-4bd7-85f0-6c5f5fedd3b6)


<img width="1123" alt="Screenshot 2025-02-28 at 5 24 50 PM" src="https://github.com/user-attachments/assets/b81e41d2-5fc1-4cf4-8055-36df7663a537" />

## Business Entity Search Dashboard
<img width="1412" alt="Screenshot 2025-02-28 at 5 08 32 PM" src="https://github.com/user-attachments/assets/3da8052f-2a73-4a00-aaa3-1314f4ea01b1" />

## Colorado County Dashboard
<img width="1575" alt="Screenshot 2025-02-28 at 5 07 06 PM" src="https://github.com/user-attachments/assets/1b246758-a1ab-4e9b-a1f5-824fa8b3e81c" />

## Colorado City Dashboard
<img width="1516" alt="Screenshot 2025-02-28 at 5 05 24 PM" src="https://github.com/user-attachments/assets/fd79e5a5-904a-4bcb-9d06-9c76d78383e4" />

## Colorado Crime Density Dashboard

<img width="1412" alt="Screenshot 2025-02-23 at 9 33 19 PM" src="https://github.com/user-attachments/assets/504f4e15-24c7-4ddd-bdb4-fddab9cbb16b" />


## Business Entity Data Pipeline

## Challenges and Findings

## Closing Thoughts and Next Steps
