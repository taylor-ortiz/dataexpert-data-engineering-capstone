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

* Grafana dashboards displaying fifteen required KPIs 
* 27 task DAG using Airflow data pipeline orchestration running in Astronomer production
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
   3. [Datasets Used](#datasets-used)


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

### Datasets Used

- [Crimes in Colorado (2016-2020)](https://data.colorado.gov/Public-Safety/Crimes-in-Colorado/j6g4-gayk/about_data): Offenses in Colorado for 2016 through 2020 by Agency from the FBI's Crime Data Explorer.
- [Crimes in Colorado (1997-2015)](https://data.colorado.gov/Public-Safety/Crimes-in-Colorado-1997-to-2015/6vnq-az4b/about_data): Crime stats for the State of Colorado from 1997 to 2015. Data provided by the CDPS and the FBI's Crime Data Explorer (CDE).



