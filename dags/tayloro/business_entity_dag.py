from airflow.decorators import dag
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import datetime, timedelta
from include.eczachly.trino_queries import execute_trino_query, run_trino_query_dq_check
from airflow.models import Variable
import os
import requests
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()  # This loads the .env file so that os.getenv() works

local_script_path = os.path.join("include", 'eczachly/scripts/kafka_read_example.py')

tabular_credential = Variable.get("TABULAR_CREDENTIAL")

DATA_COLORADO_GOV_KEY = Variable.get("DATA_COLORADO_GOV_KEY")
GEOAPIFY_KEY = Variable.get("GEOAPIFY_KEY")


@dag(
    description="A DAG that processes Colorado business entity data daily at 5 AM MT.",
    default_args={
        "owner": "Taylor Ortiz",
        "retries": 0,
        "execution_timeout": timedelta(hours=1),
    },
    start_date=datetime(2025, 2, 23),  # Ensure this is set correctly
    max_active_runs=1,  # Ensures only one active DAG run at a time
    schedule_interval="0 13 * * *",  # Runs at 12 PM UTC = 6 AM MT
    catchup=False,  # Prevents running past dates
    tags=["tayloro", "capstone"]
)

def business_entity_dag():
    yesterday = '{{ yesterday_ds }}'
    schema = 'tayloro'
    production_table = f'{schema}.colorado_business_entities'

    # Correctly reference Airflow macros
    business_entities_with_ranking = f"{production_table}_with_ranking"
    final_county_tier_rank = f'{schema}.colorado_final_county_tier_rank'
    staging_table = f"{production_table}_stg_{{{{ yesterday_ds_nodash }}}}"
    dupes_staging_table = f"{production_table}_dupes_stg_{{{{ yesterday_ds_nodash }}}}"
    dupes_table = f"{production_table}_duplicates"
    geo_validation_staging_table = f"{production_table}_geo_validation_stg_{{{{ yesterday_ds_nodash }}}}"
    misfit_entities_table = f"{production_table}_misfit_entities"
    added_city_county_zip = f"{production_table}_added_city_count_zip_stg_{{{{ yesterday_ds_nodash }}}}"

    geoapify_key = GEOAPIFY_KEY
    data_colorado_gov_api_key = DATA_COLORADO_GOV_KEY


    # Function to transform API data into records
    def transform_entity_data_to_records(response):
        records = []
        for result in response:
            # Extract data while defaulting to None for missing fields
            records.append({
                "entityid": result.get("entityid"),
                "entityname": result.get("entityname"),
                "principaladdress1": result.get("principaladdress1"),
                "principaladdress2": result.get("principaladdress2"),
                "principalcity": result.get("principalcity"),
                "principalstate": result.get("principalstate"),
                "principalzipcode": result.get("principalzipcode"),
                "principalcountry": result.get("principalcountry"),
                "entitystatus": result.get("entitystatus"),
                "jurisdictonofformation": result.get("jurisdictonofformation"),
                "entitytype": result.get("entitytype"),
                "entityformdate": result.get("entityformdate")
            })
        return records

    # Function to fetch business entity data for yesterday from the data.colorado.gov API
    def fetch_business_entity_data(yesterday, api_token):
        print('what is the value of the token? ', api_token)
        print('Grabbing Colorado Business Entity records for ', yesterday)
        formatted_date = f"{yesterday}T00:00:00.000"
        base_url = f"https://data.colorado.gov/resource/4ykn-tg5h.json?$$app_token={api_token}"
        params = {
            "entityformdate": formatted_date,
            "entitystatus": "Good Standing",
            "principalstate": "CO",
        }
        query_string = "&".join(f"{key}={value.replace(' ', '%20')}" for key, value in params.items())
        # Append with an ampersand (&) instead of a question mark
        final_url = f"{base_url}&{query_string}"
        print('what is final url? ', final_url)
        response = requests.get(final_url)

        if response.status_code == 200 and len(response.json()) > 0:
            records = transform_entity_data_to_records(response.json())
            print("Fetched Records:", records)
            return records
        else:
            print(f"No data fetched for {yesterday}")
            return []
    def insert_into_staging_table(records, staging_table):
        if not records:
            print("No records to insert.")
            return

        # Build the insert query
        insert_query = f"""
        INSERT INTO {staging_table} (
            entityid, entityname, principaladdress1, principaladdress2,
            principalcity, principalcounty, principalstate, principalzipcode,
            principalcountry, entitystatus, jurisdictonofformation, entitytype, entityformdate
        ) VALUES
        """
        values = []
        for record in records:
            # Escape single quotes in strings
            entityname = record['entityname'].replace("'", "''")
            principaladdress1 = record['principaladdress1'].replace("'", "''")
            principaladdress2 = (record.get('principaladdress2') or '').replace("'", "''")
            principalcity = record['principalcity'].replace("'", "''")
            principalstate = record['principalstate']
            principalzipcode = record['principalzipcode']
            principalcountry = record['principalcountry'].replace("'", "''")
            entitystatus = record['entitystatus'].replace("'", "''")
            jurisdictonofformation = record['jurisdictonofformation'].replace("'", "''")
            entitytype = record['entitytype'].replace("'", "''")

            # Convert entityformdate to proper DATE format
            raw_date = record['entityformdate']
            try:
                # Parse and format the date as YYYY-MM-DD
                formatted_date = datetime.strptime(raw_date[:10], '%Y-%m-%d').strftime('%Y-%m-%d')
            except ValueError:
                print(f"Invalid date format: {raw_date}")
                continue

            # Format each record as a SQL value
            values.append(
                f"({record['entityid']}, '{entityname}', '{principaladdress1}', '{principaladdress2}', "
                f"'{principalcity}', NULL, '{principalstate}', '{principalzipcode}', "
                f"'{principalcountry}', '{entitystatus}', '{jurisdictonofformation}', '{entitytype}', "
                f"DATE '{formatted_date}')"
            )
        insert_query += ", ".join(values)

        try:
            execute_trino_query(query=insert_query)
            print(f"Successfully inserted {len(records)} records into {staging_table}")
        except Exception as e:
            print(f"Failed to insert records: {e}")

    # Fetch and insert data
    def fetch_and_insert_data(yesterday, staging_table, api_token):
        print('True')
        records = fetch_business_entity_data(yesterday, api_token)
        insert_into_staging_table(records, staging_table)

    def transform_geocode_response(response_json, entityid):
        """
        Parses the Geoapify API JSON response (which is already a list of features)
        and extracts the desired fields.

        Returns a list of tuples with the following structure:
          (entityid, geo_city, geo_county, geo_state, geo_postcode, formatted_address)
        """
        records = []
        print('what is the value of response_json? ', response_json)
        # Loop directly over the list of feature dictionaries
        for feature in response_json:
            # Ensure feature is a dict before processing
            if not isinstance(feature, dict):
                continue
            props = feature.get("properties", {})
            # Extract desired fields from properties
            geo_city = props.get("city")
            geo_county = props.get("county")
            geo_state = props.get("state")
            geo_postcode = props.get("postcode")
            formatted_address = props.get("formatted")
            # Append a tuple including the entityid for later reference
            records.append((entityid, geo_city, geo_county, geo_state, geo_postcode, formatted_address))
        return records

    def process_and_insert_address_data(aggregated_records, geo_table):
        """
        Receives an aggregated list of tuples where each tuple is:
          (entityid, geo_city, geo_county, geo_state, geo_postcode, formatted_address)
        Builds and executes an INSERT query to load these records into the temporary geo table.
        Assumes that the first column (entityid) is numeric (BIGINT) and the rest are strings.
        Applies a final check on the zip code (geo_postcode) to ensure it is either a valid 5-digit string
        or is blank.
        """
        if not aggregated_records:
            print("No records to insert from API responses.")
            return

        values_list = []
        for rec in aggregated_records:
            rec_values = []
            for idx, field in enumerate(rec):
                # Final check for the zip code (geo_postcode, index 4)
                if idx == 4:
                    if field is not None:
                        field = str(field).strip()
                        if len(field) >= 5:
                            candidate = field[:5]
                            # Use candidate if it's all digits; otherwise blank it out.
                            if candidate.isdigit():
                                field = candidate
                            else:
                                field = ''
                        else:
                            field = ''
                # For the first field (entityid), assume it's numeric and do not quote.
                if idx == 0:
                    rec_values.append(str(field))
                else:
                    if field is None:
                        rec_values.append("NULL")
                    else:
                        rec_values.append("'" + str(field).replace("'", "''") + "'")
            values_list.append("(" + ", ".join(rec_values) + ")")
        values_clause = ", ".join(values_list)

        insert_query = f"""
            INSERT INTO {geo_table}
            (entityid, geo_city, geo_county, geo_state, geo_postcode, formatted_address)
            VALUES {values_clause}
        """
        execute_trino_query(query=insert_query)
        print(f"Inserted {len(aggregated_records)} records into {geo_table}.")


    def fetch_validated_address(addr, city, state, zip, api_key):
        base_url = "https://api.geoapify.com/v1/geocode/search"
        url = f"{base_url}?text={addr}, {city} {state} {zip}&apiKey={api_key}"

        response = requests.get(url)
        print(f"API call to {url} returned status: {response.status_code}")

        if response.status_code != 200:
            print("API call failed with status code", response.status_code)
            return None

        data = response.json()
        features = data.get("features", [])

        if not features:
            print("No features returned in API response.")
            return None

        validated_features = []
        for feature in features:
            props = feature.get("properties", {})
            # The rank object lives inside the properties
            rank = props.get("rank", {})
            match_type = rank.get("match_type")
            confidence = rank.get("confidence")
            confidence_city = rank.get("confidence_city_level")
            confidence_street = rank.get("confidence_street_level")
            city = props.get("city")
            county = props.get("county")
            zip = props.get("postcode")

            # Check each feature's rank details
            if (match_type == "full_match"
                    and confidence == 1
                    and confidence_city == 1
                    and confidence_street == 1
                    and city
                    and county
                    and zip):
                validated_features.append(feature)

        if validated_features:
            # If at least one validated feature is found, you can return them.
            # For example, you could choose the first one or return all validated features.
            print(f"Found {len(validated_features)} validated feature(s).")
            return validated_features  # or, for example: return validated_features[0]
        else:
            print("No feature meets the validation criteria.")
            return None


    def process_unmatched_entities_task(staging, geo_table, api_key):
        """
        1. Queries the staging table for records where principalcounty is NULL.
        2. For each record, constructs the full address and calls the address API.
        3. Aggregates the API responses while retaining the entityid.
        4. Inserts the aggregated geocoded data into the temporary geo table.
        """
        # Modify the query to select entityid, principaladdress1, and principalstate.
        query = f"""
            SELECT entityid, principaladdress1, principalcity, principalstate, principalzipcode
            FROM {staging}
            WHERE principalstate = 'CO'
              AND principalcounty IS NULL
        """
        results = execute_trino_query(query=query)
        print("Unmatched records fetched:", results)

        aggregated_records = []
        for row in results:
            entityid = row[0]
            address = row[1]
            city = row[2]
            state = row[3]
            zip = row[4]

            # Call the address validation API
            api_response = fetch_validated_address(address, city, state, zip, api_key)
            if api_response:
                # Transform the API response; include the entityid with each record
                recs = transform_geocode_response(api_response, entityid)
                aggregated_records.extend(recs)

        # Insert the aggregated geocoded records into the temporary geo table
        process_and_insert_address_data(aggregated_records, geo_table)

    def check_validated_address_combos(staging, geo_table, added_city_county_zip):
        """
        Queries the validated address combinations by joining the temporary geo validation table
        with the staging table based on entityid. It returns distinct combinations of:
          (zip_code, city, county, entityid)
        that are not already present in the reference table (colorado_city_county_zip).

        Parameters:
          staging (str): Fully resolved name of the staging table.
          geo_table (str): Fully resolved name of the temporary geo validation table.

        Returns:
          None. The function prints the query results.
        """
        query = f"""
            SELECT DISTINCT
                CAST(g.geo_postcode AS INTEGER) AS zip_code,
                g.geo_city AS city,
                replace(g.geo_county, ' County', '') AS county,
                s.entityid
            FROM {geo_table} g
            JOIN {staging} s
              ON g.entityid = s.entityid
            WHERE s.principalstate = 'CO'
              AND NOT EXISTS (
                  SELECT 1
                  FROM tayloro.colorado_city_county_zip r
                  WHERE TRIM(LOWER(r.city)) = TRIM(LOWER(g.geo_city))
                    AND TRIM(CAST(r.zip_code AS VARCHAR)) = TRIM(g.geo_postcode)
              )
        """
        results = execute_trino_query(query=query)

        if not results:
            print("No validated address combinations to insert.")
            return

        # Build the INSERT query using zip_code, city, county (ignoring entityid)
        values_list = []
        for row in results:
            # row structure: [zip_code, city, county, entityid]
            zip_code = str(row[0])  # Numeric, so no quotes
            city = "'" + str(row[1]).replace("'", "''") + "'"
            county = "'" + str(row[2]).replace("'", "''").replace(" County", "") + "'"
            values_list.append(f"({zip_code}, {city}, {county})")

        values_clause = ", ".join(values_list)
        insert_query = f"""
            INSERT INTO {added_city_county_zip} (zip_code, city, county)
            VALUES {values_clause}
        """
        execute_trino_query(query=insert_query)
        print(f"Inserted {len(results)} validated address combinations into {added_city_county_zip}.")


    create_business_entities_yesterday_stage = PythonOperator(
        task_id="create_business_entities_yesterday_stage",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                CREATE TABLE IF NOT EXISTS {staging_table} (
                    entityid BIGINT,
                    entityname VARCHAR,
                    principaladdress1 VARCHAR,
                    principaladdress2 VARCHAR,
                    principalcity VARCHAR,
                    principalcounty VARCHAR, -- Mapped county from cities dataset
                    principalstate VARCHAR,
                    principalzipcode VARCHAR,
                    principalcountry VARCHAR,
                    entitystatus VARCHAR,
                    jurisdictonofformation VARCHAR,
                    entitytype VARCHAR,
                    entityformdate DATE -- Ensure stored as DATE type
                )
                WITH (
                    partitioning = ARRAY['principalcounty'] -- Partition by principalcounty
                )
            """
        }
    )

    create_misfit_business_entities_table = PythonOperator(
        task_id="create_misfit_business_entities_table",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                CREATE TABLE IF NOT EXISTS {misfit_entities_table} (
                    entityid BIGINT,
                    entityname VARCHAR,
                    principaladdress1 VARCHAR,
                    principaladdress2 VARCHAR,
                    principalcity VARCHAR,
                    principalcounty VARCHAR, -- Mapped county from cities dataset
                    principalstate VARCHAR,
                    principalzipcode VARCHAR,
                    principalcountry VARCHAR,
                    entitystatus VARCHAR,
                    jurisdictonofformation VARCHAR,
                    entitytype VARCHAR,
                    entityformdate DATE -- Ensure stored as DATE type
                )
            """
        }
    )

    create_duplicates_table = PythonOperator(
        task_id="create_duplicates_table",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                CREATE TABLE IF NOT EXISTS {dupes_table} (
                    entityid BIGINT,
                    entityname VARCHAR,
                    principaladdress1 VARCHAR,
                    principaladdress2 VARCHAR,
                    principalcity VARCHAR,
                    principalcounty VARCHAR, -- Mapped county from cities dataset
                    principalstate VARCHAR,
                    principalzipcode VARCHAR,
                    principalcountry VARCHAR,
                    entitystatus VARCHAR,
                    jurisdictonofformation VARCHAR,
                    entitytype VARCHAR,
                    entityformdate DATE -- Ensure stored as DATE type
                )
            """
        }
    )

    create_dupes_staging_table = PythonOperator(
        task_id="create_dupes_staging_table",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                CREATE TABLE IF NOT EXISTS {dupes_staging_table} (
                    entityid BIGINT,
                    entityname VARCHAR,
                    principaladdress1 VARCHAR,
                    principaladdress2 VARCHAR,
                    principalcity VARCHAR,
                    principalcounty VARCHAR, -- Mapped county from cities dataset
                    principalstate VARCHAR,
                    principalzipcode VARCHAR,
                    principalcountry VARCHAR,
                    entitystatus VARCHAR,
                    jurisdictonofformation VARCHAR,
                    entitytype VARCHAR,
                    entityformdate DATE -- Ensure stored as DATE type
                )
                WITH (
                    partitioning = ARRAY['principalcounty'] -- Partition by principalcounty
                )
            """
        }
    )


    create_geo_validation_staging_table = PythonOperator(
        task_id="create_geo_validation_staging_table",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                CREATE TABLE IF NOT EXISTS {geo_validation_staging_table} (
                    entityid BIGINT,
                    geo_city VARCHAR,
                    geo_county VARCHAR,
                    geo_state VARCHAR,
                    geo_postcode VARCHAR,
                    formatted_address VARCHAR
                )
                WITH (
                    partitioning = ARRAY['geo_county']
                )
            """
        }
    )

    create_addded_city_county_zip_staging_table = PythonOperator(
        task_id="create_addded_city_county_zip_staging_table",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                CREATE TABLE IF NOT EXISTS {added_city_county_zip} (
                    zip_code INT,
                    city VARCHAR,
                    county VARCHAR
                )
            """
        }
    )

    fetch_and_insert_business_entities_yesterday = PythonOperator(
        task_id="fetch_and_insert_business_entities_yesterday",
        python_callable=fetch_and_insert_data,
        op_kwargs={
            'yesterday': yesterday,
            'staging_table': staging_table,
            'api_token': data_colorado_gov_api_key
        }
    )


    filter_invalid_addresses = PythonOperator(
        task_id="filter_invalid_addresses",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
            DELETE FROM {staging_table}
            WHERE principalcity IS NULL OR principalzipcode IS NULL
            """
        }
    )

    reformat_zip_code = PythonOperator(
        task_id="reformat_zip_code",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                UPDATE {staging_table}
                SET principalzipcode = 
                    CASE 
                        WHEN LENGTH(TRIM(principalzipcode)) >= 5 
                             AND TRY_CAST(SUBSTR(TRIM(principalzipcode), 1, 5) AS INTEGER) IS NOT NULL 
                        THEN SUBSTR(TRIM(principalzipcode), 1, 5)
                        ELSE ''
                    END
                WHERE principalstate = 'CO'
            """
        }
    )



    update_county_on_staging_table = PythonOperator(
        task_id="update_county_on_staging_table",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                UPDATE {staging_table}
                SET principalcounty = (
                    SELECT ccz.county
                    FROM tayloro.colorado_city_county_zip ccz
                    WHERE TRIM(LOWER({staging_table}.principalcity)) = TRIM(LOWER(ccz.city))
                      AND TRIM(CAST({staging_table}.principalzipcode AS VARCHAR)) = TRIM(CAST(ccz.zip_code AS VARCHAR))
                )
                WHERE principalstate = 'CO'
            """
        }
    )

    process_unmatched_entities_task = PythonOperator(
        task_id="process_unmatched_entities_task",
        python_callable=process_unmatched_entities_task,
        op_kwargs={
            'staging': staging_table,
            'geo_table': geo_validation_staging_table,
            'api_key': geoapify_key
        }
    )

    check_validated_address_combos_task = PythonOperator(
        task_id="check_validated_address_combos",
        python_callable=check_validated_address_combos,
        op_kwargs={
            "staging": staging_table,           # resolved staging table name
            "geo_table": geo_validation_staging_table,  # resolved temporary geo validation table name
            "added_city_county_zip": added_city_county_zip
        }
    )

    insert_validated_address_combos_task = PythonOperator(
        task_id="insert_validated_address_combos",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                INSERT INTO tayloro.colorado_city_county_zip (zip_code, city, county)
                SELECT a.zip_code, a.city, a.county
                FROM {added_city_county_zip} a
                WHERE a.zip_code IS NOT NULL
                  AND a.city IS NOT NULL
                  AND a.county IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1
                      FROM tayloro.colorado_city_county_zip r
                      WHERE r.zip_code = a.zip_code
                        AND TRIM(LOWER(r.city)) = TRIM(LOWER(a.city))
                        AND TRIM(LOWER(r.county)) = TRIM(LOWER(a.county))
                  )
            """
        }
    )


    override_from_validated_addresses = PythonOperator(
        task_id="override_from_validated_addresses",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                UPDATE tayloro.colorado_business_entities
                SET
                    principalcity = (
                        SELECT geo_city
                        FROM tayloro.colorado_temp_geocoded_entities
                        WHERE entityid = tayloro.colorado_business_entities.entityid
                    ),
                    principalzipcode = (
                        SELECT geo_postcode
                        FROM tayloro.colorado_temp_geocoded_entities
                        WHERE entityid = tayloro.colorado_business_entities.entityid
                    ),
                    principalcounty = (
                        SELECT geo_county
                        FROM tayloro.colorado_temp_geocoded_entities
                        WHERE entityid = tayloro.colorado_business_entities.entityid
                    )
                WHERE entityid IN (
                    SELECT entityid
                    FROM tayloro.colorado_temp_geocoded_entities
                )
                AND (
                    principalcity <> (
                        SELECT geo_city
                        FROM tayloro.colorado_temp_geocoded_entities
                        WHERE entityid = tayloro.colorado_business_entities.entityid
                    )
                    OR principalzipcode <> (
                        SELECT geo_postcode
                        FROM tayloro.colorado_temp_geocoded_entities
                        WHERE entityid = tayloro.colorado_business_entities.entityid
                    )
                    OR principalcounty <> (
                        SELECT geo_county
                        FROM tayloro.colorado_temp_geocoded_entities
                        WHERE entityid = tayloro.colorado_business_entities.entityid
                    )
                )
            """
        }
    )

    retry_update_county_on_staging_table = PythonOperator(
        task_id="retry_update_county_on_staging_table",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                UPDATE {staging_table}
                SET principalcounty = (
                    SELECT ccz.county
                    FROM tayloro.colorado_city_county_zip ccz
                    WHERE TRIM(LOWER({staging_table}.principalcity)) = TRIM(LOWER(ccz.city))
                      AND TRIM(CAST({staging_table}.principalzipcode AS VARCHAR)) = TRIM(CAST(ccz.zip_code AS VARCHAR))
                )
                WHERE principalstate = 'CO'
                AND principalcounty IS NULL
            """
        }
    )

    identify_duplicates_task = PythonOperator(
        task_id="identify_duplicates_task",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                INSERT INTO {dupes_staging_table}
                SELECT DISTINCT
                    s.entityid,
                    s.entityname,
                    s.principaladdress1,
                    s.principaladdress2,
                    s.principalcity,
                    s.principalcounty,
                    s.principalstate,
                    s.principalzipcode,
                    s.principalcountry,
                    s.entitystatus,
                    s.jurisdictonofformation,
                    s.entitytype,
                    s.entityformdate
                FROM {staging_table} s
                INNER JOIN {production_table} p
                  ON s.entityname = p.entityname
                  AND s.principaladdress1 = p.principaladdress1
                  AND s.principaladdress2 = p.principaladdress2
                  AND s.principalcity = p.principalcity
                  AND s.principalcounty = p.principalcounty
                  AND s.principalstate = p.principalstate
                  AND s.principalzipcode = p.principalzipcode
                  AND s.principalcountry = p.principalcountry
                  AND s.entitystatus = p.entitystatus
                  AND s.jurisdictonofformation = p.jurisdictonofformation
                  AND s.entitytype = p.entitytype
                  AND s.entityformdate = p.entityformdate
            """
        }
    )

    run_dq_check_null_values = PythonOperator(
        task_id="run_dq_check_null_values",
        python_callable=run_trino_query_dq_check,
        op_kwargs={
            'query': f"""
                SELECT COUNT(*)
                FROM {staging_table}
                WHERE entityname IS NULL
                   OR principalcity IS NULL
                   OR entitytype IS NULL
                   OR entityformdate IS NULL
            """
        }
    )

    run_dq_check_principalstate = PythonOperator(
        task_id="run_dq_check_principalstate",
        python_callable=run_trino_query_dq_check,
        op_kwargs={
            'query': f"""
                SELECT COUNT(*)
                FROM {staging_table}
                WHERE principalstate <> 'CO' OR principalstate IS NULL
            """
        }
    )

    run_dq_check_entityformdate = PythonOperator(
        task_id="run_dq_check_entityformdate",
        python_callable=run_trino_query_dq_check,
        op_kwargs={
            'query': f"""
                SELECT COUNT(*)
                FROM {staging_table}
                WHERE entityformdate <> DATE('{yesterday}') OR entityformdate IS NULL
            """
        }
    )

    run_dq_check_no_unexpected_duplicates = PythonOperator(
        task_id="run_dq_check_no_unexpected_duplicates",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                SELECT entityid, COUNT(*) AS count
                FROM {staging_table}
                WHERE entityid NOT IN (
                    SELECT entityid FROM {dupes_staging_table}
                )
                GROUP BY entityid
                HAVING COUNT(*) > 1
            """
        }
    )

    insert_new_records_task = PythonOperator(
        task_id="insert_new_records_task",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                INSERT INTO {production_table} (
                    entityid, entityname, principaladdress1, principaladdress2, principalcity,
                    principalcounty, principalstate, principalzipcode, principalcountry, entitystatus,
                    jurisdictonofformation, entitytype, entityformdate
                )
                SELECT
                    entityid, entityname, principaladdress1, principaladdress2, principalcity,
                    principalcounty, principalstate, principalzipcode, principalcountry, entitystatus,
                    jurisdictonofformation, entitytype, entityformdate
                FROM {staging_table}
                WHERE entityid NOT IN (
                    SELECT entityid FROM {dupes_staging_table}
                )
                AND principalcounty IS NOT NULL
            """
        }
    )

    insert_business_entities_with_ranking = PythonOperator(
        task_id="insert_business_entities_with_ranking",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                INSERT INTO {business_entities_with_ranking}
                SELECT
                    stg.*,
                    fr.crime_rank,
                    fr.income_rank,
                    fr.population_rank,
                    fr.final_rank
                FROM {staging_table} stg
                LEFT JOIN {final_county_tier_rank} fr
                    ON LOWER(stg.principalcounty) = LOWER(fr.county)
                WHERE stg.entityid NOT IN (
                        SELECT entityid FROM {dupes_staging_table}
                    )
                  AND stg.principalcounty IS NOT NULL
            """
        }
    )


    insert_duplicates = PythonOperator(
        task_id="insert_duplicates",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                INSERT INTO {dupes_table}
                SELECT 
                    entityid,
                    entityname,
                    principaladdress1,
                    principaladdress2,
                    principalcity,
                    principalcounty,
                    principalstate,
                    principalzipcode,
                    principalcountry,
                    entitystatus,
                    jurisdictonofformation,
                    entitytype,
                    entityformdate
                FROM {dupes_staging_table}
            """
        }
    )

    insert_misfits = PythonOperator(
        task_id="insert_misfits",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                INSERT INTO {misfit_entities_table}
                SELECT 
                    entityid,
                    entityname,
                    principaladdress1,
                    principaladdress2,
                    principalcity,
                    principalcounty,
                    principalstate,
                    principalzipcode,
                    principalcountry,
                    entitystatus,
                    jurisdictonofformation,
                    entitytype,
                    entityformdate
                FROM {staging_table}
                WHERE principalcounty IS NULL
            """
        }
    )

    drop_staging_table_task = PythonOperator(
        task_id="drop_staging_table_task",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"DROP TABLE IF EXISTS {staging_table}"
        }
    )

    drop_dupes_staging_table_task = PythonOperator(
        task_id="drop_dupes_staging_table_task",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"DROP TABLE IF EXISTS {dupes_staging_table}"
        }
    )

    drop_geo_validation_staging_table = PythonOperator(
        task_id="drop_geo_validation_staging_table",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"DROP TABLE IF EXISTS {geo_validation_staging_table}"
        }
    )

    drop_added_city_county_zip_staging_table = PythonOperator(
        task_id="drop_added_city_county_zip_staging_table",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"DROP TABLE IF EXISTS {added_city_county_zip}"
        }
    )

    (
            create_business_entities_yesterday_stage
            >> create_misfit_business_entities_table
            >> create_dupes_staging_table
            >> create_duplicates_table
            >> create_geo_validation_staging_table
            >> create_addded_city_county_zip_staging_table
            >> fetch_and_insert_business_entities_yesterday
            >> filter_invalid_addresses
            >> reformat_zip_code
            >> update_county_on_staging_table
            >> process_unmatched_entities_task
            >> check_validated_address_combos_task
            >> insert_validated_address_combos_task
            >> override_from_validated_addresses
            >> retry_update_county_on_staging_table
            >> identify_duplicates_task
            >> run_dq_check_null_values
            >> run_dq_check_principalstate
            >> run_dq_check_entityformdate
            >> run_dq_check_no_unexpected_duplicates
            >> insert_new_records_task
            >> insert_business_entities_with_ranking
            >> insert_duplicates
            >> insert_misfits
            >> drop_staging_table_task
            >> drop_dupes_staging_table_task
            >> drop_geo_validation_staging_table
            >> drop_added_city_county_zip_staging_table
     )


business_entity_dag()
