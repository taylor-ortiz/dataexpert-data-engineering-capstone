import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as F
from pyspark.sql.functions import concat_ws, col, udf
from pyspark.sql.types import StructType, StructField, StringType
import requests

# Add the parent directory of "upload_to_s3.py" to the module search path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

from aws_secret_manager import get_secret

# Load environment variables from .env file
load_dotenv()

# Fetch AWS keys and catalog credentials from environment variables/secrets
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
tabular_credential = get_secret("TABULAR_CREDENTIAL")
catalog_name = get_secret("CATALOG_NAME")

# S3 bucket name (if needed)
s3_bucket = "< zach bucket name >"

# Define required packages
extra_packages = [
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2",
    "software.amazon.awssdk:bundle:2.17.178",
    "software.amazon.awssdk:url-connection-client:2.17.178",
    "org.apache.hadoop:hadoop-aws:3.3.4"
]

# Initialize SparkConf
conf = SparkConf()
conf.set('spark.jars.packages', ','.join(extra_packages))
conf.set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
conf.set('spark.sql.defaultCatalog', catalog_name)
conf.set(f'spark.sql.catalog.{catalog_name}', 'org.apache.iceberg.spark.SparkCatalog')
conf.set(f'spark.sql.catalog.{catalog_name}.credential', tabular_credential)
conf.set(f'spark.sql.catalog.{catalog_name}.catalog-impl', 'org.apache.iceberg.rest.RESTCatalog')
conf.set(f'spark.sql.catalog.{catalog_name}.warehouse', catalog_name)
conf.set(f'spark.sql.catalog.{catalog_name}.uri', 'https://api.tabular.io/ws/')
conf.set('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog')
conf.set('spark.sql.catalog.spark_catalog.type', 'hive')
conf.set('spark.executor.memory', '16g')
conf.set('spark.sql.shuffle.partitions', '200')
conf.set('spark.driver.memory', '16g')

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Geoapify Address Validation with EntityID") \
    .config(conf=conf) \
    .getOrCreate()

# (Optional) Set AWS credentials if reading from S3; if not needed, these can be omitted.
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key_id)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_access_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

print("SparkSession initialized successfully:", spark)

# ------------------------------------------------------------------------------
# Read the required tables into DataFrames
# ------------------------------------------------------------------------------
# Business Entities DataFrame
cbeDF = spark.table("tayloro.colorado_business_entities")
# Reference table (city, county, zip) DataFrame
cczDF = spark.table("tayloro.colorado_city_county_zip")

# ------------------------------------------------------------------------------
# Prepare DataFrames for Joining
# ------------------------------------------------------------------------------
# For cbeDF: create lower-case version of principalcity and ensure ZIP is a string.
cbe_prepped = cbeDF.withColumn("city_lower", F.lower(F.col("principalcity"))) \
                   .withColumn("zip_str", F.col("principalzipcode"))
# For cczDF: create lower-case version of city and cast zip_code to string.
ccz_prepped = cczDF.withColumn("city_lower", F.lower(F.col("city"))) \
                   .withColumn("zip_str", F.col("zip_code").cast("string"))

# Join the DataFrames on the prepped columns
enrichedDF = cbe_prepped.join(ccz_prepped, on=["city_lower", "zip_str"], how="left")

# Filter for Colorado records where the reference table did not match (i.e., unmatched addresses)
unmatchedDF = enrichedDF.filter(
    (F.col("principalstate") == "CO") &
    (F.col("city").isNull())  # "city" from the reference table (ccz)
)

print("Total unmatched rows:", unmatchedDF.count())

# ------------------------------------------------------------------------------
# Build the Full Address and Define the Geocoding UDF
# ------------------------------------------------------------------------------
# Create a full address column. You can adjust the concatenation as needed.
unmatchedDF = unmatchedDF.withColumn(
    "full_address",
    concat_ws(", ", F.col("principaladdress1"), F.col("principalcity"), F.col("principalzipcode"))
)

# Define a Python function to call the Geoapify API and extract city, county, state, and postcode.
def geocode_address(address):
    if not address:
        return (None, None, None, None)
    base_url = "https://api.geoapify.com/v1/geocode/search"
    url = f"{base_url}?text={address}&apiKey={api_key}"
    try:
        resp = requests.get(url)
        if resp.status_code == 200:
            data = resp.json()
            if "features" in data and len(data["features"]) > 0:
                properties = data["features"][0].get("properties", {})
                city = properties.get("city", None)
                county = properties.get("county", None)
                state = properties.get("state", None)
                postcode = properties.get("postcode", None)
                return (city, county, state, postcode)
        return (None, None, None, None)
    except Exception as e:
        return (None, None, None, None)

# Define the schema for the geocode response as a struct
geocode_schema = StructType([
    StructField("geo_city", StringType(), True),
    StructField("geo_county", StringType(), True),
    StructField("geo_state", StringType(), True),
    StructField("geo_postcode", StringType(), True)
])

# Register the UDF
geocode_udf = udf(geocode_address, geocode_schema)

# Apply the UDF to the DataFrame to get geocoded data
geocodedDF = unmatchedDF.withColumn("geocode", geocode_udf(F.col("full_address")))

# Extract the individual fields from the geocode struct and retain the entityid
geocodedDF = geocodedDF.withColumn("geo_city", F.col("geocode.geo_city")) \
                       .withColumn("geo_county", F.col("geocode.geo_county")) \
                       .withColumn("geo_state", F.col("geocode.geo_state")) \
                       .withColumn("geo_postcode", F.col("geocode.geo_postcode")) \
                       .drop("geocode")

# ------------------------------------------------------------------------------
# Select only the columns that match the target table schema
# Target Table Columns: entityid, principaladdress1, principalcity, principalzipcode, full_address,
#                         geo_city, geo_county, geo_state, geo_postcode
# ------------------------------------------------------------------------------
outputDF = geocodedDF.select(
    "entityid",
    "principaladdress1",
    "principalcity",
    "principalzipcode",
    "full_address",
    "geo_city",
    "geo_county",
    "geo_state",
    "geo_postcode"
)

# (Optional) Show sample results
outputDF.show(20, truncate=False)

# ------------------------------------------------------------------------------
# Create the target Iceberg table if it doesn't exist
# ------------------------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS `tayloro-academy-warehouse`.tayloro.colorado_temp_geocoded_entities (
    entityid BIGINT,
    principaladdress1 STRING,
    principalcity STRING,
    principalzipcode STRING,
    full_address STRING,
    geo_city STRING,
    geo_county STRING,
    geo_state STRING,
    geo_postcode STRING
)
USING iceberg
""")

# ------------------------------------------------------------------------------
# Write the selected output DataFrame to the Iceberg table
# ------------------------------------------------------------------------------
outputDF.writeTo("`tayloro-academy-warehouse`.tayloro.colorado_temp_geocoded_entities") \
        .tableProperty("write.format.default", "parquet") \
        .overwritePartitions()

print("Geocoded data successfully written to Iceberg table: `tayloro-academy-warehouse`.tayloro.colorado_temp_geocoded_entities")
