import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import regexp_replace

# Add the parent directory of "upload_to_s3.py" to the module search path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

from aws_secret_manager import get_secret


# Load environment variables from .env file
load_dotenv()

# Fetch AWS keys from environment variables
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
tabular_credential = get_secret("TABULAR_CREDENTIAL")
catalog_name = get_secret("CATALOG_NAME")

# S3 bucket name
s3_bucket = "< zach bucket name >"

# Define required packages
extra_packages = [
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2",  # Iceberg runtime
    "software.amazon.awssdk:bundle:2.17.178",  # AWS SDK bundle
    "software.amazon.awssdk:url-connection-client:2.17.178",  # URL connection client
    "org.apache.hadoop:hadoop-aws:3.3.4"  # Hadoop AWS connector
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
conf.set('spark.executor.memory', '16g')  # Adjust the value based on your system's capacity
conf.set('spark.sql.shuffle.partitions', '200')
conf.set('spark.driver.memory', '16g')



# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Load S3 data into Iceberg") \
    .config(conf=conf) \
    .getOrCreate()

# Set AWS credentials for S3 access
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key_id)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_access_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

spark.sql("""
CREATE TABLE IF NOT EXISTS tayloro.coloraodo_county_crime_rate_per_capita (
    county_name STRING,
    periodyear INTEGER,
    total_crimes INTEGER,
    total_population INTEGER,
    crime_per_100k DOUBLE  -- Crime per 100,000 people
)
USING iceberg
PARTITIONED BY (county_name);
""")

print("Iceberg table created successfully (if it didn't already exist).")