import boto3
import os
import sys
from dotenv import load_dotenv

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

# Initialize the S3 client
s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

# Bucket and prefix
bucket_name = "< zach bucket name >"
prefix = "tayloro-capstone-data/"

# List objects in the S3 bucket directory
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

# Extract and print file names
if "Contents" in response:
    file_names = [obj["Key"] for obj in response["Contents"]]
    print("\n".join(file_names))
else:
    print("No files found.")