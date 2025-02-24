import os
import sys
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Add the parent directory of "upload_to_s3.py" to the module search path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

from upload_to_s3 import upload_to_s3

# Correct the relative path to skip the "include" directory
LOCAL_FILE = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),  # Current script's directory
        "../../../capstone_data/Colorado_County_Boundaries.csv"  # Adjusted to point to capstone_data
    )
)

print(f"Resolved path: {LOCAL_FILE}")

# Define the S3 bucket name and file path
S3_BUCKET = "<zach bucket path >"  # Replace with your actual S3 bucket name
S3_FILE_PATH = "tayloro-capstone-data/Colorado_County_Boundaries.csv"

def main():
    # Ensure the file exists locally before uploading
    if not os.path.exists(LOCAL_FILE):
        print(f"Error: Local file not found at {LOCAL_FILE}")
        return

    file_size = os.path.getsize(LOCAL_FILE)
    print(f"File size: {file_size / (1024 * 1024):.2f} MB")

    #Upload the file to S3
    s3_path = upload_to_s3(LOCAL_FILE, S3_BUCKET, S3_FILE_PATH)

    # Verify and print the result
    if s3_path:
        print(f"File successfully uploaded to {s3_path}")
    else:
        print("Failed to upload the file to S3.")

if __name__ == "__main__":
    main()