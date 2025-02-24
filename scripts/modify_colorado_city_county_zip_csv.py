import os
import sys
import csv

# Correct the relative path to skip the "include" directory
LOCAL_FILE = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),  # Current script's directory
        "../../../capstone_data/colorado_city_county_zip.csv"  # Adjusted to point to capstone_data
    )
)

print(f"Resolved path: {LOCAL_FILE}")

# Determine the output file path (we'll write the updated CSV in the same folder)
output_file = os.path.join(os.path.dirname(LOCAL_FILE), "colorado_city_county_zip_updated.csv")
print(f"Output file path: {output_file}")

# Use utf-8-sig to handle potential BOM in the CSV file.
with open(LOCAL_FILE, 'r', newline='', encoding='utf-8-sig') as infile:
    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames
    if not fieldnames:
        print("Error: No header row found in the CSV.")
        sys.exit(1)

    print("Found headers:", fieldnames)

    # Locate the ZIP Code header in a case-insensitive manner.
    zip_header = None
    for header in fieldnames:
        if header.strip().lower() == "zip code":
            zip_header = header
            break

    if not zip_header:
        print("Error: 'ZIP Code' column not found in the CSV.")
        sys.exit(1)

    updated_rows = []
    for row in reader:
        zip_value = row[zip_header]
        # Remove the prefix "ZIP Code " if it appears
        if zip_value.startswith("ZIP Code "):
            row[zip_header] = zip_value.replace("ZIP Code ", "", 1)
        updated_rows.append(row)

# Write the updated rows into a new CSV file.
with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(updated_rows)

print(f"CSV updated successfully: {output_file}")
