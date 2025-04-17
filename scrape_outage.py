from bs4 import BeautifulSoup
import requests
import os
from supabase import create_client, Client
from datetime import datetime
import pytz
import re

# Only try to load .env if running locally
try:
    from dotenv import load_dotenv
    load_dotenv()  # loads .env into os.environ
except ImportError:
    pass  # Skip if dotenv is not installed (like in GitHub Actions)

URL = "https://lumapr.com/?lang=en"
DB_URL: str = os.environ.get("SUPABASE_URL")         # or paste your project URL here
DB_KEY: str = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
OUTAGE_DB_NAME = "outage_by_region"

def scrape_data():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
    }
    # Avoid the 403 Forbidden error by the website by using a user-agent header
    response = requests.get(URL, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Failed to fetch page: {response.status_code}")

    soup = BeautifulSoup(response.text, 'html.parser')

    # Find the table body
    tbody = soup.find("tbody", class_="row-hover")

    # Initialize a list to hold the data
    region_data = []

    # Loop through each row in the table body
    for row in tbody.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) == 4:
            region = cells[0].text.strip()
            customers_restored = cells[1].text.strip()
            total_customers = cells[2].text.strip()
            percent_restored = cells[3].text.strip()
            
            region_data.append({
                "Region": region,
                "Customers Restored": customers_restored,
                "Total Customers": total_customers,
                "% Restored": percent_restored
            })
    # Find the span that contains the phrase "Information updated as of"
    timestamp_span = soup.find("span", string=re.compile(r"Information updated as of"))

    if timestamp_span:
        raw_text = timestamp_span.get_text(strip=True)
        print("Raw text found:", raw_text)

        # Extract and clean date string
        match = re.search(r"as of (.+)", raw_text)
        if match:
            date_str = match.group(1).replace("at ", "")
            
            # Normalize "a.m." and "p.m." to "AM"/"PM"
            date_str = date_str.replace("a.m.", "AM").replace("p.m.", "PM")

            # Now parse
            parsed_timestamp = datetime.strptime(date_str, "%B %d, %Y, %I:%M %p")
            print("Parsed timestamp:", parsed_timestamp.isoformat())

        return {"region_data": region_data, "published_timestamp": parsed_timestamp.isoformat()}
    else:
        raise Exception("Timestamp not found in the page")

'''
    Check published_timestamp of last row in the database
    and compare with the timestamp of the new data
'''
def is_data_new(new_timestamp):
    # Create a Supabase client
    supabase: Client = create_client(DB_URL, DB_KEY)

    # Query the last row in the database if it exists
    response = supabase.table(OUTAGE_DB_NAME).select("timestamp").order("timestamp", desc=True).limit(1).execute()
    if len(response.data) == 0:
        # No data in the database, so consider it new
        return True
    last_timestamp = response.data[0]["timestamp"]

    # Compare the timestamps
    if new_timestamp > last_timestamp:
        return True
    else:
        return False


def validate_data(region_data):
    # Check if the data is in the expected format
    for entry in region_data:
        if not isinstance(entry, dict):
            raise ValueError("Data entry is not a dictionary")
        if "Region" not in entry or "Customers Restored" not in entry or "Total Customers" not in entry or "% Restored" not in entry:
            raise ValueError("Missing required fields in data entry")
        if not isinstance(entry["Region"], str) or not isinstance(entry["Customers Restored"], str) or not isinstance(entry["Total Customers"], str) or not isinstance(entry["% Restored"], str):
            raise ValueError("Invalid data type for one of the fields")
        # Check if the values are in the expected format
        if not entry["Customers Restored"].replace(",", "").isdigit() or not entry["Total Customers"].replace(",", "").isdigit() or not entry["% Restored"].replace("%", "").replace(",", "").isdigit():
            raise ValueError("Invalid value format for one of the fields")
    return True

def insert_data_to_db(region_data_and_published_timestamp):
    # Create a Supabase client
    supabase: Client = create_client(DB_URL, DB_KEY)
    
    # Now flatten into one long row
    # Obtain all unique region values and put into long row
    long_row = {}

    region_data = region_data_and_published_timestamp["region_data"]
    for entry in region_data:
        region = entry["Region"]
        # remove accents and umlaut characters
        region = region.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
        region = region.replace("ü", "u").replace("ñ", "n").replace("Á", "A").replace("É", "E").replace("Í", "I").replace("Ó", "O").replace("Ú", "U")
        region = region.replace("Ñ", "N").replace("ü", "u").replace("Ü", "U").replace("Ö", "O").replace("Ä", "A").replace("Ö", "O")
        region = region.replace(" ", "")
        data_keys = entry.keys()
        for key in data_keys:
            if key == "Region":
                continue
            else:
                column_mappings = {"Customers Restored":"restored_customers","Total Customers":"total_customers","% Restored":"percent_restored"}
                column_name = f"{region}_{column_mappings[key]}".lower()
                processed_value = int(entry[key].replace(",", "").replace("%", ""))
                long_row[column_name] = processed_value 

    # Time stamp
    long_row["published_timestamp"] = region_data_and_published_timestamp["published_timestamp"]
    puerto_rico_tz = pytz.timezone("America/Puerto_Rico")
    long_row["timestamp"] = datetime.now(puerto_rico_tz).isoformat()
    print()
    print(long_row)
    # Insert into Supabase
    response = supabase.table(OUTAGE_DB_NAME).insert(long_row).execute()
    print("Data inserted successfully")
    return response
    

if __name__ == "__main__":
    try:
        region_data_and_published_timestamp = scrape_data()
        data_is_new = is_data_new(region_data_and_published_timestamp["published_timestamp"])
        data_is_valid = validate_data(region_data_and_published_timestamp["region_data"])
        if not data_is_new:
            print("Data is not new")
        if not data_is_valid:
            print("Data is not valid")

        if data_is_new and data_is_valid:
            insert_data_to_db(region_data_and_published_timestamp)

    except Exception as e:
        print(f"Error during scraping:\n{e}")




