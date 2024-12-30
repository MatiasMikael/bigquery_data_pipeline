import requests
import pandas as pd
from dotenv import load_dotenv
import os
import logging

# Load environment variables
load_dotenv()

# Configure paths
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))  # Path to the script
OUTPUT_FILE = os.path.join(PROJECT_ROOT, "../2_data/raw_data.csv")
LOG_FILE = os.path.join(PROJECT_ROOT, "../5_logs/check_data_structure.log")

# Configure logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Settings
BASE_URL = "https://finnhub.io/api/v1/"
API_KEY = os.getenv("FINNHUB_API_KEY")

def fetch_data(endpoint, params):
    """Fetches data from Finnhub API."""
    params["token"] = API_KEY
    response = requests.get(BASE_URL + endpoint, params=params)
    response.raise_for_status()
    logging.info(f"Data fetched from endpoint: {endpoint} with params: {params}")
    return response.json()

def analyze_and_save_data(data, output_file):
    """Analyzes the structure, columns, and sample data, and saves the data."""
    if isinstance(data, list):
        # Convert list to DataFrame
        df = pd.DataFrame(data)
        logging.info(f"Number of rows: {len(df)}, Columns: {list(df.columns)}")
        print("\n--- Data Summary ---")
        print(f"Number of rows: {len(df)}")
        print(f"Columns: {list(df.columns)}")
        print("\nData Types:")
        print(df.dtypes)
        print("\nSample Data:")
        print(df.head())

        # Save data to CSV
        df.to_csv(output_file, index=False)
        logging.info(f"Data saved to {output_file}")
    elif isinstance(data, dict):
        logging.warning("Data is a dictionary and cannot be saved as CSV.")
        print("\n--- Data Summary ---")
        print("Keys in the data:")
        for key, value in data.items():
            print(f"Key: {key}, Type: {type(value).__name__}, Sample Value: {value}")
    else:
        logging.error(f"Unknown data format: {type(data)}")
        print("Unknown data format. Data type:", type(data))

if __name__ == "__main__":
    # Example: Fetch symbol data
    endpoint = "stock/symbol"
    params = {"exchange": "US"}  # Modify as needed
    try:
        data = fetch_data(endpoint, params)
        analyze_and_save_data(data, OUTPUT_FILE)
    except Exception as e:
        logging.error("Error fetching or analyzing data", exc_info=True)
        print("Error fetching or analyzing data:", str(e))