import os
import logging
import pandas as pd
import requests
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
LOG_DIR = os.path.join(BASE_DIR, "5_logs")
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
LOG_FILE = os.path.join(LOG_DIR, "bigquery_pipeline.log")
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Environment variables
API_KEY = os.getenv("FINNHUB_API_KEY")
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
TABLE_ID = os.getenv("TABLE_ID")

RAW_DATA_PATH = os.path.join(BASE_DIR, "2_data", "raw_data.csv")
CLEANED_DATA_PATH = os.path.join(BASE_DIR, "2_data", "cleaned_data.csv")

def fetch_data():
    """Fetch data from Finnhub API and save to raw CSV."""
    try:
        url = f"https://finnhub.io/api/v1/stock/symbol?exchange=US&token={API_KEY}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)
        df.to_csv(RAW_DATA_PATH, index=False)
        logging.info(f"Fetched data saved to {RAW_DATA_PATH}. Rows: {len(df)}")
        print(f"Fetched data saved to {RAW_DATA_PATH}. Rows: {len(df)}")
    except Exception as e:
        logging.error("Error fetching data.", exc_info=True)
        print(f"Error fetching data: {e}")
        raise

def clean_data():
    """Clean raw data and save to cleaned CSV."""
    try:
        df = pd.read_csv(RAW_DATA_PATH)
        cleaned_df = df[["currency", "description", "displaySymbol", "symbol", "type"]]
        cleaned_df.to_csv(CLEANED_DATA_PATH, index=False)
        logging.info(f"Cleaned data saved to {CLEANED_DATA_PATH}. Rows: {len(cleaned_df)}")
        print(f"Cleaned data saved to {CLEANED_DATA_PATH}. Rows: {len(cleaned_df)}")
    except Exception as e:
        logging.error("Error cleaning data.", exc_info=True)
        print(f"Error cleaning data: {e}")
        raise

def load_to_bigquery():
    """Load cleaned data into BigQuery."""
    try:
        credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        df = pd.read_csv(CLEANED_DATA_PATH)
        job = client.load_table_from_dataframe(df, TABLE_ID)
        job.result()  # Wait for the job to complete
        logging.info(f"Data successfully loaded to BigQuery table {TABLE_ID}. Rows: {len(df)}")
        print(f"Data successfully loaded to BigQuery table {TABLE_ID}.")
    except Exception as e:
        logging.error("Error loading data to BigQuery.", exc_info=True)
        print(f"Error loading data to BigQuery: {e}")
        raise

if __name__ == "__main__":
    logging.info("Pipeline started.")
    print("Pipeline started.")
    try:
        fetch_data()
        clean_data()
        load_to_bigquery()
        logging.info("Pipeline completed successfully.")
        print("Pipeline completed successfully.")
    except Exception as e:
        logging.error("Pipeline failed.", exc_info=True)
        print(f"Pipeline failed: {e}")