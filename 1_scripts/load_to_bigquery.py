import logging
import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# Configure logging
log_file_path = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "5_logs", "load_to_bigquery.log"
)
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Path to credentials file
credentials_path = "C:\\Users\\Matias\\Desktop\\bigquery_data_pipeline\\credentials\\bigquery-data-pipeline-a7e3a97ee01c.json"

# Initialize BigQuery client
try:
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(credentials=credentials, project="bigquery-data-pipeline")
    logging.info("Successfully initialized BigQuery client.")
except Exception as e:
    logging.error("Failed to initialize BigQuery client.", exc_info=True)
    print(f"Error initializing BigQuery client: {e}")
    exit(1)

# Load data from CSV
input_file_path = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "2_data", "cleaned_data.csv"
)
try:
    df = pd.read_csv(input_file_path)
    logging.info(f"Loaded data from {input_file_path} with {len(df)} rows.")
except Exception as e:
    logging.error("Failed to load data from CSV.", exc_info=True)
    print(f"Error loading CSV data: {e}")
    exit(1)

# Define dataset and table
dataset_id = "bigquery-data-pipeline.stock_data"
table_id = f"{dataset_id}.cleaned_table"

# Load data to BigQuery
try:
    job = client.load_table_from_dataframe(df, table_id)  # Load DataFrame to BigQuery table
    job.result()  # Wait for the job to complete
    logging.info(f"Data successfully loaded to {table_id}. Rows: {len(df)}.")
    print(f"Data successfully loaded to {table_id}.")
except Exception as e:
    logging.error("Error loading data to BigQuery.", exc_info=True)
    print(f"Error loading data to BigQuery: {e}")
