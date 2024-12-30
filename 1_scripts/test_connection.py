import logging
from google.cloud import bigquery
from google.oauth2 import service_account
import os

# Configure logging
log_file_path = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "5_logs", "test_connection.log"
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

# List datasets
try:
    datasets = list(client.list_datasets())
    if datasets:
        print("Datasets in your project:")
        for dataset in datasets:
            print(f"- {dataset.dataset_id}")
            logging.info(f"Dataset found: {dataset.dataset_id}")
    else:
        print("No datasets found in your project.")
        logging.info("No datasets found in the project.")
except Exception as e:
    logging.error("Error while listing datasets.", exc_info=True)
    print(f"Error listing datasets: {e}")