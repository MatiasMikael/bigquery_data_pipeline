import pandas as pd
import os
import logging

def clean_data(input_file, output_file, log_file):
    """Cleans the raw stock data by keeping only selected columns."""
    # Configure logging
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    
    try:
        # Load the data
        df = pd.read_csv(input_file)
        logging.info(f"Loaded data from {input_file} with {len(df)} rows and columns: {list(df.columns)}")

        # Keep only selected columns
        columns_to_keep = ['currency', 'description', 'displaySymbol', 'symbol', 'type']
        df = df[columns_to_keep]
        logging.info(f"Kept columns: {columns_to_keep}. Remaining data has {len(df)} rows.")

        # Drop rows with missing values
        df.dropna(inplace=True)
        logging.info(f"Dropped rows with missing values. Remaining rows: {len(df)}.")

        # Save the cleaned data to a new CSV file
        df.to_csv(output_file, index=False)
        logging.info(f"Cleaned data saved to {output_file}.")
        print(f"Cleaned data saved to {output_file}. Number of rows: {len(df)}")
    except Exception as e:
        logging.error("Error during data cleaning", exc_info=True)
        print("Error during data cleaning:", str(e))

if __name__ == "__main__":
    # Configure paths
    PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
    INPUT_FILE = os.path.join(PROJECT_ROOT, "2_data/raw_data.csv")
    OUTPUT_FILE = os.path.join(PROJECT_ROOT, "2_data/cleaned_data.csv")
    LOG_FILE = os.path.join(PROJECT_ROOT, "5_logs/clean_data.log")

    # Run the cleaning process
    clean_data(INPUT_FILE, OUTPUT_FILE, LOG_FILE)