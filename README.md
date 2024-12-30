## bigquery_data_pipeline

**bigquery_data_pipeline** is a Python-based data pipeline that retrieves stock data from the [Finnhub Stock API](https://finnhub.io/), processes it by removing unnecessary columns, and loads the cleaned data into Google BigQuery.

## Features

1. Fetches stock data from Finnhub API.
2. Processes data by removing unnecessary columns.
3. Loads data into Google BigQuery.

## Prerequisites

1. Python 3.7 or higher
2. Google Cloud account with BigQuery enabled
3. Finnhub API key

## Installation

1. **Clone the Repository**

    ```bash
    git clone https://github.com/yourusername/bigquery_data_pipeline.git
    cd bigquery_data_pipeline
    ```

2. **Create and Activate a Virtual Environment**

    ```bash
    python3 -m venv venv
    source venv/bin/activate  # Linux/Mac
    venv\Scripts\activate     # Windows
    ```

3. **Install Dependencies**

    ```bash
    pip install -r requirements.txt
    ```

## Usage

1. **Set Up Configuration**

    Create a `.env` file in the project root with the following variables:

    ```env
    FINNHUB_API_KEY=your_finnhub_api_key
    GOOGLE_APPLICATION_CREDENTIALS=path_to_your_google_credentials.json
    BIGQUERY_DATASET=your_bigquery_dataset
    BIGQUERY_TABLE=your_bigquery_table
    ```

2. **Run the Pipeline**

    ```bash
    python main.py
    ```

## License

This project is licensed under the [MIT License](LICENSE). Data is sourced from [Finnhub](https://finnhub.io/).
