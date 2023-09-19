from google.cloud import bigquery
import requests
import logging
from file_names_config import schema, table_id, api_url

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def create_job_config():
    job_config = bigquery.LoadJobConfig(
    schema = schema  
    )
    return job_config


def fetch_data_from_api(api_url):
    try:
        response = requests.get(api_url)
        if response.status_code == 200:
            earthquake_data = response.json()# Parse JSON response
            earthquake_features = earthquake_data.get("features", [])
            for row in earthquake_features:
                row["properties"] = str(row["properties"])
                row["geometry"] = str(row["geometry"])
            return earthquake_features
        else:
            logging.info(f"Request failed with status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        logging.info(f"Request error: {e}")
        return None

def load_data_into_bigquery(table_id, json_data):
    client = bigquery.Client()    
    job_config = create_job_config()
    json_data = fetch_data_from_api(api_url) 
    try:
        load_job = client.load_table_from_json(json_data, table_id, job_config)
        load_job.result()  # Wait for the job to complete
        logging.info(f"Data loaded into BigQuery table {table_id}")
    except Exception as e:
        logging.info(f"Error loading data into BigQuery: {e}")

def execute_pipelines(api_url, table_id):
    api_url = api_url
    table_id = table_id
    json_data = fetch_data_from_api(api_url) 
    if json_data:
        load_data_into_bigquery(table_id, json_data)

if __name__ == "__main__":
     execute_pipelines(api_url, table_id)
