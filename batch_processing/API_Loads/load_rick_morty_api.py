from google.cloud import bigquery
import requests
import logging
from file_names_config import r_and_m_schema, r_and_m_table_id, r_and_m_api_url

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def create_job_config():
    job_config = bigquery.LoadJobConfig(
    schema = r_and_m_schema
    )
    return job_config


def fetch_data_from_api(api_url):
    try:
        character_data = []  # Initialize an empty list to store all data
        next_page = api_url  # Initialize the first page
        response = requests.get(api_url)
        while next_page:
            response = requests.get(next_page)
            if response.status_code == 200:
                character_response = response.json()# Parse JSON response
                results = character_response.get("results", [])
                for row in results:
                    row["created"] = str(row["created"])
                    row["origin"] = str(row["origin"])
                    row["location"] = str(row["location"])
                character_data.extend(results)
                next_page = character_response.get("info", {}).get("next")
            else:
                logging.info(f"Request failed with status code: {response.status_code}")
                break
        return character_data
    except requests.exceptions.RequestException as e:
        logging.info(f"Request error: {e}")
        return None

def load_data_into_bigquery(table_id, json_data):
    client = bigquery.Client()    
    job_config = create_job_config()
    try:
        load_job = client.load_table_from_json(json_data, table_id, job_config)
        load_job.result()  # Wait for the job to complete
        logging.info(f"Data loaded into BigQuery table {table_id}")
    except Exception as e:
        logging.info(f"Error loading data into BigQuery: {e}")

def execute_pipelines(api_url, table_id):
    api_url = r_and_m_api_url
    table_id = r_and_m_table_id
    json_data = fetch_data_from_api(api_url) 
    if json_data:
        load_data_into_bigquery(table_id, json_data)

if __name__ == "__main__":
    api_url = r_and_m_api_url
    table_id = r_and_m_table_id
    execute_pipelines(api_url, table_id)
