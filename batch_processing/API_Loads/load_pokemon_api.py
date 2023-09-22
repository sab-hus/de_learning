from google.cloud import bigquery
import requests
import logging
from file_names_config import poke_schema, poke_child_table_id, poke_api_url

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def create_job_config():
    job_config = bigquery.LoadJobConfig(
        autodetect = True
    # schema = poke_schema
    )
    return job_config

def fetch_data_from_api(poke_api_url):
    try:
        response = requests.get(poke_api_url)
        if response.status_code == 200:
            poke_data = response.json()# Parse JSON response
            full_poke_results = poke_data.get("results", [])
            names = [entry.get("name") for entry in full_poke_results]
            for name in names:
                encounters_endpoint = f"{poke_api_url}/{name}/encounters"
                encounters_response = requests.get(encounters_endpoint)
                if encounters_response.status_code == 200:
                    encounters_data = encounters_response.json()
                    for row in encounters_data:
                        row["version_details"] = str(row["version_details"])
                        row["location_area"] = str(row["location_area"])
                    return encounters_data
        else:
            logging.info(f"Request failed with status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        logging.info(f"Request error: {e}")
        return None
fetch_data_from_api(poke_api_url)

# make call to the parent, get what you need (i.e the names/id), then make all the required calls to child endpoint, then load to bq
# asyncronous - operations and tasks that can happen simultaneously together - if you have loads of two parents child relationships endpoints that are not related to each other - you can run the 2 streams asyncronously
# flatten out json files - practice in BQ with Characters_R_and_M table - think higher level - logic is the same conceptually in terms of how to handle a scenario
# read more batch processing, elt/etl, 

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
    api_url = poke_api_url
    table_id = poke_child_table_id
    json_data = fetch_data_from_api(api_url) 
    if json_data:
        load_data_into_bigquery(table_id, json_data)

if __name__ == "__main__":
    api_url = poke_api_url
    table_id = poke_child_table_id
    execute_pipelines(api_url, table_id)
