import requests
from google.cloud import bigquery

client = bigquery.Client()

# Fetch data from the PokeAPI
api_url = "https://pokeapi.co/api/v2/pokemon?limit=10"  
response = requests.get(api_url)
data = response.json()["results"]

# Define destination table and schema
destination_table_id = "dt-sabah-sandbox-dev.load_api_data.Pokemon_api"
schema = [
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("url", "STRING")
]

# Load data into BigQuery
job_config = bigquery.LoadJobConfig(schema=schema)
load_job = client.load_table_from_json(data, destination_table_id, job_config=job_config)
load_job.result() 

print(f"Data loaded into {destination_table_id}.")
