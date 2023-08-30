import requests
from google.cloud import bigquery

client = bigquery.Client()

# Fetch data from the PokeAPI
api_url = "https://pokeapi.co/api/v2/pokemon?limit=10"  
response = requests.get(api_url)
data = response.json()["results"]

transformed_data = []
for entry in data:
    url_parts = entry["url"].split("/")
    pokemon_id = int(url_parts[-2])  # Extracting the second-to-last part of the URL as pokemon_id
    transformed_data.append({
        "name": entry["name"].capitalize(),
        "url": entry["url"],
        "pokemon_id": pokemon_id
    })

# Define destination table and schema
destination_table_id = "dt-sabah-sandbox-dev.load_api_transformation.Pokemon_api"
schema = [
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("url", "STRING"),
    bigquery.SchemaField("pokemon_id", "INTEGER")
]

# Load data into BigQuery
job_config = bigquery.LoadJobConfig(schema=schema)
load_job = client.load_table_from_json(transformed_data, destination_table_id, job_config=job_config)
load_job.result() 

print(f"Data loaded into {destination_table_id}.")