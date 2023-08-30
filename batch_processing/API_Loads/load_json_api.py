import requests
from google.cloud import bigquery

# Initialize the BigQuery client
client = bigquery.Client()

# Fetch data from the JSONPlaceholder API
api_url = "https://jsonplaceholder.typicode.com/posts"
response = requests.get(api_url)
data = response.json()

# Define destination table and schema
destination_table_id = "dt-sabah-sandbox-dev.load_api_data.JSON_placeholder_api"
schema = [
    bigquery.SchemaField("userId", "INTEGER"),
    bigquery.SchemaField("id", "INTEGER"),
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("body", "STRING")
]

# Load data into BigQuery
job_config = bigquery.LoadJobConfig(schema=schema)
load_job = client.load_table_from_json(
    data,
    destination_table_id,
    job_config=job_config)
load_job.result()  # Wait for the job to complete

print(f"Data loaded into {destination_table_id}.")
