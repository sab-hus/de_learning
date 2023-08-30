import json
from google.cloud import bigquery

# Define a function to transform the Date format
def transform_date(date_str):
    return date_str[:4]  # Extract the first 4 characters (YYYY)

# Read the JSON file and perform the transformation
file_path = '/Users/sabahhussain/learning_development/batch_processing/json_files/flight_dates.json'

transformed_data = []

with open(file_path, 'r') as json_file:
    for row in json_file:
        json_object = json.loads(row)
        json_object['Date'] = transform_date(json_object['Date'])
        transformed_data.append(json_object)

# Initialize BigQuery client and load the transformed data
client = bigquery.Client()
table_id = "dt-sabah-sandbox-dev.load_json_transformation.Flight_Year_Transformation"


schema = [
    bigquery.SchemaField('Destination', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Airline', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Date', 'STRING', mode='NULLABLE')
]

job_config = bigquery.LoadJobConfig(
    schema=schema,
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
)

# Load the transformed data into BigQuery table
load_job = client.load_table_from_json(
    transformed_data,
    table_id,
    job_config=job_config
)

load_job.result()  # Wait for the job to complete
print(f"Data loaded into {table_id} with transformations")
