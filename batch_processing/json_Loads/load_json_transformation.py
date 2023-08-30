import json
from google.cloud import bigquery


def transform_data(age_column):
    age_column['Age'] += 5  # Adding 5 years to the Age
    return age_column

file_path = '/Users/sabahhussain/learning_development/batch_processing/json_files/birthdays.json'

transformed_data = []

with open(file_path, 'r', encoding='utf-8') as json_file:
    for row in json_file:
        try:
            json_object = json.loads(row)
            transformed_data.append(transform_data(json_object))
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {e}")

client = bigquery.Client()

table_id = "dt-sabah-sandbox-dev.load_json_transformation.Birthdays"

schema = [
    bigquery.SchemaField('Name', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Age', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('Date', 'DATE', mode='NULLABLE')
]

job_config = bigquery.LoadJobConfig(
    schema=schema,
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
)

# Load the transformed data into BigQuery table
load_job = client.load_table_from_json(
    transformed_data,
    table_id,
    job_config=job_config
)

load_job.result()  # Wait for the job to complete
print(f"Data loaded into {table_id} with transformations")


