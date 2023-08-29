import csv
from google.cloud import bigquery

file_path = '/Users/sabahhussain/learning_development/CSV_files/people_100.csv'

def transform_email(email):
    return email.split('@')[0]

transformed_data = []

# Read the CSV file and transform the data
with open(file_path, 'r', newline='', encoding='utf-8') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        row['Email'] = transform_email(row['Email'])
        transformed_data.append(row)

# Initialize BigQuery client
client = bigquery.Client()

# Define BigQuery table ID
table_id = 'dt-sabah-sandbox-dev.load_with_transformations.people_email_transformation'

# Define schema for the BigQuery table
schema = [
        bigquery.SchemaField('Index', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('User Id', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('First Name', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Last Name', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Sex', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Email', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Phone', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Date of birth', 'DATE', mode='NULLABLE'),
    bigquery.SchemaField('Job Title', 'STRING', mode='NULLABLE')
]

# Load the data into BigQuery table
job_config = bigquery.LoadJobConfig(
    schema=schema,
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
)

load_job = client.load_table_from_json(
    transformed_data,
    table_id,
    job_config=job_config)

load_job.result()  # Wait for the job to complete
print(f"Data loaded into {table_id} with transformations")
