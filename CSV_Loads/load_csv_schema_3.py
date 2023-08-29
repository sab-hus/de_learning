from google.cloud import bigquery
import os

client = bigquery.Client()

schema = [
    
    bigquery.SchemaField("index", "integer"),
    bigquery.SchemaField("user_id", "string"),
    bigquery.SchemaField("first_name", "string"),
    bigquery.SchemaField("last_name", "string"),
    bigquery.SchemaField("sex", "string"),
    bigquery.SchemaField("email", "string"),
    bigquery.SchemaField("phone", "string"),
    bigquery.SchemaField("date_of_birth", "date"),
    bigquery.SchemaField("job_title", "string")
]

table_id = "dt-sabah-sandbox-dev.load_with_schema.People"
file_path = "/Users/sabahhussain/learning_development/CSV_files/people_100.csv"

job_config = bigquery.LoadJobConfig(
    source_format = bigquery.SourceFormat.CSV, 
    skip_leading_rows = 1,
    schema = schema,
    write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
)

with open(file_path, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id, job_config=job_config)

job.result()

if job.error_result:
    print("Error loading data: {}".format(job.error_result))
else:
    print("Data loaded successfully!")
