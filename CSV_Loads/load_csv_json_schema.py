from google.cloud import bigquery
import os
from schema_files import planets

client = bigquery.Client()

schema = planets
table_id = "dt-sabah-sandbox-dev.load_with_schema.planets_json_schema"
file_path = "/Users/sabahhussain/learning_development/CSV_files/Planets.csv"

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
