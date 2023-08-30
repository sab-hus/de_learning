from google.cloud import bigquery

client = bigquery.Client()

schema = [
    
    bigquery.SchemaField("NAME", "string"),
    bigquery.SchemaField("LIGHT-YEARS FROM EARTH", "string"),
    bigquery.SchemaField("PLANET MASS", "string"),
    bigquery.SchemaField("STELLAR MAGNITUDE", "string"),
    bigquery.SchemaField("DISCOVERY DATE", "string")
]

table_id = "dt-sabah-sandbox-dev.load_with_schema.planets"
file_path = "/Users/sabahhussain/learning_development/batch_processing/csv_files/Planets.csv"

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
