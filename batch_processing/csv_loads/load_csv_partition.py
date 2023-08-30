from google.cloud import bigquery
import os

client = bigquery.Client()

schema = [
    
    bigquery.SchemaField("ISODateTimeUTC", "timestamp"),
    bigquery.SchemaField("Lat", "float"),
    bigquery.SchemaField("Lon", "float"),
    bigquery.SchemaField("SOG", "float"),
    bigquery.SchemaField("COG", "float"),
    bigquery.SchemaField("TWD", "float"),
    bigquery.SchemaField("TWA", "float"),
    bigquery.SchemaField("VMG", "float")
]

time_partitioning = bigquery.TimePartitioning(
    type_ = bigquery.TimePartitioningType.DAY,
    field = "ISODateTimeUTC"
)

table_id = "dt-sabah-sandbox-dev.load_with_partitioning.njord_csv_sample"
file_path = "/Users/sabahhussain/learning_development/batch_processing/csv_files/njord_csv_sample.csv"

job_config = bigquery.LoadJobConfig(
    source_format = bigquery.SourceFormat.CSV, 
    skip_leading_rows = 1,
    schema = schema,
    write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
    time_partitioning = time_partitioning
)

with open(file_path, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id, job_config=job_config)

job.result()

if job.error_result:
    print("Error loading data: {}".format(job.error_result))
else:
    print("Data loaded successfully!")
