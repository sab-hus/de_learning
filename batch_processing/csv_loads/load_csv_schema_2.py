from google.cloud import bigquery
import os

client = bigquery.Client()

schema = [ 
    bigquery.SchemaField("Date", "string"),
    bigquery.SchemaField("Time", "string"),
    bigquery.SchemaField("Latitude", "float"),
    bigquery.SchemaField("Longitude", "float"),
    bigquery.SchemaField("Type", "string"),
    bigquery.SchemaField("Depth", "float"),
    bigquery.SchemaField("Depth_Error", "float"),
    bigquery.SchemaField("Depth_Seismic_Stations", "integer"),
    bigquery.SchemaField("Magnitude", "float"),
    bigquery.SchemaField("Magnitude_Type", "string"),
    bigquery.SchemaField("Magnitude_Error", "float"),
    bigquery.SchemaField("Magnitude_Seismic_Stations", "integer"),
    bigquery.SchemaField("Azimuthal_Gap", "float"),
    bigquery.SchemaField("Horizontal_Distance", "float"),
    bigquery.SchemaField("Horizontal_Error", "float"),
    bigquery.SchemaField("Root_Mean_Square", "float"),
    bigquery.SchemaField("ID", "string"),
    bigquery.SchemaField("Source", "string"),
    bigquery.SchemaField("Location_Source", "string"),
    bigquery.SchemaField("Magnitude_Source", "string"),
    bigquery.SchemaField("Status", "string")
]		

table_id = "dt-sabah-sandbox-dev.load_with_schema.Significant_Earthquakes"
file_path = "/Users/sabahhussain/learning_development/batch_processing/csv_files/significant_earthquakes.csv"

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
