from google.cloud import bigquery

client = bigquery.Client()

table_id = "dt-sabah-sandbox-dev.load_json_partitioning.Flights"
file_path = "/Users/sabahhussain/learning_development/JSON_files/flight_dates.json"

schema = [
    bigquery.SchemaField('Destination', 'STRING'),
    bigquery.SchemaField('Airline', 'STRING'),
    bigquery.SchemaField('Date', 'DATE')
]

time_partitioning = bigquery.TimePartitioning(
        type_= bigquery.TimePartitioningType.YEAR,
        field= 'Date'
)

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    schema=schema,
    time_partitioning=time_partitioning
)

with open(file_path, 'rb') as source_file:
    load_job = client.load_table_from_file(
        source_file,
        table_id,
        job_config=job_config
    )
    
load_job.result()  # Wait for the job to complete
print("Data loaded successfully!")
