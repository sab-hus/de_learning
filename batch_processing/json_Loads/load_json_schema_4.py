from google.cloud import bigquery

client = bigquery.Client()

table_id = "dt-sabah-sandbox-dev.load_json_with_schema.Customer"
file_path = "/Users/sabahhussain/learning_development/batch_processing/json_files/customer_details.json"

schema = [
    bigquery.SchemaField('customer_id', 'INTEGER'),
    bigquery.SchemaField('name', 'STRING'),
    bigquery.SchemaField('age', 'INTEGER'),
    bigquery.SchemaField('city', 'STRING'),
    bigquery.SchemaField('is_student', 'BOOLEAN'),
    bigquery.SchemaField('hobbies', 'STRING', mode='REPEATED'),
    bigquery.SchemaField(
        "address",
        "RECORD",
        fields=[
        bigquery.SchemaField("street", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("zip", "STRING")
        ],
    ),
]

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    schema=schema
)

with open(file_path, 'rb') as source_file:
    load_job = client.load_table_from_file(
        source_file,
        table_id,
        job_config=job_config
    )
    
load_job.result()  # Wait for the job to complete
print("Data loaded successfully!")
