from google.cloud import bigquery

client = bigquery.Client()

# Load your JSON data
with open("customer_details.json", "r") as json_file:
    json_data = json_file.read()

# Destination table and its schema
destination_table_id = "dt-sabah-sandbox-dev.load_json_with_enrichment.Enriched_Customer_Data"
destination_schema = [
    bigquery.SchemaField('customer_id', 'INTEGER'),
    bigquery.SchemaField("payment_type", "STRING"),
    bigquery.SchemaField("store", "STRING"),
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

# Perform the enrichment using SQL JOIN and load into the destination table
query = """
    SELECT c.*, r.payment_type, r.store
    FROM UNNEST(JSON_QUERY(@json)) AS c
    LEFT JOIN `dt-sabah-sandbox-dev.load_json_with_schema.customer_reference` AS r
    ON c.customer_id = r.customer_id
"""

query_parameters = [bigquery.ArrayQueryParameter("json", "JSON_STRING", [json_data])]
job_config = bigquery.QueryJobConfig(
    query_parameters=query_parameters,
    destination=destination_table_id,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    schema=destination_schema
)

query_job = client.query(query, job_config=job_config)
query_job.result()  # Wait for the query to complete

print(f"Enriched data loaded into {destination_table_id}.")