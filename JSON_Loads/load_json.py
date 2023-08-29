from google.cloud import bigquery

client = bigquery.Client()

table_id_1 = "dt-sabah-sandbox-dev.load_json.People"
file_path_1 = "/Users/sabahhussain/learning_development/JSON_files/people.json"

table_id_2 = "dt-sabah-sandbox-dev.load_json.Colours"
file_path_2 = "/Users/sabahhussain/learning_development/JSON_files/colours.json"

table_id_3 = "dt-sabah-sandbox-dev.load_json.Fruit"
file_path_3 = "/Users/sabahhussain/learning_development/JSON_files/fruit.json"

table_id_4 = "dt-sabah-sandbox-dev.load_json.Customers"
file_path_4 = "/Users/sabahhussain/learning_development/JSON_files/customer_details.json"

table_id_5 = "dt-sabah-sandbox-dev.load_json.Birthdays"
file_path_5 = "/Users/sabahhussain/learning_development/JSON_files/birthdays.json"

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    autodetect = True
)

# Load JSON data from local file into BigQuery table
with open(file_path_1, 'rb') as source_file:
    load_job = client.load_table_from_file(
        source_file,
        table_id_1,
        job_config=job_config
    )

with open(file_path_2, 'rb') as source_file:
    load_job = client.load_table_from_file(
        source_file,
        table_id_2,
        job_config=job_config
    )

with open(file_path_3, 'rb') as source_file:
    load_job = client.load_table_from_file(
        source_file,
        table_id_3,
        job_config=job_config
    )

with open(file_path_4, 'rb') as source_file:
    load_job = client.load_table_from_file(
        source_file,
        table_id_4,
        job_config=job_config
    )
    
with open(file_path_5, 'rb') as source_file:
    load_job = client.load_table_from_file(
        source_file,
        table_id_5,
        job_config=job_config
    )
    
load_job.result()  # Wait for the job to complete
print("Data loaded successfully!")
