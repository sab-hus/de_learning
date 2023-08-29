from google.cloud import bigquery
import csv
from datetime import datetime

client = bigquery.Client()

table_id = "load_with_transformations.people_100_sample"

file = 'people_100.csv'
transformed_date_file = 'transformed_people_100.csv'

transformed_data= []
with open(file, 'r', newline='') as csvfile:
    csvreader = csv.reader(csvfile)
    for row in csvreader:
        try:
            old_date = datetime.strptime(row[7].strip(), '%Y-%m-%d')
            new_date = old_date.strftime('%d/%m/%Y')
            row[7] = new_date
        except ValueError as e:
            print(f"Error processing row: {row[7]}. Error: {e}")
            transformed_data.append(row)

# for row in data:
#     try:
#         old_date = datetime.strptime(row[7].strip(), '%Y-%m-%d')
#         new_date = old_date.strftime('%d/%m/%Y')
#         row[7] = new_date
#     except ValueError as e:
#         print(f"Error processing row: {row[7]}. Error: {e}")

with open(transformed_date_file, 'w', newline='') as csvfile:
    fieldnames = ['Index', 'User Id', 'First Name', 'Last Name', 'Sex', 'Email', 'Phone', 'Date of birth', 'Job Title']
    csvwriter = csv.DictWriter(csvfile, fieldnames=fieldnames)
    csvwriter.writeheader()
    csvwriter.writerows(transformed_data)

table_ref = f"{client.project}.{table_id}"
job_config = bigquery.LoadJobConfig(
        schema=[
        bigquery.SchemaField("Index", "integer"),
        bigquery.SchemaField("User Id", "string"),
        bigquery.SchemaField("First Name", "string"),
        bigquery.SchemaField("Last Name", "string"),
        bigquery.SchemaField("Sex", "string"),
        bigquery.SchemaField("Email", "string"),
        bigquery.SchemaField("Phone", "string"),
        bigquery.SchemaField("Date of birth", "date"),
        bigquery.SchemaField("Job Title", "string")       
    ],
    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV
)

with open(transformed_date_file, 'rb') as source_file:
    load_job = client.load_table_from_file(
        source_file,
        table_ref,
        job_config=job_config
    )

load_job.result()  # Wait for the job to complete
print(f"Data loaded into {table_ref}")


    # old_date = datetime.strptime(row[7],"%Y-%m-%dT%H:%M:%S")
    # new_date = old_date.strftime('%d/%m/%Y')
    # row[7] = new_date
    # print(row[7])

# with open(transformed_date_file, 'w', newline='') as csvfile:
#     fieldnames = ['ISODateTimeUTC', 'Lat', 'Lon', 'SOG', 'COG', 'TWD', 'TWA', 'VMG']
#     csvwriter = csv.DictWriter(csvfile, fieldnames=fieldnames)
#     csvwriter.writeheader()
#     csvwriter.writerows(data)

# print("Date transformation complete.")
