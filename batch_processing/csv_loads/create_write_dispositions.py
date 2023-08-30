from google.cloud import bigquery
import os

client = bigquery.Client()

table_id_1 = "dt-sabah-sandbox-dev.load_create_and_write_dispositions.Planets"
table_id_2 = "dt-sabah-sandbox-dev.load_create_and_write_dispositions.Nutrition"
table_id_3 = "dt-sabah-sandbox-dev.load_create_and_write_dispositions.World_Population"

file_path_1 = "/Users/sabahhussain/learning_development/batch_processing/csv_files/Planets.csv"
file_path_2 = "/Users/sabahhussain/learning_development/batch_processing/csv_files/nutrition.csv"
file_path_3 = "/Users/sabahhussain/learning_development/batch_processing/csv_files/WorldPopulation2023.csv"

job_config = bigquery.LoadJobConfig(
    source_format = bigquery.SourceFormat.CSV, 
    skip_leading_rows = 1,
    autodetect = True,
    #default create_disposition = "CREATE_IF_NEEDED",
    # create_disposition = "CREATE_NEVER",
    write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    # write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    #default write_disposition = bigquery.WriteDisposition.WRITE_EMPTY


)

with open(file_path_1, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id_1, job_config=job_config)

with open(file_path_2, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id_2, job_config=job_config)

with open(file_path_3, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id_3, job_config=job_config)

job.result()