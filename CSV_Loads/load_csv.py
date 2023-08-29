from google.cloud import bigquery

client = bigquery.Client()

table_id_1 = "dt-sabah-sandbox-dev.dummy_dataset.Planets"
table_id_2 = "dt-sabah-sandbox-dev.dummy_dataset.Nutrition"
table_id_3 = "dt-sabah-sandbox-dev.dummy_dataset.World_Population"
table_id_4 = "dt-sabah-sandbox-dev.dummy_dataset.Earnings"
table_id_5 = "dt-sabah-sandbox-dev.dummy_dataset.Economic_Freedom_of_the_World"
table_id_6 = "dt-sabah-sandbox-dev.dummy_dataset.Significant_Earthquakes"
table_id_7 = "dt-sabah-sandbox-dev.dummy_dataset.Orders"
table_id_8 = "dt-sabah-sandbox-dev.dummy_dataset.mock_orders"
table_id_9 = "dt-sabah-sandbox-dev.dummy_dataset.njord_csv_sample"
table_id_10 = "dt-sabah-sandbox-dev.dummy_dataset.people_sample"
table_id_11 = "dt-sabah-sandbox-dev.data_for_joins.people_account_types"
table_id_12 = "dt-sabah-sandbox-dev.data_for_joins.people_credit_check"

file_path_1 = "/Users/sabahhussain/learning_development/CSV_files/Planets.csv"
file_path_2 = "/Users/sabahhussain/learning_development/CSV_files/nutrition.csv"
file_path_3 = "/Users/sabahhussain/learning_development/CSV_files/WorldPopulation2023.csv"
file_path_4 = "/Users/sabahhussain/learning_development/CSV_files/earning.csv"
file_path_5 = "/Users/sabahhussain/learning_development/CSV_files/efw_cc.csv"
file_path_6 = "/Users/sabahhussain/learning_development/CSV_files/significant_earthquakes.csv"
file_path_7 = "/Users/sabahhussain/learning_development/CSV_files/SampleforOrders.csv"
file_path_8 = "/Users/sabahhussain/learning_development/CSV_files/mock_orders.csv"
file_path_9 = "/Users/sabahhussain/learning_development/CSV_files/njord_csv_sample.csv"
file_path_10 = "/Users/sabahhussain/learning_development/CSV_files/people_100.csv"
file_path_11 = "/Users/sabahhussain/learning_development/CSV_files/people_account_type.csv"
file_path_12 = "/Users/sabahhussain/learning_development/CSV_files/people_credit_check.csv"


job_config = bigquery.LoadJobConfig(
    source_format = bigquery.SourceFormat.CSV, 
    skip_leading_rows = 1,
    autodetect = True,
    write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
)

with open(file_path_1, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id_1, job_config=job_config)
    job.result()

with open(file_path_2, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id_2, job_config=job_config)
    job.result()

with open(file_path_3, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id_3, job_config=job_config)
    job.result()

with open(file_path_4, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id_4, job_config=job_config)
    job.result()

with open(file_path_5, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id_5, job_config=job_config)
    job.result()

with open(file_path_6, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id_6, job_config=job_config)
    job.result()

with open(file_path_7, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id_7, job_config=job_config)
    job.result()

with open(file_path_8, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id_8, job_config=job_config)
    job.result()

with open(file_path_9, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id_9, job_config=job_config)
    job.result()

with open(file_path_10, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id_10, job_config=job_config) 
    job.result()

with open(file_path_11, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id_11, job_config=job_config)
    job.result()

with open(file_path_12, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id_12, job_config=job_config)    
    job.result()
