from google.cloud import bigquery

def load_csv_to_bq(file_path, table_id):
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        source_format = bigquery.SourceFormat.CSV, 
        skip_leading_rows = 1,
        autodetect = True,
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
        job.result()

file_table_pairs = [
    ("/Users/sabahhussain/learning_development/CSV_files/Planets.csv", "dt-sabah-sandbox-dev.dummy_dataset.Planets"),
    ("/Users/sabahhussain/learning_development/CSV_files/nutrition.csv", "dt-sabah-sandbox-dev.dummy_dataset.Nutrition"),
    ("/Users/sabahhussain/learning_development/CSV_files/WorldPopulation2023.csv", "dt-sabah-sandbox-dev.dummy_dataset.World_Population"),
    ("/Users/sabahhussain/learning_development/CSV_files/earning.csv", "dt-sabah-sandbox-dev.dummy_dataset.Earnings"),
    ("/Users/sabahhussain/learning_development/CSV_files/efw_cc.csv", "dt-sabah-sandbox-dev.dummy_dataset.Economic_Freedom_of_the_World"),
    ("/Users/sabahhussain/learning_development/CSV_files/significant_earthquakes.csv", "dt-sabah-sandbox-dev.dummy_dataset.Significant_Earthquakes"),
    ("/Users/sabahhussain/learning_development/CSV_files/SampleforOrders.csv", "dt-sabah-sandbox-dev.dummy_dataset.Orders"),
    ("/Users/sabahhussain/learning_development/CSV_files/mock_orders.csv", "dt-sabah-sandbox-dev.dummy_dataset.mock_orders"),
    ("/Users/sabahhussain/learning_development/CSV_files/njord_csv_sample.csv", "dt-sabah-sandbox-dev.dummy_dataset.njord_csv_sample"),
    ("/Users/sabahhussain/learning_development/CSV_files/people_100.csv", "dt-sabah-sandbox-dev.dummy_dataset.people_sample"),
    ("/Users/sabahhussain/learning_development/CSV_files/people_account_type.csv", "dt-sabah-sandbox-dev.data_for_joins.people_account_types"),
    ("/Users/sabahhussain/learning_development/CSV_files/people_credit_check.csv", "dt-sabah-sandbox-dev.data_for_joins.people_credit_check")
    ]

for file_path, table_id in file_table_pairs:
    load_csv_to_bq(file_path, table_id)
