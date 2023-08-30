from google.cloud import bigquery

def load_csv_to_bq(file_path, table_id):
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        source_format = bigquery.SourceFormat.CSV, 
        skip_leading_rows = 1,
        autodetect = True,
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    with open(file_path, "r") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
        job.result()
        # add error handling

file_table_pairs = [
    ("/Users/sabahhussain/learning_development/batch_processing/csv_files/Planets.csv", "dt-sabah-sandbox-dev.dummy_dataset.Planets"),
    ("/Users/sabahhussain/learning_development/batch_processing/csv_files/nutrition.csv", "dt-sabah-sandbox-dev.dummy_dataset.Nutrition"),
    ("/Users/sabahhussain/learning_development/batch_processing/csv_files/WorldPopulation2023.csv", "dt-sabah-sandbox-dev.dummy_dataset.World_Population"),
    ("/Users/sabahhussain/learning_development/batch_processing/csv_files/earning.csv", "dt-sabah-sandbox-dev.dummy_dataset.Earnings"),
    ("/Users/sabahhussain/learning_development/batch_processing/csv_files/efw_cc.csv", "dt-sabah-sandbox-dev.dummy_dataset.Economic_Freedom_of_the_World"),
    ("/Users/sabahhussain/learning_development/batch_processing/csv_files/significant_earthquakes.csv", "dt-sabah-sandbox-dev.dummy_dataset.Significant_Earthquakes"),
    ("/Users/sabahhussain/learning_development/batch_processing/csv_files/SampleforOrders.csv", "dt-sabah-sandbox-dev.dummy_dataset.Orders"),
    ("/Users/sabahhussain/learning_development/batch_processing/csv_files/mock_orders.csv", "dt-sabah-sandbox-dev.dummy_dataset.mock_orders"),
    ("/Users/sabahhussain/learning_development/batch_processing/csv_files/njord_csv_sample.csv", "dt-sabah-sandbox-dev.dummy_dataset.njord_csv_sample"),
    ("/Users/sabahhussain/learning_development/batch_processing/csv_files/people_100.csv", "dt-sabah-sandbox-dev.dummy_dataset.people_sample"),
    ("/Users/sabahhussain/learning_development/batch_processing/csv_files/people_account_type.csv", "dt-sabah-sandbox-dev.data_for_joins.people_account_types"),
    ("/Users/sabahhussain/learning_development/batch_processing/csv_files/people_credit_check.csv", "dt-sabah-sandbox-dev.data_for_joins.people_credit_check")
    ]
# add to environment file
# create another function to remove the repetition of file path up to file name and dataset ID up to the table name
for file_path, table_id in file_table_pairs:
    load_csv_to_bq(file_path, table_id)

# break the above down for smaller functions: 1. reading the data 2. processing the data (job_config), 3. writing the data into bq
# a final function to execute the 3 functions
# if you need to have an 'and' when talking through your code, you should have another function
# the same follows for commit messages - keep it short and sweet - best practice