import logging
from google.cloud import bigquery

def generate_file_table_pairs(*args):
    file_table_pairs = []
    for file, table in args:
        file_table_pairs.append((
            f"/Users/sabahhussain/learning_development/batch_processing/csv_files/{file}",
            f"dt-sabah-sandbox-dev.{table}"
        ))
    return file_table_pairs
# add if else to remove the 'dummy_dataset' and 'data_for_joins'

file_table_pairs = generate_file_table_pairs(
    ("Planets.csv", "dummy_dataset.Planets"),
    ("nutrition.csv", "dummy_dataset.Nutrition"),
    ("WorldPopulation2023.csv", "dummy_dataset.World_Population"),
    ("earning.csv", "dummy_dataset.Earnings"),
    ("efw_cc.json", "dummy_dataset.Economic_Freedom_of_the_World"),
    ("significant_earthquakes.csv", "dummy_dataset.Significant_Earthquakes"),
    ("SampleforOrders.csv", "dummy_dataset.Orders"),
    ("mock_orders.csv", "dummy_dataset.mock_orders"),
    ("njord_csv_sample.csv", "dummy_dataset.njord_csv_sample"),
    ("people_100.csv", "dummy_dataset.people_sample"),
    ("people_account_type.csv", "data_for_joins.people_account_types"),
    ("people_credit_check.csv", "data_for_joins.people_credit_check")
)

# manual inputs and manual values in envion/config files: separate out codebase from the parameters you need
input_csv = "/Users/sabahhussain/learning_development/batch_processing/csv_files/transformation_dataset1.csv"
untransformed_input_csv = "/Users/sabahhussain/learning_development/batch_processing/csv_files/transformation_dataset1.csv"
transform_data_file_2 = "/Users/sabahhussain/learning_development/batch_processing/csv_files/transformation_dataset2.csv"
transformed_file_1 = "/Users/sabahhussain/learning_development/batch_processing/csv_loads/transform_1.csv"
transformed_output_csv = "/Users/sabahhussain/learning_development/batch_processing/csv_loads/transform_2.csv"
transformed_table_path = "dt-sabah-sandbox-dev.load_with_schema.transformed_csv"

schema = [
    bigquery.SchemaField("legal_entity", "STRING"),
    bigquery.SchemaField("counter_party", "STRING"),
    bigquery.SchemaField("sum_value_ARAP_status", "FLOAT64"),
    bigquery.SchemaField("sum_value_ACCR_status", "FLOAT64"),
]

input_file = 'mock_orders.csv'
output_file = 'mock_orders_updated.csv'
table_path = "dt-sabah-sandbox-dev.load_with_partitioning.mock_orders_year"
table_path_2 = "dt-sabah-sandbox-dev.load_with_partitioning.mock_orders_month"
