from google.cloud import bigquery
from file_names import file_table_pairs
import io
import logging
from schema_validation import convert_data_types, validate_mock_orders_schema

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def read_csv_file(file_path):
    with open(file_path, "rb") as source_file:
        return source_file.read()
    
def configure_job_config(file_content):
    job_config = bigquery.LoadJobConfig(
        source_format = bigquery.SourceFormat.CSV, 
        skip_leading_rows = 1,
        autodetect = True,
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema = validate_mock_orders_schema
    )
    return job_config

def load_data_to_bq(client, file_content, table_id, job_config):
    file_stream = io.BytesIO(file_content)
    job=client.load_table_from_file(file_stream, table_id, job_config=job_config)
    job.result()
    return job

# think about reasoning behind why using certain code structure - why using certain functions from library, how the flow works
def execute_single_load(file_path, table_id, client, schema):
        try:
            file_content = read_csv_file(file_path)
            job_config = configure_job_config(file_content)
            if schema_validation(file_path):
                job = load_data_to_bq(client, file_content, table_id, job_config)
                logging.info(f"Data loaded successfully to table {table_id}!")
            else:
                logging.error(f"Schema validation failed for file {file_path}. Data not loaded.")
        except Exception as e:
            logging.error(f"Error occured in loading data to table {table_id}: {e}")

# def execute_all_pipelines():
#     client = bigquery.Client()
#     try:
#         for file_path, table_id in file_table_pairs:
#                 execute_single_load(file_path, table_id, client)
#     except Exception as main_exception:
#         logging.info(f"An error occurred in the main function: {main_exception}")

if __name__ == "__main__":
    execute_single_load()
