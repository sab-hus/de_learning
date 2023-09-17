import google.cloud
from google.cloud import bigquery
from file_names_config import file_for_schema_load, table_id_json_schema_load, json_schema_load
import io
import logging
import pandera as pa
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def read_json_file(file_path):
    with open(file_path, "rb") as source_file:
        return source_file.read()
    
def configure_job_config(file_content):
    job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    schema=json_schema_load)
    return job_config

def validate_schema(df):
    try:
        schema = pa.DataFrameSchema({
            "Name": pa.Column(pa.String, required=True, nullable=True),
            "Age": pa.Column(pa.Int, required=True, nullable=True)
        })
        schema.validate(df, lazy=True)
        logging.info("Validation successful for all columns.")
    except pa.errors.SchemaErrors as err:
        logging.error(f"Dataframe validation failed. Errors:\n{err.failure_cases}")
        logging.error(f"Invalid data rows:\n{err.data}")

def load_data_to_bq(client, file_content, table_id, job_config):
    try:
        file_stream = io.BytesIO(file_content)
        job=client.load_table_from_file(
            file_stream,
            table_id,
            job_config=job_config)
        job.result()
    except google.api_core.exceptions.GoogleAPIError as e:
        logging.error(f"An error occurred while loading data to BigQuery: {str(e)}")
    except Exception as e:
            # Handle other exceptions
        logging.error(f"An unexpected error occurred: {str(e)}")

def execute_single_load(file_path, table_id, client):
        try:
            file_content = read_json_file(file_path)
            job_config = configure_job_config(file_content)
            df = pd.read_json(io.BytesIO(file_content), lines=True)
            validate_schema(df)
            job = load_data_to_bq(client, file_content, table_id, job_config)
            logging.info(f"Data loaded successfully to table {table_id}!")
        except Exception as e:
            logging.error(f"Error occurred in loading data to table {table_id}: {e}")
            

if __name__ == "__main__":
    client = bigquery.Client()
    file_path = file_for_schema_load
    table_id = table_id_json_schema_load
    execute_single_load(file_path, table_id, client)
