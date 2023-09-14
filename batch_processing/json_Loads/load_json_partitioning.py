import google.cloud
from google.cloud import bigquery
from file_names_config import file_for_partitioning_load, table_id_json_partitioning_load, schema
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

time_partitioning = bigquery.TimePartitioning(
        type_= bigquery.TimePartitioningType.YEAR,
        field= 'Date'
)

def configure_job_config(file_content):
    job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    schema=schema,
    time_partitioning=time_partitioning,
    create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
    )
    return job_config

def validate_schema(df):
    try:
        schema = pa.DataFrameSchema({
            "Destination": pa.Column(pa.String, required=True, nullable=False),
            "Airline": pa.Column(pa.String, required=True, nullable=False),
            "Date": pa.Column(pa.DateTime, required=True, nullable=False),
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
            logging.info(job_config.create_disposition)
            df = pd.read_json(io.BytesIO(file_content), lines=True)
            print(df.dtypes)
            validate_schema(df)
            job = load_data_to_bq(client, file_content, table_id, job_config)
            logging.info(f"Data loaded successfully to table {table_id}!")
        except Exception as e:
            logging.error(f"Error occurred in loading data to table {table_id}: {e}")
            

if __name__ == "__main__":
    client = bigquery.Client()
    file_path = "/Users/sabahhussain/learning_development/batch_processing/json_files/flight_dates.json"
    table_id = "dt-sabah-sandbox-dev.load_json_partitioning.Flight_Dates"
    execute_single_load(file_path, table_id, client)

