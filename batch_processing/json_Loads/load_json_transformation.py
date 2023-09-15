import google.cloud
from google.cloud import bigquery
from file_names_config import transformed_json_output_file, table_id_json_transformation_load, schema_transformation_load, file_for_transformation_load
import io
import logging
import pandera as pa
import pandas as pd
import json

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
    schema=schema_transformation_load,
    create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    return job_config 

def transform_date_column_to_correct_format(file_for_transformation_load, transformed_json_output_file):
    try:
        df = pd.read_json(file_for_transformation_load, lines=True)
        df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
        df.to_json(transformed_json_output_file, orient='records', lines=True)
        logging.info(f"JSON data writing completed successfully.")
        return df  # Return the DataFrame
    except Exception as e:
        logging.error(f"An error occurred while transforming the data: {str(e)}")
        return pd.DataFrame()  # Return an empty DataFrame in case of an error


def validate_schema(df):
    try:
        schema = pa.DataFrameSchema({
            "Name": pa.Column(pa.String, required=True, nullable=False),
            "Age": pa.Column(pa.Int, required=True, nullable=False),
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

def execute_single_load(file_for_transformation_load, file_path, table_id, client):        
        try:
            transform_date_column_to_correct_format(
                file_for_transformation_load, transformed_json_output_file
                )
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
    file_path = transformed_json_output_file
    table_id = table_id_json_transformation_load
    execute_single_load(file_for_transformation_load, file_path, table_id, client)

