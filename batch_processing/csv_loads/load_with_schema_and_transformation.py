import pandas as pd
import google
from google.cloud import bigquery
import logging
import io
import pandera as pa
from file_names_config import untransformed_input_csv, transformed_output_csv, transformed_table_path, schema

logging.basicConfig(level=logging.INFO)

def read_csv_file(file_path):
    with open(file_path, "rb") as source_file:
        return source_file.read()

def configure_job_config():
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    return job_config

def add_sum_value_each_status_groupby_legal_entity_counter_party(untransformed_input_csv, transformed_output_csv):
    try:
        df = pd.read_csv(untransformed_input_csv)
        aggregated_df_1 = df[df['status'] == 'ARAP'].groupby(['legal_entity', 'counter_party'])['value'].sum()
        aggregated_df_2 = df[df['status'] == 'ACCR'].groupby(['legal_entity', 'counter_party'])['value'].sum()
        aggregated_df_1 = aggregated_df_1.to_frame(name='sum_value_ARAP_status').reset_index()
        aggregated_df_2 = aggregated_df_2.to_frame(name='sum_value_ACCR_status').reset_index()
        aggregated_df = aggregated_df_1.merge(aggregated_df_2, on=['legal_entity', 'counter_party'], how='outer')
        aggregated_df.to_csv(transformed_output_csv, index=False)
        logging.info(f"Data aggregation and CSV writing completed successfully.")
        return aggregated_df  # Return the DataFrame
    except Exception as e:
        logging.error(f"An error occurred in add_sum_value_each_status_legal_entity: {str(e)}")
        return pd.DataFrame()  # Return an empty DataFrame in case of an error

def validate_schema(df):
    try:
        schema = pa.DataFrameSchema({
            "legal_entity": pa.Column(pa.String, required=True, nullable=True),
            "counter_party": pa.Column(pa.String, required=True, nullable=True),
            "sum_value_ARAP_status": pa.Column(pa.Float64, required=True, nullable=True),
            "sum_value_ACCR_status": pa.Column(pa.Float64, required=True, nullable=True)
        })
        schema.validate(df, lazy=True)
        logging.info("Validation successful for all columns.")
    except pa.errors.SchemaErrors as err:
        logging.error(f"Dataframe validation failed. Errors:\n{err.failure_cases}")
        logging.error(f"Invalid data rows:\n{err.data}")

def load_data_to_bq(client, file_content, table_id, job_config):
    try:
        file_stream = io.BytesIO(file_content)
        job = client.load_table_from_file(file_stream, table_id, job_config=job_config)
        job.result()
    except google.api_core.exceptions.GoogleAPIError as e:
        # Handle BigQuery API errors
        logging.error(f"An error occurred while loading data to BigQuery: {str(e)}")
    except Exception as e:
        # Handle other exceptions
        logging.error(f"An unexpected error occurred: {str(e)}")

def execute_single_load(untransformed_input_csv, file_path, table_id, client):
    try:
        add_sum_value_each_status_groupby_legal_entity_counter_party(
            untransformed_input_csv, transformed_output_csv
        )
        file_content = read_csv_file(file_path)
        job_config = configure_job_config(file_content)
        df = pd.read_csv(io.BytesIO(file_content))
        validate_schema(df)
        job = load_data_to_bq(client, file_content, table_id, job_config)
        logging.info(f"Data loaded successfully to table {table_id}!")
    except Exception as e:
        logging.error(f"Error occurred in loading data to table {table_id}: {e}")

if __name__ == "__main__":
    input_csv = untransformed_input_csv
    file_path = transformed_output_csv
    table_id = transformed_table_path
    client = bigquery.Client()
    execute_single_load(input_csv, file_path, table_id, client)

