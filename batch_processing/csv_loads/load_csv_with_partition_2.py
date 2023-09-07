from google.cloud import bigquery
from file_names import input_file, table_path_2
import io
import logging

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
        create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="billed_date"
        )
    )
    return job_config

def load_data_to_bq(client, file_content, table_id, job_config):
    file_stream = io.BytesIO(file_content)
    job=client.load_table_from_file(file_stream, table_id, job_config=job_config)
    job.result()
    return job

# think about reasoning behind why using certain code structure - why using certain functions from library, how the flow works
def execute_single_load(file_path, table_id, client):
        try:
            file_content = read_csv_file(file_path)
            job_config = configure_job_config(file_content)
            job = load_data_to_bq(client, file_content, table_id, job_config)
            logging.info(f"Data loaded successfully to table {table_id}!")
        except Exception as e:
            logging.error(f"Error occured in loading data to table {table_id}: {e}")

if __name__ == "__main__":
    client = bigquery.Client()
    file_path = input_file
    table_id = table_path_2 
    execute_single_load(file_path, table_id, client)
