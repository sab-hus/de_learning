import pandas as pd
import pandera as pa
import logging
from file_names import input_file, output_file

logging.basicConfig(level=logging.INFO)

def convert_data_types(input_file, output_file):
    try:
        df = pd.read_csv(input_file)

        conversion_errors = []
        # for loop performance is slower. Big O notation - measure code performance - not optimal choice for larger dataset - not worth for simply carrying out data validation
        # .map .reduce don't present the same issues as in with looping in terms of code performance
        for idx, row in df.iterrows(): 
            try:
                contribution = float(row['contribution'])
                billed_date = pd.to_datetime(row['billed_date'], format='%m/%d/%Y')
                order_id = str(row['order_id'])                
            except (ValueError, TypeError) as e:
                logging.error(f"Error converting data type in row {idx}: {str(e)}")
                conversion_errors.append(idx)

        df['missing_flag'] = df.isna().any(axis=1)

        df.to_csv(output_file, index=False)
# the below is somewhat repetitive and can become very cumbersome when scaling up and working with larger amounts of data
        # if conversion_errors:
        #     logging.error(f"Errors occurred while converting data type in rows: {conversion_errors}")
        # else:
        #     logging.info("Data type conversion and CSV saved successful.")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

convert_data_types(input_file, output_file)

def validate_mock_orders_schema(csv_file_path):
    try:
        # df = pd.read_csv(csv_file_path) warning_on_error instead of hard stopping
        schema = pa.DataFrameSchema({
            "contribution": pa.Column(pa.Float, required=True, nullable=False),
            "billed_date": pa.Column(pa.DateTime, required=True, nullable=False),
            "order_id": pa.Column(pa.String, required=True, nullable=False)
        })
        schema.validate(df, lazy=True)
        logging.info("Validation successful for all columns.")    
        # return validated df
    
    except pa.errors.SchemaErrors as err:
        logging.error(f"Dataframe validation failed. Errors:\n{err.failure_cases}")
        logging.error(f"Invalid data rows:\n{err.data}")

csv_file_path = output_file
validate_mock_orders_schema(csv_file_path)
