import pandas as pd
import pandera as pa
import logging

logging.basicConfig(level=logging.INFO)

def convert_data_types(input_file, output_file):
    try:
        df = pd.read_csv(input_file)
        
        conversion_errors = []
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

        if conversion_errors:
            logging.error(f"Errors occurred while converting data type in rows: {conversion_errors}")
        else:
            logging.info("Data type conversion and CSV saved successful.")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

input_file = 'mock_orders.csv'
output_file = 'mock_orders_updated.csv'
convert_data_types(input_file, output_file)

# def validate_mock_orders_schema(csv_file_path):
#     try:
#         df = pd.read_csv(csv_file_path)
#         schema = pa.DataFrameSchema({
#             "contribution": pa.Column(pa.Float, required=True, nullable=False),
#             "billed_date": pa.Column(pa.DateTime, required=True, nullable=False),
#             "order_id": pa.Column(pa.String, required=True, nullable=False)
#         })
#         schema.validate(df, lazy=True)
#         logging.info("Validation successful for all columns.")    
#         # return validated df
    
#     except pa.errors.SchemaErrors as err:
#         logging.error(f"Dataframe validation failed. Errors:\n{err.failure_cases}")
#         logging.error(f"Invalid data rows:\n{err.data}")

# csv_file_path = "mock_orders_updated.csv"
# validated_df = validate_mock_orders_schema(csv_file_path)
