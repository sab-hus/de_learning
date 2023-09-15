import logging
import pandera as pa

def validate_schema(df):
    try:
        schema = pa.DataFrameSchema({
            "legal_entity": pa.Column(pa.String, required=True, nullable=False),
            "counter_party": pa.Column(pa.String, required=True, nullable=False),
            "sum_value_ARAP_status": pa.Column(pa.Float64, required=True, nullable=True),
            "sum_value_ACCR_status": pa.Column(pa.Float64, required=True, nullable=True)
        })
        schema.validate(df, lazy=True)
        logging.info("Validation successful for all columns.")
        return []
    except pa.errors.SchemaErrors as err:
        logging.error(f"Dataframe validation failed. Errors:\n{err.failure_cases}")
        logging.error(f"Invalid data found in:\n{err.data}")
        return err.failure_cases