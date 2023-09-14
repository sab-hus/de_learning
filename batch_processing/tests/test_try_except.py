# test error handling try catch 
import pytest
import pandas as pd
import pandera as pa
from my_modules import validate_schema

def test_validate_schema_datatype_error_check():
    invalid_df = pd.DataFrame({
        "legal_entity": ["DFEW"],
        "counter_party": ["DEF"],
        "sum_value_ARAP_status": ["invalid_float"],
        "sum_value_ACCR_status": [400.1]
    })
    errors = validate_schema(invalid_df) 
    assert len(errors) == 1

def test_validate_schema_missing_column():
    invalid_df = pd.DataFrame({
        "legal_entity": ["DFEW"],
        "sum_value_ARAP_status": [5.221],
        "sum_value_ACCR_status": [400.1]
    })
    errors = validate_schema(invalid_df) 
    assert len(errors) == 1
