# test error handling try catch - 
# not retuning anythiing from method - try returning list of errors
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

def test_validate_schema_datatype_error_message():
    invalid_df = pd.DataFrame({
        "legal_entity": ["DFEW"],
        "counter_party": ["DEF"],
        "sum_value_ARAP_status": ["invalid_float"],
        "sum_value_ACCR_status": [400.1]
    })
    errors = validate_schema(invalid_df) 
    assert len(errors) == 1
    # Check that the error is for the `sum_value_ARAP_status` column
    assert errors.column == "sum_value_ARAP_status"

   
def test_validate_schema_missing_value():
    invalid_df = pd.DataFrame({
        "legal_entity": ["DFEW"],
        "counter_party": [],
        "sum_value_ARAP_status": [5.221],
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


# # validate that the errors are of the same type, iterate over errors to see if they are what I would 
# incorporate failure_cases into test 
# # match the invalid data rows with what is expected
# # build a few more asserts to ensure picking up on correct errors - do this on a single row that is invalid
# # try identify a number of things i want to test - data type, missing value, missing column
# # name the test cases appropriately