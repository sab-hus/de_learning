# schema validation method: test out behaviour if send in a valid dataframe (correct types) - happy path - could have an assert saying the result is an empty list

import pytest
import pandas as pd
from my_modules import validate_schema

@pytest.fixture
def valid_df():
    return pd.DataFrame({
        "legal_entity": ["OLPPP244", "XY88Z"],
        "counter_party": ["DEFTT332", "GH56I"],
        "sum_value_ARAP_status": [1.23378, 224.526],
        "sum_value_ACCR_status": [7.83429, 1320.11]
    })

def test_validate_schema_happy_path(valid_df):
    errors = validate_schema(valid_df)
    assert errors == []

def test_validate_schema_datatype_error_check():
    invalid_df = pd.DataFrame({
        "legal_entity": ["DFEW"],
        "counter_party": ["DEF"],
        "sum_value_ARAP_status": ["invalid_float"],
        "sum_value_ACCR_status": [400.1]
    })
    actual_errors = validate_schema(invalid_df) 
    expected_errors = pd.DataFrame({
        "schema_context": ["Column"],
        "column": ["sum_value_ARAP_status"],
        "check": ["dtype('float64')"],
        "check_number": [None],
        "failure_case": ["object"],
        "index": [None]
    })
    assert actual_errors.equals(expected_errors)

def test_validate_schema_missing_column():
    invalid_df = pd.DataFrame({
        "legal_entity": ["DFEW"],
        "sum_value_ARAP_status": [5.221],
        "sum_value_ACCR_status": [400.1]
    })
    expected_errors = pd.DataFrame({
        "schema_context": ["DataFrameSchema"],
        "column": [None],
        "check": ["column_in_dataframe"],
        "check_number": [None],
        "failure_case": ["counter_party"],
        "index": [None]
    })
    # print(expected_errors.columns)
    actual_errors = validate_schema(invalid_df) 
    print(actual_errors.compare(expected_errors))

    assert actual_errors.equals(expected_errors)

