# schema validation method: test out behaviour if send in a valid dataframe (correct types) - happy path - could have an assert saying the result is an empty list

import pytest
import pandas as pd
from my_modules import validate_schema

@pytest.fixture
def df():
    return pd.DataFrame({
        "legal_entity": ["OLPPP244", "XY88Z"],
        "counter_party": ["DEFTT332", "GH56I"],
        "sum_value_ARAP_status": [1.23378, 224.526],
        "sum_value_ACCR_status": [7.83429, 1320.11]
    })

def test_validate_schema_happy_path(df):
    result = validate_schema(df)
    assert result == []

