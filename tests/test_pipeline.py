import pytest
import pandas as pd
import numpy as np


from query_engine import QueryEngine
from transformer.transform import Transform

transformer = Transform()
query_engine = QueryEngine()
@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "txn_id": [1, 2, 3],
        "cust_id": [101, 102, 103],
        "City_name": ["Paris", "Berlin", "Madrid"],
        "Country_code": ["2", "3", None],
        "Tx_time": ["2023-08-01 12:34:56", "2023-07-01 10:00:00", "2022-06-01 09:00:00"]
    })

@pytest.fixture
def mapping_config():
    return {
        "column_names": {
            "txn_id": "transaction_id",
            "cust_id": "customer_id",
            "City_name": "city",
            "Country_code": "country",
            "Tx_time": "time_of_transaction"
        },
        "countries": {
            "2": "FR",
            "3": "DE",
            "4": "UK"
        }
    }

@pytest.fixture
def schema():
    return {
        "transaction_id": "string",
        "customer_id": "string",
        "city": "string",
        "country": "string",
        "time_of_transaction": "datetime"
    }

@pytest.fixture
def duality_query():
    return {
        "retrieve_fields": ["customer_id", "city", "time_of_transaction"],
        "table": "transactions",
        "predicates": [
            {
                "field_name": "time_of_transaction",
                "operator": "last_time_period",
                "time_period_type": "year",
                "value": 1
            },
            {
                "field_name": "country",
                "operator": "eq",
                "value": None
            }
        ]
    }

def test_transform(sample_df: pd.DataFrame, mapping_config : dict, schema: dict):
    _df = transformer.map_and_transform(sample_df)
    transformed_df = _df.replace(to_replace=[None], value=np.nan)
    assert "transaction_id" in transformed_df.columns
    assert transformed_df["country"].iloc[0] == "FR"
    assert pd.api.types.is_datetime64_any_dtype(transformed_df["time_of_transaction"])

def test_query_filtering(sample_df: pd.DataFrame, mapping_config: dict, schema: dict, duality_query: dict):
    df = transformer.map_and_transform(sample_df)
    filtered_df = query_engine.apply_duality_query(df)
    assert set(filtered_df.columns) == set(duality_query["retrieve_fields"])


