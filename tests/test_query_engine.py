"""Pytest unit‑suite for duality_pipeline.query_engine

This single file covers happy‑paths and several edge‑cases so you can run

    pytest -q

from the project root and immediately see green tests.
"""
import json
from pathlib import Path

import pandas as pd
import pytest

from query_engine import QueryEngine

#from duality_pipeline.query_engine import QueryEngine

# ---------------------------------------------------------------------------
# Helpers & fixtures
# ---------------------------------------------------------------------------
NOW = pd.Timestamp("2025-07-05 12:00:00Z")


@pytest.fixture()
def sample_df() -> pd.DataFrame:
    """Three rows with a timestamp, a nullable string column, and a number."""
    return pd.DataFrame(
        {
            "event_time": pd.to_datetime(
                [
                    "2025-07-04",
                    "2025-07-01",
                    "2024-07-05",
                ],
                utc=True,
            ),
            "status": ["open", "closed", None],
            "value": [1, 2, 3],
        }
    )


def _engine(tmp_path: Path, payload: dict) -> QueryEngine:
    """Write *payload* to a temp JSON file and build a QueryEngine bound to *NOW*."""
    qfile = tmp_path / "query.json"
    qfile.write_text(json.dumps(payload))
    return QueryEngine(qfile, utc_now=NOW)


# ---------------------------------------------------------------------------
# Happy‑path tests
# ---------------------------------------------------------------------------

def test_last_time_period_days(sample_df, tmp_path):
    qe = _engine(
        tmp_path,
        {
            "retrieve_fields": [
                "event_time",
                "status",
                "value"
            ],
            "predicates": [
                {
                    "field_name": "event_time",
                    "operator": "last_time_period",
                    "time_period_type": "day",
                    "value": 30,
                }
            ]
        },
    )
    result = qe.apply_duality_query(sample_df)
    assert len(result) == 2
    assert result["event_time"].min() >= NOW - pd.DateOffset(days=30)


def test_eq_null(sample_df, tmp_path):
    qe = _engine(
        tmp_path,
        {
            "predicates": [
                {"field_name": "status", "operator": "eq"}  # value omitted ⇒ null‑eq
            ]
        },
    )
    result = qe.apply_duality_query(sample_df)
    assert result["status"].isna().all()


def test_eq_value(sample_df, tmp_path):
    qe = _engine(
        tmp_path,
        {
            "predicates": [
                {
                    "field_name": "status",
                    "operator": "eq",
                    "value": "open",
                }
            ]
        },
    )
    result = qe.apply_duality_query(sample_df)
    assert (result["status"] == "open").all()
    assert len(result) == 1


def test_neq_value(sample_df, tmp_path):
    qe = _engine(
        tmp_path,
        {
            "predicates": [
                {
                    "field_name": "status",
                    "operator": "neq",
                    "value": "open",
                }
            ]
        },
    )
    result = qe.apply_duality_query(sample_df)
    assert "open" not in result["status"].values


def test_retrieve_fields(sample_df, tmp_path):
    qe = _engine(
        tmp_path,
        {
            "predicates": [],
            "retrieve_fields": ["status", "value"],
        },
    )
    result = qe.apply_duality_query(sample_df)
    assert list(result.columns) == ["status", "value"]


def test_original_df_not_mutated(sample_df, tmp_path):
    df_copy = sample_df.copy(deep=True)
    qe = _engine(tmp_path, {"predicates": []})
    _ = qe.apply_duality_query(sample_df)
    pd.testing.assert_frame_equal(sample_df, df_copy)


# ---------------------------------------------------------------------------
# Edge‑case / failure‑mode tests
# ---------------------------------------------------------------------------

def test_missing_column_raises(sample_df, tmp_path):
    qe = _engine(
        tmp_path,
        {
            "predicates": [
                {
                    "field_name": "nonexistent",
                    "operator": "eq",
                    "value": 1,
                }
            ]
        },
    )
    with pytest.raises(KeyError):
        qe.apply_duality_query(sample_df)


def test_bad_time_period_type(sample_df, tmp_path):
    qe = _engine(
        tmp_path,
        {
            "predicates": [
                {
                    "field_name": "event_time",
                    "operator": "last_time_period",
                    "time_period_type": "quarter",  # unsupported
                    "value": 1,
                }
            ]
        },
    )
    with pytest.raises(ValueError):
        qe.apply_duality_query(sample_df)
