import json
import os
from pathlib import Path
from typing import List, Optional

import pandas as pd

absolute_path = os.path.dirname(__file__)
conf_path =  absolute_path + '/config/'

class QueryEngine:
    """
    Apply the “duality” query defined in a JSON file to a pandas DataFrame.

    Supported operators
    -------------------
    - last_time_period  { "time_period_type": "year" | "month" | "day",
                          "value": <int> }
    - eq                { "value": <any-scalar> | null }
    - neq               { "value": <any-scalar> }
    """

    def __init__(
        self,
        config_path: str | Path = conf_path + "duality_query.json",
        *,
        utc_now: Optional[pd.Timestamp] = None,
    ) -> None:
        config_path = Path(config_path)
        if not config_path.is_file():
            raise FileNotFoundError(
                f"Query-definition not found at {config_path.resolve()}"
            )

        with config_path.open(encoding="utf-8") as fp:
            self.query: dict = json.load(fp)

        self._now = utc_now or pd.Timestamp.utcnow()

    # --------------------------------------------------------------------- #
    # public                                                                #
    # --------------------------------------------------------------------- #
    def apply_duality_query(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return a new DataFrame filtered according to the JSON predicates."""
        if "predicates" not in self.query:
            return self._maybe_select_fields(df)

        # Work on a copy so we never mutate the caller’s DataFrame in place.
        out = df.copy(deep=False)

        for pred in self.query["predicates"]:
            field = pred["field_name"]
            operator = pred["operator"]

            if field not in out.columns:
                raise KeyError(f"Column '{field}' not found in DataFrame")

            if operator == "last_time_period":
                out = self._filter_last_time_period(out, field, pred)
            elif operator == "eq":
                out = self._filter_eq(out, field, pred.get("value"))
            elif operator == "neq":
                out = out[out[field] != pred.get("value")]
            else:
                raise ValueError(f"Unsupported operator: {operator!r}")

        return self._maybe_select_fields(out)

    # ------------------------------------------------------------------ #
    # helpers (private)                                                  #
    # ------------------------------------------------------------------ #
    def _filter_last_time_period(
        self, df: pd.DataFrame, field: str, pred: dict
    ) -> pd.DataFrame:
        value = pred.get("value")
        if value is None:
            raise ValueError("'last_time_period' predicate requires a 'value'")

        period_type = pred.get("time_period_type")
        offset_map = {
            "year": dict(years=value),
            "month": dict(months=value),
            "day": dict(days=value),
        }
        if period_type not in offset_map:
            raise ValueError(f"Unsupported time_period_type: {period_type!r}")

        threshold = self._now - pd.DateOffset(**offset_map[period_type])

        # Ensure the column is datetime-like so the comparison works
        col = pd.to_datetime(df[field], errors="coerce")
        return df[col >= threshold]

    @staticmethod
    def _filter_eq(df: pd.DataFrame, field: str, value) -> pd.DataFrame:
        """Equality that treats None/NaN consistently."""
        if value is None:
            df_1 = df[field].isna()
            df_2 = df[field].eq('nan')
            return df[df_1 | df_2]
        return df[df[field] == value]

    def _maybe_select_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Restrict to the requested columns, if any."""
        fields: List[str] | None = self.query.get("retrieve_fields")
        if fields:
            missing = set(fields) - set(df.columns)
            if missing:
                raise KeyError(f"retrieve_fields missing in DataFrame: {missing}")
            return df[fields]
        return df
