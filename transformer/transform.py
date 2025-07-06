import json
import os

import pandas as pd
absolute_path = os.path.dirname(__file__)
conf_path = os.path.join(absolute_path, '../config/')

class Transform:

    def __init__(self):
        self.mapping_config = json.load(open(conf_path+"country_code_mapping.json"))
        self.schema_json = json.load(open(conf_path+"duality_schema.json"))
        self.query = json.load(open(conf_path+"duality_query.json"))



    def extract_schema_for_table( self):
        for schema in self.schema_json["duality_schemas"]:
            if schema["name"] == self.query["table"]:
                return schema["table_schema"]
        raise ValueError(f"No schema found for table: {self.query['table']}")

    def map_and_transform(self, df)-> pd.DataFrame:
        schema = self.extract_schema_for_table()
        column_mapping = self.mapping_config["column_names"]
        df = df.rename(columns=column_mapping)

        country_map = self.mapping_config["countries"]
        if "country" in df.columns:
            df["country"] = df["country"].astype(pd.Int64Dtype()).astype(str).map(country_map)

        for col, dtype in schema.items():
            if col not in df.columns:
                continue
            if dtype == "string":
                df[col] = df[col].astype(str)
            elif dtype in ["datetime", "timestamp"]:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

        return df
