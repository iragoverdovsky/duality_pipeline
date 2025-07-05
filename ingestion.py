
from loader.KafkaLoader import KafkaLoader
from loader.ParquetLoader import ParquetLoader
from loader.PostgresLoader import PostgresLoader
from loader.ApiLoader import ApiLoader
from loader.CsvLoader import CsvLoader

conf = {
        "csv" : {"csv_path":"data/transactions.csv",
                 "chunksize": 10000},

        "parquet": {"path": ""},
        "kafka": {"topic": "irena",
                   "bootstrap_servers": "127.0.0.1:9092"},
        "api": {
            "url": "https://temp.co.il",
            "headers": ""}
        ,
        "postgres": {
            "host": "localhost",
            "port":"123",
            "dbname": "temp",
            "user":"admin",
            "password":"password",
            "schema":"main",
            "query":"SELECT 1"

        }
        }

SOURCE_LOADERS = {
    "csv": CsvLoader(conf),
    "parquet": ParquetLoader(conf),
    "postgres": PostgresLoader(conf),
    "api": ApiLoader(conf),
    "kafka": KafkaLoader(conf),
}

def get_loader(source_type):

    loader = SOURCE_LOADERS.get(source_type)
    if not loader:
        raise ValueError(f"Unsupported source_type: {source_type}")
    return loader