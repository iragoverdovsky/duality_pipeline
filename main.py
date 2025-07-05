import pandas as pd
from pandas import DataFrame

from ingestion import  get_loader
from loader.BaseLoader import BaseLoader

from query_engine import QueryEngine
from logger import get_logger

logger = get_logger()

# could be inject from external sources
#source_config = {"source_type":"csv"}


def run_pipeline(source_type = "csv"):
    logger.info("Starting Duality Pipeline")
    #source_type = _source_type if _source_type else source_config["source_type"]
    loader = get_loader(source_type)
    query_engine = QueryEngine()

    df : pd.DataFrame = loader.load()

    df : pd.DataFrame= query_engine.apply_duality_query(df)
    logger.info(f"Filtered to {len(df)} rows after Duality query")

    df.to_json("output/duality_output.json", orient="records", lines=True)
    logger.info("Pipeline complete. Output written to duality_output.json")

if __name__ == "__main__":
    run_pipeline()
