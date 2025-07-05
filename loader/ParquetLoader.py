import logging
from pathlib import Path
from typing import List
import pyarrow.dataset as ds
import pandas as pd

from loader.BaseLoader import BaseLoader
from transformer.transform import Transform

logger = logging.getLogger(__name__)

DEFAULT_THRESHOLD = 5_000_000  # rows
DEFAULT_MEMORY_MB = 1024
CHUNKSIZE_SQL = 1_000_000
PROGRESS_EVERY = 10

class ParquetLoader(BaseLoader):
    def __init__(self, config: dict):
        super().__init__(config)
        self.pq_cfg = config.get("parquet", {})
        self.path = self.pq_cfg.get("path")
        self.rows_threshold: int = int(self.pq_cfg.get("rows_threshold", DEFAULT_THRESHOLD))
        self.memory_limit_mb: int = int(self.pq_cfg.get("memory_limit_mb", DEFAULT_MEMORY_MB))
        self.progress_every: int = int(self.pq_cfg.get("progress_every", PROGRESS_EVERY))
        self.transformer = Transform()

    def load(self)-> pd.DataFrame:
        try:
            self.path = Path(self.pq_cfg["path"]).expanduser()
        except KeyError as exc:
            raise ValueError("'path' is required in parquet config") from exc
        df = self.load_parquet()
        if df.empty:
            return pd.DataFrame()
        return df

    def load_parquet(self) -> pd.DataFrame:  # noqa: D401
            """Return the full Parquet dataset as a transformed DataFrame."""
            dataset = ds.dataset(str(self.path))  # type: ignore[arg-type]
            n_rows = dataset.count_rows()
            eager_possible = (
                    n_rows < self.rows_threshold and self._fits_memory_estimate(n_rows)
            )

            if eager_possible:
                logger.info("ParquetLoader eager mode (rows=%d)", n_rows)
                table = dataset.to_table()
                df = table.to_pandas()
                return self.transformer.map_and_transform(df)

            logger.info(
                "ParquetLoader streaming with record batches; rows≈%d rows_threshold=%d",
                n_rows,
                self.rows_threshold,
            )
            return self._stream_and_concat(dataset)

        # ------------------------------------------------------------------ #
        # helpers                                                            #
        # ------------------------------------------------------------------ #
    def _stream_and_concat(self, dataset: ds.Dataset) -> pd.DataFrame:  # type: ignore[name-defined]
            parts: List[pd.DataFrame] = []
            for i, batch in enumerate(dataset.to_batches(), start=1):
                df_chunk = batch.to_pandas()
                transformed = self.transformer.map_and_transform(df_chunk)
                parts.append(transformed)
                if i % self.progress_every == 0:
                    logger.info(" … processed %d record batches", i)
            result = pd.concat(parts, ignore_index=True)
            logger.info("Finished Parquet streaming. Total rows=%d", len(result))
            return result

    def _fits_memory_estimate(self, n_rows: int) -> bool:
            est_mb = n_rows * 0.5 / 1024
            return est_mb < self.memory_limit_mb

