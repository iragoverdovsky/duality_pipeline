import logging
from pathlib import Path
from typing import Optional, List

import pandas as pd

from loader.BaseLoader import BaseLoader
from transformer.transform import Transform

logger = logging.getLogger(__name__)

class CsvLoader(BaseLoader):
    def __init__(self, config: dict):
        super().__init__(config)
        csv_cfg = config.get("csv", {})
        try:
            self.csv_path = Path(csv_cfg["csv_path"]).expanduser()
        except KeyError as exc:
            raise ValueError("'csv_path' is required in csv config") from exc

        if not self.csv_path.is_file():
            raise FileNotFoundError(self.csv_path)

        self.chunksize: int = int(csv_cfg.get("chunksize", 1_000_000))
        self.rows_threshold: int = int(csv_cfg.get("rows_threshold", 5_000_000))
        self.memory_limit_mb: int = int(csv_cfg.get("memory_limit_mb", 1024))
        self.progress_every: int = int(csv_cfg.get("progress_every", 10))

        self.transformer = Transform()

    def load(self)-> pd.DataFrame:
      df =  self.load_csv()
      if df is None:
        return pd.DataFrame()
      return df

    def load_csv(self)-> pd.DataFrame:
            n_rows = self._estimate_rows()
            eager_possible = (
                    n_rows is not None
                    and n_rows < self.rows_threshold
                    and self._fits_memory_estimate(n_rows)
            )

            if eager_possible:
                logger.info("CsvLoader using eager mode (≈%s rows)", n_rows)
                df = pd.read_csv(self.csv_path)
                return self.transformer.map_and_transform(df)

            logger.info(
                "CsvLoader streaming with chunksize=%d (rows≈%s)",
                self.chunksize,
                n_rows or "unknown",
            )
            return self._stream_and_concat()

    def _estimate_rows(self) -> Optional[int]:
        """Try to estimate the number of rows cheaply (returns *None* if unsure)."""
        try:
            # Pandas can quickly count lines using C engine if no quoting
            with self.csv_path.open("rb") as fp:
                return sum(1 for _ in fp) - 1  # minus header
        except Exception:  # pragma: no cover – fall back to unknown
            return None

    def _fits_memory_estimate(self, n_rows: int) -> bool:
        """Rough heuristic: assume 0.5 KB per row as mixed types average."""
        est_mb = n_rows * 0.5 / 1024
        fits = est_mb < self.memory_limit_mb
        logger.debug("Estimated %d MB for %d rows → fits=%s", est_mb, n_rows, fits)
        return fits

    def _stream_and_concat(self) -> pd.DataFrame:
        """Stream chunks, transform, and concatenate.

        Returns
        -------
        pd.DataFrame
            The concatenated (and transformed) result.
        """
        parts: List[pd.DataFrame] = []
        for i, chunk in enumerate(
                pd.read_csv(self.csv_path, chunksize=self.chunksize), start=1
        ):
            transformed = self.transformer.map_and_transform(chunk)
            parts.append(transformed)

            if i % self.progress_every == 0:
                logger.info(" … processed %d chunks (rows=%d)", i, i * self.chunksize)

        result = pd.concat(parts, ignore_index=True)
        logger.info("Finished streaming CSV. Total rows=%d", len(result))
        return result
